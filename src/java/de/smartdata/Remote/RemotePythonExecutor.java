package de.smartdata.Remote;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Signal;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.sftp.Response.StatusCode;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** Executes a Python file already installed in the ML server's script directory. */
public class RemotePythonExecutor {
    private final String user;
    private final String password;
    private final String host;
    private final int port;
    private final String python;
    private final String remoteWorkdir;

    public RemotePythonExecutor(String user, String password, String host, int port,
            String python, String remoteWorkdir) {
        this.user = user;
        this.password = password;
        this.host = host;
        this.port = port;
        this.python = python;
        this.remoteWorkdir = remoteWorkdir;
    }

    public static boolean isScriptPath(String path) {
        if (path == null || !path.endsWith(".py") || path.contains("\\") || path.matches("^[A-Za-z]:.*")
                || path.chars().anyMatch(Character::isISOControl)) {
            return false;
        }
        for (String part : path.split("/", -1)) {
            if (part.isEmpty() || part.equals(".") || part.equals("..")) {
                return false;
            }
        }
        return true;
    }

    public JsonObject execute(String scriptPath, JsonObject input, long timeoutSeconds) throws Exception {
        if (!isScriptPath(scriptPath)) {
            throw new IllegalArgumentException("Expected a relative Python script path");
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        ExecutorService workers = Executors.newCachedThreadPool();
        AtomicReference<SSHClient> connection = new AtomicReference<>();
        AtomicReference<RunningProcess> active = new AtomicReference<>();
        Future<JsonObject> run = workers.submit(() -> {
            try (SSHClient client = new SSHClient()) {
                connection.set(client);
                client.loadKnownHosts();
                int remaining = remainingMillis(deadline);
                client.setConnectTimeout(remaining);
                client.setTimeout(remaining);
                client.connect(host, port);
                client.authPassword(user, password);
                String file = resolveScript(client, scriptPath);
                boolean nativeTimeout;
                try (Session probe = client.startSession()) {
                    Session.Command check = probe.exec("command -v timeout >/dev/null 2>&1");
                    check.join();
                    nativeTimeout = Integer.valueOf(0).equals(check.getExitStatus());
                }
                try (Session session = client.startSession()) {
                    String command = quote(python) + " -B -u -- " + quote(file);
                    int remainingMs = remainingMillis(deadline);
                    if (nativeTimeout) {
                        long seconds = (remainingMs + 999L) / 1000;
                        command = "timeout -k 1 " + seconds + " " + command;
                    }
                    String cwd = remoteWorkdir.startsWith("/") ? remoteWorkdir : "./" + remoteWorkdir;
                    Session.Command process = session.exec("cd " + quote(cwd) + " && exec " + command);
                    RunningProcess running = new RunningProcess(process, nativeTimeout);
                    active.set(running);
                    if (Thread.currentThread().isInterrupted() || System.nanoTime() >= deadline) {
                        running.stop();
                        throw new TimeoutException("Python script timeout");
                    }
                    return exchangeJson(process, workers, input);
                }
            }
        });
        try {
            return run.get(remainingMillis(deadline), TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException ex) {
            RunningProcess running = active.get();
            if (running != null) {
                // SSH signal delivery is best effort. Bound the wait even when the network is down.
                Future<?> stop = workers.submit(() -> {
                    running.stop();
                    return null;
                });
                try {
                    stop.get(200, TimeUnit.MILLISECONDS);
                } catch (TimeoutException | ExecutionException ignored) {
                    // Closing the connection below also terminates the local SSH session.
                }
            }
            throw ex;
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw ex;
        } finally {
            run.cancel(true);
            try {
                // Unblock SSH reads and writes even if the peer stops responding.
                SSHClient client = connection.get();
                Socket socket = client == null ? null : client.getSocket();
                if (socket != null) {
                    socket.close();
                }
            } finally {
                workers.shutdownNow();
            }
        }
    }

    private record RunningProcess(Session.Command command, boolean supervised) {
        void stop() throws IOException {
            // Let the host's timeout utility forward TERM and escalate to its process group.
            command.signal(supervised ? Signal.TERM : Signal.KILL);
        }
    }

    private String resolveScript(SSHClient client, String path) throws IOException {
        try (SFTPClient sftp = client.newSFTPClient()) {
            String root = sftp.canonicalize(remoteWorkdir);
            if (sftp.stat(root).getType() != FileMode.Type.DIRECTORY) {
                throw new IOException("Configured ML script directory is not a directory");
            }
            String prefix = root.endsWith("/") ? root : root + "/";
            try {
                String file = sftp.canonicalize(prefix + path);
                if (!file.startsWith(prefix) || !file.endsWith(".py")
                        || sftp.stat(file).getType() != FileMode.Type.REGULAR) {
                    throw new NoSuchFileException(path);
                }
                return file;
            } catch (SFTPException ex) {
                if (ex.getStatusCode() == StatusCode.NO_SUCH_FILE || ex.getStatusCode() == StatusCode.NO_SUCH_PATH) {
                    throw new NoSuchFileException(path);
                }
                throw ex;
            }
        }
    }

    private JsonObject exchangeJson(Session.Command process, ExecutorService workers, JsonObject input) throws Exception {
        Future<String> stdout = workers.submit(() -> readBounded(process.getInputStream(), 4_000_000));
        Future<String> stderr = workers.submit(() -> readBounded(process.getErrorStream(), 256_000));
        Future<?> writer = workers.submit(() -> {
            try (OutputStream stdin = process.getOutputStream()) {
                stdin.write(input.toString().getBytes(StandardCharsets.UTF_8));
            }
            return null;
        });
        process.join();
        String out = stdout.get();
        String err = stderr.get();
        Integer exit = process.getExitStatus();
        if (exit == null || exit != 0) {
            return Json.createObjectBuilder()
                    .add("state", "failed")
                    .add("error", Json.createObjectBuilder()
                            .add("code", "SCRIPT_EXIT_NONZERO")
                            .add("message", "Remote Python-Prozess fehlgeschlagen.")
                            .add("exitCode", exit == null ? -1 : exit)
                            .add("stderr", err.length() > 4000 ? err.substring(0, 4000) : err))
                    .build();
        }
        writer.get();
        try (var reader = Json.createReader(new java.io.StringReader(out))) {
            return Json.createObjectBuilder(reader.readObject()).add("state", "finished").build();
        }
    }

    private static int remainingMillis(long deadline) throws TimeoutException {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0 || Thread.currentThread().isInterrupted()) {
            throw new TimeoutException("Python script timeout");
        }
        return (int) Math.max(1L, TimeUnit.NANOSECONDS.toMillis(nanos));
    }

    private static String quote(String value) {
        return "'" + value.replace("'", "'\\''") + "'";
    }

    private static String readBounded(InputStream input, int maxBytes) throws Exception {
        try (input; ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) != -1) {
                if (out.size() + count > maxBytes) {
                    throw new IllegalStateException("Remote-Prozessausgabe ist zu groß.");
                }
                out.write(buffer, 0, count);
            }
            return out.toString(StandardCharsets.UTF_8);
        }
    }
}
