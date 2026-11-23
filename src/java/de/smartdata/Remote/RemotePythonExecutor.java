package de.smartdata.Remote;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** Transfers an approved script to the configured Python host and returns its JSON stdout. */
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

    public JsonObject execute(String scriptId, int version, String source,
            JsonObject input, long timeoutSeconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        ExecutorService workers = Executors.newFixedThreadPool(4);
        AtomicReference<SSHClient> connection = new AtomicReference<>();
        Future<JsonObject> run = workers.submit(() -> {
            try (SSHClient client = new SSHClient()) {
                connection.set(client);
                client.loadKnownHosts();
                int remaining = remainingMillis(deadline);
                client.setConnectTimeout(remaining);
                client.setTimeout(remaining);
                client.connect(host, port);
                client.authPassword(user, password);
                try (Session session = client.startSession()) {
                    return runScript(session, workers, deadline, scriptId + "_v" + version + ".py", source, input);
                }
            }
        });
        try {
            return run.get(remainingMillis(deadline), TimeUnit.MILLISECONDS);
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

    private JsonObject runScript(Session session, ExecutorService workers, long deadline, String scriptName,
            String source, JsonObject input) throws Exception {
        byte[] script = source.getBytes(StandardCharsets.UTF_8);
        String launcher;
        try (InputStream resource = RemotePythonExecutor.class.getResourceAsStream("python_runner.py")) {
            if (resource == null) {
                throw new IOException("Missing Python runner in deployment");
            }
            launcher = new String(resource.readAllBytes(), StandardCharsets.UTF_8);
        }
        String command = "exec " + quote(python) + " -I -c " + quote(launcher) + " " + quote(remoteWorkdir)
                + " " + quote(scriptName) + " " + script.length + " " + remainingMillis(deadline);
        Session.Command process = session.exec(command);
        Future<String> stdout = workers.submit(() -> readBounded(process.getInputStream(), 4_000_000));
        Future<String> stderr = workers.submit(() -> readBounded(process.getErrorStream(), 256_000));
        Future<?> writer = workers.submit(() -> {
            try (OutputStream stdin = process.getOutputStream()) {
                stdin.write(script);
                stdin.write(input.toString().getBytes(StandardCharsets.UTF_8));
            }
            return null;
        });
        process.join();
        String out = stdout.get();
        String err = stderr.get();
        Integer exit = process.getExitStatus();
        if (Integer.valueOf(124).equals(exit) && err.endsWith("\nSMARTDATA_SCRIPT_TIMEOUT\n")) {
            throw new TimeoutException("Python script timeout");
        }
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
