package de.smartdata.Remote;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.xfer.FileSystemFile;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.*;

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
        String runId = UUID.randomUUID().toString();
        Path localScript = Files.createTempFile("smartdata-python-", ".py");
        ExecutorService readers = Executors.newFixedThreadPool(2);
        try {
            Files.writeString(localScript, source, StandardCharsets.UTF_8);
            String workdir = remoteWorkdir + "/" + runId;
            String scriptName = scriptId + "_v" + version + ".py";
            String remoteScript = workdir + "/" + scriptName;

            try (SSHClient client = new SSHClient()) {
                // Use the server user's ~/.ssh/known_hosts; never accept arbitrary host keys.
                client.loadKnownHosts();
                client.connect(host, port);
                client.authPassword(user, password);
                try (Session session = client.startSession()) {
                    Session.Command mkdir = session.exec("mkdir -p " + quote(workdir));
                    mkdir.join(15, TimeUnit.SECONDS);
                    if (mkdir.getExitStatus() == null || mkdir.getExitStatus() != 0) {
                        throw new IllegalStateException("Konnte Remote-Arbeitsverzeichnis nicht anlegen.");
                    }
                }
                try (SFTPClient sftp = client.newSFTPClient()) {
                    sftp.put(new FileSystemFile(localScript.toFile()), remoteScript);
                }

                try (Session session = client.startSession()) {
                    String command = quote(python) + " -I -u " + quote(remoteScript);
                    Session.Command process = session.exec(command);
                    Future<String> stdout = readers.submit(() -> readBounded(process.getInputStream(), 4_000_000));
                    Future<String> stderr = readers.submit(() -> readBounded(process.getErrorStream(), 256_000));
                    try (OutputStream stdin = process.getOutputStream()) {
                        stdin.write(input.toString().getBytes(StandardCharsets.UTF_8));
                    }
                    process.join(timeoutSeconds, TimeUnit.SECONDS);
                    if (process.isOpen()) {
                        process.close();
                        throw new TimeoutException("Python-Skript hat das Zeitlimit überschritten.");
                    }
                    String out = stdout.get(5, TimeUnit.SECONDS).trim();
                    String err = stderr.get(5, TimeUnit.SECONDS);
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
                    try (var reader = Json.createReader(new java.io.StringReader(out))) {
                        JsonObject parsed = reader.readObject();
                        return Json.createObjectBuilder(parsed).add("state", "finished").build();
                    }
                } finally {
                    try (Session cleanup = client.startSession()) {
                        Session.Command cmd = cleanup.exec("rm -rf " + quote(workdir));
                        cmd.join(10, TimeUnit.SECONDS);
                    } catch (Exception ignored) {
                        // Cleanup failure must not hide the execution result.
                    }
                }
            }
        } finally {
            readers.shutdownNow();
            Files.deleteIfExists(localScript);
        }
    }

    private static String quote(String value) {
        return "'" + value.replace("'", "'\\''") + "'";
    }

    private static String readBounded(InputStream input, int maxBytes) throws Exception {
        try (input; ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int total = 0;
            int count;
            while ((count = input.read(buffer)) != -1) {
                total += count;
                if (total > maxBytes) throw new IllegalStateException("Remote-Prozessausgabe ist zu groß.");
                out.write(buffer, 0, count);
            }
            return out.toString(StandardCharsets.UTF_8);
        }
    }
}
