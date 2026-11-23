package de.smartdata.jobs.actions;

import de.smartdata.Remote.RemotePythonExecutor;
import de.smartdata.jobs.config.Configuration;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import java.net.SocketTimeoutException;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.TimeoutException;

/** Executes Python script on the configured host */
public class PythonAction extends Action {

    public static final String NAME = "PythonScript";

    @Override
    public String getName() {
        return NAME;
    }

    private String scriptId;
    private int scriptVersion;
    private String scriptPath;
    private JsonObject scriptParams = Json.createObjectBuilder().build();
    private JsonValue input;
    private long timeoutSeconds = 300;

    @Override
    public JsonArray getParamsDescription() {
        return Json.createArrayBuilder()
                .add(Json.createObjectBuilder().add("name", "scriptPath").add("type", "string")
                        .add("description", "Relativer Pfad einer vorhandenen .py-Datei im Skriptverzeichnis des ML-Servers. Alternativ scriptId und scriptVersion angeben.").add("optional", true))
                .add(Json.createObjectBuilder().add("name", "scriptId").add("type", "string")
                        .add("description", "Für bestehende Jobs: ID in der Datei <scriptId>_v<scriptVersion>.py").add("optional", true))
                .add(Json.createObjectBuilder().add("name", "scriptVersion").add("type", "int")
                        .add("description", "Versionsnummer im Dateinamen, zusammen mit scriptId").add("optional", true))
                .add(Json.createObjectBuilder().add("name", "parameters").add("type", "json")
                        .add("description", "Fachliche Parameter für das Skript").add("optional", true))
                .add(Json.createObjectBuilder().add("name", "input").add("type", "json")
                        .add("description", "JSON-Eingabe für das Skript").add("optional", true))
                .add(Json.createObjectBuilder().add("name", "timeoutSeconds").add("type", "int")
                        .add("description", "Maximale Ausführungszeit in Sekunden").add("optional", true))
                .build();
    }

    @Override
    public void setParams(JsonObject params) throws ActionConfigurationException {
        if (params == null) {
            throw new ActionConfigurationException("Skriptpfad fehlt.");
        }
        String path;
        String id = null;
        int version = 0;
        if (params.containsKey("scriptPath")) {
            if (params.containsKey("scriptId") || params.containsKey("scriptVersion")) {
                throw new ActionConfigurationException("Nur scriptPath oder das Paar scriptId und scriptVersion angeben.");
            }
            if (!(params.get("scriptPath") instanceof JsonString file)
                    || !RemotePythonExecutor.isScriptPath(file.getString())) {
                throw new ActionConfigurationException("scriptPath muss ein relativer Pfad zu einer .py-Datei sein.");
            }
            path = file.getString();
        } else {
            if (!(params.get("scriptId") instanceof JsonString legacyId)
                    || !legacyId.getString().matches("[A-Za-z0-9_-]{1,100}")) {
                throw new ActionConfigurationException("scriptPath oder gültige scriptId mit scriptVersion angeben.");
            }
            id = legacyId.getString();
            version = integerParam(params, "scriptVersion");
            if (version < 1) {
                throw new ActionConfigurationException("scriptVersion muss positiv sein.");
            }
            path = id + "_v" + version + ".py";
        }
        int timeout = params.containsKey("timeoutSeconds") ? integerParam(params, "timeoutSeconds") : 300;
        if (timeout < 1 || timeout > 3600) {
            throw new ActionConfigurationException("timeoutSeconds muss zwischen 1 und 3600 liegen.");
        }
        JsonValue parameters = params.get("parameters");
        if (parameters != null && !(parameters instanceof JsonObject)) {
            throw new ActionConfigurationException("parameters muss ein JSON-Objekt sein.");
        }
        scriptPath = path;
        scriptId = id;
        scriptVersion = version;
        timeoutSeconds = timeout;
        scriptParams = parameters == null ? JsonValue.EMPTY_JSON_OBJECT : parameters.asJsonObject();
        input = params.get("input");
    }

    private static int integerParam(JsonObject params, String key) throws ActionConfigurationException {
        if (params.get(key) instanceof JsonNumber number) {
            try {
                return number.intValueExact();
            } catch (ArithmeticException ex) {
                throw new ActionConfigurationException(key + " muss eine ganze 32-Bit-Zahl sein.");
            }
        }
        throw new ActionConfigurationException(key + " fehlt oder ist keine Zahl.");
    }

    @Override
    public JsonObject call() {
        JsonObjectBuilder result = scriptReference();
        try {
            Configuration config = new Configuration("SmartDataJobs");

            JsonValue effectiveInput = input;
            if (effectiveInput == null) {
                effectiveInput = extractPreJobResult();
            }
            if (effectiveInput == null) {
                effectiveInput = Json.createObjectBuilder().build();
            }
            JsonObject payload = scriptReference()
                    .add("contractVersion", 1)
                    .add("parameters", scriptParams)
                    .add("input", effectiveInput)
                    .build();

            RemotePythonExecutor executor = new RemotePythonExecutor(
                    required(config, "python.ssh.user"),
                    required(config, "python.ssh.password"),
                    required(config, "python.ssh.host"),
                    Integer.parseInt(valueOr(config, "python.ssh.port", "22")),
                    required(config, "python.ssh.python"),
                    required(config, "python.ssh.remoteWorkdir"));
            JsonObject remoteResult = executor.execute(scriptPath, payload, timeoutSeconds);
            remoteResult.forEach(result::add);
            return result.build();
        } catch (NoSuchFileException ex) {
            return failed(result, "SCRIPT_NOT_FOUND", "Skript im Verzeichnis des ML-Servers nicht gefunden.");
        } catch (TimeoutException | SocketTimeoutException ex) {
            result.add("statemsg", "Python script timeout");
            return failed(result, "SCRIPT_TIMEOUT", "Python-Skript hat das Zeitlimit überschritten.");
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return failed(result, "EXECUTION_INTERRUPTED", "Python-Ausführung wurde unterbrochen.");
        } catch (Exception ex) {
            return failed(result, "REMOTE_EXECUTION_FAILED", ex.getMessage());
        }
    }

    private JsonObjectBuilder scriptReference() {
        if (scriptId != null) {
            return Json.createObjectBuilder().add("scriptId", scriptId).add("scriptVersion", scriptVersion);
        }
        return Json.createObjectBuilder().add("scriptPath", scriptPath);
    }

    private static JsonObject failed(JsonObjectBuilder result, String code, String message) {
        return result.add("state", "failed")
                .add("error", Json.createObjectBuilder().add("code", code)
                        .add("message", message == null ? code : message)).build();
    }

    private static String required(Configuration config, String key) {
        String value = config.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Fehlende Konfiguration: " + key);
        }
        return value;
    }

    private static String valueOr(Configuration config, String key, String fallback) {
        String value = config.getProperty(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
