package de.smartdata.jobs.actions;

import de.smartdata.Remote.RemotePythonExecutor;
import de.smartdata.jobs.config.Configuration;
import de.smartdata.jobs.data.SmartDataAccessor;
import de.smartdata.jobs.data.SmartDataAccessorException;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonNumber;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import java.net.SocketTimeoutException;
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
    private JsonObject scriptParams = Json.createObjectBuilder().build();
    private JsonValue input;
    private long timeoutSeconds = 300;

    @Override
    public JsonArray getParamsDescription() {
        return Json.createArrayBuilder()
                .add(Json.createObjectBuilder().add("name", "scriptId").add("type", "string")
                        .add("description", "ID des freigegebenen Python-Skripts").add("optional", false))
                .add(Json.createObjectBuilder().add("name", "scriptVersion").add("type", "int")
                        .add("description", "Unveränderliche Skriptversion").add("optional", false))
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
        if (params == null || !(params.get("scriptId") instanceof JsonString id)
                || !id.getString().matches("[A-Za-z0-9_-]{1,100}")) {
            throw new ActionConfigurationException("scriptId fehlt oder enthält ungültige Zeichen.");
        }
        int version = integerParam(params, "scriptVersion");
        if (version < 1) {
            throw new ActionConfigurationException("scriptVersion muss positiv sein.");
        }
        int timeout = params.containsKey("timeoutSeconds") ? integerParam(params, "timeoutSeconds") : 300;
        if (timeout < 1 || timeout > 3600) {
            throw new ActionConfigurationException("timeoutSeconds muss zwischen 1 und 3600 liegen.");
        }
        JsonValue parameters = params.get("parameters");
        if (parameters != null && !(parameters instanceof JsonObject)) {
            throw new ActionConfigurationException("parameters muss ein JSON-Objekt sein.");
        }
        scriptId = id.getString();
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
        JsonObjectBuilder result = Json.createObjectBuilder()
                .add("scriptId", scriptId)
                .add("scriptVersion", scriptVersion);
        try {
            Configuration config = new Configuration("SmartDataJobs");
            String smartDataUrl = required(config, "python.smartdata.url");
            String storage = valueOr(config, "python.scripts.storage", "public");
            String collection = valueOr(config, "python.scripts.collection", "pythonscripts");

            JsonObject scriptRecord = new SmartDataAccessor(smartDataUrl)
                    .fetchPythonScriptVersion(storage, collection, scriptId, scriptVersion);
            if (scriptRecord == null) {
                return failed(result, "SCRIPT_NOT_FOUND", "Skriptversion wurde nicht gefunden.");
            }
            if (!scriptRecord.getBoolean("approved", false)) {
                return failed(result, "SCRIPT_NOT_APPROVED", "Skriptversion ist nicht freigegeben.");
            }
            String content = scriptRecord.getString("scriptContent", null);
            if (content == null || content.isBlank()) {
                return failed(result, "SCRIPT_EMPTY", "Skriptinhalt fehlt.");
            }

            JsonValue effectiveInput = input;
            if (effectiveInput == null) {
                effectiveInput = extractPreJobResult();
            }
            if (effectiveInput == null) {
                effectiveInput = Json.createObjectBuilder().build();
            }
            JsonObject payload = Json.createObjectBuilder()
                    .add("contractVersion", 1)
                    .add("scriptId", scriptId)
                    .add("scriptVersion", scriptVersion)
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
            JsonObject remoteResult = executor.execute(scriptId, scriptVersion, content, payload, timeoutSeconds);
            remoteResult.forEach(result::add);
            return result.build();
        } catch (SmartDataAccessorException ex) {
            return failed(result, "SCRIPT_LOOKUP_FAILED", ex.getMessage());
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
