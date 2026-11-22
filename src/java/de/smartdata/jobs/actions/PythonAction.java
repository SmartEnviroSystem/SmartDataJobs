package de.smartdata.jobs.actions;

import de.smartdata.Remote.RemotePythonExecutor;
import de.smartdata.jobs.config.Configuration;
import de.smartdata.jobs.data.SmartDataAccessor;
import de.smartdata.jobs.data.SmartDataAccessorException;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonValue;

/** Executes Python script on the configured host */
public class PythonAction extends Action {

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
        scriptId = params.getString("scriptId", null);
        if (scriptId == null || !scriptId.matches("[A-Za-z0-9_-]{1,100}")) {
            throw new ActionConfigurationException("scriptId fehlt oder enthält ungültige Zeichen.");
        }
        if (!params.containsKey("scriptVersion") || params.getJsonNumber("scriptVersion") == null) {
            throw new ActionConfigurationException("scriptVersion fehlt.");
        }
        scriptVersion = params.getInt("scriptVersion");
        if (scriptVersion < 1) {
            throw new ActionConfigurationException("scriptVersion muss positiv sein.");
        }
        if (params.containsKey("parameters")) {
            if (params.get("parameters").getValueType() != JsonValue.ValueType.OBJECT) {
                throw new ActionConfigurationException("parameters muss ein JSON-Objekt sein.");
            }
            scriptParams = params.getJsonObject("parameters");
        }
        input = params.get("input");
        if (params.containsKey("timeoutSeconds")) {
            timeoutSeconds = params.getInt("timeoutSeconds");
        }
        if (timeoutSeconds < 1 || timeoutSeconds > 3600) {
            throw new ActionConfigurationException("timeoutSeconds muss zwischen 1 und 3600 liegen.");
        }
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

            JsonObjectBuilder fetchInfo = Json.createObjectBuilder();
            JsonArray matches = new SmartDataAccessor(smartDataUrl).fetchData(
                    storage, collection, "scriptId,version,approved,scriptContent",
                    "scriptId,eq," + scriptId, null, null, null, null, 100L, fetchInfo);
            JsonObject scriptRecord = null;
            for (JsonValue value : matches) {
                JsonObject row = value.asJsonObject();
                if (row.getInt("version", -1) == scriptVersion) {
                    scriptRecord = row;
                    break;
                }
            }
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
