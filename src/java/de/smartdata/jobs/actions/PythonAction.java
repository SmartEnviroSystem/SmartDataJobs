package de.smartdata.jobs.actions;

import de.smartdata.Remote.RemotePythonExecutor;

public class PythonAction  extends  Action{

    private String scriptId;
    private JsonObject scriptParams;
    private long timeoutSeconds;

    @Override
    public JsonArray getParamsDescription() {
        return Json.createArrayBuilder()
                .add(Json.createObjectBuilder()
                        .add("name", "script")
                        .add("type", "string")
                        .add("description", "Freigegebene Skript-ID")
                        .add("optional", false))
                .add(Json.createObjectBuilder()
                        .add("name", "parameters")
                        .add("type", "json")
                        .add("description", "Fachliche Parameter für das Skript")
                        .add("optional", true))
                .add(Json.createObjectBuilder()
                        .add("name", "timeoutseconds")
                        .add("type", "integer")
                        .add("description", "Maximale Laufzeit")
                        .add("optional", true))
                .build();
    }

    @Override
    public void setParams(JsonObject params) throws ActionConfigurationException {

    }

    @Override
    public JsonObject call() {
        String name = "";
        String pwd = "";
        String host = "";
        RemotePythonExecutor executor = new RemotePythonExecutor(name,pwd, host);

        String script = executor.find("", 1);

        if (script == null){
          return null;
        }
        else {
            //Testen ob das Sktipt Approved ist
        }
        String result = executor.execute(script, "", "");


        return null;
    }

}
