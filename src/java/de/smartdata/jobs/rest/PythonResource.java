package de.smartdata.jobs.rest;

import de.fhbielefeld.smartuser.annotations.SmartUserAuth;
import de.smartdata.jobs.actions.ActionConfigurationException;
import de.smartdata.jobs.actions.PythonAction;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;

/** REST endpoints for invoking the remote Python action. */
@Path("/pythonexecution")
@Produces(MediaType.APPLICATION_JSON)
public class PythonResource {

    @POST
    @SmartUserAuth
    @Path("/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary = "Run approved Python script",
            description = "Runs an approved script version on the configured Python host.")
    @APIResponse(responseCode = "200", description = "Script completed",
            content = @Content(mediaType = MediaType.APPLICATION_JSON))
    @APIResponse(responseCode = "400", description = "Invalid request or script is not approved",
            content = @Content(mediaType = MediaType.APPLICATION_JSON))
    @APIResponse(responseCode = "401", description = "SmartUser authentication required")
    @APIResponse(responseCode = "403", description = "Python execution permission required")
    @APIResponse(responseCode = "502", description = "Remote execution failed",
            content = @Content(mediaType = MediaType.APPLICATION_JSON))
    public Response run(JsonObject request) {
        PythonAction action = new PythonAction();
        try {
            action.setParams(request);
        } catch (ActionConfigurationException ex) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(error("INVALID_REQUEST", ex.getMessage())).build();
        }

        JsonObject result = action.call();
        String state = result.getString("state", "failed");
        if ("finished".equals(state)) {
            return Response.ok(result).build();
        }
        String code = result.containsKey("error")
                ? result.getJsonObject("error").getString("code", "REMOTE_EXECUTION_FAILED")
                : "REMOTE_EXECUTION_FAILED";
        if ("SCRIPT_NOT_FOUND".equals(code) || "SCRIPT_NOT_APPROVED".equals(code)
                || "SCRIPT_EMPTY".equals(code)) {
            return Response.status(Response.Status.BAD_REQUEST).entity(result).build();
        }
        return Response.status(Response.Status.BAD_GATEWAY).entity(result).build();
    }

    private static JsonObject error(String code, String message) {
        return Json.createObjectBuilder().add("state", "failed")
                .add("error", Json.createObjectBuilder().add("code", code)
                        .add("message", message == null ? code : message)).build();
    }
}
