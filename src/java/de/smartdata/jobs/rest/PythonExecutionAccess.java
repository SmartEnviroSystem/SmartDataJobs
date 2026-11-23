package de.smartdata.jobs.rest;

import de.fhbielefeld.smartuser.rest.RightsCache;
import de.fhbielefeld.smartuser.securitycontext.SmartPrincipal;
import de.fhbielefeld.smartuser.securitycontext.SmartPrincipalRight;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.DynamicFeature;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.FeatureContext;
import jakarta.ws.rs.core.Response;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/** Additional checks around the shared SmartUser filter for direct script execution. */
public class PythonExecutionAccess implements DynamicFeature {
    private static final String RUN_PATH = "pythonexecution/run";

    @Override
    public void configure(ResourceInfo resource, FeatureContext context) {
        if (resource.getResourceClass() == PythonResource.class) {
            context.register((ContainerRequestFilter) this::validateRequest, Priorities.AUTHENTICATION - 1);
            context.register((ContainerRequestFilter) this::requireExecutionGrant, Priorities.AUTHORIZATION);
        }
    }

    private void validateRequest(ContainerRequestContext request) {
        Cookie token = request.getCookies().get("authtoken");
        // SmartUser issues hex tokens. Validate before its JDBC filter sees request data.
        if (token == null || !token.getValue().matches("[a-fA-F0-9]{32}")) {
            request.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
        } else if (!RUN_PATH.equals(request.getUriInfo().getPath()) || !"POST".equals(request.getMethod())) {
            request.abortWith(Response.status(Response.Status.FORBIDDEN).build());
        }
    }

    private void requireExecutionGrant(ContainerRequestContext request) {
        if (!(request.getSecurityContext().getUserPrincipal() instanceof SmartPrincipal principal)) {
            request.abortWith(Response.status(Response.Status.UNAUTHORIZED).build());
            return;
        }
        try {
            String module = InitialContext.doLookup("java:module/ModuleName");
            String token = request.getCookies().get("authtoken").getValue();
            String key = token + "/rest." + module + "." + RUN_PATH.replace('/', '.') + "/POST";
            SmartPrincipalRight grant = RightsCache.getInstance().get(key);
            // A record-specific or neighbouring grant does not authorize this operation.
            if (grant == null || !grant.getIds().contains(Long.MAX_VALUE)
                    || !principal.getName().equals(grant.getUsername())) {
                request.abortWith(Response.status(Response.Status.FORBIDDEN).build());
            }
        } catch (NamingException ex) {
            request.abortWith(Response.status(Response.Status.SERVICE_UNAVAILABLE).build());
        }
    }
}
