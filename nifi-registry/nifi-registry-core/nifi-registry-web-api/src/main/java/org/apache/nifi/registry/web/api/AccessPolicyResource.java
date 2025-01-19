/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.registry.web.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.authorization.AccessPolicy;
import org.apache.nifi.registry.authorization.AccessPolicySummary;
import org.apache.nifi.registry.authorization.Resource;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.revision.web.ClientIdParameter;
import org.apache.nifi.registry.revision.web.LongParameter;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.Collections;
import java.util.List;

/**
 * RESTful endpoint for managing access policies.
 */
@Component
@Path("/policies")
@Tag(name = "Policies")
public class AccessPolicyResource extends ApplicationResource {

    @Autowired
    public AccessPolicyResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    /**
     * Create a new access policy.
     *
     * @param httpServletRequest  request
     * @param requestAccessPolicy the access policy to create.
     * @return The created access policy.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Create access policy",
            responses = {
                    @ApiResponse(content = @Content(schema = @Schema(implementation = AccessPolicy.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry might not be configured to use a ConfigurableAccessPolicyProvider.")
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/policies")}
                    )
            }
    )
    public Response createAccessPolicy(
            @Context final HttpServletRequest httpServletRequest,
            @Parameter(description = "The access policy configuration details.", required = true) final AccessPolicy requestAccessPolicy) {

        AccessPolicy createdPolicy = serviceFacade.createAccessPolicy(requestAccessPolicy);
        String locationUri = generateAccessPolicyUri(createdPolicy);
        return generateCreatedResponse(URI.create(locationUri), createdPolicy).build();
    }

    /**
     * Retrieves all access policies
     *
     * @return A list of access policies
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get all access policies",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            content = @Content(array = @ArraySchema(schema = @Schema(implementation = AccessPolicy.class)))
                    ),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/policies")}
                    )
            }
    )
    public Response getAccessPolicies() {
        List<AccessPolicy> accessPolicies = serviceFacade.getAccessPolicies();
        if (accessPolicies == null) {
            accessPolicies = Collections.emptyList();
        }

        return generateOkResponse(accessPolicies).build();
    }

    /**
     * Retrieves the specified access policy.
     *
     * @param identifier The id of the access policy to retrieve
     * @return An accessPolicyEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Get access policy",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AccessPolicy.class))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/policies")}
                    )
            }
    )
    public Response getAccessPolicy(
            @Parameter(description = "The access policy id.", required = true)
            @PathParam("id") final String identifier) {
        final AccessPolicy accessPolicy = serviceFacade.getAccessPolicy(identifier);
        return generateOkResponse(accessPolicy).build();
    }


    /**
     * Retrieve a specified access policy for a given (action, resource) pair.
     *
     * @param action      the action, i.e. "read", "write"
     * @param rawResource the name of the resource as a raw string
     * @return An access policy.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{action}/{resource: .+}")
    @Operation(
            summary = "Get access policy for resource",
            description = "Gets an access policy for the specified action and resource",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AccessPolicy.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/policies")}
                    )
            }
    )
    public Response getAccessPolicyForResource(
            @Parameter(description = "The request action.", required = true)
            @PathParam("action") final String action,
            @Parameter(description = "The resource of the policy.", required = true)
            @PathParam("resource") final String rawResource) {

        // parse the action and resource type
        final RequestAction requestAction = RequestAction.valueOfValue(action);
        final String resource = "/" + rawResource;

        final AccessPolicy accessPolicy = serviceFacade.getAccessPolicy(resource, requestAction);
        return generateOkResponse(accessPolicy).build();
    }


    /**
     * Update an access policy.
     *
     * @param httpServletRequest  request
     * @param identifier          The id of the access policy to update.
     * @param requestAccessPolicy An access policy.
     * @return the updated access policy.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Update access policy",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AccessPolicy.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry might not be configured to use a ConfigurableAccessPolicyProvider.")
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/policies")}
                    )
            }
    )
    public Response updateAccessPolicy(
            @Context final HttpServletRequest httpServletRequest,
            @Parameter(description = "The access policy id.", required = true)
            @PathParam("id") final String identifier,
            @Parameter(description = "The access policy configuration details.", required = true) final AccessPolicy requestAccessPolicy) {

        if (requestAccessPolicy == null) {
            throw new IllegalArgumentException("Access policy details must be specified when updating a policy.");
        }
        if (!identifier.equals(requestAccessPolicy.getIdentifier())) {
            throw new IllegalArgumentException(String.format("The policy id in the request body (%s) does not equal the "
                    + "policy id of the requested resource (%s).", requestAccessPolicy.getIdentifier(), identifier));
        }

        final AccessPolicy createdPolicy = serviceFacade.updateAccessPolicy(requestAccessPolicy);
        return generateOkResponse(createdPolicy).build();
    }


    /**
     * Remove a specified access policy.
     *
     * @param httpServletRequest request
     * @param identifier         The id of the access policy to remove.
     * @return The deleted access policy
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Delete access policy",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AccessPolicy.class))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry might not be configured to use a ConfigurableAccessPolicyProvider.")
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "delete"),
                            @ExtensionProperty(name = "resource", value = "/policies")}
                    )
            }
    )
    public Response removeAccessPolicy(
            @Context final HttpServletRequest httpServletRequest,
            @Parameter(description = "The version is used to verify the client is working with the latest version of the entity.", required = true)
            @QueryParam(VERSION) final LongParameter version,
            @Parameter(description = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.")
            @QueryParam(CLIENT_ID)
            @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @Parameter(description = "The access policy id.", required = true)
            @PathParam("id") final String identifier) {

        final RevisionInfo revisionInfo = getRevisionInfo(version, clientId);
        final AccessPolicy deletedPolicy = serviceFacade.deleteAccessPolicy(identifier, revisionInfo);
        return generateOkResponse(deletedPolicy).build();
    }

    /**
     * Gets the available resources that support access/authorization policies.
     *
     * @return A resourcesEntity.
     */
    @GET
    @Path("/resources")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get available resources",
            description = "Gets the available resources that support access/authorization policies",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = Resource.class)))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/policies")}
                    )
            }
    )
    public Response getResources() {
        final List<Resource> resources = serviceFacade.getResources();
        return generateOkResponse(resources).build();
    }

    private String generateAccessPolicyUri(final AccessPolicySummary accessPolicy) {
        return generateResourceUri("policies", accessPolicy.getIdentifier());
    }

}
