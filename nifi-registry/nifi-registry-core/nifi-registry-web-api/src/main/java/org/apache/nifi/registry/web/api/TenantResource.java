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
import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.event.EventFactory;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.revision.web.ClientIdParameter;
import org.apache.nifi.registry.revision.web.LongParameter;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;

/**
 * RESTful endpoints for managing tenants, ie, users and user groups.
 */
@Component
@Path("tenants")
@Tag(name = "Tenants")
public class TenantResource extends ApplicationResource {

    @Autowired
    public TenantResource(final ServiceFacade serviceFacade,
                          final EventService eventService) {
        super(serviceFacade, eventService);
    }


    // ---------- User endpoints --------------------------------------------------------------------------------------

    /**
     * Creates a new user.
     *
     * @param httpServletRequest request
     * @param requestUser the user to create
     * @return the user that was created
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users")
    @Operation(
            summary = "Create user",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = User.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/tenants")}
                    )
            }
    )
    public Response createUser(
            @Context final HttpServletRequest httpServletRequest,
            @Parameter(description = "The user configuration details.", required = true) final User requestUser) {

        final User createdUser = serviceFacade.createUser(requestUser);
        publish(EventFactory.userCreated(createdUser));

        String locationUri = generateUserUri(createdUser);
        return generateCreatedResponse(URI.create(locationUri), createdUser).build();
    }

    /**
     * Retrieves all the of users in this NiFi.
     *
     * @return a list of users
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users")
    @Operation(
            summary = "Get all users",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = User.class)))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/tenants")}
                    )
            }
    )
    public Response getUsers() {
        // get all the users
        final List<User> users = serviceFacade.getUsers();

        // generate the response
        return generateOkResponse(users).build();
    }

    /**
     * Retrieves the specified user.
     *
     * @param identifier The id of the user to retrieve
     * @return An userEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users/{id}")
    @Operation(
            summary = "Get user",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = User.class))),
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
                            @ExtensionProperty(name = "resource", value = "/tenants")}
                    )
            }
    )
    public Response getUser(
            @Parameter(description = "The user id.", required = true)
            @PathParam("id") final String identifier) {
        final User user = serviceFacade.getUser(identifier);
        return generateOkResponse(user).build();
    }

    /**
     * Updates a user.
     *
     * @param httpServletRequest request
     * @param identifier The id of the user to update
     * @param requestUser The user with updated fields.
     * @return The updated user
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users/{id}")
    @Operation(
            summary = "Update user",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = User.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/tenants")}
                    )
            }
    )
    public Response updateUser(
            @Context final HttpServletRequest httpServletRequest,
            @Parameter(description = "The user id.", required = true)
            @PathParam("id") final String identifier,
            @Parameter(description = "The user configuration details.", required = true) final User requestUser) {

        if (requestUser == null) {
            throw new IllegalArgumentException("User details must be specified when updating a user.");
        }
        if (!identifier.equals(requestUser.getIdentifier())) {
            throw new IllegalArgumentException(String.format("The user id in the request body (%s) does not equal the "
                    + "user id of the requested resource (%s).", requestUser.getIdentifier(), identifier));
        }

        final User updatedUser = serviceFacade.updateUser(requestUser);
        publish(EventFactory.userUpdated(updatedUser));
        return generateOkResponse(updatedUser).build();
    }

    /**
     * Removes the specified user.
     *
     * @param httpServletRequest request
     * @param identifier The id of the user to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users/{id}")
    @Operation(
            summary = "Delete user",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = User.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "delete"),
                            @ExtensionProperty(name = "resource", value = "/tenants")}
                    )
            }
    )
    public Response removeUser(
            @Context final HttpServletRequest httpServletRequest,
            @Parameter(description = "The version is used to verify the client is working with the latest version of the entity.", required = true)
            @QueryParam(VERSION) final LongParameter version,
            @Parameter(description = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.")
            @QueryParam(CLIENT_ID)
            @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @Parameter(description = "The user id.", required = true)
            @PathParam("id") final String identifier) {

        final RevisionInfo revisionInfo = getRevisionInfo(version, clientId);
        final User user = serviceFacade.deleteUser(identifier, revisionInfo);
        publish(EventFactory.userDeleted(user));
        return generateOkResponse(user).build();
    }


    // ---------- User Group endpoints --------------------------------------------------------------------------------

    /**
     * Creates a new user group.
     *
     * @param httpServletRequest request
     * @param requestUserGroup the user group to create
     * @return the created user group
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups")
    @Operation(
            summary = "Create user group",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = UserGroup.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/tenants")}
                    )
            }
    )
    public Response createUserGroup(
            @Context final HttpServletRequest httpServletRequest,
            @Parameter(description = "The user group configuration details.", required = true) final UserGroup requestUserGroup) {

        final UserGroup createdGroup = serviceFacade.createUserGroup(requestUserGroup);
        publish(EventFactory.userGroupCreated(createdGroup));

        final String locationUri = generateUserGroupUri(createdGroup);
        return generateCreatedResponse(URI.create(locationUri), createdGroup).build();
    }

    /**
     * Retrieves all the of user groups in this NiFi.
     *
     * @return a list of all user groups in this NiFi.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups")
    @Operation(
            summary = "Get user groups",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = UserGroup.class)))),
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
                            @ExtensionProperty(name = "resource", value = "/tenants")}
                    )
            }
    )
    public Response getUserGroups() {
        final List<UserGroup> userGroups = serviceFacade.getUserGroups();
        return generateOkResponse(userGroups).build();
    }

    /**
     * Retrieves the specified user group.
     *
     * @param identifier The id of the user group to retrieve
     * @return An userGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups/{id}")
    @Operation(
            summary = "Get user group",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UserGroup.class))),
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
                            @ExtensionProperty(name = "resource", value = "/tenants")}
                    )
            }
    )
    public Response getUserGroup(
            @Parameter(description = "The user group id.", required = true)
            @PathParam("id") final String identifier) {
        final UserGroup userGroup = serviceFacade.getUserGroup(identifier);
        return generateOkResponse(userGroup).build();
    }

    /**
     * Updates a user group.
     *
     * @param httpServletRequest request
     * @param identifier The id of the user group to update.
     * @param requestUserGroup The user group with updated fields.
     * @return The resulting, updated user group.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups/{id}")
    @Operation(
            summary = "Update user group",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UserGroup.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/tenants")}
                    )
            }
    )
    public Response updateUserGroup(
            @Context final HttpServletRequest httpServletRequest,
            @Parameter(description = "The user group id.", required = true)
            @PathParam("id") final String identifier,
            @Parameter(description = "The user group configuration details.", required = true) final UserGroup requestUserGroup) {

        if (requestUserGroup == null) {
            throw new IllegalArgumentException("User group details must be specified to update a user group.");
        }
        if (!identifier.equals(requestUserGroup.getIdentifier())) {
            throw new IllegalArgumentException(String.format("The user group id in the request body (%s) does not equal the "
                    + "user group id of the requested resource (%s).", requestUserGroup.getIdentifier(), identifier));
        }

        final UserGroup updatedUserGroup = serviceFacade.updateUserGroup(requestUserGroup);
        publish(EventFactory.userGroupUpdated(updatedUserGroup));
        return generateOkResponse(updatedUserGroup).build();
    }

    /**
     * Removes the specified user group.
     *
     * @param httpServletRequest request
     * @param identifier The id of the user group to remove.
     * @return The deleted user group.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups/{id}")
    @Operation(
            summary = "Delete user group",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UserGroup.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "delete"),
                            @ExtensionProperty(name = "resource", value = "/tenants")}
                    )
            }
    )
    public Response removeUserGroup(
            @Context final HttpServletRequest httpServletRequest,
            @Parameter(description = "The version is used to verify the client is working with the latest version of the entity.", required = true)
            @QueryParam(VERSION) final LongParameter version,
            @Parameter(description = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.")
            @QueryParam(CLIENT_ID)
            @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @Parameter(description = "The user group id.", required = true)
            @PathParam("id") final String identifier) {

        final RevisionInfo revisionInfo = getRevisionInfo(version, clientId);
        final UserGroup userGroup = serviceFacade.deleteUserGroup(identifier, revisionInfo);
        publish(EventFactory.userGroupDeleted(userGroup));
        return generateOkResponse(userGroup).build();
    }

    private String generateUserUri(final User user) {
        return generateResourceUri("tenants", "users", user.getIdentifier());
    }

    private String generateUserGroupUri(final UserGroup userGroup) {
        return generateResourceUri("tenants", "user-groups", userGroup.getIdentifier());
    }

}
