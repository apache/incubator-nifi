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
package org.apache.nifi.web.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.AuthorizerCapabilityDetection;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.entity.TenantsEntity;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.api.entity.UserGroupsEntity;
import org.apache.nifi.web.api.entity.UsersEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.net.URI;
import java.util.Date;
import java.util.Set;

@Controller
@Path("tenants")
@Tag(name = "Tenants")
public class TenantsResource extends ApplicationResource {

    private final NiFiServiceFacade serviceFacade;
    private final Authorizer authorizer;

    public TenantsResource(
            NiFiServiceFacade serviceFacade,
            Authorizer authorizer,
            NiFiProperties properties,
            @Autowired(required = false) final RequestReplicator requestReplicator,
            @Autowired(required = false) final ClusterCoordinator clusterCoordinator,
            @Autowired(required = false) final FlowController flowController
    ) {
        this.serviceFacade = serviceFacade;
        this.authorizer = authorizer;
        setProperties(properties);
        setRequestReplicator(requestReplicator);
        setClusterCoordinator(clusterCoordinator);
        setFlowController(flowController);
    }

    /**
     * Populates the uri for the specified users.
     *
     * @param userEntities users
     * @return user entities
     */
    public Set<UserEntity> populateRemainingUserEntitiesContent(Set<UserEntity> userEntities) {
        for (UserEntity userEntity : userEntities) {
            populateRemainingUserEntityContent(userEntity);
        }
        return userEntities;
    }

    /**
     * Populates the uri for the specified user.
     *
     * @param userEntity userEntity
     * @return userEntity
     */
    public UserEntity populateRemainingUserEntityContent(UserEntity userEntity) {
        userEntity.setUri(generateResourceUri("tenants", "users", userEntity.getId()));
        return userEntity;
    }

    /**
     * Creates a new user.
     *
     * @param requestUserEntity An userEntity.
     * @return An userEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users")
    @Operation(
            summary = "Creates a user",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = UserEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /tenants")
            }
    )
    public Response createUser(
            @Parameter(
                    description = "The user configuration details.",
                    required = true
            ) final UserEntity requestUserEntity) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (requestUserEntity == null || requestUserEntity.getComponent() == null) {
            throw new IllegalArgumentException("User details must be specified.");
        }

        if (requestUserEntity.getRevision() == null || (requestUserEntity.getRevision().getVersion() == null || requestUserEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new User.");
        }

        if (requestUserEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("User ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestUserEntity.getComponent().getIdentity())) {
            throw new IllegalArgumentException("User identity must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestUserEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestUserEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestUserEntity,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                userEntity -> {
                    // set the user id as appropriate
                    userEntity.getComponent().setId(generateUuid());

                    // get revision from the config
                    final RevisionDTO revisionDTO = userEntity.getRevision();
                    Revision revision = new Revision(revisionDTO.getVersion(), revisionDTO.getClientId(), userEntity.getComponent().getId());

                    // create the user and generate the json
                    final UserEntity entity = serviceFacade.createUser(revision, userEntity.getComponent());
                    populateRemainingUserEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Retrieves the specified user.
     *
     * @param id The id of the user to retrieve
     * @return An userEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users/{id}")
    @Operation(
            summary = "Gets a user",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UserEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /tenants")
            }
    )
    public Response getUser(
            @Parameter(
                    description = "The user id.",
                    required = true
            )
            @PathParam("id") final String id) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_MANAGED_AUTHORIZER);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable tenants = lookup.getTenant();
            tenants.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the user
        final UserEntity entity = serviceFacade.getUser(id);
        populateRemainingUserEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves all the of users in this NiFi.
     *
     * @return A UsersEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users")
    @Operation(
            summary = "Gets all users",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UsersEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /tenants")
            }
    )
    public Response getUsers() {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_MANAGED_AUTHORIZER);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable tenants = lookup.getTenant();
            tenants.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get all the users
        final Set<UserEntity> users = serviceFacade.getUsers();

        // create the response entity
        final UsersEntity entity = new UsersEntity();
        entity.setGenerated(new Date());
        entity.setUsers(populateRemainingUserEntitiesContent(users));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Updates a user.
     *
     * @param id The id of the user to update.
     * @param requestUserEntity An userEntity.
     * @return An userEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users/{id}")
    @Operation(
            summary = "Updates a user",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UserEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /tenants")
            }
    )
    public Response updateUser(
            @Parameter(
                    description = "The user id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The user configuration details.",
                    required = true
            ) final UserEntity requestUserEntity) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (requestUserEntity == null || requestUserEntity.getComponent() == null) {
            throw new IllegalArgumentException("User details must be specified.");
        }

        if (requestUserEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final UserDTO requestUserDTO = requestUserEntity.getComponent();
        if (!id.equals(requestUserDTO.getId())) {
            throw new IllegalArgumentException(String.format("The user id (%s) in the request body does not equal the "
                    + "user id of the requested resource (%s).", requestUserDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestUserEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestUserEntity.isDisconnectedNodeAcknowledged());
        }

        // Extract the revision
        final Revision requestRevision = getRevision(requestUserEntity, id);
        return withWriteLock(
                serviceFacade,
                requestUserEntity,
                requestRevision,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, userEntity) -> {
                    // update the user
                    final UserEntity entity = serviceFacade.updateUser(revision, userEntity.getComponent());
                    populateRemainingUserEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified user.
     *
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the user to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("users/{id}")
    @Operation(
            summary = "Deletes a user",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UserEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /tenants")
            }
    )
    public Response removeUser(
            @Parameter(
                    description = "The revision is used to verify the client is working with the latest version of the flow."
            )
            @QueryParam(VERSION) final LongParameter version,
            @Parameter(
                    description = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response."
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @Parameter(
                    description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(
                    description = "The user id.",
                    required = true
            )
            @PathParam("id") final String id) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final UserEntity requestUserEntity = new UserEntity();
        requestUserEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestUserEntity,
                requestRevision,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, userEntity) -> {
                    // delete the specified user
                    final UserEntity entity = serviceFacade.deleteUser(revision, userEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Populates the uri for the specified user groups.
     *
     * @param userGroupEntities user groups
     * @return user group entities
     */
    public Set<UserGroupEntity> populateRemainingUserGroupEntitiesContent(Set<UserGroupEntity> userGroupEntities) {
        for (UserGroupEntity userGroupEntity : userGroupEntities) {
            populateRemainingUserGroupEntityContent(userGroupEntity);
        }
        return userGroupEntities;
    }

    /**
     * Populates the uri for the specified user group.
     *
     * @param userGroupEntity userGroupEntity
     * @return userGroupEntity
     */
    public UserGroupEntity populateRemainingUserGroupEntityContent(UserGroupEntity userGroupEntity) {
        userGroupEntity.setUri(generateResourceUri("tenants", "user-groups", userGroupEntity.getId()));
        return userGroupEntity;
    }

    /**
     * Creates a new user group.
     *
     * @param requestUserGroupEntity An userGroupEntity.
     * @return An userGroupEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups")
    @Operation(
            summary = "Creates a user group",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = UserGroupEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /tenants")
            }
    )
    public Response createUserGroup(
            @Parameter(
                    description = "The user group configuration details.",
                    required = true
            ) final UserGroupEntity requestUserGroupEntity) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (requestUserGroupEntity == null || requestUserGroupEntity.getComponent() == null) {
            throw new IllegalArgumentException("User group details must be specified.");
        }

        if (requestUserGroupEntity.getRevision() == null || (requestUserGroupEntity.getRevision().getVersion() == null || requestUserGroupEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new User Group.");
        }

        if (requestUserGroupEntity.getComponent().getId() != null) {
            throw new IllegalArgumentException("User group ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestUserGroupEntity.getComponent().getIdentity())) {
            throw new IllegalArgumentException("User group identity must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestUserGroupEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestUserGroupEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestUserGroupEntity,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                userGroupEntity -> {
                    // set the user group id as appropriate
                    userGroupEntity.getComponent().setId(generateUuid());

                    // get revision from the config
                    final RevisionDTO revisionDTO = userGroupEntity.getRevision();
                    Revision revision = new Revision(revisionDTO.getVersion(), revisionDTO.getClientId(), userGroupEntity.getComponent().getId());

                    // create the user group and generate the json
                    final UserGroupEntity entity = serviceFacade.createUserGroup(revision, userGroupEntity.getComponent());
                    populateRemainingUserGroupEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Retrieves the specified user group.
     *
     * @param id The id of the user group to retrieve
     * @return An userGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups/{id}")
    @Operation(
            summary = "Gets a user group",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UserGroupEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /tenants")
            }
    )
    public Response getUserGroup(
            @Parameter(
                    description = "The user group id.",
                    required = true
            )
            @PathParam("id") final String id) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_MANAGED_AUTHORIZER);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable tenants = lookup.getTenant();
            tenants.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the user group
        final UserGroupEntity entity = serviceFacade.getUserGroup(id);
        populateRemainingUserGroupEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves all the of user groups in this NiFi.
     *
     * @return A UserGroupsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups")
    @Operation(
            summary = "Gets all user groups",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UserGroupsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /tenants")
            }
    )
    public Response getUserGroups() {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_MANAGED_AUTHORIZER);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable tenants = lookup.getTenant();
            tenants.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get all the user groups
        final Set<UserGroupEntity> users = serviceFacade.getUserGroups();

        // create the response entity
        final UserGroupsEntity entity = new UserGroupsEntity();
        entity.setUserGroups(populateRemainingUserGroupEntitiesContent(users));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Updates a user group.
     *
     * @param id The id of the user group to update.
     * @param requestUserGroupEntity An userGroupEntity.
     * @return An userGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups/{id}")
    @Operation(
            summary = "Updates a user group",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UserGroupEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /tenants")
            }
    )
    public Response updateUserGroup(
            @Parameter(
                    description = "The user group id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The user group configuration details.",
                    required = true
            ) final UserGroupEntity requestUserGroupEntity) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (requestUserGroupEntity == null || requestUserGroupEntity.getComponent() == null) {
            throw new IllegalArgumentException("User group details must be specified.");
        }

        if (requestUserGroupEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final UserGroupDTO requestUserGroupDTO = requestUserGroupEntity.getComponent();
        if (!id.equals(requestUserGroupDTO.getId())) {
            throw new IllegalArgumentException(String.format("The user group id (%s) in the request body does not equal the "
                    + "user group id of the requested resource (%s).", requestUserGroupDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestUserGroupEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestUserGroupEntity.isDisconnectedNodeAcknowledged());
        }

        // Extract the revision
        final Revision requestRevision = getRevision(requestUserGroupEntity, id);
        return withWriteLock(
                serviceFacade,
                requestUserGroupEntity,
                requestRevision,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, userGroupEntity) -> {
                    // update the user group
                    final UserGroupEntity entity = serviceFacade.updateUserGroup(revision, userGroupEntity.getComponent());
                    populateRemainingUserGroupEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified user group.
     *
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the user group to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("user-groups/{id}")
    @Operation(
            summary = "Deletes a user group",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = UserGroupEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /tenants")
            }
    )
    public Response removeUserGroup(
            @Parameter(
                    description = "The revision is used to verify the client is working with the latest version of the flow."
            )
            @QueryParam(VERSION) final LongParameter version,
            @Parameter(
                    description = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response."
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @Parameter(
                    description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(
                    description = "The user group id.",
                    required = true
            )
            @PathParam("id") final String id) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableUserGroupProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_USERS);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final UserGroupEntity requestUserGroupEntity = new UserGroupEntity();
        requestUserGroupEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestUserGroupEntity,
                requestRevision,
                lookup -> {
                    final Authorizable tenants = lookup.getTenant();
                    tenants.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, userGroupEntity) -> {
                    // delete the specified user group
                    final UserGroupEntity entity = serviceFacade.deleteUserGroup(revision, userGroupEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    // ------------
    // search users
    // ------------

    /**
     * Searches for a tenant with a given identity.
     *
     * @param value Search value that will be matched against a user/group identity
     * @return Tenants match the specified criteria
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("search-results")
    @Operation(
            summary = "Searches for a tenant with the specified identity",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = TenantsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /tenants")
            }
    )
    public Response searchTenants(
            @Parameter(
                    description = "Identity to search for.",
                    required = true
            )
            @QueryParam("q") @DefaultValue(StringUtils.EMPTY) String value) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_MANAGED_AUTHORIZER);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable tenants = lookup.getTenant();
            tenants.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        final TenantsEntity results = serviceFacade.searchTenants(value);
        return noCache(Response.ok(results)).build();
    }
}
