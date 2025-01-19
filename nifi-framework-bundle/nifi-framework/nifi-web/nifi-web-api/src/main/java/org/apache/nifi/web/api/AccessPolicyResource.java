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
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.net.URI;

/**
 * RESTful endpoint for managing access policies.
 */
@Controller
@Path("/policies")
@Tag(name = "Policies")
public class AccessPolicyResource extends ApplicationResource {

    private final NiFiServiceFacade serviceFacade;
    private final Authorizer authorizer;

    public AccessPolicyResource(
            NiFiServiceFacade serviceFacade,
            Authorizer authorizer,
            NiFiProperties properties,
            @Autowired(required = false) final RequestReplicator requestReplicator,
            @Autowired(required = false) ClusterCoordinator clusterCoordinator,
            @Autowired(required = false) FlowController flowController
    ) {
        this.serviceFacade = serviceFacade;
        this.authorizer = authorizer;
        setProperties(properties);
        setRequestReplicator(requestReplicator);
        setClusterCoordinator(clusterCoordinator);
        setFlowController(flowController);
    }

    /**
     * Populates the uri for the specified access policy.
     *
     * @param accessPolicyEntity accessPolicyEntity
     * @return accessPolicyEntity
     */
    public AccessPolicyEntity populateRemainingAccessPolicyEntityContent(AccessPolicyEntity accessPolicyEntity) {
        accessPolicyEntity.setUri(generateResourceUri("policies", accessPolicyEntity.getId()));
        return accessPolicyEntity;
    }

    // -----------------
    // get access policy
    // -----------------

    /**
     * Retrieves the specified access policy.
     *
     * @return An accessPolicyEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{action}/{resource: .+}")
    @Operation(
            summary = "Gets an access policy for the specified action and resource",
            description = "Will return the effective policy if no component specific policy exists for the specified action and resource. "
                    + "Must have Read permissions to the policy with the desired action and resource. Permissions for the policy that is "
                    + "returned will be indicated in the response. This means the client could be authorized to get the policy for a "
                    + "given component but the effective policy may be inherited from an ancestor Process Group. If the client does not "
                    + "have permissions to that policy, the response will not include the policy and the permissions in the response "
                    + "will be marked accordingly. If the client does not have permissions to the policy of the desired action and resource "
                    + "a 403 response will be returned.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AccessPolicyEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /policies/{resource}")
            }
    )
    public Response getAccessPolicyForResource(
            @Parameter(
                    description = "The request action.",
                    required = true
            )
            @PathParam("action") final String action,
            @Parameter(
                    description = "The resource of the policy.",
                    required = true
            )
            @PathParam("resource") String rawResource) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isManagedAuthorizer(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_MANAGED_AUTHORIZER);
        }

        // parse the action and resource type
        final RequestAction requestAction = RequestAction.valueOfValue(action);
        final String resource = "/" + rawResource;

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable accessPolicy = lookup.getAccessPolicyByResource(resource);
            accessPolicy.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the access policy
        final AccessPolicyEntity entity = serviceFacade.getAccessPolicy(requestAction, resource);
        populateRemainingAccessPolicyEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    // -----------------------
    // manage an access policy
    // -----------------------

    /**
     * Creates a new access policy.
     *
     * @param requestAccessPolicyEntity An accessPolicyEntity.
     * @return An accessPolicyEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Creates an access policy",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = AccessPolicyEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /policies/{resource}")
            }
    )
    public Response createAccessPolicy(
            @Parameter(
                    description = "The access policy configuration details.",
                    required = true
            ) final AccessPolicyEntity requestAccessPolicyEntity) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableAccessPolicyProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_POLICIES);
        }

        if (requestAccessPolicyEntity == null || requestAccessPolicyEntity.getComponent() == null) {
            throw new IllegalArgumentException("Access policy details must be specified.");
        }

        if (requestAccessPolicyEntity.getRevision() == null || (requestAccessPolicyEntity.getRevision().getVersion() == null || requestAccessPolicyEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Policy.");
        }

        final AccessPolicyDTO requestAccessPolicy = requestAccessPolicyEntity.getComponent();
        if (requestAccessPolicy.getId() != null) {
            throw new IllegalArgumentException("Access policy ID cannot be specified.");
        }

        if (requestAccessPolicy.getResource() == null) {
            throw new IllegalArgumentException("Access policy resource must be specified.");
        }

        // ensure this is a valid action
        RequestAction.valueOfValue(requestAccessPolicy.getAction());

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestAccessPolicyEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestAccessPolicyEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        return withWriteLock(
                serviceFacade,
                requestAccessPolicyEntity,
                lookup -> {
                    final Authorizable accessPolicies = lookup.getAccessPolicyByResource(requestAccessPolicy.getResource());
                    accessPolicies.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                accessPolicyEntity -> {
                    final AccessPolicyDTO accessPolicy = accessPolicyEntity.getComponent();

                    // set the access policy id as appropriate
                    accessPolicy.setId(generateUuid());

                    // get revision from the config
                    final RevisionDTO revisionDTO = accessPolicyEntity.getRevision();
                    Revision revision = new Revision(revisionDTO.getVersion(), revisionDTO.getClientId(), accessPolicyEntity.getComponent().getId());

                    // create the access policy and generate the json
                    final AccessPolicyEntity entity = serviceFacade.createAccessPolicy(revision, accessPolicyEntity.getComponent());
                    populateRemainingAccessPolicyEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Retrieves the specified access policy.
     *
     * @param id The id of the access policy to retrieve
     * @return An accessPolicyEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Gets an access policy",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AccessPolicyEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /policies/{resource}")
            }
    )
    public Response getAccessPolicy(
            @Parameter(
                    description = "The access policy id.",
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
            Authorizable authorizable = lookup.getAccessPolicyById(id);
            authorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the access policy
        final AccessPolicyEntity entity = serviceFacade.getAccessPolicy(id);
        populateRemainingAccessPolicyEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Updates an access policy.
     *
     * @param id The id of the access policy to update.
     * @param requestAccessPolicyEntity An accessPolicyEntity.
     * @return An accessPolicyEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Updates a access policy",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AccessPolicyEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /policies/{resource}")
            }
    )
    public Response updateAccessPolicy(
            @Parameter(
                    description = "The access policy id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The access policy configuration details.",
                    required = true
            ) final AccessPolicyEntity requestAccessPolicyEntity) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableAccessPolicyProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_POLICIES);
        }

        if (requestAccessPolicyEntity == null || requestAccessPolicyEntity.getComponent() == null) {
            throw new IllegalArgumentException("Access policy details must be specified.");
        }

        if (requestAccessPolicyEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final AccessPolicyDTO requestAccessPolicyDTO = requestAccessPolicyEntity.getComponent();
        if (!id.equals(requestAccessPolicyDTO.getId())) {
            throw new IllegalArgumentException(String.format("The access policy id (%s) in the request body does not equal the "
                    + "access policy id of the requested resource (%s).", requestAccessPolicyDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestAccessPolicyEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestAccessPolicyEntity.isDisconnectedNodeAcknowledged());
        }

        // Extract the revision
        final Revision requestRevision = getRevision(requestAccessPolicyEntity, id);
        return withWriteLock(
                serviceFacade,
                requestAccessPolicyEntity,
                requestRevision,
                lookup -> {
                    Authorizable authorizable = lookup.getAccessPolicyById(id);
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, accessPolicyEntity) -> {
                    final AccessPolicyDTO accessPolicyDTO = accessPolicyEntity.getComponent();

                    // update the access policy
                    final AccessPolicyEntity entity = serviceFacade.updateAccessPolicy(revision, accessPolicyDTO);
                    populateRemainingAccessPolicyEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified access policy.
     *
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the access policy to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Deletes an access policy",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AccessPolicyEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /policies/{resource}"),
                    @SecurityRequirement(name = "Write - Policy of the parent resource - /policies/{resource}")
            }
    )
    public Response removeAccessPolicy(
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
                    description = "The access policy id.",
                    required = true
            )
            @PathParam("id") final String id) {

        // ensure we're running with a configurable authorizer
        if (!AuthorizerCapabilityDetection.isConfigurableAccessPolicyProvider(authorizer)) {
            throw new IllegalStateException(AccessPolicyDAO.MSG_NON_CONFIGURABLE_POLICIES);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final AccessPolicyEntity requestAccessPolicyEntity = new AccessPolicyEntity();
        requestAccessPolicyEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestAccessPolicyEntity,
                requestRevision,
                lookup -> {
                    final Authorizable accessPolicy = lookup.getAccessPolicyById(id);

                    // ensure write permission to the access policy
                    accessPolicy.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the policy for the parent process group
                    accessPolicy.getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, accessPolicyEntity) -> {
                    // delete the specified access policy
                    final AccessPolicyEntity entity = serviceFacade.deleteAccessPolicy(revision, accessPolicyEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }
}
