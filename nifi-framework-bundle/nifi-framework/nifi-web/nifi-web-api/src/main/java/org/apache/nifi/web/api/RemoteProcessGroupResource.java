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
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ProcessGroupAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.RemotePortRunStatusEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupPortEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupsEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.net.URI;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * RESTful endpoint for managing a Remote group.
 */
@Controller
@Path("/remote-process-groups")
@Tag(name = "RemoteProcessGroups")
public class RemoteProcessGroupResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populates the remaining content for each remote process group. The uri must be generated and the remote process groups name must be retrieved.
     *
     * @param remoteProcessGroupEntities groups
     * @return dtos
     */
    public Set<RemoteProcessGroupEntity> populateRemainingRemoteProcessGroupEntitiesContent(Set<RemoteProcessGroupEntity> remoteProcessGroupEntities) {
        for (RemoteProcessGroupEntity remoteProcessEntities : remoteProcessGroupEntities) {
            populateRemainingRemoteProcessGroupEntityContent(remoteProcessEntities);
        }
        return remoteProcessGroupEntities;
    }

    /**
     * Populates the remaining content for each remote process group. The uri must be generated and the remote process groups name must be retrieved.
     *
     * @param remoteProcessGroupEntity groups
     * @return dtos
     */
    public RemoteProcessGroupEntity populateRemainingRemoteProcessGroupEntityContent(RemoteProcessGroupEntity remoteProcessGroupEntity) {
        remoteProcessGroupEntity.setUri(generateResourceUri("remote-process-groups", remoteProcessGroupEntity.getId()));
        return remoteProcessGroupEntity;
    }

    /**
     * Retrieves the specified remote process group.
     *
     * @param id The id of the remote process group to retrieve
     * @return A remoteProcessGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Gets a remote process group",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RemoteProcessGroupEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /remote-process-groups/{uuid}")
            }
    )
    public Response getRemoteProcessGroup(
            @Parameter(
                    description = "The remote process group id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable remoteProcessGroup = lookup.getRemoteProcessGroup(id);
            remoteProcessGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the remote process group
        final RemoteProcessGroupEntity entity = serviceFacade.getRemoteProcessGroup(id);
        populateRemainingRemoteProcessGroupEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Removes the specified remote process group.
     *
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the remote process group to be removed.
     * @return A remoteProcessGroupEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Deletes a remote process group",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RemoteProcessGroupEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /remote-process-groups/{uuid}"),
                    @SecurityRequirement(name = "Write - Parent Process Group - /process-groups/{uuid}")
            }
    )
    public Response removeRemoteProcessGroup(
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
                    description = "The remote process group id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final RemoteProcessGroupEntity requestRemoteProcessGroupEntity = new RemoteProcessGroupEntity();
        requestRemoteProcessGroupEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestRemoteProcessGroupEntity,
                requestRevision,
                lookup -> {
                    final Authorizable remoteProcessGroup = lookup.getRemoteProcessGroup(id);

                    // ensure write permission to the remote process group
                    remoteProcessGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    remoteProcessGroup.getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyDeleteRemoteProcessGroup(id),
                (revision, remoteProcessGroupEntity) -> {
                    final RemoteProcessGroupEntity entity = serviceFacade.deleteRemoteProcessGroup(revision, remoteProcessGroupEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified remote process group input port.
     *
     * @param id The id of the remote process group to update.
     * @param portId The id of the input port to update.
     * @param requestRemoteProcessGroupPortEntity The remoteProcessGroupPortEntity
     * @return A remoteProcessGroupPortEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/input-ports/{port-id}")
    @Operation(
            summary = "Updates a remote port",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RemoteProcessGroupPortEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /remote-process-groups/{uuid}")
            }
    )
    public Response updateRemoteProcessGroupInputPort(
            @Parameter(
                    description = "The remote process group id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The remote process group port id.",
                    required = true
            )
            @PathParam("port-id") final String portId,
            @Parameter(
                    description = "The remote process group port.",
                    required = true
            ) final RemoteProcessGroupPortEntity requestRemoteProcessGroupPortEntity) {

        if (requestRemoteProcessGroupPortEntity == null || requestRemoteProcessGroupPortEntity.getRemoteProcessGroupPort() == null) {
            throw new IllegalArgumentException("Remote process group port details must be specified.");
        }

        if (requestRemoteProcessGroupPortEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final RemoteProcessGroupPortDTO requestRemoteProcessGroupPort = requestRemoteProcessGroupPortEntity.getRemoteProcessGroupPort();
        if (!portId.equals(requestRemoteProcessGroupPort.getId())) {
            throw new IllegalArgumentException(String.format("The remote process group port id (%s) in the request body does not equal the "
                    + "remote process group port id of the requested resource (%s).", requestRemoteProcessGroupPort.getId(), portId));
        }

        // ensure the group ids are the same
        if (!id.equals(requestRemoteProcessGroupPort.getGroupId())) {
            throw new IllegalArgumentException(String.format("The remote process group id (%s) in the request body does not equal the "
                    + "remote process group id of the requested resource (%s).", requestRemoteProcessGroupPort.getGroupId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRemoteProcessGroupPortEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRemoteProcessGroupPortEntity.isDisconnectedNodeAcknowledged());
        }

        final Revision requestRevision = getRevision(requestRemoteProcessGroupPortEntity, id);
        return withWriteLock(
                serviceFacade,
                requestRemoteProcessGroupPortEntity,
                requestRevision,
                lookup -> {
                    final Authorizable remoteProcessGroup = lookup.getRemoteProcessGroup(id);
                    remoteProcessGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateRemoteProcessGroupInputPort(id, requestRemoteProcessGroupPort),
                (revision, remoteProcessGroupPortEntity) -> {
                    final RemoteProcessGroupPortDTO remoteProcessGroupPort = remoteProcessGroupPortEntity.getRemoteProcessGroupPort();

                    // update the specified remote process group
                    final RemoteProcessGroupPortEntity controllerResponse = serviceFacade.updateRemoteProcessGroupInputPort(revision, id, remoteProcessGroupPort);

                    // get the updated revision
                    final RevisionDTO updatedRevision = controllerResponse.getRevision();

                    // build the response entity
                    final RemoteProcessGroupPortEntity entity = new RemoteProcessGroupPortEntity();
                    entity.setRevision(updatedRevision);
                    entity.setRemoteProcessGroupPort(controllerResponse.getRemoteProcessGroupPort());

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified remote process group output port.
     *
     * @param id The id of the remote process group to update.
     * @param portId The id of the output port to update.
     * @param requestRemoteProcessGroupPortEntity The remoteProcessGroupPortEntity
     * @return A remoteProcessGroupPortEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/output-ports/{port-id}")
    @Operation(
            summary = "Updates a remote port",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RemoteProcessGroupPortEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /remote-process-groups/{uuid}")
            }
    )
    public Response updateRemoteProcessGroupOutputPort(
            @Parameter(
                    description = "The remote process group id.",
                    required = true
            )
            @PathParam("id") String id,
            @Parameter(
                    description = "The remote process group port id.",
                    required = true
            )
            @PathParam("port-id") String portId,
            @Parameter(
                    description = "The remote process group port.",
                    required = true
            ) RemoteProcessGroupPortEntity requestRemoteProcessGroupPortEntity) {

        if (requestRemoteProcessGroupPortEntity == null || requestRemoteProcessGroupPortEntity.getRemoteProcessGroupPort() == null) {
            throw new IllegalArgumentException("Remote process group port details must be specified.");
        }

        if (requestRemoteProcessGroupPortEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final RemoteProcessGroupPortDTO requestRemoteProcessGroupPort = requestRemoteProcessGroupPortEntity.getRemoteProcessGroupPort();
        if (!portId.equals(requestRemoteProcessGroupPort.getId())) {
            throw new IllegalArgumentException(String.format("The remote process group port id (%s) in the request body does not equal the "
                    + "remote process group port id of the requested resource (%s).", requestRemoteProcessGroupPort.getId(), portId));
        }

        // ensure the group ids are the same
        if (!id.equals(requestRemoteProcessGroupPort.getGroupId())) {
            throw new IllegalArgumentException(String.format("The remote process group id (%s) in the request body does not equal the "
                    + "remote process group id of the requested resource (%s).", requestRemoteProcessGroupPort.getGroupId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRemoteProcessGroupPortEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRemoteProcessGroupPortEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestRemoteProcessGroupPortEntity, id);
        return withWriteLock(
                serviceFacade,
                requestRemoteProcessGroupPortEntity,
                requestRevision,
                lookup -> {
                    final Authorizable remoteProcessGroup = lookup.getRemoteProcessGroup(id);
                    remoteProcessGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateRemoteProcessGroupOutputPort(id, requestRemoteProcessGroupPort),
                (revision, remoteProcessGroupPortEntity) -> {
                    final RemoteProcessGroupPortDTO remoteProcessGroupPort = remoteProcessGroupPortEntity.getRemoteProcessGroupPort();

                    // update the specified remote process group
                    final RemoteProcessGroupPortEntity controllerResponse = serviceFacade.updateRemoteProcessGroupOutputPort(revision, id, remoteProcessGroupPort);

                    // get the updated revision
                    final RevisionDTO updatedRevision = controllerResponse.getRevision();

                    // build the response entity
                    RemoteProcessGroupPortEntity entity = new RemoteProcessGroupPortEntity();
                    entity.setRevision(updatedRevision);
                    entity.setRemoteProcessGroupPort(controllerResponse.getRemoteProcessGroupPort());

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified remote process group input port run status.
     *
     * @param id The id of the remote process group to update.
     * @param portId The id of the input port to update.
     * @param requestRemotePortRunStatusEntity The remoteProcessGroupPortRunStatusEntity
     * @return A remoteProcessGroupPortEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/input-ports/{port-id}/run-status")
    @Operation(
            summary = "Updates run status of a remote port",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RemoteProcessGroupPortEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /remote-process-groups/{uuid} or /operation/remote-process-groups/{uuid}")
            }
    )
    public Response updateRemoteProcessGroupInputPortRunStatus(
            @Parameter(
                    description = "The remote process group id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The remote process group port id.",
                    required = true
            )
            @PathParam("port-id") final String portId,
            @Parameter(
                    description = "The remote process group port.",
                    required = true
            ) final RemotePortRunStatusEntity requestRemotePortRunStatusEntity) {

        if (requestRemotePortRunStatusEntity == null) {
            throw new IllegalArgumentException("Remote process group port run status must be specified.");
        }

        if (requestRemotePortRunStatusEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        requestRemotePortRunStatusEntity.validateState();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRemotePortRunStatusEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRemotePortRunStatusEntity.isDisconnectedNodeAcknowledged());
        }

        final Revision requestRevision = getRevision(requestRemotePortRunStatusEntity.getRevision(), id);
        return withWriteLock(
                serviceFacade,
                requestRemotePortRunStatusEntity,
                requestRevision,
                lookup -> {
                    final Authorizable remoteProcessGroup = lookup.getRemoteProcessGroup(id);
                    OperationAuthorizable.authorizeOperation(remoteProcessGroup, authorizer, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateRemoteProcessGroupInputPort(id, createPortDTOWithDesiredRunStatus(portId, id, requestRemotePortRunStatusEntity)),
                (revision, remotePortRunStatusEntity) -> {
                    // update the specified remote process group
                    final RemoteProcessGroupPortEntity controllerResponse = serviceFacade.updateRemoteProcessGroupInputPort(revision, id,
                            createPortDTOWithDesiredRunStatus(portId, id, remotePortRunStatusEntity));

                    // get the updated revision
                    final RevisionDTO updatedRevision = controllerResponse.getRevision();

                    // build the response entity
                    final RemoteProcessGroupPortEntity entity = new RemoteProcessGroupPortEntity();
                    entity.setRevision(updatedRevision);
                    entity.setRemoteProcessGroupPort(controllerResponse.getRemoteProcessGroupPort());

                    return generateOkResponse(entity).build();
                }
        );
    }

    private RemoteProcessGroupPortDTO createPortDTOWithDesiredRunStatus(final String portId, final String groupId, final RemotePortRunStatusEntity entity) {
        final RemoteProcessGroupPortDTO dto = new RemoteProcessGroupPortDTO();
        dto.setId(portId);
        dto.setGroupId(groupId);
        dto.setTransmitting(shouldTransmit(entity));
        return dto;
    }

    /**
     * Updates the specified remote process group output port run status.
     *
     * @param id The id of the remote process group to update.
     * @param portId The id of the output port to update.
     * @param requestRemotePortRunStatusEntity The remoteProcessGroupPortEntity
     * @return A remoteProcessGroupPortEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/output-ports/{port-id}/run-status")
    @Operation(
            summary = "Updates run status of a remote port",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RemoteProcessGroupPortEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /remote-process-groups/{uuid} or /operation/remote-process-groups/{uuid}")
            }
    )
    public Response updateRemoteProcessGroupOutputPortRunStatus(
            @Parameter(
                    description = "The remote process group id.",
                    required = true
            )
            @PathParam("id") String id,
            @Parameter(
                    description = "The remote process group port id.",
                    required = true
            )
            @PathParam("port-id") String portId,
            @Parameter(
                    description = "The remote process group port.",
                    required = true
            ) RemotePortRunStatusEntity requestRemotePortRunStatusEntity) {

        if (requestRemotePortRunStatusEntity == null) {
            throw new IllegalArgumentException("Remote process group port run status must be specified.");
        }

        if (requestRemotePortRunStatusEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        requestRemotePortRunStatusEntity.validateState();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRemotePortRunStatusEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRemotePortRunStatusEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestRemotePortRunStatusEntity.getRevision(), id);
        return withWriteLock(
                serviceFacade,
                requestRemotePortRunStatusEntity,
                requestRevision,
                lookup -> {
                    final Authorizable remoteProcessGroup = lookup.getRemoteProcessGroup(id);
                    OperationAuthorizable.authorizeOperation(remoteProcessGroup, authorizer, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateRemoteProcessGroupOutputPort(id, createPortDTOWithDesiredRunStatus(portId, id, requestRemotePortRunStatusEntity)),
                (revision, remotePortRunStatusEntity) -> {
                    // update the specified remote process group
                    final RemoteProcessGroupPortEntity controllerResponse = serviceFacade.updateRemoteProcessGroupOutputPort(revision, id,
                            createPortDTOWithDesiredRunStatus(portId, id, remotePortRunStatusEntity));

                    // get the updated revision
                    final RevisionDTO updatedRevision = controllerResponse.getRevision();

                    // build the response entity
                    RemoteProcessGroupPortEntity entity = new RemoteProcessGroupPortEntity();
                    entity.setRevision(updatedRevision);
                    entity.setRemoteProcessGroupPort(controllerResponse.getRemoteProcessGroupPort());

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified remote process group.
     *
     * @param id The id of the remote process group to update.
     * @param requestRemoteProcessGroupEntity A remoteProcessGroupEntity.
     * @return A remoteProcessGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Updates a remote process group",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RemoteProcessGroupEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /remote-process-groups/{uuid}")
            }
    )
    public Response updateRemoteProcessGroup(
            @Parameter(
                    description = "The remote process group id.",
                    required = true
            )
            @PathParam("id") String id,
            @Parameter(
                    description = "The remote process group.",
                    required = true
            ) final RemoteProcessGroupEntity requestRemoteProcessGroupEntity) {

        if (requestRemoteProcessGroupEntity == null || requestRemoteProcessGroupEntity.getComponent() == null) {
            throw new IllegalArgumentException("Remote process group details must be specified.");
        }

        if (requestRemoteProcessGroupEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final RemoteProcessGroupDTO requestRemoteProcessGroup = requestRemoteProcessGroupEntity.getComponent();
        if (!id.equals(requestRemoteProcessGroup.getId())) {
            throw new IllegalArgumentException(String.format("The remote process group id (%s) in the request body does not equal the "
                    + "remote process group id of the requested resource (%s).", requestRemoteProcessGroup.getId(), id));
        }

        final PositionDTO proposedPosition = requestRemoteProcessGroup.getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRemoteProcessGroupEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRemoteProcessGroupEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestRemoteProcessGroupEntity, id);
        return withWriteLock(
                serviceFacade,
                requestRemoteProcessGroupEntity,
                requestRevision,
                lookup -> {
                    Authorizable authorizable = lookup.getRemoteProcessGroup(id);
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateRemoteProcessGroup(requestRemoteProcessGroup),
                (revision, remoteProcessGroupEntity) -> {
                    final RemoteProcessGroupDTO remoteProcessGroup = remoteProcessGroupEntity.getComponent();

                    // if the target uri is set we have to verify it here - we don't support updating the target uri on
                    // an existing remote process group, however if the remote process group is being created with an id
                    // as is the case in clustered mode we need to verify the remote process group. treat this request as
                    // though its a new remote process group.
                    if (remoteProcessGroup.getTargetUri() != null) {
                        // parse the uri
                        final URI uri;
                        try {
                            uri = URI.create(remoteProcessGroup.getTargetUri());
                        } catch (final IllegalArgumentException e) {
                            throw new IllegalArgumentException("The specified remote process group URL is malformed: " + remoteProcessGroup.getTargetUri());
                        }

                        // validate each part of the uri
                        if (uri.getScheme() == null || uri.getHost() == null) {
                            throw new IllegalArgumentException("The specified remote process group URL is malformed: " + remoteProcessGroup.getTargetUri());
                        }

                        if (!(uri.getScheme().equalsIgnoreCase("http") || uri.getScheme().equalsIgnoreCase("https"))) {
                            throw new IllegalArgumentException("The specified remote process group URL is invalid because it is not http or https: " + remoteProcessGroup.getTargetUri());
                        }

                        // normalize the uri to the other controller
                        String controllerUri = uri.toString();
                        if (controllerUri.endsWith("/")) {
                            controllerUri = StringUtils.substringBeforeLast(controllerUri, "/");
                        }

                        // update with the normalized uri
                        remoteProcessGroup.setTargetUri(controllerUri);
                    }

                    // update the specified remote process group
                    final RemoteProcessGroupEntity entity = serviceFacade.updateRemoteProcessGroup(revision, remoteProcessGroup);
                    populateRemainingRemoteProcessGroupEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the operational status for the specified remote process group with the specified value.
     *
     * @param id The id of the remote process group to update.
     * @param requestRemotePortRunStatusEntity A remotePortRunStatusEntity.
     * @return A remoteProcessGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/run-status")
    @Operation(
            summary = "Updates run status of a remote process group",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RemoteProcessGroupEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /remote-process-groups/{uuid} or /operation/remote-process-groups/{uuid}")
            }
    )
    public Response updateRemoteProcessGroupRunStatus(
            @Parameter(
                    description = "The remote process group id.",
                    required = true
            )
            @PathParam("id") String id,
            @Parameter(
                    description = "The remote process group run status.",
                    required = true
            ) final RemotePortRunStatusEntity requestRemotePortRunStatusEntity) {

        if (requestRemotePortRunStatusEntity == null) {
            throw new IllegalArgumentException("Remote process group run status must be specified.");
        }

        if (requestRemotePortRunStatusEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        requestRemotePortRunStatusEntity.validateState();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRemotePortRunStatusEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRemotePortRunStatusEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestRemotePortRunStatusEntity.getRevision(), id);
        return withWriteLock(
                serviceFacade,
                requestRemotePortRunStatusEntity,
                requestRevision,
                lookup -> {
                    Authorizable authorizable = lookup.getRemoteProcessGroup(id);
                    OperationAuthorizable.authorizeOperation(authorizable, authorizer, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateRemoteProcessGroup(createDTOWithDesiredRunStatus(id, requestRemotePortRunStatusEntity)),
                (revision, remotePortRunStatusEntity) -> {
                    // update the specified remote process group
                    final RemoteProcessGroupEntity entity = serviceFacade.updateRemoteProcessGroup(revision, createDTOWithDesiredRunStatus(id, remotePortRunStatusEntity));
                    populateRemainingRemoteProcessGroupEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the operational status for all remote process groups in the specified process group with the specified value.
     *
     * @param processGroupId The id of the process group in which all remote process groups to update.
     * @param requestRemotePortRunStatusEntity A remotePortRunStatusEntity that holds the desired run status
     * @return A response with an array of RemoteProcessGroupEntity objects.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-group/{id}/run-status")
    @Operation(
            summary = "Updates run status of all remote process groups in a process group (recursively)",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RemoteProcessGroupEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /remote-process-groups/{uuid} or /operation/remote-process-groups/{uuid}")
            }
    )
    public Response updateRemoteProcessGroupRunStatuses(
            @Parameter(
                    description = "The process group id.",
                    required = true
            )
            @PathParam("id") String processGroupId,
            @Parameter(
                    description = "The remote process groups run status.",
                    required = true
            ) final RemotePortRunStatusEntity requestRemotePortRunStatusEntity
    ) {
        if (requestRemotePortRunStatusEntity == null) {
            throw new IllegalArgumentException("Remote process group run status must be specified.");
        }

        requestRemotePortRunStatusEntity.validateState();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRemotePortRunStatusEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRemotePortRunStatusEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Set<Revision> revisions = serviceFacade.getRevisionsFromGroup(
                processGroupId,
                group -> group.findAllRemoteProcessGroups().stream()
                        .filter(remoteProcessGroup ->
                                requestRemotePortRunStatusEntity.getState().equals("TRANSMITTING") && !remoteProcessGroup.isTransmitting()
                                        || requestRemotePortRunStatusEntity.getState().equals("STOPPED") && remoteProcessGroup.isTransmitting()
                        )
                        .filter(remoteProcessGroup -> OperationAuthorizable.isOperationAuthorized(remoteProcessGroup, authorizer, NiFiUserUtils.getNiFiUser()))
                        .map(RemoteProcessGroup::getIdentifier)
                        .collect(Collectors.toSet())
        );
        return withWriteLock(
                serviceFacade,
                requestRemotePortRunStatusEntity,
                revisions,
                lookup -> {
                    final ProcessGroupAuthorizable processGroup = lookup.getProcessGroup(processGroupId);

                    authorizeProcessGroup(processGroup, authorizer, lookup, RequestAction.READ, false, false, false, false, false);

                    Set<Authorizable> remoteProcessGroups = processGroup.getEncapsulatedRemoteProcessGroups();
                    for (Authorizable remoteProcessGroup : remoteProcessGroups) {
                        OperationAuthorizable.authorizeOperation(remoteProcessGroup, authorizer, NiFiUserUtils.getNiFiUser());
                    }
                },
                () -> serviceFacade.verifyUpdateRemoteProcessGroups(processGroupId, shouldTransmit(requestRemotePortRunStatusEntity)),
                (_revisions, remotePortRunStatusEntity) -> {
                    _revisions.forEach(revision -> {
                                final RemoteProcessGroupEntity entity =
                                        serviceFacade.updateRemoteProcessGroup(revision, createDTOWithDesiredRunStatus(revision.getComponentId(), remotePortRunStatusEntity));
                                populateRemainingRemoteProcessGroupEntityContent(entity);
                            });

                    RemoteProcessGroupsEntity remoteProcessGroupsEntity = new RemoteProcessGroupsEntity();

                    Response response = generateOkResponse(remoteProcessGroupsEntity).build();

                    return response;
                }
        );
    }

    /**
     * Gets the state for a RemoteProcessGroup.
     *
     * @param id The id of the RemoteProcessGroup
     * @return a componentStateEntity
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/state")
    @Operation(
            summary = "Gets the state for a RemoteProcessGroup",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ComponentStateEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /remote-process-groups/{uuid}")
            }
    )
    public Response getState(
            @Parameter(
                    description = "The processor id.",
                    required = true
            )
            @PathParam("id") final String id) throws InterruptedException {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable authorizable = lookup.getRemoteProcessGroup(id);
            authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        // get the component state
        final ComponentStateDTO state = serviceFacade.getRemoteProcessGroupState(id);

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setComponentState(state);

        // generate the response
        return generateOkResponse(entity).build();
    }

    private RemoteProcessGroupDTO createDTOWithDesiredRunStatus(final String id, final RemotePortRunStatusEntity entity) {
        final RemoteProcessGroupDTO dto = new RemoteProcessGroupDTO();
        dto.setId(id);
        dto.setTransmitting(shouldTransmit(entity));
        return dto;
    }


    private boolean shouldTransmit(RemotePortRunStatusEntity requestRemotePortRunStatusEntity) {
        return "TRANSMITTING".equals(requestRemotePortRunStatusEntity.getState());
    }

    @Autowired
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Autowired
    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
