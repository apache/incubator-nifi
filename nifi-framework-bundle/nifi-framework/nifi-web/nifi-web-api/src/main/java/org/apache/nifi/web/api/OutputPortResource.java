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
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.PortRunStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.util.Set;

/**
 * RESTful endpoint for managing an Output Port.
 */
@Controller
@Path("/output-ports")
@Tag(name = "OutputPorts")
public class OutputPortResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populates the uri for the specified output ports.
     *
     * @param outputPortEntities ports
     * @return dtos
     */
    public Set<PortEntity> populateRemainingOutputPortEntitiesContent(Set<PortEntity> outputPortEntities) {
        for (PortEntity outputPortEntity : outputPortEntities) {
            populateRemainingOutputPortEntityContent(outputPortEntity);
        }
        return outputPortEntities;
    }

    /**
     * Populates the uri for the specified output ports.
     *
     * @param outputPortEntity ports
     * @return dtos
     */
    public PortEntity populateRemainingOutputPortEntityContent(PortEntity outputPortEntity) {
        outputPortEntity.setUri(generateResourceUri("output-ports", outputPortEntity.getId()));
        return outputPortEntity;
    }

    /**
     * Retrieves the specified output port.
     *
     * @param id The id of the output port to retrieve
     * @return A outputPortEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Gets an output port",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PortEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /output-ports/{uuid}")
            }
    )
    public Response getOutputPort(
            @Parameter(
                    description = "The output port id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable outputPort = lookup.getOutputPort(id);
            outputPort.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the port
        final PortEntity entity = serviceFacade.getOutputPort(id);
        populateRemainingOutputPortEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Updates the specified output port.
     *
     * @param id The id of the output port to update.
     * @param requestPortEntity A outputPortEntity.
     * @return A outputPortEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Updates an output port",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PortEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /output-ports/{uuid}")
            }
    )
    public Response updateOutputPort(
            @Parameter(
                    description = "The output port id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The output port configuration details.",
                    required = true
            ) final PortEntity requestPortEntity) {

        if (requestPortEntity == null || requestPortEntity.getComponent() == null) {
            throw new IllegalArgumentException("Output port details must be specified.");
        }

        if (requestPortEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        PortDTO requestPortDTO = requestPortEntity.getComponent();
        if (!id.equals(requestPortDTO.getId())) {
            throw new IllegalArgumentException(String.format("The output port id (%s) in the request body does not equal the "
                    + "output port id of the requested resource (%s).", requestPortDTO.getId(), id));
        }

        final PositionDTO proposedPosition = requestPortDTO.getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestPortEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestPortEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestPortEntity, id);
        return withWriteLock(
                serviceFacade,
                requestPortEntity,
                requestRevision,
                lookup -> {
                    Authorizable authorizable = lookup.getOutputPort(id);
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateOutputPort(requestPortDTO),
                (revision, portEntity) -> {
                    final PortDTO portDTO = portEntity.getComponent();

                    // update the output port
                    final PortEntity entity = serviceFacade.updateOutputPort(revision, portDTO);
                    populateRemainingOutputPortEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified output port.
     *
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the output port to remove.
     * @return A outputPortEntity.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Deletes an output port",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PortEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /output-ports/{uuid}"),
                    @SecurityRequirement(name = "Write - Parent Process Group - /process-groups/{uuid}")
            }
    )
    public Response removeOutputPort(
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
                    description = "The output port id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final PortEntity requestPortEntity = new PortEntity();
        requestPortEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestPortEntity,
                requestRevision,
                lookup -> {
                    final Authorizable outputPort = lookup.getOutputPort(id);

                    // ensure write permission to the output port
                    outputPort.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    outputPort.getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyDeleteOutputPort(id),
                (revision, portEntity) -> {
                    // delete the specified output port
                    final PortEntity entity = serviceFacade.deleteOutputPort(revision, portEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }


    /**
     * Updates the operational status for the specified input port with the specified values.
     *
     * @param id The id of the port to update.
     * @param requestRunStatus A portRunStatusEntity.
     * @return A portEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/run-status")
    @Operation(
            summary = "Updates run status of an output-port",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /output-ports/{uuid} or /operation/output-ports/{uuid}")
            }
    )
    public Response updateRunStatus(
            @Parameter(
                    description = "The port id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The port run status.",
                    required = true
            ) final PortRunStatusEntity requestRunStatus) {

        if (requestRunStatus == null) {
            throw new IllegalArgumentException("Port run status must be specified.");
        }

        if (requestRunStatus.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        requestRunStatus.validateState();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestRunStatus);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestRunStatus.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestRunStatus.getRevision(), id);
        return withWriteLock(
                serviceFacade,
                requestRunStatus,
                requestRevision,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();

                    final Authorizable authorizable = lookup.getOutputPort(id);
                    OperationAuthorizable.authorizeOperation(authorizable, authorizer, user);
                },
                () -> serviceFacade.verifyUpdateOutputPort(createDTOWithDesiredRunStatus(id, requestRunStatus.getState())),
                (revision, runStatusEntity) -> {
                    // update the output port
                    final PortEntity entity = serviceFacade.updateOutputPort(revision, createDTOWithDesiredRunStatus(id, runStatusEntity.getState()));
                    populateRemainingOutputPortEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    private PortDTO createDTOWithDesiredRunStatus(final String id, final String runStatus) {
        final PortDTO dto = new PortDTO();
        dto.setId(id);
        dto.setState(runStatus);
        return dto;
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
