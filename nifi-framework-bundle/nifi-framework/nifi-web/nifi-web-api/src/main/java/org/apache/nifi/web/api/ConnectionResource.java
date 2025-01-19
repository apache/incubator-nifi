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
import org.apache.nifi.authorization.ConnectionAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.Set;

/**
 * RESTful endpoint for managing a Connection.
 */
@Controller
@Path("/connections")
@Tag(name = "Connections")
public class ConnectionResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populate the URIs for the specified connections.
     *
     * @param connectionEntities connections
     * @return dtos
     */
    public Set<ConnectionEntity> populateRemainingConnectionEntitiesContent(Set<ConnectionEntity> connectionEntities) {
        for (ConnectionEntity connectionEntity : connectionEntities) {
            populateRemainingConnectionEntityContent(connectionEntity);
        }
        return connectionEntities;
    }

    /**
     * Populate the URIs for the specified connection.
     *
     * @param connectionEntity connection
     * @return dto
     */
    public ConnectionEntity populateRemainingConnectionEntityContent(ConnectionEntity connectionEntity) {
        connectionEntity.setUri(generateResourceUri("connections", connectionEntity.getId()));
        return connectionEntity;
    }

    /**
     * Retrieves the specified connection.
     *
     * @param id The id of the connection.
     * @return A connectionEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    @Operation(
            summary = "Gets a connection",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConnectionEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read Source - /{component-type}/{uuid}"),
                    @SecurityRequirement(name = "Read Destination - /{component-type}/{uuid}")
            }
    )
    public Response getConnection(
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") final String id) throws InterruptedException {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            // ensure read access to this connection (checks source and destination)
            final Authorizable authorizable = lookup.getConnection(id).getAuthorizable();
            authorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the specified relationship
        ConnectionEntity entity = serviceFacade.getConnection(id);
        populateRemainingConnectionEntityContent(entity);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Updates the specified connection.
     *
     * @param id The id of the connection.
     * @param requestConnectionEntity A connectionEntity.
     * @return A connectionEntity.
     * @throws InterruptedException if interrupted
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    @Operation(
            summary = "Updates a connection",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConnectionEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write Source - /{component-type}/{uuid}"),
                    @SecurityRequirement(name = "Write Destination - /{component-type}/{uuid}"),
                    @SecurityRequirement(name = "Write New Destination - /{component-type}/{uuid} - if updating Destination"),
                    @SecurityRequirement(name = "Write Process Group - /process-groups/{uuid} - if updating Destination")
            }
    )
    public Response updateConnection(
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The connection configuration details.",
                    required = true
            ) final ConnectionEntity requestConnectionEntity) throws InterruptedException {

        if (requestConnectionEntity == null || requestConnectionEntity.getComponent() == null) {
            throw new IllegalArgumentException("Connection details must be specified.");
        }

        if (requestConnectionEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ConnectionDTO requestConnection = requestConnectionEntity.getComponent();
        if (!id.equals(requestConnection.getId())) {
            throw new IllegalArgumentException(String.format("The connection id "
                    + "(%s) in the request body does not equal the connection id of the "
                    + "requested resource (%s).", requestConnection.getId(), id));
        }

        if (requestConnection.getDestination() != null) {
            if (requestConnection.getDestination().getId() == null) {
                throw new IllegalArgumentException("When specifying a destination component, the destination id is required.");
            }

            if (requestConnection.getDestination().getType() == null) {
                throw new IllegalArgumentException("When specifying a destination component, the type of the destination is required.");
            }
        }

        final List<PositionDTO> proposedBends = requestConnection.getBends();
        if (proposedBends != null) {
            for (final PositionDTO proposedBend : proposedBends) {
                if (proposedBend.getX() == null || proposedBend.getY() == null) {
                    throw new IllegalArgumentException("The x and y coordinate of the each bend must be specified.");
                }
            }
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestConnectionEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestConnectionEntity.isDisconnectedNodeAcknowledged());
        }

        final Revision requestRevision = getRevision(requestConnectionEntity, id);
        return withWriteLock(
                serviceFacade,
                requestConnectionEntity,
                requestRevision,
                lookup -> {
                    // verifies write access to this connection (this checks the current source and destination)
                    ConnectionAuthorizable connAuth = lookup.getConnection(id);
                    connAuth.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // if a destination has been specified and is different
                    final Connectable currentDestination = connAuth.getDestination();
                    if (requestConnection.getDestination() != null && !currentDestination.getIdentifier().equals(requestConnection.getDestination().getId())) {
                        try {
                            final ConnectableType destinationConnectableType = ConnectableType.valueOf(requestConnection.getDestination().getType());

                            // explicitly handle RPGs differently as the connectable id can be ambiguous if self referencing
                            final Authorizable newDestinationAuthorizable;
                            if (ConnectableType.REMOTE_INPUT_PORT.equals(destinationConnectableType)) {
                                newDestinationAuthorizable = lookup.getRemoteProcessGroup(requestConnection.getDestination().getGroupId());
                            } else {
                                newDestinationAuthorizable = lookup.getLocalConnectable(requestConnection.getDestination().getId());
                            }

                            // verify access of the new destination (current destination was already authorized as part of the connection check)
                            newDestinationAuthorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                        } catch (final IllegalArgumentException e) {
                            throw new IllegalArgumentException(String.format("Unrecognized destination type %s. Excepted values are [%s]",
                                    requestConnection.getDestination().getType(), StringUtils.join(ConnectableType.values(), ", ")));
                        }

                        // verify access of the parent group (this is the same check that is performed when creating the connection)
                        connAuth.getParentGroup().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                    }
                },
                () -> serviceFacade.verifyUpdateConnection(requestConnection),
                (revision, connectionEntity) -> {
                    final ConnectionDTO connection = connectionEntity.getComponent();

                    final ConnectionEntity entity = serviceFacade.updateConnection(revision, connection);
                    populateRemainingConnectionEntityContent(entity);

                    // generate the response
                    return generateOkResponse(entity).build();
                });
    }

    /**
     * Removes the specified connection.
     *
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the connection.
     * @return An Entity containing the client id and an updated revision.
     * @throws InterruptedException if interrupted
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    @Operation(
            summary = "Deletes a connection",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConnectionEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write Source - /{component-type}/{uuid}"),
                    @SecurityRequirement(name = "Write - Parent Process Group - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Write Destination - /{component-type}/{uuid}")
            }
    )
    public Response deleteConnection(
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
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") final String id) throws InterruptedException {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        // determine the specified version
        final Long clientVersion = version == null ? null : version.getLong();
        final Revision requestRevision = new Revision(clientVersion, clientId.getClientId(), id);

        final ConnectionEntity requestConnectionEntity = new ConnectionEntity();
        requestConnectionEntity.setId(id);

        // get the current user
        return withWriteLock(
                serviceFacade,
                requestConnectionEntity,
                requestRevision,
                lookup -> {
                    // verifies write access to the source and destination
                    final Authorizable authorizable = lookup.getConnection(id).getAuthorizable();

                    // ensure write permission to the connection
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    authorizable.getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyDeleteConnection(id),
                (revision, connectionEntity) -> {
                    // delete the connection
                    final ConnectionEntity entity = serviceFacade.deleteConnection(revision, connectionEntity.getId());

                    // generate the response
                    return generateOkResponse(entity).build();
                }
        );
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
