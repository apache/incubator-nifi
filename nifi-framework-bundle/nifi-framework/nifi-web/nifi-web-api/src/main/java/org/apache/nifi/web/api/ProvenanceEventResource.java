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
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicationHeader;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.entity.LatestProvenanceEventsEntity;
import org.apache.nifi.web.api.entity.ProvenanceEventEntity;
import org.apache.nifi.web.api.entity.ReplayLastEventRequestEntity;
import org.apache.nifi.web.api.entity.ReplayLastEventResponseEntity;
import org.apache.nifi.web.api.entity.ReplayLastEventSnapshotDTO;
import org.apache.nifi.web.api.entity.SubmitReplayRequestEntity;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.api.streaming.StreamingOutputResponseBuilder;
import org.apache.nifi.web.util.ResponseBuilderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.net.URI;
import java.util.Collections;

/**
 * RESTful endpoint for querying data provenance.
 */
@Controller
@Path("/provenance-events")
@Tag(name = "ProvenanceEvents")
public class ProvenanceEventResource extends ApplicationResource {
    private static final Logger logger = LoggerFactory.getLogger(ProvenanceEventResource.class);

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Gets the content for the input of the specified event.
     *
     * @param clusterNodeId The id of the node within the cluster this content is on. Required if clustered.
     * @param id The id of the provenance event associated with this content.
     * @return The content stream
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("{id}/content/input")
    @Operation(
            summary = "Gets the input content for a provenance event",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = StreamingOutput.class))),
                    @ApiResponse(responseCode = "206", description = "Partial Content with range of bytes requested"),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "416", description = "Requested Range Not Satisfiable based on bytes requested")
            },
            security = {
                    @SecurityRequirement(name = "Read Component Provenance Data - /provenance-data/{component-type}/{uuid}"),
                    @SecurityRequirement(name = "Read Component Data - /data/{component-type}/{uuid}")
            }
    )
    public Response getInputContent(
            @Parameter(
                    description = "Range of bytes requested"
            )
            @HeaderParam("Range") final String rangeHeader,
            @Parameter(
                    description = "The id of the node where the content exists if clustered."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @Parameter(
                    description = "The provenance event id.",
                    required = true
            )
            @PathParam("id") final LongParameter id) {

        // ensure proper input
        if (id == null) {
            throw new IllegalArgumentException("The event id must be specified.");
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the uri of the request
        final String uri = generateResourceUri("provenance", "events", String.valueOf(id.getLong()), "content", "input");

        final DownloadableContent content = serviceFacade.getContent(id.getLong(), uri, ContentDirection.INPUT);
        final Response.ResponseBuilder responseBuilder = noCache(new StreamingOutputResponseBuilder(content.getContent()).range(rangeHeader).build());
        String contentType = content.getType();
        if (contentType == null) {
            contentType = MediaType.APPLICATION_OCTET_STREAM;
        }
        responseBuilder.type(contentType);
        return ResponseBuilderUtils.setContentDisposition(responseBuilder, content.getFilename()).build();
    }

    /**
     * Gets the content for the output of the specified event.
     *
     * @param clusterNodeId The id of the node within the cluster this content is on. Required if clustered.
     * @param id The id of the provenance event associated with this content.
     * @return The content stream
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("{id}/content/output")
    @Operation(
            summary = "Gets the output content for a provenance event",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = StreamingOutput.class))),
                    @ApiResponse(responseCode = "206", description = "Partial Content with range of bytes requested"),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "416", description = "Requested Range Not Satisfiable based on bytes requested"),
            },
            security = {
                    @SecurityRequirement(name = "Read Component Provenance Data - /provenance-data/{component-type}/{uuid}"),
                    @SecurityRequirement(name = "Read Component Data - /data/{component-type}/{uuid}")
            }
    )
    public Response getOutputContent(
            @Parameter(
                    description = "Range of bytes requested"
            )
            @HeaderParam("Range") final String rangeHeader,
            @Parameter(
                    description = "The id of the node where the content exists if clustered."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @Parameter(
                    description = "The provenance event id.",
                    required = true
            )
            @PathParam("id") final LongParameter id) {

        // ensure proper input
        if (id == null) {
            throw new IllegalArgumentException("The event id must be specified.");
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the uri of the request
        final String uri = generateResourceUri("provenance", "events", String.valueOf(id.getLong()), "content", "output");

        final DownloadableContent content = serviceFacade.getContent(id.getLong(), uri, ContentDirection.OUTPUT);
        final Response.ResponseBuilder responseBuilder = noCache(new StreamingOutputResponseBuilder(content.getContent()).range(rangeHeader).build());
        String contentType = content.getType();
        if (contentType == null) {
            contentType = MediaType.APPLICATION_OCTET_STREAM;
        }
        responseBuilder.type(contentType);
        return ResponseBuilderUtils.setContentDisposition(responseBuilder, content.getFilename()).build();
    }

    /**
     * Gets the details for a provenance event.
     *
     * @param id The id of the event
     * @param clusterNodeId The id of node in the cluster that the event/flowfile originated from. This is only required when clustered.
     * @return A provenanceEventEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Gets a provenance event",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProvenanceEventEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read Component Provenance Data - /provenance-data/{component-type}/{uuid}")
            }
    )
    public Response getProvenanceEvent(
            @Parameter(
                    description = "The id of the node where this event exists if clustered."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @Parameter(
                    description = "The provenance event id.",
                    required = true
            )
            @PathParam("id") final LongParameter id) {

        // ensure the id is specified
        if (id == null) {
            throw new IllegalArgumentException("Provenance event id must be specified.");
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // since we're cluster we must specify the cluster node identifier
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The cluster node identifier must be specified.");
            }

            return replicate(HttpMethod.GET, clusterNodeId);
        }

        // get the provenance event
        final ProvenanceEventDTO event = serviceFacade.getProvenanceEvent(id.getLong());
        event.setClusterNodeId(clusterNodeId);

        // populate the cluster node address
        final ClusterCoordinator coordinator = getClusterCoordinator();
        if (coordinator != null && clusterNodeId != null) {
            final NodeIdentifier nodeId = coordinator.getNodeIdentifier(clusterNodeId);

            if (nodeId != null) {
                event.setClusterNodeAddress(nodeId.getApiAddress() + ":" + nodeId.getApiPort());
            }
        }

        // create a response entity
        final ProvenanceEventEntity entity = new ProvenanceEventEntity();
        entity.setProvenanceEvent(event);

        // generate the response
        return generateOkResponse(entity).build();
    }


    /**
     * Triggers the latest Provenance Event for the specified component to be replayed.
     *
     * @param requestEntity The replay request
     * @return A replayLastEventResponseEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("latest/replays")
    @Operation(
            summary = "Replays content from a provenance event",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ReplayLastEventResponseEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read Component Provenance Data - /provenance-data/{component-type}/{uuid}"),
                    @SecurityRequirement(name = "Read Component Data - /data/{component-type}/{uuid}"),
                    @SecurityRequirement(name = "Write Component Data - /data/{component-type}/{uuid}")
            }
    )
    public Response submitReplayLatestEvent(
            @Parameter(
                    description = "The replay request.",
                    required = true
            ) final ReplayLastEventRequestEntity requestEntity) {

        // ensure the event id is specified
        if (requestEntity == null || requestEntity.getComponentId() == null) {
            throw new IllegalArgumentException("The id of the component must be specified.");
        }
        final String requestedNodes = requestEntity.getNodes();
        if (requestedNodes == null) {
            throw new IllegalArgumentException("The nodes must be specified.");
        }
        if (!"ALL".equalsIgnoreCase(requestedNodes) && !"PRIMARY".equalsIgnoreCase(requestedNodes)) {
            throw new IllegalArgumentException("The nodes must be either ALL or PRIMARY");
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // Replicate to either Primary Node or all nodes
            if (requestedNodes.equalsIgnoreCase("PRIMARY")) {
                final NodeIdentifier primaryNodeId = getPrimaryNodeId().orElseThrow(() -> new IllegalStateException("There is currently no Primary Node elected"));
                return replicate(HttpMethod.POST, requestEntity, primaryNodeId.getId());
            } else {
                return replicate(HttpMethod.POST, requestEntity);
            }
        }

        return withWriteLock(
                serviceFacade,
                requestEntity,
                lookup -> {
                    final Authorizable provenance = lookup.getProvenance();
                    provenance.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                }, // No verification step necessary - this can be done any time
                entity -> {
                    final ReplayLastEventSnapshotDTO aggregateSnapshot = new ReplayLastEventSnapshotDTO();

                    // Submit provenance query
                    try {
                        final ProvenanceEventDTO provenanceEventDto = serviceFacade.submitReplayLastEvent(entity.getComponentId());

                        if (provenanceEventDto == null) {
                            aggregateSnapshot.setEventAvailable(false);
                        } else {
                            aggregateSnapshot.setEventAvailable(true);
                            aggregateSnapshot.setEventsReplayed(Collections.singleton(provenanceEventDto.getEventId()));
                        }
                    } catch (final AccessDeniedException ade) {
                        logger.error("Failed to replay latest Provenance Event", ade);
                        aggregateSnapshot.setFailureExplanation("Access Denied");
                    } catch (final Exception e) {
                        logger.error("Failed to replay latest Provenance Event", e);
                        aggregateSnapshot.setFailureExplanation(e.getMessage());
                    }

                    final ReplayLastEventResponseEntity responseEntity = new ReplayLastEventResponseEntity();
                    responseEntity.setComponentId(entity.getComponentId());
                    responseEntity.setNodes(entity.getNodes());
                    responseEntity.setAggregateSnapshot(aggregateSnapshot);

                    return generateOkResponse(responseEntity).build();
                });
    }


    /**
     * Creates a new replay request for the content associated with the specified provenance event id.
     *
     * @param httpServletRequest request
     * @param replayRequestEntity The replay request
     * @return A provenanceEventEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("replays")
    @Operation(
            summary = "Replays content from a provenance event",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = ProvenanceEventEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read Component Provenance Data - /provenance-data/{component-type}/{uuid}"),
                    @SecurityRequirement(name = "Read Component Data - /data/{component-type}/{uuid}"),
                    @SecurityRequirement(name = "Write Component Data - /data/{component-type}/{uuid}")
            }
    )
    public Response submitReplay(
            @Context final HttpServletRequest httpServletRequest,
            @Parameter(
                    description = "The replay request.",
                    required = true
            ) final SubmitReplayRequestEntity replayRequestEntity) {

        // ensure the event id is specified
        if (replayRequestEntity == null || replayRequestEntity.getEventId() == null) {
            throw new IllegalArgumentException("The id of the event must be specified.");
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (replayRequestEntity.getClusterNodeId() == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
            } else {
                return replicate(HttpMethod.POST, replayRequestEntity, replayRequestEntity.getClusterNodeId());
            }
        }

        // handle expects request (usually from the cluster manager)
        final String expects = httpServletRequest.getHeader(RequestReplicationHeader.VALIDATION_EXPECTS.getHeader());
        if (expects != null) {
            return generateContinueResponse().build();
        }

        // submit the provenance replay request
        final ProvenanceEventDTO event = serviceFacade.submitReplay(replayRequestEntity.getEventId());
        event.setClusterNodeId(replayRequestEntity.getClusterNodeId());

        // populate the cluster node address
        final ClusterCoordinator coordinator = getClusterCoordinator();
        if (coordinator != null) {
            final NodeIdentifier nodeId = coordinator.getNodeIdentifier(replayRequestEntity.getClusterNodeId());
            event.setClusterNodeAddress(nodeId.getApiAddress() + ":" + nodeId.getApiPort());
        }

        // create a response entity
        final ProvenanceEventEntity entity = new ProvenanceEventEntity();
        entity.setProvenanceEvent(event);

        // generate the response
        URI uri = URI.create(generateResourceUri("provenance-events", event.getId()));
        return generateCreatedResponse(uri, entity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("latest/{componentId}")
    @Operation(
        summary = "Retrieves the latest cached Provenance Events for the specified component",
        responses = {
                @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = LatestProvenanceEventsEntity.class))),
                @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
        },
        security = {
            @SecurityRequirement(name = "Read Component Provenance Data - /provenance-data/{component-type}/{uuid}"),
            @SecurityRequirement(name = "Read Component Data - /data/{component-type}/{uuid}")
        }
    )
    public Response getLatestProvenanceEvents(
        @Parameter(
            description = "The ID of the component to retrieve the latest Provenance Events for.",
            required = true
        )
        @PathParam("componentId") final String componentId,
        @Parameter(
                description = "The number of events to limit the response to. Defaults to 10."
        )
        @DefaultValue("10")
        @QueryParam("limit") int limit
    ) {
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the latest provenance events
        final LatestProvenanceEventsEntity entity = serviceFacade.getLatestProvenanceEvents(componentId, limit);

        // generate the response
        return generateOkResponse(entity).build();
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
