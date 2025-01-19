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
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.StreamingOutput;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ConnectionAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.DownloadableContent;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.dto.FlowFileDTO;
import org.apache.nifi.web.api.dto.FlowFileSummaryDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.ListingRequestEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.streaming.StreamingOutputResponseBuilder;
import org.apache.nifi.web.util.ResponseBuilderUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.net.URI;

/**
 * RESTful endpoint for managing a flowfile queue.
 */
@Controller
@Path("/flowfile-queues")
@Tag(name = "FlowFileQueues")
public class FlowFileQueueResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populate the URIs for the specified flowfile listing.
     *
     * @param connectionId connection
     * @param flowFileListing flowfile listing
     * @return dto
     */
    public ListingRequestDTO populateRemainingFlowFileListingContent(final String connectionId, final ListingRequestDTO flowFileListing) {
        // uri of the listing
        flowFileListing.setUri(generateResourceUri("flowfile-queues", connectionId, "listing-requests", flowFileListing.getId()));

        // uri of each flowfile
        if (flowFileListing.getFlowFileSummaries() != null) {
            for (final FlowFileSummaryDTO flowFile : flowFileListing.getFlowFileSummaries()) {
                populateRemainingFlowFileContent(connectionId, flowFile);
            }
        }
        return flowFileListing;
    }

    /**
     * Populate the URIs for the specified flowfile.
     *
     * @param connectionId the connection id
     * @param flowFile the flowfile
     * @return the dto
     */
    public FlowFileSummaryDTO populateRemainingFlowFileContent(final String connectionId, final FlowFileSummaryDTO flowFile) {
        flowFile.setUri(generateResourceUri("flowfile-queues", connectionId, "flowfiles", flowFile.getUuid()));
        return flowFile;
    }

    /**
     * Gets the specified flowfile from the specified connection.
     *
     * @param connectionId The connection id
     * @param flowFileUuid The flowfile uuid
     * @param clusterNodeId The cluster node id where the flowfile resides
     * @return a flowFileDTO
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/flowfiles/{flowfile-uuid}")
    @Operation(
            summary = "Gets a FlowFile from a Connection.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowFileEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read Source Data - /data/{component-type}/{uuid}")
            }
    )
    public Response getFlowFile(
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @Parameter(
                    description = "The flowfile uuid.",
                    required = true
            )
            @PathParam("flowfile-uuid") final String flowFileUuid,
            @Parameter(
                    description = "The id of the node where the content exists if clustered."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId) throws InterruptedException {

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
            } else {
                // get the target node and ensure it exists
                final NodeIdentifier targetNode = getClusterCoordinator().getNodeIdentifier(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                return replicate(HttpMethod.GET, targetNode);
            }
        }

        // NOTE - deferred authorization so we can consider flowfile attributes in the access decision

        // get the flowfile
        final FlowFileDTO flowfileDto = serviceFacade.getFlowFile(connectionId, flowFileUuid);
        populateRemainingFlowFileContent(connectionId, flowfileDto);

        // create the response entity
        final FlowFileEntity entity = new FlowFileEntity();
        entity.setFlowFile(flowfileDto);

        return generateOkResponse(entity).build();
    }

    /**
     * Gets the content for the specified flowfile in the specified connection.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param connectionId The connection id
     * @param flowFileUuid The flowfile uuid
     * @param clusterNodeId The cluster node id
     * @return The content stream
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("{id}/flowfiles/{flowfile-uuid}/content")
    @Operation(
            summary = "Gets the content for a FlowFile in a Connection.",
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
                    @SecurityRequirement(name = "Read Source Data - /data/{component-type}/{uuid}")
            }
    )
    public Response downloadFlowFileContent(
            @Parameter(
                    description = "Range of bytes requested"
            )
            @HeaderParam("Range") final String rangeHeader,
            @Parameter(
                    description = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response."
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @Parameter(
                    description = "The flowfile uuid.",
                    required = true
            )
            @PathParam("flowfile-uuid") final String flowFileUuid,
            @Parameter(
                    description = "The id of the node where the content exists if clustered."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId) {

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                throw new IllegalArgumentException("The id of the node in the cluster is required.");
            } else {
                // get the target node and ensure it exists
                final NodeIdentifier targetNode = getClusterCoordinator().getNodeIdentifier(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                return replicate(HttpMethod.GET, targetNode);
            }
        }

        // NOTE - deferred authorization so we can consider flowfile attributes in the access decision

        // get the uri of the request
        final String uri = generateResourceUri("flowfile-queues", connectionId, "flowfiles", flowFileUuid, "content");

        final DownloadableContent content = serviceFacade.getContent(connectionId, flowFileUuid, uri);
        final Response.ResponseBuilder responseBuilder = noCache(new StreamingOutputResponseBuilder(content.getContent()).range(rangeHeader).build());

        String contentType = content.getType();
        if (contentType == null) {
            contentType = MediaType.APPLICATION_OCTET_STREAM;
        }
        responseBuilder.type(contentType);
        return ResponseBuilderUtils.setContentDisposition(responseBuilder, content.getFilename()).build();
    }

    /**
     * Creates a request to list the flowfiles in the queue of the specified connection.
     *
     * @param id The id of the connection
     * @return A listRequestEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/listing-requests")
    @Operation(
            summary = "Lists the contents of the queue in this connection.",
            responses = {
                    @ApiResponse(
                            responseCode = "202", description = "The request has been accepted. A HTTP response header will contain the URI where the response can be polled.",
                            content = @Content(schema = @Schema(implementation = ListingRequestEntity.class))
                    ),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read Source Data - /data/{component-type}/{uuid}")
            }
    )
    public Response createFlowFileListing(
            @Parameter(description = "The connection id.", required = true)
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final ConnectionEntity requestConnectionEntity = new ConnectionEntity();
        requestConnectionEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestConnectionEntity,
                lookup -> {
                    final ConnectionAuthorizable connAuth = lookup.getConnection(id);
                    final Authorizable dataAuthorizable = connAuth.getSourceData();
                    dataAuthorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyListQueue(id),
                (connectionEntity) -> {
                    // ensure the id is the same across the cluster
                    final String listingRequestId = generateUuid();

                    // submit the listing request
                    final ListingRequestDTO listingRequest = serviceFacade.createFlowFileListingRequest(connectionEntity.getId(), listingRequestId);
                    populateRemainingFlowFileListingContent(connectionEntity.getId(), listingRequest);

                    // create the response entity
                    final ListingRequestEntity entity = new ListingRequestEntity();
                    entity.setListingRequest(listingRequest);

                    // generate the URI where the response will be
                    final URI location = URI.create(listingRequest.getUri());
                    return Response.status(Status.ACCEPTED).location(location).entity(entity).build();
                }
        );
    }

    /**
     * Checks the status of an outstanding listing request.
     *
     * @param connectionId The id of the connection
     * @param listingRequestId The id of the drop request
     * @return A dropRequestEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/listing-requests/{listing-request-id}")
    @Operation(
            summary = "Gets the current status of a listing request for the specified connection.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ListingRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read Source Data - /data/{component-type}/{uuid}")
            }
    )
    public Response getListingRequest(
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @Parameter(
                    description = "The listing request id.",
                    required = true
            )
            @PathParam("listing-request-id") final String listingRequestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final ConnectionAuthorizable connAuth = lookup.getConnection(connectionId);
            final Authorizable dataAuthorizable = connAuth.getSourceData();
            dataAuthorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the listing request
        final ListingRequestDTO listingRequest = serviceFacade.getFlowFileListingRequest(connectionId, listingRequestId);
        populateRemainingFlowFileListingContent(connectionId, listingRequest);

        // create the response entity
        final ListingRequestEntity entity = new ListingRequestEntity();
        entity.setListingRequest(listingRequest);

        return generateOkResponse(entity).build();
    }

    /**
     * Deletes the specified listing request.
     *
     * @param connectionId The connection id
     * @param listingRequestId The drop request id
     * @return A dropRequestEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/listing-requests/{listing-request-id}")
    @Operation(
            summary = "Cancels and/or removes a request to list the contents of this connection.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ListingRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read Source Data - /data/{component-type}/{uuid}")
            }
    )
    public Response deleteListingRequest(
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @Parameter(
                    description = "The listing request id.",
                    required = true
            )
            @PathParam("listing-request-id") final String listingRequestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        return withWriteLock(
                serviceFacade,
                new ListingEntity(connectionId, listingRequestId),
                lookup -> {
                    final ConnectionAuthorizable connAuth = lookup.getConnection(connectionId);
                    final Authorizable dataAuthorizable = connAuth.getSourceData();
                    dataAuthorizable.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                null,
                (listingEntity) -> {
                    // delete the listing request
                    final ListingRequestDTO listingRequest = serviceFacade.deleteFlowFileListingRequest(listingEntity.getConnectionId(), listingEntity.getListingRequestId());

                    // prune the results as they were already received when the listing completed
                    listingRequest.setFlowFileSummaries(null);

                    // populate remaining content
                    populateRemainingFlowFileListingContent(listingEntity.getConnectionId(), listingRequest);

                    // create the response entity
                    final ListingRequestEntity entity = new ListingRequestEntity();
                    entity.setListingRequest(listingRequest);

                    return generateOkResponse(entity).build();
                }
        );
    }

    private static class ListingEntity extends Entity {
        final String connectionId;
        final String listingRequestId;

        public ListingEntity(String connectionId, String listingRequestId) {
            this.connectionId = connectionId;
            this.listingRequestId = listingRequestId;
        }

        public String getConnectionId() {
            return connectionId;
        }

        public String getListingRequestId() {
            return listingRequestId;
        }
    }

    /**
     * Creates a request to delete the flowfiles in the queue of the specified connection.
     *
     * @param id The id of the connection
     * @return A dropRequestEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/drop-requests")
    @Operation(
            summary = "Creates a request to drop the contents of the queue in this connection.",
            responses = {
                    @ApiResponse(
                            responseCode = "202", description = "The request has been accepted. A HTTP response header will contain the URI where the response can be polled.",
                            content = @Content(schema = @Schema(implementation = DropRequestEntity.class))
                    ),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write Source Data - /data/{component-type}/{uuid}")
            }
    )
    public Response createDropRequest(
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final ConnectionEntity requestConnectionEntity = new ConnectionEntity();
        requestConnectionEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestConnectionEntity,
                lookup -> {
                    final ConnectionAuthorizable connAuth = lookup.getConnection(id);
                    final Authorizable dataAuthorizable = connAuth.getSourceData();
                    dataAuthorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (connectionEntity) -> {
                    // ensure the id is the same across the cluster
                    final String dropRequestId = generateUuid();

                    // submit the drop request
                    final DropRequestDTO dropRequest = serviceFacade.createFlowFileDropRequest(connectionEntity.getId(), dropRequestId);
                    dropRequest.setUri(generateResourceUri("flowfile-queues", connectionEntity.getId(), "drop-requests", dropRequest.getId()));

                    // create the response entity
                    final DropRequestEntity entity = new DropRequestEntity();
                    entity.setDropRequest(dropRequest);

                    // generate the URI where the response will be
                    final URI location = URI.create(dropRequest.getUri());
                    return Response.status(Status.ACCEPTED).location(location).entity(entity).build();
                }
        );
    }

    /**
     * Checks the status of an outstanding drop request.
     *
     * @param connectionId The id of the connection
     * @param dropRequestId The id of the drop request
     * @return A dropRequestEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/drop-requests/{drop-request-id}")
    @Operation(
            summary = "Gets the current status of a drop request for the specified connection.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = DropRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write Source Data - /data/{component-type}/{uuid}")
            }
    )
    public Response getDropRequest(
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @Parameter(
                    description = "The drop request id.",
                    required = true
            )
            @PathParam("drop-request-id") final String dropRequestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final ConnectionAuthorizable connAuth = lookup.getConnection(connectionId);
            final Authorizable dataAuthorizable = connAuth.getSourceData();
            dataAuthorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        // get the drop request
        final DropRequestDTO dropRequest = serviceFacade.getFlowFileDropRequest(connectionId, dropRequestId);
        dropRequest.setUri(generateResourceUri("flowfile-queues", connectionId, "drop-requests", dropRequestId));

        // create the response entity
        final DropRequestEntity entity = new DropRequestEntity();
        entity.setDropRequest(dropRequest);

        return generateOkResponse(entity).build();
    }

    /**
     * Deletes the specified drop request.
     *
     * @param connectionId The connection id
     * @param dropRequestId The drop request id
     * @return A dropRequestEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/drop-requests/{drop-request-id}")
    @Operation(
            summary = "Cancels and/or removes a request to drop the contents of this connection.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = DropRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write Source Data - /data/{component-type}/{uuid}")
            }
    )
    public Response removeDropRequest(
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") final String connectionId,
            @Parameter(
                    description = "The drop request id.",
                    required = true
            )
            @PathParam("drop-request-id") final String dropRequestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        return withWriteLock(
                serviceFacade,
                new DropEntity(connectionId, dropRequestId),
                lookup -> {
                    final ConnectionAuthorizable connAuth = lookup.getConnection(connectionId);
                    final Authorizable dataAuthorizable = connAuth.getSourceData();
                    dataAuthorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (dropEntity) -> {
                    // delete the drop request
                    final DropRequestDTO dropRequest = serviceFacade.deleteFlowFileDropRequest(dropEntity.getConnectionId(), dropEntity.getDropRequestId());
                    dropRequest.setUri(generateResourceUri("flowfile-queues", dropEntity.getConnectionId(), "drop-requests", dropEntity.getDropRequestId()));

                    // create the response entity
                    final DropRequestEntity entity = new DropRequestEntity();
                    entity.setDropRequest(dropRequest);

                    return generateOkResponse(entity).build();
                }
        );
    }

    private static class DropEntity extends Entity {
        final String connectionId;
        final String dropRequestId;

        public DropEntity(String connectionId, String dropRequestId) {
            this.connectionId = connectionId;
            this.dropRequestId = dropRequestId;
        }

        public String getConnectionId() {
            return connectionId;
        }

        public String getDropRequestId() {
            return dropRequestId;
        }
    }

    @Autowired
    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Autowired
    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
