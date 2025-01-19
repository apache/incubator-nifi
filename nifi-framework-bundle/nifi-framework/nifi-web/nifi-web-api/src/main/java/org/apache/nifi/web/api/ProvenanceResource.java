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
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceOptionsDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageRequestDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageResultsDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.LineageEntity;
import org.apache.nifi.web.api.entity.ProvenanceEntity;
import org.apache.nifi.web.api.entity.ProvenanceOptionsEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;


/**
 * RESTful endpoint for querying data provenance.
 */
@Controller
@Path("/provenance")
@Tag(name = "Provenance")
public class ProvenanceResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populates the uri for the specified provenance.
     */
    private ProvenanceDTO populateRemainingProvenanceContent(ProvenanceDTO provenance) {
        provenance.setUri(generateResourceUri("provenance", provenance.getId()));
        return provenance;
    }

    /**
     * Populates the uri for the specified lineage.
     */
    private LineageDTO populateRemainingLineageContent(LineageDTO lineage, String clusterNodeId) {
        lineage.setUri(generateResourceUri("provenance", "lineage", lineage.getId()));

        // set the cluster node id
        lineage.getRequest().setClusterNodeId(clusterNodeId);
        final LineageResultsDTO results = lineage.getResults();
        if (results != null && results.getNodes() != null) {
            results.getNodes().forEach(node -> node.setClusterNodeIdentifier(clusterNodeId));
        }

        return lineage;
    }

    private void authorizeProvenanceRequest() {
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable provenance = lookup.getProvenance();
            provenance.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });
    }

    /**
     * Gets the provenance search options for this NiFi.
     *
     * @return A provenanceOptionsEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("search-options")
    @Operation(
            summary = "Gets the searchable attributes for provenance events",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProvenanceOptionsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /provenance")
            }
    )
    public Response getSearchOptions() {

        authorizeProvenanceRequest();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get provenance search options
        final ProvenanceOptionsDTO searchOptions = serviceFacade.getProvenanceSearchOptions();

        // create the response entity
        final ProvenanceOptionsEntity entity = new ProvenanceOptionsEntity();
        entity.setProvenanceOptions(searchOptions);

        // generate the response
        return noCache(Response.ok(entity)).build();
    }

    /**
     * Creates provenance using the specified query criteria.
     *
     * @param requestProvenanceEntity A provenanceEntity
     * @return A provenanceEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("")
    @Operation(
            summary = "Submits a provenance query",
            description = "Provenance queries may be long running so this endpoint submits a request. The response will include the "
                    + "current state of the query. If the request is not completed the URI in the response can be used at a "
                    + "later time to get the updated state of the query. Once the query has completed the provenance request "
                    + "should be deleted by the client who originally submitted it.",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = ProvenanceEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /provenance"),
                    @SecurityRequirement(name = "Read - /data/{component-type}/{uuid}")
            }
    )
    public Response submitProvenanceRequest(
            @Parameter(description = "The provenance query details.", required = true)
            ProvenanceEntity requestProvenanceEntity) {

        // check the request
        if (requestProvenanceEntity == null) {
            requestProvenanceEntity = new ProvenanceEntity();
        }

        // get the provenance
        final ProvenanceDTO requestProvenanceDto;
        if (requestProvenanceEntity.getProvenance() != null) {
            requestProvenanceDto = requestProvenanceEntity.getProvenance();
        } else {
            requestProvenanceDto = new ProvenanceDTO();
            requestProvenanceEntity.setProvenance(requestProvenanceDto);
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);

            // determine where this request should be sent
            if (requestProvenanceDto.getRequest() == null || requestProvenanceDto.getRequest().getClusterNodeId() == null) {
                // replicate to all nodes
                return replicate(HttpMethod.POST, requestProvenanceEntity, headersToOverride);
            } else {
                return replicate(HttpMethod.POST, requestProvenanceEntity, requestProvenanceDto.getRequest().getClusterNodeId(), headersToOverride);
            }
        }

        return withWriteLock(
                serviceFacade,
                requestProvenanceEntity,
                lookup -> authorizeProvenanceRequest(),
                null,
                (provenanceEntity) -> {
                    final ProvenanceDTO provenanceDTO = provenanceEntity.getProvenance();

                    // ensure the id is the same across the cluster
                    final String provenanceId = generateUuid();

                    // set the provenance id accordingly
                    provenanceDTO.setId(provenanceId);

                    // submit the provenance request
                    final ProvenanceDTO dto = serviceFacade.submitProvenance(provenanceDTO);
                    populateRemainingProvenanceContent(dto);

                    // set the cluster id if necessary
                    if (provenanceDTO.getRequest() != null && provenanceDTO.getRequest().getClusterNodeId() != null) {
                        dto.getRequest().setClusterNodeId(provenanceDTO.getRequest().getClusterNodeId());
                    }

                    // create the response entity
                    final ProvenanceEntity entity = new ProvenanceEntity();
                    entity.setProvenance(dto);

                    // generate the response
                    return generateCreatedResponse(URI.create(dto.getUri()), entity).build();
                }
        );
    }

    /**
     * Gets the provenance with the specified id.
     *
     * @param id The id of the provenance
     * @param clusterNodeId The id of node in the cluster to search. This is optional and only relevant when clustered. If clustered and it is not specified the entire cluster is searched.
     * @return A provenanceEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Gets a provenance query",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProvenanceEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /provenance"),
                    @SecurityRequirement(name = "Read - /data/{component-type}/{uuid}")
            }
    )
    public Response getProvenance(
            @Parameter(
                    description = "The id of the node where this query exists if clustered."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @Parameter(
                    description = "Whether or not incremental results are returned. If false, provenance events"
                            + " are only returned once the query completes. This property is true by default."
            )
            @QueryParam("summarize") @DefaultValue(value = "false") final Boolean summarize,
            @Parameter(
                    description = "Whether or not to summarize provenance events returned. This property is false by default."
            )
            @QueryParam("incrementalResults") @DefaultValue(value = "true") final Boolean incrementalResults,
            @Parameter(
                    description = "The id of the provenance query.",
                    required = true
            )
            @PathParam("id") final String id) {

        authorizeProvenanceRequest();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                // replicate to all nodes
                return replicate(HttpMethod.GET);
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the provenance
        final ProvenanceDTO dto = serviceFacade.getProvenance(id, summarize, incrementalResults);
        dto.getRequest().setClusterNodeId(clusterNodeId);
        populateRemainingProvenanceContent(dto);

        // create the response entity
        final ProvenanceEntity entity = new ProvenanceEntity();
        entity.setProvenance(dto);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Deletes the provenance with the specified id.
     *
     * @param id The id of the provenance
     * @param clusterNodeId The id of node in the cluster to search. This is optional and only relevant when clustered. If clustered and it is not specified the entire cluster is searched.
     * @return A provenanceEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Deletes a provenance query",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProvenanceEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /provenance")
            }
    )
    public Response deleteProvenance(
            @Parameter(
                    description = "The id of the node where this query exists if clustered."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @Parameter(
                    description = "The id of the provenance query.",
                    required = true
            )
            @PathParam("id") final String id) {

        // replicate if cluster manager
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                // replicate to all nodes
                return replicate(HttpMethod.DELETE);
            } else {
                return replicate(HttpMethod.DELETE, clusterNodeId);
            }
        }

        final ComponentEntity requestEntity = new ComponentEntity();
        requestEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestEntity,
                lookup -> authorizeProvenanceRequest(),
                null,
                (entity) -> {
                    // delete the provenance
                    serviceFacade.deleteProvenance(entity.getId());

                    // generate the response
                    return generateOkResponse(new ProvenanceEntity()).build();
                }
        );
    }

    /**
     * Submits a lineage request based on an event or a flowfile uuid.
     * <p>
     * When querying for the lineage of an event you must specify the eventId and the eventDirection. The eventDirection must be 'parents' or 'children' and specifies whether we are going up or down
     * the flowfile ancestry. The uuid cannot be specified in these cases.
     * <p>
     * When querying for the lineage of a flowfile you must specify the uuid. The eventId and eventDirection cannot be specified in this case.
     *
     * @param requestLineageEntity A lineageEntity
     * @return A lineageEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("lineage")
    @Operation(
            summary = "Submits a lineage query",
            description = "Lineage queries may be long running so this endpoint submits a request. The response will include the "
                    + "current state of the query. If the request is not completed the URI in the response can be used at a "
                    + "later time to get the updated state of the query. Once the query has completed the lineage request "
                    + "should be deleted by the client who originally submitted it.",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = LineageEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /provenance"),
                    @SecurityRequirement(name = "Read - /data/{component-type}/{uuid}")
            }
    )
    public Response submitLineageRequest(
            @Parameter(
                    description = "The lineage query details.",
                    required = true
            ) final LineageEntity requestLineageEntity) {

        if (requestLineageEntity == null || requestLineageEntity.getLineage() == null || requestLineageEntity.getLineage().getRequest() == null) {
            throw new IllegalArgumentException("Lineage request must be specified.");
        }

        // ensure the request is well formed
        final LineageDTO requestLineageDto = requestLineageEntity.getLineage();
        final LineageRequestDTO requestDto = requestLineageDto.getRequest();

        // ensure the type has been specified
        if (requestDto.getLineageRequestType() == null) {
            throw new IllegalArgumentException("The type of lineage request must be specified.");
        }

        // validate the remainder of the request
        switch (requestDto.getLineageRequestType()) {
            case CHILDREN:
            case PARENTS:
                // ensure the event has been specified
                if (requestDto.getEventId() == null) {
                    throw new IllegalArgumentException("The event id must be specified when the event type is PARENTS or CHILDREN.");
                }
                break;
            case FLOWFILE:
                // ensure the uuid or event id has been specified
                if (requestDto.getUuid() == null && requestDto.getEventId() == null) {
                    throw new IllegalArgumentException("The flowfile uuid or event id must be specified when the event type is FLOWFILE.");
                }
                break;
        }

        // replicate if cluster manager
        if (isReplicateRequest()) {
            if (requestDto.getClusterNodeId() == null) {
                throw new IllegalArgumentException("The cluster node identifier must be specified.");
            }

            // change content type to JSON for serializing entity
            final Map<String, String> headersToOverride = new HashMap<>();
            headersToOverride.put("content-type", MediaType.APPLICATION_JSON);
            return replicate(HttpMethod.POST, requestLineageEntity, requestDto.getClusterNodeId(), headersToOverride);
        }

        return withWriteLock(
                serviceFacade,
                requestLineageEntity,
                lookup -> authorizeProvenanceRequest(),
                null,
                (lineageEntity) -> {
                    final LineageDTO lineageDTO = lineageEntity.getLineage();

                    // get the provenance event
                    final LineageDTO dto = serviceFacade.submitLineage(lineageDTO);
                    populateRemainingLineageContent(dto, lineageDTO.getRequest().getClusterNodeId());

                    // create a response entity
                    final LineageEntity entity = new LineageEntity();
                    entity.setLineage(dto);

                    // generate the response
                    return generateCreatedResponse(URI.create(dto.getUri()), entity).build();
                }
        );
    }

    /**
     * Gets the lineage with the specified id.
     *
     * @param clusterNodeId The id of node in the cluster that the event/flowfile originated from. This is only required when clustered.
     * @param id The id of the lineage
     * @return A lineageEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("lineage/{id}")
    @Operation(
            summary = "Gets a lineage query",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = LineageEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /provenance"),
                    @SecurityRequirement(name = "Read - /data/{component-type}/{uuid}")
            }
    )
    public Response getLineage(
            @Parameter(
                    description = "The id of the node where this query exists if clustered."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @Parameter(
                    description = "The id of the lineage query.",
                    required = true
            )
            @PathParam("id") final String id) {

        authorizeProvenanceRequest();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET, clusterNodeId);
        }

        // get the lineage
        final LineageDTO dto = serviceFacade.getLineage(id);
        populateRemainingLineageContent(dto, clusterNodeId);

        // create the response entity
        final LineageEntity entity = new LineageEntity();
        entity.setLineage(dto);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Deletes the lineage with the specified id.
     *
     * @param clusterNodeId The id of node in the cluster that the event/flowfile originated from. This is only required when clustered.
     * @param id The id of the lineage
     * @return A lineageEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("lineage/{id}")
    @Operation(
            summary = "Deletes a lineage query",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = LineageEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /provenance")
            }
    )
    public Response deleteLineage(
            @Parameter(
                    description = "The id of the node where this query exists if clustered."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId,
            @Parameter(
                    description = "The id of the lineage query.",
                    required = true
            )
            @PathParam("id") final String id) {

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE, clusterNodeId);
        }

        final ComponentEntity requestEntity = new ComponentEntity();
        requestEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestEntity,
                lookup -> authorizeProvenanceRequest(),
                null,
                (entity) -> {
                    // delete the lineage
                    serviceFacade.deleteLineage(entity.getId());

                    // generate the response
                    return generateOkResponse(new LineageEntity()).build();
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
