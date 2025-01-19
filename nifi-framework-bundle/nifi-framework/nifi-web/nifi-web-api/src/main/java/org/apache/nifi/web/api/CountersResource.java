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
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.CounterEntity;
import org.apache.nifi.web.api.entity.CountersEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

/**
 * RESTful endpoint for managing Counters.
 */
@Controller
@Path("/counters")
@Tag(name = "Counters")
public class CountersResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Authorizes access to the flow.
     */
    private void authorizeCounters(final RequestAction action) {
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable counters = lookup.getCounters();
            counters.authorize(authorizer, action, NiFiUserUtils.getNiFiUser());
        });
    }

    /**
     * Retrieves the counters report for this NiFi.
     *
     * @return A countersEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("")
    @Operation(
            summary = "Gets the current counters for this NiFi",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = CountersEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /counters")
            }
    )
    public Response getCounters(
            @Parameter(
                    description = "Whether or not to include the breakdown per node. Optional, defaults to false"
            )
            @QueryParam("nodewise") @DefaultValue(NODEWISE) final Boolean nodewise,
            @Parameter(
                    description = "The id of the node where to get the status."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId) throws InterruptedException {

        authorizeCounters(RequestAction.READ);

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        // replicate if necessary
        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse;

                // Determine whether we should replicate only to the cluster coordinator, or if we should replicate directly
                // to the cluster nodes themselves.
                if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                    nodeResponse = getRequestReplicator().replicate(HttpMethod.GET, getAbsolutePath(), getRequestParameters(), getHeaders()).awaitMergedResponse();
                } else {
                    nodeResponse = getRequestReplicator().forwardToCoordinator(
                            getClusterCoordinatorNode(), HttpMethod.GET, getAbsolutePath(), getRequestParameters(), getHeaders()).awaitMergedResponse();
                }

                final CountersEntity entity = (CountersEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getCounters().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                // get the target node and ensure it exists
                final NodeIdentifier targetNode = getClusterCoordinator().getNodeIdentifier(clusterNodeId);
                if (targetNode == null) {
                    throw new UnknownNodeException("The specified cluster node does not exist.");
                }

                return replicate(HttpMethod.GET, targetNode);
            }
        }

        final CountersDTO countersReport = serviceFacade.getCounters();

        // create the response entity
        final CountersEntity entity = new CountersEntity();
        entity.setCounters(countersReport);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Update the specified counter. This will reset the counter value to 0.
     *
     * @param id The id of the counter.
     * @return A counterEntity.
     */
    @PUT
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Updates the specified counter. This will reset the counter value to 0",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = CounterEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /counters")
            }
    )
    public Response updateCounter(
            @Parameter(description = "The id of the counter.")
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT);
        }

        final ComponentEntity requestComponentEntity = new ComponentEntity();
        requestComponentEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestComponentEntity,
                lookup -> authorizeCounters(RequestAction.WRITE),
                null,
                (componentEntity) -> {
                    // reset the specified counter
                    final CounterDTO counter = serviceFacade.updateCounter(componentEntity.getId());

                    // create the response entity
                    final CounterEntity entity = new CounterEntity();
                    entity.setCounter(counter);

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
