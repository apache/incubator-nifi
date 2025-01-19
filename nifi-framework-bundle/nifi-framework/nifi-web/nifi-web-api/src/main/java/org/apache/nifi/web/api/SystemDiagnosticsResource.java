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
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.diagnostics.DiagnosticLevel;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.JmxMetricsResultDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsDTO;
import org.apache.nifi.web.api.entity.JmxMetricsResultsEntity;
import org.apache.nifi.web.api.entity.SystemDiagnosticsEntity;
import org.apache.nifi.web.api.metrics.jmx.JmxMetricsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.util.Collection;

/**
 * RESTful endpoint for retrieving system diagnostics.
 */
@Controller
@Path("/system-diagnostics")
@Tag(name = "SystemDiagnostics")
public class SystemDiagnosticsResource extends ApplicationResource {
    private JmxMetricsService jmxMetricsService;
    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    private void authorizeSystem() {
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable system = lookup.getSystem();
            system.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });
    }

    /**
     * Gets the system diagnostics for this NiFi instance.
     *
     * @return A systemDiagnosticsEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Gets the diagnostics for the system NiFi is running on",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = SystemDiagnosticsEntity.class))),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
            },
            security = {
                    @SecurityRequirement(name = "Read - /system")
            }
    )
    public Response getSystemDiagnostics(
            @Parameter(
                    description = "Whether or not to include the breakdown per node. Optional, defaults to false"
            )
            @QueryParam("nodewise") @DefaultValue(NODEWISE) final Boolean nodewise,
            @Parameter(
                    description = "Whether or not to include verbose details. Optional, defaults to false"
            )
            @QueryParam("diagnosticLevel") @DefaultValue("BASIC") final DiagnosticLevel diagnosticLevel,
            @Parameter(
                    description = "The id of the node where to get the status."
            )
            @QueryParam("clusterNodeId") final String clusterNodeId) throws InterruptedException {

        authorizeSystem();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

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

                final SystemDiagnosticsEntity entity = (SystemDiagnosticsEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getSystemDiagnostics().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET);
            }
        }

        final SystemDiagnosticsDTO systemDiagnosticsDto = serviceFacade.getSystemDiagnostics(diagnosticLevel);

        // create the response
        final SystemDiagnosticsEntity entity = new SystemDiagnosticsEntity();
        entity.setSystemDiagnostics(systemDiagnosticsDto);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the JMX metrics.
     *
     * @return A jmxMetricsResult list.
     */
    @Path("jmx-metrics")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Retrieve available JMX metrics",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = JmxMetricsResultsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /system")
            }
    )
    public Response getJmxMetrics(
            @Parameter(description = "Regular Expression Pattern to be applied against the ObjectName")
            @QueryParam("beanNameFilter") final String beanNameFilter
    ) {
        authorizeJmxMetrics();

        final Collection<JmxMetricsResultDTO> results = jmxMetricsService.getFilteredMBeanMetrics(beanNameFilter);
        final JmxMetricsResultsEntity entity = new JmxMetricsResultsEntity();
        entity.setJmxMetricsResults(results);

        return generateOkResponse(entity)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }

    private void authorizeJmxMetrics() {
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable system = lookup.getSystem();
            system.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });
    }

    @Autowired
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Autowired
    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Autowired
    public void setJmxMetricsService(final JmxMetricsService jmxMetricsService) {
        this.jmxMetricsService = jmxMetricsService;
    }
}
