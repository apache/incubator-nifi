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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.c2.protocol.component.api.ControllerServiceDefinition;
import org.apache.nifi.c2.protocol.component.api.FlowAnalysisRuleDefinition;
import org.apache.nifi.c2.protocol.component.api.ParameterProviderDefinition;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.ReportingTaskDefinition;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.NarClassLoadersHolder;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.FlowVersionLocation;
import org.apache.nifi.ui.extension.contentviewer.ContentViewer;
import org.apache.nifi.web.IllegalClusterResourceRequestException;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.AboutDTO;
import org.apache.nifi.web.api.dto.BannerDTO;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ClusterSummaryDTO;
import org.apache.nifi.web.api.dto.ComponentDifferenceDTO;
import org.apache.nifi.web.api.dto.ContentViewerDTO;
import org.apache.nifi.web.api.dto.DifferenceDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SupportedMimeTypesDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.dto.search.NodeSearchResultDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.entity.AboutEntity;
import org.apache.nifi.web.api.entity.ActionEntity;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.AdditionalDetailsEntity;
import org.apache.nifi.web.api.entity.BannerEntity;
import org.apache.nifi.web.api.entity.BulletinBoardEntity;
import org.apache.nifi.web.api.entity.ClusterSearchResultsEntity;
import org.apache.nifi.web.api.entity.ClusterSummaryEntity;
import org.apache.nifi.web.api.entity.ComponentHistoryEntity;
import org.apache.nifi.web.api.entity.ConnectionStatisticsEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.ContentViewerEntity;
import org.apache.nifi.web.api.entity.ControllerBulletinsEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceTypesEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.apache.nifi.web.api.entity.ControllerStatusEntity;
import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisResultEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleTypesEntity;
import org.apache.nifi.web.api.entity.FlowBreadcrumbEntity;
import org.apache.nifi.web.api.entity.FlowComparisonEntity;
import org.apache.nifi.web.api.entity.FlowConfigurationEntity;
import org.apache.nifi.web.api.entity.FlowRegistryBranchEntity;
import org.apache.nifi.web.api.entity.FlowRegistryBranchesEntity;
import org.apache.nifi.web.api.entity.FlowRegistryBucketEntity;
import org.apache.nifi.web.api.entity.FlowRegistryBucketsEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientsEntity;
import org.apache.nifi.web.api.entity.HistoryEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextsEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ParameterProviderTypesEntity;
import org.apache.nifi.web.api.entity.ParameterProvidersEntity;
import org.apache.nifi.web.api.entity.PortStatusEntity;
import org.apache.nifi.web.api.entity.PrioritizerTypesEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorTypesEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ReportingTaskTypesEntity;
import org.apache.nifi.web.api.entity.ReportingTasksEntity;
import org.apache.nifi.web.api.entity.RuntimeManifestEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.apache.nifi.web.api.entity.SearchResultsEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.api.entity.VersionedFlowEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataSetEntity;
import org.apache.nifi.web.api.entity.VersionedFlowsEntity;
import org.apache.nifi.web.api.metrics.JsonFormatPrometheusMetricsWriter;
import org.apache.nifi.web.api.metrics.PrometheusMetricsWriter;
import org.apache.nifi.web.api.metrics.TextFormatPrometheusMetricsWriter;
import org.apache.nifi.web.api.request.BulletinBoardPatternParameter;
import org.apache.nifi.web.api.request.DateTimeParameter;
import org.apache.nifi.web.api.request.FlowMetricsProducer;
import org.apache.nifi.web.api.request.FlowMetricsRegistry;
import org.apache.nifi.web.api.request.IntegerParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.servlet.shared.RequestUriBuilder;
import org.apache.nifi.web.util.PaginationHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.text.Collator;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.web.api.entity.ScheduleComponentsEntity.STATE_DISABLED;
import static org.apache.nifi.web.api.entity.ScheduleComponentsEntity.STATE_ENABLED;

/**
 * RESTful endpoint for managing a Flow.
 */
@Controller
@Path("/flow")
@Tag(name = "Flow")
public class FlowResource extends ApplicationResource {

    private static final String RECURSIVE = "false";

    private static final String VERSIONED_REPORTING_TASK_SNAPSHOT_FILENAME_PATTERN = "VersionedReportingTaskSnapshot-%s.json";
    private static final String VERSIONED_REPORTING_TASK_SNAPSHOT_DATE_FORMAT = "yyyyMMddHHmmss";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    private ProcessorResource processorResource;
    private InputPortResource inputPortResource;
    private OutputPortResource outputPortResource;
    private FunnelResource funnelResource;
    private LabelResource labelResource;
    private RemoteProcessGroupResource remoteProcessGroupResource;
    private ConnectionResource connectionResource;
    private ProcessGroupResource processGroupResource;
    private ControllerServiceResource controllerServiceResource;
    private ReportingTaskResource reportingTaskResource;
    private ParameterProviderResource parameterProviderResource;

    @Context
    private ServletContext servletContext;

    public FlowResource() {
        super();
    }

    /**
     * Populates the remaining fields in the specified process group.
     *
     * @param flow group
     */
    private void populateRemainingFlowContent(ProcessGroupFlowDTO flow) {
        FlowDTO flowStructure = flow.getFlow();

        // populate the remaining fields for the processors, connections, process group refs, remote process groups, and labels if appropriate
        if (flowStructure != null) {
            populateRemainingFlowStructure(flowStructure);
        }

        // set the process group uri
        flow.setUri(generateResourceUri("flow", "process-groups", flow.getId()));
    }

    /**
     * Populates the remaining content of the specified snippet.
     */
    private void populateRemainingFlowStructure(FlowDTO flowStructure) {
        processorResource.populateRemainingProcessorEntitiesContent(flowStructure.getProcessors());
        connectionResource.populateRemainingConnectionEntitiesContent(flowStructure.getConnections());
        inputPortResource.populateRemainingInputPortEntitiesContent(flowStructure.getInputPorts());
        outputPortResource.populateRemainingOutputPortEntitiesContent(flowStructure.getOutputPorts());
        remoteProcessGroupResource.populateRemainingRemoteProcessGroupEntitiesContent(flowStructure.getRemoteProcessGroups());
        funnelResource.populateRemainingFunnelEntitiesContent(flowStructure.getFunnels());
        labelResource.populateRemainingLabelEntitiesContent(flowStructure.getLabels());
        processGroupResource.populateRemainingProcessGroupEntitiesContent(flowStructure.getProcessGroups());

        // go through each process group child and populate its uri
        for (final ProcessGroupEntity processGroupEntity : flowStructure.getProcessGroups()) {
            final ProcessGroupDTO processGroup = processGroupEntity.getComponent();
            if (processGroup != null) {
                processGroup.setContents(null);
            }
        }
    }

    /**
     * Authorizes access to the flow.
     */
    private void authorizeFlow() {
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable flow = lookup.getFlow();
            flow.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });
    }

    // ----
    // flow
    // ----

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("client-id")
    @Operation(
            summary = "Generates a client id.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response generateClientId() {
        authorizeFlow();
        return generateOkResponse(generateUuid()).build();
    }

    /**
     * Retrieves the configuration for the flow.
     *
     * @return A flowConfigurationEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    @Operation(
            summary = "Retrieves the configuration for this NiFi flow",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowConfigurationEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getFlowConfig() {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final FlowConfigurationEntity entity = serviceFacade.getFlowConfiguration();
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the registered content viewers.
     *
     * @return A contentViewerEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("content-viewers")
    @Operation(
            summary = "Retrieves the registered content viewers",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ContentViewerEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getContentViewers(@Context final HttpServletRequest httpServletRequest) {
        authorizeFlow();

        final Collection<ContentViewer> contentViewers = (Collection<ContentViewer>) servletContext.getAttribute("content-viewers");

        final List<ContentViewerDTO> dtos = new ArrayList<>();
        contentViewers.forEach((contentViewer) -> {
            final String contextPath = contentViewer.getContextPath();
            final BundleCoordinate bundleCoordinate = contentViewer.getBundle().getBundleDetails().getCoordinate();

            final String displayName = StringUtils.substringBefore(contextPath.substring(1), "-" + bundleCoordinate.getVersion());

            final ContentViewerDTO dto = new ContentViewerDTO();
            dto.setDisplayName(displayName + " " + bundleCoordinate.getVersion());

            final List<SupportedMimeTypesDTO> supportedMimeTypes = contentViewer.getSupportedMimeTypes().stream().map((supportedMimeType -> {
                final SupportedMimeTypesDTO mimeTypesDto = new SupportedMimeTypesDTO();
                mimeTypesDto.setDisplayName(supportedMimeType.getDisplayName());
                mimeTypesDto.setMimeTypes(supportedMimeType.getMimeTypes());
                return mimeTypesDto;
            })).collect(Collectors.toList());
            dto.setSupportedMimeTypes(supportedMimeTypes);

            final URI contentViewerUri = RequestUriBuilder.fromHttpServletRequest(httpServletRequest).path(contextPath).build();
            dto.setUri(contentViewerUri.toString());

            dtos.add(dto);
        });

        final ContentViewerEntity entity = new ContentViewerEntity();
        entity.setContentViewers(dtos);

        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the identity of the user making the request.
     *
     * @return An identityEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("current-user")
    @Operation(
            summary = "Retrieves the user identity of the user making the request",
            responses = @ApiResponse(content = @Content(schema = @Schema(implementation = CurrentUserEntity.class))),
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getCurrentUser() {

        authorizeFlow();

        final CurrentUserEntity entity;
        if (isReplicateRequest()) {
            try (Response replicatedResponse = replicate(HttpMethod.GET)) {
                final CurrentUserEntity replicatedCurrentUserEntity = readReplicatedCurrentUserEntity(replicatedResponse);
                final CurrentUserEntity currentUserEntity = serviceFacade.getCurrentUser();
                // Set Logout Supported based on local client information instead of replicated and merged responses
                replicatedCurrentUserEntity.setLogoutSupported(currentUserEntity.isLogoutSupported());
                entity = replicatedCurrentUserEntity;
            }
        } else {
            entity = serviceFacade.getCurrentUser();
        }

        return generateOkResponse(entity).build();
    }

    private CurrentUserEntity readReplicatedCurrentUserEntity(final Response replicatedResponse) {
        final Object entity = replicatedResponse.getEntity();
        if (entity instanceof CurrentUserEntity replicatedCurrentUserEntity) {
            return replicatedCurrentUserEntity;
        } else if (entity instanceof StreamingOutput streamingOutput) {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                streamingOutput.write(outputStream);

                final byte[] bytes = outputStream.toByteArray();
                return objectMapper.readValue(bytes, CurrentUserEntity.class);
            } catch (final IOException e) {
                throw new UncheckedIOException("Read Current User Entity failed", e);
            }
        } else {
            throw new IllegalStateException("Current User Entity not expected [%s]".formatted(entity));
        }
    }

    /**
     * Retrieves the contents of the specified group.
     *
     * @param groupId The id of the process group.
     * @return A processGroupEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @Operation(
            summary = "Gets a process group",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessGroupFlowEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            },
            description = "If the uiOnly query parameter is provided with a value of true, the returned entity may only contain fields that are necessary for rendering the NiFi User Interface. As " +
                    "such, " +
                    "the selected fields may change at any time, even during incremental releases, without warning. As a result, this parameter should not be provided by any client other than the UI."
    )
    public Response getFlow(
            @Parameter(
                    description = "The process group id."
            )
            @PathParam("id") final String groupId,
            @QueryParam("uiOnly") @DefaultValue("false") final boolean uiOnly) {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get this process group flow
        final ProcessGroupFlowEntity entity = serviceFacade.getProcessGroupFlow(groupId, uiOnly);
        populateRemainingFlowContent(entity.getProcessGroupFlow());
        return generateOkResponse(entity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}/breadcrumbs")
    @Operation(
            summary = "Gets the breadcrumbs for a process group",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowBreadcrumbEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getBreadcrumbs(
            @Parameter(description = "The process group id.")
            @PathParam("id") final String groupId) {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the breadcrumbs for this process group
        final FlowBreadcrumbEntity breadcrumbEntity = serviceFacade.getProcessGroupBreadcrumbs(groupId);
        return generateOkResponse(breadcrumbEntity).build();
    }

    /**
     * Retrieves the metrics of the entire flow.
     *
     * @return A flowMetricsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("metrics/{producer}")
    @Operation(
            summary = "Gets all metrics for the flow from a particular node",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = StreamingOutput.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getFlowMetrics(
            @Parameter(
                    description = "The producer for flow file metrics. Each producer may have its own output format.",
                    required = true,
                    schema = @Schema(allowableValues = {"prometheus", "json"})
            )
            @PathParam("producer") final String producer,
            @Parameter(
                    description = "Set of included metrics registries. Duplicate the parameter to include multiple registries. " +
                            "All registries are included by default.",

                    schema = @Schema(allowableValues = {"NIFI", "JVM", "BULLETIN", "CONNECTION", "CLUSTER"})
            )
            @QueryParam("includedRegistries") final Set<FlowMetricsRegistry> includedRegistries,
            @Parameter(
                    description = "Regular Expression Pattern to be applied against the sample name field"
            )
            @QueryParam("sampleName") final String sampleName,
            @Parameter(
                    description = "Regular Expression Pattern to be applied against the sample label value field"
            )
            @QueryParam("sampleLabelValue") final String sampleLabelValue,
            @Parameter(
                    description = "Name of the first field of JSON object. Applicable for JSON producer only."
            )
            @QueryParam("rootFieldName") final String rootFieldName
    ) {

        authorizeFlow();

        final Set<FlowMetricsRegistry> selectedRegistries = includedRegistries == null ? Collections.emptySet() : includedRegistries;
        final Collection<CollectorRegistry> registries = serviceFacade.generateFlowMetrics(selectedRegistries);

        if (FlowMetricsProducer.PROMETHEUS.getProducer().equalsIgnoreCase(producer)) {
            final StreamingOutput response = (outputStream -> {
                final PrometheusMetricsWriter prometheusMetricsWriter = new TextFormatPrometheusMetricsWriter(sampleName, sampleLabelValue);
                prometheusMetricsWriter.write(registries, outputStream);
            });
            return generateOkResponse(response).type(TextFormat.CONTENT_TYPE_004).build();

        } else if (FlowMetricsProducer.JSON.getProducer().equals(producer)) {
            final StreamingOutput output = outputStream -> {
                final JsonFormatPrometheusMetricsWriter jsonPrometheusMetricsWriter = new JsonFormatPrometheusMetricsWriter(sampleName, sampleLabelValue, rootFieldName);
                jsonPrometheusMetricsWriter.write(registries, outputStream);
            };
            return generateOkResponse(output)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .build();

        } else {
            throw new ResourceNotFoundException("The specified producer is missing or invalid.");
        }
    }

    // -------------------
    // controller services
    // -------------------

    /**
     * Retrieves controller services for reporting tasks in this NiFi.
     *
     * @return A controllerServicesEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("controller/controller-services")
    @Operation(
            summary = "Gets controller services for reporting tasks",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerServicesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            },
            description = "If the uiOnly query parameter is provided with a value of true, the returned entity may only contain fields that are necessary for rendering the NiFi User Interface. As " +
             "such, " +
                    "the selected fields may change at any time, even during incremental releases, without warning. As a result, this parameter should not be provided by any client other than the UI."
    )
    public Response getControllerServicesFromController(@QueryParam("uiOnly") @DefaultValue("false") final boolean uiOnly,
                                                        @QueryParam("includeReferencingComponents") @DefaultValue("true")
                                                        @Parameter(description = "Whether or not to include services' referencing components in the response") boolean includeReferences) {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get all the controller services
        final Set<ControllerServiceEntity> controllerServices = serviceFacade.getControllerServices(null, false, false, includeReferences);
        if (uiOnly) {
            controllerServices.forEach(this::stripNonUiRelevantFields);
        }

        controllerServiceResource.populateRemainingControllerServiceEntitiesContent(controllerServices);

        // create the response entity
        final ControllerServicesEntity entity = new ControllerServicesEntity();
        entity.setCurrentTime(new Date());
        entity.setControllerServices(controllerServices);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves all the of controller services in this NiFi.
     *
     * @return A controllerServicesEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}/controller-services")
    @Operation(
            summary = "Gets all controller services",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerServicesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            },
            description = "If the uiOnly query parameter is provided with a value of true, the returned entity may only contain fields that are necessary for rendering the NiFi User Interface. As " +
             "such, " +
                    "the selected fields may change at any time, even during incremental releases, without warning. As a result, this parameter should not be provided by any client other than the UI."
    )
    public Response getControllerServicesFromGroup(
            @Parameter(description = "The process group id.", required = true)
            @PathParam("id") final String groupId,
            @Parameter(description = "Whether or not to include parent/ancestor process groups")
            @QueryParam("includeAncestorGroups")
            @DefaultValue("true") final boolean includeAncestorGroups,
            @Parameter(description = "Whether or not to include descendant process groups")
            @QueryParam("includeDescendantGroups")
            @DefaultValue("false") final boolean includeDescendantGroups,
            @Parameter(description = "Whether or not to include services' referencing components in the response")
            @QueryParam("includeReferencingComponents")
            @DefaultValue("true") final boolean includeReferences,
            @QueryParam("uiOnly")
            @DefaultValue("false") final boolean uiOnly) {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get all the controller services
        final Set<ControllerServiceEntity> controllerServices = serviceFacade.getControllerServices(groupId, includeAncestorGroups, includeDescendantGroups, includeReferences);
        if (uiOnly) {
            controllerServices.forEach(this::stripNonUiRelevantFields);
        }
        controllerServiceResource.populateRemainingControllerServiceEntitiesContent(controllerServices);

        // create the response entity
        final ControllerServicesEntity entity = new ControllerServicesEntity();
        entity.setCurrentTime(new Date());
        entity.setControllerServices(controllerServices);

        // generate the response
        return generateOkResponse(entity).build();
    }


    // ---------------
    // parameter-providers
    // ---------------

    /**
     * Retrieves all the of parameter providers in this NiFi.
     *
     * @return A parameterProvidersEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("parameter-providers")
    @Operation(
            summary = "Gets all parameter providers",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterProvidersEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getParameterProviders() {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get all the parameter providers
        final Set<ParameterProviderEntity> parameterProviders = serviceFacade.getParameterProviders();
        parameterProviderResource.populateRemainingParameterProviderEntitiesContent(parameterProviders);

        // create the response entity
        final ParameterProvidersEntity entity = new ParameterProvidersEntity();
        entity.setParameterProviders(parameterProviders);
        entity.setCurrentTime(new Date());

        // generate the response
        return generateOkResponse(entity).build();
    }

    // ---------------
    // reporting-tasks
    // ---------------

    /**
     * Retrieves all the of reporting tasks in this NiFi.
     *
     * @return A reportingTasksEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-tasks")
    @Operation(
            summary = "Gets all reporting tasks",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ReportingTasksEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getReportingTasks() {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get all the reporting tasks
        final Set<ReportingTaskEntity> reportingTasks = serviceFacade.getReportingTasks();
        reportingTaskResource.populateRemainingReportingTaskEntitiesContent(reportingTasks);

        // create the response entity
        final ReportingTasksEntity entity = new ReportingTasksEntity();
        entity.setCurrentTime(new Date());
        entity.setReportingTasks(reportingTasks);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets a snapshot of reporting tasks.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-tasks/snapshot")
    @Operation(
            summary = "Get a snapshot of the given reporting tasks and any controller services they use",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedReportingTaskSnapshot.class))),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getReportingTaskSnapshot(
            @Parameter(description = "Specifies a reporting task id to export. If not specified, all reporting tasks will be exported.")
            @QueryParam("reportingTaskId") final String reportingTaskId
    ) {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final VersionedReportingTaskSnapshot snapshot = reportingTaskId == null
                ? serviceFacade.getVersionedReportingTaskSnapshot() :
                serviceFacade.getVersionedReportingTaskSnapshot(reportingTaskId);

        return generateOkResponse(snapshot).build();
    }

    /**
     * Downloads a snapshot of reporting tasks.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-tasks/download")
    @Operation(
            summary = "Download a snapshot of the given reporting tasks and any controller services they use",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = byte[].class))),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response downloadReportingTaskSnapshot(
            @Parameter(description = "Specifies a reporting task id to export. If not specified, all reporting tasks will be exported.")
            @QueryParam("reportingTaskId") final String reportingTaskId
    ) {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final VersionedReportingTaskSnapshot snapshot = reportingTaskId == null
                ? serviceFacade.getVersionedReportingTaskSnapshot() :
                serviceFacade.getVersionedReportingTaskSnapshot(reportingTaskId);

        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(VERSIONED_REPORTING_TASK_SNAPSHOT_DATE_FORMAT);
        final String filename = VERSIONED_REPORTING_TASK_SNAPSHOT_FILENAME_PATTERN.formatted(formatter.format(OffsetDateTime.now()));
        return generateOkResponse(snapshot).header(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", filename)).build();
    }

    /**
     * Updates the specified process group.
     *
     * @param id The id of the process group.
     * @param requestScheduleComponentsEntity A scheduleComponentsEntity.
     * @return A processGroupEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @Operation(
            summary = "Schedule or unschedule components in the specified Process Group.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ScheduleComponentsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow"),
                    @SecurityRequirement(name = "Write - /{component-type}/{uuid} or /operation/{component-type}/{uuid} - For every component being scheduled/unscheduled")
            }
    )
    public Response scheduleComponents(
            @Parameter(
                    description = "The process group id.",
                    required = true
            )
            @PathParam("id") String id,
            @Parameter(
                    description = "The request to schedule or unschedule. If the components in the request are not specified, all authorized components will be considered.",
                    required = true
            ) final ScheduleComponentsEntity requestScheduleComponentsEntity) {

        if (requestScheduleComponentsEntity == null) {
            throw new IllegalArgumentException("Schedule Component must be specified.");
        }

        // ensure the same id is being used
        if (!id.equals(requestScheduleComponentsEntity.getId())) {
            throw new IllegalArgumentException(String.format("The process group id (%s) in the request body does "
                    + "not equal the process group id of the requested resource (%s).", requestScheduleComponentsEntity.getId(), id));
        }

        final ScheduledState state;
        if (requestScheduleComponentsEntity.getState() == null) {
            throw new IllegalArgumentException("The scheduled state must be specified.");
        } else {
            if (requestScheduleComponentsEntity.getState().equals(STATE_ENABLED)) {
                state = ScheduledState.STOPPED;
            } else {
                try {
                    state = ScheduledState.valueOf(requestScheduleComponentsEntity.getState());
                } catch (final IllegalArgumentException iae) {
                    throw new IllegalArgumentException(String.format("The scheduled must be one of [%s].",
                            StringUtils.join(Stream.of(ScheduledState.RUNNING, ScheduledState.STOPPED, STATE_ENABLED, ScheduledState.DISABLED), ", ")));
                }
            }
        }

        // ensure its a supported scheduled state
        if (ScheduledState.STARTING.equals(state) || ScheduledState.STOPPING.equals(state)) {
            throw new IllegalArgumentException(String.format("The scheduled must be one of [%s].",
                    StringUtils.join(Stream.of(ScheduledState.RUNNING, ScheduledState.STOPPED, STATE_ENABLED, ScheduledState.DISABLED), ", ")));
        }

        // if the components are not specified, gather all components and their current revision
        if (requestScheduleComponentsEntity.getComponents() == null) {
            final Supplier<Predicate<ProcessorNode>> getProcessorFilter = () -> {
                if (ScheduledState.RUNNING.equals(state)) {
                    return ProcessGroup.START_PROCESSORS_FILTER;
                } else if (ScheduledState.STOPPED.equals(state)) {
                    if (requestScheduleComponentsEntity.getState().equals(STATE_ENABLED)) {
                        return ProcessGroup.ENABLE_PROCESSORS_FILTER;
                    } else {
                        return ProcessGroup.STOP_PROCESSORS_FILTER;
                    }
                } else {
                    return ProcessGroup.DISABLE_PROCESSORS_FILTER;
                }
            };

            final Supplier<Predicate<Port>> getPortFilter = () -> {
                if (ScheduledState.RUNNING.equals(state)) {
                    return ProcessGroup.START_PORTS_FILTER;
                } else if (ScheduledState.STOPPED.equals(state)) {
                    if (requestScheduleComponentsEntity.getState().equals(STATE_ENABLED)) {
                        return ProcessGroup.ENABLE_PORTS_FILTER;
                    } else {
                        return ProcessGroup.STOP_PORTS_FILTER;
                    }
                } else {
                    return ProcessGroup.DISABLE_PORTS_FILTER;
                }
            };

            // get the current revisions for the components being updated
            final Set<Revision> revisions = serviceFacade.getRevisionsFromGroup(id, group -> {
                final Set<String> componentIds = new HashSet<>();
                final Set<String> statelessGroupIdsToSchedule = new HashSet<>();

                // Any group that is configured to run as stateless whose parent is not stateless we will attempt to schedule
                group.findAllProcessGroups().stream()
                        .filter(child -> child.getExecutionEngine() == ExecutionEngine.STATELESS)
                        .filter(child -> child.getParent() == null || child.getParent().resolveExecutionEngine() == ExecutionEngine.STANDARD)
                        .filter(child -> OperationAuthorizable.isOperationAuthorized(child, authorizer, NiFiUserUtils.getNiFiUser()))
                        .forEach(child -> {
                            componentIds.add(child.getIdentifier());
                            statelessGroupIdsToSchedule.add(child.getIdentifier());
                            child.findAllProcessGroups().forEach(descendent -> statelessGroupIdsToSchedule.add(descendent.getIdentifier()));
                        });

                // ensure authorized for each processor we will attempt to schedule
                group.findAllProcessors().stream()
                        .filter(getProcessorFilter.get())
                        .filter(processor -> !statelessGroupIdsToSchedule.contains(processor.getProcessGroupIdentifier()))
                        .filter(processor -> OperationAuthorizable.isOperationAuthorized(processor, authorizer, NiFiUserUtils.getNiFiUser()))
                        .forEach(processor -> componentIds.add(processor.getIdentifier()));

                // ensure authorized for each input port we will attempt to schedule
                group.findAllInputPorts().stream()
                        .filter(getPortFilter.get())
                        .filter(processor -> !statelessGroupIdsToSchedule.contains(processor.getProcessGroupIdentifier()))
                        .filter(inputPort -> OperationAuthorizable.isOperationAuthorized(inputPort, authorizer, NiFiUserUtils.getNiFiUser()))
                        .forEach(inputPort -> componentIds.add(inputPort.getIdentifier()));

                // ensure authorized for each output port we will attempt to schedule
                group.findAllOutputPorts().stream()
                        .filter(getPortFilter.get())
                        .filter(processor -> !statelessGroupIdsToSchedule.contains(processor.getProcessGroupIdentifier()))
                        .filter(outputPort -> OperationAuthorizable.isOperationAuthorized(outputPort, authorizer, NiFiUserUtils.getNiFiUser()))
                        .forEach(outputPort -> componentIds.add(outputPort.getIdentifier()));

                return componentIds;
            });

            // build the component mapping
            final Map<String, RevisionDTO> componentsToSchedule = new HashMap<>();
            revisions.forEach(revision -> {
                final RevisionDTO dto = new RevisionDTO();
                dto.setClientId(revision.getClientId());
                dto.setVersion(revision.getVersion());
                componentsToSchedule.put(revision.getComponentId(), dto);
            });

            // set the components and their current revision
            requestScheduleComponentsEntity.setComponents(componentsToSchedule);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestScheduleComponentsEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestScheduleComponentsEntity.isDisconnectedNodeAcknowledged());
        }

        final Map<String, RevisionDTO> requestComponentsToSchedule = requestScheduleComponentsEntity.getComponents();
        final Map<String, Revision> requestComponentRevisions =
                requestComponentsToSchedule.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> getRevision(e.getValue(), e.getKey())));
        final Set<Revision> requestRevisions = new HashSet<>(requestComponentRevisions.values());

        return withWriteLock(
                serviceFacade,
                requestScheduleComponentsEntity,
                requestRevisions,
                lookup -> {
                    // ensure access to the flow
                    authorizeFlow();

                    // ensure access to every component being scheduled
                    requestComponentsToSchedule.keySet().forEach(componentId -> {
                        final Authorizable connectable = lookup.getLocalConnectable(componentId);
                        OperationAuthorizable.authorizeOperation(connectable, authorizer, NiFiUserUtils.getNiFiUser());
                    });
                },
                () -> {
                    if (STATE_ENABLED.equals(requestScheduleComponentsEntity.getState()) || STATE_DISABLED.equals(requestScheduleComponentsEntity.getState())) {
                        serviceFacade.verifyEnableComponents(id, state, requestComponentRevisions.keySet());
                    } else {
                        serviceFacade.verifyScheduleComponents(id, state, requestComponentRevisions.keySet());
                    }
                },
                (revisions, scheduleComponentsEntity) -> {

                    final ScheduledState scheduledState;
                    if (STATE_ENABLED.equals(scheduleComponentsEntity.getState())) {
                        scheduledState = ScheduledState.STOPPED;
                    } else {
                        scheduledState = ScheduledState.valueOf(scheduleComponentsEntity.getState());
                    }

                    final Map<String, RevisionDTO> componentsToSchedule = scheduleComponentsEntity.getComponents();
                    final Map<String, Revision> componentRevisions =
                            componentsToSchedule.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> getRevision(e.getValue(), e.getKey())));

                    // update the process group
                    final ScheduleComponentsEntity entity;
                    if (STATE_ENABLED.equals(scheduleComponentsEntity.getState()) || STATE_DISABLED.equals(scheduleComponentsEntity.getState())) {
                        entity = serviceFacade.enableComponents(id, scheduledState, componentRevisions);
                    } else {
                        entity = serviceFacade.scheduleComponents(id, scheduledState, componentRevisions);
                    }

                    return generateOkResponse(entity).build();
                }
        );
    }


    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}/controller-services")
    @Operation(
            summary = "Enable or disable Controller Services in the specified Process Group.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ActivateControllerServicesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow"),
                    @SecurityRequirement(name = "Write - /{component-type}/{uuid} or /operation/{component-type}/{uuid} - For every service being enabled/disabled")
            }
    )
    public Response activateControllerServices(
            @Parameter(description = "The process group id.", required = true)
            @PathParam("id") String id,
            @Parameter(description = "The request to schedule or unschedule. If the comopnents in the request are not specified, all authorized components will be considered.", required = true)
            final ActivateControllerServicesEntity requestEntity) {

        if (requestEntity == null) {
            throw new IllegalArgumentException("Controller service must be specified.");
        }

        // ensure the same id is being used
        if (!id.equals(requestEntity.getId())) {
            throw new IllegalArgumentException(String.format("The process group id (%s) in the request body does "
                    + "not equal the process group id of the requested resource (%s).", requestEntity.getId(), id));
        }

        final ControllerServiceState desiredState;
        if (requestEntity.getState() == null) {
            throw new IllegalArgumentException("The controller service state must be specified.");
        } else {
            try {
                desiredState = ControllerServiceState.valueOf(requestEntity.getState());
            } catch (final IllegalArgumentException iae) {
                throw new IllegalArgumentException(String.format("The controller service state must be one of [%s].",
                        StringUtils.join(EnumSet.of(ControllerServiceState.ENABLED, ControllerServiceState.DISABLED), ", ")));
            }
        }

        // ensure its a supported scheduled state
        if (ControllerServiceState.DISABLING.equals(desiredState) || ControllerServiceState.ENABLING.equals(desiredState)) {
            throw new IllegalArgumentException(String.format("The scheduled must be one of [%s].",
                    StringUtils.join(EnumSet.of(ControllerServiceState.ENABLED, ControllerServiceState.DISABLED), ", ")));
        }

        // if the components are not specified, gather all components and their current revision
        if (requestEntity.getComponents() == null) {
            // get the current revisions for the components being updated
            final Set<Revision> revisions = serviceFacade.getRevisionsFromGroup(id, group -> {
                final Set<String> componentIds = new HashSet<>();

                final Predicate<ControllerServiceNode> filter;
                if (ControllerServiceState.ENABLED.equals(desiredState)) {
                    filter = service -> !service.isActive();
                } else {
                    filter = ControllerServiceNode::isActive;
                }

                group.findAllControllerServices().stream()
                        .filter(filter)
                        .filter(service -> service.getProcessGroup().resolveExecutionEngine() == ExecutionEngine.STANDARD)
                        .filter(service -> OperationAuthorizable.isOperationAuthorized(service, authorizer, NiFiUserUtils.getNiFiUser()))
                        .forEach(service -> componentIds.add(service.getIdentifier()));
                return componentIds;
            });

            // build the component mapping
            final Map<String, RevisionDTO> componentsToSchedule = new HashMap<>();
            revisions.forEach(revision -> {
                final RevisionDTO dto = new RevisionDTO();
                dto.setClientId(revision.getClientId());
                dto.setVersion(revision.getVersion());
                componentsToSchedule.put(revision.getComponentId(), dto);
            });

            // set the components and their current revision
            requestEntity.setComponents(componentsToSchedule);
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        final Map<String, RevisionDTO> requestComponentsToSchedule = requestEntity.getComponents();
        final Map<String, Revision> requestComponentRevisions =
                requestComponentsToSchedule.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> getRevision(e.getValue(), e.getKey())));
        final Set<Revision> requestRevisions = new HashSet<>(requestComponentRevisions.values());

        return withWriteLock(
                serviceFacade,
                requestEntity,
                requestRevisions,
                lookup -> {
                    // ensure access to the flow
                    authorizeFlow();

                    // ensure access to every component being scheduled
                    requestComponentsToSchedule.keySet().forEach(componentId -> {
                        final Authorizable authorizable = lookup.getControllerService(componentId).getAuthorizable();
                        OperationAuthorizable.authorizeOperation(authorizable, authorizer, NiFiUserUtils.getNiFiUser());
                    });
                },
                () -> serviceFacade.verifyActivateControllerServices(id, desiredState, requestComponentRevisions.keySet()),
                (revisions, scheduleComponentsEntity) -> {
                    final ControllerServiceState serviceState = ControllerServiceState.valueOf(scheduleComponentsEntity.getState());

                    final Map<String, RevisionDTO> componentsToSchedule = scheduleComponentsEntity.getComponents();
                    final Map<String, Revision> componentRevisions =
                            componentsToSchedule.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> getRevision(e.getValue(), e.getKey())));

                    // update the controller services
                    final ActivateControllerServicesEntity entity = serviceFacade.activateControllerServices(id, serviceState, componentRevisions);
                    return generateOkResponse(entity).build();
                }
        );
    }

    // ------
    // search
    // ------

    /**
     * Performs a search request in this flow.
     *
     * @param value Search string
     * @return A searchResultsEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("search-results")
    @Operation(
            summary = "Performs a search against this NiFi using the specified search term",
            description = "Only search results from authorized components will be returned.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = SearchResultsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response searchFlow(
            @QueryParam("q") @DefaultValue(StringUtils.EMPTY) String value,
            @QueryParam("a") @DefaultValue(StringUtils.EMPTY) String activeGroupId
    ) {
        authorizeFlow();

        // query the controller
        final SearchResultsDTO results = serviceFacade.searchController(value, activeGroupId);

        // create the entity
        final SearchResultsEntity entity = new SearchResultsEntity();
        entity.setSearchResultsDTO(results);

        // generate the response
        return noCache(Response.ok(entity)).build();
    }

    /**
     * Retrieves the status for this NiFi.
     *
     * @return A controllerStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("status")
    @Operation(
            summary = "Gets the current status of this NiFi",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerStatusEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getControllerStatus() throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final ControllerStatusDTO controllerStatus = serviceFacade.getControllerStatus();

        // create the response entity
        final ControllerStatusEntity entity = new ControllerStatusEntity();
        entity.setControllerStatus(controllerStatus);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the cluster summary for this NiFi.
     *
     * @return A clusterSummaryEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/summary")
    @Operation(
            summary = "The cluster summary for this NiFi",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ClusterSummaryEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getClusterSummary() {

        authorizeFlow();

        final ClusterSummaryDTO clusterConfiguration = new ClusterSummaryDTO();
        final ClusterCoordinator clusterCoordinator = getClusterCoordinator();

        if (clusterCoordinator != null && clusterCoordinator.isConnected()) {
            final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = clusterCoordinator.getConnectionStates();
            int totalNodeCount = 0;
            for (final List<NodeIdentifier> nodeList : stateMap.values()) {
                totalNodeCount += nodeList.size();
            }
            final List<NodeIdentifier> connectedNodeIds = stateMap.get(NodeConnectionState.CONNECTED);
            final int connectedNodeCount = (connectedNodeIds == null) ? 0 : connectedNodeIds.size();

            clusterConfiguration.setConnectedNodeCount(connectedNodeCount);
            clusterConfiguration.setTotalNodeCount(totalNodeCount);
            clusterConfiguration.setConnectedNodes(connectedNodeCount + " / " + totalNodeCount);
        }

        clusterConfiguration.setClustered(isClustered());
        clusterConfiguration.setConnectedToCluster(isConnectedToCluster());

        // create the response entity
        final ClusterSummaryEntity entity = new ClusterSummaryEntity();
        entity.setClusterSummary(clusterConfiguration);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the controller level bulletins.
     *
     * @return A controllerBulletinsEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("controller/bulletins")
    @Operation(
            summary = "Retrieves Controller level bulletins",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerBulletinsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow"),
                    @SecurityRequirement(name = "Read - /controller - For controller bulletins"),
                    @SecurityRequirement(name = "Read - /controller-services/{uuid} - For controller service bulletins"),
                    @SecurityRequirement(name = "Read - /reporting-tasks/{uuid} - For reporting task bulletins")
            }
    )
    public Response getBulletins() {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final ControllerBulletinsEntity entity = serviceFacade.getControllerBulletins();
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the banners for this NiFi.
     *
     * @return A bannerEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("banners")
    @Operation(
            summary = "Retrieves the banners for this NiFi",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = BannerEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getBanners() {

        authorizeFlow();

        // get the banner from the properties
        final String bannerText = getProperties().getBannerText();

        // create the DTO
        final BannerDTO bannerDTO = new BannerDTO();
        bannerDTO.setHeaderText(bannerText);
        bannerDTO.setFooterText(bannerText);

        // create the response entity
        final BannerEntity entity = new BannerEntity();
        entity.setBanners(bannerDTO);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the types of processors that this NiFi supports.
     *
     * @return A processorTypesEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("processor-types")
    @Operation(
            summary = "Retrieves the types of processors that this NiFi supports",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessorTypesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getProcessorTypes(
            @Parameter(
                    description = "If specified, will only return types that are a member of this bundle group."
            )
            @QueryParam("bundleGroupFilter") String bundleGroupFilter,
            @Parameter(
                    description = "If specified, will only return types that are a member of this bundle artifact."
            )
            @QueryParam("bundleArtifactFilter") String bundleArtifactFilter,
            @Parameter(
                    description = "If specified, will only return types whose fully qualified classname matches."
            )
            @QueryParam("type") String typeFilter) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final ProcessorTypesEntity entity = new ProcessorTypesEntity();
        entity.setProcessorTypes(serviceFacade.getProcessorTypes(bundleGroupFilter, bundleArtifactFilter, typeFilter));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the types of controller services that this NiFi supports.
     *
     * @param serviceType Returns only services that implement this type
     * @return A controllerServicesTypesEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("controller-service-types")
    @Operation(
            summary = "Retrieves the types of controller services that this NiFi supports",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerServiceTypesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getControllerServiceTypes(
            @Parameter(
                    description = "If specified, will only return controller services that are compatible with this type of service."
            )
            @QueryParam("serviceType") String serviceType,
            @Parameter(
                    description = "If serviceType specified, is the bundle group of the serviceType."
            )
            @QueryParam("serviceBundleGroup") String serviceBundleGroup,
            @Parameter(
                    description = "If serviceType specified, is the bundle artifact of the serviceType."
            )
            @QueryParam("serviceBundleArtifact") String serviceBundleArtifact,
            @Parameter(
                    description = "If serviceType specified, is the bundle version of the serviceType."
            )
            @QueryParam("serviceBundleVersion") String serviceBundleVersion,
            @Parameter(
                    description = "If specified, will only return types that are a member of this bundle group."
            )
            @QueryParam("bundleGroupFilter") String bundleGroupFilter,
            @Parameter(
                    description = "If specified, will only return types that are a member of this bundle artifact."
            )
            @QueryParam("bundleArtifactFilter") String bundleArtifactFilter,
            @Parameter(
                    description = "If specified, will only return types whose fully qualified classname matches."
            )
            @QueryParam("typeFilter") String typeFilter) throws InterruptedException {

        authorizeFlow();

        if (serviceType != null) {
            if (serviceBundleGroup == null || serviceBundleArtifact == null || serviceBundleVersion == null) {
                throw new IllegalArgumentException("When specifying the serviceType the serviceBundleGroup, serviceBundleArtifact, and serviceBundleVersion must be specified.");
            }
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final ControllerServiceTypesEntity entity = new ControllerServiceTypesEntity();
        entity.setControllerServiceTypes(serviceFacade.getControllerServiceTypes(serviceType, serviceBundleGroup, serviceBundleArtifact, serviceBundleVersion,
                bundleGroupFilter, bundleArtifactFilter, typeFilter));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the types of reporting tasks that this NiFi supports.
     *
     * @return A ReportingTaskTypesEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-task-types")
    @Operation(
            summary = "Retrieves the types of reporting tasks that this NiFi supports",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ReportingTaskTypesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getReportingTaskTypes(
            @Parameter(
                    description = "If specified, will only return types that are a member of this bundle group."
            )
            @QueryParam("bundleGroupFilter") String bundleGroupFilter,
            @Parameter(
                    description = "If specified, will only return types that are a member of this bundle artifact."
            )
            @QueryParam("bundleArtifactFilter") String bundleArtifactFilter,
            @Parameter(
                    description = "If specified, will only return types whose fully qualified classname matches."
            )
            @QueryParam("type") String typeFilter) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final ReportingTaskTypesEntity entity = new ReportingTaskTypesEntity();
        entity.setReportingTaskTypes(serviceFacade.getReportingTaskTypes(bundleGroupFilter, bundleArtifactFilter, typeFilter));

        // generate the response
        return generateOkResponse(entity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("runtime-manifest")
    @Operation(
            summary = "Retrieves the runtime manifest for this NiFi instance.",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RuntimeManifestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getRuntimeManifest() throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final RuntimeManifestEntity entity = new RuntimeManifestEntity();
        entity.setRuntimeManifest(serviceFacade.getRuntimeManifest());

        // generate the response
        return generateOkResponse(entity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("processor-definition/{group}/{artifact}/{version}/{type}")
    @Operation(
            summary = "Retrieves the Processor Definition for the specified component type.",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessorDefinition.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The processor definition for the coordinates could not be located.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getProcessorDefinition(
            @Parameter(
                    description = "The bundle group",
                    required = true
            )
            @PathParam("group") String group,
            @Parameter(
                    description = "The bundle artifact",
                    required = true
            )
            @PathParam("artifact") String artifact,
            @Parameter(
                    description = "The bundle version",
                    required = true
            )
            @PathParam("version") String version,
            @Parameter(
                    description = "The processor type",
                    required = true
            )
            @PathParam("type") String type
    ) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final ProcessorDefinition entity = serviceFacade.getProcessorDefinition(group, artifact, version, type);

        // generate the response
        return generateOkResponse(entity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("controller-service-definition/{group}/{artifact}/{version}/{type}")
    @Operation(
            summary = "Retrieves the Controller Service Definition for the specified component type.",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerServiceDefinition.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The controller service definition for the coordinates could not be located.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getControllerServiceDefinition(
            @Parameter(
                    description = "The bundle group",
                    required = true
            )
            @PathParam("group") String group,
            @Parameter(
                    description = "The bundle artifact",
                    required = true
            )
            @PathParam("artifact") String artifact,
            @Parameter(
                    description = "The bundle version",
                    required = true
            )
            @PathParam("version") String version,
            @Parameter(
                    description = "The controller service type",
                    required = true
            )
            @PathParam("type") String type
    ) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final ControllerServiceDefinition entity = serviceFacade.getControllerServiceDefinition(group, artifact, version, type);

        // generate the response
        return generateOkResponse(entity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-task-definition/{group}/{artifact}/{version}/{type}")
    @Operation(
            summary = "Retrieves the Reporting Task Definition for the specified component type.",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ReportingTaskDefinition.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The reporting task definition for the coordinates could not be located.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getReportingTaskDefinition(
            @Parameter(
                    description = "The bundle group",
                    required = true
            )
            @PathParam("group") String group,
            @Parameter(
                    description = "The bundle artifact",
                    required = true
            )
            @PathParam("artifact") String artifact,
            @Parameter(
                    description = "The bundle version",
                    required = true
            )
            @PathParam("version") String version,
            @Parameter(
                    description = "The reporting task type",
                    required = true
            )
            @PathParam("type") String type
    ) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final ReportingTaskDefinition entity = serviceFacade.getReportingTaskDefinition(group, artifact, version, type);

        // generate the response
        return generateOkResponse(entity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("parameter-provider-definition/{group}/{artifact}/{version}/{type}")
    @Operation(
            summary = "Retrieves the Parameter Provider Definition for the specified component type.",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterProviderDefinition.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The reporting task definition for the coordinates could not be located.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getParameterProviderDefinition(
            @Parameter(
                    description = "The bundle group",
                    required = true
            )
            @PathParam("group") String group,
            @Parameter(
                    description = "The bundle artifact",
                    required = true
            )
            @PathParam("artifact") String artifact,
            @Parameter(
                    description = "The bundle version",
                    required = true
            )
            @PathParam("version") String version,
            @Parameter(
                    description = "The parameter provider type",
                    required = true
            )
            @PathParam("type") String type
    ) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final ParameterProviderDefinition entity = serviceFacade.getParameterProviderDefinition(group, artifact, version, type);

        // generate the response
        return generateOkResponse(entity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rule-definition/{group}/{artifact}/{version}/{type}")
    @Operation(
            summary = "Retrieves the Flow Analysis Rule Definition for the specified component type.",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowAnalysisRuleDefinition.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The flow analysis rule definition for the coordinates could not be located.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getFlowAnalysisRuleDefinition(
            @Parameter(
                    description = "The bundle group",
                    required = true
            )
            @PathParam("group") String group,
            @Parameter(
                    description = "The bundle artifact",
                    required = true
            )
            @PathParam("artifact") String artifact,
            @Parameter(
                    description = "The bundle version",
                    required = true
            )
            @PathParam("version") String version,
            @Parameter(
                    description = "The flow analysis rule type",
                    required = true
            )
            @PathParam("type") String type
    ) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final FlowAnalysisRuleDefinition entity = serviceFacade.getFlowAnalysisRuleDefinition(group, artifact, version, type);

        // generate the response
        return generateOkResponse(entity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("additional-details/{group}/{artifact}/{version}/{type}")
    @Operation(
            summary = "Retrieves the additional details for the specified component type.",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AdditionalDetailsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The additional details for the coordinates could not be located.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getAdditionalDetails(
            @Parameter(
                    description = "The bundle group",
                    required = true
            )
            @PathParam("group") String group,
            @Parameter(
                    description = "The bundle artifact",
                    required = true
            )
            @PathParam("artifact") String artifact,
            @Parameter(
                    description = "The bundle version",
                    required = true
            )
            @PathParam("version") String version,
            @Parameter(
                    description = "The processor type",
                    required = true
            )
            @PathParam("type") String type
    ) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final String additionalDetails = serviceFacade.getAdditionalDetails(group, artifact, version, type);
        final AdditionalDetailsEntity entity = new AdditionalDetailsEntity();
        entity.setAdditionalDetails(additionalDetails);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the types of parameter providers that this NiFi supports.
     *
     * @return A ParameterProviderTypesEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("parameter-provider-types")
    @Operation(
            summary = "Retrieves the types of parameter providers that this NiFi supports",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterProviderTypesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getParameterProviderTypes(
            @Parameter(
                    description = "If specified, will only return types that are a member of this bundle group."
            )
            @QueryParam("bundleGroupFilter") String bundleGroupFilter,
            @Parameter(
                    description = "If specified, will only return types that are a member of this bundle artifact."
            )
            @QueryParam("bundleArtifactFilter") String bundleArtifactFilter,
            @Parameter(
                    description = "If specified, will only return types whose fully qualified classname matches."
            )
            @QueryParam("type") String typeFilter) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final ParameterProviderTypesEntity entity = new ParameterProviderTypesEntity();
        entity.setParameterProviderTypes(serviceFacade.getParameterProviderTypes(bundleGroupFilter, bundleArtifactFilter, typeFilter));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the types of available Flow Analysis Rules.
     *
     * @return A controllerServicesTypesEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rule-types")
    @Operation(
            summary = "Retrieves the types of available Flow Analysis Rules",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowAnalysisRuleTypesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getFlowAnalysisRuleTypes(
            @Parameter(
                    description = "If specified, will only return types that are a member of this bundle group."
            )
            @QueryParam("bundleGroupFilter") String bundleGroupFilter,
            @Parameter(
                    description = "If specified, will only return types that are a member of this bundle artifact."
            )
            @QueryParam("bundleArtifactFilter") String bundleArtifactFilter,
            @Parameter(
                    description = "If specified, will only return types whose fully qualified classname matches."
            )
            @QueryParam("type") String typeFilter) throws InterruptedException {

        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final FlowAnalysisRuleTypesEntity entity = new FlowAnalysisRuleTypesEntity();
        entity.setFlowAnalysisRuleTypes(serviceFacade.getFlowAnalysisRuleTypes(bundleGroupFilter, bundleArtifactFilter, typeFilter));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the types of prioritizers that this NiFi supports.
     *
     * @return A prioritizerTypesEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("prioritizers")
    @Operation(
            summary = "Retrieves the types of prioritizers that this NiFi supports",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PrioritizerTypesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getPrioritizers() throws InterruptedException {
        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // create response entity
        final PrioritizerTypesEntity entity = new PrioritizerTypesEntity();
        entity.setPrioritizerTypes(serviceFacade.getWorkQueuePrioritizerTypes());

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves details about this NiFi to put in the About dialog.
     *
     * @return An aboutEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("about")
    @Operation(
            summary = "Retrieves details about this NiFi to put in the About dialog",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AboutEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getAboutInfo(@Context final HttpServletRequest httpServletRequest) {
        authorizeFlow();

        // create the about dto
        final AboutDTO aboutDTO = new AboutDTO();
        aboutDTO.setTitle("NiFi");
        aboutDTO.setUri(generateResourceUri());
        aboutDTO.setTimezone(new Date());

        final URI contentViewerUri = RequestUriBuilder.fromHttpServletRequest(httpServletRequest).path("/nifi/").fragment("/content-viewer").build();
        aboutDTO.setContentViewerUrl(contentViewerUri.toString());

        final Bundle frameworkBundle = NarClassLoadersHolder.getInstance().getFrameworkBundle();
        if (frameworkBundle != null) {
            final BundleDetails frameworkDetails = frameworkBundle.getBundleDetails();

            // set the version
            aboutDTO.setVersion(frameworkDetails.getCoordinate().getVersion());

            // Get build info
            aboutDTO.setBuildTag(frameworkDetails.getBuildTag());
            aboutDTO.setBuildRevision(frameworkDetails.getBuildRevision());
            aboutDTO.setBuildBranch(frameworkDetails.getBuildBranch());
            aboutDTO.setBuildTimestamp(frameworkDetails.getBuildTimestampDate());
        }

        // create the response entity
        final AboutEntity entity = new AboutEntity();
        entity.setAbout(aboutDTO);

        // generate the response
        return generateOkResponse(entity).build();
    }

    // ----------
    // registries
    // ----------

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registries")
    @Operation(
            summary = "Gets the listing of available flow registry clients",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowRegistryClientsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getRegistryClients() {
        authorizeFlow();

        final Set<FlowRegistryClientEntity> registryClients = serviceFacade.getRegistryClientsForUser();
        final FlowRegistryClientsEntity registryClientEntities = new FlowRegistryClientsEntity();
        registryClientEntities.setCurrentTime(new Date());
        registryClientEntities.setRegistries(registryClients);

        return generateOkResponse(populateRemainingRegistryClientEntityContent(registryClientEntities)).build();
    }

    /**
     * Populate the uri's for the specified flow registry client and also extend the result to make it backward compatible.
     *
     * @param flowRegistryClientEntity flow registry client
     * @return the updated entity
     */
    private FlowRegistryClientEntity populateRemainingRegistryClientEntityContent(final FlowRegistryClientEntity flowRegistryClientEntity) {
        flowRegistryClientEntity.setUri(generateResourceUri("controller", "registry-clients", flowRegistryClientEntity.getId()));
        return flowRegistryClientEntity;
    }

    /**
     * Populate the uri's for all contained flow registry clients and also extend the result to make it backward compatible.
     */
    private FlowRegistryClientsEntity populateRemainingRegistryClientEntityContent(final FlowRegistryClientsEntity flowRegistryClientsEntity) {
        for (final FlowRegistryClientEntity entity : flowRegistryClientsEntity.getRegistries()) {
            populateRemainingRegistryClientEntityContent(entity);
        }

        return flowRegistryClientsEntity;
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registries/{id}/branches")
    @Operation(
            summary = "Gets the branches from the specified registry for the current user",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowRegistryBranchesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getBranches(
            @Parameter(
                    description = "The registry id.",
                    required = true
            )
            @PathParam("id") String id) throws NiFiRegistryException {

        authorizeFlow();

        final Set<FlowRegistryBranchEntity> branches = serviceFacade.getBranches(id);

        final FlowRegistryBranchesEntity flowRegistryBranchesEntity = new FlowRegistryBranchesEntity();
        flowRegistryBranchesEntity.setBranches(branches);

        return generateOkResponse(flowRegistryBranchesEntity).build();
    }


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registries/{id}/buckets")
    @Operation(
            summary = "Gets the buckets from the specified registry for the current user",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowRegistryBucketsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getBuckets(
            @Parameter(
                    description = "The registry id.",
                    required = true
            )
            @PathParam("id") String id,
            @Parameter(
                    description = "The name of a branch to get the buckets from. If not specified the default branch of the registry client will be used."
            )
            @QueryParam("branch") String branch) {

        authorizeFlow();

        final String selectedBranch = branch == null ? serviceFacade.getDefaultBranch(id).getBranch().getName() : branch;
        final Set<FlowRegistryBucketEntity> buckets = serviceFacade.getBucketsForUser(id, selectedBranch);
        final SortedSet<FlowRegistryBucketEntity> sortedBuckets = sortBuckets(buckets);

        final FlowRegistryBucketsEntity flowRegistryBucketsEntity = new FlowRegistryBucketsEntity();
        flowRegistryBucketsEntity.setBuckets(sortedBuckets);

        return generateOkResponse(flowRegistryBucketsEntity).build();
    }

    private SortedSet<FlowRegistryBucketEntity> sortBuckets(final Set<FlowRegistryBucketEntity> buckets) {
        final SortedSet<FlowRegistryBucketEntity> sortedBuckets = new TreeSet<>((entity1, entity2) -> Collator.getInstance().compare(getBucketName(entity1), getBucketName(entity2)));

        sortedBuckets.addAll(buckets);
        return sortedBuckets;
    }

    private String getBucketName(final FlowRegistryBucketEntity entity) {
        return entity.getBucket() == null ? null : entity.getBucket().getName();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registries/{registry-id}/buckets/{bucket-id}/flows")
    @Operation(
            summary = "Gets the flows from the specified registry and bucket for the current user",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getFlows(
            @Parameter(
                    description = "The registry client id.",
                    required = true
            )
            @PathParam("registry-id") String registryId,
            @Parameter(
                    description = "The bucket id.",
                    required = true
            )
            @PathParam("bucket-id") String bucketId,
            @Parameter(
                    description = "The name of a branch to get the flows from. If not specified the default branch of the registry client will be used."
            )
            @QueryParam("branch") String branch) {

        authorizeFlow();

        final String selectedBranch = branch == null ? serviceFacade.getDefaultBranch(registryId).getBranch().getName() : branch;
        final Set<VersionedFlowEntity> registeredFlows = serviceFacade.getFlowsForUser(registryId, selectedBranch, bucketId);
        final SortedSet<VersionedFlowEntity> sortedFlows = sortFlows(registeredFlows);

        final VersionedFlowsEntity versionedFlowsEntity = new VersionedFlowsEntity();
        versionedFlowsEntity.setVersionedFlows(sortedFlows);

        return generateOkResponse(versionedFlowsEntity).build();
    }

    private SortedSet<VersionedFlowEntity> sortFlows(final Set<VersionedFlowEntity> versionedFlows) {
        final SortedSet<VersionedFlowEntity> sortedFlows = new TreeSet<>((entity1, entity2) -> Collator.getInstance().compare(getFlowName(entity1), getFlowName(entity2)));

        sortedFlows.addAll(versionedFlows);
        return sortedFlows;
    }

    private String getFlowName(final VersionedFlowEntity flowEntity) {
        return flowEntity.getVersionedFlow() == null ? "" : flowEntity.getVersionedFlow().getFlowName();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registries/{registry-id}/buckets/{bucket-id}/flows/{flow-id}/details")
    @Operation(
            summary = "Gets the details of a flow from the specified registry and bucket for the specified flow for the current user",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getDetails(
            @Parameter(
                    description = "The registry client id.",
                    required = true
            )
            @PathParam("registry-id") String registryId,
            @Parameter(
                    description = "The bucket id.",
                    required = true
            )
            @PathParam("bucket-id") String bucketId,
            @Parameter(
                    description = "The flow id.",
                    required = true
            )
            @PathParam("flow-id") String flowId,
            @Parameter(
                    description = "The name of a branch to get the flow from. If not specified the default branch of the registry client will be used."
            )
            @QueryParam("branch") String branch) {

        authorizeFlow();

        final String selectedBranch = branch == null ? serviceFacade.getDefaultBranch(registryId).getBranch().getName() : branch;
        final VersionedFlowEntity flowDetails = serviceFacade.getFlowForUser(registryId, selectedBranch, bucketId, flowId);
        return generateOkResponse(flowDetails).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registries/{registry-id}/branches/{branch-id-a}/buckets/{bucket-id-a}/flows/{flow-id-a}/{version-a}/diff/branches/{branch-id-b}/buckets/{bucket-id-b}/flows/{flow-id-b}/{version-b}")
    @Operation(
            summary = "Gets the differences between two versions of the same versioned flow, the basis of the comparison will be the first version",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowComparisonEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getVersionDifferences(
            @Parameter(
                    description = "The registry client id.",
                    required = true
            )
            @PathParam("registry-id") String registryId,

            @Parameter(
                    description = "The branch id for the base version.",
                    required = true
            )
            @PathParam("branch-id-a") String branchIdA,

            @Parameter(
                    description = "The bucket id for the base version.",
                    required = true
            )
            @PathParam("bucket-id-a") String bucketIdA,

            @Parameter(
                    description = "The flow id for the base version.",
                    required = true
            )
            @PathParam("flow-id-a") String flowIdA,

            @Parameter(
                    description = "The base version.",
                    required = true
            )
            @PathParam("version-a") String versionA,

            @Parameter(
                    description = "The branch id for the compared version.",
                    required = true
            )
            @PathParam("branch-id-b") String branchIdB,

            @Parameter(
                    description = "The bucket id for the compared version.",
                    required = true
            )
            @PathParam("bucket-id-b") String bucketIdB,

            @Parameter(
                    description = "The flow id for the compared version.",
                    required = true
            )
            @PathParam("flow-id-b") String flowIdB,

            @Parameter(
                    description = "The compared version.",
                    required = true
            )
            @PathParam("version-b") String versionB,
            @QueryParam("offset")
            @Parameter(description = "Must be a non-negative number. Specifies the starting point of the listing. 0 means start from the beginning.")
            @DefaultValue("0")
            int offset,
            @QueryParam("limit")
            @Parameter(description = "Limits the number of differences listed. This might lead to partial result. 0 means no limitation is applied.")
            @DefaultValue("1000")
            int limit
    ) {
        authorizeFlow();
        FlowVersionLocation baseVersionLocation = new FlowVersionLocation(branchIdA, bucketIdA, flowIdA, versionA);
        FlowVersionLocation comparedVersionLocation = new FlowVersionLocation(branchIdB, bucketIdB, flowIdB, versionB);
            final FlowComparisonEntity versionDifference = serviceFacade.getVersionDifference(registryId, baseVersionLocation, comparedVersionLocation);
        // Note: with the current implementation, this is deterministic. However, the internal data structure used in comparison is set, thus
        // later changes might cause discrepancies. Practical use of the endpoint usually remains within one "page" though.
        return generateOkResponse(limitDifferences(versionDifference, offset, limit))
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registries/{registry-id}/buckets/{bucket-id}/flows/{flow-id}/versions")
    @Operation(
            summary = "Gets the flow versions from the specified registry and bucket for the specified flow for the current user",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowSnapshotMetadataSetEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getVersions(
            @Parameter(
                    description = "The registry client id.",
                    required = true
            )
            @PathParam("registry-id") String registryId,
            @Parameter(
                    description = "The bucket id.",
                    required = true
            )
            @PathParam("bucket-id") String bucketId,
            @Parameter(
                    description = "The flow id.",
                    required = true
            )
            @PathParam("flow-id") String flowId,
            @Parameter(
                    description = "The name of a branch to get the flow versions from. If not specified the default branch of the registry client will be used."
            )
            @QueryParam("branch") String branch) {

        authorizeFlow();

        final String selectedBranch = branch == null ? serviceFacade.getDefaultBranch(registryId).getBranch().getName() : branch;
        final Set<VersionedFlowSnapshotMetadataEntity> registeredFlowSnapshotMetadataSet = serviceFacade.getFlowVersionsForUser(registryId, selectedBranch, bucketId, flowId);

        final VersionedFlowSnapshotMetadataSetEntity versionedFlowSnapshotMetadataSetEntity = new VersionedFlowSnapshotMetadataSetEntity();
        versionedFlowSnapshotMetadataSetEntity.setVersionedFlowSnapshotMetadataSet(registeredFlowSnapshotMetadataSet);

        return generateOkResponse(versionedFlowSnapshotMetadataSetEntity).build();
    }

    private static FlowComparisonEntity limitDifferences(final FlowComparisonEntity original, final int offset, final int limit) {
        final List<ComponentDifferenceDTO> limited = PaginationHelper.paginateByContainedItems(
                original.getComponentDifferences(), offset, limit, ComponentDifferenceDTO::getDifferences, FlowResource::limitDifferences);
        final FlowComparisonEntity result = new FlowComparisonEntity();
        result.setComponentDifferences(new HashSet<>(limited));
        return result;
    }

    private static ComponentDifferenceDTO limitDifferences(final ComponentDifferenceDTO original, final List<DifferenceDTO> partial) {
        final ComponentDifferenceDTO result = new ComponentDifferenceDTO();
        result.setComponentType(original.getComponentType());
        result.setComponentId(original.getComponentId());
        result.setComponentName(original.getComponentName());
        result.setProcessGroupId(original.getProcessGroupId());
        result.setDifferences(partial);
        return result;
    }

    // --------------
    // bulletin board
    // --------------

    /**
     * Retrieves all the of bulletins in this NiFi.
     *
     * @param after Supporting querying for bulletins after a particular
     * bulletin id.
     * @param limit The max number of bulletins to return.
     * @param sourceName Source name filter. Supports a regular expression.
     * @param message Message filter. Supports a regular expression.
     * @param sourceId Source id filter. Supports a regular expression.
     * @param groupId Group id filter. Supports a regular expression.
     * @return A bulletinBoardEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("bulletin-board")
    @Operation(
            summary = "Gets current bulletins",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = BulletinBoardEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow"),
                    @SecurityRequirement(name = "Read - /{component-type}/{uuid} - For component specific bulletins")
            }
    )
    public Response getBulletinBoard(
            @Parameter(
                    description = "Includes bulletins with an id after this value."
            )
            @QueryParam("after") LongParameter after,
            @Parameter(
                    description = "Includes bulletins originating from this sources whose name match this regular expression."
            )
            @QueryParam("sourceName") BulletinBoardPatternParameter sourceName,
            @Parameter(
                    description = "Includes bulletins whose message that match this regular expression."
            )
            @QueryParam("message") BulletinBoardPatternParameter message,
            @Parameter(
                    description = "Includes bulletins originating from this sources whose id match this regular expression."
            )
            @QueryParam("sourceId") BulletinBoardPatternParameter sourceId,
            @Parameter(
                    description = "Includes bulletins originating from this sources whose group id match this regular expression."
            )
            @QueryParam("groupId") BulletinBoardPatternParameter groupId,
            @Parameter(
                    description = "The number of bulletins to limit the response to."
            )
            @QueryParam("limit") IntegerParameter limit) throws InterruptedException {

        authorizeFlow();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // build the bulletin query
        final BulletinQueryDTO query = new BulletinQueryDTO();

        if (sourceId != null) {
            query.setSourceId(sourceId.getRawPattern());
        }
        if (groupId != null) {
            query.setGroupId(groupId.getRawPattern());
        }
        if (sourceName != null) {
            query.setName(sourceName.getRawPattern());
        }
        if (message != null) {
            query.setMessage(message.getRawPattern());
        }
        if (after != null) {
            query.setAfter(after.getLong());
        }
        if (limit != null) {
            query.setLimit(limit.getInteger());
        }

        // get the bulletin board
        final BulletinBoardDTO bulletinBoard = serviceFacade.getBulletinBoard(query);

        // create the response entity
        BulletinBoardEntity entity = new BulletinBoardEntity();
        entity.setBulletinBoard(bulletinBoard);

        // generate the response
        return generateOkResponse(entity).build();
    }

    // ------
    // status
    // ------

    /**
     * Retrieves the specified processor status.
     *
     * @param id The id of the processor history to retrieve.
     * @return A processorStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("processors/{id}/status")
    @Operation(
            summary = "Gets status for a processor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessorStatusEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getProcessorStatus(
            @Parameter(
                    description = "Whether or not to include the breakdown per node. Optional, defaults to false"
            )
            @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
            @Parameter(
                    description = "The id of the node where to get the status."
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @Parameter(
                    description = "The processor id.",
                    required = true
            )
            @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final ProcessorStatusEntity entity = (ProcessorStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getProcessorStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the specified processor status
        final ProcessorStatusEntity entity = serviceFacade.getProcessorStatus(id);
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the specified input port status.
     *
     * @param id The id of the processor history to retrieve.
     * @return A portStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("input-ports/{id}/status")
    @Operation(
            summary = "Gets status for an input port",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PortStatusEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getInputPortStatus(
            @Parameter(
                    description = "Whether or not to include the breakdown per node. Optional, defaults to false"
            )
            @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
            @Parameter(
                    description = "The id of the node where to get the status."
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @Parameter(
                    description = "The input port id.",
                    required = true
            )
            @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final PortStatusEntity entity = (PortStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getPortStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the specified input port status
        final PortStatusEntity entity = serviceFacade.getInputPortStatus(id);
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the specified output port status.
     *
     * @param id The id of the processor history to retrieve.
     * @return A portStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("output-ports/{id}/status")
    @Operation(
            summary = "Gets status for an output port",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PortStatusEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getOutputPortStatus(
            @Parameter(
                    description = "Whether or not to include the breakdown per node. Optional, defaults to false"
            )
            @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
            @Parameter(
                    description = "The id of the node where to get the status."
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @Parameter(
                    description = "The output port id.",
                    required = true
            )
            @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final PortStatusEntity entity = (PortStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getPortStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the specified output port status
        final PortStatusEntity entity = serviceFacade.getOutputPortStatus(id);
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the specified remote process group status.
     *
     * @param id The id of the processor history to retrieve.
     * @return A remoteProcessGroupStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("remote-process-groups/{id}/status")
    @Operation(
            summary = "Gets status for a remote process group",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RemoteProcessGroupStatusEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getRemoteProcessGroupStatus(
            @Parameter(
                    description = "Whether or not to include the breakdown per node. Optional, defaults to false"
            )
            @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
            @Parameter(
                    description = "The id of the node where to get the status."
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @Parameter(
                    description = "The remote process group id."
            )
            @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final RemoteProcessGroupStatusEntity entity = (RemoteProcessGroupStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getRemoteProcessGroupStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the specified remote process group status
        final RemoteProcessGroupStatusEntity entity = serviceFacade.getRemoteProcessGroupStatus(id);
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the status report for this NiFi.
     *
     * @param recursive Optional recursive flag that defaults to false. If set to true, all descendant groups and the status of their content will be included.
     * @param groupId The group id
     * @return A processGroupStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}/status")
    @Operation(
            summary = "Gets the status for a process group",
            description = "The status for a process group includes status for all descendent components. When invoked on the root group with "
                    + "recursive set to true, it will return the current status of every component in the flow.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessGroupStatusEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getProcessGroupStatus(
            @Parameter(
                    description = "Whether all descendant groups and the status of their content will be included. Optional, defaults to false"
            )
            @QueryParam("recursive") @DefaultValue(RECURSIVE) Boolean recursive,
            @Parameter(
                    description = "Whether or not to include the breakdown per node. Optional, defaults to false"
            )
            @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
            @Parameter(
                    description = "The id of the node where to get the status."
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @Parameter(
                    description = "The process group id.",
                    required = true
            )
            @PathParam("id") String groupId) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final ProcessGroupStatusEntity entity = (ProcessGroupStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getProcessGroupStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the status
        final ProcessGroupStatusEntity entity = serviceFacade.getProcessGroupStatus(groupId, recursive);
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the specified connection status.
     *
     * @param id The id of the connection history to retrieve.
     * @return A connectionStatusEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("connections/{id}/status")
    @Operation(
            summary = "Gets status for a connection",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConnectionStatusEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getConnectionStatus(
            @Parameter(
                    description = "Whether or not to include the breakdown per node. Optional, defaults to false"
            )
            @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
            @Parameter(
                    description = "The id of the node where to get the status."
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final ConnectionStatusEntity entity = (ConnectionStatusEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getConnectionStatus().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the specified connection status
        final ConnectionStatusEntity entity = serviceFacade.getConnectionStatus(id);
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the specified connection statistics.
     *
     * @param id The id of the connection statistics to retrieve.
     * @return A ConnectionStatisticsEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("connections/{id}/statistics")
    @Operation(
            summary = "Gets statistics for a connection",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConnectionStatisticsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getConnectionStatistics(
            @Parameter(
                    description = "Whether or not to include the breakdown per node. Optional, defaults to false"
            )
            @QueryParam("nodewise") @DefaultValue(NODEWISE) Boolean nodewise,
            @Parameter(
                    description = "The id of the node where to get the statistics."
            )
            @QueryParam("clusterNodeId") String clusterNodeId,
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // ensure a valid request
        if (Boolean.TRUE.equals(nodewise) && clusterNodeId != null) {
            throw new IllegalArgumentException("Nodewise requests cannot be directed at a specific node.");
        }

        if (isReplicateRequest()) {
            // determine where this request should be sent
            if (clusterNodeId == null) {
                final NodeResponse nodeResponse = replicateNodeResponse(HttpMethod.GET);
                final ConnectionStatisticsEntity entity = (ConnectionStatisticsEntity) nodeResponse.getUpdatedEntity();

                // ensure there is an updated entity (result of merging) and prune the response as necessary
                if (entity != null && !nodewise) {
                    entity.getConnectionStatistics().setNodeSnapshots(null);
                }

                return nodeResponse.getResponse();
            } else {
                return replicate(HttpMethod.GET, clusterNodeId);
            }
        }

        // get the specified connection status
        final ConnectionStatisticsEntity entity = serviceFacade.getConnectionStatistics(id);
        return generateOkResponse(entity).build();
    }

    // --------------
    // status history
    // --------------

    /**
     * Retrieves the specified processor status history.
     *
     * @param id The id of the processor history to retrieve.
     * @return A statusHistoryEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("processors/{id}/status/history")
    @Operation(
            summary = "Gets status history for a processor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = StatusHistoryEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getProcessorStatusHistory(
            @Parameter(
                    description = "The processor id.",
                    required = true
            )
            @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the specified processor status history
        final StatusHistoryEntity entity = serviceFacade.getProcessorStatusHistory(id);
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the specified remote process groups status history.
     *
     * @param groupId The group id
     * @return A processorEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}/status/history")
    @Operation(
            summary = "Gets status history for a remote process group",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = StatusHistoryEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getProcessGroupStatusHistory(
            @Parameter(
                    description = "The process group id.",
                    required = true
            )
            @PathParam("id") String groupId) throws InterruptedException {

        authorizeFlow();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the specified processor status history
        final StatusHistoryEntity entity = serviceFacade.getProcessGroupStatusHistory(groupId);
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the specified remote process groups status history.
     *
     * @param id The id of the remote process group to retrieve the status fow.
     * @return A statusHistoryEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("remote-process-groups/{id}/status/history")
    @Operation(
            summary = "Gets the status history",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = StatusHistoryEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getRemoteProcessGroupStatusHistory(
            @Parameter(
                    description = "The remote process group id.",
                    required = true
            )
            @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the specified processor status history
        final StatusHistoryEntity entity = serviceFacade.getRemoteProcessGroupStatusHistory(id);
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the specified connection status history.
     *
     * @param id The id of the connection to retrieve.
     * @return A statusHistoryEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("connections/{id}/status/history")
    @Operation(
            summary = "Gets the status history for a connection",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = StatusHistoryEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getConnectionStatusHistory(
            @Parameter(
                    description = "The connection id.",
                    required = true
            )
            @PathParam("id") String id) throws InterruptedException {

        authorizeFlow();

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the specified processor status history
        final StatusHistoryEntity entity = serviceFacade.getConnectionStatusHistory(id);
        return generateOkResponse(entity).build();
    }


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("parameter-contexts")
    @Operation(
            summary = "Gets all Parameter Contexts",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterContextsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /parameter-contexts/{id} for each Parameter Context")
            }
    )
    public Response getParameterContexts() {
        authorizeFlow();

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final Set<ParameterContextEntity> parameterContexts = serviceFacade.getParameterContexts();
        parameterContexts.forEach(entity -> entity.setUri(generateResourceUri("parameter-contexts", entity.getId())));

        final ParameterContextsEntity entity = new ParameterContextsEntity();
        entity.setParameterContexts(parameterContexts);
        entity.setCurrentTime(new Date());

        // generate the response
        return generateOkResponse(entity).build();
    }


    // -------
    // history
    // -------

    /**
     * Queries the history of this Controller.
     *
     * @param offset The offset into the data. This parameter is required and is
     * used in conjunction with count.
     * @param count The number of rows that should be returned. This parameter
     * is required and is used in conjunction with page.
     * @param sortColumn The column to sort on. This parameter is optional. If
     * not specified the results will be returned with the most recent first.
     * @param sortOrder The sort order.
     * @param startDate The start date/time for the query. The start date/time
     * must be formatted as 'MM/dd/yyyy HH:mm:ss'. This parameter is optional
     * and must be specified in the timezone of the server. The server's
     * timezone can be determined by inspecting the result of a status or
     * history request.
     * @param endDate The end date/time for the query. The end date/time must be
     * formatted as 'MM/dd/yyyy HH:mm:ss'. This parameter is optional and must
     * be specified in the timezone of the server. The server's timezone can be
     * determined by inspecting the result of a status or history request.
     * @param userIdentity The user name of the user who's actions are being
     * queried. This parameter is optional.
     * @param sourceId The id of the source being queried (usually a processor
     * id). This parameter is optional.
     * @return A historyEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("history")
    @Operation(
            summary = "Gets configuration history",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = HistoryEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response queryHistory(
            @Parameter(
                    description = "The offset into the result set.",
                    required = true
            )
            @QueryParam("offset") IntegerParameter offset,
            @Parameter(
                    description = "The number of actions to return.",
                    required = true
            )
            @QueryParam("count") IntegerParameter count,
            @Parameter(
                    description = "The field to sort on."
            )
            @QueryParam("sortColumn") String sortColumn,
            @Parameter(
                    description = "The direction to sort."
            )
            @QueryParam("sortOrder") String sortOrder,
            @Parameter(
                    description = "Include actions after this date."
            )
            @QueryParam("startDate") DateTimeParameter startDate,
            @Parameter(
                    description = "Include actions before this date."
            )
            @QueryParam("endDate") DateTimeParameter endDate,
            @Parameter(
                    description = "Include actions performed by this user."
            )
            @QueryParam("userIdentity") String userIdentity,
            @Parameter(
                    description = "Include actions on this component."
            )
            @QueryParam("sourceId") String sourceId) {

        authorizeFlow();

        // ensure the page is specified
        if (offset == null) {
            throw new IllegalArgumentException("The desired offset must be specified.");
        } else if (offset.getInteger() < 0) {
            throw new IllegalArgumentException("The desired offset must be an integer value greater than or equal to 0.");
        }

        // ensure the row count is specified
        if (count == null) {
            throw new IllegalArgumentException("The desired row count must be specified.");
        } else if (count.getInteger() < 1) {
            throw new IllegalArgumentException("The desired row count must be an integer value greater than 0.");
        }

        // normalize the sort order
        if (sortOrder != null) {
            if (!sortOrder.equalsIgnoreCase("asc") && !sortOrder.equalsIgnoreCase("desc")) {
                throw new IllegalArgumentException("The sort order must be 'asc' or 'desc'.");
            }
        }

        // ensure the start and end dates are specified
        if (endDate != null && startDate != null) {
            if (endDate.getDateTime().before(startDate.getDateTime())) {
                throw new IllegalArgumentException("The start date/time must come before the end date/time.");
            }
        }

        // Note: History requests are not replicated throughout the cluster and are instead handled by the nodes independently

        // create a history query
        final HistoryQueryDTO query = new HistoryQueryDTO();
        query.setSortColumn(sortColumn);
        query.setSortOrder(sortOrder);
        query.setOffset(offset.getInteger());
        query.setCount(count.getInteger());

        // optionally set the start date
        if (startDate != null) {
            query.setStartDate(startDate.getDateTime());
        }

        // optionally set the end date
        if (endDate != null) {
            query.setEndDate(endDate.getDateTime());
        }

        // optionally set the user id
        if (userIdentity != null) {
            query.setUserIdentity(userIdentity);
        }

        // optionally set the processor id
        if (sourceId != null) {
            query.setSourceId(sourceId);
        }

        // perform the query
        final HistoryDTO history = serviceFacade.getActions(query);

        // create the response entity
        final HistoryEntity entity = new HistoryEntity();
        entity.setHistory(history);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the action for the corresponding id.
     *
     * @param id The id of the action to get.
     * @return An actionEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("history/{id}")
    @Operation(
            summary = "Gets an action",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ActionEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getAction(
            @Parameter(
                    description = "The action id.",
                    required = true
            )
            @PathParam("id") IntegerParameter id) {

        authorizeFlow();

        // ensure the id was specified
        if (id == null) {
            throw new IllegalArgumentException("The action id must be specified.");
        }

        // Note: History requests are not replicated throughout the cluster and are instead handled by the nodes independently

        // get the response entity for the specified action
        final ActionEntity entity = serviceFacade.getAction(id.getInteger());

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the actions for the specified component.
     *
     * @param componentId The id of the component.
     * @return An processorHistoryEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("history/components/{componentId}")
    @Operation(
            summary = "Gets configuration history for a component",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ComponentHistoryEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow"),
                    @SecurityRequirement(name = "Read underlying component - /{component-type}/{uuid}")
            }
    )
    public Response getComponentHistory(
            @Parameter(
                    description = "The component id.",
                    required = true
            )
            @PathParam("componentId") final String componentId) {

        serviceFacade.authorizeAccess(lookup -> {
            final NiFiUser user = NiFiUserUtils.getNiFiUser();

            // authorize the flow
            authorizeFlow();

            try {
                final Authorizable authorizable = lookup.getProcessor(componentId).getAuthorizable();
                authorizable.authorize(authorizer, RequestAction.READ, user);
                return;
            } catch (final ResourceNotFoundException ignored) {
                // ignore as the component may not be a processor
            }

            try {
                final Authorizable authorizable = lookup.getControllerService(componentId).getAuthorizable();
                authorizable.authorize(authorizer, RequestAction.READ, user);
                return;
            } catch (final ResourceNotFoundException ignored) {
                // ignore as the component may not be a controller service
            }

            try {
                final Authorizable authorizable = lookup.getReportingTask(componentId).getAuthorizable();
                authorizable.authorize(authorizer, RequestAction.READ, user);
                return;
            } catch (final ResourceNotFoundException ignored) {
                // ignore as the component may not be a reporting task
            }

            try {
                final Authorizable authorizable = lookup.getParameterProvider(componentId).getAuthorizable();
                authorizable.authorize(authorizer, RequestAction.READ, user);
                return;
            } catch (final ResourceNotFoundException ignored) {
                // ignore as the component may not be a parameter provider
            }

            // a component for the specified id could not be found, attempt to authorize based on read to the controller
            final Authorizable controller = lookup.getController();
            controller.authorize(authorizer, RequestAction.READ, user);
        });

        // Note: History requests are not replicated throughout the cluster and are instead handled by the nodes independently

        // create the response entity
        final ComponentHistoryEntity entity = new ComponentHistoryEntity();
        entity.setComponentHistory(serviceFacade.getComponentHistory(componentId));

        // generate the response
        return generateOkResponse(entity).build();
    }

    // -------------
    // flow-analysis
    // -------------

    /**
     * Returns flow analysis results produced by the analysis of a given process group.
     *
     * @return a flowAnalysisResultEntity containing flow analysis results produced by the analysis of the given process group
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis/results/{processGroupId}")
    @Operation(
            summary = "Returns flow analysis results produced by the analysis of a given process group",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowAnalysisResultEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getFlowAnalysisResults(
            @Parameter(
                    description = "The id of the process group representing (a part of) the flow to be analyzed.",
                    required = true
            )
            @PathParam("processGroupId") final String processGroupId
    ) {
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        authorizeFlow();

        FlowAnalysisResultEntity entity = serviceFacade.getFlowAnalysisResult(processGroupId);

        return generateOkResponse(entity).build();
    }

    /**
     * Returns all flow analysis results currently in effect.
     *
     * @return a flowAnalysisRuleEntity containing all flow analysis results currently in-effect
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis/results")
    @Operation(
            summary = "Returns all flow analysis results currently in effect",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowAnalysisResultEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getAllFlowAnalysisResults() {
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        authorizeFlow();

        FlowAnalysisResultEntity entity = serviceFacade.getFlowAnalysisResult();

        return generateOkResponse(entity).build();
    }

    // --------------------
    // search cluster nodes
    // --------------------

    /**
     * Searches the cluster for a node with a given address.
     *
     * @param value Search value that will be matched against a node's address
     * @return Nodes that match the specified criteria
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/search-results")
    @Operation(
            summary = "Searches the cluster for a node with the specified address",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ClusterSearchResultsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response searchCluster(
            @Parameter(
                    description = "Node address to search for.",
                    required = true
            )
            @QueryParam("q") @DefaultValue(StringUtils.EMPTY) String value) {

        authorizeFlow();

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        final List<NodeSearchResultDTO> nodeMatches = new ArrayList<>();

        // get the nodes in the cluster
        final ClusterDTO cluster = serviceFacade.getCluster();

        // check each to see if it matches the search term
        for (NodeDTO node : cluster.getNodes()) {
            // ensure the node is connected
            if (!NodeConnectionState.CONNECTED.name().equals(node.getStatus())) {
                continue;
            }

            // determine the current nodes address
            final String address = node.getAddress() + ":" + node.getApiPort();

            // count the node if there is no search or it matches the address
            if (StringUtils.isBlank(value) || StringUtils.containsIgnoreCase(address, value)) {
                final NodeSearchResultDTO nodeMatch = new NodeSearchResultDTO();
                nodeMatch.setId(node.getNodeId());
                nodeMatch.setAddress(address);
                nodeMatches.add(nodeMatch);
            }
        }

        // build the response
        ClusterSearchResultsEntity results = new ClusterSearchResultsEntity();
        results.setNodeResults(nodeMatches);

        // generate an 200 - OK response
        return noCache(Response.ok(results)).build();
    }

    @Autowired
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Autowired
    public void setProcessorResource(ProcessorResource processorResource) {
        this.processorResource = processorResource;
    }

    @Autowired
    public void setInputPortResource(InputPortResource inputPortResource) {
        this.inputPortResource = inputPortResource;
    }

    @Autowired
    public void setOutputPortResource(OutputPortResource outputPortResource) {
        this.outputPortResource = outputPortResource;
    }

    @Autowired
    public void setFunnelResource(FunnelResource funnelResource) {
        this.funnelResource = funnelResource;
    }

    @Autowired
    public void setLabelResource(LabelResource labelResource) {
        this.labelResource = labelResource;
    }

    @Autowired
    public void setRemoteProcessGroupResource(RemoteProcessGroupResource remoteProcessGroupResource) {
        this.remoteProcessGroupResource = remoteProcessGroupResource;
    }

    @Autowired
    public void setConnectionResource(ConnectionResource connectionResource) {
        this.connectionResource = connectionResource;
    }

    @Autowired
    public void setProcessGroupResource(ProcessGroupResource processGroupResource) {
        this.processGroupResource = processGroupResource;
    }

    @Autowired
    public void setControllerServiceResource(ControllerServiceResource controllerServiceResource) {
        this.controllerServiceResource = controllerServiceResource;
    }

    @Autowired
    public void setReportingTaskResource(ReportingTaskResource reportingTaskResource) {
        this.reportingTaskResource = reportingTaskResource;
    }

    @Autowired
    public void setParameterProviderResource(final ParameterProviderResource parameterProviderResource) {
        this.parameterProviderResource = parameterProviderResource;
    }

    @Autowired
    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }
}
