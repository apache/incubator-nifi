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
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AuthorizeControllerServiceReference;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.UploadRequest;
import org.apache.nifi.cluster.coordination.http.replication.UploadRequestReplicator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.registry.flow.FlowRegistryUtils;
import org.apache.nifi.stream.io.MaxLengthInputStream;
import org.apache.nifi.web.IllegalClusterResourceRequestException;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.concurrent.AsyncRequestManager;
import org.apache.nifi.web.api.concurrent.AsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.RequestManager;
import org.apache.nifi.web.api.concurrent.StandardAsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.StandardUpdateStep;
import org.apache.nifi.web.api.concurrent.UpdateStep;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.ConfigurationAnalysisDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.dto.FlowRegistryClientDTO;
import org.apache.nifi.web.api.dto.NarCoordinateDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.ParameterProviderDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.VerifyConfigRequestDTO;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.ComponentHistoryEntity;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConfigurationAnalysisEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleRunStatusEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRulesEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientTypesEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientsEntity;
import org.apache.nifi.web.api.entity.HistoryEntity;
import org.apache.nifi.web.api.entity.NarDetailsEntity;
import org.apache.nifi.web.api.entity.NarSummariesEntity;
import org.apache.nifi.web.api.entity.NarSummaryEntity;
import org.apache.nifi.web.api.entity.NodeEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;
import org.apache.nifi.web.api.entity.VersionedReportingTaskImportRequestEntity;
import org.apache.nifi.web.api.entity.VersionedReportingTaskImportResponseEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DateTimeParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * RESTful endpoint for managing a Flow Controller.
 */
@Controller
@Path("/controller")
@Tag(name = "Controller")
public class ControllerResource extends ApplicationResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerResource.class);
    public static final String VERIFICATION_REQUEST_TYPE = "verification-request";

    private static final String FILENAME_HEADER = "Filename";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String UPLOAD_CONTENT_TYPE = "application/octet-stream";

    public RequestManager<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> configVerificationRequestManager =
            new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Verify Flow Analysis Rule Config Thread");

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;
    private UploadRequestReplicator uploadRequestReplicator;

    private ReportingTaskResource reportingTaskResource;
    private ParameterProviderResource parameterProviderResource;
    private ControllerServiceResource controllerServiceResource;

    /**
     * Authorizes access to the flow.
     */
    private void authorizeController(final RequestAction action) {
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable controller = lookup.getController();
            controller.authorize(authorizer, action, NiFiUserUtils.getNiFiUser());
        });
    }

    /**
     * Retrieves the configuration for this NiFi.
     *
     * @return A controllerConfigurationEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    @Operation(
            summary = "Retrieves the configuration for this NiFi Controller",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerConfigurationEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller")
            }
    )
    public Response getControllerConfig() {

        authorizeController(RequestAction.READ);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final ControllerConfigurationEntity entity = serviceFacade.getControllerConfiguration();
        return generateOkResponse(entity).build();
    }

    /**
     * Update the configuration for this NiFi.
     *
     * @param requestConfigEntity A controllerConfigurationEntity.
     * @return A controllerConfigurationEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("config")
    @Operation(
            summary = "Retrieves the configuration for this NiFi",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerConfigurationEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller")
            }
    )
    public Response updateControllerConfig(
            @Parameter(
                    description = "The controller configuration.",
                    required = true
            ) final ControllerConfigurationEntity requestConfigEntity) {

        if (requestConfigEntity == null || requestConfigEntity.getComponent() == null) {
            throw new IllegalArgumentException("Controller configuration must be specified");
        }

        if (requestConfigEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestConfigEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestConfigEntity.isDisconnectedNodeAcknowledged());
        }

        final Revision requestRevision = getRevision(requestConfigEntity.getRevision(), FlowController.class.getSimpleName());
        return withWriteLock(
                serviceFacade,
                requestConfigEntity,
                requestRevision,
                lookup -> authorizeController(RequestAction.WRITE),
                null,
                (revision, configEntity) -> {
                    final ControllerConfigurationEntity entity = serviceFacade.updateControllerConfiguration(revision, configEntity.getComponent());
                    return generateOkResponse(entity).build();
                }
        );
    }

    // ---------------
    // parameter providers
    // ---------------

    /**
     * Creates a new Parameter Provider.
     *
     * @param requestParameterProviderEntity A parameterProviderEntity.
     * @return A parameterProviderEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("parameter-providers")
    @Operation(
            summary = "Creates a new parameter provider",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = ParameterProviderEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services - /controller-services/{uuid}"),
                    @SecurityRequirement(name = "Write - if the Parameter Provider is restricted - /restricted-components")
            }
    )
    public Response createParameterProvider(
            @Parameter(
                    description = "The parameter provider configuration details.",
                    required = true
            ) final ParameterProviderEntity requestParameterProviderEntity) {

        if (requestParameterProviderEntity == null || requestParameterProviderEntity.getComponent() == null) {
            throw new IllegalArgumentException("Parameter provider details must be specified.");
        }

        if (requestParameterProviderEntity.getRevision() == null || (requestParameterProviderEntity.getRevision().getVersion() == null
                || requestParameterProviderEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Parameter provider.");
        }

        final ParameterProviderDTO requestParameterProvider = requestParameterProviderEntity.getComponent();
        if (requestParameterProvider.getId() != null) {
            throw new IllegalArgumentException("Parameter provider ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestParameterProvider.getType())) {
            throw new IllegalArgumentException("The type of parameter provider to create must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestParameterProviderEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestParameterProviderEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestParameterProviderEntity,
                lookup -> {
                    authorizeController(RequestAction.WRITE);

                    ComponentAuthorizable authorizable = null;
                    try {
                        authorizable = lookup.getConfigurableComponent(requestParameterProvider.getType(), requestParameterProvider.getBundle());

                        if (authorizable.isRestricted()) {
                            authorizeRestrictions(authorizer, authorizable);
                        }

                        if (requestParameterProvider.getProperties() != null) {
                            AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestParameterProvider.getProperties(), authorizable, authorizer, lookup);
                        }
                    } finally {
                        if (authorizable != null) {
                            authorizable.cleanUpResources();
                        }
                    }
                },
                () -> serviceFacade.verifyCreateParameterProvider(requestParameterProvider),
                (parameterProviderEntity) -> {
                    final ParameterProviderDTO parameterProvider = parameterProviderEntity.getComponent();

                    // set the processor id as appropriate
                    parameterProvider.setId(generateUuid());

                    // create the parameter provider and generate the json
                    final Revision revision = getRevision(parameterProviderEntity, parameterProvider.getId());
                    final ParameterProviderEntity entity = serviceFacade.createParameterProvider(revision, parameterProvider);
                    parameterProviderResource.populateRemainingParameterProviderEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    // ---------------
    // reporting tasks
    // ---------------

    /**
     * Creates a new Reporting Task.
     *
     * @param requestReportingTaskEntity A reportingTaskEntity.
     * @return A reportingTaskEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-tasks")
    @Operation(
            summary = "Creates a new reporting task",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = ReportingTaskEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services - /controller-services/{uuid}"),
                    @SecurityRequirement(name = "Write - if the Reporting Task is restricted - /restricted-components")
            }
    )
    public Response createReportingTask(
            @Parameter(
                    description = "The reporting task configuration details.",
                    required = true
            ) final ReportingTaskEntity requestReportingTaskEntity) {

        if (requestReportingTaskEntity == null || requestReportingTaskEntity.getComponent() == null) {
            throw new IllegalArgumentException("Reporting task details must be specified.");
        }

        if (requestReportingTaskEntity.getRevision() == null || (requestReportingTaskEntity.getRevision().getVersion() == null || requestReportingTaskEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Reporting task.");
        }

        final ReportingTaskDTO requestReportingTask = requestReportingTaskEntity.getComponent();
        if (requestReportingTask.getId() != null) {
            throw new IllegalArgumentException("Reporting task ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestReportingTask.getType())) {
            throw new IllegalArgumentException("The type of reporting task to create must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestReportingTaskEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestReportingTaskEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestReportingTaskEntity,
                lookup -> {
                    authorizeController(RequestAction.WRITE);

                    ComponentAuthorizable authorizable = null;
                    try {
                        authorizable = lookup.getConfigurableComponent(requestReportingTask.getType(), requestReportingTask.getBundle());

                        if (authorizable.isRestricted()) {
                            authorizeRestrictions(authorizer, authorizable);
                        }

                        if (requestReportingTask.getProperties() != null) {
                            AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestReportingTask.getProperties(), authorizable, authorizer, lookup);
                        }
                    } finally {
                        if (authorizable != null) {
                            authorizable.cleanUpResources();
                        }
                    }
                },
                () -> serviceFacade.verifyCreateReportingTask(requestReportingTask),
                (reportingTaskEntity) -> {
                    final ReportingTaskDTO reportingTask = reportingTaskEntity.getComponent();

                    // set the processor id as appropriate
                    reportingTask.setId(generateUuid());

                    // create the reporting task and generate the json
                    final Revision revision = getRevision(reportingTaskEntity, reportingTask.getId());
                    final ReportingTaskEntity entity = serviceFacade.createReportingTask(revision, reportingTask);
                    reportingTaskResource.populateRemainingReportingTaskEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Imports a reporting task snapshot.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-tasks/import")
    @Operation(
            summary = "Imports a reporting task snapshot",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedReportingTaskImportResponseEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller")
            }
    )
    public Response importReportingTaskSnapshot(
            @Parameter(
                    description = "The import request containing the reporting task snapshot to import.",
                    required = true
            ) final VersionedReportingTaskImportRequestEntity importRequestEntity) {

        if (importRequestEntity == null || importRequestEntity.getReportingTaskSnapshot() == null) {
            throw new IllegalArgumentException("Reporting task snapshot is required");
        }

        final VersionedReportingTaskSnapshot requestSnapshot = importRequestEntity.getReportingTaskSnapshot();
        serviceFacade.discoverCompatibleBundles(requestSnapshot);
        serviceFacade.generateIdentifiersForImport(requestSnapshot, () -> generateUuid());

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, importRequestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(importRequestEntity.getDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                importRequestEntity,
                lookup -> {
                    authorizeController(RequestAction.WRITE);

                    final Set<ConfigurableComponent> restrictedComponents = FlowRegistryUtils.getRestrictedComponents(requestSnapshot, serviceFacade);
                    restrictedComponents.forEach(restrictedComponent -> {
                        final ComponentAuthorizable restrictedComponentAuthorizable = lookup.getConfigurableComponent(restrictedComponent);
                        authorizeRestrictions(authorizer, restrictedComponentAuthorizable);
                    });
                },
                () -> {
                    // Nothing to verify
                },
                (importRequest) -> {
                    final VersionedReportingTaskSnapshot snapshot = importRequest.getReportingTaskSnapshot();
                    final VersionedReportingTaskImportResponseEntity responseEntity = serviceFacade.importReportingTasks(snapshot);
                    return generateOkResponse(responseEntity).build();
                }
        );
    }

    // -------------------
    // flow-analysis-rules
    // -------------------

    /**
     * Creates a new Flow Analysis Rule.
     *
     * @param requestFlowAnalysisRuleEntity A flowAnalysisRuleEntity.
     * @return A flowAnalysisRuleEntity.0
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules")
    @Operation(
            summary = "Creates a new flow analysis rule",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = FlowAnalysisRuleEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services - /controller-services/{uuid}"),
                    @SecurityRequirement(name = "Write - if the Flow Analysis Rule is restricted - /restricted-components")
            }
    )
    public Response createFlowAnalysisRule(
            @Parameter(description = "The flow analysis rule configuration details.", required = true) final FlowAnalysisRuleEntity requestFlowAnalysisRuleEntity
    ) {

        if (requestFlowAnalysisRuleEntity == null || requestFlowAnalysisRuleEntity.getComponent() == null) {
            throw new IllegalArgumentException("Flow analysis rule details must be specified.");
        }

        if (
                requestFlowAnalysisRuleEntity.getRevision() == null
                        || (requestFlowAnalysisRuleEntity.getRevision().getVersion() == null
                        || requestFlowAnalysisRuleEntity.getRevision().getVersion() != 0)
        ) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Flow analysis rule.");
        }

        final FlowAnalysisRuleDTO requestFlowAnalysisRule = requestFlowAnalysisRuleEntity.getComponent();
        if (requestFlowAnalysisRule.getId() != null) {
            throw new IllegalArgumentException("Flow analysis rule ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestFlowAnalysisRule.getType())) {
            throw new IllegalArgumentException("The type of flow analysis rule to create must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestFlowAnalysisRuleEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestFlowAnalysisRuleEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestFlowAnalysisRuleEntity,
                lookup -> {
                    authorizeController(RequestAction.WRITE);

                    ComponentAuthorizable authorizable = null;
                    try {
                        authorizable = lookup.getConfigurableComponent(requestFlowAnalysisRule.getType(), requestFlowAnalysisRule.getBundle());

                        if (authorizable.isRestricted()) {
                            authorizeRestrictions(authorizer, authorizable);
                        }

                        if (requestFlowAnalysisRule.getProperties() != null) {
                            AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestFlowAnalysisRule.getProperties(), authorizable, authorizer, lookup);
                        }
                    } finally {
                        if (authorizable != null) {
                            authorizable.cleanUpResources();
                        }
                    }
                },
                () -> serviceFacade.verifyCreateFlowAnalysisRule(requestFlowAnalysisRule),
                (flowAnalysisRuleEntity) -> {
                    final FlowAnalysisRuleDTO flowAnalysisRule = flowAnalysisRuleEntity.getComponent();

                    // set the processor id as appropriate
                    flowAnalysisRule.setId(generateUuid());

                    // create the flow analysis rule and generate the json
                    final Revision revision = getRevision(flowAnalysisRuleEntity, flowAnalysisRule.getId());
                    final FlowAnalysisRuleEntity entity = serviceFacade.createFlowAnalysisRule(revision, flowAnalysisRule);
                    populateRemainingFlowAnalysisRuleEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Clears the state for a flow analysis rule.
     *
     * @param id The id of the flow analysis rule
     * @return a componentStateEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules/{id}/state/clear-requests")
    @Operation(
            summary = "Clears the state for a flow analysis rule",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ComponentStateEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /flow-analysis-rules/{uuid}")
            }
    )
    public Response clearState(
            @Parameter(
                    description = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final FlowAnalysisRuleEntity requestFlowAnalysisRuleEntity = new FlowAnalysisRuleEntity();
        requestFlowAnalysisRuleEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestFlowAnalysisRuleEntity,
                lookup -> authorizeController(RequestAction.WRITE),
                () -> serviceFacade.verifyCanClearFlowAnalysisRuleState(id),
                (flowAnalysisRuleEntity) -> {
                    // get the component state
                    serviceFacade.clearFlowAnalysisRuleState(flowAnalysisRuleEntity.getId());

                    // generate the response entity
                    final ComponentStateEntity entity = new ComponentStateEntity();

                    // generate the response
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified Flow Analysis Rule.
     *
     * @param id The id of the flow analysis rule to update.
     * @param requestFlowAnalysisRuleEntity A flowAnalysisRuleEntity.
     * @return A flowAnalysisRuleEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules/{id}")
    @Operation(
            summary = "Updates a flow analysis rule",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowAnalysisRuleEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /flow-analysis-rules/{uuid}"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services if this request changes the reference - /controller-services/{uuid}")
            }
    )
    public Response updateFlowAnalysisRule(
            @Parameter(
                    description = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The flow analysis rule configuration details.",
                    required = true
            ) final FlowAnalysisRuleEntity requestFlowAnalysisRuleEntity) {

        if (requestFlowAnalysisRuleEntity == null || requestFlowAnalysisRuleEntity.getComponent() == null) {
            throw new IllegalArgumentException("Flow analysis rule details must be specified.");
        }

        if (requestFlowAnalysisRuleEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final FlowAnalysisRuleDTO requestFlowAnalysisRuleDTO = requestFlowAnalysisRuleEntity.getComponent();
        if (!id.equals(requestFlowAnalysisRuleDTO.getId())) {
            throw new IllegalArgumentException(String.format("The flow analysis rule id (%s) in the request body does not equal the "
                    + "flow analysis rule id of the requested resource (%s).", requestFlowAnalysisRuleDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestFlowAnalysisRuleEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestFlowAnalysisRuleEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestFlowAnalysisRuleEntity, id);
        return withWriteLock(
                serviceFacade,
                requestFlowAnalysisRuleEntity,
                requestRevision,
                lookup -> {
                    // authorize flow analysis rule
                    authorizeController(RequestAction.WRITE);

                    final ComponentAuthorizable authorizable = lookup.getFlowAnalysisRule(id);

                    // authorize any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestFlowAnalysisRuleDTO.getProperties(), authorizable, authorizer, lookup);
                },
                () -> serviceFacade.verifyUpdateFlowAnalysisRule(requestFlowAnalysisRuleDTO),
                (revision, flowAnalysisRuleEntity) -> {
                    final FlowAnalysisRuleDTO flowAnalysisRuleDTO = flowAnalysisRuleEntity.getComponent();

                    // update the flow analysis rule
                    final FlowAnalysisRuleEntity entity = serviceFacade.updateFlowAnalysisRule(revision, flowAnalysisRuleDTO);
                    populateRemainingFlowAnalysisRuleEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified flow analysis rule.
     *
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the flow analysis rule to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules/{id}")
    @Operation(
            summary = "Deletes a flow analysis rule",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowAnalysisRuleEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /flow-analysis-rules/{uuid}"),
                    @SecurityRequirement(name = "Write - /controller"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services - /controller-services/{uuid}")
            }
    )
    public Response removeFlowAnalysisRule(
            @Parameter(
                    description = "The revision is used to verify the client is working with the latest version of the flow."
            )
            @QueryParam(VERSION) LongParameter version,
            @Parameter(
                    description = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response."
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @Parameter(
                    description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(
                    description = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final FlowAnalysisRuleEntity requestFlowAnalysisRuleEntity = new FlowAnalysisRuleEntity();
        requestFlowAnalysisRuleEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestFlowAnalysisRuleEntity,
                requestRevision,
                lookup -> {
                    final ComponentAuthorizable flowAnalysisRule = lookup.getFlowAnalysisRule(id);

                    authorizeController(RequestAction.WRITE);

                    // verify any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(flowAnalysisRule, authorizer, lookup, false);
                },
                () -> serviceFacade.verifyDeleteFlowAnalysisRule(id),
                (revision, flowAnalysisRuleEntity) -> {
                    // delete the specified flow analysis rule
                    final FlowAnalysisRuleEntity entity = serviceFacade.deleteFlowAnalysisRule(revision, flowAnalysisRuleEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the operational status for the specified FlowAnalysisRule with the specified values.
     *
     * @param id The id of the flow analysis rule to update.
     * @param requestRunStatus A runStatusEntity.
     * @return A flowAnalysisRuleEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules/{id}/run-status")
    @Operation(
            summary = "Updates run status of a flow analysis rule",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowAnalysisRuleEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /flow-analysis-rules/{uuid} or  or /operation/flow-analysis-rules/{uuid}")
            }
    )
    public Response updateRunStatus(
            @Parameter(
                    description = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The flow analysis rule run status.",
                    required = true
            ) final FlowAnalysisRuleRunStatusEntity requestRunStatus) {

        if (requestRunStatus == null) {
            throw new IllegalArgumentException("Flow analysis rule run status must be specified.");
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
                lookup -> authorizeController(RequestAction.WRITE),
                () -> serviceFacade.verifyUpdateFlowAnalysisRule(createFlowAnalysisRuleDtoWithDesiredRunStatus(id, requestRunStatus.getState())),
                (revision, flowAnalysisRuleRunStatusEntity) -> {
                    // update the flow analysis rule
                    final FlowAnalysisRuleEntity entity = serviceFacade.updateFlowAnalysisRule(revision, createFlowAnalysisRuleDtoWithDesiredRunStatus(id, flowAnalysisRuleRunStatusEntity.getState()));
                    populateRemainingFlowAnalysisRuleEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    private FlowAnalysisRuleDTO createFlowAnalysisRuleDtoWithDesiredRunStatus(final String id, final String runStatus) {
        final FlowAnalysisRuleDTO dto = new FlowAnalysisRuleDTO();
        dto.setId(id);
        dto.setState(runStatus);
        return dto;
    }

    /**
     * Populate the uri's for the specified flow analysis rules.
     *
     * @param flowAnalysisRuleEntities flow analysis rules
     * @return dtos
     */
    private Set<FlowAnalysisRuleEntity> populateRemainingFlowAnalysisRuleEntitiesContent(final Set<FlowAnalysisRuleEntity> flowAnalysisRuleEntities) {
        for (FlowAnalysisRuleEntity flowAnalysisRuleEntity : flowAnalysisRuleEntities) {
            populateRemainingFlowAnalysisRuleEntityContent(flowAnalysisRuleEntity);
        }
        return flowAnalysisRuleEntities;
    }

    /**
     * Populate the uri's for the specified flow analysis rule.
     *
     * @param flowAnalysisRuleEntity flow analysis rule
     * @return dtos
     */
    private FlowAnalysisRuleEntity populateRemainingFlowAnalysisRuleEntityContent(final FlowAnalysisRuleEntity flowAnalysisRuleEntity) {
        flowAnalysisRuleEntity.setUri(generateResourceUri("controller/flow-analysis-rules", flowAnalysisRuleEntity.getId()));

        return flowAnalysisRuleEntity;
    }

    //

    /**
     * Retrieves all the flow analysis rules in this NiFi.
     *
     * @return A flowAnalysisRulesEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules")
    @Operation(
            summary = "Gets all flow analysis rules",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowAnalysisRulesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getFlowAnalysisRules() {
        authorizeController(RequestAction.READ);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get all the flow analysis rules
        final Set<FlowAnalysisRuleEntity> flowAnalysisRules = serviceFacade.getFlowAnalysisRules();
        populateRemainingFlowAnalysisRuleEntitiesContent(flowAnalysisRules);

        // create the response entity
        final FlowAnalysisRulesEntity entity = new FlowAnalysisRulesEntity();
        entity.setFlowAnalysisRules(flowAnalysisRules);
        entity.setCurrentTime(new Date());

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the specified flow analysis rule.
     *
     * @param id The id of the flow analysis rule to retrieve
     * @return A flowAnalysisRuleEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules/{id}")
    @Operation(
            summary = "Gets a flow analysis rule",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowAnalysisRuleEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow-analysis-rules/{uuid}")
            }
    )
    public Response getFlowAnalysisRule(
            @Parameter(
                    description = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") final String id
    ) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        authorizeController(RequestAction.READ);

        // get the flow analysis rule
        final FlowAnalysisRuleEntity flowAnalysisRule = serviceFacade.getFlowAnalysisRule(id);
        populateRemainingFlowAnalysisRuleEntityContent(flowAnalysisRule);

        return generateOkResponse(flowAnalysisRule).build();
    }

    /**
     * Returns the descriptor for the specified property.
     *
     * @param id The id of the flow analysis rule.
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules/{id}/descriptors")
    @Operation(
            summary = "Gets a flow analysis rule property descriptor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PropertyDescriptorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow-analysis-rules/{uuid}")
            }
    )
    public Response getFlowAnalysisRulePropertyDescriptor(
            @Parameter(
                    description = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The property name.",
                    required = true
            )
            @QueryParam("propertyName") final String propertyName,
            @Parameter(description = "Property Descriptor requested sensitive status")
            @QueryParam("sensitive") final boolean sensitive
    ) {

        // ensure the property name is specified
        if (propertyName == null) {
            throw new IllegalArgumentException("The property name must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        authorizeController(RequestAction.READ);

        // get the property descriptor
        final PropertyDescriptorDTO descriptor = serviceFacade.getFlowAnalysisRulePropertyDescriptor(id, propertyName, sensitive);

        // generate the response entity
        final PropertyDescriptorEntity entity = new PropertyDescriptorEntity();
        entity.setPropertyDescriptor(descriptor);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the state for a flow analysis rule.
     *
     * @param id The id of the flow analysis rule
     * @return a componentStateEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules/{id}/state")
    @Operation(
            summary = "Gets the state for a flow analysis rule",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ComponentStateEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /flow-analysis-rules/{uuid}")
            }
    )
    public Response getFlowAnalysisRuleState(
            @Parameter(
                    description = "The flow analysis rule id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        authorizeController(RequestAction.READ);

        // get the component state
        final ComponentStateDTO state = serviceFacade.getFlowAnalysisRuleState(id);

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setComponentState(state);

        // generate the response
        return generateOkResponse(entity).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules/{id}/config/analysis")
    @Operation(
            summary = "Performs analysis of the component's configuration, providing information about which attributes are referenced.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ConfigurationAnalysisEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow-analysis-rules/{uuid}")
            }
    )
    public Response analyzeFlowAnalysisRuleConfiguration(
            @Parameter(description = "The flow analysis rules id.", required = true) @PathParam("id") final String flowAnalysisRuleId,
            @Parameter(description = "The configuration analysis request.", required = true) final ConfigurationAnalysisEntity configurationAnalysis) {

        if (configurationAnalysis == null || configurationAnalysis.getConfigurationAnalysis() == null) {
            throw new IllegalArgumentException("Flow Analysis Rules's configuration must be specified");
        }

        final ConfigurationAnalysisDTO dto = configurationAnalysis.getConfigurationAnalysis();
        if (dto.getComponentId() == null) {
            throw new IllegalArgumentException("Flow Analysis Rule's identifier must be specified in the request");
        }

        if (!dto.getComponentId().equals(flowAnalysisRuleId)) {
            throw new IllegalArgumentException("Flow Analysis Rule's identifier in the request must match the identifier provided in the URL");
        }

        if (dto.getProperties() == null) {
            throw new IllegalArgumentException("Flow Analysis Rule's properties must be specified in the request");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, configurationAnalysis);
        }

        return withWriteLock(
                serviceFacade,
                configurationAnalysis,
                lookup -> authorizeController(RequestAction.READ),
                () -> {
                },
                entity -> {
                    final ConfigurationAnalysisDTO analysis = entity.getConfigurationAnalysis();
                    final ConfigurationAnalysisEntity resultsEntity = serviceFacade.analyzeFlowAnalysisRuleConfiguration(analysis.getComponentId(), analysis.getProperties());
                    return generateOkResponse(resultsEntity).build();
                }
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules/{id}/config/verification-requests")
    @Operation(
            summary = "Performs verification of the Flow Analysis Rule's configuration",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VerifyConfigRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This will initiate the process of verifying a given Flow Analysis Rule configuration. This may be a long-running task. As a result, this endpoint will immediately return " +
                    "a " +
                    "FlowAnalysisRuleConfigVerificationRequestEntity, and the process of performing the verification will occur asynchronously in the background. " +
                    "The client may then periodically poll the status of the request by " +
                    "issuing a GET request to /flow-analysis-rules/{taskId}/verification-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
                    "/flow-analysis-rules/{serviceId}/verification-requests/{requestId}.",
            security = {
                    @SecurityRequirement(name = "Read - /flow-analysis-rules/{uuid}")
            }
    )
    public Response submitFlowAnalysisRuleConfigVerificationRequest(
            @Parameter(description = "The flow analysis rules id.", required = true)
            @PathParam("id") final String flowAnalysisRuleId,
            @Parameter(description = "The flow analysis rules configuration verification request.", required = true) final VerifyConfigRequestEntity flowAnalysisRuleConfigRequest) {

        if (flowAnalysisRuleConfigRequest == null) {
            throw new IllegalArgumentException("Flow Analysis Rule's configuration must be specified");
        }

        final VerifyConfigRequestDTO requestDto = flowAnalysisRuleConfigRequest.getRequest();
        if (requestDto == null || requestDto.getProperties() == null) {
            throw new IllegalArgumentException("Flow Analysis Rule Properties must be specified");
        }

        if (requestDto.getComponentId() == null) {
            throw new IllegalArgumentException("Flow Analysis Rule's identifier must be specified in the request");
        }

        if (!requestDto.getComponentId().equals(flowAnalysisRuleId)) {
            throw new IllegalArgumentException("Flow Analysis Rule's identifier in the request must match the identifier provided in the URL");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, flowAnalysisRuleConfigRequest);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return withWriteLock(
                serviceFacade,
                flowAnalysisRuleConfigRequest,
                lookup -> authorizeController(RequestAction.READ),
                () -> serviceFacade.verifyCanVerifyFlowAnalysisRuleConfig(flowAnalysisRuleId),
                entity -> performAsyncFlowAnalysisRuleConfigVerification(entity, user)
        );
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules/{id}/config/verification-requests/{requestId}")
    @Operation(
            summary = "Returns the Verification Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VerifyConfigRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Returns the Verification Request with the given ID. Once an Verification Request has been created, "
                    + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
                    + "current state of the request, and any failures. ",
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can get it")
            }
    )
    public Response getFlowAnalysisRuleVerificationRequest(
            @Parameter(description = "The ID of the Flow Analysis Rule")
            @PathParam("id") final String flowAnalysisRuleId,
            @Parameter(description = "The ID of the Verification Request")
            @PathParam("requestId") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest =
                configVerificationRequestManager.getRequest(VERIFICATION_REQUEST_TYPE, requestId, user);

        final VerifyConfigRequestEntity updateRequestEntity = createVerifyFlowAnalysisRuleConfigRequestEntity(asyncRequest, requestId);
        return generateOkResponse(updateRequestEntity).build();
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("flow-analysis-rules/{id}/config/verification-requests/{requestId}")
    @Operation(
            summary = "Deletes the Verification Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VerifyConfigRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Deletes the Verification Request with the given ID. After a request is created, it is expected "
                    + "that the client will properly clean up the request by DELETE'ing it, once the Verification process has completed. If the request is deleted before the request "
                    + "completes, then the Verification request will finish the step that it is currently performing and then will cancel any subsequent steps.",
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can remove it")
            }
    )
    public Response deleteFlowAnalysisRuleVerificationRequest(
            @Parameter(description = "The ID of the Flow Analysis Rule")
            @PathParam("id") final String flowAnalysisRuleId,
            @Parameter(description = "The ID of the Verification Request")
            @PathParam("requestId") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final boolean twoPhaseRequest = isTwoPhaseRequest(httpServletRequest);
        final boolean executionPhase = isExecutionPhase(httpServletRequest);

        // If this is a standalone node, or if this is the execution phase of the request, perform the actual request.
        if (!twoPhaseRequest || executionPhase) {
            // request manager will ensure that the current is the user that submitted this request
            final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest =
                    configVerificationRequestManager.removeRequest(VERIFICATION_REQUEST_TYPE, requestId, user);

            if (asyncRequest == null) {
                throw new ResourceNotFoundException("Could not find request of type " + VERIFICATION_REQUEST_TYPE + " with ID " + requestId);
            }

            if (!asyncRequest.isComplete()) {
                asyncRequest.cancel();
            }

            final VerifyConfigRequestEntity updateRequestEntity = createVerifyFlowAnalysisRuleConfigRequestEntity(asyncRequest, requestId);
            return generateOkResponse(updateRequestEntity).build();
        }

        if (isValidationPhase(httpServletRequest)) {
            // Perform authorization by attempting to get the request
            configVerificationRequestManager.getRequest(VERIFICATION_REQUEST_TYPE, requestId, user);
            return generateContinueResponse().build();
        } else if (isCancellationPhase(httpServletRequest)) {
            return generateOkResponse().build();
        } else {
            throw new IllegalStateException("This request does not appear to be part of the two phase commit.");
        }
    }

    public Response performAsyncFlowAnalysisRuleConfigVerification(final VerifyConfigRequestEntity configRequest, final NiFiUser user) {
        // Create an asynchronous request that will occur in the background, because this request may take an indeterminate amount of time.
        final String requestId = generateUuid();

        final VerifyConfigRequestDTO requestDto = configRequest.getRequest();
        final String taskId = requestDto.getComponentId();
        final List<UpdateStep> updateSteps = Collections.singletonList(new StandardUpdateStep("Verify Flow Analysis Rule Configuration"));

        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> request =
                new StandardAsynchronousWebRequest<>(requestId, configRequest, taskId, user, updateSteps);

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>>> verificationTask = asyncRequest -> {
            try {
                final List<ConfigVerificationResultDTO> results = serviceFacade.performFlowAnalysisRuleConfigVerification(taskId, requestDto.getProperties());
                asyncRequest.markStepComplete(results);
            } catch (final Exception e) {
                LOGGER.error("Failed to verify Flow Analysis Rule configuration", e);
                asyncRequest.fail("Failed to verify Flow Analysis Rule configuration due to " + e);
            }
        };

        configVerificationRequestManager.submitRequest(VERIFICATION_REQUEST_TYPE, requestId, request, verificationTask);

        // Generate the response
        final VerifyConfigRequestEntity resultsEntity = createVerifyFlowAnalysisRuleConfigRequestEntity(request, requestId);
        return generateOkResponse(resultsEntity).build();
    }

    private VerifyConfigRequestEntity createVerifyFlowAnalysisRuleConfigRequestEntity(
            final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest, final String requestId) {

        final VerifyConfigRequestDTO requestDto = asyncRequest.getRequest().getRequest();
        final List<ConfigVerificationResultDTO> resultsList = asyncRequest.getResults();

        final VerifyConfigRequestDTO dto = new VerifyConfigRequestDTO();
        dto.setComponentId(requestDto.getComponentId());
        dto.setProperties(requestDto.getProperties());
        dto.setResults(resultsList);

        dto.setComplete(asyncRequest.isComplete());
        dto.setFailureReason(asyncRequest.getFailureReason());
        dto.setLastUpdated(asyncRequest.getLastUpdated());
        dto.setPercentCompleted(asyncRequest.getPercentComplete());
        dto.setRequestId(requestId);
        dto.setState(asyncRequest.getState());
        dto.setUri(generateResourceUri("controller/flow-analysis-rules", requestDto.getComponentId(), "config", "verification-requests", requestId));

        final VerifyConfigRequestEntity entity = new VerifyConfigRequestEntity();
        entity.setRequest(dto);
        return entity;
    }
    //--

    // ----------
    // registries
    // ----------

    /**
     * Lists existing clients.
     *
     * @return A FlowRegistryClientsEntity which contains a set of FlowRegistryClientEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registry-clients")
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
    public Response getFlowRegistryClients() {
        authorizeController(RequestAction.READ);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final Set<FlowRegistryClientEntity> flowRegistryClients = serviceFacade.getRegistryClients();
        final FlowRegistryClientsEntity flowRegistryClientEntities = new FlowRegistryClientsEntity();
        flowRegistryClientEntities.setCurrentTime(new Date());
        flowRegistryClientEntities.setRegistries(flowRegistryClients);

        return generateOkResponse(populateRemainingRegistryClientEntityContent(flowRegistryClientEntities)).build();
    }

    /**
     * Creates a new flow registry client.
     *
     * @param requestFlowRegistryClientEntity A registryClientEntity.
     * @return A FlowRegistryClientEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registry-clients")
    @Operation(
            summary = "Creates a new flow registry client",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = FlowRegistryClientEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller")
            }
    )
    public Response createFlowRegistryClient(
            @Parameter(
                    description = "The flow registry client configuration details.",
                    required = true
            ) final FlowRegistryClientEntity requestFlowRegistryClientEntity) {
        authorizeController(RequestAction.READ);

        if (requestFlowRegistryClientEntity == null || requestFlowRegistryClientEntity.getComponent() == null) {
            throw new IllegalArgumentException("Flow Registry client details must be specified.");
        }

        if (requestFlowRegistryClientEntity.getRevision() == null
                || (requestFlowRegistryClientEntity.getRevision().getVersion() == null
                || requestFlowRegistryClientEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Flow Registry.");
        }

        final FlowRegistryClientDTO requestRegistryClient = requestFlowRegistryClientEntity.getComponent();
        if (requestRegistryClient.getId() != null) {
            throw new IllegalArgumentException("Flow Registry ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestRegistryClient.getName())) {
            throw new IllegalArgumentException("Flow Registry name must be specified.");
        }

        if (requestRegistryClient.getType() == null) {
            throw new IllegalArgumentException("The flow registry client type must be specified.");
        }

        if (serviceFacade.getRegistryClients().stream().anyMatch(rce -> requestRegistryClient.getName().equals(rce.getComponent().getName()))) {
            throw new IllegalArgumentException("A Flow Registry already exists with the name " + requestRegistryClient.getName());
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestFlowRegistryClientEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestFlowRegistryClientEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestFlowRegistryClientEntity,
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                null,
                (registryClientEntity) -> {
                    final FlowRegistryClientDTO flowRegistryClient = registryClientEntity.getComponent();

                    // set the processor id as appropriate
                    flowRegistryClient.setId(generateUuid());

                    // create the reporting task and generate the json
                    final Revision revision = getRevision(registryClientEntity, flowRegistryClient.getId());
                    final FlowRegistryClientEntity entity = serviceFacade.createRegistryClient(revision, flowRegistryClient);
                    populateRemainingRegistryClientEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    /**
     * Retrieves the specified flow registry client.
     *
     * @param id The id of the flow registry client to retrieve
     * @return A flowRegistryClientEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/registry-clients/{id}")
    @Operation(
            summary = "Gets a flow registry client",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowRegistryClientEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller")
            }
    )
    public Response getFlowRegistryClient(
            @Parameter(
                    description = "The flow registry client id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        authorizeController(RequestAction.READ);

        // get the flow registry client
        final FlowRegistryClientEntity entity = serviceFacade.getRegistryClient(id);
        return generateOkResponse(populateRemainingRegistryClientEntityContent(entity)).build();
    }

    /**
     * Updates the specified flow registry client.
     *
     * @param id The id of the flow registry client to update.
     * @param requestFlowRegistryClientEntity A flowRegistryClientEntity.
     * @return A flowRegistryClientEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/registry-clients/{id}")
    @Operation(
            summary = "Updates a flow registry client",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowRegistryClientEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller")
            }
    )
    public Response updateFlowRegistryClient(
            @Parameter(
                    description = "The flow registry client id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The flow registry client configuration details.",
                    required = true
            ) final FlowRegistryClientEntity requestFlowRegistryClientEntity) {

        if (requestFlowRegistryClientEntity == null || requestFlowRegistryClientEntity.getComponent() == null) {
            throw new IllegalArgumentException("Flow registry client details must be specified.");
        }

        if (requestFlowRegistryClientEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // authorize access
        authorizeController(RequestAction.WRITE);

        // ensure the ids are the same
        final FlowRegistryClientDTO requestRegistryClient = requestFlowRegistryClientEntity.getComponent();
        if (!id.equals(requestRegistryClient.getId())) {
            throw new IllegalArgumentException(String.format("The flow registry client id (%s) in the request body does not equal the "
                    + " id of the requested resource (%s).", requestRegistryClient.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestFlowRegistryClientEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestFlowRegistryClientEntity.isDisconnectedNodeAcknowledged());
        }

        if (requestRegistryClient.getName() != null && StringUtils.isBlank(requestRegistryClient.getName())) {
            throw new IllegalArgumentException("Flow registry client name must be specified.");
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestFlowRegistryClientEntity, id);
        return withWriteLock(
                serviceFacade,
                requestFlowRegistryClientEntity,
                requestRevision,
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                null,
                (revision, registryClientEntity) -> {
                    final FlowRegistryClientDTO registry = registryClientEntity.getComponent();

                    // update the controller service
                    final FlowRegistryClientEntity entity = serviceFacade.updateRegistryClient(revision, registry);

                    return generateOkResponse(populateRemainingRegistryClientEntityContent(entity)).build();
                }
        );
    }

    /**
     * Removes the specified flow registry client.
     *
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the flow registry client to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/registry-clients/{id}")
    @Operation(
            summary = "Deletes a flow registry client",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowRegistryClientEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller")
            }
    )
    public Response deleteFlowRegistryClient(
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
                    description = "The flow registry client id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        // authorize access
        authorizeController(RequestAction.WRITE);

        final FlowRegistryClientEntity requestFlowRegistryClientEntity = new FlowRegistryClientEntity();
        requestFlowRegistryClientEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestFlowRegistryClientEntity,
                requestRevision,
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                () -> serviceFacade.verifyDeleteRegistry(id),
                (revision, registryClientEntity) -> {
                    // delete the specified flow registry client
                    final FlowRegistryClientEntity entity = serviceFacade.deleteRegistryClient(revision, registryClientEntity.getId());
                    return generateOkResponse(populateRemainingRegistryClientEntityContent(entity)).build();
                }
        );
    }

    /**
     * Returns the descriptor for the specified property.
     *
     * @param id The id of the flow registry client.
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/registry-clients/{id}/descriptors")
    @Operation(
            summary = "Gets a flow registry client property descriptor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PropertyDescriptorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller/registry-clients/{uuid}")
            }
    )
    public Response getPropertyDescriptor(
            @Parameter(
                    description = "The flow registry client id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The property name.",
                    required = true
            )
            @QueryParam("propertyName") final String propertyName,
            @Parameter(description = "Property Descriptor requested sensitive status")
            @QueryParam("sensitive") final boolean sensitive
    ) {

        // ensure the property name is specified
        if (propertyName == null) {
            throw new IllegalArgumentException("The property name must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        authorizeController(RequestAction.READ);

        // get the property descriptor
        final PropertyDescriptorDTO descriptor = serviceFacade.getRegistryClientPropertyDescriptor(id, propertyName, sensitive);

        // generate the response entity
        final PropertyDescriptorEntity entity = new PropertyDescriptorEntity();
        entity.setPropertyDescriptor(descriptor);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Retrieves the types of flow registry clients that this NiFi supports.
     *
     * @return A flowRegistryTypesEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("registry-types")
    @Operation(
            summary = "Retrieves the types of flow  that this NiFi supports",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = FlowRegistryClientTypesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /flow")
            }
    )
    public Response getRegistryClientTypes() {
        // authorize access
        authorizeController(RequestAction.READ);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final FlowRegistryClientTypesEntity entity = new FlowRegistryClientTypesEntity();
        entity.setFlowRegistryClientTypes(serviceFacade.getFlowRegistryTypes());

        // generate the response
        return generateOkResponse(entity).build();
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

    // -------------------
    // bulletin
    // -------------------

    /**
     * Creates a Bulletin.
     *
     * @param requestBulletinEntity A bulletinEntity.
     * @return A bulletinEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("bulletin")
    @Operation(
            summary = "Creates a new bulletin",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = BulletinEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller")
            }
    )
    public Response createBulletin(
            @Parameter(
                    description = "The reporting task configuration details.",
                    required = true
            ) final BulletinEntity requestBulletinEntity) {

        if (requestBulletinEntity == null || requestBulletinEntity.getBulletin() == null) {
            throw new IllegalArgumentException("Bulletin details must be specified.");
        }

        final BulletinDTO requestBulletin = requestBulletinEntity.getBulletin();
        if (requestBulletin.getId() != null) {
            throw new IllegalArgumentException("A bulletin ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestBulletin.getMessage())) {
            throw new IllegalArgumentException("The bulletin message must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestBulletinEntity);
        }

        return withWriteLock(
                serviceFacade,
                requestBulletinEntity,
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                null,
                (bulletinEntity) -> {
                    final BulletinDTO bulletin = bulletinEntity.getBulletin();
                    final BulletinEntity entity = serviceFacade.createBulletin(bulletin, true);
                    return generateOkResponse(entity).build();
                }
        );
    }

    // -------------------
    // controller services
    // -------------------

    /**
     * Creates a new Controller Service.
     *
     * @param requestControllerServiceEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("controller-services")
    @Operation(
            summary = "Creates a new controller service",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = ControllerServiceEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services - /controller-services/{uuid}"),
                    @SecurityRequirement(name = "Write - if the Controller Service is restricted - /restricted-components")
            }
    )
    public Response createControllerService(
            @Parameter(
                    description = "The controller service configuration details.",
                    required = true
            ) final ControllerServiceEntity requestControllerServiceEntity) {

        if (requestControllerServiceEntity == null || requestControllerServiceEntity.getComponent() == null) {
            throw new IllegalArgumentException("Controller service details must be specified.");
        }

        if (requestControllerServiceEntity.getRevision() == null
                || (requestControllerServiceEntity.getRevision().getVersion() == null || requestControllerServiceEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Controller service.");
        }

        final ControllerServiceDTO requestControllerService = requestControllerServiceEntity.getComponent();
        if (requestControllerService.getId() != null) {
            throw new IllegalArgumentException("Controller service ID cannot be specified.");
        }

        if (requestControllerService.getParentGroupId() != null) {
            throw new IllegalArgumentException("Parent process group ID cannot be specified.");
        }

        if (StringUtils.isBlank(requestControllerService.getType())) {
            throw new IllegalArgumentException("The type of controller service to create must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestControllerServiceEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestControllerServiceEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestControllerServiceEntity,
                lookup -> {
                    authorizeController(RequestAction.WRITE);

                    ComponentAuthorizable authorizable = null;
                    try {
                        authorizable = lookup.getConfigurableComponent(requestControllerService.getType(), requestControllerService.getBundle());

                        if (authorizable.isRestricted()) {
                            authorizeRestrictions(authorizer, authorizable);
                        }

                        if (requestControllerService.getProperties() != null) {
                            AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestControllerService.getProperties(), authorizable, authorizer, lookup);
                        }
                    } finally {
                        if (authorizable != null) {
                            authorizable.cleanUpResources();
                        }
                    }
                },
                () -> serviceFacade.verifyCreateControllerService(requestControllerService),
                (controllerServiceEntity) -> {
                    final ControllerServiceDTO controllerService = controllerServiceEntity.getComponent();

                    // set the processor id as appropriate
                    controllerService.setId(generateUuid());

                    // create the controller service and generate the json
                    final Revision revision = getRevision(controllerServiceEntity, controllerService.getId());
                    final ControllerServiceEntity entity = serviceFacade.createControllerService(revision, null, controllerService);
                    controllerServiceResource.populateRemainingControllerServiceEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getUri()), entity).build();
                }
        );
    }

    // -------
    // cluster
    // -------

    /**
     * Gets the contents of this NiFi cluster. This includes all nodes and their status.
     *
     * @return A clusterEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster")
    @Operation(
            summary = "Gets the contents of the cluster",
            description = "Returns the contents of the cluster including all nodes and their status.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ClusterEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller")
            }
    )
    public Response getCluster() {

        authorizeController(RequestAction.READ);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET, getClusterCoordinatorNode());
        }

        final ClusterDTO dto = serviceFacade.getCluster();

        // create entity
        final ClusterEntity entity = new ClusterEntity();
        entity.setCluster(dto);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the contents of the specified node in this NiFi cluster.
     *
     * @param id The node id.
     * @return A nodeEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/nodes/{id}")
    @Operation(
            summary = "Gets a node in the cluster",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = NodeEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller")
            }
    )
    public Response getNode(
            @Parameter(
                    description = "The node id.",
                    required = true
            )
            @PathParam("id") String id) {

        authorizeController(RequestAction.READ);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET, getClusterCoordinatorNode());
        }

        // get the specified relationship
        final NodeDTO dto = serviceFacade.getNode(id);

        // create the response entity
        final NodeEntity entity = new NodeEntity();
        entity.setNode(dto);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Updates the contents of the specified node in this NiFi cluster.
     *
     * @param id The id of the node
     * @param nodeEntity A nodeEntity
     * @return A nodeEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/nodes/{id}")
    @Operation(
            summary = "Updates a node in the cluster",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = NodeEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller")
            }
    )
    public Response updateNode(
            @Parameter(
                    description = "The node id.",
                    required = true
            )
            @PathParam("id") String id,
            @Parameter(
                    description = "The node configuration. The only configuration that will be honored at this endpoint is the status.",
                    required = true
            ) NodeEntity nodeEntity) {

        authorizeController(RequestAction.WRITE);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        if (nodeEntity == null || nodeEntity.getNode() == null) {
            throw new IllegalArgumentException("Node details must be specified.");
        }

        // get the request node
        final NodeDTO requestNodeDTO = nodeEntity.getNode();
        if (!id.equals(requestNodeDTO.getNodeId())) {
            throw new IllegalArgumentException(String.format("The node id (%s) in the request body does "
                    + "not equal the node id of the requested resource (%s).", requestNodeDTO.getNodeId(), id));
        }

        if (isReplicateRequest()) {
            return replicateToCoordinator(HttpMethod.PUT, nodeEntity);
        }

        // update the node
        final NodeDTO node = serviceFacade.updateNode(requestNodeDTO);

        // create the response entity
        NodeEntity entity = new NodeEntity();
        entity.setNode(node);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Removes the specified from this NiFi cluster.
     *
     * @param id The id of the node
     * @return A nodeEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("cluster/nodes/{id}")
    @Operation(
            summary = "Removes a node from the cluster",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = NodeEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller")
            }
    )
    public Response deleteNode(
            @Parameter(
                    description = "The node id.",
                    required = true
            )
            @PathParam("id") String id) {

        authorizeController(RequestAction.WRITE);

        // ensure connected to the cluster
        if (!isConnectedToCluster()) {
            throw new IllegalClusterResourceRequestException("Only a node connected to a cluster can process the request.");
        }

        if (isReplicateRequest()) {
            return replicateToCoordinator(HttpMethod.DELETE, getRequestParameters());
        }

        serviceFacade.deleteNode(id);

        // create the response entity
        final NodeEntity entity = new NodeEntity();

        // generate the response
        return generateOkResponse(entity).build();
    }

    // -------
    // history
    // -------

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("status/history")
    @Operation(
            summary = "Gets status history for the node",
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
                    @SecurityRequirement(name = "Read - /controller")
            }
    )
    public Response getNodeStatusHistory() {
        authorizeController(RequestAction.READ);

        // replicate if cluster manager
        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // generate the response
        return generateOkResponse(serviceFacade.getNodeStatusHistory()).build();
    }

    /**
     * Deletes flow history from the specified end date.
     *
     * @param endDate The end date for the purge action.
     * @return A historyEntity
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("history")
    @Operation(
            summary = "Purges history",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = HistoryEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller")
            }
    )
    public Response deleteHistory(
            @Parameter(
                    description = "Purge actions before this date/time.",
                    required = true
            )
            @QueryParam("endDate") DateTimeParameter endDate) {

        // ensure the end date is specified
        if (endDate == null) {
            throw new IllegalArgumentException("The end date must be specified.");
        }

        // Note: History requests are not replicated throughout the cluster and are instead handled by the nodes independently

        return withWriteLock(
                serviceFacade,
                new EndDateEntity(endDate.getDateTime()),
                lookup -> {
                    authorizeController(RequestAction.WRITE);
                },
                null,
                (endDateEntity) -> {
                    // purge the actions
                    serviceFacade.deleteActions(endDateEntity.getEndDate());

                    // generate the response
                    return generateOkResponse(new HistoryEntity()).build();
                }
        );
    }

    private class EndDateEntity extends Entity {
        final Date endDate;

        public EndDateEntity(Date endDate) {
            this.endDate = endDate;
        }

        public Date getEndDate() {
            return endDate;
        }
    }

    // ------------
    // NARs
    // ------------

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("nar-manager/nars/content")
    @Operation(
            summary = "Uploads a NAR and requests for it to be installed",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = NarSummaryEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller")
            }
    )
    public Response uploadNar(
            @HeaderParam(FILENAME_HEADER)
            final String filename,
            @Parameter(description = "The contents of the NAR file.", required = true)
            final InputStream inputStream) throws IOException {

        authorizeController(RequestAction.WRITE);

        if (StringUtils.isBlank(filename)) {
            throw new IllegalArgumentException(FILENAME_HEADER + " header is required");
        }
        if (inputStream == null) {
            throw new IllegalArgumentException("NAR contents are required");
        }

        // If clustered and not all nodes are connected, do not allow uploading a NAR.
        // Generally, we allow the flow to be modified when nodes are disconnected, but we do not allow uploading a NAR because
        // the cluster has no mechanism for synchronizing those NARs after the upload.
        final ClusterCoordinator clusterCoordinator = getClusterCoordinator();
        if (clusterCoordinator != null) {
            final Set<NodeIdentifier> disconnectedNodes = clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTING, NodeConnectionState.DISCONNECTED, NodeConnectionState.DISCONNECTING);
            if (!disconnectedNodes.isEmpty()) {
                throw new IllegalStateException("Cannot upload NAR because the following %s nodes are not currently connected: %s".formatted(disconnectedNodes.size(), disconnectedNodes));
            }
        }

        final long startTime = System.currentTimeMillis();
        final InputStream maxLengthInputStream = new MaxLengthInputStream(inputStream, (long) DataUnit.GB.toB(1));

        if (isReplicateRequest()) {
            final UploadRequest<NarSummaryEntity> uploadRequest = new UploadRequest.Builder<NarSummaryEntity>()
                    .user(NiFiUserUtils.getNiFiUser())
                    .filename(filename)
                    .identifier(UUID.randomUUID().toString())
                    .contents(maxLengthInputStream)
                    .header(FILENAME_HEADER, filename)
                    .header(CONTENT_TYPE_HEADER, UPLOAD_CONTENT_TYPE)
                    .exampleRequestUri(getAbsolutePath())
                    .responseClass(NarSummaryEntity.class)
                    .successfulResponseStatus(HttpResponseStatus.OK.getCode())
                    .build();
            final NarSummaryEntity summaryEntity = uploadRequestReplicator.upload(uploadRequest);
            return generateOkResponse(summaryEntity).build();
        }

        final NarSummaryEntity summaryEntity = serviceFacade.uploadNar(maxLengthInputStream);
        final NarSummaryDTO summary = summaryEntity.getNarSummary();

        final long elapsedTime = System.currentTimeMillis() - startTime;
        LOGGER.info("Upload completed for NAR [{}] in {} ms", summary.getIdentifier(), elapsedTime);

        return generateOkResponse(summaryEntity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/nar-manager/nars")
    @Operation(
            summary = "Retrieves summary information for installed NARs",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = NarSummariesEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller")
            }
    )
    public Response getNarSummaries() {
        authorizeController(RequestAction.READ);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final NarSummariesEntity summariesEntity = new NarSummariesEntity();
        summariesEntity.setNarSummaries(serviceFacade.getNarSummaries());
        summariesEntity.setCurrentTime(new Date());

        return generateOkResponse(summariesEntity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/nar-manager/nars/{id}")
    @Operation(
            summary = "Retrieves the summary information for the NAR with the given identifier",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = NarDetailsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller")
            }
    )
    public Response getNarSummary(
            @PathParam("id")
            @Parameter(description = "The id of the NAR.", required = true)
            final String id) {
        authorizeController(RequestAction.READ);

        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Id is required");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final NarSummaryEntity summaryEntity = serviceFacade.getNarSummary(id);
        return generateOkResponse(summaryEntity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/nar-manager/nars/{id}/details")
    @Operation(
            summary = "Retrieves the component types available from the installed NARs",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = NarDetailsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller")
            }
    )
    public Response getNarDetails(
            @PathParam("id")
            @Parameter(description = "The id of the NAR.", required = true)
            final String id) {
        authorizeController(RequestAction.READ);

        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Id is required");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final NarDetailsEntity detailsEntity = serviceFacade.getNarDetails(id);
        return generateOkResponse(detailsEntity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/nar-manager/nars/{id}/content")
    @Operation(
            summary = "Retrieves the content of the NAR with the given id",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = byte[].class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller")
            }
    )
    public Response downloadNar(
            @PathParam("id")
            @Parameter(description = "The id of the NAR.", required = true)
            final String id) {
        authorizeController(RequestAction.READ);

        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Id is required");
        }

        final NarSummaryEntity summaryEntity = serviceFacade.getNarSummary(id);
        final NarSummaryDTO summaryDTO = summaryEntity.getNarSummary();
        final NarCoordinateDTO coordinateDTO = summaryDTO.getCoordinate();
        final String filename = coordinateDTO.getArtifact() + "-" + coordinateDTO.getVersion() + ".nar";

        final StreamingOutput streamingOutput = (outputStream) -> {
            try (final InputStream narInputStream = serviceFacade.readNar(id)) {
                narInputStream.transferTo(outputStream);
            }
        };

        return generateOkResponse(streamingOutput)
                .header(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", filename))
                .build();
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/nar-manager/nars/{id}")
    @Operation(
            summary = "Deletes an installed NAR",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = NarSummaryEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller")
            }
    )
    public Response deleteNar(
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false")
            final Boolean disconnectedNodeAcknowledged,
            @QueryParam("force") @DefaultValue("false")
            final Boolean forceDelete,
            @PathParam("id")
            @Parameter(description = "The id of the NAR.", required = true)
            final String id) throws IOException {

        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Id is required");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final NarSummaryEntity summaryEntity = new NarSummaryEntity(new NarSummaryDTO(id));

        return withWriteLock(
                serviceFacade,
                summaryEntity,
                lookup -> authorizeController(RequestAction.WRITE),
                () -> serviceFacade.verifyDeleteNar(id, forceDelete),
                requestEntity -> {
                    try {
                        final String requestId = requestEntity.getNarSummary().getIdentifier();
                        final NarSummaryEntity deletedNarSummary = serviceFacade.deleteNar(requestId);
                        return generateOkResponse(deletedNarSummary).build();
                    } catch (IOException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                });
    }

    @Autowired
    public void setServiceFacade(final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Autowired
    public void setReportingTaskResource(final ReportingTaskResource reportingTaskResource) {
        this.reportingTaskResource = reportingTaskResource;
    }

    @Autowired
    public void setParameterProviderResource(final ParameterProviderResource parameterProviderResource) {
        this.parameterProviderResource = parameterProviderResource;
    }

    @Autowired
    public void setControllerServiceResource(final ControllerServiceResource controllerServiceResource) {
        this.controllerServiceResource = controllerServiceResource;
    }

    @Autowired
    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Autowired(required = false)
    public void setUploadRequestReplicator(final UploadRequestReplicator uploadRequestReplicator) {
        this.uploadRequestReplicator = uploadRequestReplicator;
    }

}
