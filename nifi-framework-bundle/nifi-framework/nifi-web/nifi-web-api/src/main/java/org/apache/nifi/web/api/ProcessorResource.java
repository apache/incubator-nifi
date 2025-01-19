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
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AuthorizeControllerServiceReference;
import org.apache.nifi.authorization.AuthorizeParameterReference;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.controller.BackoffMechanism;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.UiExtensionType;
import org.apache.nifi.web.api.concurrent.AsyncRequestManager;
import org.apache.nifi.web.api.concurrent.AsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.RequestManager;
import org.apache.nifi.web.api.concurrent.StandardAsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.StandardUpdateStep;
import org.apache.nifi.web.api.concurrent.UpdateStep;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.ConfigurationAnalysisDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.VerifyConfigRequestDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConfigurationAnalysisEntity;
import org.apache.nifi.web.api.entity.ProcessorDiagnosticsEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorRunStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorsRunStatusDetailsEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.RunStatusDetailsRequestEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * RESTful endpoint for managing a Processor.
 */
@Controller
@Path("/processors")
@Tag(name = "Processors")
public class ProcessorResource extends ApplicationResource {
    private static final Logger logger = LoggerFactory.getLogger(ProcessorResource.class);

    private static final String VERIFICATION_REQUEST_TYPE = "verification-request";
    private RequestManager<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> configVerificationRequestManager =
            new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Verify Processor Config Thread");

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    @Context
    private ServletContext servletContext;

    /**
     * Populate the uri's for the specified processors and their relationships.
     *
     * @param processorEntities processors
     * @return dtos
     */
    public Set<ProcessorEntity> populateRemainingProcessorEntitiesContent(Set<ProcessorEntity> processorEntities) {
        for (ProcessorEntity processorEntity : processorEntities) {
            populateRemainingProcessorEntityContent(processorEntity);
        }
        return processorEntities;
    }

    /**
     * Populate the uri's for the specified processors and their relationships.
     *
     * @param processorEntity processors
     * @return dtos
     */
    public ProcessorEntity populateRemainingProcessorEntityContent(ProcessorEntity processorEntity) {
        processorEntity.setUri(generateResourceUri("processors", processorEntity.getId()));

        // populate remaining content
        if (processorEntity.getComponent() != null) {
            populateRemainingProcessorContent(processorEntity.getComponent());
        }
        return processorEntity;
    }

    /**
     * Populate the uri's for the specified processors and their relationships.
     *
     * @param processorDiagnosticsEntity processor's diagnostics entity
     * @return processor diagnostics entity
     */
    public ProcessorDiagnosticsEntity populateRemainingProcessorDiagnosticsEntityContent(ProcessorDiagnosticsEntity processorDiagnosticsEntity) {
        processorDiagnosticsEntity.setUri(generateResourceUri("processors", processorDiagnosticsEntity.getId(), "diagnostics"));

        // populate remaining content
        if (processorDiagnosticsEntity.getComponent() != null && processorDiagnosticsEntity.getComponent().getProcessor() != null) {
            populateRemainingProcessorContent(processorDiagnosticsEntity.getComponent().getProcessor());
        }
        return processorDiagnosticsEntity;
    }

    /**
     * Populate the uri's for the specified processor and its relationships.
     */
    public ProcessorDTO populateRemainingProcessorContent(ProcessorDTO processor) {
        // get the config details and see if there is a custom ui for this processor type
        ProcessorConfigDTO config = processor.getConfig();
        if (config != null) {
            final BundleDTO bundle = processor.getBundle();

            // see if this processor has any ui extensions
            final UiExtensionMapping uiExtensionMapping = (UiExtensionMapping) servletContext.getAttribute("nifi-ui-extensions");
            if (uiExtensionMapping.hasUiExtension(processor.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion())) {
                final List<UiExtension> uiExtensions = uiExtensionMapping.getUiExtension(processor.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
                for (final UiExtension uiExtension : uiExtensions) {
                    if (UiExtensionType.ProcessorConfiguration.equals(uiExtension.getExtensionType())) {
                        config.setCustomUiUrl(generateExternalUiUri(uiExtension.getContextPath()));
                    }
                }
            }
        }

        return processor;
    }

    /**
     * Retrieves the specified processor.
     *
     * @param id The id of the processor to retrieve.
     * @return A processorEntity.
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    @Operation(
            summary = "Gets a processor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /processors/{uuid}")
            }
    )
    public Response getProcessor(
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
            final Authorizable processor = lookup.getProcessor(id).getAuthorizable();
            processor.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the specified processor
        final ProcessorEntity entity = serviceFacade.getProcessor(id);
        populateRemainingProcessorEntityContent(entity);

        // generate the response
        return generateOkResponse(entity).build();
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/run-status-details/queries")
    @Operation(
            summary = "Submits a query to retrieve the run status details of all processors that are in the given list of Processor IDs",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessorsRunStatusDetailsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /processors/{uuid} for each processor whose run status information is requested")
            }
    )
    public Response getProcessorRunStatusDetails(
            @Parameter(description = "The request for the processors that should be included in the results") final RunStatusDetailsRequestEntity requestEntity) {

        if (requestEntity.getProcessorIds() == null) {
            throw new IllegalArgumentException("List of Processor IDs must be provided");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestEntity);
        }

        return withWriteLock(serviceFacade,
                requestEntity,
                lookup -> {
                },
                null,
                providedEntity -> {
                    final ProcessorsRunStatusDetailsEntity entity = serviceFacade.getProcessorsRunStatusDetails(requestEntity.getProcessorIds(), NiFiUserUtils.getNiFiUser());
                    return generateOkResponse(entity).build();
                });
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/threads")
    @Operation(
            summary = "Terminates a processor, essentially \"deleting\" its threads and any active tasks",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /processors/{uuid} or /operation/processors/{uuid}")
            }
    )
    public Response terminateProcessor(
            @Parameter(description = "The processor id.", required = true) @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        }

        final ProcessorEntity requestProcessorEntity = new ProcessorEntity();
        requestProcessorEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestProcessorEntity,
                lookup -> {
                    final Authorizable authorizable = lookup.getProcessor(id).getAuthorizable();
                    OperationAuthorizable.authorizeOperation(authorizable, authorizer, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyTerminateProcessor(id),
                processorEntity -> {
                    final ProcessorEntity entity = serviceFacade.terminateProcessor(processorEntity.getId());
                    populateRemainingProcessorEntityContent(entity);

                    return generateOkResponse(entity).build();
                });
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/diagnostics")
    @Operation(
            summary = "Gets diagnostics information about a processor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Read - /processors/{uuid}")
            }
    )
    public Response getProcessorDiagnostics(
            @Parameter(description = "The processor id.", required = true) @PathParam("id") final String id) throws InterruptedException {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processor = lookup.getProcessor(id).getAuthorizable();
            processor.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the specified processor's diagnostics
        final ProcessorDiagnosticsEntity entity = serviceFacade.getProcessorDiagnostics(id);
        populateRemainingProcessorDiagnosticsEntityContent(entity);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Returns the descriptor for the specified property.
     *
     * @param id The id of the processor
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/descriptors")
    @Operation(
            summary = "Gets the descriptor for a processor property",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PropertyDescriptorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /processors/{uuid}")
            }
    )
    public Response getPropertyDescriptor(
            @Parameter(
                    description = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response."
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @Parameter(
                    description = "The processor id.",
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
    ) throws InterruptedException {

        // ensure the property name is specified
        if (propertyName == null) {
            throw new IllegalArgumentException("The property name must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processor = lookup.getProcessor(id).getAuthorizable();
            processor.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the property descriptor
        final PropertyDescriptorDTO descriptor = serviceFacade.getProcessorPropertyDescriptor(id, propertyName, sensitive);

        // generate the response entity
        final PropertyDescriptorEntity entity = new PropertyDescriptorEntity();
        entity.setPropertyDescriptor(descriptor);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the state for a processor.
     *
     * @param id The id of the processor
     * @return a componentStateEntity
     * @throws InterruptedException if interrupted
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/state")
    @Operation(
            summary = "Gets the state for a processor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ComponentStateEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /processors/{uuid}")
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
            final Authorizable processor = lookup.getProcessor(id).getAuthorizable();
            processor.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        // get the component state
        final ComponentStateDTO state = serviceFacade.getProcessorState(id);

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setComponentState(state);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Clears the state for a processor.
     *
     * @param id The id of the processor
     * @return a componentStateEntity
     * @throws InterruptedException if interrupted
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state/clear-requests")
    @Operation(
            summary = "Clears the state for a processor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ComponentStateEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /processors/{uuid}")
            }
    )
    public Response clearState(
            @Parameter(
                    description = "The processor id.",
                    required = true
            )
            @PathParam("id") final String id) throws InterruptedException {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final ProcessorEntity requestProcessorEntity = new ProcessorEntity();
        requestProcessorEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestProcessorEntity,
                lookup -> {
                    final Authorizable processor = lookup.getProcessor(id).getAuthorizable();
                    processor.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyCanClearProcessorState(id),
                (processorEntity) -> {
                    // get the component state
                    serviceFacade.clearProcessorState(processorEntity.getId());

                    // generate the response entity
                    final ComponentStateEntity entity = new ComponentStateEntity();

                    // generate the response
                    return generateOkResponse(entity).build();
                }
        );
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/config/analysis")
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
                    @SecurityRequirement(name = "Read - /processors/{uuid}")
            }
    )
    public Response analyzeConfiguration(
            @Parameter(description = "The processor id.", required = true) @PathParam("id") final String processorId,
            @Parameter(description = "The processor configuration analysis request.", required = true) final ConfigurationAnalysisEntity configurationAnalysis) {

        if (configurationAnalysis == null || configurationAnalysis.getConfigurationAnalysis() == null) {
            throw new IllegalArgumentException("Processor's configuration must be specified");
        }

        final ConfigurationAnalysisDTO dto = configurationAnalysis.getConfigurationAnalysis();
        if (dto.getComponentId() == null) {
            throw new IllegalArgumentException("Processor's identifier must be specified in the request");
        }

        if (!dto.getComponentId().equals(processorId)) {
            throw new IllegalArgumentException("Processor's identifier in the request must match the identifier provided in the URL");
        }

        if (dto.getProperties() == null) {
            throw new IllegalArgumentException("Processor's properties must be specified in the request");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, configurationAnalysis);
        }

        return withWriteLock(
                serviceFacade,
                configurationAnalysis,
                lookup -> {
                    final ComponentAuthorizable processor = lookup.getProcessor(processorId);
                    processor.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                },
                entity -> {
                    final ConfigurationAnalysisDTO analysis = entity.getConfigurationAnalysis();
                    final ConfigurationAnalysisEntity resultsEntity = serviceFacade.analyzeProcessorConfiguration(analysis.getComponentId(), analysis.getProperties());
                    return generateOkResponse(resultsEntity).build();
                }
        );
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/config/verification-requests")
    @Operation(
            summary = "Performs verification of the Processor's configuration",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VerifyConfigRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This will initiate the process of verifying a given Processor configuration. This may be a long-running task. As a result, this endpoint will immediately return a " +
                    "ProcessorConfigVerificationRequestEntity, and the process of performing the verification will occur asynchronously in the background. " +
                    "The client may then periodically poll the status of the request by " +
                    "issuing a GET request to /processors/{processorId}/verification-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
                    "/processors/{processorId}/verification-requests/{requestId}.",
            security = {
                    @SecurityRequirement(name = "Read - /processors/{uuid}")
            }
    )
    public Response submitProcessorVerificationRequest(
            @Parameter(description = "The processor id.", required = true) @PathParam("id") final String processorId,
            @Parameter(description = "The processor configuration verification request.", required = true) final VerifyConfigRequestEntity processorConfigRequest) {

        if (processorConfigRequest == null) {
            throw new IllegalArgumentException("Processor's configuration must be specified");
        }

        final VerifyConfigRequestDTO requestDto = processorConfigRequest.getRequest();
        if (requestDto == null || requestDto.getProperties() == null) {
            throw new IllegalArgumentException("Processor's properties must be specified");
        }

        if (requestDto.getComponentId() == null) {
            throw new IllegalArgumentException("Processor's identifier must be specified in the request");
        }

        if (!requestDto.getComponentId().equals(processorId)) {
            throw new IllegalArgumentException("Processor's identifier in the request must match the identifier provided in the URL");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, processorConfigRequest);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return withWriteLock(
                serviceFacade,
                processorConfigRequest,
                lookup -> {
                    final ComponentAuthorizable processor = lookup.getProcessor(processorId);
                    processor.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                    serviceFacade.verifyCanVerifyProcessorConfig(processorId);
                },
                entity -> performAsyncConfigVerification(entity, user)
        );
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/config/verification-requests/{requestId}")
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
    public Response getVerificationRequest(
            @Parameter(description = "The ID of the Processor") @PathParam("id") final String processorId,
            @Parameter(description = "The ID of the Verification Request") @PathParam("requestId") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest = configVerificationRequestManager
                .getRequest(VERIFICATION_REQUEST_TYPE, requestId, user);
        final VerifyConfigRequestEntity updateRequestEntity = createVerifyProcessorConfigRequestEntity(asyncRequest, requestId);
        return generateOkResponse(updateRequestEntity).build();
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/config/verification-requests/{requestId}")
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
    public Response deleteVerificationRequest(
            @Parameter(description = "The ID of the Processor") @PathParam("id") final String processorId,
            @Parameter(description = "The ID of the Verification Request") @PathParam("requestId") final String requestId) {

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

            if (!asyncRequest.isComplete()) {
                asyncRequest.cancel();
            }

            final VerifyConfigRequestEntity updateRequestEntity = createVerifyProcessorConfigRequestEntity(asyncRequest, requestId);
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


    /**
     * Updates the specified processor with the specified values.
     *
     * @param id The id of the processor to update.
     * @param requestProcessorEntity A processorEntity.
     * @return A processorEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    @Operation(
            summary = "Updates a processor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /processors/{uuid}"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services if this request changes the reference - /controller-services/{uuid}")
            }
    )
    public Response updateProcessor(
            @Parameter(
                    description = "The processor id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The processor configuration details.",
                    required = true
            ) final ProcessorEntity requestProcessorEntity) {

        if (requestProcessorEntity == null || requestProcessorEntity.getComponent() == null) {
            throw new IllegalArgumentException("Processor details must be specified.");
        }

        if (requestProcessorEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the same id is being used
        final ProcessorDTO requestProcessorDTO = requestProcessorEntity.getComponent();
        if (!id.equals(requestProcessorDTO.getId())) {
            throw new IllegalArgumentException(String.format("The processor id (%s) in the request body does "
                    + "not equal the processor id of the requested resource (%s).", requestProcessorDTO.getId(), id));
        }

        final PositionDTO proposedPosition = requestProcessorDTO.getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        final ProcessorConfigDTO processorConfig = requestProcessorDTO.getConfig();
        if (processorConfig != null) {
            if (processorConfig.getRetryCount() != null && processorConfig.getRetryCount() < 0) {
                throw new IllegalArgumentException("Retry Count should not be less than zero.");
            }

            if (processorConfig.getBackoffMechanism() != null) {
                try {
                    BackoffMechanism.valueOf(processorConfig.getBackoffMechanism());
                } catch (Exception e) {
                    throw new IllegalArgumentException("Backoff Mechanism " + processorConfig.getBackoffMechanism() + " is invalid.");
                }
            }

            if (processorConfig.getMaxBackoffPeriod() != null && !FormatUtils.TIME_DURATION_PATTERN.matcher(processorConfig.getMaxBackoffPeriod()).matches()) {
                throw new IllegalArgumentException("Max Backoff Period should be specified as time, for example 5 mins");
            }
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestProcessorEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestProcessorEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestProcessorEntity, id);
        return withWriteLock(
                serviceFacade,
                requestProcessorEntity,
                requestRevision,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();

                    final ComponentAuthorizable authorizable = lookup.getProcessor(id);
                    authorizable.getAuthorizable().authorize(authorizer, RequestAction.WRITE, user);

                    final ProcessorConfigDTO config = requestProcessorDTO.getConfig();
                    if (config != null) {
                        AuthorizeControllerServiceReference.authorizeControllerServiceReferences(config.getProperties(), authorizable, authorizer, lookup);
                        AuthorizeParameterReference.authorizeParameterReferences(config.getProperties(), authorizer, authorizable.getParameterContext(), user);
                    }
                },
                () -> serviceFacade.verifyUpdateProcessor(requestProcessorDTO),
                (revision, processorEntity) -> {
                    final ProcessorDTO processorDTO = processorEntity.getComponent();

                    // update the processor
                    final ProcessorEntity entity = serviceFacade.updateProcessor(revision, processorDTO);
                    populateRemainingProcessorEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified processor.
     *
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the processor to remove.
     * @return A processorEntity.
     * @throws InterruptedException if interrupted
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}")
    @Operation(
            summary = "Deletes a processor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /processors/{uuid}"),
                    @SecurityRequirement(name = "Write - Parent Process Group - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services - /controller-services/{uuid}")
            }
    )
    public Response deleteProcessor(
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
                    description = "The processor id.",
                    required = true
            )
            @PathParam("id") final String id) throws InterruptedException {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final ProcessorEntity requestProcessorEntity = new ProcessorEntity();
        requestProcessorEntity.setId(id);

        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestProcessorEntity,
                requestRevision,
                lookup -> {
                    final ComponentAuthorizable processor = lookup.getProcessor(id);

                    // ensure write permission to the processor
                    processor.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    processor.getAuthorizable().getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // verify any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(processor, authorizer, lookup, false);
                },
                () -> serviceFacade.verifyDeleteProcessor(id),
                (revision, processorEntity) -> {
                    // delete the processor
                    final ProcessorEntity entity = serviceFacade.deleteProcessor(revision, processorEntity.getId());

                    // generate the response
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the operational status for the specified processor with the specified values.
     *
     * @param id The id of the processor to update.
     * @param requestRunStatus A processorEntity.
     * @return A processorEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/run-status")
    @Operation(
            summary = "Updates run status of a processor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ProcessorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /processors/{uuid} or /operation/processors/{uuid}")
            }
    )
    public Response updateRunStatus(
            @Parameter(
                    description = "The processor id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The processor run status.",
                    required = true
            ) final ProcessorRunStatusEntity requestRunStatus) {

        if (requestRunStatus == null) {
            throw new IllegalArgumentException("Processor run status must be specified.");
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

                    final Authorizable authorizable = lookup.getProcessor(id).getAuthorizable();
                    OperationAuthorizable.authorizeOperation(authorizable, authorizer, user);
                },
                () -> serviceFacade.verifyUpdateProcessor(createDTOWithDesiredRunStatus(id, requestRunStatus.getState())),
                (revision, runStatusEntity) -> {
                    // update the processor
                    final ProcessorEntity entity = serviceFacade.updateProcessor(revision, createDTOWithDesiredRunStatus(id, runStatusEntity.getState()));
                    populateRemainingProcessorEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    private ProcessorDTO createDTOWithDesiredRunStatus(final String id, final String runStatus) {
        final ProcessorDTO dto = new ProcessorDTO();
        dto.setId(id);
        dto.setState(runStatus);
        return dto;
    }

    public Response performAsyncConfigVerification(final VerifyConfigRequestEntity processorConfigRequest, final NiFiUser user) {
        // Create an asynchronous request that will occur in the background, because this request may take an indeterminate amount of time.
        final String requestId = generateUuid();
        logger.debug("Generated Config Verification Request with ID {} for Processor {}", requestId, processorConfigRequest.getRequest().getComponentId());

        final VerifyConfigRequestDTO requestDto = processorConfigRequest.getRequest();
        final String processorId = requestDto.getComponentId();
        final List<UpdateStep> updateSteps = Collections.singletonList(new StandardUpdateStep("Verify Processor Configuration"));

        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> request =
                new StandardAsynchronousWebRequest<>(requestId, processorConfigRequest, processorId, user, updateSteps);

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>>> updateTask = asyncRequest -> {
            try {
                final Map<String, String> attributes = requestDto.getAttributes() == null ? Collections.emptyMap() : requestDto.getAttributes();
                final List<ConfigVerificationResultDTO> results = serviceFacade.performProcessorConfigVerification(processorId, requestDto.getProperties(), attributes);
                asyncRequest.markStepComplete(results);
            } catch (final Exception e) {
                logger.error("Failed to verify Processor configuration", e);
                asyncRequest.fail("Failed to verify Processor configuration due to " + e);
            }
        };

        configVerificationRequestManager.submitRequest(VERIFICATION_REQUEST_TYPE, requestId, request, updateTask);

        // Generate the response
        final VerifyConfigRequestEntity resultsEntity = createVerifyProcessorConfigRequestEntity(request, requestId);
        return generateOkResponse(resultsEntity).build();
    }

    private VerifyConfigRequestEntity createVerifyProcessorConfigRequestEntity(
            final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest,
            final String requestId) {
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
        dto.setUri(generateResourceUri("processors", requestDto.getComponentId(), "config", "verification-requests", requestId));

        final VerifyConfigRequestEntity entity = new VerifyConfigRequestEntity();
        entity.setRequest(dto);
        return entity;
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
