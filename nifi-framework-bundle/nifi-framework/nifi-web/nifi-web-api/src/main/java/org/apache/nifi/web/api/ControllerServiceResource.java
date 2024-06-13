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
import org.apache.nifi.authorization.ProcessGroupAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
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
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.VerifyConfigRequestDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConfigurationAnalysisEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.ControllerServiceRunStatusEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.UpdateControllerServiceReferenceRequestEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * RESTful endpoint for managing a Controller Service.
 */
@Controller
@Path("/controller-services")
@Tag(name = "Controller Services")
public class ControllerServiceResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(ControllerServiceResource.class);
    private static final String VERIFICATION_REQUEST_TYPE = "verification-request";
    private RequestManager<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> configVerificationRequestManager =
            new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Verify Controller Service Config Thread");

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    @Context
    private ServletContext servletContext;

    /**
     * Populate the uri's for the specified controller services.
     *
     * @param controllerServiceEntities controller services
     * @return dtos
     */
    public Set<ControllerServiceEntity> populateRemainingControllerServiceEntitiesContent(final Set<ControllerServiceEntity> controllerServiceEntities) {
        for (ControllerServiceEntity controllerServiceEntity : controllerServiceEntities) {
            populateRemainingControllerServiceEntityContent(controllerServiceEntity);
        }
        return controllerServiceEntities;
    }

    /**
     * Populate the uri's for the specified controller service.
     *
     * @param controllerServiceEntity controller service
     * @return dtos
     */
    public ControllerServiceEntity populateRemainingControllerServiceEntityContent(final ControllerServiceEntity controllerServiceEntity) {
        // populate the controller service href
        controllerServiceEntity.setUri(generateResourceUri("controller-services", controllerServiceEntity.getId()));

        // populate the remaining content
        if (controllerServiceEntity.getComponent() != null) {
            populateRemainingControllerServiceContent(controllerServiceEntity.getComponent());
        }
        return controllerServiceEntity;
    }

    /**
     * Populates the uri for the specified controller service.
     */
    public ControllerServiceDTO populateRemainingControllerServiceContent(final ControllerServiceDTO controllerService) {
        final BundleDTO bundle = controllerService.getBundle();

        // see if this processor has any ui extensions
        final UiExtensionMapping uiExtensionMapping = (UiExtensionMapping) servletContext.getAttribute("nifi-ui-extensions");
        if (uiExtensionMapping.hasUiExtension(controllerService.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion())) {
            final List<UiExtension> uiExtensions = uiExtensionMapping.getUiExtension(controllerService.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
            for (final UiExtension uiExtension : uiExtensions) {
                if (UiExtensionType.ControllerServiceConfiguration.equals(uiExtension.getExtensionType())) {
                    controllerService.setCustomUiUrl(generateExternalUiUri(uiExtension.getContextPath()));
                }
            }
        }

        return controllerService;
    }

    /**
     * Retrieves the specified controller service.
     *
     * @param id The id of the controller service to retrieve
     * @return A controllerServiceEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Gets a controller service",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerServiceEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller-services/{uuid}")
            },
            description = "If the uiOnly query parameter is provided with a value of true, the returned entity may only contain fields that are necessary for rendering the NiFi User Interface. As " +
                    "such, " +
                    "the selected fields may change at any time, even during incremental releases, without warning. As a result, this parameter should not be provided by any client other than the UI."
    )
    public Response getControllerService(
            @Parameter(
                    description = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id,
            @QueryParam("uiOnly") @DefaultValue("false") final boolean uiOnly) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable controllerService = lookup.getControllerService(id).getAuthorizable();
            controllerService.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the controller service
        final ControllerServiceEntity entity = serviceFacade.getControllerService(id, true);
        if (uiOnly) {
            stripNonUiRelevantFields(entity);
        }
        populateRemainingControllerServiceEntityContent(entity);

        return generateOkResponse(entity).build();
    }


    /**
     * Returns the descriptor for the specified property.
     *
     * @param id The id of the controller service.
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/descriptors")
    @Operation(
            summary = "Gets a controller service property descriptor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PropertyDescriptorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller-services/{uuid}")
            }
    )
    public Response getPropertyDescriptor(
            @Parameter(
                    description = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The property name to return the descriptor for.",
                    required = true
            )
            @QueryParam("propertyName") final String propertyName,
            @Parameter(
                    description = "Property Descriptor requested sensitive status"
            )
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
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable controllerService = lookup.getControllerService(id).getAuthorizable();
            controllerService.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the property descriptor
        final PropertyDescriptorDTO descriptor = serviceFacade.getControllerServicePropertyDescriptor(id, propertyName, sensitive);

        // generate the response entity
        final PropertyDescriptorEntity entity = new PropertyDescriptorEntity();
        entity.setPropertyDescriptor(descriptor);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the state for a controller service.
     *
     * @param id The id of the controller service
     * @return a componentStateEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state")
    @Operation(
            summary = "Gets the state for a controller service",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ComponentStateEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller-services/{uuid}")
            }
    )
    public Response getState(
            @Parameter(
                    description = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable controllerService = lookup.getControllerService(id).getAuthorizable();
            controllerService.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        // get the component state
        final ComponentStateDTO state = serviceFacade.getControllerServiceState(id);

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setComponentState(state);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Clears the state for a controller service.
     *
     * @param id The id of the controller service
     * @return a componentStateEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state/clear-requests")
    @Operation(
            summary = "Clears the state for a controller service",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ComponentStateEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller-services/{uuid}")
            }
    )
    public Response clearState(
            @Parameter(
                    description = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final ControllerServiceEntity requestControllerServiceEntity = new ControllerServiceEntity();
        requestControllerServiceEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestControllerServiceEntity,
                lookup -> {
                    final Authorizable controllerService = lookup.getControllerService(id).getAuthorizable();
                    controllerService.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyCanClearControllerServiceState(id),
                (controllerServiceEntity) -> {
                    // get the component state
                    serviceFacade.clearControllerServiceState(controllerServiceEntity.getId());

                    // generate the response entity
                    final ComponentStateEntity entity = new ComponentStateEntity();

                    // generate the response
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Retrieves the references of the specified controller service.
     *
     * @param id The id of the controller service to retrieve
     * @return A controllerServiceEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/references")
    @Operation(
            summary = "Gets a controller service",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerServiceReferencingComponentsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /controller-services/{uuid}")
            }
    )
    public Response getControllerServiceReferences(
            @Parameter(
                    description = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable controllerService = lookup.getControllerService(id).getAuthorizable();
            controllerService.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the controller service
        final ControllerServiceReferencingComponentsEntity entity = serviceFacade.getControllerServiceReferencingComponents(id);

        return generateOkResponse(entity).build();
    }

    /**
     * Updates the references of the specified controller service.
     *
     * @param requestUpdateReferenceRequest The update request
     * @return A controllerServiceReferencingComponentsEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/references")
    @Operation(
            summary = "Updates a controller services references",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerServiceReferencingComponentsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /{component-type}/{uuid} or /operate/{component-type}/{uuid} - For each referencing component specified")
            }
    )
    public Response updateControllerServiceReferences(
            @Parameter(
                    description = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The controller service request update request.",
                    required = true
            ) final UpdateControllerServiceReferenceRequestEntity requestUpdateReferenceRequest) {

        if (requestUpdateReferenceRequest == null || requestUpdateReferenceRequest.getId() == null) {
            throw new IllegalArgumentException("The controller service identifier must be specified.");
        }

        if (requestUpdateReferenceRequest.getReferencingComponentRevisions() == null) {
            throw new IllegalArgumentException("The controller service referencing components revisions must be specified.");
        }

        // parse the state to determine the desired action
        // need to consider controller service state first as it shares a state with
        // scheduled state (disabled) which is applicable for referencing services
        // but not referencing schedulable components
        ControllerServiceState requestControllerServiceState = null;
        try {
            requestControllerServiceState = ControllerServiceState.valueOf(requestUpdateReferenceRequest.getState());
        } catch (final IllegalArgumentException ignored) {
        }

        ScheduledState requestScheduledState = null;
        try {
            requestScheduledState = ScheduledState.valueOf(requestUpdateReferenceRequest.getState());
        } catch (final IllegalArgumentException ignored) {
        }

        // ensure an action has been specified
        if (requestScheduledState == null && requestControllerServiceState == null) {
            throw new IllegalArgumentException("Must specify the updated state. To update referencing Processors "
                    + "and Reporting Tasks the state should be RUNNING or STOPPED. To update the referencing Controller Services the "
                    + "state should be ENABLED or DISABLED.");
        }

        // ensure the controller service state is not ENABLING or DISABLING
        if (requestControllerServiceState != null
                && (ControllerServiceState.ENABLING.equals(requestControllerServiceState) || ControllerServiceState.DISABLING.equals(requestControllerServiceState))) {

            throw new IllegalArgumentException("Cannot set the referencing services to ENABLING or DISABLING");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestUpdateReferenceRequest);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestUpdateReferenceRequest.isDisconnectedNodeAcknowledged());
        }

        // convert the referencing revisions
        final Map<String, Revision> requestReferencingRevisions = requestUpdateReferenceRequest.getReferencingComponentRevisions().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    final RevisionDTO rev = e.getValue();
                    return new Revision(rev.getVersion(), rev.getClientId(), e.getKey());
                }));
        final Set<Revision> requestRevisions = new HashSet<>(requestReferencingRevisions.values());

        final ScheduledState verifyScheduledState = requestScheduledState;
        final ControllerServiceState verifyControllerServiceState = requestControllerServiceState;
        return withWriteLock(
                serviceFacade,
                requestUpdateReferenceRequest,
                requestRevisions,
                lookup -> {
                    requestReferencingRevisions.entrySet().stream().forEach(e -> {
                        final Authorizable controllerService = lookup.getControllerServiceReferencingComponent(id, e.getKey());
                        OperationAuthorizable.authorizeOperation(controllerService, authorizer, NiFiUserUtils.getNiFiUser());
                    });
                },
                () -> serviceFacade.verifyUpdateControllerServiceReferencingComponents(requestUpdateReferenceRequest.getId(), verifyScheduledState, verifyControllerServiceState),
                (revisions, updateReferenceRequest) -> {
                    ScheduledState scheduledState = null;
                    try {
                        scheduledState = ScheduledState.valueOf(updateReferenceRequest.getState());
                    } catch (final IllegalArgumentException ignored) {
                    }

                    ControllerServiceState controllerServiceState = null;
                    try {
                        controllerServiceState = ControllerServiceState.valueOf(updateReferenceRequest.getState());
                    } catch (final IllegalArgumentException ignored) {
                    }

                    final Map<String, Revision> referencingRevisions = updateReferenceRequest.getReferencingComponentRevisions().entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                                final RevisionDTO rev = e.getValue();
                                return new Revision(rev.getVersion(), rev.getClientId(), e.getKey());
                            }));

                    // update the controller service references
                    final ControllerServiceReferencingComponentsEntity entity = serviceFacade.updateControllerServiceReferencingComponents(
                            referencingRevisions, updateReferenceRequest.getId(), scheduledState, controllerServiceState);

                    if (updateReferenceRequest.getUiOnly() == Boolean.TRUE) {
                        entity.getControllerServiceReferencingComponents().forEach(this::stripNonUiRelevantFields);
                    }

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified a new Controller Service.
     *
     * @param id The id of the controller service to update.
     * @param requestControllerServiceEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Updates a controller service",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerServiceEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller-services/{uuid}"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services if this request changes the reference - /controller-services/{uuid}")
            }
    )
    public Response updateControllerService(
            @Parameter(
                    description = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The controller service configuration details.",
                    required = true
            ) final ControllerServiceEntity requestControllerServiceEntity) {

        if (requestControllerServiceEntity == null || requestControllerServiceEntity.getComponent() == null) {
            throw new IllegalArgumentException("Controller service details must be specified.");
        }

        if (requestControllerServiceEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ControllerServiceDTO requestControllerServiceDTO = requestControllerServiceEntity.getComponent();
        if (!id.equals(requestControllerServiceDTO.getId())) {
            throw new IllegalArgumentException(String.format("The controller service id (%s) in the request body does not equal the "
                    + "controller service id of the requested resource (%s).", requestControllerServiceDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestControllerServiceEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestControllerServiceEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestControllerServiceEntity, id);
        return withWriteLock(
                serviceFacade,
                requestControllerServiceEntity,
                requestRevision,
                lookup -> {
                    // authorize the service
                    final ComponentAuthorizable authorizable = lookup.getControllerService(id);
                    authorizable.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // authorize any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestControllerServiceDTO.getProperties(), authorizable, authorizer, lookup);
                    AuthorizeParameterReference.authorizeParameterReferences(requestControllerServiceDTO.getProperties(), authorizer, authorizable.getParameterContext(),
                            NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateControllerService(requestControllerServiceDTO),
                (revision, controllerServiceEntity) -> {
                    final ControllerServiceDTO controllerService = controllerServiceEntity.getComponent();

                    // update the controller service
                    final ControllerServiceEntity entity = serviceFacade.updateControllerService(revision, controllerService);
                    populateRemainingControllerServiceEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Moves the specified Controller Service to parent/child process groups.
     *
     * @param id The id of the controller service to update.
     * @param requestControllerServiceEntity A controllerServiceEntity.
     * @return A controllerServiceEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/move")
    @Operation(
            summary = "Move Controller Service to the specified Process Group.",
            responses = {
                    @ApiResponse(content = @Content(schema = @Schema(implementation = ControllerServiceEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                @SecurityRequirement(name = "Write - /controller-services/{uuid}"),
                @SecurityRequirement(name = "Write - Parent Process Group if scoped by Process Group - /process-groups/{uuid}"),
                @SecurityRequirement(name = "Write - Controller if scoped by Controller - /controller"),
                @SecurityRequirement(name = "Read - any referenced Controller Services - /controller-services/{uuid}")
            })
    public Response moveControllerServices(
            @Parameter(
                    description = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The controller service entity",
                    required = true
            )
            final ControllerServiceEntity requestControllerServiceEntity) {

        if (requestControllerServiceEntity == null) {
            throw new IllegalArgumentException("Controller service must be specified.");
        }

        if (requestControllerServiceEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (requestControllerServiceEntity.getParentGroupId() == null) {
            throw new IllegalArgumentException("ParentGroupId must be specified.");
        }

        final ControllerServiceDTO requestControllerServiceDTO = serviceFacade.getControllerService(id, true).getComponent();

        final Revision requestRevision = getRevision(requestControllerServiceEntity, id);
        return withWriteLock(
                serviceFacade,
                serviceFacade.getControllerService(id, true),
                requestRevision,
                lookup -> {
                    // authorize the service
                    final ComponentAuthorizable authorizable = lookup.getControllerService(requestControllerServiceDTO.getId());
                    authorizable.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // authorize the current and new process groups
                    final ProcessGroupAuthorizable authorizableProcessGroupNew = lookup.getProcessGroup(requestControllerServiceEntity.getParentGroupId());
                    authorizableProcessGroupNew.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    final ProcessGroupAuthorizable authorizableProcessGroupOld = lookup.getProcessGroup(requestControllerServiceDTO.getParentGroupId());
                    authorizableProcessGroupOld.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // Verify all referencing and referenced components are still within scope
                    List<String> conflictingComponents = new ArrayList<>();
                    requestControllerServiceDTO.getReferencingComponents().forEach(e -> {
                        if (authorizableProcessGroupNew.getProcessGroup().findProcessor(e.getId()) == null
                            && authorizableProcessGroupNew.getProcessGroup().findControllerService(e.getId(), true, false) == null) {
                            conflictingComponents.add("[" + e.getComponent().getName() + "]");
                        }

                        final Authorizable referencingComponent = lookup.getControllerServiceReferencingComponent(requestControllerServiceDTO.getId(), e.getId());
                        OperationAuthorizable.authorizeOperation(referencingComponent, authorizer, NiFiUserUtils.getNiFiUser());
                    });

                    requestControllerServiceDTO.getProperties().forEach((key, value) -> {
                        try {
                            ControllerServiceEntity refControllerService = serviceFacade.getControllerService(value, false);
                            if (refControllerService != null) {
                                if (authorizableProcessGroupNew.getProcessGroup().findControllerService(value, false, true) == null) {
                                    conflictingComponents.add("[" + refControllerService.getComponent().getName() + "]");
                                }
                            }
                        } catch (Exception ignored) { }
                    });

                    if (!conflictingComponents.isEmpty()) {
                        String errorMessage = "Could not move controller service because the following components would be out of scope: ";
                        errorMessage += String.join(" ", conflictingComponents);
                        throw new IllegalStateException(errorMessage);
                    }
                },
                null,
                (revision, controllerServiceEntity) -> {
                    final ControllerServiceDTO controllerService = controllerServiceEntity.getComponent();
                    final ControllerServiceEntity entity = serviceFacade.moveControllerService(revision, controllerService, requestControllerServiceEntity.getParentGroupId());
                    populateRemainingControllerServiceEntityContent(entity);
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified controller service.
     *
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the controller service to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Deletes a controller service",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerServiceEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller-services/{uuid}"),
                    @SecurityRequirement(name = "Write - Parent Process Group if scoped by Process Group - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Write - Controller if scoped by Controller - /controller"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services - /controller-services/{uuid}")
            }
    )
    public Response removeControllerService(
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
                    description = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final ControllerServiceEntity requestControllerServiceEntity = new ControllerServiceEntity();
        requestControllerServiceEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestControllerServiceEntity,
                requestRevision,
                lookup -> {
                    final ComponentAuthorizable controllerService = lookup.getControllerService(id);

                    // ensure write permission to the controller service
                    controllerService.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    controllerService.getAuthorizable().getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // verify any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(controllerService, authorizer, lookup, false);
                },
                () -> serviceFacade.verifyDeleteControllerService(id),
                (revision, controllerServiceEntity) -> {
                    // delete the specified controller service
                    final ControllerServiceEntity entity = serviceFacade.deleteControllerService(revision, controllerServiceEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the operational status for the specified controller service with the specified values.
     *
     * @param id The id of the controller service to update.
     * @param requestRunStatus A runStatusEntity.
     * @return A controllerServiceEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/run-status")
    @Operation(
            summary = "Updates run status of a controller service",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ControllerServiceEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /controller-services/{uuid} or /operation/controller-services/{uuid}")
            }
    )
    public Response updateRunStatus(
            @Parameter(
                    description = "The controller service id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The controller service run status.",
                    required = true
            ) final ControllerServiceRunStatusEntity requestRunStatus) {

        if (requestRunStatus == null) {
            throw new IllegalArgumentException("Controller service run status must be specified.");
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
                    // authorize the service
                    final Authorizable authorizable = lookup.getControllerService(id).getAuthorizable();
                    OperationAuthorizable.authorizeOperation(authorizable, authorizer, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateControllerService(createDTOWithDesiredRunStatus(id, requestRunStatus.getState())),
                (revision, runStatusEntity) -> {
                    // update the controller service
                    final ControllerServiceEntity entity = serviceFacade.updateControllerService(revision, createDTOWithDesiredRunStatus(id, runStatusEntity.getState()));
                    populateRemainingControllerServiceEntityContent(entity);

                    if (runStatusEntity.getUiOnly() == Boolean.TRUE) {
                        stripNonUiRelevantFields(entity);
                    }

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
                    @SecurityRequirement(name = "Read - /controller-services/{uuid}")
            }
    )
    public Response analyzeConfiguration(
            @Parameter(description = "The controller service id.", required = true)
            @PathParam("id") final String controllerServiceId,
            @Parameter(description = "The configuration analysis request.", required = true) final ConfigurationAnalysisEntity configurationAnalysis) {

        if (configurationAnalysis == null || configurationAnalysis.getConfigurationAnalysis() == null) {
            throw new IllegalArgumentException("Controller Service's configuration must be specified");
        }

        final ConfigurationAnalysisDTO dto = configurationAnalysis.getConfigurationAnalysis();
        if (dto.getComponentId() == null) {
            throw new IllegalArgumentException("Controller Service's identifier must be specified in the request");
        }

        if (!dto.getComponentId().equals(controllerServiceId)) {
            throw new IllegalArgumentException("Controller Service's identifier in the request must match the identifier provided in the URL");
        }

        if (dto.getProperties() == null) {
            throw new IllegalArgumentException("Controller Service's properties must be specified in the request");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, configurationAnalysis);
        }

        return withWriteLock(
                serviceFacade,
                configurationAnalysis,
                lookup -> {
                    final ComponentAuthorizable controllerService = lookup.getControllerService(controllerServiceId);
                    controllerService.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                },
                entity -> {
                    final ConfigurationAnalysisDTO analysis = entity.getConfigurationAnalysis();
                    final ConfigurationAnalysisEntity resultsEntity = serviceFacade.analyzeControllerServiceConfiguration(analysis.getComponentId(), analysis.getProperties());
                    return generateOkResponse(resultsEntity).build();
                }
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/config/verification-requests")
    @Operation(
            summary = "Performs verification of the Controller Service's configuration",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VerifyConfigRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This will initiate the process of verifying a given Controller Service configuration. This may be a long-running task. As a result, this endpoint will immediately return " +
                    "a " +
                    "ControllerServiceConfigVerificationRequestEntity, and the process of performing the verification will occur asynchronously in the background. " +
                    "The client may then periodically poll the status of the request by " +
                    "issuing a GET request to /controller-services/{serviceId}/verification-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
                    "/controller-services/{serviceId}/verification-requests/{requestId}.",
            security = {
                    @SecurityRequirement(name = "Read - /controller-services/{uuid}")
            }
    )
    public Response submitConfigVerificationRequest(
            @Parameter(description = "The controller service id.", required = true)
            @PathParam("id") final String controllerServiceId,
            @Parameter(description = "The controller service configuration verification request.", required = true) final VerifyConfigRequestEntity controllerServiceConfigRequest) {

        if (controllerServiceConfigRequest == null) {
            throw new IllegalArgumentException("Controller Service's configuration must be specified");
        }

        final VerifyConfigRequestDTO requestDto = controllerServiceConfigRequest.getRequest();
        if (requestDto == null || requestDto.getProperties() == null) {
            throw new IllegalArgumentException("Controller Service properties must be specified");
        }

        if (requestDto.getComponentId() == null) {
            throw new IllegalArgumentException("Controller Service's identifier must be specified in the request");
        }

        if (!requestDto.getComponentId().equals(controllerServiceId)) {
            throw new IllegalArgumentException("Controller Service's identifier in the request must match the identifier provided in the URL");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, controllerServiceConfigRequest);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return withWriteLock(
                serviceFacade,
                controllerServiceConfigRequest,
                lookup -> {
                    final ComponentAuthorizable controllerService = lookup.getControllerService(controllerServiceId);
                    controllerService.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                    serviceFacade.verifyCanVerifyControllerServiceConfig(controllerServiceId);
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
            @Parameter(description = "The ID of the Controller Service") @PathParam("id") final String controllerServiceId,
            @Parameter(description = "The ID of the Verification Request") @PathParam("requestId") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest =
                configVerificationRequestManager.getRequest(VERIFICATION_REQUEST_TYPE, requestId, user);

        final VerifyConfigRequestEntity updateRequestEntity = createVerifyControllerServiceConfigRequestEntity(asyncRequest, requestId);
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
            @Parameter(description = "The ID of the Controller Service")
            @PathParam("id") final String controllerServiceId,
            @Parameter(description = "The ID of the Verification Request")
            @PathParam("requestId") final String requestId
    ) {

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

            final VerifyConfigRequestEntity updateRequestEntity = createVerifyControllerServiceConfigRequestEntity(asyncRequest, requestId);
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


    public Response performAsyncConfigVerification(final VerifyConfigRequestEntity configRequest, final NiFiUser user) {
        // Create an asynchronous request that will occur in the background, because this request may take an indeterminate amount of time.
        final String requestId = generateUuid();

        final VerifyConfigRequestDTO requestDto = configRequest.getRequest();
        final String serviceId = requestDto.getComponentId();
        final List<UpdateStep> updateSteps = Collections.singletonList(new StandardUpdateStep("Verify Controller Service Configuration"));

        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> request =
                new StandardAsynchronousWebRequest<>(requestId, configRequest, serviceId, user, updateSteps);

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>>> updateTask = asyncRequest -> {
            try {
                final Map<String, String> attributes = requestDto.getAttributes() == null ? Collections.emptyMap() : requestDto.getAttributes();
                final List<ConfigVerificationResultDTO> results = serviceFacade.performControllerServiceConfigVerification(serviceId, requestDto.getProperties(), attributes);
                asyncRequest.markStepComplete(results);
            } catch (final Exception e) {
                logger.error("Failed to verify Controller Service configuration", e);
                asyncRequest.fail("Failed to verify Controller Service configuration due to " + e);
            }
        };

        configVerificationRequestManager.submitRequest(VERIFICATION_REQUEST_TYPE, requestId, request, updateTask);

        // Generate the response
        final VerifyConfigRequestEntity resultsEntity = createVerifyControllerServiceConfigRequestEntity(request, requestId);
        return generateOkResponse(resultsEntity).build();
    }

    private VerifyConfigRequestEntity createVerifyControllerServiceConfigRequestEntity(
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
        dto.setUri(generateResourceUri("controller-services", requestDto.getComponentId(), "config", "verification-requests", requestId));

        final VerifyConfigRequestEntity entity = new VerifyConfigRequestEntity();
        entity.setRequest(dto);
        return entity;
    }

    private ControllerServiceDTO createDTOWithDesiredRunStatus(final String id, final String runStatus) {
        final ControllerServiceDTO dto = new ControllerServiceDTO();
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
