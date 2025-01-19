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

import com.google.common.base.Functions;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.PostConstruct;
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
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizeControllerServiceReference;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterGroupConfiguration;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ParameterSensitivity;
import org.apache.nifi.ui.extension.UiExtension;
import org.apache.nifi.ui.extension.UiExtensionMapping;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.ResumeFlowException;
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
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.dto.ParameterProviderApplyParametersRequestDTO;
import org.apache.nifi.web.api.dto.ParameterProviderApplyParametersUpdateStepDTO;
import org.apache.nifi.web.api.dto.ParameterProviderConfigurationDTO;
import org.apache.nifi.web.api.dto.ParameterProviderDTO;
import org.apache.nifi.web.api.dto.ParameterStatus;
import org.apache.nifi.web.api.dto.ParameterStatusDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.VerifyConfigRequestDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConfigurationAnalysisEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.ParameterGroupConfigurationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderApplyParametersRequestEntity;
import org.apache.nifi.web.api.entity.ParameterProviderConfigurationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterApplicationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderParameterFetchEntity;
import org.apache.nifi.web.api.entity.ParameterProviderReferencingComponentEntity;
import org.apache.nifi.web.api.entity.ParameterProviderReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.VerifyConfigRequestEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.apache.nifi.web.util.LifecycleManagementException;
import org.apache.nifi.web.util.ParameterUpdateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * RESTful endpoint for managing a Parameter Provider.
 */
@Controller
@Path("/parameter-providers")
@Tag(name = "ParameterProviders")
public class ParameterProviderResource extends AbstractParameterResource {
    private static final Logger logger = LoggerFactory.getLogger(ParameterProviderResource.class);

    private NiFiServiceFacade serviceFacade;
    private DtoFactory dtoFactory;
    private Authorizer authorizer;
    private ParameterUpdateManager parameterUpdateManager;

    private static final String VERIFICATION_REQUEST_TYPE = "verification-request";
    private RequestManager<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> configVerificationRequestManager =
            new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Verify Parameter Provider Config Thread");

    private RequestManager<List<ParameterContextEntity>, List<ParameterContextEntity>> updateRequestManager =
            new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Parameter Provider Apply Thread");

    private ComponentLifecycle clusterComponentLifecycle;
    private ComponentLifecycle localComponentLifecycle;

    @Context
    private ServletContext servletContext;

    @PostConstruct
    public void init() {
        parameterUpdateManager = new ParameterUpdateManager(serviceFacade, dtoFactory, authorizer, this);
    }

    private void authorizeReadParameterProvider(final String parameterProviderId) {
        if (parameterProviderId == null) {
            throw new IllegalArgumentException("Parameter Provider ID must be specified");
        }

        serviceFacade.authorizeAccess(lookup -> {
            final ComponentAuthorizable parameterProvider = lookup.getParameterProvider(parameterProviderId);
            parameterProvider.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });
    }

    /**
     * Populate the uri's for the specified parameter providers.
     *
     * @param parameterProviderEntities parameter providers
     * @return dtos
     */
    public Set<ParameterProviderEntity> populateRemainingParameterProviderEntitiesContent(final Set<ParameterProviderEntity> parameterProviderEntities) {
        for (ParameterProviderEntity parameterProviderEntity : parameterProviderEntities) {
            populateRemainingParameterProviderEntityContent(parameterProviderEntity);
        }
        return parameterProviderEntities;
    }

    /**
     * Populate the uri's for the specified parameter provider.
     *
     * @param parameterProviderEntity parameter provider
     * @return dtos
     */
    public ParameterProviderEntity populateRemainingParameterProviderEntityContent(final ParameterProviderEntity parameterProviderEntity) {
        parameterProviderEntity.setUri(generateResourceUri("parameter-providers", parameterProviderEntity.getId()));

        // populate the remaining content
        if (parameterProviderEntity.getComponent() != null) {
            populateRemainingParameterProviderContent(parameterProviderEntity.getComponent());
        }
        return parameterProviderEntity;
    }

    /**
     * Populates the uri for the specified parameter provider.
     */
    public ParameterProviderDTO populateRemainingParameterProviderContent(final ParameterProviderDTO parameterProvider) {
        final BundleDTO bundle = parameterProvider.getBundle();
        if (bundle == null) {
            return parameterProvider;
        }

        // see if this processor has any ui extensions
        final UiExtensionMapping uiExtensionMapping = (UiExtensionMapping) servletContext.getAttribute("nifi-ui-extensions");
        if (uiExtensionMapping.hasUiExtension(parameterProvider.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion())) {
            final List<UiExtension> uiExtensions = uiExtensionMapping.getUiExtension(parameterProvider.getType(), bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
            for (final UiExtension uiExtension : uiExtensions) {
                if (UiExtensionType.ParameterProviderConfiguration.equals(uiExtension.getExtensionType())) {
                    parameterProvider.setCustomUiUrl(generateExternalUiUri(uiExtension.getContextPath()));
                }
            }
        }

        return parameterProvider;
    }

    /**
     * Retrieves the specified parameter provider.
     *
     * @param id The id of the parameter provider to retrieve
     * @return A parameterProviderEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Gets a parameter provider",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterProviderEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /parameter-providers/{uuid}")
            }
    )
    public Response getParameterProvider(
            @Parameter(
                    description = "The parameter provider id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable parameterProvider = lookup.getParameterProvider(id).getAuthorizable();
            parameterProvider.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the parameter provider
        final ParameterProviderEntity parameterProvider = serviceFacade.getParameterProvider(id);
        populateRemainingParameterProviderEntityContent(parameterProvider);

        return generateOkResponse(parameterProvider).build();
    }

    /**
     * Retrieves the references of the specified parameter provider.
     *
     * @param id The id of the parameter provider to retrieve
     * @return A parameterProviderEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/references")
    @Operation(
            summary = "Gets all references to a parameter provider",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterProviderReferencingComponentsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /parameter-providers/{uuid}")
            }
    )
    public Response getParameterProviderReferences(
            @Parameter(
                    description = "The parameter provider id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable parameterProvider = lookup.getParameterProvider(id).getAuthorizable();
            parameterProvider.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the parameter provider references
        final ParameterProviderReferencingComponentsEntity entity = serviceFacade.getParameterProviderReferencingComponents(id);

        return generateOkResponse(entity).build();
    }

    /**
     * Returns the descriptor for the specified property.
     *
     * @param id The id of the parameter provider.
     * @param propertyName The property
     * @return a propertyDescriptorEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/descriptors")
    @Operation(
            summary = "Gets a parameter provider property descriptor",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = PropertyDescriptorEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /parameter-providers/{uuid}")
            }
    )
    public Response getPropertyDescriptor(
            @Parameter(
                    description = "The parameter provider id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The property name.",
                    required = true
            )
            @QueryParam("propertyName") final String propertyName) {

        // ensure the property name is specified
        if (propertyName == null) {
            throw new IllegalArgumentException("The property name must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable parameterProvider = lookup.getParameterProvider(id).getAuthorizable();
            parameterProvider.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the property descriptor
        final PropertyDescriptorDTO descriptor = serviceFacade.getParameterProviderPropertyDescriptor(id, propertyName);

        // generate the response entity
        final PropertyDescriptorEntity entity = new PropertyDescriptorEntity();
        entity.setPropertyDescriptor(descriptor);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the state for a parameter provider.
     *
     * @param id The id of the parameter provider
     * @return a componentStateEntity
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state")
    @Operation(
            summary = "Gets the state for a parameter provider",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ComponentStateEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /parameter-providers/{uuid}")
            }
    )
    public Response getState(
            @Parameter(
                    description = "The parameter provider id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable parameterProvider = lookup.getParameterProvider(id).getAuthorizable();
            parameterProvider.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        // get the component state
        final ComponentStateDTO state = serviceFacade.getParameterProviderState(id);

        // generate the response entity
        final ComponentStateEntity entity = new ComponentStateEntity();
        entity.setComponentState(state);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Clears the state for a parameter provider.
     *
     * @param id The id of the parameter provider
     * @return a componentStateEntity
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/state/clear-requests")
    @Operation(
            summary = "Clears the state for a parameter provider",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ComponentStateEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /parameter-providers/{uuid}")
            }
    )
    public Response clearState(
            @Parameter(
                    description = "The parameter provider id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST);
        }

        final ParameterProviderEntity requestParameterProviderEntity = new ParameterProviderEntity();
        requestParameterProviderEntity.setId(id);

        return withWriteLock(
                serviceFacade,
                requestParameterProviderEntity,
                lookup -> {
                    final Authorizable processor = lookup.getParameterProvider(id).getAuthorizable();
                    processor.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyCanClearParameterProviderState(id),
                (parameterProviderEntity) -> {
                    // get the component state
                    serviceFacade.clearParameterProviderState(parameterProviderEntity.getId());

                    // generate the response entity
                    final ComponentStateEntity entity = new ComponentStateEntity();

                    // generate the response
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Updates the specified a Parameter Provider.
     *
     * @param id The id of the parameter provider to update.
     * @param requestParameterProviderEntity A parameterProviderEntity.
     * @return A parameterProviderEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Updates a parameter provider",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterProviderEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /parameter-providers/{uuid}"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services if this request changes the reference - /controller-services/{uuid}")
            }
    )
    public Response updateParameterProvider(
            @Parameter(
                    description = "The parameter provider id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The parameter provider configuration details.",
                    required = true
            ) final ParameterProviderEntity requestParameterProviderEntity) {

        if (requestParameterProviderEntity == null || requestParameterProviderEntity.getComponent() == null) {
            throw new IllegalArgumentException("Parameter provider details must be specified.");
        }

        if (requestParameterProviderEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final ParameterProviderDTO requestParameterProviderDTO = requestParameterProviderEntity.getComponent();
        if (!id.equals(requestParameterProviderDTO.getId())) {
            throw new IllegalArgumentException(String.format("The parameter provider id (%s) in the request body does not equal the "
                    + "parameter provider id of the requested resource (%s).", requestParameterProviderDTO.getId(), id));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestParameterProviderEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestParameterProviderEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestParameterProviderEntity, id);
        return withWriteLock(
                serviceFacade,
                requestParameterProviderEntity,
                requestRevision,
                lookup -> {
                    // authorize parameter provider
                    final ComponentAuthorizable authorizable = lookup.getParameterProvider(id);
                    authorizable.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // authorize any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(requestParameterProviderDTO.getProperties(), authorizable, authorizer, lookup);
                },
                () -> serviceFacade.verifyUpdateParameterProvider(requestParameterProviderDTO),
                (revision, parameterProviderEntity) -> {
                    final ParameterProviderDTO parameterProviderDTO = parameterProviderEntity.getComponent();

                    // update the parameter provider
                    final ParameterProviderEntity entity = serviceFacade.updateParameterProvider(revision, parameterProviderDTO);
                    populateRemainingParameterProviderEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified parameter provider.
     *
     * @param version The revision is used to verify the client is working with
     * the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the parameter provider to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Deletes a parameter provider",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterProviderEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /parameter-providers/{uuid}"),
                    @SecurityRequirement(name = "Write - /controller"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services - /controller-services/{uuid}")
            }
    )
    public Response removeParameterProvider(
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
                    description = "The parameter provider id.",
                    required = true
            )
            @PathParam("id") String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final ParameterProviderEntity requestParameterProviderEntity = new ParameterProviderEntity();
        requestParameterProviderEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestParameterProviderEntity,
                requestRevision,
                lookup -> {
                    final ComponentAuthorizable parameterProvider = lookup.getParameterProvider(id);

                    // ensure write permission to the parameter provider
                    parameterProvider.getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    parameterProvider.getAuthorizable().getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // verify any referenced services
                    AuthorizeControllerServiceReference.authorizeControllerServiceReferences(parameterProvider, authorizer, lookup, false);
                },
                () -> serviceFacade.verifyDeleteParameterProvider(id),
                (revision, parameterProviderEntity) -> {
                    // delete the specified parameter provider
                    final ParameterProviderEntity entity = serviceFacade.deleteParameterProvider(revision, parameterProviderEntity.getId());
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Tells the Parameter Provider to fetch its parameters.  This will temporarily cache the fetched parameters,
     * but the changes will not be applied to the flow until an "apply-parameters-requests" request is created.
     *
     * @param parameterProviderId The id of the parameter provider.
     * @return A parameterProviderEntity.
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}/parameters/fetch-requests")
    @Operation(
            summary = "Fetches and temporarily caches the parameters for a provider",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterProviderEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /parameter-providers/{uuid} or  or /operation/parameter-providers/{uuid}")
            }
    )
    public Response fetchParameters(
            @Parameter(
                    description = "The parameter provider id.",
                    required = true
            )
            @PathParam("id") final String parameterProviderId,
            @Parameter(
                    description = "The parameter fetch request.",
                    required = true
            ) final ParameterProviderParameterFetchEntity fetchParametersEntity) {

        if (fetchParametersEntity.getId() == null) {
            throw new IllegalArgumentException("The ID of the Parameter Provider must be specified");
        }
        if (!fetchParametersEntity.getId().equals(parameterProviderId)) {
            throw new IllegalArgumentException("The ID of the Parameter Provider must match the ID specified in the URL's path");
        }

        if (fetchParametersEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, fetchParametersEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(fetchParametersEntity.isDisconnectedNodeAcknowledged());
        }
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final ParameterProviderEntity parameterProviderEntity = serviceFacade.getParameterProvider(fetchParametersEntity.getId());

        final Collection<ParameterProviderReferencingComponentEntity> references = Optional.ofNullable(parameterProviderEntity.getComponent().getReferencingParameterContexts())
                .orElse(Collections.emptySet());

        // The full list of referencing components is used for authorization, since the actual affected components is not known until after the fetch
        final Set<AffectedComponentEntity> referencingComponents = new HashSet<>();
        final Collection<ParameterContextDTO> referencingParameterContextDtos = new HashSet<>();
        references.forEach(referencingEntity -> {
            final String parameterContextId = referencingEntity.getComponent().getId();
            final ParameterContextEntity parameterContextEntity = serviceFacade.getParameterContext(parameterContextId, true, user);
            parameterContextEntity.getComponent().getParameters().stream()
                    .filter(dto -> Boolean.TRUE.equals(dto.getParameter().getProvided()))
                    .forEach(p -> referencingComponents.addAll(p.getParameter().getReferencingComponents()));
            referencingParameterContextDtos.add(parameterContextEntity.getComponent());
        });

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(fetchParametersEntity.getRevision(), fetchParametersEntity.getId());
        return withWriteLock(
                serviceFacade,
                fetchParametersEntity,
                requestRevision,
                lookup -> {
                    // authorize parameter provider
                    final ComponentAuthorizable authorizable = lookup.getParameterProvider(fetchParametersEntity.getId());
                    authorizable.getAuthorizable().authorize(authorizer, RequestAction.READ, user);

                    references.forEach(reference -> lookup.getParameterContext(reference.getComponent().getId()).authorize(authorizer, RequestAction.READ, user));
                },
                () -> serviceFacade.verifyCanFetchParameters(fetchParametersEntity.getId()),
                (revision, parameterProviderFetchEntity) -> {
                    // fetch the parameters
                    final ParameterProviderEntity entity = serviceFacade.fetchParameters(parameterProviderFetchEntity.getId());

                    final Collection<ParameterGroupConfiguration> parameterGroupConfigurations = new ArrayList<>();
                    // If parameter sensitivities have not yet been specified, assume all are sensitive
                    getParameterGroupConfigurations(entity.getComponent().getParameterGroupConfigurations()).forEach(group -> {
                        final Set<String> unspecifiedParameters = group.getParameterSensitivities().entrySet().stream()
                                .filter(entry -> entry.getValue() == null)
                                .map(Map.Entry::getKey)
                                .collect(Collectors.toSet());
                        final Map<String, ParameterSensitivity> updatedSensitivities = new HashMap<>(group.getParameterSensitivities());
                        unspecifiedParameters.forEach(parameterName -> {
                            updatedSensitivities.put(parameterName, ParameterSensitivity.SENSITIVE);
                        });
                        parameterGroupConfigurations.add(new ParameterGroupConfiguration(group.getGroupName(), group.getParameterContextName(),
                                updatedSensitivities, group.isSynchronized()));
                    });
                    final List<ParameterContextEntity> parameterContextUpdates = serviceFacade.getParameterContextUpdatesForAppliedParameters(parameterProviderId, parameterGroupConfigurations);

                    final Set<ParameterEntity> removedParameters = parameterContextUpdates.stream()
                            .flatMap(context -> context.getComponent().getParameters().stream())
                            .filter(parameterEntity -> {
                                final ParameterDTO dto = parameterEntity.getParameter();
                                return dto.getSensitive() == null && dto.getValue() == null && dto.getDescription() == null;
                            })
                            .collect(Collectors.toSet());
                    final Set<AffectedComponentEntity> affectedComponents = getAffectedComponentEntities(parameterContextUpdates);
                    if (!affectedComponents.isEmpty()) {
                        entity.getComponent().setAffectedComponents(affectedComponents);
                    }
                    final Set<ParameterStatusDTO> parameterStatus = getParameterStatus(entity, parameterContextUpdates, removedParameters, user);
                    if (!parameterStatus.isEmpty()) {
                        entity.getComponent().setParameterStatus(parameterStatus);
                    }
                    populateRemainingParameterProviderEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    private void authorizeReferencingComponents(final String parameterContextId, final AuthorizableLookup lookup, final NiFiUser user) {
        final ParameterContextEntity context = serviceFacade.getParameterContext(parameterContextId, false, NiFiUserUtils.getNiFiUser());

        for (final ParameterEntity parameterEntity : context.getComponent().getParameters()) {
            final ParameterDTO dto = parameterEntity.getParameter();
            if (dto == null) {
                continue;
            }

            for (final AffectedComponentEntity affectedComponent : dto.getReferencingComponents()) {
                parameterUpdateManager.authorizeAffectedComponent(affectedComponent, lookup, user, true, false);
            }
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{providerId}/apply-parameters-requests")
    @Operation(
            summary = "Initiate a request to apply the fetched parameters of a Parameter Provider",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterProviderApplyParametersRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This will initiate the process of applying fetched parameters to all referencing Parameter Contexts. Changing the value of a Parameter may require that one or more " +
                    "components be stopped and restarted, so this action may take significantly more time than many other REST API actions. As a result, this endpoint will immediately return a " +
                    "ParameterProviderApplyParametersRequestEntity, and the process of updating the necessary components will occur asynchronously in the background. The client may then " +
                    "periodically poll the status of the request by issuing a GET request to /parameter-providers/apply-parameters-requests/{requestId}. Once the request is completed, the client " +
                    "is expected to issue a DELETE request to /parameter-providers/apply-parameters-requests/{requestId}.",
            security = {
                    @SecurityRequirement(name = "Read - /parameter-providers/{parameterProviderId}"),
                    @SecurityRequirement(name = "Write - /parameter-providers/{parameterProviderId}"),
                    @SecurityRequirement(name = "Read - for every component that is affected by the update"),
                    @SecurityRequirement(name = "Write - for every component that is affected by the update")
            }
    )
    public Response submitApplyParameters(
            @PathParam("providerId") final String parameterProviderId,
            @Parameter(description = "The apply parameters request.", required = true) final ParameterProviderParameterApplicationEntity requestEntity) {

        if (requestEntity == null) {
            throw new IllegalArgumentException("Apply Parameters Request must be specified.");
        }

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Parameter Provider Revision must be specified");
        }

        if (requestEntity.getId() == null) {
            throw new IllegalArgumentException("Parameter Provider's ID must be specified");
        }
        if (!requestEntity.getId().equals(parameterProviderId)) {
            throw new IllegalArgumentException("ID of Parameter Provider in message body does not match Parameter Provider ID supplied in URI");
        }

        // We will perform the updating of the Parameter Contexts in a background thread because it can be a long-running process.
        // In order to do this, we will need some objects that are only available as Thread-Local variables to the current
        // thread, so we will gather the values for these objects up front.
        final boolean replicateRequest = isReplicateRequest();
        final ComponentLifecycle componentLifecycle = replicateRequest ? clusterComponentLifecycle : localComponentLifecycle;
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // Workflow for this process:
        // 1. Determine which components will be affected and are enabled/running
        // 2. Verify READ and WRITE permissions for user, for every component that is affected
        // 3. Verify READ and WRITE permissions for user, for each referenced Parameter Context
        // 4. Stop all Processors that are affected.
        // 5. Wait for all of the Processors to finish stopping.
        // 6. Disable all Controller Services that are affected.
        // 7. Wait for all Controller Services to finish disabling.
        // 8. Update Parameter Contexts with fetched parameters from the Parameter Provider
        // 9. Re-Enable all affected Controller Services
        // 10. Re-Start all Processors

        final Collection<ParameterGroupConfiguration> parameterGroupConfigurations = getParameterGroupConfigurations(requestEntity.getParameterGroupConfigurations());
        validateParameterGroupConfigurations(parameterProviderId, parameterGroupConfigurations, user);
        parameterGroupConfigurations.stream()
                .filter(parameterGroupConfiguration -> requiresNewParameterContext(parameterGroupConfiguration, user))
                .forEach(parameterGroupConfiguration -> {
                    final ParameterContextEntity newParameterContext = getNewParameterContextEntity(parameterProviderId, parameterGroupConfiguration);
                    try {
                        performParameterContextCreate(user, getAbsolutePath(), replicateRequest, newParameterContext);
                    } catch (final LifecycleManagementException e) {
                        throw new RuntimeException("Failed to create Parameter Context " + parameterGroupConfiguration.getGroupName(), e);
                    }
                });

        // Get a list of parameter context entities representing changes needed in order to apply the fetched parameters
        final List<ParameterContextEntity> parameterContextUpdates = serviceFacade.getParameterContextUpdatesForAppliedParameters(parameterProviderId, parameterGroupConfigurations);

        final Set<AffectedComponentEntity> affectedComponents = getAffectedComponentEntities(parameterContextUpdates);
        logger.debug("Received Apply Request for Parameter Provider: {}; the following {} components will be affected: {}", requestEntity, affectedComponents.size(), affectedComponents);

        final InitiateParameterProviderApplyParametersRequestWrapper requestWrapper = new InitiateParameterProviderApplyParametersRequestWrapper(parameterProviderId,
                parameterContextUpdates, componentLifecycle, getAbsolutePath(), affectedComponents, replicateRequest, user);

        final Revision requestRevision = getRevision(requestEntity.getRevision(), parameterProviderId);
        return withWriteLock(
                serviceFacade,
                requestWrapper,
                requestRevision,
                lookup -> {
                    // Verify READ permissions for user, for the Parameter Provider itself
                    final ComponentAuthorizable parameterProvider = lookup.getParameterProvider(parameterProviderId);
                    parameterProvider.getAuthorizable().authorize(authorizer, RequestAction.READ, user);

                    parameterContextUpdates.forEach(context -> {
                        final Authorizable parameterContext = lookup.getParameterContext(context.getComponent().getId());
                        parameterContext.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                        parameterContext.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                    });

                    // Verify READ and WRITE permissions for user, for every component that is affected
                    affectedComponents.forEach(component -> parameterUpdateManager.authorizeAffectedComponent(component, lookup, user, true, true));
                },
                () -> {
                    // Verify Request
                    serviceFacade.verifyCanApplyParameters(parameterProviderId, parameterGroupConfigurations);
                },
                this::submitApplyRequest
        );
    }

    private Set<AffectedComponentEntity> getAffectedComponentEntities(final List<ParameterContextEntity> parameterContextUpdates) {
        final Collection<ParameterContextDTO> updatedParameterContextDTOs = parameterContextUpdates.stream()
                .map(ParameterContextEntity::getComponent)
                .collect(Collectors.toList());
        return serviceFacade.getComponentsAffectedByParameterContextUpdate(updatedParameterContextDTOs);
    }

    private Set<ParameterStatusDTO> getParameterStatus(final ParameterProviderEntity parameterProvider, final List<ParameterContextEntity> parameterContextUpdates,
                                                       final Set<ParameterEntity> removedParameters, final NiFiUser niFiUser) {
        final Set<ParameterStatusDTO> parameterStatus = new HashSet<>();
        if (parameterProvider.getComponent() == null || parameterProvider.getComponent().getReferencingParameterContexts() == null) {
            return parameterStatus;
        }

        final Map<String, Set<String>> removedParameterNamesByContextId = new HashMap<>();
        removedParameters.forEach(parameterEntity -> {
            removedParameterNamesByContextId.computeIfAbsent(parameterEntity.getParameter().getParameterContext().getComponent().getId(), key -> new HashSet<>())
                    .add(parameterEntity.getParameter().getName());
        });

        final Map<String, ParameterContextEntity> parameterContextUpdateMap = parameterContextUpdates.stream()
                .collect(Collectors.toMap(entity -> entity.getComponent().getId(), Functions.identity()));

        for (final ParameterProviderReferencingComponentEntity reference : parameterProvider.getComponent().getReferencingParameterContexts()) {
            final String parameterContextId = reference.getComponent().getId();
            final ParameterContextEntity parameterContext = serviceFacade.getParameterContext(parameterContextId, false, niFiUser);
            if (parameterContext.getComponent() == null) {
                continue;
            }

            final Set<String> removedParameterNames = removedParameterNamesByContextId.get(parameterContext.getComponent().getId());
            final ParameterContextEntity parameterContextUpdate = parameterContextUpdateMap.get(parameterContextId);
            final Map<String, ParameterEntity> updatedParameters = parameterContextUpdate == null ? Collections.emptyMap() : parameterContextUpdate.getComponent().getParameters().stream()
                    .collect(Collectors.toMap(parameter -> parameter.getParameter().getName(), Functions.identity()));
            final Set<String> currentParameterNames = new HashSet<>();

            // Report changed and removed parameters
            for (final ParameterEntity parameter : parameterContext.getComponent().getParameters()) {
                currentParameterNames.add(parameter.getParameter().getName());

                final ParameterStatusDTO dto = new ParameterStatusDTO();
                final ParameterEntity updatedParameter = updatedParameters.get(parameter.getParameter().getName());
                if (updatedParameter == null) {
                    dto.setParameter(parameter);
                    if (removedParameterNames != null && removedParameterNames.contains(parameter.getParameter().getName())) {
                        dto.setStatus(ParameterStatus.MISSING_BUT_REFERENCED);
                    } else {
                        dto.setStatus(ParameterStatus.UNCHANGED);
                    }
                } else {
                    final ParameterDTO updatedParameterDTO = updatedParameter.getParameter();
                    final boolean isDeletion = updatedParameterDTO.getSensitive() == null && updatedParameterDTO.getDescription() == null && updatedParameterDTO.getValue() == null;
                    dto.setParameter(updatedParameter);
                    dto.setStatus(isDeletion ? ParameterStatus.REMOVED : ParameterStatus.CHANGED);
                }
                parameterStatus.add(dto);
            }
            // Report new parameters
            updatedParameters.forEach((parameterName, parameterEntity) -> {
                if (!currentParameterNames.contains(parameterName)) {
                    final ParameterStatusDTO dto = new ParameterStatusDTO();
                    dto.setParameter(parameterEntity);
                    dto.setStatus(ParameterStatus.NEW);
                    parameterStatus.add(dto);
                }
            });
            parameterStatus.forEach(dto -> {
                final ParameterDTO parameterDTO = dto.getParameter().getParameter();
                if (parameterDTO.getValue() != null && parameterDTO.getSensitive() != null && parameterDTO.getSensitive()) {
                    parameterDTO.setValue(DtoFactory.SENSITIVE_VALUE_MASK);
                }
            });
        }
        return parameterStatus;
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{providerId}/apply-parameters-requests/{requestId}")
    @Operation(
            summary = "Returns the Apply Parameters Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterProviderApplyParametersRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Returns the Apply Parameters Request with the given ID. Once an Apply Parameters Request has been created by performing a POST to /nifi-api/parameter-providers, "
                    + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the state, such as percent complete, the "
                    + "current state of the request, and any failures. ",
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can get it")
            }
    )
    public Response getParameterProviderApplyParametersRequest(
            @Parameter(description = "The ID of the Parameter Provider") @PathParam("providerId") final String parameterProviderId,
            @Parameter(description = "The ID of the Apply Parameters Request") @PathParam("requestId") final String applyParametersRequestId) {

        authorizeReadParameterProvider(parameterProviderId);

        return retrieveApplyParametersRequest("apply-parameters-requests", parameterProviderId, applyParametersRequestId);
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{providerId}/apply-parameters-requests/{requestId}")
    @Operation(
            summary = "Deletes the Apply Parameters Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterProviderApplyParametersRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Deletes the Apply Parameters Request with the given ID. After a request is created via a POST to /nifi-api/parameter-providers/apply-parameters-requests, it is expected "
                    + "that the client will properly clean up the request by DELETE'ing it, once the Apply process has completed. If the request is deleted before the request "
                    + "completes, then the Apply Parameters Request will finish the step that it is currently performing and then will cancel any subsequent steps.",
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can remove it")
            }
    )
    public Response deleteApplyParametersRequest(
            @Parameter(
                    description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(description = "The ID of the Parameter Provider") @PathParam("providerId") final String parameterProviderId,
            @Parameter(description = "The ID of the Apply Parameters Request") @PathParam("requestId") final String applyParametersRequestId) {

        authorizeReadParameterProvider(parameterProviderId);
        return deleteApplyParametersRequest("apply-parameters-requests", parameterProviderId, applyParametersRequestId, disconnectedNodeAcknowledged.booleanValue());
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
                    @SecurityRequirement(name = "Read - /parameter-providers/{uuid}")
            }
    )
    public Response analyzeConfiguration(
            @Parameter(description = "The parameter provider id.", required = true) @PathParam("id") final String parameterProviderId,
            @Parameter(description = "The configuration analysis request.", required = true) final ConfigurationAnalysisEntity configurationAnalysis) {

        if (configurationAnalysis == null || configurationAnalysis.getConfigurationAnalysis() == null) {
            throw new IllegalArgumentException("Parameter Provider's configuration must be specified");
        }

        final ConfigurationAnalysisDTO dto = configurationAnalysis.getConfigurationAnalysis();
        if (dto.getComponentId() == null) {
            throw new IllegalArgumentException("Parameter Provider's identifier must be specified in the request");
        }

        if (!dto.getComponentId().equals(parameterProviderId)) {
            throw new IllegalArgumentException("Parameter Provider's identifier in the request must match the identifier provided in the URL");
        }

        if (dto.getProperties() == null) {
            throw new IllegalArgumentException("Parameter Provider's properties must be specified in the request");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, configurationAnalysis);
        }

        return withWriteLock(
                serviceFacade,
                configurationAnalysis,
                lookup -> {
                    final ComponentAuthorizable parameterProvider = lookup.getParameterProvider(parameterProviderId);
                    parameterProvider.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                },
                entity -> {
                    final ConfigurationAnalysisDTO analysis = entity.getConfigurationAnalysis();
                    final ConfigurationAnalysisEntity resultsEntity = serviceFacade.analyzeParameterProviderConfiguration(analysis.getComponentId(), analysis.getProperties());
                    return generateOkResponse(resultsEntity).build();
                }
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{id}/config/verification-requests")
    @Operation(
            summary = "Performs verification of the Parameter Provider's configuration",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VerifyConfigRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This will initiate the process of verifying a given Parameter Provider configuration. This may be a long-running task. As a result, this endpoint will immediately return " +
                    "a " +
                    "ParameterProviderConfigVerificationRequestEntity, and the process of performing the verification will occur asynchronously in the background. " +
                    "The client may then periodically poll the status of the request by " +
                    "issuing a GET request to /parameter-providers/{serviceId}/verification-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
                    "/parameter-providers/{providerId}/verification-requests/{requestId}.",
            security = {
                    @SecurityRequirement(name = "Read - /parameter-providers/{uuid}")
            }
    )
    public Response submitConfigVerificationRequest(
            @Parameter(description = "The parameter provider id.", required = true) @PathParam("id") final String parameterProviderId,
            @Parameter(description = "The parameter provider configuration verification request.", required = true) final VerifyConfigRequestEntity parameterProviderConfigRequest) {

        if (parameterProviderConfigRequest == null) {
            throw new IllegalArgumentException("Parameter Provider's configuration must be specified");
        }

        final VerifyConfigRequestDTO requestDto = parameterProviderConfigRequest.getRequest();
        if (requestDto == null || requestDto.getProperties() == null) {
            throw new IllegalArgumentException("Parameter Provider Properties must be specified");
        }

        if (requestDto.getComponentId() == null) {
            throw new IllegalArgumentException("Parameter Provider's identifier must be specified in the request");
        }

        if (!requestDto.getComponentId().equals(parameterProviderId)) {
            throw new IllegalArgumentException("Parameter Provider's identifier in the request must match the identifier provided in the URL");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, parameterProviderConfigRequest);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return withWriteLock(
                serviceFacade,
                parameterProviderConfigRequest,
                lookup -> {
                    final ComponentAuthorizable parameterProvider = lookup.getParameterProvider(parameterProviderId);
                    parameterProvider.getAuthorizable().authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                    serviceFacade.verifyCanVerifyParameterProviderConfig(parameterProviderId);
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
            @Parameter(description = "The ID of the Parameter Provider") @PathParam("id") final String parameterProviderId,
            @Parameter(description = "The ID of the Verification Request") @PathParam("requestId") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest =
                configVerificationRequestManager.getRequest(VERIFICATION_REQUEST_TYPE, requestId, user);

        final VerifyConfigRequestEntity updateRequestEntity = createVerifyParameterProviderConfigRequestEntity(asyncRequest, requestId);
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
            @Parameter(description = "The ID of the Parameter Provider") @PathParam("id") final String parameterProviderId,
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

            if (asyncRequest == null) {
                throw new ResourceNotFoundException("Could not find request of type " + VERIFICATION_REQUEST_TYPE + " with ID " + requestId);
            }

            if (!asyncRequest.isComplete()) {
                asyncRequest.cancel();
            }

            final VerifyConfigRequestEntity updateRequestEntity = createVerifyParameterProviderConfigRequestEntity(asyncRequest, requestId);
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
        final String parameterProviderId = requestDto.getComponentId();
        final List<UpdateStep> updateSteps = Collections.singletonList(new StandardUpdateStep("Verify Parameter Provider Configuration"));

        final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> request =
                new StandardAsynchronousWebRequest<>(requestId, configRequest, parameterProviderId, user, updateSteps);

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>>> verifyTask = asyncRequest -> {
            try {
                final List<ConfigVerificationResultDTO> results = serviceFacade.performParameterProviderConfigVerification(parameterProviderId, requestDto.getProperties());
                asyncRequest.markStepComplete(results);
            } catch (final Exception e) {
                logger.error("Failed to verify Parameter Provider configuration", e);
                asyncRequest.fail("Failed to verify Parameter Provider configuration due to " + e);
            }
        };

        configVerificationRequestManager.submitRequest(VERIFICATION_REQUEST_TYPE, requestId, request, verifyTask);

        // Generate the response
        final VerifyConfigRequestEntity resultsEntity = createVerifyParameterProviderConfigRequestEntity(request, requestId);
        return generateOkResponse(resultsEntity).build();
    }

    private VerifyConfigRequestEntity createVerifyParameterProviderConfigRequestEntity(
            final AsynchronousWebRequest<VerifyConfigRequestEntity, List<ConfigVerificationResultDTO>> asyncRequest, final String requestId) {

        final VerifyConfigRequestDTO requestDto = asyncRequest.getRequest().getRequest();
        final List<ConfigVerificationResultDTO> resultsList = asyncRequest.getResults();

        final VerifyConfigRequestDTO dto = new VerifyConfigRequestDTO();
        dto.setComponentId(requestDto.getComponentId());
        dto.setProperties(requestDto.getProperties());
        dto.setResults(resultsList);

        dto.setComplete(resultsList != null);
        dto.setFailureReason(asyncRequest.getFailureReason());
        dto.setLastUpdated(asyncRequest.getLastUpdated());
        dto.setPercentCompleted(asyncRequest.getPercentComplete());
        dto.setRequestId(requestId);
        dto.setState(asyncRequest.getState());
        dto.setUri(generateResourceUri("parameter-providers", requestDto.getComponentId(), "config", "verification-requests", requestId));

        final VerifyConfigRequestEntity entity = new VerifyConfigRequestEntity();
        entity.setRequest(dto);
        return entity;
    }


    private Response retrieveApplyParametersRequest(final String requestType, final String parameterProviderId, final String requestId) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<List<ParameterContextEntity>, List<ParameterContextEntity>> asyncRequest = updateRequestManager.getRequest(requestType, requestId, user);
        final ParameterProviderApplyParametersRequestEntity applyParametersRequestEntity = createApplyParametersRequestEntity(asyncRequest, requestType, parameterProviderId, requestId);
        return generateOkResponse(applyParametersRequestEntity).build();
    }

    private Response deleteApplyParametersRequest(final String requestType, final String parameterProviderId, final String requestId, final boolean disconnectedNodeAcknowledged) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<List<ParameterContextEntity>, List<ParameterContextEntity>> asyncRequest = updateRequestManager.removeRequest(requestType, requestId, user);
        if (asyncRequest == null) {
            throw new ResourceNotFoundException("Could not find request of type " + requestType + " with ID " + requestId);
        }

        if (!asyncRequest.isComplete()) {
            asyncRequest.cancel();
        }

        final ParameterProviderApplyParametersRequestEntity applyParametersRequestEntity = createApplyParametersRequestEntity(asyncRequest, requestType, parameterProviderId, requestId);
        return generateOkResponse(applyParametersRequestEntity).build();
    }

    private ParameterProviderApplyParametersRequestEntity createApplyParametersRequestEntity(final AsynchronousWebRequest<List<ParameterContextEntity>, List<ParameterContextEntity>> asyncRequest,
                                                                                             final String requestType, final String parameterProviderId, final String requestId) {
        final ParameterProviderApplyParametersRequestEntity applyParametersRequestEntity = new ParameterProviderApplyParametersRequestEntity();
        final ParameterProviderApplyParametersRequestDTO applyParametersRequestDTO = new ParameterProviderApplyParametersRequestDTO();
        applyParametersRequestDTO.setComplete(asyncRequest.isComplete());
        applyParametersRequestDTO.setFailureReason(asyncRequest.getFailureReason());
        applyParametersRequestDTO.setLastUpdated(asyncRequest.getLastUpdated());
        applyParametersRequestDTO.setRequestId(requestId);
        applyParametersRequestDTO.setUri(generateResourceUri("parameter-providers", parameterProviderId, requestType, requestId));
        applyParametersRequestDTO.setState(asyncRequest.getState());
        applyParametersRequestDTO.setPercentCompleted(asyncRequest.getPercentComplete());

        final ParameterProviderEntity parameterProvider = serviceFacade.getParameterProvider(parameterProviderId);
        applyParametersRequestDTO.setParameterProvider(parameterProvider.getComponent());

        final List<ParameterProviderApplyParametersUpdateStepDTO> updateSteps = new ArrayList<>();
        for (final UpdateStep updateStep : asyncRequest.getUpdateSteps()) {
            final ParameterProviderApplyParametersUpdateStepDTO stepDto = new ParameterProviderApplyParametersUpdateStepDTO();
            stepDto.setDescription(updateStep.getDescription());
            stepDto.setComplete(updateStep.isComplete());
            stepDto.setFailureReason(updateStep.getFailureReason());
            updateSteps.add(stepDto);
        }
        applyParametersRequestDTO.setUpdateSteps(updateSteps);

        final Map<String, AffectedComponentEntity> overallAffectedComponents = new HashMap<>();
        final List<ParameterContextUpdateEntity> parameterContextUpdates = new ArrayList<>();
        applyParametersRequestDTO.setParameterContextUpdates(parameterContextUpdates);
        final List<ParameterContextEntity> initialRequestList = asyncRequest.getRequest();
        for (final ParameterContextEntity parameterContextEntity : initialRequestList) {
            // The AffectedComponentEntity itself does not evaluate equality based on component information. As a result, we want to de-dupe the entities based on their identifiers.
            final Map<String, AffectedComponentEntity> affectedComponents = new HashMap<>();
            for (final ParameterEntity entity : parameterContextEntity.getComponent().getParameters()) {
                for (final AffectedComponentEntity affectedComponentEntity : entity.getParameter().getReferencingComponents()) {
                    final AffectedComponentEntity updatedAffectedComponentEntity = serviceFacade.getUpdatedAffectedComponentEntity(affectedComponentEntity);
                    final String affectedComponentEntityId = affectedComponentEntity.getId();

                    affectedComponents.put(affectedComponentEntityId, updatedAffectedComponentEntity);
                    overallAffectedComponents.put(affectedComponentEntityId, updatedAffectedComponentEntity);
                }
            }

            // Populate the Affected Components
            final ParameterContextEntity contextEntity = serviceFacade.getParameterContext(parameterContextEntity.getId(), false, NiFiUserUtils.getNiFiUser());
            final ParameterContextUpdateEntity parameterContextUpdate = new ParameterContextUpdateEntity();
            parameterContextUpdate.setReferencingComponents(new HashSet<>(affectedComponents.values()));

            // If the request is complete, include the new representation of the Parameter Context along with its new Revision. Otherwise, do not include the information, since it is 'stale'
            if (applyParametersRequestDTO.isComplete()) {
                parameterContextUpdate.setParameterContext(contextEntity == null ? null : contextEntity.getComponent());
                parameterContextUpdate.setParameterContextRevision(contextEntity == null ? null : contextEntity.getRevision());
            }
            parameterContextUpdates.add(parameterContextUpdate);
        }
        applyParametersRequestDTO.setReferencingComponents(new HashSet<>(overallAffectedComponents.values()));

        applyParametersRequestEntity.setRequest(applyParametersRequestDTO);
        return applyParametersRequestEntity;
    }

    private void validateParameterGroupConfigurations(final String parameterProviderId, final Collection<ParameterGroupConfiguration> configurations, final NiFiUser user) {
        final Set<String> synchronizedParameterContextNames = new HashSet<>();
        for (final ParameterGroupConfiguration configuration : configurations) {
            validateParameterGroupConfiguration(parameterProviderId, configuration, user);
            if (configuration.isSynchronized() != null && configuration.isSynchronized()) {
                final String parameterContextName = Optional.ofNullable(configuration.getParameterContextName()).orElse(configuration.getGroupName());
                if (parameterContextName == null) {
                    throw new IllegalArgumentException("A parameter group name is required for every synchronized parameter group");
                }
                if (!synchronizedParameterContextNames.add(parameterContextName)) {
                    throw new IllegalArgumentException(String.format("Two parameter groups cannot be mapped to the same parameter context [%s]", parameterContextName));
                }
            }
        }
    }

    private void validateParameterGroupConfiguration(final String parameterProviderId, final ParameterGroupConfiguration configuration, final NiFiUser user) {
        if (configuration.isSynchronized() != null && configuration.isSynchronized()) {
            final String parameterContextName = Optional.ofNullable(configuration.getParameterContextName()).orElse(configuration.getGroupName());
            final ParameterContext existingParameterContext = serviceFacade.getParameterContextByName(parameterContextName, user);
            if (existingParameterContext != null) {
                final ParameterProvider existingParameterProvider = existingParameterContext.getParameterProvider();
                if (existingParameterProvider == null || !existingParameterProvider.getIdentifier().equals(parameterProviderId)) {
                    final ParameterProviderEntity requestedParameterProvider = serviceFacade.getParameterProvider(parameterProviderId);
                    throw new IllegalStateException(String.format("Cannot synchronize parameter group [%s] to a Parameter Context [%s] that is not provided " +
                            "by Parameter Provider [%s]", configuration.getGroupName(), existingParameterContext.getName(), requestedParameterProvider.getId()));
                }
            }
        }
    }

    private boolean requiresNewParameterContext(final ParameterGroupConfiguration parameterGroupConfiguration, final NiFiUser user) {
        if (parameterGroupConfiguration.isSynchronized() != null && parameterGroupConfiguration.isSynchronized()) {
            final ParameterContext existingParameterContext = serviceFacade.getParameterContextByName(parameterGroupConfiguration.getParameterContextName(), user);
            return existingParameterContext == null;
        }
        return false;
    }

    private ParameterContextEntity getNewParameterContextEntity(final String parameterProviderId, final ParameterGroupConfiguration parameterGroupConfiguration) {
        final ParameterContextEntity parameterContextEntity = new ParameterContextEntity();
        final ParameterContextDTO parameterContextDTO = new ParameterContextDTO();
        parameterContextDTO.setName(parameterGroupConfiguration.getParameterContextName());

        final ParameterProviderConfigurationEntity parameterProviderConfiguration = new ParameterProviderConfigurationEntity();
        final ParameterProviderConfigurationDTO parameterProviderConfigurationDTO = new ParameterProviderConfigurationDTO();
        parameterProviderConfigurationDTO.setParameterProviderId(parameterProviderId);
        parameterProviderConfigurationDTO.setParameterGroupName(parameterGroupConfiguration.getGroupName());
        parameterProviderConfigurationDTO.setSynchronized(true);

        parameterProviderConfiguration.setComponent(parameterProviderConfigurationDTO);
        parameterContextDTO.setParameterProviderConfiguration(parameterProviderConfiguration);

        parameterContextEntity.setComponent(parameterContextDTO);

        final RevisionDTO initialRevision = new RevisionDTO();
        initialRevision.setVersion(0L);
        parameterContextEntity.setRevision(initialRevision);
        return parameterContextEntity;
    }

    private Response submitApplyRequest(final Revision requestRevision, final InitiateParameterProviderApplyParametersRequestWrapper requestWrapper) {
        // Create an asynchronous request that will occur in the background, because this request may
        // result in stopping components, which can take an indeterminate amount of time.
        final String requestId = UUID.randomUUID().toString();
        final String parameterProviderId = requestWrapper.getParameterProviderId();

        final AsynchronousWebRequest<List<ParameterContextEntity>, List<ParameterContextEntity>> request = new StandardAsynchronousWebRequest<>(
                requestId, requestWrapper.getParameterContextEntities(), parameterProviderId, requestWrapper.getUser(), getUpdateSteps());

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<List<ParameterContextEntity>, List<ParameterContextEntity>>> updateTask = asyncRequest -> {
            try {
                final List<ParameterContextEntity> updatedParameterContextEntities = parameterUpdateManager.updateParameterContexts(asyncRequest, requestWrapper.getComponentLifecycle(),
                        requestWrapper.getExampleUri(), requestWrapper.getReferencingComponents(), requestWrapper.isReplicateRequest(), requestRevision, requestWrapper.getParameterContextEntities());

                asyncRequest.markStepComplete(updatedParameterContextEntities);
            } catch (final ResumeFlowException rfe) {
                // Treat ResumeFlowException differently because we don't want to include a message that we couldn't update the flow
                // since in this case the flow was successfully updated - we just couldn't re-enable the components.
                logger.error(rfe.getMessage(), rfe);
                asyncRequest.fail(rfe.getMessage());
            } catch (final Exception e) {
                logger.error("Failed to apply parameters", e);
                asyncRequest.fail("Failed to apply parameters due to " + e);
            }
        };

        updateRequestManager.submitRequest("apply-parameters-requests", requestId, request, updateTask);

        // Generate the response.
        final ParameterProviderApplyParametersRequestEntity applicationRequestEntity = createApplyParametersRequestEntity(
                request, "apply-parameters-requests", parameterProviderId, requestId);
        return generateOkResponse(applicationRequestEntity).build();
    }

    private ParameterContextEntity performParameterContextCreate(final NiFiUser user, final URI exampleUri, final boolean replicateRequest,
                                                                 final ParameterContextEntity parameterContext) throws LifecycleManagementException {

        if (replicateRequest) {
            final URI updateUri;
            try {
                updateUri = new URI(exampleUri.getScheme(), exampleUri.getUserInfo(), exampleUri.getHost(),
                        exampleUri.getPort(), "/nifi-api/parameter-contexts", null, exampleUri.getFragment());
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }

            final Map<String, String> headers = new HashMap<>();
            headers.put("content-type", MediaType.APPLICATION_JSON);

            final NodeResponse clusterResponse = createParameterContext(parameterContext, updateUri, headers, user);

            final int updateFlowStatus = clusterResponse.getStatus();
            if (updateFlowStatus != Response.Status.CREATED.getStatusCode()) {
                final String explanation = ParameterUpdateManager.getResponseEntity(clusterResponse, String.class);
                logger.error("Failed to update flow across cluster when replicating POST request to {} for user {}. Received {} response with explanation: {}",
                        updateUri, user, updateFlowStatus, explanation);
                throw new LifecycleManagementException("Failed to update Flow on all nodes in cluster due to " + explanation);
            }
            final String parameterContextId = ParameterUpdateManager.getResponseEntity(clusterResponse, ParameterContextEntity.class).getId();

            return serviceFacade.getParameterContext(parameterContextId, false, user);
        } else {
            serviceFacade.verifyCreateParameterContext(parameterContext.getComponent());
            final String contextId = generateUuid();
            parameterContext.getComponent().setId(contextId);

            final Revision revision = getRevision(parameterContext.getRevision(), contextId);
            return serviceFacade.createParameterContext(revision, parameterContext.getComponent());
        }
    }

    public NodeResponse createParameterContext(final ParameterContextEntity parameterContext, final URI updateUri,
                                               final Map<String, String> headers, final NiFiUser user) throws LifecycleManagementException {
        final NodeResponse clusterResponse;
        try {
            logger.debug("Replicating POST request to {} for user {}", updateUri, user);

            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(user, HttpMethod.POST, updateUri, parameterContext, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), user, HttpMethod.POST, updateUri, parameterContext, headers).awaitMergedResponse();
            }
        } catch (final InterruptedException ie) {
            logger.warn("Interrupted while replicating POST request to {} for user {}", updateUri, user);
            Thread.currentThread().interrupt();
            throw new LifecycleManagementException("Interrupted while updating flows across cluster", ie);
        }
        return clusterResponse;
    }

    private List<UpdateStep> getUpdateSteps() {
        return Arrays.asList(new StandardUpdateStep("Stopping Affected Processors"),
                new StandardUpdateStep("Disabling Affected Controller Services"),
                new StandardUpdateStep("Updating Parameter Contexts"),
                new StandardUpdateStep("Re-Enabling Affected Controller Services"),
                new StandardUpdateStep("Restarting Affected Processors"));
    }

    private Collection<ParameterGroupConfiguration> getParameterGroupConfigurations(final Collection<ParameterGroupConfigurationEntity> groupConfigurationEntities) {
        final Collection<ParameterGroupConfiguration> parameterGroupConfigurations = new ArrayList<>();

        if (groupConfigurationEntities != null) {
            for (final ParameterGroupConfigurationEntity configurationEntity : groupConfigurationEntities) {
                parameterGroupConfigurations.add(new ParameterGroupConfiguration(configurationEntity.getGroupName(),
                        configurationEntity.getParameterContextName(), configurationEntity.getParameterSensitivities(), configurationEntity.isSynchronized()));
            }
        }
        return parameterGroupConfigurations;
    }

    private static class InitiateParameterProviderApplyParametersRequestWrapper extends Entity {
        private final String parameterProviderId;
        private final List<ParameterContextEntity> parameterContextEntities;
        private final ComponentLifecycle componentLifecycle;
        private final URI exampleUri;
        private final Set<AffectedComponentEntity> affectedComponents;
        private final boolean replicateRequest;
        private final NiFiUser nifiUser;

        public InitiateParameterProviderApplyParametersRequestWrapper(final String parameterProviderId, final List<ParameterContextEntity> parameterContextEntities,
                                                                      final ComponentLifecycle componentLifecycle,
                                                                      final URI exampleUri, final Set<AffectedComponentEntity> affectedComponents, final boolean replicateRequest,
                                                                      final NiFiUser nifiUser) {
            this.parameterProviderId = parameterProviderId;
            this.parameterContextEntities = parameterContextEntities;
            this.componentLifecycle = componentLifecycle;
            this.exampleUri = exampleUri;
            this.affectedComponents = affectedComponents;
            this.replicateRequest = replicateRequest;
            this.nifiUser = nifiUser;
        }

        public String getParameterProviderId() {
            return parameterProviderId;
        }

        public List<ParameterContextEntity> getParameterContextEntities() {
            return parameterContextEntities;
        }

        public ComponentLifecycle getComponentLifecycle() {
            return componentLifecycle;
        }

        public URI getExampleUri() {
            return exampleUri;
        }

        public Set<AffectedComponentEntity> getReferencingComponents() {
            return affectedComponents;
        }

        public boolean isReplicateRequest() {
            return replicateRequest;
        }

        public NiFiUser getUser() {
            return nifiUser;
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

    @Qualifier("clusterComponentLifecycle")
    @Autowired(required = false)
    public void setClusterComponentLifecycle(final ComponentLifecycle clusterComponentLifecycle) {
        this.clusterComponentLifecycle = clusterComponentLifecycle;
    }

    @Qualifier("localComponentLifecycle")
    @Autowired
    public void setLocalComponentLifecycle(final ComponentLifecycle localComponentLifecycle) {
        this.localComponentLifecycle = localComponentLifecycle;
    }

    public DtoFactory getDtoFactory() {
        return dtoFactory;
    }

    @Autowired
    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }
}
