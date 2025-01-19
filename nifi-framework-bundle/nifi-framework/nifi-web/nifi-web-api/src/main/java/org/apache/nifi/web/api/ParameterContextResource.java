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
import jakarta.annotation.PostConstruct;
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
import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetComponentManager;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.authorization.AuthorizableLookup;
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
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterReferencedControllerServiceData;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.stream.io.MaxLengthInputStream;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.ResumeFlowException;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.concurrent.AsyncRequestManager;
import org.apache.nifi.web.api.concurrent.AsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.RequestManager;
import org.apache.nifi.web.api.concurrent.StandardAsynchronousWebRequest;
import org.apache.nifi.web.api.concurrent.StandardUpdateStep;
import org.apache.nifi.web.api.concurrent.UpdateStep;
import org.apache.nifi.web.api.dto.AssetDTO;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextUpdateRequestDTO;
import org.apache.nifi.web.api.dto.ParameterContextUpdateStepDTO;
import org.apache.nifi.web.api.dto.ParameterContextValidationRequestDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.api.entity.ComponentValidationResultEntity;
import org.apache.nifi.web.api.entity.ComponentValidationResultsEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;
import org.apache.nifi.web.api.entity.ParameterContextValidationRequestEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.apache.nifi.web.util.ParameterUpdateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Controller
@Path("/parameter-contexts")
@Tag(name = "ParameterContexts")
public class ParameterContextResource extends AbstractParameterResource {
    private static final Logger logger = LoggerFactory.getLogger(ParameterContextResource.class);
    private static final Pattern VALID_PARAMETER_NAME_PATTERN = Pattern.compile("[A-Za-z0-9 ._\\-]+");
    private static final String FILENAME_HEADER = "Filename";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String UPLOAD_CONTENT_TYPE = "application/octet-stream";
    private static final long MAX_ASSET_SIZE_BYTES = (long) DataUnit.GB.toB(1);

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;
    private DtoFactory dtoFactory;
    private ComponentLifecycle clusterComponentLifecycle;
    private ComponentLifecycle localComponentLifecycle;
    private AssetManager assetManager;
    private AssetComponentManager assetComponentManager;
    private UploadRequestReplicator uploadRequestReplicator;

    private ParameterUpdateManager parameterUpdateManager;
    private final RequestManager<List<ParameterContextEntity>, List<ParameterContextEntity>> updateRequestManager =
        new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Parameter Context Update Thread");
    private final RequestManager<ParameterContextValidationRequestEntity, ComponentValidationResultsEntity> validationRequestManager =
        new AsyncRequestManager<>(100, TimeUnit.MINUTES.toMillis(1L), "Parameter Context Validation Thread");

    @PostConstruct
    public void init() {
        parameterUpdateManager = new ParameterUpdateManager(serviceFacade, dtoFactory, authorizer, this);
    }

    private void authorizeReadParameterContext(final String parameterContextId) {
        if (parameterContextId == null) {
            throw new IllegalArgumentException("Parameter Context ID must be specified");
        }

        // In order for a node to sync its assets with the cluster coordinator, it needs to be able to READ from any parameter context, and parameter contexts can have specific policies
        // which would require users adding the node identities to all of these policies, so this identifies if the incoming request is made directly by a known node identity and allows
        // it to bypass standard authorization, meaning a node is automatically granted READ to any parameter context
        final NiFiUser currentUser = NiFiUserUtils.getNiFiUser();
        if (isRequestFromClusterNode()) {
            logger.debug("Authorizing READ on ParameterContext[{}] to cluster node [{}]", parameterContextId, currentUser.getIdentity());
            return;
        }

        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable parameterContext = lookup.getParameterContext(parameterContextId);
            parameterContext.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Returns the Parameter Context with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterContextEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Returns the Parameter Context with the given ID.",
            security = {
                    @SecurityRequirement(name = "Read - /parameter-contexts/{id}")
            }
    )
    public Response getParameterContext(
            @Parameter(description = "The ID of the Parameter Context")
            @PathParam("id") final String parameterContextId,
            @Parameter(
                    description = "Whether or not to include inherited parameters from other parameter contexts, and therefore also overridden values.  " +
                            "If true, the result will be the 'effective' parameter context."
            ) @QueryParam("includeInheritedParameters")
            @DefaultValue("false") final boolean includeInheritedParameters) {
        // authorize access
        authorizeReadParameterContext(parameterContextId);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // get the specified parameter context
        final ParameterContextEntity entity = serviceFacade.getParameterContext(parameterContextId, includeInheritedParameters, NiFiUserUtils.getNiFiUser());
        entity.setUri(generateResourceUri("parameter-contexts", entity.getId()));

        // generate the response
        return generateOkResponse(entity).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Create a Parameter Context",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = ParameterContextEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /parameter-contexts"),
                    @SecurityRequirement(name = "Read - for every inherited parameter context")
            }
    )
    public Response createParameterContext(
            @Parameter(description = "The Parameter Context.", required = true) final ParameterContextEntity requestEntity) {

        if (requestEntity == null || requestEntity.getComponent() == null) {
            throw new IllegalArgumentException("Parameter Context must be specified");
        }

        if (requestEntity.getRevision() == null || (requestEntity.getRevision().getVersion() == null || requestEntity.getRevision().getVersion() != 0)) {
            throw new IllegalArgumentException("A revision of 0 must be specified when creating a new Parameter Context.");
        }

        final ParameterContextDTO context = requestEntity.getComponent();
        if (context.getName() == null) {
            throw new IllegalArgumentException("Parameter Context's Name must be specified");
        }

        validateParameterNames(requestEntity.getComponent());

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestEntity,
                lookup -> {
                    final Authorizable parameterContexts = lookup.getParameterContexts();
                    parameterContexts.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyCreateParameterContext(requestEntity.getComponent()),
                entity -> {
                    final String contextId = generateUuid();
                    entity.getComponent().setId(contextId);

                    final Revision revision = getRevision(entity.getRevision(), contextId);
                    final ParameterContextEntity contextEntity = serviceFacade.createParameterContext(revision, entity.getComponent());

                    // generate a 201 created response
                    final String uri = generateResourceUri("parameter-contexts", contextEntity.getId());
                    contextEntity.setUri(uri);
                    return generateCreatedResponse(URI.create(uri), contextEntity).build();
                });
    }


    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Modifies a Parameter Context",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterContextEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This endpoint will update a Parameter Context to match the provided entity. However, this request will fail if any component is running and is referencing a Parameter in " +
                    "the Parameter Context. Generally, this endpoint is not called directly. Instead, an update request should be submitted by making a POST to the " +
                    "/parameter-contexts/update-requests endpoint. That endpoint will, in turn, call this endpoint.",
            security = {
                    @SecurityRequirement(name = "Read - /parameter-contexts/{id}"),
                    @SecurityRequirement(name = "Write - /parameter-contexts/{id}")
            }
    )
    public Response updateParameterContext(
            @PathParam("id") String contextId,
            @Parameter(description = "The updated Parameter Context", required = true) final ParameterContextEntity requestEntity) {

        // Validate request
        if (requestEntity.getId() == null) {
            throw new IllegalArgumentException("The ID of the Parameter Context must be specified");
        }
        if (!requestEntity.getId().equals(contextId)) {
            throw new IllegalArgumentException("The ID of the Parameter Context must match the ID specified in the URL's path");
        }

        final ParameterContextDTO updateDto = requestEntity.getComponent();
        if (updateDto == null) {
            throw new IllegalArgumentException("The Parameter Context must be supplied");
        }

        final RevisionDTO revisionDto = requestEntity.getRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("The Revision of the Parameter Context must be specified.");
        }

        // Perform the request
        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        final Revision requestRevision = getRevision(requestEntity.getRevision(), updateDto.getId());
        return withWriteLock(
                serviceFacade,
                requestEntity,
                requestRevision,
                lookup -> {
                    final Authorizable parameterContext = lookup.getParameterContext(contextId);
                    parameterContext.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                    parameterContext.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyUpdateParameterContext(updateDto, true),
                (rev, entity) -> {
                    final ParameterContextEntity updatedEntity = serviceFacade.updateParameterContext(rev, entity.getComponent());

                    updatedEntity.setUri(generateResourceUri("parameter-contexts", entity.getId()));
                    return generateOkResponse(updatedEntity).build();
                }
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{contextId}/assets")
    @Operation(
            summary = "Creates a new Asset in the given Parameter Context",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AssetEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This endpoint will create a new Asset in the given Parameter Context. The Asset will be created with the given name and the contents of the file that is uploaded. " +
                    "The Asset will be created in the given Parameter Context, and will be available for use by any component that references the Parameter Context.",
            security = {
                @SecurityRequirement(name = "Read - /parameter-contexts/{parameterContextId}"),
                @SecurityRequirement(name = "Write - /parameter-contexts/{parameterContextId}"),
                @SecurityRequirement(name = "Read - for every component that is affected by the update"),
                @SecurityRequirement(name = "Write - for every component that is affected by the update"),
                @SecurityRequirement(name = "Read - for every currently inherited parameter context")
            }
    )
    public Response createAsset(
            @PathParam("contextId") final String contextId,
            @HeaderParam(FILENAME_HEADER) final String assetName,
            @Parameter(description = "The contents of the asset.", required = true) final InputStream assetContents) throws IOException {

        // Validate input
        if (StringUtils.isBlank(assetName)) {
            throw new IllegalArgumentException(FILENAME_HEADER + " header is required");
        }
        if (assetContents == null) {
            throw new IllegalArgumentException("Asset contents must be specified.");
        }

        final String sanitizedAssetName = FileUtils.getSanitizedFilename(assetName);
        if (!assetName.equals(sanitizedAssetName)) {
            throw new IllegalArgumentException(FILENAME_HEADER + " header contains an invalid file name");
        }

        // If clustered and not all nodes are connected, do not allow creating an asset.
        // Generally, we allow the flow to be modified when nodes are disconnected, but we do not allow creating an asset because
        // the cluster has no mechanism for synchronizing those assets after the upload.
        final ClusterCoordinator clusterCoordinator = getClusterCoordinator();
        if (clusterCoordinator != null) {
            final Set<NodeIdentifier> disconnectedNodes = clusterCoordinator.getNodeIdentifiers(NodeConnectionState.CONNECTING, NodeConnectionState.DISCONNECTED, NodeConnectionState.DISCONNECTING);
            if (!disconnectedNodes.isEmpty()) {
                throw new IllegalStateException("Cannot create an Asset because the following %s nodes are not currently connected: %s".formatted(disconnectedNodes.size(), disconnectedNodes));
            }
        }

        // Get the context or throw ResourceNotFoundException
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final ParameterContextEntity contextEntity = serviceFacade.getParameterContext(contextId, false, user);
        final Set<AffectedComponentEntity> affectedComponents = serviceFacade.getComponentsAffectedByParameterContextUpdate(Collections.singletonList(contextEntity.getComponent()));
        final Optional<Asset> previousAsset = assetManager.getAssets(contextId).stream().filter(asset -> asset.getName().equals(sanitizedAssetName)).findAny();

        // Authorize the request
        serviceFacade.authorizeAccess(lookup -> {
            // Verify READ and WRITE permissions for user, for the Parameter Context itself
            final ParameterContext parameterContext = lookup.getParameterContext(contextId);
            parameterContext.authorize(authorizer, RequestAction.READ, user);
            parameterContext.authorize(authorizer, RequestAction.WRITE, user);

            // Verify READ and WRITE permissions for user, for every component that is affected
            // This is necessary because this end-point may be called to replace the content of an asset that is referenced in a parameter that is already in use
            if (previousAsset.isPresent()) {
                affectedComponents.forEach(component -> parameterUpdateManager.authorizeAffectedComponent(component, lookup, user, true, true));
            }
        });

        // If we need to replicate the request, we do so using the Upload Request Replicator, rather than the typical replicate() method.
        // This is because Upload Request Replication works differently in that it needs to be able to replicate the InputStream multiple times,
        // so it must create a file on disk to do so and then use the file's content to replicate the request. It also bypasses the two-phase
        // commit process that is used for other requests because doing so would result in uploading the file twice to each node or providing a
        // different request for each of the two phases.

        final long startTime = System.currentTimeMillis();
        final InputStream maxLengthInputStream = new MaxLengthInputStream(assetContents, MAX_ASSET_SIZE_BYTES);

        final AssetEntity assetEntity;
        if (isReplicateRequest()) {
            final UploadRequest<AssetEntity> uploadRequest = new UploadRequest.Builder<AssetEntity>()
                    .user(NiFiUserUtils.getNiFiUser())
                    .filename(sanitizedAssetName)
                    .identifier(UUID.randomUUID().toString())
                    .contents(maxLengthInputStream)
                    .header(FILENAME_HEADER, sanitizedAssetName)
                    .header(CONTENT_TYPE_HEADER, UPLOAD_CONTENT_TYPE)
                    .exampleRequestUri(getAbsolutePath())
                    .responseClass(AssetEntity.class)
                    .successfulResponseStatus(HttpResponseStatus.OK.getCode())
                    .build();
            assetEntity = uploadRequestReplicator.upload(uploadRequest);
        } else {
            final String existingContextId = contextEntity.getId();
            final Asset asset = assetManager.createAsset(existingContextId, sanitizedAssetName, maxLengthInputStream);
            assetEntity = dtoFactory.createAssetEntity(asset);
            previousAsset.ifPresent(value -> assetComponentManager.restartReferencingComponentsAsync(value));
        }

        final AssetDTO assetDTO = assetEntity.getAsset();
        final long elapsedTime = System.currentTimeMillis() - startTime;
        logger.info("Creation of asset [id={},name={}] in parameter context [{}] completed in {} ms", assetDTO.getId(), assetDTO.getName(), contextId, elapsedTime);

        return generateOkResponse(assetEntity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{contextId}/assets")
    @Operation(
            summary = "Lists the assets that belong to the Parameter Context with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AssetsEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Lists the assets that belong to the Parameter Context with the given ID.",
            security = {
                    @SecurityRequirement(name = "Read - /parameter-contexts/{id}")
            }
    )
    public Response getAssets(
            @Parameter(description = "The ID of the Parameter Context")
            @PathParam("contextId") final String parameterContextId
    ) {
        // authorize access
        authorizeReadParameterContext(parameterContextId);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // Get the assets from the manager rather than from the context's values since there could be assets that were created, but never referenced
        final List<Asset> paramContextAssets = assetManager.getAssets(parameterContextId);

        final AssetsEntity assetsEntity = new AssetsEntity();
        assetsEntity.setAssets(paramContextAssets.stream()
                .map(dtoFactory::createAssetEntity)
                .toList());

        return generateOkResponse(assetsEntity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("{contextId}/assets/{assetId}")
    @Operation(
            summary = "Retrieves the content of the asset with the given id",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = byte[].class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /parameter-contexts/{id}")
            }
    )
    public Response getAssetContent(
            @Parameter(description = "The ID of the Parameter Context")
            @PathParam("contextId")
            final String parameterContextId,
            @Parameter(description = "The ID of the Asset")
            @PathParam("assetId")
            final String assetId
    ) {
        // authorize access
        authorizeReadParameterContext(parameterContextId);

        final Asset asset = assetManager.getAsset(assetId)
                .orElseThrow(() -> new ResourceNotFoundException("Asset does not exist with the id %s".formatted(assetId)));

        if (!asset.getParameterContextIdentifier().equals(parameterContextId)) {
            throw new ResourceNotFoundException("Asset does not exist with id %s".formatted(assetId));
        }

        if (!asset.getFile().exists()) {
            throw new IllegalStateException("Content does not exist for asset with id %s".formatted(assetId));
        }

        final StreamingOutput streamingOutput = (outputStream) -> {
            try (final InputStream assetInputStream = new FileInputStream(asset.getFile())) {
                assetInputStream.transferTo(outputStream);
            }
        };

        return generateOkResponse(streamingOutput)
                .header(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", asset.getName()))
                .build();
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{contextId}/assets/{assetId}")
    @Operation(
            summary = "Deletes an Asset from the given Parameter Context",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = AssetEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This endpoint will create a new Asset in the given Parameter Context. The Asset will be created with the given name and the contents of the file that is uploaded. " +
                    "The Asset will be created in the given Parameter Context, and will be available for use by any component that references the Parameter Context.",
            security = {
                    @SecurityRequirement(name = "Read - /parameter-contexts/{parameterContextId}"),
                    @SecurityRequirement(name = "Write - /parameter-contexts/{parameterContextId}"),
                    @SecurityRequirement(name = "Read - for every component that is affected by the update"),
                    @SecurityRequirement(name = "Write - for every component that is affected by the update"),
                    @SecurityRequirement(name = "Read - for every currently inherited parameter context")
            }
    )
    public Response deleteAsset(
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false")
            final Boolean disconnectedNodeAcknowledged,
            @Parameter(description = "The ID of the Parameter Context")
            @PathParam("contextId")
            final String parameterContextId,
            @Parameter(description = "The ID of the Asset")
            @PathParam("assetId")
            final String assetId
    ) {
        if (StringUtils.isBlank(parameterContextId)) {
            throw new IllegalArgumentException("Parameter context id is required");
        }
        if (StringUtils.isBlank(assetId)) {
            throw new IllegalArgumentException("Asset id is required");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        // Get the context or throw ResourceNotFoundException
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final ParameterContextEntity contextEntity = serviceFacade.getParameterContext(parameterContextId, false, user);

        final AssetDTO assetDTO = new AssetDTO();
        assetDTO.setId(assetId);

        final AssetEntity assetEntity = new AssetEntity();
        assetEntity.setAsset(assetDTO);

        // Need to call into service facade with a lock to ensure that the parameter context can't be updated to
        // reference the asset being deleted at the same time that we are verifying no parameters reference it
        return withWriteLock(
                serviceFacade,
                assetEntity,
                lookup -> {
                    // Deletion of an asset will only be allowed when it is not referenced by any parameters, so we only need to
                    // authorize that the user has access to modify the context which is READ and WRITE on the context itself
                    final ParameterContext parameterContext = lookup.getParameterContext(contextEntity.getId());
                    parameterContext.authorize(authorizer, RequestAction.READ, user);
                    parameterContext.authorize(authorizer, RequestAction.WRITE, user);
                },
                () -> serviceFacade.verifyDeleteAsset(contextEntity.getId(), assetId),
                requestEntity -> {
                    final String requestAssetId = requestEntity.getAsset().getId();
                    final AssetEntity deletedAsset = serviceFacade.deleteAsset(contextEntity.getId(), requestAssetId);
                    return generateOkResponse(deletedAsset).build();
                }
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{contextId}/update-requests")
    @Operation(
            summary = "Initiate the Update Request of a Parameter Context",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterContextUpdateRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This will initiate the process of updating a Parameter Context. Changing the value of a Parameter may require that one or more components be stopped and " +
                    "restarted, so this action may take significantly more time than many other REST API actions. As a result, this endpoint will immediately return a " +
                    "ParameterContextUpdateRequestEntity, " +
                    "and the process of updating the necessary components will occur asynchronously in the background. The client may then periodically poll the status of the request by " +
                    "issuing a GET request to /parameter-contexts/update-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
                    "/parameter-contexts/update-requests/{requestId}.",
            security = {
                    @SecurityRequirement(name = "Read - /parameter-contexts/{parameterContextId}"),
                    @SecurityRequirement(name = "Write - /parameter-contexts/{parameterContextId}"),
                    @SecurityRequirement(name = "Read - for every component that is affected by the update"),
                    @SecurityRequirement(name = "Write - for every component that is affected by the update"),
                    @SecurityRequirement(name = "Read - for every currently inherited parameter context"),
                    @SecurityRequirement(name = "Read - for any new inherited parameter context")
            }
    )
    public Response submitParameterContextUpdate(
            @PathParam("contextId") final String contextId,
            @Parameter(description = "The updated version of the parameter context.", required = true) final ParameterContextEntity requestEntity) {

        if (requestEntity == null) {
            throw new IllegalArgumentException("Parameter Context must be specified.");
        }

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Parameter Context Revision must be specified");
        }

        final ParameterContextDTO contextDto = requestEntity.getComponent();
        if (contextDto == null) {
            throw new IllegalArgumentException("Parameter Context must be specified");
        }

        if (contextDto.getId() == null) {
            throw new IllegalArgumentException("Parameter Context's ID must be specified");
        }
        if (!contextDto.getId().equals(contextId)) {
            throw new IllegalArgumentException("ID of Parameter Context in message body does not match Parameter Context ID supplied in URI");
        }

        validateParameterNames(contextDto);
        validateAssetReferences(contextDto);

        // We will perform the updating of the Parameter Context in a background thread because it can be a long-running process.
        // In order to do this, we will need some objects that are only available as Thread-Local variables to the current
        // thread, so we will gather the values for these objects up front.
        final boolean replicateRequest = isReplicateRequest();
        final ComponentLifecycle componentLifecycle = replicateRequest ? clusterComponentLifecycle : localComponentLifecycle;
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // Workflow for this process:
        // 1. Determine which components will be affected and are enabled/running
        // 2. Verify READ and WRITE permissions for user, for every component that is affected
        // 3. Verify READ and WRITE permissions for user, for Parameter Context
        // 4. Stop all Processors that are affected.
        // 5. Wait for all of the Processors to finish stopping.
        // 6. Disable all Controller Services that are affected.
        // 7. Wait for all Controller Services to finish disabling.
        // 8. Update Parameter Context
        // 9. Re-Enable all affected Controller Services
        // 10. Re-Start all Processors

        final Set<AffectedComponentEntity> affectedComponents = serviceFacade.getComponentsAffectedByParameterContextUpdate(Collections.singletonList(contextDto));
        logger.debug("Received Update Request for Parameter Context: {}; the following {} components will be affected: {}", requestEntity, affectedComponents.size(), affectedComponents);

        final InitiateChangeParameterContextRequestWrapper requestWrapper = new InitiateChangeParameterContextRequestWrapper(requestEntity, componentLifecycle, getAbsolutePath(),
                affectedComponents, replicateRequest, user);

        final Revision requestRevision = getRevision(requestEntity.getRevision(), contextDto.getId());
        return withWriteLock(
                serviceFacade,
                requestWrapper,
                requestRevision,
                lookup -> {
                    // Verify READ and WRITE permissions for user, for the Parameter Context itself
                    final ParameterContext parameterContext = lookup.getParameterContext(contextId);
                    parameterContext.authorize(authorizer, RequestAction.READ, user);
                    parameterContext.authorize(authorizer, RequestAction.WRITE, user);

                    // Verify READ and WRITE permissions for user, for every component that is affected
                    affectedComponents.forEach(component -> parameterUpdateManager.authorizeAffectedComponent(component, lookup, user, true, true));

                    validateControllerServiceReferences(requestEntity, lookup, parameterContext, user);
                },
                () -> {
                    // Verify Request
                    serviceFacade.verifyUpdateParameterContext(contextDto, false);
                },
                this::submitUpdateRequest
        );
    }

    private void validateControllerServiceReferences(final ParameterContextEntity requestEntity, final AuthorizableLookup lookup, final ParameterContext parameterContext, final NiFiUser user) {
        final Set<ParameterEntity> parametersEntities = requestEntity.getComponent().getParameters();
        for (ParameterEntity parameterEntity : parametersEntities) {
            final String parameterName = parameterEntity.getParameter().getName();
            final List<ParameterReferencedControllerServiceData> referencedControllerServiceDataSet =
                parameterContext.getParameterReferenceManager().getReferencedControllerServiceData(parameterContext, parameterName);

            final Set<? extends Class<? extends ControllerService>> referencedControllerServiceTypes = referencedControllerServiceDataSet.stream()
                .map(ParameterReferencedControllerServiceData::getReferencedControllerServiceType)
                .collect(Collectors.toSet());

            if (referencedControllerServiceTypes.size() > 1) {
                throw new IllegalStateException("Parameter is used by multiple different types of controller service references");
            } else if (!referencedControllerServiceTypes.isEmpty()) {
                final Optional<org.apache.nifi.parameter.Parameter> parameterOptional = parameterContext.getParameter(parameterName);
                if (parameterOptional.isPresent()) {
                    final String currentParameterValue = parameterOptional.get().getValue();
                    if (currentParameterValue != null) {
                        final ComponentAuthorizable currentControllerService = lookup.getControllerService(currentParameterValue);
                        if (currentControllerService != null) {
                            final Authorizable currentControllerServiceAuthorizable = currentControllerService.getAuthorizable();
                            currentControllerServiceAuthorizable.authorize(authorizer, RequestAction.READ, user);
                            currentControllerServiceAuthorizable.authorize(authorizer, RequestAction.WRITE, user);
                        }
                    }
                }

                final String newParameterValue = parameterEntity.getParameter().getValue();
                if (newParameterValue != null) {
                    final ComponentAuthorizable newControllerService = lookup.getControllerService(newParameterValue);
                    if (newControllerService != null) {
                        final Authorizable newControllerServiceAuthorizable = newControllerService.getAuthorizable();
                        if (newControllerServiceAuthorizable != null) {
                            newControllerServiceAuthorizable.authorize(authorizer, RequestAction.READ, user);
                            newControllerServiceAuthorizable.authorize(authorizer, RequestAction.WRITE, user);

                            final Class<?> firstClass = referencedControllerServiceTypes.iterator().next();
                            final ComponentNode componentNode = (ComponentNode) newControllerServiceAuthorizable;
                            final Class<?> componentClass = componentNode.getComponent().getClass();

                            if (!firstClass.isAssignableFrom(componentClass)) {
                                throw new IllegalArgumentException("New Parameter value attempts to reference an incompatible controller service");
                            }
                        }
                    }
                }
            }
        }
    }


    private void validateAssetReferences(final ParameterContextDTO parameterContextDto) {
        if (parameterContextDto.getParameters() != null) {
            for (final ParameterEntity entity : parameterContextDto.getParameters()) {
                final List<AssetReferenceDTO> referencedAssets = entity.getParameter().getReferencedAssets();
                if (referencedAssets == null) {
                    continue;
                }

                for (final AssetReferenceDTO referencedAsset : referencedAssets) {
                    if (StringUtils.isBlank(referencedAsset.getId())) {
                        throw new IllegalArgumentException("Asset reference id cannot be blank");
                    }
                    final Asset asset = assetManager.getAsset(referencedAsset.getId())
                            .orElseThrow(() -> new IllegalArgumentException("Request contains a reference to an Asset (%s) that does not exist".formatted(referencedAsset)));
                    if (!asset.getParameterContextIdentifier().equals(parameterContextDto.getId())) {
                        throw new IllegalArgumentException("Request contains a reference to an Asset (%s) that does not exist in Parameter Context (%s)"
                                .formatted(referencedAsset, parameterContextDto.getId()));
                    }
                }

                if (!referencedAssets.isEmpty() && entity.getParameter().getValue() != null) {
                    throw new IllegalArgumentException(
                        "Request contains a Parameter (%s) with both a value and a reference to an Asset. A Parameter may have either a value or a reference to an Asset, but not both."
                            .formatted(entity.getParameter().getName()));
                }

                if (!referencedAssets.isEmpty() && entity.getParameter().getSensitive() != null && entity.getParameter().getSensitive()) {
                    throw new IllegalArgumentException("Request contains a sensitive Parameter (%s) with references to an Assets. Sensitive parameters may not reference Assets."
                            .formatted(entity.getParameter().getName()));
                }
            }
        }
    }

    private void validateParameterNames(final ParameterContextDTO parameterContextDto) {
        if (parameterContextDto.getParameters() != null) {
            for (final ParameterEntity entity : parameterContextDto.getParameters()) {
                final String parameterName = entity.getParameter().getName();
                if (!isLegalParameterName(parameterName)) {
                    throw new IllegalArgumentException("Request contains an illegal Parameter Name (" + parameterName
                            + "). Parameter names may only include letters, numbers, spaces, and the special characters .-_");
                }
            }
        }
    }

    private boolean isLegalParameterName(final String parameterName) {
        return VALID_PARAMETER_NAME_PATTERN.matcher(parameterName).matches();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{contextId}/update-requests/{requestId}")
    @Operation(
            summary = "Returns the Update Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterContextUpdateRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Returns the Update Request with the given ID. Once an Update Request has been created by performing a POST to /nifi-api/parameter-contexts, "
                    + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
                    + "current state of the request, and any failures. ",
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can get it")
            }
    )
    public Response getParameterContextUpdate(
            @Parameter(description = "The ID of the Parameter Context")
            @PathParam("contextId") final String contextId,
            @Parameter(description = "The ID of the Update Request")
            @PathParam("requestId") final String updateRequestId) {

        authorizeReadParameterContext(contextId);

        return retrieveUpdateRequest("update-requests", contextId, updateRequestId);
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{contextId}/update-requests/{requestId}")
    @Operation(
            summary = "Deletes the Update Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterContextUpdateRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Deletes the Update Request with the given ID. After a request is created via a POST to /nifi-api/parameter-contexts/update-requests, it is expected "
                    + "that the client will properly clean up the request by DELETE'ing it, once the Update process has completed. If the request is deleted before the request "
                    + "completes, then the Update request will finish the step that it is currently performing and then will cancel any subsequent steps.",
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can remove it")
            }
    )
    public Response deleteUpdateRequest(
            @Parameter(
                    description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(description = "The ID of the ParameterContext")
            @PathParam("contextId") final String contextId,
            @Parameter(description = "The ID of the Update Request")
            @PathParam("requestId") final String updateRequestId) {

        authorizeReadParameterContext(contextId);
        return deleteUpdateRequest("update-requests", contextId, updateRequestId, disconnectedNodeAcknowledged.booleanValue());
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Deletes the Parameter Context with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterContextEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Deletes the Parameter Context with the given ID.",
            security = {
                    @SecurityRequirement(name = "Read - /parameter-contexts/{uuid}"),
                    @SecurityRequirement(name = "Write - /parameter-contexts/{uuid}"),
                    @SecurityRequirement(name = "Read - /process-groups/{uuid}, for any Process Group that is currently bound to the Parameter Context"),
                    @SecurityRequirement(name = "Write - /process-groups/{uuid}, for any Process Group that is currently bound to the Parameter Context")
            }
    )
    public Response deleteParameterContext(
            @Parameter(
                    description = "The version is used to verify the client is working with the latest version of the flow."
            )
            @QueryParam(VERSION) final LongParameter version,
            @Parameter(
                    description = "If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response."
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @Parameter(
                    description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED)
            @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(description = "The Parameter Context ID.")
            @PathParam("id") final String parameterContextId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), parameterContextId);
        return withWriteLock(
                serviceFacade,
                null,
                requestRevision,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();

                    final Authorizable parameterContext = lookup.getParameterContext(parameterContextId);
                    parameterContext.authorize(authorizer, RequestAction.READ, user);
                    parameterContext.authorize(authorizer, RequestAction.WRITE, user);

                    final ParameterContextEntity contextEntity = serviceFacade.getParameterContext(parameterContextId, false, user);
                    for (final ProcessGroupEntity boundGroupEntity : contextEntity.getComponent().getBoundProcessGroups()) {
                        final String groupId = boundGroupEntity.getId();
                        final Authorizable groupAuthorizable = lookup.getProcessGroup(groupId).getAuthorizable();
                        groupAuthorizable.authorize(authorizer, RequestAction.READ, user);
                        groupAuthorizable.authorize(authorizer, RequestAction.WRITE, user);
                    }
                },
                () -> serviceFacade.verifyDeleteParameterContext(parameterContextId),
                (revision, groupEntity) -> {
                    // disconnect from version control
                    final ParameterContextEntity entity = serviceFacade.deleteParameterContext(revision, parameterContextId);

                    // generate the response
                    return generateOkResponse(entity).build();
                });

    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{contextId}/validation-requests")
    @Operation(
            summary = "Initiate a Validation Request to determine how the validity of components will change if a Parameter Context were to be updated",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterContextValidationRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "This will initiate the process of validating all components whose Process Group is bound to the specified Parameter Context. Performing validation against " +
                    "an arbitrary number of components may be expect and take significantly more time than many other REST API actions. As a result, this endpoint will immediately return " +
                    "a ParameterContextValidationRequestEntity, " +
                    "and the process of validating the necessary components will occur asynchronously in the background. The client may then periodically poll the status of the request by " +
                    "issuing a GET request to /parameter-contexts/validation-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to " +
                    "/parameter-contexts/validation-requests/{requestId}.",
            security = {
                    @SecurityRequirement(name = "Read - /parameter-contexts/{parameterContextId}")
            }
    )
    public Response submitValidationRequest(
            @PathParam("contextId") final String contextId,
            @Parameter(description = "The validation request", required = true) final ParameterContextValidationRequestEntity requestEntity) {

        if (requestEntity == null) {
            throw new IllegalArgumentException("Parameter Context must be specified.");
        }

        final ParameterContextValidationRequestDTO requestDto = requestEntity.getRequest();
        if (requestDto == null) {
            throw new IllegalArgumentException("Parameter Context must be specified");
        }

        if (requestDto.getParameterContext() == null) {
            throw new IllegalArgumentException("Parameter Context must be specified");
        }
        if (requestDto.getParameterContext().getId() == null) {
            throw new IllegalArgumentException("Parameter Context's ID must be specified");
        }

        if (isReplicateRequest()) {
            return replicate("POST", requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestEntity,
                lookup -> {
                    final Authorizable parameterContext = lookup.getParameterContext(contextId);
                    parameterContext.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());

                    authorizeReferencingComponents(requestEntity.getRequest().getParameterContext().getId(), lookup, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                },
                entity -> performAsyncValidation(entity, NiFiUserUtils.getNiFiUser())
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

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{contextId}/validation-requests/{id}")
    @Operation(
            summary = "Returns the Validation Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterContextValidationRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Returns the Validation Request with the given ID. Once a Validation Request has been created by performing a POST to /nifi-api/validation-contexts, "
                    + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
                    + "current state of the request, and any failures. ",
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can get it")
            }
    )
    public Response getValidationRequest(
            @Parameter(description = "The ID of the Parameter Context")
            @PathParam("contextId") final String contextId,
            @Parameter(description = "The ID of the Validation Request")
            @PathParam("id") final String validationRequestId) {

        authorizeReadParameterContext(contextId);

        if (isReplicateRequest()) {
            return replicate("GET");
        }

        return retrieveValidationRequest("validation-requests", contextId, validationRequestId);
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{contextId}/validation-requests/{id}")
    @Operation(
            summary = "Deletes the Validation Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ParameterContextValidationRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Deletes the Validation Request with the given ID. After a request is created via a POST to /nifi-api/validation-contexts, it is expected "
                    + "that the client will properly clean up the request by DELETE'ing it, once the validation process has completed. If the request is deleted before the request "
                    + "completes, then the Validation request will finish the step that it is currently performing and then will cancel any subsequent steps.",
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can remove it")
            }
    )
    public Response deleteValidationRequest(
            @Parameter(description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.")
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED)
            @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(description = "The ID of the Parameter Context")
            @PathParam("contextId") final String contextId,
            @Parameter(description = "The ID of the Update Request")
            @PathParam("id") final String validationRequestId) {

        authorizeReadParameterContext(contextId);

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        return deleteValidationRequest("validation-requests", contextId, validationRequestId, disconnectedNodeAcknowledged.booleanValue());
    }


    private Response performAsyncValidation(final ParameterContextValidationRequestEntity requestEntity, final NiFiUser user) {
        // Create an asynchronous request that will occur in the background, because this request may
        // result in stopping components, which can take an indeterminate amount of time.
        final String requestId = generateUuid();
        final AsynchronousWebRequest<ParameterContextValidationRequestEntity, ComponentValidationResultsEntity> request = new StandardAsynchronousWebRequest<>(requestId, requestEntity, null, user,
                getValidationSteps());

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<ParameterContextValidationRequestEntity, ComponentValidationResultsEntity>> validationTask = asyncRequest -> {
            try {
                final ComponentValidationResultsEntity resultEntity = validateComponents(requestEntity, user);
                asyncRequest.markStepComplete(resultEntity);
            } catch (final Exception e) {
                logger.error("Failed to validate components", e);
                asyncRequest.fail("Failed to validation components due to " + e);
            }
        };

        validationRequestManager.submitRequest("validation-requests", requestId, request, validationTask);

        // Generate the response.
        final ParameterContextValidationRequestDTO validationRequestDto = new ParameterContextValidationRequestDTO();
        validationRequestDto.setComplete(request.isComplete());
        validationRequestDto.setFailureReason(request.getFailureReason());
        validationRequestDto.setLastUpdated(request.getLastUpdated());
        validationRequestDto.setRequestId(requestId);
        validationRequestDto.setUri(generateResourceUri("parameter-contexts", "validation-requests", requestId));
        validationRequestDto.setPercentCompleted(request.getPercentComplete());
        validationRequestDto.setState(request.getState());
        validationRequestDto.setComponentValidationResults(request.getResults());

        final ParameterContextValidationRequestEntity validationRequestEntity = new ParameterContextValidationRequestEntity();
        validationRequestEntity.setRequest(validationRequestDto);

        return generateOkResponse(validationRequestEntity).build();
    }

    private List<UpdateStep> getValidationSteps() {
        return Collections.singletonList(new StandardUpdateStep("Validating Components"));
    }

    private ComponentValidationResultsEntity validateComponents(final ParameterContextValidationRequestEntity requestEntity, final NiFiUser user) {
        final List<ComponentValidationResultEntity> resultEntities = serviceFacade.validateComponents(requestEntity.getRequest().getParameterContext(), user);
        final ComponentValidationResultsEntity resultsEntity = new ComponentValidationResultsEntity();
        resultsEntity.setValidationResults(resultEntities);
        return resultsEntity;
    }

    private Response submitUpdateRequest(final Revision requestRevision, final InitiateChangeParameterContextRequestWrapper requestWrapper) {
        // Create an asynchronous request that will occur in the background, because this request may
        // result in stopping components, which can take an indeterminate amount of time.
        final String requestId = UUID.randomUUID().toString();
        final String contextId = requestWrapper.getParameterContextEntity().getComponent().getId();
        final AsynchronousWebRequest<List<ParameterContextEntity>, List<ParameterContextEntity>> request = new StandardAsynchronousWebRequest<>(
                requestId, Collections.singletonList(requestWrapper.getParameterContextEntity()), contextId, requestWrapper.getUser(), getUpdateSteps());

        // Submit the request to be performed in the background
        final Consumer<AsynchronousWebRequest<List<ParameterContextEntity>, List<ParameterContextEntity>>> updateTask = asyncRequest -> {
            try {
                final List<ParameterContextEntity> updatedParameterContextEntities = parameterUpdateManager.updateParameterContexts(
                    asyncRequest, requestWrapper.getComponentLifecycle(), requestWrapper.getExampleUri(),
                    requestWrapper.getReferencingComponents(), requestWrapper.isReplicateRequest(), requestRevision,
                    List.of(requestWrapper.getParameterContextEntity()));

                asyncRequest.markStepComplete(updatedParameterContextEntities);
            } catch (final ResumeFlowException rfe) {
                // Treat ResumeFlowException differently because we don't want to include a message that we couldn't update the flow
                // since in this case the flow was successfully updated - we just couldn't re-enable the components.
                logger.error(rfe.getMessage(), rfe);
                asyncRequest.fail(rfe.getMessage());
            } catch (final Exception e) {
                logger.error("Failed to update Parameter Context", e);
                asyncRequest.fail("Failed to update Parameter Context due to " + e);
            }
        };

        updateRequestManager.submitRequest("update-requests", requestId, request, updateTask);

        // Generate the response.
        final ParameterContextUpdateRequestEntity updateRequestEntity = createUpdateRequestEntity(request, "update-requests", contextId, requestId);
        return generateOkResponse(updateRequestEntity).build();
    }

    private List<UpdateStep> getUpdateSteps() {
        return Arrays.asList(new StandardUpdateStep("Stopping Affected Processors"),
                new StandardUpdateStep("Disabling Affected Controller Services"),
                new StandardUpdateStep("Updating Parameter Context"),
                new StandardUpdateStep("Re-Enabling Affected Controller Services"),
                new StandardUpdateStep("Restarting Affected Processors"));
    }

    private Response retrieveValidationRequest(final String requestType, final String contextId, final String requestId) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final AsynchronousWebRequest<?, ComponentValidationResultsEntity> asyncRequest = validationRequestManager.getRequest(requestType, requestId, user);
        final ParameterContextValidationRequestEntity requestEntity = createValidationRequestEntity(asyncRequest, contextId, requestType, requestId);
        return generateOkResponse(requestEntity).build();
    }

    private Response deleteValidationRequest(final String requestType, final String contextId, final String requestId, final boolean disconnectedNodeAcknowledged) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<?, ComponentValidationResultsEntity> asyncRequest = validationRequestManager.removeRequest(requestType, requestId, user);
        if (asyncRequest == null) {
            throw new ResourceNotFoundException("Could not find request of type " + requestType + " with ID " + requestId);
        }

        if (!asyncRequest.isComplete()) {
            asyncRequest.cancel();
        }

        final ParameterContextValidationRequestEntity requestEntity = createValidationRequestEntity(asyncRequest, contextId, requestType, requestId);
        return generateOkResponse(requestEntity).build();
    }

    private ParameterContextValidationRequestEntity createValidationRequestEntity(final AsynchronousWebRequest<?, ComponentValidationResultsEntity> asyncRequest, final String requestType,
                                                                                  final String contextId, final String requestId) {
        final ParameterContextValidationRequestDTO requestDto = new ParameterContextValidationRequestDTO();

        requestDto.setComplete(asyncRequest.isComplete());
        requestDto.setFailureReason(asyncRequest.getFailureReason());
        requestDto.setLastUpdated(asyncRequest.getLastUpdated());
        requestDto.setRequestId(requestId);
        requestDto.setUri(generateResourceUri("parameter-contexts", contextId, requestType, requestId));
        requestDto.setState(asyncRequest.getState());
        requestDto.setPercentCompleted(asyncRequest.getPercentComplete());
        requestDto.setComponentValidationResults(asyncRequest.getResults());

        final ParameterContextValidationRequestEntity entity = new ParameterContextValidationRequestEntity();
        entity.setRequest(requestDto);
        return entity;
    }

    private Response retrieveUpdateRequest(final String requestType, final String contextId, final String requestId) {
        if (requestId == null) {
            throw new IllegalArgumentException("Request ID must be specified.");
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request manager will ensure that the current is the user that submitted this request
        final AsynchronousWebRequest<List<ParameterContextEntity>, List<ParameterContextEntity>> asyncRequest = updateRequestManager.getRequest(requestType, requestId, user);
        final ParameterContextUpdateRequestEntity updateRequestEntity = createUpdateRequestEntity(asyncRequest, requestType, contextId, requestId);
        return generateOkResponse(updateRequestEntity).build();
    }

    private Response deleteUpdateRequest(final String requestType, final String contextId, final String requestId, final boolean disconnectedNodeAcknowledged) {
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

        final ParameterContextUpdateRequestEntity updateRequestEntity = createUpdateRequestEntity(asyncRequest, requestType, contextId, requestId);
        return generateOkResponse(updateRequestEntity).build();
    }

    private ParameterContextUpdateRequestEntity createUpdateRequestEntity(final AsynchronousWebRequest<List<ParameterContextEntity>, List<ParameterContextEntity>> asyncRequest,
                                                                          final String requestType, final String contextId, final String requestId) {
        final List<ParameterContextEntity> initialRequestList = asyncRequest.getRequest();
        // Safe because this is from the request, not the response
        final ParameterContextEntity initialEntity = initialRequestList.get(0);
        final ParameterContextUpdateRequestDTO updateRequestDto = new ParameterContextUpdateRequestDTO();
        updateRequestDto.setComplete(asyncRequest.isComplete());
        updateRequestDto.setFailureReason(asyncRequest.getFailureReason());
        updateRequestDto.setLastUpdated(asyncRequest.getLastUpdated());
        updateRequestDto.setRequestId(requestId);
        updateRequestDto.setUri(generateResourceUri("parameter-contexts", contextId, requestType, requestId));
        updateRequestDto.setState(asyncRequest.getState());
        updateRequestDto.setPercentCompleted(asyncRequest.getPercentComplete());

        final List<ParameterContextUpdateStepDTO> updateSteps = new ArrayList<>();
        for (final UpdateStep updateStep : asyncRequest.getUpdateSteps()) {
            final ParameterContextUpdateStepDTO stepDto = new ParameterContextUpdateStepDTO();
            stepDto.setDescription(updateStep.getDescription());
            stepDto.setComplete(updateStep.isComplete());
            stepDto.setFailureReason(updateStep.getFailureReason());
            updateSteps.add(stepDto);
        }
        updateRequestDto.setUpdateSteps(updateSteps);

        // The AffectedComponentEntity itself does not evaluate equality based on component information. As a result, we want to de-dupe the entities based on their identifiers.
        final Map<String, AffectedComponentEntity> affectedComponents = new HashMap<>();
        for (final ParameterEntity entity : initialEntity.getComponent().getParameters()) {
            for (final AffectedComponentEntity affectedComponentEntity : entity.getParameter().getReferencingComponents()) {
                affectedComponents.put(affectedComponentEntity.getId(), serviceFacade.getUpdatedAffectedComponentEntity(affectedComponentEntity));
            }
        }
        updateRequestDto.setReferencingComponents(new HashSet<>(affectedComponents.values()));

        // Populate the Affected Components
        final ParameterContextEntity contextEntity = serviceFacade.getParameterContext(asyncRequest.getComponentId(), false, NiFiUserUtils.getNiFiUser());
        final ParameterContextUpdateRequestEntity updateRequestEntity = new ParameterContextUpdateRequestEntity();

        // If the request is complete, include the new representation of the Parameter Context along with its new Revision. Otherwise, do not include the information, since it is 'stale'
        if (updateRequestDto.isComplete()) {
            updateRequestDto.setParameterContext(contextEntity == null ? null : contextEntity.getComponent());
            updateRequestEntity.setParameterContextRevision(contextEntity == null ? null : contextEntity.getRevision());
        }

        updateRequestEntity.setRequest(updateRequestDto);
        return updateRequestEntity;
    }

    private static class InitiateChangeParameterContextRequestWrapper extends Entity {
        private final ParameterContextEntity parameterContextEntity;
        private final ComponentLifecycle componentLifecycle;
        private final URI exampleUri;
        private final Set<AffectedComponentEntity> affectedComponents;
        private final boolean replicateRequest;
        private final NiFiUser nifiUser;

        public InitiateChangeParameterContextRequestWrapper(final ParameterContextEntity parameterContextEntity, final ComponentLifecycle componentLifecycle,
                                                            final URI exampleUri, final Set<AffectedComponentEntity> affectedComponents, final boolean replicateRequest,
                                                            final NiFiUser nifiUser) {

            this.parameterContextEntity = parameterContextEntity;
            this.componentLifecycle = componentLifecycle;
            this.exampleUri = exampleUri;
            this.affectedComponents = affectedComponents;
            this.replicateRequest = replicateRequest;
            this.nifiUser = nifiUser;
        }

        public ParameterContextEntity getParameterContextEntity() {
            return parameterContextEntity;
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
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @Autowired
    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Qualifier("clusterComponentLifecycle")
    @Autowired(required = false)
    public void setClusterComponentLifecycle(ComponentLifecycle componentLifecycle) {
        this.clusterComponentLifecycle = componentLifecycle;
    }

    @Qualifier("localComponentLifecycle")
    @Autowired
    public void setLocalComponentLifecycle(ComponentLifecycle componentLifecycle) {
        this.localComponentLifecycle = componentLifecycle;
    }

    @Autowired
    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

    @Autowired
    public void setAssetManager(final AssetManager assetManager) {
        this.assetManager = assetManager;
    }

    @Autowired(required = false)
    public void setUploadRequestReplicator(final UploadRequestReplicator uploadRequestReplicator) {
        this.uploadRequestReplicator = uploadRequestReplicator;
    }

    @Autowired
    public void setAffectedComponentManager(final AssetComponentManager assetComponentManager) {
        this.assetComponentManager = assetComponentManager;
    }
}
