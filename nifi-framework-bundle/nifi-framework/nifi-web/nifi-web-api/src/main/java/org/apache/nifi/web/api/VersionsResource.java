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
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.ProcessGroupAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.FlowLocation;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.VersionedFlowDTO;
import org.apache.nifi.web.api.dto.VersionedFlowUpdateRequestDTO;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.CreateActiveRequestEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.StartVersionControlRequestEntity;
import org.apache.nifi.web.api.entity.VersionControlComponentMappingEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotEntity;
import org.apache.nifi.web.api.entity.VersionedFlowUpdateRequestEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.nifi.web.util.ComponentLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Controller
@Path("/versions")
@Tag(name = "Versions")
public class VersionsResource extends FlowUpdateResource<VersionControlInformationEntity, VersionedFlowUpdateRequestEntity> {
    private static final Logger logger = LoggerFactory.getLogger(VersionsResource.class);

    // We need to ensure that only a single Version Control Request can occur throughout the flow.
    // Otherwise, User 1 could log into Node 1 and choose to Version Control Group A.
    // At the same time, User 2 could log into Node 2 and choose to Version Control Group B, which is a child of Group A.
    // As a result, only one of the requests would succeed (the other would be rejected due to the Revision being wrong).
    // However, the request that was rejected may well have already caused the flow to be added to the Flow Registry,
    // so we would have created a flow in the Flow Registry that will never be referenced and will essentially be "orphaned."
    private ActiveRequest activeRequest = null;
    private final Object activeRequestMonitor = new Object();

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @Operation(
            summary = "Gets the Version Control information for a process group",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionControlInformationEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Read - /process-groups/{uuid}")
            }
    )
    public Response getVersionInformation(@Parameter(description = "The process group id.", required = true) @PathParam("id") final String groupId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
            processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the version control information for this process group
        VersionControlInformationEntity entity = serviceFacade.getVersionControlInformation(groupId);
        if (entity == null) {
            final ProcessGroupEntity processGroup = serviceFacade.getProcessGroup(groupId);
            entity = new VersionControlInformationEntity();
            entity.setProcessGroupRevision(processGroup.getRevision());
        }

        return generateOkResponse(entity).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}/download")
    @Operation(
            summary = "Gets the latest version of a Process Group for download",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /process-groups/{uuid}")
            }
    )
    public Response exportFlowVersion(@Parameter(description = "The process group id.", required = true) @PathParam("id") final String groupId) {
        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
            // ensure access to process groups (nested), encapsulated controller services and referenced parameter contexts
            authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.READ, true,
                    false, false, false, true);
        });

        // get the versioned flow
        final FlowSnapshotContainer snapshotContainer = serviceFacade.getVersionedFlowSnapshotByGroupId(groupId);
        final RegisteredFlowSnapshot versionedFlowSnapshot = snapshotContainer.getFlowSnapshot();

        final VersionedProcessGroup versionedProcessGroup = versionedFlowSnapshot.getFlowContents();
        final String flowName = versionedProcessGroup.getName();
        final String flowVersion = versionedFlowSnapshot.getSnapshotMetadata().getVersion();

        // clear top-level registry data which doesn't belong in versioned flow download
        versionedFlowSnapshot.setFlow(null);
        versionedFlowSnapshot.setBucket(null);
        versionedFlowSnapshot.setSnapshotMetadata(null);

        // clear nested process group registry data which doesn't belong in versioned flow download
        sanitizeRegistryInfo(versionedProcessGroup);

        // determine the name of the attachment - possible issues with spaces in file names
        final String filename = flowName.replaceAll("\\s", "_") + "_" + flowVersion + ".json";

        return generateOkResponse(versionedFlowSnapshot).header(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", filename)).build();
    }

    /**
     * Recursively clear the registry info in the given versioned process group and all nested versioned process groups
     *
     * @param versionedProcessGroup the process group to sanitize
     */
    private void sanitizeRegistryInfo(final VersionedProcessGroup versionedProcessGroup) {
        versionedProcessGroup.setVersionedFlowCoordinates(null);

        for (final VersionedProcessGroup innerVersionedProcessGroup : versionedProcessGroup.getProcessGroups()) {
            sanitizeRegistryInfo(innerVersionedProcessGroup);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("active-requests")
    @Operation(
            summary = "Create a version control request",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Creates a request so that a Process Group can be placed under Version Control or have its Version Control configuration changed. Creating this request will "
                    + "prevent any other threads from simultaneously saving local changes to Version Control. It will not, however, actually save the local flow to the Flow Registry. A "
                    + "POST to /versions/process-groups/{id} should be used to initiate saving of the local flow to the Flow Registry. "
                    + NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Write - /process-groups/{uuid}")
            }
    )
    public Response createVersionControlRequest(
            @Parameter(description = "The versioned flow details.", required = true) final CreateActiveRequestEntity requestEntity) {

        if (requestEntity == null || requestEntity.getProcessGroupId() == null) {
            throw new IllegalArgumentException("The id of the process group that will be updated must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return withWriteLock(
                serviceFacade,
                requestEntity,
                lookup -> {
                    final Authorizable processGroup = lookup.getProcessGroup(requestEntity.getProcessGroupId()).getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.WRITE, user);
                },
                /* verifier */ null,
                entity -> {
                    final String requestId = generateUuid();

                    // We need to ensure that only a single Version Control Request can occur throughout the flow.
                    // Otherwise, User 1 could log into Node 1 and choose to Version Control Group A.
                    // At the same time, User 2 could log into Node 2 and choose to Version Control Group B, which is a child of Group A.
                    // As a result, may could end up in a situation where we are creating flows in the registry that are never referenced.
                    synchronized (activeRequestMonitor) {
                        if (activeRequest == null || activeRequest.isExpired()) {
                            activeRequest = new ActiveRequest(requestId, user, entity.getProcessGroupId());
                        } else {
                            throw new IllegalStateException("A request is already underway to place a Process Group in this NiFi instance under Version Control. "
                                    + "Only a single such request is allowed to occurred at a time. Please try the request again momentarily.");
                        }
                    }

                    return generateOkResponse(requestId).build();
                });
    }


    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("active-requests/{id}")
    @Operation(
            summary = "Updates the request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionControlInformationEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can update it")
            }
    )
    public Response updateVersionControlRequest(@Parameter(description = "The request ID.") @PathParam("id") final String requestId,
                                                @Parameter(description = "The version control component mapping.", required = true) final VersionControlComponentMappingEntity requestEntity) {

        if (requestEntity == null) {
            throw new IllegalArgumentException("Version control information must be specified.");
        }

        // Verify request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified");
        }

        final VersionControlInformationDTO versionControlInfo = requestEntity.getVersionControlInformation();
        if (versionControlInfo == null) {
            throw new IllegalArgumentException("Version Control Information must be supplied");
        }
        if (versionControlInfo.getGroupId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Process Group ID");
        }
        if (versionControlInfo.getBucketId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Bucket ID");
        }
        if (versionControlInfo.getFlowId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Flow ID");
        }
        if (versionControlInfo.getRegistryId() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Registry ID");
        }
        if (versionControlInfo.getVersion() == null) {
            throw new IllegalArgumentException("Version Control Information must supply Version");
        }

        final Map<String, String> mapping = requestEntity.getVersionControlComponentMapping();
        if (mapping == null) {
            throw new IllegalArgumentException("Version Control Component Mapping must be supplied");
        }

        // Replicate if necessary
        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        // Perform the update
        synchronized (activeRequestMonitor) {
            if (activeRequest == null) {
                throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
            }

            if (!requestId.equals(activeRequest.getRequestId())) {
                throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
            }

            if (activeRequest.isExpired()) {
                throw new IllegalStateException("Version Control Request with ID " + requestId + " has already expired");
            }

            if (activeRequest.isUpdatePerformed()) {
                throw new IllegalStateException("Version Control Request with ID " + requestId + " has already been performed");
            }

            final String groupId = requestEntity.getVersionControlInformation().getGroupId();

            if (!activeRequest.getProcessGroupId().equals(groupId)) {
                throw new IllegalStateException("Version Control Request with ID " + requestId + " was created for a different process group id");
            }

            final Revision groupRevision = new Revision(revisionDto.getVersion(), revisionDto.getClientId(), groupId);
            return withWriteLock(
                    serviceFacade,
                    requestEntity,
                    groupRevision,
                    lookup -> {
                        final NiFiUser user = NiFiUserUtils.getNiFiUser();
                        if (user == null) {
                            throw new AccessDeniedException("Unknown user.");
                        }

                        if (!user.equals(activeRequest.getUser())) {
                            throw new AccessDeniedException("Only the user that creates the Version Control Request can use it.");
                        }
                    },
                    null,
                    (rev, mappingEntity) -> {
                        // set the version control information
                        final VersionControlInformationEntity responseEntity = serviceFacade.setVersionControlInformation(rev, groupId,
                                mappingEntity.getVersionControlInformation(), mappingEntity.getVersionControlComponentMapping());

                        // indicate that the active request has performed the update
                        activeRequest.updatePerformed();

                        return generateOkResponse(responseEntity).build();
                    });
        }
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("active-requests/{id}")
    @Operation(
            summary = "Deletes the version control request with the given ID",
            description = "Deletes the Version Control Request with the given ID. This will allow other threads to save flows to the Flow Registry. See also the documentation "
                    + "for POSTing to /versions/active-requests for information regarding why this is done. "
                    + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200"),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can remove it")
            }
    )
    public Response deleteVersionControlRequest(
            @Parameter(
                    description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(description = "The request ID.") @PathParam("id") final String requestId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        synchronized (activeRequestMonitor) {
            if (activeRequest == null) {
                throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
            }

            if (!requestId.equals(activeRequest.getRequestId())) {
                throw new IllegalStateException("No Version Control Request with ID " + requestId + " is currently active");
            }

            return withWriteLock(
                    serviceFacade,
                    null,
                    lookup -> {
                        final NiFiUser user = NiFiUserUtils.getNiFiUser();
                        if (user == null) {
                            throw new AccessDeniedException("Unknown user.");
                        }

                        if (!user.equals(activeRequest.getUser())) {
                            throw new AccessDeniedException("Only the user that creates the Version Control Request can use it.");
                        }
                    },
                    null,
                    requestEntity -> {
                        // clear the active request
                        activeRequest = null;

                        return generateOkResponse().build();
                    });
        }
    }


    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @Operation(
            summary = "Save the Process Group with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionControlInformationEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Begins version controlling the Process Group with the given ID or commits changes to the Versioned Flow, "
                    + "depending on if the provided VersionControlInformation includes a flowId. "
                    + NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Read - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Write - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Read - /{component-type}/{uuid} - For all encapsulated components"),
                    @SecurityRequirement(name = "Read - any referenced Controller Services by any encapsulated components - /controller-services/{uuid}")
            }
    )
    public Response saveToFlowRegistry(
            @Parameter(description = "The process group id.") @PathParam("id") final String groupId,
            @Parameter(description = "The versioned flow details.", required = true) final StartVersionControlRequestEntity requestEntity) {

        if (requestEntity == null) {
            throw new IllegalArgumentException("Version control request must be specified.");
        }

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified");
        }

        final VersionedFlowDTO versionedFlowDto = requestEntity.getVersionedFlow();
        if (versionedFlowDto == null) {
            throw new IllegalArgumentException("Version Control Information must be supplied.");
        }
        if (StringUtils.isEmpty(versionedFlowDto.getBucketId())) {
            throw new IllegalArgumentException("The Bucket ID must be supplied.");
        }
        if (StringUtils.isEmpty(versionedFlowDto.getFlowName()) && StringUtils.isEmpty(versionedFlowDto.getFlowId())) {
            throw new IllegalArgumentException("The Flow Name or Flow ID must be supplied.");
        }
        if (versionedFlowDto.getFlowName() != null && versionedFlowDto.getFlowName().length() > 1000) {
            throw new IllegalArgumentException("The Flow Name cannot exceed 1,000 characters");
        }
        if (StringUtils.isEmpty(versionedFlowDto.getRegistryId())) {
            throw new IllegalArgumentException("The Registry ID must be supplied.");
        }
        if (versionedFlowDto.getDescription() != null && versionedFlowDto.getDescription().length() > 65535) {
            throw new IllegalArgumentException("Flow Description cannot exceed 65,535 characters");
        }
        if (versionedFlowDto.getComments() != null && versionedFlowDto.getComments().length() > 65535) {
            throw new IllegalArgumentException("Comments cannot exceed 65,535 characters");
        }
        if (StringUtils.isEmpty(versionedFlowDto.getAction())) {
            throw new IllegalArgumentException("Action is required");
        }
        if (!VersionedFlowDTO.COMMIT_ACTION.equals(versionedFlowDto.getAction())
                && !VersionedFlowDTO.FORCE_COMMIT_ACTION.equals(versionedFlowDto.getAction())) {
            throw new IllegalArgumentException("Action must be one of " + VersionedFlowDTO.COMMIT_ACTION + " or " + VersionedFlowDTO.FORCE_COMMIT_ACTION);
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        // ensure we're not attempting to version the root group
        final ProcessGroupEntity root = serviceFacade.getProcessGroup(FlowManager.ROOT_GROUP_ID_ALIAS);
        if (root.getId().equals(groupId)) {
            throw new IllegalArgumentException("The Root Process Group cannot be versioned.");
        }

        if (isReplicateRequest()) {
            // We first have to obtain a "lock" on all nodes in the cluster so that multiple Version Control requests
            // are not being made simultaneously. We do this by making a POST to /nifi-api/versions/active-requests.
            // The Response gives us back the Request ID.
            final URI requestUri;
            try {
                final URI originalUri = getAbsolutePath();
                final String requestId = lockVersionControl(originalUri, groupId);

                requestUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                        originalUri.getPort(), "/nifi-api/versions/active-requests/" + requestId, null, originalUri.getFragment());
            } catch (final URISyntaxException e) {
                throw new RuntimeException(e);
            }


            // Now that we have the Request, we know that no other thread is updating the Flow Registry. So we can now
            // create the Flow in the Flow Registry and push the Process Group as the first version of the Flow. Once we've
            // succeeded with that, we need to update all nodes' Process Group to contain the new Version Control Information.
            // Finally, we can delete the Request.
            try {
                final VersionControlComponentMappingEntity mappingEntity = serviceFacade.registerFlowWithFlowRegistry(groupId, requestEntity);
                replicateVersionControlMapping(mappingEntity, requestEntity, requestUri, groupId);

                final VersionControlInformationEntity responseEntity = serviceFacade.getVersionControlInformation(groupId);
                return generateOkResponse(responseEntity).build();
            } finally {
                unlockVersionControl(requestUri, groupId);
            }
        }

        // Perform local task. If running in a cluster environment, we will never get to this point. This is because
        // in the above block, we check if (isReplicate()) and if true, we implement the 'cluster logic', but this
        // does not involve replicating the actual request, because we only want a single node to handle the logic of
        // creating the flow in the Registry.
        final Revision groupRevision = new Revision(revisionDto.getVersion(), revisionDto.getClientId(), groupId);
        return withWriteLock(
                serviceFacade,
                requestEntity,
                groupRevision,
                lookup -> {
                    final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
                    final Authorizable processGroup = groupAuthorizable.getAuthorizable();

                    // require write to this group
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // require read to this group and all descendants
                    authorizeProcessGroup(groupAuthorizable, authorizer, lookup, RequestAction.READ, true, false, true, false, true);
                },
                () -> {
                    final VersionedFlowDTO versionedFlow = requestEntity.getVersionedFlow();
                    final String registryId = versionedFlow.getRegistryId();
                    final String action = versionedFlow.getAction();
                    final FlowLocation flowLocation = new FlowLocation(versionedFlow.getBranch(), versionedFlow.getBucketId(), versionedFlow.getFlowId());
                    serviceFacade.verifyCanSaveToFlowRegistry(groupId, registryId, flowLocation, action);
                },
                (rev, flowEntity) -> {
                    // Register the current flow with the Flow Registry.
                    final VersionControlComponentMappingEntity mappingEntity = serviceFacade.registerFlowWithFlowRegistry(groupId, flowEntity);

                    // Update the Process Group's Version Control Information
                    final VersionControlInformationEntity responseEntity = serviceFacade.setVersionControlInformation(rev, groupId,
                            mappingEntity.getVersionControlInformation(), mappingEntity.getVersionControlComponentMapping());
                    return generateOkResponse(responseEntity).build();
                });
    }

    private void unlockVersionControl(final URI requestUri, final String groupId) {
        final NodeResponse clusterResponse;
        try {
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(HttpMethod.DELETE, requestUri, new MultivaluedHashMap<>(), Collections.emptyMap()).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), HttpMethod.DELETE, requestUri, new MultivaluedHashMap<>(), Collections.emptyMap()).awaitMergedResponse();
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("After starting Version Control on Process Group with ID " + groupId + ", interrupted while waiting for deletion of Version Control Request. "
                    + "Users may be unable to Version Control other Process Groups until the request lock times out.", ie);
        }

        if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
            logger.error("After starting Version Control on Process Group with ID {}, failed to delete Version Control Request. "
                    + "Users may be unable to Version Control other Process Groups until the request lock times out. Response status code was {}", groupId, clusterResponse.getStatus());
        }
    }

    private String lockVersionControl(final URI originalUri, final String groupId) throws URISyntaxException {
        final URI createRequestUri = new URI(originalUri.getScheme(), originalUri.getUserInfo(), originalUri.getHost(),
                originalUri.getPort(), "/nifi-api/versions/active-requests", null, originalUri.getFragment());

        final NodeResponse clusterResponse;
        try {
            // create an active request entity to indicate the group id
            final CreateActiveRequestEntity activeRequestEntity = new CreateActiveRequestEntity();
            activeRequestEntity.setProcessGroupId(groupId);

            final Map<String, String> headers = new HashMap<>();
            headers.put("content-type", MediaType.APPLICATION_JSON);

            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(HttpMethod.POST, createRequestUri, activeRequestEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), HttpMethod.POST, createRequestUri, activeRequestEntity, headers).awaitMergedResponse();
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while updating Version Control Information for Process Group with ID " + groupId + ".", ie);
        }

        if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
            final String errorResponse = getResponseEntity(clusterResponse, String.class);
            throw new IllegalStateException(
                    "Failed to create a Version Control Request across all nodes in the cluster. Received response code " + clusterResponse.getStatus() + " with content: " + errorResponse);
        }

        final String requestId = getResponseEntity(clusterResponse, String.class);
        return requestId;
    }

    private void replicateVersionControlMapping(final VersionControlComponentMappingEntity mappingEntity, final StartVersionControlRequestEntity requestEntity,
                                                final URI requestUri, final String groupId) {

        final Map<String, String> headers = new HashMap<>();
        headers.put("content-type", MediaType.APPLICATION_JSON);

        final NodeResponse clusterResponse;
        try {
            if (getReplicationTarget() == ReplicationTarget.CLUSTER_NODES) {
                clusterResponse = getRequestReplicator().replicate(HttpMethod.PUT, requestUri, mappingEntity, headers).awaitMergedResponse();
            } else {
                clusterResponse = getRequestReplicator().forwardToCoordinator(
                        getClusterCoordinatorNode(), HttpMethod.PUT, requestUri, mappingEntity, headers).awaitMergedResponse();
            }
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();

            if (requestEntity.getVersionedFlow().getFlowId() == null) {
                // We had to create the flow for this snapshot. Since we failed to replicate the Version Control Info, remove the
                // flow from the Flow Registry (use best effort; if we can't remove it, just log and move on).
                final VersionControlInformationDTO vci = mappingEntity.getVersionControlInformation();
                try {
                    serviceFacade.deleteVersionedFlow(vci.getRegistryId(), vci.getBranch(), vci.getBucketId(), vci.getFlowId());
                } catch (final Exception e) {
                    logger.error("Created Versioned Flow with ID {} in bucket with ID {} but failed to replicate the Version Control Information to cluster. "
                            + "Attempted to delete the newly created (empty) flow from the Flow Registry but failed", vci.getFlowId(), vci.getBucketId(), e);
                }
            }

            throw new RuntimeException("Interrupted while updating Version Control Information for Process Group with ID " + groupId + ".", ie);
        }

        if (clusterResponse.getStatus() != Status.OK.getStatusCode()) {
            if (requestEntity.getVersionedFlow().getFlowId() == null) {
                // We had to create the flow for this snapshot. Since we failed to replicate the Version Control Info, remove the
                // flow from the Flow Registry (use best effort; if we can't remove it, just log and move on).
                final VersionControlInformationDTO vci = mappingEntity.getVersionControlInformation();
                try {
                    serviceFacade.deleteVersionedFlow(vci.getRegistryId(), vci.getBranch(), vci.getBucketId(), vci.getFlowId());
                } catch (final Exception e) {
                    logger.error("Created Versioned Flow with ID {} in bucket with ID {} but failed to replicate the Version Control Information to cluster. "
                            + "Attempted to delete the newly created (empty) flow from the Flow Registry but failed", vci.getFlowId(), vci.getBucketId(), e);
                }
            }

            final String message = "Failed to update Version Control Information for Process Group with ID " + groupId + ".";
            final Throwable cause = clusterResponse.getThrowable();
            if (cause == null) {
                throw new IllegalStateException(message);
            } else {
                throw new IllegalStateException(message, cause);
            }
        }
    }


    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @Operation(
            summary = "Stops version controlling the Process Group with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionControlInformationEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Stops version controlling the Process Group with the given ID. The Process Group will no longer track to any Versioned Flow. "
                    + NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Read - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Write - /process-groups/{uuid}"),
            }
    )
    public Response stopVersionControl(
            @Parameter(description = "The version is used to verify the client is working with the latest version of the flow.")
            @QueryParam(VERSION) final LongParameter version,
            @Parameter(description = "If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.")
            @QueryParam(CLIENT_ID)
            @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @Parameter(description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed.")
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED)
            @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(description = "The process group id.")
            @PathParam("id") final String groupId
    ) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), groupId);
        return withWriteLock(
                serviceFacade,
                null,
                requestRevision,
                lookup -> {
                    final Authorizable processGroup = lookup.getProcessGroup(groupId).getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                    final VersionControlInformationEntity currentVersionControlInfo = serviceFacade.getVersionControlInformation(groupId);
                    if (currentVersionControlInfo == null) {
                        throw new IllegalStateException("Process Group with ID " + groupId + " is not currently under Version Control");
                    }
                },
                (revision, groupEntity) -> {
                    // disconnect from version control
                    final VersionControlInformationEntity entity = serviceFacade.deleteVersionControl(revision, groupId);

                    // generate the response
                    return generateOkResponse(entity).build();
                });
    }


    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("process-groups/{id}")
    @Operation(
            summary = "Update the version of a Process Group with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionControlInformationEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "For a Process Group that is already under Version Control, this will update the version of the flow to a different version. This endpoint expects "
                    + "that the given snapshot will not modify any Processor that is currently running or any Controller Service that is enabled. "
                    + NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Read - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Write - /process-groups/{uuid}")
            }
    )
    public Response updateFlowVersion(@Parameter(description = "The process group id.") @PathParam("id") final String groupId,
                                      @Parameter(description = "The controller service configuration details.", required = true) final VersionedFlowSnapshotEntity requestEntity) {

        if (requestEntity == null) {
            throw new IllegalArgumentException("Version control information must be specified.");
        }

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified.");
        }

        final RegisteredFlowSnapshot requestFlowSnapshot = requestEntity.getVersionedFlowSnapshot();
        if (requestFlowSnapshot == null) {
            throw new IllegalArgumentException("Versioned Flow Snapshot must be supplied.");
        }

        final RegisteredFlowSnapshotMetadata requestSnapshotMetadata = requestFlowSnapshot.getSnapshotMetadata();
        if (requestSnapshotMetadata == null) {
            throw new IllegalArgumentException("Snapshot Metadata must be supplied.");
        }
        if (requestSnapshotMetadata.getBucketIdentifier() == null) {
            throw new IllegalArgumentException("The Bucket ID must be supplied.");
        }
        if (requestSnapshotMetadata.getFlowIdentifier() == null) {
            throw new IllegalArgumentException("The Flow ID must be supplied.");
        }

        // Perform the request
        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        final Revision requestRevision = getRevision(requestEntity.getProcessGroupRevision(), groupId);
        return withWriteLock(
                serviceFacade,
                requestEntity,
                requestRevision,
                lookup -> {
                    final ProcessGroupAuthorizable groupAuthorizable = lookup.getProcessGroup(groupId);
                    final Authorizable processGroup = groupAuthorizable.getAuthorizable();
                    processGroup.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
                    processGroup.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> {
                    // We do not enforce that the Process Group is 'not dirty' because at this point,
                    // the client has explicitly indicated the dataflow that the Process Group should
                    // provide and provided the Revision to ensure that they have the most up-to-date
                    // view of the Process Group.
                    serviceFacade.verifyCanUpdate(groupId, requestFlowSnapshot, true, false);
                },
                (revision, entity) -> {
                    // prepare an entity similar to initial request to pass registry id to performUpdateFlow
                    final VersionControlInformationDTO versionControlInfoDto = new VersionControlInformationDTO();
                    versionControlInfoDto.setRegistryId(entity.getRegistryId());
                    final VersionControlInformationEntity versionControlInfo = new VersionControlInformationEntity();
                    versionControlInfo.setVersionControlInformation(versionControlInfoDto);

                    final ProcessGroupEntity updatedGroup =
                            performUpdateFlow(groupId, revision, versionControlInfo, entity.getVersionedFlowSnapshot(),
                                    getIdGenerationSeed().orElse(null), false,
                                    entity.getUpdateDescendantVersionedFlows());

                    final VersionControlInformationDTO updatedVci = updatedGroup.getComponent().getVersionControlInformation();

                    // response to replication request is a version control entity with revision and versioning info
                    final VersionControlInformationEntity responseEntity = new VersionControlInformationEntity();
                    responseEntity.setProcessGroupRevision(updatedGroup.getRevision());
                    responseEntity.setVersionControlInformation(updatedVci);

                    return generateOkResponse(responseEntity).build();
                });
    }


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("update-requests/{id}")
    @Operation(
            summary = "Returns the Update Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowUpdateRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Returns the Update Request with the given ID. Once an Update Request has been created by performing a POST to /versions/update-requests/process-groups/{id}, "
                    + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
                    + "current state of the request, and any failures. "
                    + NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can get it")
            }
    )
    public Response getUpdateRequest(@Parameter(description = "The ID of the Update Request") @PathParam("id") final String updateRequestId) {
        return retrieveFlowUpdateRequest("update-requests", updateRequestId);
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("revert-requests/{id}")
    @Operation(
            summary = "Returns the Revert Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowUpdateRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Returns the Revert Request with the given ID. Once a Revert Request has been created by performing a POST to /versions/revert-requests/process-groups/{id}, "
                    + "that request can subsequently be retrieved via this endpoint, and the request that is fetched will contain the updated state, such as percent complete, the "
                    + "current state of the request, and any failures. "
                    + NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can get it")
            }
    )
    public Response getRevertRequest(@Parameter(description = "The ID of the Revert Request") @PathParam("id") final String revertRequestId) {
        return retrieveFlowUpdateRequest("revert-requests", revertRequestId);
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("update-requests/{id}")
    @Operation(
            summary = "Deletes the Update Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowUpdateRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Deletes the Update Request with the given ID. After a request is created via a POST to /versions/update-requests/process-groups/{id}, it is expected "
                    + "that the client will properly clean up the request by DELETE'ing it, once the Update process has completed. If the request is deleted before the request "
                    + "completes, then the Update request will finish the step that it is currently performing and then will cancel any subsequent steps. "
                    + NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can remove it")
            }
    )
    public Response deleteUpdateRequest(
            @Parameter(
                    description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(description = "The ID of the Update Request") @PathParam("id") final String updateRequestId) {

        return deleteFlowUpdateRequest("update-requests", updateRequestId, disconnectedNodeAcknowledged.booleanValue());
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("revert-requests/{id}")
    @Operation(
            summary = "Deletes the Revert Request with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowUpdateRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "Deletes the Revert Request with the given ID. After a request is created via a POST to /versions/revert-requests/process-groups/{id}, it is expected "
                    + "that the client will properly clean up the request by DELETE'ing it, once the Revert process has completed. If the request is deleted before the request "
                    + "completes, then the Revert request will finish the step that it is currently performing and then will cancel any subsequent steps. "
                    + NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Only the user that submitted the request can remove it")
            }
    )
    public Response deleteRevertRequest(
            @Parameter(
                    description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(description = "The ID of the Revert Request") @PathParam("id") final String revertRequestId) {

        return deleteFlowUpdateRequest("revert-requests", revertRequestId, disconnectedNodeAcknowledged.booleanValue());
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("update-requests/process-groups/{id}")
    @Operation(
            summary = "Initiate the Update Request of a Process Group with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowUpdateRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "For a Process Group that is already under Version Control, this will initiate the action of changing "
                    + "from a specific version of the flow in the Flow Registry to a different version of the flow. This can be a lengthy "
                    + "process, as it will stop any Processors and disable any Controller Services necessary to perform the action and then restart them. As a result, "
                    + "the endpoint will immediately return a VersionedFlowUpdateRequestEntity, and the process of updating the flow will occur "
                    + "asynchronously in the background. The client may then periodically poll the status of the request by issuing a GET request to "
                    + "/versions/update-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to "
                    + "/versions/update-requests/{requestId}. " + NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Read - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Write - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Read - /{component-type}/{uuid} - For all encapsulated components"),
                    @SecurityRequirement(name = "Write - /{component-type}/{uuid} - For all encapsulated components"),
                    @SecurityRequirement(name = "Write - if the template contains any restricted components - /restricted-components"),
                    @SecurityRequirement(name = "Read - /parameter-contexts/{uuid} - For any Parameter Context that is referenced by a Property that is changed, added, or removed")
            }
    )
    public Response initiateVersionControlUpdate(
            @Parameter(description = "The process group id.") @PathParam("id") final String groupId,
            @Parameter(description = "The controller service configuration details.", required = true) final VersionControlInformationEntity requestEntity) {

        if (requestEntity == null) {
            throw new IllegalArgumentException("Version control information must be specified.");
        }

        // validate version control info
        final VersionControlInformationDTO requestVersionControlInfoDto = requestEntity.getVersionControlInformation();
        if (requestVersionControlInfoDto == null) {
            throw new IllegalArgumentException("Version Control Information must be supplied.");
        }
        if (requestVersionControlInfoDto.getGroupId() == null) {
            throw new IllegalArgumentException("The Process Group ID must be supplied.");
        }
        if (!requestVersionControlInfoDto.getGroupId().equals(groupId)) {
            throw new IllegalArgumentException("The Process Group ID in the request body does not match the Process Group ID of the requested resource.");
        }
        if (requestVersionControlInfoDto.getBucketId() == null) {
            throw new IllegalArgumentException("The Bucket ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getFlowId() == null) {
            throw new IllegalArgumentException("The Flow ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getRegistryId() == null) {
            throw new IllegalArgumentException("The Registry ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getVersion() == null) {
            throw new IllegalArgumentException("The Version of the flow must be supplied.");
        }

        // supplier retrieves Versioned Flow Snapshot from the Flow Registry
        return initiateFlowUpdate(groupId, requestEntity, false, "update-requests",
                "/nifi-api/versions/process-groups/" + groupId,
                () -> serviceFacade.getVersionedFlowSnapshot(requestVersionControlInfoDto, true)
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("revert-requests/process-groups/{id}")
    @Operation(
            summary = "Initiate the Revert Request of a Process Group with the given ID",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowUpdateRequestEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            description = "For a Process Group that is already under Version Control, this will initiate the action of reverting "
                    + "any local changes that have been made to the Process Group since it was last synchronized with the Flow Registry. This will result in the "
                    + "flow matching the Versioned Flow that exists in the Flow Registry. This can be a lengthy "
                    + "process, as it will stop any Processors and disable any Controller Services necessary to perform the action and then restart them. As a result, "
                    + "the endpoint will immediately return a VersionedFlowUpdateRequestEntity, and the process of updating the flow will occur "
                    + "asynchronously in the background. The client may then periodically poll the status of the request by issuing a GET request to "
                    + "/versions/revert-requests/{requestId}. Once the request is completed, the client is expected to issue a DELETE request to "
                    + "/versions/revert-requests/{requestId}. " + NON_GUARANTEED_ENDPOINT,
            security = {
                    @SecurityRequirement(name = "Read - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Write - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Read - /{component-type}/{uuid} - For all encapsulated components"),
                    @SecurityRequirement(name = "Write - /{component-type}/{uuid} - For all encapsulated components"),
                    @SecurityRequirement(name = "Write - if the template contains any restricted components - /restricted-components"),
                    @SecurityRequirement(name = "Read - /parameter-contexts/{uuid} - For any Parameter Context that is referenced by a Property that is changed, added, or removed")
            }
    )
    public Response initiateRevertFlowVersion(@Parameter(description = "The process group id.") @PathParam("id") final String groupId,
                                              @Parameter(description = "The Version Control Information to revert to.", required = true) final VersionControlInformationEntity requestEntity) {

        if (requestEntity == null) {
            throw new IllegalArgumentException("Version control information must be specified.");
        }

        // Verify the request
        final RevisionDTO revisionDto = requestEntity.getProcessGroupRevision();
        if (revisionDto == null) {
            throw new IllegalArgumentException("Process Group Revision must be specified");
        }

        final VersionControlInformationDTO requestVersionControlInfoDto = requestEntity.getVersionControlInformation();
        if (requestVersionControlInfoDto == null) {
            throw new IllegalArgumentException("Version Control Information must be supplied.");
        }
        if (requestVersionControlInfoDto.getGroupId() == null) {
            throw new IllegalArgumentException("The Process Group ID must be supplied.");
        }
        if (!requestVersionControlInfoDto.getGroupId().equals(groupId)) {
            throw new IllegalArgumentException("The Process Group ID in the request body does not match the Process Group ID of the requested resource.");
        }
        if (requestVersionControlInfoDto.getBucketId() == null) {
            throw new IllegalArgumentException("The Bucket ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getFlowId() == null) {
            throw new IllegalArgumentException("The Flow ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getRegistryId() == null) {
            throw new IllegalArgumentException("The Registry ID must be supplied.");
        }
        if (requestVersionControlInfoDto.getVersion() == null) {
            throw new IllegalArgumentException("The Version of the flow must be supplied.");
        }

        if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestEntity.isDisconnectedNodeAcknowledged());
        }

        // We will perform the updating of the Versioned Flow in a background thread because it can be a long-running process.
        // In order to do this, we will need some parameters that are only available as Thread-Local variables to the current
        // thread, so we will gather the values for these parameters up front.
        final boolean replicateRequest = isReplicateRequest();
        final ComponentLifecycle componentLifecycle = replicateRequest ? clusterComponentLifecycle : localComponentLifecycle;
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // Step 0: Get the Versioned Flow Snapshot from the Flow Registry
        final FlowSnapshotContainer flowSnapshotContainer = serviceFacade.getVersionedFlowSnapshot(requestEntity.getVersionControlInformation(), true);
        final RegisteredFlowSnapshot flowSnapshot = flowSnapshotContainer.getFlowSnapshot();

        // The flow in the registry may not contain the same versions of components that we have in our flow. As a result, we need to update
        // the flow snapshot to contain compatible bundles.
        serviceFacade.discoverCompatibleBundles(flowSnapshot.getFlowContents());

        // If there are any Controller Services referenced that are inherited from the parent group, resolve those to point to the appropriate Controller Service, if we are able to.
        final Set<String> unresolvedControllerServices = serviceFacade.resolveInheritedControllerServices(flowSnapshotContainer, groupId, NiFiUserUtils.getNiFiUser());

        // If there are any Parameter Providers referenced by Parameter Contexts, resolve these to point to the appropriate Parameter Provider, if we are able to.
        final Set<String> unresolvedParameterProviders = serviceFacade.resolveParameterProviders(flowSnapshot, NiFiUserUtils.getNiFiUser());

        // Step 1: Determine which components will be affected by updating the version
        final Set<AffectedComponentEntity> affectedComponents = serviceFacade.getComponentsAffectedByFlowUpdate(groupId, flowSnapshot);

        // build a request wrapper
        final InitiateUpdateFlowRequestWrapper requestWrapper =
                new InitiateUpdateFlowRequestWrapper(requestEntity, componentLifecycle, "revert-requests", getAbsolutePath(),
                        "/nifi-api/versions/process-groups/" + groupId, affectedComponents, replicateRequest, flowSnapshot);

        final Revision requestRevision = getRevision(requestEntity.getProcessGroupRevision(), groupId);
        return withWriteLock(
                serviceFacade,
                requestWrapper,
                requestRevision,
                lookup -> authorizeFlowUpdate(lookup, user, groupId, flowSnapshot, unresolvedControllerServices, unresolvedParameterProviders),
                () -> {
                    // Step 3: Verify that all components in the snapshot exist on all nodes
                    // Step 4: Verify that Process Group is already under version control. If not, must start Version Control instead of updating flow
                    serviceFacade.verifyCanRevertLocalModifications(groupId, flowSnapshot);
                },
                (revision, wrapper) -> {
                    final VersionControlInformationEntity versionControlInformationEntity = wrapper.getRequestEntity();
                    final VersionControlInformationDTO versionControlInformationDTO = versionControlInformationEntity.getVersionControlInformation();

                    // Ensure that the information passed in is correct
                    final VersionControlInformationEntity currentVersionEntity = serviceFacade.getVersionControlInformation(groupId);
                    if (currentVersionEntity == null) {
                        throw new IllegalStateException("Process Group cannot be reverted to the previous version of the flow because Process Group is not under Version Control.");
                    }

                    final VersionControlInformationDTO currentVersion = currentVersionEntity.getVersionControlInformation();
                    if (!currentVersion.getBucketId().equals(versionControlInformationDTO.getBucketId())) {
                        throw new IllegalArgumentException("The Version Control Information provided does not match the flow that the Process Group is currently synchronized with.");
                    }
                    if (!currentVersion.getFlowId().equals(versionControlInformationDTO.getFlowId())) {
                        throw new IllegalArgumentException("The Version Control Information provided does not match the flow that the Process Group is currently synchronized with.");
                    }
                    if (!currentVersion.getRegistryId().equals(versionControlInformationDTO.getRegistryId())) {
                        throw new IllegalArgumentException("The Version Control Information provided does not match the flow that the Process Group is currently synchronized with.");
                    }
                    if (!currentVersion.getVersion().equals(versionControlInformationDTO.getVersion())) {
                        throw new IllegalArgumentException("The Version Control Information provided does not match the flow that the Process Group is currently synchronized with.");
                    }

                    return submitFlowUpdateRequest(user, groupId, revision, wrapper, true);
                });
    }

    /**
     * Perform actual flow update of the specified flow. This is used for the initial flow update and replication updates.
     */
    @Override
    protected ProcessGroupEntity performUpdateFlow(final String groupId, final Revision revision,
                                                   final VersionControlInformationEntity requestEntity,
                                                   final RegisteredFlowSnapshot flowSnapshot, final String idGenerationSeed,
                                                   final boolean verifyNotModified, final boolean updateDescendantVersionedFlows) {
        logger.info("Updating Process Group with ID {} to version {} of the Versioned Flow", groupId, flowSnapshot.getSnapshotMetadata().getVersion());

        // Step 10-11. Update Process Group to the new flow
        final VersionControlInformationDTO requestVci = requestEntity.getVersionControlInformation();

        final FlowRegistryBucket bucket = flowSnapshot.getBucket();
        final RegisteredFlow flow = flowSnapshot.getFlow();

        final RegisteredFlowSnapshotMetadata metadata = flowSnapshot.getSnapshotMetadata();
        final VersionControlInformationDTO versionControlInfo = new VersionControlInformationDTO();
        versionControlInfo.setBranch(metadata.getBranch());
        versionControlInfo.setBucketId(metadata.getBucketIdentifier());
        versionControlInfo.setBucketName(bucket.getName());
        versionControlInfo.setFlowDescription(flow.getDescription());
        versionControlInfo.setFlowId(flow.getIdentifier());
        versionControlInfo.setFlowName(flow.getName());
        versionControlInfo.setGroupId(groupId);
        versionControlInfo.setRegistryId(requestVci.getRegistryId());
        versionControlInfo.setRegistryName(serviceFacade.getFlowRegistryName(requestVci.getRegistryId()));
        versionControlInfo.setStorageLocation(requestVci.getStorageLocation());
        versionControlInfo.setVersion(metadata.getVersion());
        versionControlInfo.setState(flowSnapshot.isLatest() ? VersionedFlowState.UP_TO_DATE.name() : VersionedFlowState.STALE.name());

        return serviceFacade.updateProcessGroupContents(revision, groupId, versionControlInfo, flowSnapshot, idGenerationSeed,
                verifyNotModified, false, updateDescendantVersionedFlows);
    }

    /**
     * Create the entity that is used for update flow replication. Versioned flow update replication creates a new entity type containing
     * the actual versioned flow snapshot and a registry identifier.
     */
    @Override
    protected Entity createReplicateUpdateFlowEntity(final Revision revision, final VersionControlInformationEntity requestEntity,
                                                     final RegisteredFlowSnapshot flowSnapshot) {
        final VersionedFlowSnapshotEntity snapshotEntity = new VersionedFlowSnapshotEntity();
        snapshotEntity.setProcessGroupRevision(dtoFactory.createRevisionDTO(revision));
        snapshotEntity.setRegistryId(requestEntity.getVersionControlInformation().getRegistryId());
        snapshotEntity.setVersionedFlow(flowSnapshot);
        snapshotEntity.setUpdateDescendantVersionedFlows(true);
        return snapshotEntity;
    }

    /**
     * Create the entity that captures the status and result of an update or revert request
     *
     * @return a new instance of a VersionedFlowUpdateRequestEntity
     */
    @Override
    protected VersionedFlowUpdateRequestEntity createUpdateRequestEntity() {
        return new VersionedFlowUpdateRequestEntity();
    }

    /**
     * Finalize a completed update request for an existing update or revert request. This is used when retrieving and deleting an update request.
     *
     * @param requestEntity the request entity to finalize
     */
    @Override
    protected void finalizeCompletedUpdateRequest(final VersionedFlowUpdateRequestEntity requestEntity) {
        final VersionedFlowUpdateRequestDTO updateRequestDto = requestEntity.getRequest();
        if (updateRequestDto.isComplete()) {
            final VersionControlInformationEntity vciEntity =
                    serviceFacade.getVersionControlInformation(updateRequestDto.getProcessGroupId());
            updateRequestDto.setVersionControlInformation(vciEntity == null ? null : vciEntity.getVersionControlInformation());
        }
    }

    private static class ActiveRequest {
        private static final long MAX_REQUEST_LOCK_NANOS = TimeUnit.MINUTES.toNanos(1L);

        private final String requestId;
        private final NiFiUser user;
        private final String processGroupId;
        private final long creationNanos = System.nanoTime();
        private final long expirationTime = creationNanos + MAX_REQUEST_LOCK_NANOS;

        private boolean updatePerformed = false;

        private ActiveRequest(final String requestId, final NiFiUser user, final String processGroupId) {
            this.requestId = requestId;
            this.user = user;
            this.processGroupId = processGroupId;
        }

        public boolean isExpired() {
            return System.nanoTime() > expirationTime;
        }

        public String getRequestId() {
            return requestId;
        }

        public NiFiUser getUser() {
            return user;
        }

        public String getProcessGroupId() {
            return processGroupId;
        }

        public void updatePerformed() {
            updatePerformed = true;
        }

        public boolean isUpdatePerformed() {
            return updatePerformed;
        }
    }
}
