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
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizeParameterReference;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.ComponentAuthorizable;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.SnippetAuthorizable;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.SnippetEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.net.URI;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * RESTful endpoint for querying dataflow snippets.
 */
@Controller
@Path("/snippets")
@Tag(name = "Snippets")
public class SnippetResource extends ApplicationResource {
    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populate the uri's for the specified snippet.
     *
     * @param entity processors
     * @return dtos
     */
    private SnippetEntity populateRemainingSnippetEntityContent(SnippetEntity entity) {
        if (entity.getSnippet() != null) {
            populateRemainingSnippetContent(entity.getSnippet());
        }
        return entity;
    }

    /**
     * Populates the uri for the specified snippet.
     */
    private SnippetDTO populateRemainingSnippetContent(SnippetDTO snippet) {
        String snippetGroupId = snippet.getParentGroupId();

        // populate the snippet href
        snippet.setUri(generateResourceUri("process-groups", snippetGroupId, "snippets", snippet.getId()));

        return snippet;
    }

    // --------
    // snippets
    // --------

    /**
     * Authorizes the specified snippet request with the specified request action. This method is used when creating a snippet. Because we do not know what
     * the snippet will be used for, we just ensure the user has permissions to each selected component. Some actions may require additional permissions
     * (including referenced services) but those will be enforced when the snippet is used.
     *
     * @param authorizer authorizer
     * @param lookup lookup
     * @param action action
     */
    private void authorizeSnippetRequest(final SnippetDTO snippetRequest, final Authorizer authorizer, final AuthorizableLookup lookup, final RequestAction action) {
        final Consumer<Authorizable> authorize = authorizable -> authorizable.authorize(authorizer, action, NiFiUserUtils.getNiFiUser());

        // note - we are not authorizing controller services as they are not considered when using this snippet
        snippetRequest.getProcessGroups().keySet().stream().map(id -> lookup.getProcessGroup(id)).forEach(processGroupAuthorizable -> {
            // we are not checking referenced services since we do not know how this snippet will be used. these checks should be performed
            // in a subsequent action with this snippet
            authorizeProcessGroup(processGroupAuthorizable, authorizer, lookup, action, false, false, false, false, false);
        });
        snippetRequest.getRemoteProcessGroups().keySet().stream().map(id -> lookup.getRemoteProcessGroup(id)).forEach(authorize);
        snippetRequest.getProcessors().keySet().stream().map(id -> lookup.getProcessor(id).getAuthorizable()).forEach(authorize);
        snippetRequest.getInputPorts().keySet().stream().map(id -> lookup.getInputPort(id)).forEach(authorize);
        snippetRequest.getOutputPorts().keySet().stream().map(id -> lookup.getOutputPort(id)).forEach(authorize);
        snippetRequest.getConnections().keySet().stream().map(id -> lookup.getConnection(id).getAuthorizable()).forEach(authorize);
        snippetRequest.getFunnels().keySet().stream().map(id -> lookup.getFunnel(id)).forEach(authorize);
        snippetRequest.getLabels().keySet().stream().map(id -> lookup.getLabel(id)).forEach(authorize);
    }

    /**
     * Creates a snippet based off the specified configuration.
     *
     * @param requestSnippetEntity A snippetEntity
     * @return A snippetEntity
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Creates a snippet. The snippet will be automatically discarded if not used in a subsequent request after 1 minute.",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = SnippetEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read or Write - /{component-type}/{uuid} - For every component (all Read or all Write) in the Snippet and their descendant components")
            }
    )
    public Response createSnippet(
            @Parameter(
                    description = "The snippet configuration details.",
                    required = true
            ) final SnippetEntity requestSnippetEntity) {

        if (requestSnippetEntity == null || requestSnippetEntity.getSnippet() == null) {
            throw new IllegalArgumentException("Snippet details must be specified.");
        }

        if (requestSnippetEntity.getSnippet().getId() != null) {
            throw new IllegalArgumentException("Snippet ID cannot be specified.");
        }

        if (requestSnippetEntity.getSnippet().getParentGroupId() == null) {
            throw new IllegalArgumentException("The parent Process Group of the snippet must be specified.");
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.POST, requestSnippetEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestSnippetEntity.isDisconnectedNodeAcknowledged());
        }

        return withWriteLock(
                serviceFacade,
                requestSnippetEntity,
                lookup -> {
                    final SnippetDTO snippetRequest = requestSnippetEntity.getSnippet();

                    // the snippet being created may be used later for batch component modifications or
                    // copy/paste. During those subsequent actions, the snippet
                    // will again be authorized accordingly (read or write). at this point we do not
                    // know what the snippet will be used for so we need to attempt to authorize as
                    // read OR write

                    try {
                        authorizeSnippetRequest(snippetRequest, authorizer, lookup, RequestAction.READ);
                    } catch (final AccessDeniedException e) {
                        authorizeSnippetRequest(snippetRequest, authorizer, lookup, RequestAction.WRITE);
                    }
                },
                null,
                (snippetEntity) -> {
                    // set the processor id as appropriate
                    snippetEntity.getSnippet().setId(generateUuid());

                    // create the snippet
                    final SnippetEntity entity = serviceFacade.createSnippet(snippetEntity.getSnippet());
                    populateRemainingSnippetEntityContent(entity);

                    // build the response
                    return generateCreatedResponse(URI.create(entity.getSnippet().getUri()), entity).build();
                }
        );
    }

    /**
     * Move's the components in this Snippet into a new Process Group.
     *
     * @param snippetId The id of the snippet.
     * @param requestSnippetEntity A snippetEntity
     * @return A snippetEntity
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Move's the components in this Snippet into a new Process Group and discards the snippet",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = SnippetEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write Process Group - /process-groups/{uuid}"),
                    @SecurityRequirement(name = "Write - /{component-type}/{uuid} - For each component in the Snippet and their descendant components")
            }
    )
    public Response updateSnippet(
            @Parameter(
                    description = "The snippet id.",
                    required = true
            )
            @PathParam("id") String snippetId,
            @Parameter(
                    description = "The snippet configuration details.",
                    required = true
            ) final SnippetEntity requestSnippetEntity) {

        if (requestSnippetEntity == null || requestSnippetEntity.getSnippet() == null) {
            throw new IllegalArgumentException("Snippet details must be specified.");
        }

        // ensure the ids are the same
        final SnippetDTO requestSnippetDTO = requestSnippetEntity.getSnippet();
        if (!snippetId.equals(requestSnippetDTO.getId())) {
            throw new IllegalArgumentException(String.format("The snippet id (%s) in the request body does not equal the "
                    + "snippet id of the requested resource (%s).", requestSnippetDTO.getId(), snippetId));
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestSnippetEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestSnippetEntity.isDisconnectedNodeAcknowledged());
        }

        // get the revision from this snippet
        final Set<Revision> requestRevisions = serviceFacade.getRevisionsFromSnippet(snippetId);
        return withWriteLock(
                serviceFacade,
                requestSnippetEntity,
                requestRevisions,
                lookup -> {
                    final NiFiUser user = NiFiUserUtils.getNiFiUser();

                    // ensure write access to the target process group
                    if (requestSnippetDTO.getParentGroupId() != null) {
                        lookup.getProcessGroup(requestSnippetDTO.getParentGroupId()).getAuthorizable().authorize(authorizer, RequestAction.WRITE, user);
                    }

                    // ensure write permission to every component in the snippet excluding referenced services
                    final SnippetAuthorizable snippet = lookup.getSnippet(snippetId);

                    // Note: we are explicitly not authorizing parameter references here because they are being authorized below
                    authorizeSnippet(snippet, authorizer, lookup, RequestAction.WRITE, false, false, false, false);

                    final ProcessGroup destinationGroup = lookup.getProcessGroup(requestSnippetDTO.getParentGroupId()).getProcessGroup();

                    for (final ComponentAuthorizable componentAuthorizable : snippet.getSelectedProcessors()) {
                        AuthorizeParameterReference.authorizeParameterReferences(destinationGroup, componentAuthorizable, authorizer, user);
                    }
                },
                () -> serviceFacade.verifyUpdateSnippet(requestSnippetDTO, requestRevisions.stream().map(Revision::getComponentId).collect(Collectors.toSet())),
                (revisions, snippetEntity) -> {
                    // update the snippet
                    final SnippetEntity entity = serviceFacade.updateSnippet(revisions, snippetEntity.getSnippet());
                    populateRemainingSnippetEntityContent(entity);
                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified snippet.
     *
     * @param snippetId The id of the snippet to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Deletes the components in a snippet and discards the snippet",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = SnippetEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /{component-type}/{uuid} - For each component in the Snippet and their descendant components"),
                    @SecurityRequirement(name = "Write - Parent Process Group - /process-groups/{uuid}"),
            }
    )
    public Response deleteSnippet(
            @Parameter(
                    description = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
            )
            @QueryParam(DISCONNECTED_NODE_ACKNOWLEDGED) @DefaultValue("false") final Boolean disconnectedNodeAcknowledged,
            @Parameter(
                    description = "The snippet id.",
                    required = true
            )
            @PathParam("id") final String snippetId) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final ComponentEntity requestEntity = new ComponentEntity();
        requestEntity.setId(snippetId);

        // get the revision from this snippet
        final Set<Revision> requestRevisions = serviceFacade.getRevisionsFromSnippet(snippetId);
        return withWriteLock(
                serviceFacade,
                requestEntity,
                requestRevisions,
                lookup -> {
                    // ensure write permission to every component in the snippet excluding referenced services
                    final SnippetAuthorizable snippet = lookup.getSnippet(snippetId);
                    authorizeSnippet(snippet, authorizer, lookup, RequestAction.WRITE, true, false, false, false);

                    // ensure write permission to the parent process group
                    snippet.getParentProcessGroup().getAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                () -> serviceFacade.verifyDeleteSnippet(snippetId, requestRevisions.stream().map(rev -> rev.getComponentId()).collect(Collectors.toSet())),
                (revisions, entity) -> {
                    // delete the specified snippet
                    final SnippetEntity snippetEntity = serviceFacade.deleteSnippet(revisions, entity.getId());
                    return generateOkResponse(snippetEntity).build();
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
