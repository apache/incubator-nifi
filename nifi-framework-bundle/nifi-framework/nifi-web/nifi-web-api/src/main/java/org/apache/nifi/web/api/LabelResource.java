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
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.Revision;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.util.Set;

/**
 * RESTful endpoint for managing a Label.
 */
@Controller
@Path("/labels")
@Tag(name = "Labels")
public class LabelResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;
    private Authorizer authorizer;

    /**
     * Populates the uri for the specified labels.
     *
     * @param labelEntities labels
     * @return entites
     */
    public Set<LabelEntity> populateRemainingLabelEntitiesContent(Set<LabelEntity> labelEntities) {
        for (LabelEntity labelEntity : labelEntities) {
            populateRemainingLabelEntityContent(labelEntity);
        }
        return labelEntities;
    }

    /**
     * Populates the uri for the specified labels.
     *
     * @param labelEntity label
     * @return entities
     */
    public LabelEntity populateRemainingLabelEntityContent(LabelEntity labelEntity) {
        labelEntity.setUri(generateResourceUri("labels", labelEntity.getId()));
        return labelEntity;
    }

    /**
     * Retrieves the specified label.
     *
     * @param id The id of the label to retrieve
     * @return A labelEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Gets a label",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = LabelEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Read - /labels/{uuid}")
            }
    )
    public Response getLabel(
            @Parameter(
                    description = "The label id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.GET);
        }

        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable label = lookup.getLabel(id);
            label.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        // get the label
        final LabelEntity entity = serviceFacade.getLabel(id);
        populateRemainingLabelEntityContent(entity);

        return generateOkResponse(entity).build();
    }

    /**
     * Updates the specified label.
     *
     * @param id The id of the label to update.
     * @param requestLabelEntity A labelEntity.
     * @return A labelEntity.
     */
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Updates a label",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = LabelEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /labels/{uuid}")
            }
    )
    public Response updateLabel(
            @Parameter(
                    description = "The label id.",
                    required = true
            )
            @PathParam("id") final String id,
            @Parameter(
                    description = "The label configuration details.",
                    required = true
            ) final LabelEntity requestLabelEntity) {

        if (requestLabelEntity == null || requestLabelEntity.getComponent() == null) {
            throw new IllegalArgumentException("Label details must be specified.");
        }

        if (requestLabelEntity.getRevision() == null) {
            throw new IllegalArgumentException("Revision must be specified.");
        }

        // ensure the ids are the same
        final LabelDTO requestLabelDTO = requestLabelEntity.getComponent();
        if (!id.equals(requestLabelDTO.getId())) {
            throw new IllegalArgumentException(String.format("The label id (%s) in the request body does not equal the "
                    + "label id of the requested resource (%s).", requestLabelDTO.getId(), id));
        }

        final PositionDTO proposedPosition = requestLabelDTO.getPosition();
        if (proposedPosition != null) {
            if (proposedPosition.getX() == null || proposedPosition.getY() == null) {
                throw new IllegalArgumentException("The x and y coordinate of the proposed position must be specified.");
            }
        }

        if (isReplicateRequest()) {
            return replicate(HttpMethod.PUT, requestLabelEntity);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(requestLabelEntity.isDisconnectedNodeAcknowledged());
        }

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = getRevision(requestLabelEntity, id);
        return withWriteLock(
                serviceFacade,
                requestLabelEntity,
                requestRevision,
                lookup -> {
                    Authorizable authorizable = lookup.getLabel(id);
                    authorizable.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, labelEntity) -> {
                    final LabelDTO labelDTO = labelEntity.getComponent();

                    // update the label
                    final LabelEntity entity = serviceFacade.updateLabel(revision, labelDTO);
                    populateRemainingLabelEntityContent(entity);

                    return generateOkResponse(entity).build();
                }
        );
    }

    /**
     * Removes the specified label.
     *
     * @param version The revision is used to verify the client is working with the latest version of the flow.
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param id The id of the label to remove.
     * @return A entity containing the client id and an updated revision.
     */
    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    @Operation(
            summary = "Deletes a label",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = LabelEntity.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "401", description = "Client could not be authenticated."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "404", description = "The specified resource could not be found."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it.")
            },
            security = {
                    @SecurityRequirement(name = "Write - /labels/{uuid}"),
                    @SecurityRequirement(name = "Write - Parent Process Group - /process-groups/{uuid}")
            }
    )
    public Response removeLabel(
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
                    description = "The label id.",
                    required = true
            )
            @PathParam("id") final String id) {

        if (isReplicateRequest()) {
            return replicate(HttpMethod.DELETE);
        } else if (isDisconnectedFromCluster()) {
            verifyDisconnectedNodeModification(disconnectedNodeAcknowledged);
        }

        final LabelEntity requestLabelEntity = new LabelEntity();
        requestLabelEntity.setId(id);

        // handle expects request (usually from the cluster manager)
        final Revision requestRevision = new Revision(version == null ? null : version.getLong(), clientId.getClientId(), id);
        return withWriteLock(
                serviceFacade,
                requestLabelEntity,
                requestRevision,
                lookup -> {
                    final Authorizable label = lookup.getLabel(id);

                    // ensure write permission to the label
                    label.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());

                    // ensure write permission to the parent process group
                    label.getParentAuthorizable().authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
                },
                null,
                (revision, labelEntity) -> {
                    // delete the specified label
                    final LabelEntity entity = serviceFacade.deleteLabel(revision, labelEntity.getId());
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
