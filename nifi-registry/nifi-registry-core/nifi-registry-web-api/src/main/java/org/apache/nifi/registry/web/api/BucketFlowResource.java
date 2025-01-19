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
package org.apache.nifi.registry.web.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.diff.VersionedFlowDifference;
import org.apache.nifi.registry.event.EventFactory;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.revision.web.ClientIdParameter;
import org.apache.nifi.registry.revision.web.LongParameter;
import org.apache.nifi.registry.security.authorization.user.NiFiUserUtils;
import org.apache.nifi.registry.web.service.ExportedVersionedFlowSnapshot;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;
import java.util.SortedSet;

@Component
@Path("/buckets/{bucketId}/flows")
@Tag(name = "BucketFlows")
public class BucketFlowResource extends ApplicationResource {

    @Autowired
    public BucketFlowResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Create flow",
            description = "Creates a flow in the given bucket. The flow id is created by the server and populated in the returned entity.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlow.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response createFlow(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @Parameter(description = "The details of the flow to create.", required = true) final VersionedFlow flow) {

        verifyPathParamsMatchBody(bucketId, flow);

        final VersionedFlow createdFlow = serviceFacade.createFlow(bucketId, flow);
        publish(EventFactory.flowCreated(createdFlow));
        return Response.status(Response.Status.OK).entity(createdFlow).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bucket flows",
            description = "Retrieves all flows in the given bucket.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = VersionedFlow.class)))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response getFlows(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId) {

        final List<VersionedFlow> flows = serviceFacade.getFlows(bucketId);
        return Response.status(Response.Status.OK).entity(flows).build();
    }

    @GET
    @Path("{flowId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bucket flow",
            description = "Retrieves the flow with the given id in the given bucket.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlow.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response getFlow(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId) {

        final VersionedFlow flow = serviceFacade.getFlow(bucketId, flowId);
        return Response.status(Response.Status.OK).entity(flow).build();
    }

    @PUT
    @Path("{flowId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Update bucket flow",
            description = "Updates the flow with the given id in the given bucket.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlow.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response updateFlow(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId,
            @Parameter(description = "The updated flow", required = true) final VersionedFlow flow) {

        verifyPathParamsMatchBody(bucketId, flowId, flow);

        // bucketId and flowId fields are optional in the body parameter, but required before calling the service layer
        setBucketItemMetadataIfMissing(bucketId, flowId, flow);

        final VersionedFlow updatedFlow = serviceFacade.updateFlow(flow);
        publish(EventFactory.flowUpdated(updatedFlow));
        return Response.status(Response.Status.OK).entity(updatedFlow).build();
    }

    @DELETE
    @Path("{flowId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Delete bucket flow",
            description = "Deletes a flow, including all saved versions of that flow.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlow.class))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "delete"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response deleteFlow(
            @Parameter(description = "The version is used to verify the client is working with the latest version of the entity.", required = true)
            @QueryParam(VERSION) final LongParameter version,
            @Parameter(description = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.")
            @QueryParam(CLIENT_ID)
            @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId) {

        final RevisionInfo revisionInfo = getRevisionInfo(version, clientId);
        final VersionedFlow deletedFlow = serviceFacade.deleteFlow(bucketId, flowId, revisionInfo);
        publish(EventFactory.flowDeleted(deletedFlow));

        return Response.status(Response.Status.OK).entity(deletedFlow).build();
    }

    @POST
    @Path("{flowId}/versions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Create flow version",
            description = "Creates the next version of a flow. The version number of the object being created must be the " +
                    "next available version integer. Flow versions are immutable after they are created.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowSnapshot.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response createFlowVersion(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId,
            @Parameter(description = "The new versioned flow snapshot.", required = true) final VersionedFlowSnapshot snapshot,
            @Parameter(description = "Whether source properties like author should be kept")
            @QueryParam("preserveSourceProperties") final boolean preserveSourceProperties) {

        verifyPathParamsMatchBody(bucketId, flowId, snapshot);

        // bucketId and flowId fields are optional in the body parameter, but required before calling the service layer
        setSnaphotMetadataIfMissing(bucketId, flowId, snapshot);

        if (preserveSourceProperties) {
            if (StringUtils.isBlank(snapshot.getSnapshotMetadata().getAuthor())) {
                throw new BadRequestException("Author must not be blank");
            }
            if (snapshot.getSnapshotMetadata().getTimestamp() <= 0) {
                throw new BadRequestException("Invalid timestamp");
            }
        } else {
            final String userIdentity = NiFiUserUtils.getNiFiUserIdentity();
            snapshot.getSnapshotMetadata().setAuthor(userIdentity);
        }

        final VersionedFlowSnapshot createdSnapshot = serviceFacade.createFlowSnapshot(snapshot);
        publish(EventFactory.flowVersionCreated(createdSnapshot));

        return Response.status(Response.Status.OK).entity(createdSnapshot).build();
    }

    @POST
    @Path("{flowId}/versions/import")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Import flow version",
            description = "Import the next version of a flow. The version number of the object being created will be the " +
                    "next available version integer. Flow versions are immutable after they are created.",
            responses = {
                    @ApiResponse(responseCode = "201", description = HttpStatusMessages.MESSAGE_201, content = @Content(schema = @Schema(implementation = VersionedFlowSnapshot.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response importVersionedFlow(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId,
            @Parameter(description = "file") final VersionedFlowSnapshot versionedFlowSnapshot,
            @HeaderParam("Comments") final String comments) {

        final VersionedFlowSnapshot createdSnapshot = serviceFacade.importVersionedFlowSnapshot(versionedFlowSnapshot, bucketId, flowId, comments);
        publish(EventFactory.flowVersionCreated(createdSnapshot));
        String locationUri = createdSnapshot.getSnapshotMetadata().getLink().getUri().getPath();
        return generateCreatedResponse(URI.create(locationUri), createdSnapshot).build();
    }

    @GET
    @Path("{flowId}/versions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bucket flow versions",
            description = "Gets summary information for all versions of a flow. Versions are ordered newest->oldest.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = VersionedFlowSnapshotMetadata.class)))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response getFlowVersions(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId) {

        final SortedSet<VersionedFlowSnapshotMetadata> snapshots = serviceFacade.getFlowSnapshots(bucketId, flowId);
        return Response.status(Response.Status.OK).entity(snapshots).build();
    }

    @GET
    @Path("{flowId}/versions/latest")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get latest bucket flow version content",
            description = "Gets the latest version of a flow, including the metadata and content of the flow.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowSnapshot.class))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response getLatestFlowVersion(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId) {

        final VersionedFlowSnapshot lastSnapshot = serviceFacade.getLatestFlowSnapshot(bucketId, flowId);
        return Response.status(Response.Status.OK).entity(lastSnapshot).build();
    }

    @GET
    @Path("{flowId}/versions/latest/metadata")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get latest bucket flow version metadata",
            description = "Gets the metadata for the latest version of a flow.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowSnapshotMetadata.class))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response getLatestFlowVersionMetadata(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId) {

        final VersionedFlowSnapshotMetadata latest = serviceFacade.getLatestFlowSnapshotMetadata(bucketId, flowId);
        return Response.status(Response.Status.OK).entity(latest).build();
    }

    @GET
    @Path("{flowId}/versions/{versionNumber: \\d+}/export")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Exports specified bucket flow version content",
            description = "Exports the specified version of a flow, including the metadata and content of the flow.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowSnapshot.class))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response exportVersionedFlow(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId,
            @PathParam("versionNumber")
            @Parameter(description = "The version number") final Integer versionNumber) {

        final ExportedVersionedFlowSnapshot exportedVersionedFlowSnapshot = serviceFacade.exportFlowSnapshot(bucketId, flowId, versionNumber);

        final VersionedFlowSnapshot versionedFlowSnapshot = exportedVersionedFlowSnapshot.getVersionedFlowSnapshot();

        final String contentDisposition = String.format(
                "attachment; filename=\"%s\"",
                exportedVersionedFlowSnapshot.getFilename());

        return generateOkResponse(versionedFlowSnapshot)
                .header(HttpHeaders.CONTENT_DISPOSITION, contentDisposition)
                .header("Filename", exportedVersionedFlowSnapshot.getFilename())
                .build();
    }

    @GET
    @Path("{flowId}/versions/{versionNumber: \\d+}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bucket flow version",
            description = "Gets the given version of a flow, including the metadata and content for the version.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowSnapshot.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response getFlowVersion(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId,
            @PathParam("versionNumber")
            @Parameter(description = "The version number") final Integer versionNumber) {

        final VersionedFlowSnapshot snapshot = serviceFacade.getFlowSnapshot(bucketId, flowId, versionNumber);
        return Response.status(Response.Status.OK).entity(snapshot).build();
    }

    @GET
    @Path("{flowId}/diff/{versionA: \\d+}/{versionB: \\d+}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bucket flow diff",
            description = "Computes the differences between two given versions of a flow.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = VersionedFlowDifference.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response getFlowDiff(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId,
            @PathParam("versionA")
            @Parameter(description = "The first version number") final Integer versionNumberA,
            @PathParam("versionB")
            @Parameter(description = "The second version number") final Integer versionNumberB) {

        final VersionedFlowDifference result = serviceFacade.getFlowDiff(bucketId, flowId, versionNumberA, versionNumberB);
        return Response.status(Response.Status.OK).entity(result).build();
    }

    private static void verifyPathParamsMatchBody(String bucketIdParam, BucketItem bodyBucketItem) throws BadRequestException {
        if (StringUtils.isBlank(bucketIdParam)) {
            throw new BadRequestException("Bucket id path parameter cannot be blank");
        }

        if (bodyBucketItem == null) {
            throw new BadRequestException("Object in body cannot be null");
        }

        if (bodyBucketItem.getBucketIdentifier() != null && !bucketIdParam.equals(bodyBucketItem.getBucketIdentifier())) {
            throw new BadRequestException("Bucket id in path param must match bucket id in body");
        }
    }

    private static void verifyPathParamsMatchBody(String bucketIdParam, String flowIdParam, BucketItem bodyBucketItem) throws BadRequestException {
        verifyPathParamsMatchBody(bucketIdParam, bodyBucketItem);

        if (StringUtils.isBlank(flowIdParam)) {
            throw new BadRequestException("Flow id path parameter cannot be blank");
        }

        if (bodyBucketItem.getIdentifier() != null && !flowIdParam.equals(bodyBucketItem.getIdentifier())) {
            throw new BadRequestException("Item id in path param must match item id in body");
        }
    }

    private static void verifyPathParamsMatchBody(String bucketIdParam, String flowIdParam, VersionedFlowSnapshot flowSnapshot) throws BadRequestException {
        if (StringUtils.isBlank(bucketIdParam)) {
            throw new BadRequestException("Bucket id path parameter cannot be blank");
        }

        if (StringUtils.isBlank(flowIdParam)) {
            throw new BadRequestException("Flow id path parameter cannot be blank");
        }

        if (flowSnapshot == null) {
            throw new BadRequestException("VersionedFlowSnapshot cannot be null in body");
        }

        final VersionedFlowSnapshotMetadata metadata = flowSnapshot.getSnapshotMetadata();
        if (metadata != null && metadata.getBucketIdentifier() != null && !bucketIdParam.equals(metadata.getBucketIdentifier())) {
            throw new BadRequestException("Bucket id in path param must match bucket id in body");
        }
        if (metadata != null && metadata.getFlowIdentifier() != null && !flowIdParam.equals(metadata.getFlowIdentifier())) {
            throw new BadRequestException("Flow id in path param must match flow id in body");
        }
    }

    private static void setBucketItemMetadataIfMissing(
            @NotNull String bucketIdParam,
            @NotNull String bucketItemIdParam,
            @NotNull BucketItem bucketItem) {
        if (bucketItem.getBucketIdentifier() == null) {
            bucketItem.setBucketIdentifier(bucketIdParam);
        }

        if (bucketItem.getIdentifier() == null) {
            bucketItem.setIdentifier(bucketItemIdParam);
        }
    }

    private static void setSnaphotMetadataIfMissing(
            @NotNull String bucketIdParam,
            @NotNull String flowIdParam,
            @NotNull VersionedFlowSnapshot flowSnapshot) {

        VersionedFlowSnapshotMetadata metadata = flowSnapshot.getSnapshotMetadata();
        if (metadata == null) {
            metadata = new VersionedFlowSnapshotMetadata();
        }

        if (metadata.getBucketIdentifier() == null) {
            metadata.setBucketIdentifier(bucketIdParam);
        }

        if (metadata.getFlowIdentifier() == null) {
            metadata.setFlowIdentifier(flowIdParam);
        }

        flowSnapshot.setSnapshotMetadata(metadata);
    }
}
