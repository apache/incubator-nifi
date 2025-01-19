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
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.field.Fields;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.SortedSet;

@Component
@Path("/flows")
@Tag(name = "Flows")
public class FlowResource extends ApplicationResource {

    @Autowired
    public FlowResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    @GET
    @Path("fields")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get flow fields",
            description = "Retrieves the flow field names that can be used for searching or sorting on flows.",
            responses = @ApiResponse(content = @Content(schema = @Schema(implementation = Fields.class)))
    )
    public Response getAvailableFlowFields() {
        final Set<String> flowFields = serviceFacade.getFlowFields();
        final Fields fields = new Fields(flowFields);
        return Response.status(Response.Status.OK).entity(fields).build();
    }

    @GET
    @Path("{flowId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get flow",
            description = "Gets a flow by id.",
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
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId) {

        final VersionedFlow flow = serviceFacade.getFlow(flowId);
        return Response.status(Response.Status.OK).entity(flow).build();
    }

    @GET
    @Path("{flowId}/versions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get flow versions",
            description = "Gets summary information for all versions of a given flow. Versions are ordered newest->oldest.",
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
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId) {

        final SortedSet<VersionedFlowSnapshotMetadata> snapshots = serviceFacade.getFlowSnapshots(flowId);
        return Response.status(Response.Status.OK).entity(snapshots).build();
    }

    @GET
    @Path("{flowId}/versions/{versionNumber: \\d+}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get flow version",
            description = "Gets the given version of a flow, including metadata and flow content.",
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
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId,
            @PathParam("versionNumber")
            @Parameter(description = "The version number") final Integer versionNumber) {

        final VersionedFlowSnapshot snapshot = serviceFacade.getFlowSnapshot(flowId, versionNumber);
        return Response.status(Response.Status.OK).entity(snapshot).build();
    }

    @GET
    @Path("{flowId}/versions/latest")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get latest flow version",
            description = "Gets the latest version of a flow, including metadata and flow content.",
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
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId) {

        final VersionedFlowSnapshot lastSnapshot = serviceFacade.getLatestFlowSnapshot(flowId);
        return Response.status(Response.Status.OK).entity(lastSnapshot).build();
    }

    @GET
    @Path("{flowId}/versions/latest/metadata")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get latest flow version metadata",
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
            @PathParam("flowId")
            @Parameter(description = "The flow identifier") final String flowId) {

        final VersionedFlowSnapshotMetadata latestMetadata = serviceFacade.getLatestFlowSnapshotMetadata(flowId);
        return Response.status(Response.Status.OK).entity(latestMetadata).build();
    }
}
