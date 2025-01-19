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
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import org.apache.nifi.extension.ExtensionMetadata;
import org.apache.nifi.registry.event.EventFactory;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.extension.bundle.BundleVersionFilterParams;
import org.apache.nifi.registry.extension.bundle.BundleVersionMetadata;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.apache.nifi.registry.web.service.StreamingContent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.SortedSet;

@Component
@Path("/bundles")
@Tag(name = "Bundles")
public class BundleResource extends ApplicationResource {

    public static final String CONTENT_DISPOSITION_HEADER = "content-disposition";

    @Autowired
    public BundleResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    // ---------- Extension Bundles ----------

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get all bundles",
            description = "Gets the metadata for all bundles across all authorized buckets with optional filters applied. " +
                    "The returned results will include only items from buckets for which the user is authorized. " +
                    "If the user is not authorized to any buckets, an empty list will be returned. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = Bundle.class)))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401)
            }
    )
    public Response getBundles(
            @QueryParam("bucketName")
            @Parameter(
                    description = "Optional bucket name to filter results. The value may be an exact match, or a wildcard, " +
                            "such as 'My Bucket%' to select all bundles where the bucket name starts with 'My Bucket'."
            ) final String bucketName,
            @QueryParam("groupId")
            @Parameter(
                    description = "Optional groupId to filter results. The value may be an exact match, or a wildcard, " +
                            "such as 'com.%' to select all bundles where the groupId starts with 'com.'."
            ) final String groupId,
            @QueryParam("artifactId")
            @Parameter(
                    description = "Optional artifactId to filter results. The value may be an exact match, or a wildcard, " +
                            "such as 'nifi-%' to select all bundles where the artifactId starts with 'nifi-'."
            ) final String artifactId) {

        final BundleFilterParams filterParams = BundleFilterParams.of(bucketName, groupId, artifactId);

        // Service facade will return only bundles from authorized buckets
        final List<Bundle> bundles = serviceFacade.getBundles(filterParams);
        return Response.status(Response.Status.OK).entity(bundles).build();
    }

    @GET
    @Path("{bundleId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bundle",
            description = "Gets the metadata about an extension bundle. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = Bundle.class))),
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
    public Response getBundle(
            @PathParam("bundleId")
            @Parameter(description = "The extension bundle identifier") final String bundleId) {

        final Bundle bundle = serviceFacade.getBundle(bundleId);
        return Response.status(Response.Status.OK).entity(bundle).build();
    }

    @DELETE
    @Path("{bundleId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Delete bundle",
            description = "Deletes the given extension bundle and all of it's versions. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = Bundle.class))),
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
    public Response deleteBundle(
            @PathParam("bundleId")
            @Parameter(description = "The extension bundle identifier") final String bundleId) {

        final Bundle deletedBundle = serviceFacade.deleteBundle(bundleId);
        publish(EventFactory.extensionBundleDeleted(deletedBundle));
        return Response.status(Response.Status.OK).entity(deletedBundle).build();
    }

    // ---------- Extension Bundle Versions ----------

    @GET
    @Path("versions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get all bundle versions",
            description = "Gets the metadata about extension bundle versions across all authorized buckets with optional filters applied. " +
                    "If the user is not authorized to any buckets, an empty list will be returned. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = BundleVersionMetadata.class)))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401)
            }
    )
    public Response getBundleVersions(
            @QueryParam("groupId")
            @Parameter(
                    description = "Optional groupId to filter results. The value may be an exact match, or a wildcard, " +
                            "such as 'com.%' to select all bundle versions where the groupId starts with 'com.'."
            ) final String groupId,
            @QueryParam("artifactId")
            @Parameter(
                    description = "Optional artifactId to filter results. The value may be an exact match, or a wildcard, " +
                            "such as 'nifi-%' to select all bundle versions where the artifactId starts with 'nifi-'."
            ) final String artifactId,
            @QueryParam("version")
            @Parameter(
                    description = "Optional version to filter results. The value maye be an exact match, or a wildcard, " +
                            "such as '1.0.%' to select all bundle versions where the version starts with '1.0.'."
            ) final String version
    ) {

        final BundleVersionFilterParams filterParams = BundleVersionFilterParams.of(groupId, artifactId, version);
        final SortedSet<BundleVersionMetadata> bundleVersions = serviceFacade.getBundleVersions(filterParams);
        return Response.status(Response.Status.OK).entity(bundleVersions).build();
    }

    @GET
    @Path("{bundleId}/versions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bundle versions",
            description = "Gets the metadata for the versions of the given extension bundle. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = BundleVersionMetadata.class)))),
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
    public Response getBundleVersions(
            @PathParam("bundleId")
            @Parameter(description = "The extension bundle identifier") final String bundleId) {

        final SortedSet<BundleVersionMetadata> bundleVersions = serviceFacade.getBundleVersions(bundleId);
        return Response.status(Response.Status.OK).entity(bundleVersions).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bundle version",
            description = "Gets the descriptor for the given version of the given extension bundle. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = BundleVersion.class))),
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
    public Response getBundleVersion(
            @PathParam("bundleId")
            @Parameter(description = "The extension bundle identifier") final String bundleId,
            @PathParam("version")
            @Parameter(description = "The version of the bundle") final String version) {

        final BundleVersion bundleVersion = serviceFacade.getBundleVersion(bundleId, version);
        return Response.ok(bundleVersion).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}/content")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Operation(
            summary = "Get bundle version content",
            description = "Gets the binary content for the given version of the given extension bundle. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = byte[].class))),
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
    public Response getBundleVersionContent(
            @PathParam("bundleId")
            @Parameter(description = "The extension bundle identifier") final String bundleId,
            @PathParam("version")
            @Parameter(description = "The version of the bundle") final String version) {

        final StreamingContent streamingContent = serviceFacade.getBundleVersionContent(bundleId, version);

        final String filename = streamingContent.getFilename();
        final StreamingOutput output = streamingContent.getOutput();

        return Response.ok(output)
                .header(CONTENT_DISPOSITION_HEADER, "attachment; filename = " + filename)
                .build();
    }

    @DELETE
    @Path("{bundleId}/versions/{version}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Delete bundle version",
            description = "Deletes the given extension bundle version and it's associated binary content. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = BundleVersion.class))),
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
    public Response deleteBundleVersion(
            @PathParam("bundleId")
            @Parameter(description = "The extension bundle identifier") final String bundleId,
            @PathParam("version")
            @Parameter(description = "The version of the bundle") final String version) {

        final BundleVersion deletedBundleVersion = serviceFacade.deleteBundleVersion(bundleId, version);
        publish(EventFactory.extensionBundleVersionDeleted(deletedBundleVersion));
        return Response.status(Response.Status.OK).entity(deletedBundleVersion).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}/extensions")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bundle version extensions",
            description = "Gets the metadata about the extensions in the given extension bundle version. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = ExtensionMetadata.class)))),
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
    public Response getBundleVersionExtensions(
            @PathParam("bundleId")
            @Parameter(description = "The extension bundle identifier") final String bundleId,
            @PathParam("version")
            @Parameter(description = "The version of the bundle") final String version) {

        final SortedSet<ExtensionMetadata> extensions = serviceFacade.getExtensionMetadata(bundleId, version);
        return Response.ok(extensions).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}/extensions/{name}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bundle version extension",
            description = "Gets the metadata about the extension with the given name in the given extension bundle version. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = org.apache.nifi.extension.manifest.Extension.class)))),
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
    public Response getBundleVersionExtension(
            @PathParam("bundleId")
            @Parameter(description = "The extension bundle identifier") final String bundleId,
            @PathParam("version")
            @Parameter(description = "The version of the bundle") final String version,
            @PathParam("name")
            @Parameter(description = "The fully qualified name of the extension") final String name
    ) {

        final org.apache.nifi.extension.manifest.Extension extension =
                serviceFacade.getExtension(bundleId, version, name);
        return Response.ok(extension).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}/extensions/{name}/docs")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_HTML)
    @Operation(
            summary = "Get bundle version extension docs",
            description = "Gets the documentation for the given extension in the given extension bundle version. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200"),
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
    public Response getBundleVersionExtensionDocs(
            @PathParam("bundleId")
            @Parameter(description = "The extension bundle identifier") final String bundleId,
            @PathParam("version")
            @Parameter(description = "The version of the bundle") final String version,
            @PathParam("name")
            @Parameter(description = "The fully qualified name of the extension") final String name
    ) {
        final StreamingOutput streamingOutput = serviceFacade.getExtensionDocs(bundleId, version, name);
        return Response.ok(streamingOutput).build();
    }

    @GET
    @Path("{bundleId}/versions/{version}/extensions/{name}/docs/additional-details")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_HTML)
    @Operation(
            summary = "Get bundle version extension docs details",
            description = "Gets the additional details documentation for the given extension in the given extension bundle version. " + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200"),
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
    public Response getBundleVersionExtensionAdditionalDetailsDocs(
            @PathParam("bundleId")
            @Parameter(description = "The extension bundle identifier") final String bundleId,
            @PathParam("version")
            @Parameter(description = "The version of the bundle") final String version,
            @PathParam("name")
            @Parameter(description = "The fully qualified name of the extension") final String name
    ) {
        final StreamingOutput streamingOutput = serviceFacade.getAdditionalDetailsDocs(bundleId, version, name);
        return Response.ok(streamingOutput).build();
    }

}
