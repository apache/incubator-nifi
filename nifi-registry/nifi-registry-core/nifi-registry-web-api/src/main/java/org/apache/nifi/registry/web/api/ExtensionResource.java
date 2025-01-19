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
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.extension.ExtensionFilterParams;
import org.apache.nifi.extension.ExtensionMetadata;
import org.apache.nifi.extension.ExtensionMetadataContainer;
import org.apache.nifi.extension.TagCount;
import org.apache.nifi.extension.manifest.ExtensionType;
import org.apache.nifi.extension.manifest.ProvidedServiceAPI;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;

@Component
@Path("/extensions")
@Tag(name = "Extensions")
public class ExtensionResource extends ApplicationResource {

    public ExtensionResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get all extensions",
            description = "Gets the metadata for all extensions that match the filter params and are part of bundles located in buckets the " +
                    "current user is authorized for. If the user is not authorized to any buckets, an empty result set will be returned." +
                    NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ExtensionMetadataContainer.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            }
    )
    public Response getExtensions(
            @QueryParam("bundleType")
            @Parameter(description = "The type of bundles to return") final BundleType bundleType,
            @QueryParam("extensionType")
            @Parameter(description = "The type of extensions to return") final ExtensionType extensionType,
            @QueryParam("tag")
            @Parameter(description = "The tags to filter on, will be used in an OR statement") final Set<String> tags
    ) {

        final ExtensionFilterParams filterParams = new ExtensionFilterParams.Builder()
                .bundleType(bundleType)
                .extensionType(extensionType)
                .addTags(tags == null ? Collections.emptyList() : tags)
                .build();

        final SortedSet<ExtensionMetadata> extensionMetadata = serviceFacade.getExtensionMetadata(filterParams);

        final ExtensionMetadataContainer container = new ExtensionMetadataContainer();
        container.setExtensions(extensionMetadata);
        container.setNumResults(extensionMetadata.size());
        container.setFilterParams(filterParams);

        return Response.status(Response.Status.OK).entity(container).build();
    }

    @GET
    @Path("provided-service-api")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get extensions providing service API",
            description = "Gets the metadata for extensions that provide the specified API and are part of bundles located in buckets the " +
                    "current user is authorized for. If the user is not authorized to any buckets, an empty result set will be returned." +
                    NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = ExtensionMetadataContainer.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            }
    )
    public Response getExtensionsProvidingServiceAPI(
            @QueryParam("className")
            @Parameter(description = "The name of the service API class", required = true) final String className,
            @QueryParam("groupId")
            @Parameter(description = "The groupId of the bundle containing the service API class", required = true) final String groupId,
            @QueryParam("artifactId")
            @Parameter(description = "The artifactId of the bundle containing the service API class", required = true) final String artifactId,
            @QueryParam("version")
            @Parameter(description = "The version of the bundle containing the service API class", required = true) final String version
    ) {
        final ProvidedServiceAPI serviceAPI = new ProvidedServiceAPI();
        serviceAPI.setClassName(className);
        serviceAPI.setGroupId(groupId);
        serviceAPI.setArtifactId(artifactId);
        serviceAPI.setVersion(version);

        final SortedSet<ExtensionMetadata> extensionMetadata = serviceFacade.getExtensionMetadata(serviceAPI);

        final ExtensionMetadataContainer container = new ExtensionMetadataContainer();
        container.setExtensions(extensionMetadata);
        container.setNumResults(extensionMetadata.size());

        return Response.status(Response.Status.OK).entity(container).build();
    }

    @GET
    @Path("/tags")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get extension tags",
            description = "Gets all the extension tags known to this NiFi Registry instance, along with the " +
                    "number of extensions that have the given tag." + NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = TagCount.class)))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409)
            }
    )
    public Response getTags() {
        final SortedSet<TagCount> tags = serviceFacade.getExtensionTags();
        return Response.status(Response.Status.OK).entity(tags).build();
    }

}
