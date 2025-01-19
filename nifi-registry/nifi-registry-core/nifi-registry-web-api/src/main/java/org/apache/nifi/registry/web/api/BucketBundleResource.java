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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.registry.event.EventFactory;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.extension.bundle.Bundle;
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.bundle.BundleVersion;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Component
@Path("/buckets/{bucketId}/bundles")
@Tag(name = "BucketBundles")
public class BucketBundleResource extends ApplicationResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(BucketBundleResource.class);

    @Autowired
    public BucketBundleResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    @POST
    @Path("{bundleType}")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Create extension bundle version",
            description = "Creates a version of an extension bundle by uploading a binary artifact. " +
                    "If an extension bundle already exists in the given bucket with the same group id and artifact id " +
                    "as that of the bundle being uploaded, then it will be added as a new version to the existing bundle. " +
                    "If an extension bundle does not already exist in the given bucket with the same group id and artifact id, " +
                    "then a new extension bundle will be created and this version will be added to the new bundle. " +
                    "Client's may optionally supply a SHA-256 in hex format through the multi-part form field 'sha256'. " +
                    "If supplied, then this value will be compared against the SHA-256 computed by the server, and the bundle " +
                    "will be rejected if the values do not match. If not supplied, the bundle will be accepted, but will be marked " +
                    "to indicate that the client did not supply a SHA-256 during creation. " + NON_GUARANTEED_ENDPOINT,
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
    public Response createExtensionBundleVersion(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier", required = true) final String bucketId,
            @PathParam("bundleType")
            @Parameter(description = "The type of the bundle", required = true) final BundleType bundleType,
            @FormDataParam("file") final InputStream fileInputStream,
            @FormDataParam("file") final FormDataContentDisposition fileMetaData,
            @FormDataParam("sha256") final String clientSha256) throws IOException {

        LOGGER.debug("Creating extension bundle version for bundle type {}", bundleType);

        final BundleVersion createdBundleVersion = serviceFacade.createBundleVersion(
                bucketId, bundleType, fileInputStream, clientSha256);

        publish(EventFactory.extensionBundleCreated(createdBundleVersion.getBundle()));
        publish(EventFactory.extensionBundleVersionCreated(createdBundleVersion));

        return Response.status(Response.Status.OK).entity(createdBundleVersion).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get extension bundles by bucket",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = Bundle.class)))),
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
    public Response getExtensionBundles(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier", required = true) final String bucketId) {

        final List<Bundle> bundles = serviceFacade.getBundlesByBucket(bucketId);
        return Response.status(Response.Status.OK).entity(bundles).build();
    }

}
