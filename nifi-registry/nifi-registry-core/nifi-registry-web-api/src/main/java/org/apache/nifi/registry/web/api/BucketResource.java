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
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.event.EventFactory;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.field.Fields;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.revision.web.ClientIdParameter;
import org.apache.nifi.registry.revision.web.LongParameter;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
@Path("/buckets")
@Tag(name = "Buckets")
public class BucketResource extends ApplicationResource {

    @Autowired
    public BucketResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Create bucket",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = Bucket.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "write"),
                            @ExtensionProperty(name = "resource", value = "/buckets")}
                    )
            }
    )
    public Response createBucket(
            @Parameter(description = "The bucket to create", required = true) final Bucket bucket,
            @Parameter(description = "Whether source properties like identifier should be kept")
            @QueryParam("preserveSourceProperties") final boolean preserveSourceProperties) {

        final Bucket createdBucket = serviceFacade.createBucket(bucket, preserveSourceProperties);
        publish(EventFactory.bucketCreated(createdBucket));
        return Response.status(Response.Status.OK).entity(createdBucket).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get all buckets",
            description = "The returned list will include only buckets for which the user is authorized." +
                    "If the user is not authorized for any buckets, this returns an empty list.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = Bucket.class)))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401)
            }
    )
    public Response getBuckets() {
        // ServiceFacade will determine which buckets the user is authorized for
        // Note: We don't explicitly check for access to (READ, /buckets) because
        // a user might have access to individual buckets without top-level access.
        // For example, a user that has (READ, /buckets/bucket-id-1) but not access
        // to /buckets should not get a 403 error returned from this endpoint.
        // This has the side effect that a user with no access to any buckets
        // gets an empty array returned from this endpoint instead of 403 as one
        // might expect.
        final List<Bucket> buckets = serviceFacade.getBuckets();
        return Response.status(Response.Status.OK).entity(buckets).build();
    }

    @GET
    @Path("{bucketId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bucket",
            description = "Gets the bucket with the given id.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = Bucket.class))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response getBucket(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId) {

        final Bucket bucket = serviceFacade.getBucket(bucketId);
        return Response.status(Response.Status.OK).entity(bucket).build();
    }

    @PUT
    @Path("{bucketId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Update bucket",
            description = "Updates the bucket with the given id.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = Bucket.class))),
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
    public Response updateBucket(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId,
            @Parameter(description = "The updated bucket", required = true) final Bucket bucket) {

        if (StringUtils.isBlank(bucketId)) {
            throw new BadRequestException("Bucket id cannot be blank");
        }

        if (bucket == null) {
            throw new BadRequestException("Bucket cannot be null");
        }

        if (bucket.getIdentifier() != null && !bucketId.equals(bucket.getIdentifier())) {
            throw new BadRequestException("Bucket id in path param must match bucket id in body");
        } else {
            bucket.setIdentifier(bucketId);
        }

        final Bucket updatedBucket = serviceFacade.updateBucket(bucket);
        publish(EventFactory.bucketUpdated(updatedBucket));
        return Response.status(Response.Status.OK).entity(updatedBucket).build();
    }

    @DELETE
    @Path("{bucketId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Delete bucket",
            description = "Deletes the bucket with the given id, along with all objects stored in the bucket",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = Bucket.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "403", description = HttpStatusMessages.MESSAGE_403),
                    @ApiResponse(responseCode = "404", description = HttpStatusMessages.MESSAGE_404)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "delete"),
                            @ExtensionProperty(name = "resource", value = "/buckets/{bucketId}")}
                    )
            }
    )
    public Response deleteBucket(
            @Parameter(description = "The version is used to verify the client is working with the latest version of the entity.", required = true)
            @QueryParam(VERSION) final LongParameter version,
            @Parameter(description = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.")
            @QueryParam(CLIENT_ID)
            @DefaultValue(StringUtils.EMPTY) final ClientIdParameter clientId,
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId) {

        if (StringUtils.isBlank(bucketId)) {
            throw new BadRequestException("Bucket id cannot be blank");
        }

        final RevisionInfo revisionInfo = getRevisionInfo(version, clientId);
        final Bucket deletedBucket = serviceFacade.deleteBucket(bucketId, revisionInfo);
        publish(EventFactory.bucketDeleted(deletedBucket));

        return Response.status(Response.Status.OK).entity(deletedBucket).build();
    }

    @GET
    @Path("fields")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bucket fields",
            description = "Retrieves bucket field names for searching or sorting on buckets.",
            responses = @ApiResponse(content = @Content(schema = @Schema(implementation = Fields.class)))
    )
    public Response getAvailableBucketFields() {
        final Set<String> bucketFields = serviceFacade.getBucketFields();
        final Fields fields = new Fields(bucketFields);
        return Response.status(Response.Status.OK).entity(fields).build();
    }
}
