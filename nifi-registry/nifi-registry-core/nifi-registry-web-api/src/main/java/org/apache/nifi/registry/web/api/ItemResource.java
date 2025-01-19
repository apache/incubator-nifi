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
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.apache.nifi.registry.bucket.BucketItem;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.field.Fields;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
@Path("/items")
@Tag(name = "Items")
public class ItemResource extends ApplicationResource {

    @Context
    UriInfo uriInfo;

    @Autowired
    public ItemResource(final ServiceFacade serviceFacade, final EventService eventService) {
        super(serviceFacade, eventService);
    }


    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get all items",
            description = "Get items across all buckets. The returned items will include only items from buckets for which the user is authorized. " +
                    "If the user is not authorized to any buckets, an empty list will be returned.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = BucketItem.class)))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401)
            }
    )
    public Response getItems() {
        // Service facade with return only items from authorized buckets
        // Note: We don't explicitly check for access to (READ, /buckets) or
        // (READ, /items ) because a user might have access to individual buckets
        // without top-level access. For example, a user that has
        // (READ, /buckets/bucket-id-1) but not access to /buckets should not
        // get a 403 error returned from this endpoint. This has the side effect
        // that a user with no access to any buckets gets an empty array returned
        // from this endpoint instead of 403 as one might expect.
        final List<BucketItem> items = serviceFacade.getBucketItems();
        return Response.status(Response.Status.OK).entity(items).build();
    }

    @GET
    @Path("{bucketId}")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get bucket items",
            description = "Gets the items located in the given bucket.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(array = @ArraySchema(schema = @Schema(implementation = BucketItem.class)))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
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
    public Response getItems(
            @PathParam("bucketId")
            @Parameter(description = "The bucket identifier") final String bucketId) {

        final List<BucketItem> items = serviceFacade.getBucketItems(bucketId);
        return Response.status(Response.Status.OK).entity(items).build();
    }

    @GET
    @Path("fields")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get item fields",
            description = "Retrieves the item field names for searching or sorting on bucket items.",
            responses = @ApiResponse(content = @Content(schema = @Schema(implementation = Fields.class)))
    )
    public Response getAvailableBucketItemFields() {
        final Set<String> bucketFields = serviceFacade.getBucketItemFields();
        final Fields fields = new Fields(bucketFields);
        return Response.status(Response.Status.OK).entity(fields).build();
    }

}
