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
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.registry.RegistryConfiguration;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Path("/config")
@Tag(name = "Config")
public class ConfigResource extends ApplicationResource {

    @Autowired
    public ConfigResource(
            final ServiceFacade serviceFacade,
            final EventService eventService) {
        super(serviceFacade, eventService);
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get configration",
            description = "Gets the NiFi Registry configurations.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = RegistryConfiguration.class))),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401)
            },
            extensions = {
                    @Extension(
                            name = "access-policy", properties = {
                            @ExtensionProperty(name = "action", value = "read"),
                            @ExtensionProperty(name = "resource", value = "/policies,/tenants")}
                    )
            }
    )
    public Response getConfiguration() {
        final RegistryConfiguration config = serviceFacade.getRegistryConfiguration();
        return Response.status(Response.Status.OK).entity(config).build();
    }
}
