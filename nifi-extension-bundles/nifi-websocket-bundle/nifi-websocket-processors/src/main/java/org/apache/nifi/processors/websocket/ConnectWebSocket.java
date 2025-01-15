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

package org.apache.nifi.processors.websocket;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.websocket.WebSocketClientService;
import org.apache.nifi.websocket.WebSocketService;
import org.apache.nifi.websocket.WebSocketSessionInfo;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_CS_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_ENDPOINT_ID;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_LOCAL_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_MESSAGE_TYPE;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_REMOTE_ADDRESS;
import static org.apache.nifi.processors.websocket.WebSocketProcessorAttributes.ATTR_WS_SESSION_ID;

@Tags({"subscribe", "WebSocket", "consume", "listen"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@TriggerSerially
@CapabilityDescription("Acts as a WebSocket client endpoint to interact with a remote WebSocket server." +
        " FlowFiles are transferred to downstream relationships according to received message types" +
        " as WebSocket client configured with this processor receives messages from remote WebSocket server." +
        " If a new flowfile is passed to the processor, the previous sessions will be closed and any data being" +
        " sent will be aborted.")
@WritesAttributes({
        @WritesAttribute(attribute = ATTR_WS_CS_ID, description = "WebSocket Controller Service id."),
        @WritesAttribute(attribute = ATTR_WS_SESSION_ID, description = "Established WebSocket session id."),
        @WritesAttribute(attribute = ATTR_WS_ENDPOINT_ID, description = "WebSocket endpoint id."),
        @WritesAttribute(attribute = ATTR_WS_LOCAL_ADDRESS, description = "WebSocket client address."),
        @WritesAttribute(attribute = ATTR_WS_REMOTE_ADDRESS, description = "WebSocket server address."),
        @WritesAttribute(attribute = ATTR_WS_MESSAGE_TYPE, description = "TEXT or BINARY."),
})
public class ConnectWebSocket extends AbstractWebSocketGatewayProcessor {

    public static final PropertyDescriptor PROP_WEBSOCKET_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("websocket-client-controller-service")
            .displayName("WebSocket Client ControllerService")
            .description("A WebSocket CLIENT Controller Service which can connect to a WebSocket server.")
            .required(true)
            .identifiesControllerService(WebSocketClientService.class)
            .build();

    public static final PropertyDescriptor PROP_WEBSOCKET_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("websocket-client-id")
            .displayName("WebSocket Client Id")
            .description("The client ID to identify WebSocket session." +
                    " It should be unique within the WebSocket Client Controller Service." +
                    " Otherwise, it throws WebSocketConfigurationException when it gets started.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PROP_WEBSOCKET_CLIENT_SERVICE,
            PROP_WEBSOCKET_CLIENT_ID
    );

    private static final Set<Relationship> RELATIONSHIPS = Stream.concat(
            getAbstractRelationships().stream(),
            Stream.of(REL_SUCCESS, REL_FAILURE)
            ).collect(Collectors.toUnmodifiableSet());

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected WebSocketService getWebSocketService(final ProcessContext context) {
        return context.getProperty(PROP_WEBSOCKET_CLIENT_SERVICE)
                .asControllerService(WebSocketService.class);
    }

    @Override
    protected String getEndpointId(final ProcessContext context) {
        return context.getProperty(PROP_WEBSOCKET_CLIENT_ID).getValue();
    }

    @Override
    protected String getTransitUri(final WebSocketSessionInfo sessionInfo) {
        return ((WebSocketClientService) webSocketService).getTargetUri();
    }
}
