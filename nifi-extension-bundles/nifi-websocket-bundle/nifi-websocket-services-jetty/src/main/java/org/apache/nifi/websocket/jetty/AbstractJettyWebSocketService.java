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
package org.apache.nifi.websocket.jetty;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.websocket.AbstractWebSocketService;

import java.util.List;

public abstract class AbstractJettyWebSocketService extends AbstractWebSocketService {

    public static final PropertyDescriptor INPUT_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("input-buffer-size")
            .displayName("Input Buffer Size")
            .description("The size of the input (read from network layer) buffer size.")
            .required(true)
            .defaultValue("4 kb")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_TEXT_MESSAGE_SIZE = new PropertyDescriptor.Builder()
            .name("max-text-message-size")
            .displayName("Max Text Message Size")
            .description("The maximum size of a text message during parsing/generating.")
            .required(true)
            .defaultValue("64 kb")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_BINARY_MESSAGE_SIZE = new PropertyDescriptor.Builder()
            .name("max-binary-message-size")
            .displayName("Max Binary Message Size")
            .description("The maximum size of a binary message during parsing/generating.")
            .required(true)
            .defaultValue("64 kb")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            INPUT_BUFFER_SIZE,
            MAX_TEXT_MESSAGE_SIZE,
            MAX_BINARY_MESSAGE_SIZE
    );

    static List<PropertyDescriptor> getAbstractPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }
}
