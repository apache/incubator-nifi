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
package org.apache.nifi.processors.gcp.pubsub.consume;

import org.apache.nifi.components.DescribedValue;

/**
 * Enumeration of supporting strategies for consuming and serializing PubSub
 * messages
 */
public enum ProcessingStrategy implements DescribedValue {
    FLOW_FILE("Write one FlowFile for each PubSub message consumed"),

    DEMARCATOR("Write one FlowFile for each batch of PubSub messages consumed"),

    RECORD("Write one FlowFile containing multiple PubSub messages consumed and processed with Record Reader and Record Writer");

    private final String description;

    ProcessingStrategy(final String description) {
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return name();
    }

    @Override
    public String getDescription() {
        return description;
    }
}
