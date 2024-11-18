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
 * Enumeration of supported PubSub Output Strategies
 */
public enum OutputStrategy implements DescribedValue {
    USE_VALUE("USE_VALUE", "Use Content as Value", "Write only the message payload to the FlowFile record."),
    USE_WRAPPER("USE_WRAPPER", "Use Wrapper", "Write the message's attributes into the FlowFile record.");

    private final String value;
    private final String displayName;
    private final String description;

    OutputStrategy(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
