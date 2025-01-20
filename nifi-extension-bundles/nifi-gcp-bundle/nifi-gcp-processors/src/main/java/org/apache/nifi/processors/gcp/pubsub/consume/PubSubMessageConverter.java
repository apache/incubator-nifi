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

import com.google.pubsub.v1.ReceivedMessage;
import org.apache.nifi.processor.ProcessSession;

import java.util.List;

/**
 * Implementations of {@link PubSubMessageConverter} employ specialized
 * strategies for converting PubSub messages into NiFi FlowFiles.
 */
public interface PubSubMessageConverter {

    void toFlowFiles(final ProcessSession session, final List<ReceivedMessage> messages, final List<String> ackIds, final String subscriptionName);

}
