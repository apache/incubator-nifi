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
package org.apache.nifi.processors.gcp.pubsub;

public class PubSubAttributes {

    public static final String MESSAGE_ID_ATTRIBUTE = "gcp.pubsub.messageId";
    public static final String MESSAGE_ID_DESCRIPTION = "ID of the pubsub message published to the configured Google Cloud PubSub topic";

    public static final String RECORDS_ATTRIBUTE = "gcp.pubsub.count.records";
    public static final String RECORDS_DESCRIPTION = "Count of pubsub messages published to the configured Google Cloud PubSub topic";

    public static final String TOPIC_NAME_ATTRIBUTE = "gcp.pubsub.topic";
    public static final String TOPIC_NAME_DESCRIPTION = "Name of the Google Cloud PubSub topic the message was published to";

    public static final String ACK_ID_ATTRIBUTE = "gcp.pubsub.ackId";
    public static final String ACK_ID_DESCRIPTION = "Acknowledgement Id of the consumed Google Cloud PubSub message";

    public static final String SERIALIZED_SIZE_ATTRIBUTE = "gcp.pubsub.messageSize";
    public static final String SERIALIZED_SIZE_DESCRIPTION = "Serialized size of the consumed Google Cloud PubSub message";

    public static final String MSG_ATTRIBUTES_COUNT_ATTRIBUTE = "gcp.pubsub.attributesCount";
    public static final String MSG_ATTRIBUTES_COUNT_DESCRIPTION = "Number of attributes the consumed PubSub message has, if any";

    public static final String MSG_PUBLISH_TIME_ATTRIBUTE = "gcp.pubsub.publishTime";
    public static final String MSG_PUBLISH_TIME_DESCRIPTION = "Timestamp value when the message was published";

    public static final String SUBSCRIPTION_NAME_ATTRIBUTE = "gcp.pubsub.subscription";
    public static final String SUBSCRIPTION_NAME_DESCRIPTION = "Name of the PubSub subscription";

    public static final String DYNAMIC_ATTRIBUTES_ATTRIBUTE = "Dynamic Attributes";
    public static final String DYNAMIC_ATTRIBUTES_DESCRIPTION = "Other than the listed attributes, this processor may write zero or more attributes, " +
            "if the original Google Cloud Publisher client added any attributes to the message while sending";

    public static final String ORDERING_KEY_ATTRIBUTE = "gcp.pubsub.ordering.key";
    public static final String ORDERING_KEY_DESCRIPTION = "If non-empty, identifies related messages for which publish order should be"
            + " respected. If a 'Subscription' has 'enable_message_ordering' set to 'true',"
            + " messages published with the same non-empty 'ordering_key' value will be"
            + " delivered to subscribers in the order in which they are received by the"
            + " Pub/Sub system. All 'PubsubMessage's published in a given 'PublishRequest'"
            + " must specify the same 'ordering_key' value.";
}
