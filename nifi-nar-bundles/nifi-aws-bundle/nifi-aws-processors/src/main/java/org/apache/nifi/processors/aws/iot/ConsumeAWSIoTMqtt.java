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
package org.apache.nifi.processors.aws.iot;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.aws.iot.util.IoTMessage;
import org.apache.nifi.processors.aws.iot.util.MqttWebSocketAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

@Tags({"Amazon", "AWS", "IOT", "MQTT", "Websockets", "Get", "Subscribe", "Receive"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Subscribes to and receives messages from MQTT-topic(s) of AWS IoT." +
    "The processor keeps open a WebSocket connection and will automatically renew the " +
    "connection to overcome Amazon's service limit on maximum connection duration. Depending on " +
    "your set up QoS the processor will miss some messages (QoS=0) or receives messages twice (QoS=1) " +
    "while reconnecting to AWS IoT WebSocket endpoint. We strongly recommend you to make use of " +
    "processor isolation as concurrent subscriptions to an MQTT topic result in multiple message receiptions.")
@SeeAlso({ GetAWSIoTShadow.class })
@WritesAttributes({
        @WritesAttribute(attribute = "aws.iot.mqtt.endpoint", description = "AWS endpoint this message was received from."),
        @WritesAttribute(attribute = "aws.iot.mqtt.topic", description = "MQTT topic this message was received from."),
        @WritesAttribute(attribute = "aws.iot.mqtt.client", description = "MQTT client which received the message."),
        @WritesAttribute(attribute = "aws.iot.mqtt.qos", description = "Underlying MQTT quality-of-service.")
})
public class ConsumeAWSIoTMqtt extends AbstractAWSIoTProcessor {

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(
                    PROP_QOS,
                    PROP_TOPIC,
                    PROP_ENDPOINT,
                    PROP_KEEPALIVE,
                    PROP_CLIENT,
                    AWS_CREDENTIALS_PROVIDER_SERVICE,
                    REGION));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // init to build up mqtt connection over web-sockets
        init(context);
        if (mqttClient != null && mqttClient.isConnected()) {
            try {
                // subscribe to topic with configured qos in order to start receiving messages
                mqttClient.subscribe(awsTopic, awsQos);
            } catch (MqttException e) {
                getLogger().error("Error while subscribing to topic " + awsTopic + " with client-id " + mqttClient.getClientId() + " caused by " + e.getMessage());
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // check if connection is about to terminate
        if (isConnectionAboutToExpire()) {
            MqttWebSocketAsyncClient _mqttClient = null;
            try {
                // before subscribing to the topic with new connection first unsubscribe
                // old connection from same topic if subscription is set to QoS 0
                if (awsQos == 0) {
                    mqttClient.unsubscribe(awsTopic).waitForCompletion(mqttActionTimeout);
                }
                // establish a second connection
                _mqttClient = connect(context);
                // now subscribe to topic with new connection
                _mqttClient.subscribe(awsTopic, awsQos).waitForCompletion(mqttActionTimeout);
                // between re-subscription and disconnect from old connection
                // QoS=0 subscription eventually lose some messages
                // QoS=1 subscription eventually receive some messages twice
                // now terminate old connection
                mqttClient.disconnect().waitForCompletion(mqttActionTimeout);
            } catch (MqttException e) {
                getLogger().error("Error while renewing connection with client " + mqttClient.getClientId() + " caused by " + e.getMessage());
            } finally {
                if (_mqttClient != null) {
                    // grab messages left over from old connection
                    _mqttClient.setAwsQueuedMqttMessages(mqttClient.getAwsQueuedMqttMessages());
                    // now set the new connection as the default connection
                    mqttClient = _mqttClient;
                }
            }
        }
        processQueuedMessages(session);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {

    }

    private void processQueuedMessages(ProcessSession session) {
        LinkedBlockingQueue<IoTMessage> messageQueue = mqttClient.getAwsQueuedMqttMessages();

        while (!messageQueue.isEmpty()) {
            FlowFile flowFile = session.create();
            final IoTMessage msg = messageQueue.peek();
            final Map<String, String> attributes = new HashMap<>();

            attributes.put(PROP_NAME_ENDPOINT, awsEndpoint);
            attributes.put(PROP_NAME_TOPIC, msg.getTopic());
            attributes.put(PROP_NAME_CLIENT, awsClientId);
            attributes.put(PROP_NAME_QOS, msg.getQos().toString());
            flowFile = session.putAllAttributes(flowFile, attributes);

            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(msg.getPayload());
                }
            });
            session.transfer(flowFile, REL_SUCCESS);
            session.commit();
        }
    }
}