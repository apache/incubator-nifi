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

package org.apache.nifi.processors.mqtt;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.mqtt.common.AbstractMQTTProcessor;
import org.apache.nifi.processors.mqtt.common.MqttException;
import org.apache.nifi.processors.mqtt.common.ReceivedMqttMessage;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SchemaValidationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.processors.mqtt.ConsumeMQTT.BROKER_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_DUPLICATE_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.IS_RETAINED_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.QOS_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.RECORD_COUNT_KEY;
import static org.apache.nifi.processors.mqtt.ConsumeMQTT.TOPIC_ATTRIBUTE_KEY;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_QOS_0;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_QOS_1;
import static org.apache.nifi.processors.mqtt.common.MqttConstants.ALLOWABLE_VALUE_QOS_2;


@Tags({"subscribe", "MQTT", "IOT", "consume", "listen"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@CapabilityDescription("Subscribes to a topic and receives messages from an MQTT broker")
@SeeAlso({PublishMQTT.class})
@WritesAttributes({
    @WritesAttribute(attribute = RECORD_COUNT_KEY, description = "The number of records received"),
    @WritesAttribute(attribute = BROKER_ATTRIBUTE_KEY, description = "MQTT broker that was the message source"),
    @WritesAttribute(attribute = TOPIC_ATTRIBUTE_KEY, description = "MQTT topic on which message was received"),
    @WritesAttribute(attribute = QOS_ATTRIBUTE_KEY, description = "The quality of service for this message."),
    @WritesAttribute(attribute = IS_DUPLICATE_ATTRIBUTE_KEY, description = "Whether or not this message might be a duplicate of one which has already been received."),
    @WritesAttribute(attribute = IS_RETAINED_ATTRIBUTE_KEY, description = "Whether or not this message was from a current publisher, or was \"retained\" by the server as the last message published " +
            "on the topic.")})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The 'Max Queue Size' specifies the maximum number of messages that can be hold in memory by NiFi by a single "
        + "instance of this processor. A high value for this property could represent a lot of data being stored in memory.")
public class ConsumeMQTT extends AbstractMQTTProcessor {

    public final static String RECORD_COUNT_KEY = "record.count";
    public final static String BROKER_ATTRIBUTE_KEY = "mqtt.broker";
    public final static String TOPIC_ATTRIBUTE_KEY = "mqtt.topic";
    public final static String QOS_ATTRIBUTE_KEY = "mqtt.qos";
    public final static String IS_DUPLICATE_ATTRIBUTE_KEY = "mqtt.isDuplicate";
    public final static String IS_RETAINED_ATTRIBUTE_KEY = "mqtt.isRetained";

    public final static String TOPIC_FIELD_KEY = "_topic";
    public final static String QOS_FIELD_KEY = "_qos";
    public final static String IS_DUPLICATE_FIELD_KEY = "_isDuplicate";
    public final static String IS_RETAINED_FIELD_KEY = "_isRetained";

    private final static String COUNTER_PARSE_FAILURES = "Parse Failures";
    private final static String COUNTER_RECORDS_RECEIVED = "Records Received";
    private final static String COUNTER_RECORDS_PROCESSED = "Records Processed";

    private final static int MAX_MESSAGES_PER_FLOW_FILE = 10000;

    public static final PropertyDescriptor PROP_GROUPID = new PropertyDescriptor.Builder()
            .name("Group ID")
            .description("MQTT consumer group ID to use. If group ID not set, client will connect as individual consumer.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_TOPIC_FILTER = new PropertyDescriptor.Builder()
            .name("Topic Filter")
            .description("The MQTT topic filter to designate the topics to subscribe to.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_QOS = new PropertyDescriptor.Builder()
            .name("Quality of Service(QoS)")
            .displayName("Quality of Service (QoS)")
            .description("The Quality of Service (QoS) to receive the message with. Accepts values '0', '1' or '2'; '0' for 'at most once', '1' for 'at least once', '2' for 'exactly once'.")
            .required(true)
            .defaultValue(ALLOWABLE_VALUE_QOS_0.getValue())
            .allowableValues(
                    ALLOWABLE_VALUE_QOS_0,
                    ALLOWABLE_VALUE_QOS_1,
                    ALLOWABLE_VALUE_QOS_2)
            .build();

    public static final PropertyDescriptor PROP_MAX_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("Max Queue Size")
            .description("The MQTT messages are always being sent to subscribers on a topic regardless of how frequently the processor is scheduled to run. If the 'Run Schedule' is "
                    + "significantly behind the rate at which the messages are arriving to this processor, then a back up can occur in the internal queue of this processor. This property "
                    + "specifies the maximum number of messages this processor will hold in memory at one time in the internal queue. This data would be lost in case of a NiFi restart.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BASE_RECORD_READER)
            .description("The Record Reader to use for parsing received MQTT Messages into Records.")
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BASE_RECORD_WRITER)
            .description("The Record Writer to use for serializing Records before writing them to a FlowFile.")
            .build();

    public static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BASE_MESSAGE_DEMARCATOR)
            .description("With this property, you have an option to output FlowFiles which contains multiple messages. "
                    + "This property allows you to provide a string (interpreted as UTF-8) to use for demarcating apart "
                    + "multiple messages. This is an optional property ; if not provided, and if not defining a "
                    + "Record Reader/Writer, each message received will result in a single FlowFile. To enter special "
                    + "character such as 'new line' use CTRL+Enter or Shift+Enter depending on the OS.")
            .build();

    public static final PropertyDescriptor ADD_ATTRIBUTES_AS_FIELDS = new PropertyDescriptor.Builder()
            .name("add-attributes-as-fields")
            .displayName("Add attributes as fields")
            .description("If setting this property to true, default fields "
                    + "are going to be added in each record: _topic, _qos, _isDuplicate, _isRetained.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .dependsOn(RECORD_READER)
            .build();

    private volatile int qos;
    private volatile String topicPrefix = "";
    private volatile String topicFilter;
    private final AtomicBoolean scheduled = new AtomicBoolean(false);

    private volatile LinkedBlockingQueue<ReceivedMqttMessage> mqttQueue;

    public static final Relationship REL_MESSAGE = new Relationship.Builder()
            .name("Message")
            .description("The MQTT message output")
            .build();

    public static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse.failure")
            .description("If a message cannot be parsed using the configured Record Reader, the contents of the "
                + "message will be routed to this Relationship as its own individual FlowFile.")
            .autoTerminateDefault(true) // to make sure flow are still valid after upgrades
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PROP_BROKER_URI,
            PROP_MQTT_VERSION,
            PROP_USERNAME,
            PROP_PASSWORD,
            PROP_SSL_CONTEXT_SERVICE,
            PROP_CLEAN_SESSION,
            PROP_SESSION_EXPIRY_INTERVAL,
            PROP_CLIENTID,
            PROP_GROUPID,
            PROP_TOPIC_FILTER,
            PROP_QOS,
            RECORD_READER,
            RECORD_WRITER,
            ADD_ATTRIBUTES_AS_FIELDS,
            MESSAGE_DEMARCATOR,
            PROP_CONN_TIMEOUT,
            PROP_KEEP_ALIVE_INTERVAL,
            PROP_LAST_WILL_MESSAGE,
            PROP_LAST_WILL_TOPIC,
            PROP_LAST_WILL_RETAIN,
            PROP_LAST_WILL_QOS,
            PROP_MAX_QUEUE_SIZE
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_MESSAGE,
            REL_PARSE_FAILURE
    );

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        // resize the receive buffer, but preserve data
        if (descriptor == PROP_MAX_QUEUE_SIZE) {
            // it's a mandatory integer, never null
            int newSize = Integer.parseInt(newValue);
            if (mqttQueue != null) {
                int msgPending = mqttQueue.size();
                if (msgPending > newSize) {
                    logger.warn("New receive buffer size ({}) is smaller than the number of messages pending ({}), ignoring resize request. Processor will be invalid.", newSize, msgPending);
                    return;
                }
                LinkedBlockingQueue<ReceivedMqttMessage> newBuffer = new LinkedBlockingQueue<>(newSize);
                mqttQueue.drainTo(newBuffer);
                mqttQueue = newBuffer;
            }

        }
    }

    @Override
    public Collection<ValidationResult> customValidate(ValidationContext context) {
        final Collection<ValidationResult> results = super.customValidate(context);
        int newSize = context.getProperty(PROP_MAX_QUEUE_SIZE).asInteger();
        if (mqttQueue == null) {
            mqttQueue = new LinkedBlockingQueue<>(context.getProperty(PROP_MAX_QUEUE_SIZE).asInteger());
        }
        int msgPending = mqttQueue.size();
        if (msgPending > newSize) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject("ConsumeMQTT Configuration")
                    .explanation(String.format("%s (%d) is smaller than the number of messages pending (%d).",
                            PROP_MAX_QUEUE_SIZE.getDisplayName(), newSize, msgPending))
                    .build());
        }

        final boolean clientIDSet = context.getProperty(PROP_CLIENTID).isSet();
        final boolean clientIDwithEL = context.getProperty(PROP_CLIENTID).isExpressionLanguagePresent();
        final boolean groupIDSet = context.getProperty(PROP_GROUPID).isSet();

        if (!clientIDwithEL && clientIDSet && groupIDSet) {
            results.add(new ValidationResult.Builder()
                    .subject("Client ID and Group ID").valid(false)
                    .explanation("if client ID is not unique, multiple nodes cannot join the consumer group (if you want "
                            + "to set the client ID, please use expression language to make sure each node in the NiFi "
                            + "cluster gets a unique client ID with something like ${hostname()}).")
                    .build());
        }

        return results;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = getLogger();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        qos = context.getProperty(PROP_QOS).asInteger();
        topicFilter = context.getProperty(PROP_TOPIC_FILTER).evaluateAttributeExpressions().getValue();

        if (context.getProperty(PROP_GROUPID).isSet()) {
            topicPrefix = "$share/" + context.getProperty(PROP_GROUPID).getValue() + "/";
        } else {
            topicPrefix = "";
        }

        scheduled.set(true);
    }

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        scheduled.set(false);
        synchronized (this) {
            stopClient();
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        if (mqttQueue != null && !mqttQueue.isEmpty() && processSessionFactory != null) {
            logger.info("Finishing processing leftover messages");
            final ProcessSession session = processSessionFactory.createSession();
            if (context.getProperty(RECORD_READER).isSet()) {
                transferQueueRecord(context, session);
            } else if (context.getProperty(MESSAGE_DEMARCATOR).isSet()) {
                transferQueueDemarcator(context, session);
            } else {
                transferQueue(session);
            }
        } else {
            if (mqttQueue != null && !mqttQueue.isEmpty()) {
                throw new ProcessException("Stopping the processor but there is no ProcessSessionFactory stored and there are messages in the MQTT internal queue. Removing the processor now will " +
                        "clear the queue but will result in DATA LOSS. This is normally due to starting the processor, receiving messages and stopping before the onTrigger happens. The messages " +
                        "in the MQTT internal queue cannot finish processing until until the processor is triggered to run.");
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final boolean isScheduled = scheduled.get();
        if (!isConnected() && isScheduled) {
            synchronized (this) {
                if (!isConnected()) {
                    initializeClient(context);
                }
            }
        }

        if (mqttQueue.isEmpty()) {
            context.yield();
            return;
        }

        if (context.getProperty(RECORD_READER).isSet()) {
            transferQueueRecord(context, session);
        } else if (context.getProperty(MESSAGE_DEMARCATOR).isSet()) {
            transferQueueDemarcator(context, session);
        } else {
            transferQueue(session);
        }
    }

    private void initializeClient(ProcessContext context) {
        // NOTE: This method is called when isConnected returns false which can happen when the client is null, or when it is
        // non-null but not connected, so we need to handle each case and only create a new client when it is null
        try {
            mqttClient = createMqttClient();
            mqttClient.connect();
            mqttClient.subscribe(topicPrefix + topicFilter, qos, this::handleReceivedMessage);
        } catch (Exception e) {
            logger.error("Connection failed to {}. Yielding processor", clientProperties.getRawBrokerUris(), e);
            mqttClient = null; // prevent stucked processor when subscribe fails
            context.yield();
        }
    }

    private void transferQueue(ProcessSession session) {
        while (!mqttQueue.isEmpty()) {
            final ReceivedMqttMessage mqttMessage = mqttQueue.peek();

            final FlowFile messageFlowfile = session.write(createFlowFileAndPopulateAttributes(session, mqttMessage),
                    out -> out.write(mqttMessage.getPayload() == null ? new byte[0] : mqttMessage.getPayload()));

            session.getProvenanceReporter().receive(messageFlowfile, getTransitUri(mqttMessage.getTopic()));
            session.transfer(messageFlowfile, REL_MESSAGE);
            session.commitAsync();
            mqttQueue.remove(mqttMessage);
        }
    }

    private void transferQueueDemarcator(final ProcessContext context, final ProcessSession session) {
        final byte[] demarcator = context.getProperty(MESSAGE_DEMARCATOR).evaluateAttributeExpressions().getValue().getBytes(StandardCharsets.UTF_8);

        FlowFile messageFlowfile = session.create();
        session.putAttribute(messageFlowfile, BROKER_ATTRIBUTE_KEY, clientProperties.getRawBrokerUris());

        messageFlowfile = session.append(messageFlowfile, out -> {
            int i = 0;
            while (!mqttQueue.isEmpty() && i < MAX_MESSAGES_PER_FLOW_FILE) {
                final ReceivedMqttMessage mqttMessage = mqttQueue.poll();
                if (i > 0) {
                    out.write(demarcator);
                }
                out.write(mqttMessage.getPayload() == null ? new byte[0] : mqttMessage.getPayload());
                session.adjustCounter(COUNTER_RECORDS_RECEIVED, 1L, false);
                i++;
            }
        });

        session.getProvenanceReporter().receive(messageFlowfile, getTransitUri(topicPrefix, topicFilter));
        session.transfer(messageFlowfile, REL_MESSAGE);
        session.commitAsync();
    }

    private void transferFailure(final ProcessSession session, final ReceivedMqttMessage mqttMessage) {
        final FlowFile messageFlowfile = session.write(createFlowFileAndPopulateAttributes(session, mqttMessage),
                out -> out.write(mqttMessage.getPayload()));

        session.getProvenanceReporter().receive(messageFlowfile, getTransitUri(mqttMessage.getTopic()));
        session.transfer(messageFlowfile, REL_PARSE_FAILURE);
        session.adjustCounter(COUNTER_PARSE_FAILURES, 1, false);
    }

    private FlowFile createFlowFileAndPopulateAttributes(ProcessSession session, ReceivedMqttMessage mqttMessage) {
        FlowFile messageFlowfile = session.create();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(BROKER_ATTRIBUTE_KEY, clientProperties.getRawBrokerUris());
        attrs.put(TOPIC_ATTRIBUTE_KEY, mqttMessage.getTopic());
        attrs.put(QOS_ATTRIBUTE_KEY, String.valueOf(mqttMessage.getQos()));
        attrs.put(IS_DUPLICATE_ATTRIBUTE_KEY, String.valueOf(mqttMessage.isDuplicate()));
        attrs.put(IS_RETAINED_ATTRIBUTE_KEY, String.valueOf(mqttMessage.isRetained()));

        messageFlowfile = session.putAllAttributes(messageFlowfile, attrs);
        return messageFlowfile;
    }

    private void transferQueueRecord(final ProcessContext context, final ProcessSession session) {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final FlowFile flowFile = session.create();
        session.putAttribute(flowFile, BROKER_ATTRIBUTE_KEY, clientProperties.getRawBrokerUris());

        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();

        final List<ReceivedMqttMessage> doneList = new ArrayList<>();

        RecordSetWriter writer = null;
        boolean isWriterInitialized = false;
        int i = 0;

        try {
            while (!mqttQueue.isEmpty() && i < MAX_MESSAGES_PER_FLOW_FILE) {
                final ReceivedMqttMessage mqttMessage = mqttQueue.poll();
                if (mqttMessage == null) {
                    break;
                }

                final byte[] recordBytes = mqttMessage.getPayload() == null ? new byte[0] : mqttMessage.getPayload();

                try (final InputStream in = new ByteArrayInputStream(recordBytes)) {
                    final RecordReader reader;

                    try {
                        reader = readerFactory.createRecordReader(attributes, in, recordBytes.length, logger);
                    } catch (final Exception e) {
                        logger.error("Failed to parse the message from the internal queue, sending to the parse failure relationship", e);
                        transferFailure(session, mqttMessage);
                        continue;
                    }

                    try {
                        Record record;
                        while ((record = reader.nextRecord()) != null) {

                            if (!isWriterInitialized) {
                                final RecordSchema recordSchema = record.getSchema();
                                final OutputStream rawOut = session.write(flowFile);

                                RecordSchema writeSchema;
                                try {
                                    writeSchema = writerFactory.getSchema(flowFile.getAttributes(), recordSchema);
                                    if (context.getProperty(ADD_ATTRIBUTES_AS_FIELDS).asBoolean()) {
                                        final List<RecordField> fields = new ArrayList<>(writeSchema.getFields());

                                        fields.add(new RecordField(TOPIC_FIELD_KEY, RecordFieldType.STRING.getDataType()));
                                        fields.add(new RecordField(QOS_FIELD_KEY, RecordFieldType.INT.getDataType()));
                                        fields.add(new RecordField(IS_DUPLICATE_FIELD_KEY, RecordFieldType.BOOLEAN.getDataType()));
                                        fields.add(new RecordField(IS_RETAINED_FIELD_KEY, RecordFieldType.BOOLEAN.getDataType()));

                                        writeSchema = new SimpleRecordSchema(fields);
                                    }
                                } catch (final Exception e) {
                                    logger.error("Failed to obtain Schema for FlowFile, sending to the parse failure relationship", e);
                                    transferFailure(session, mqttMessage);
                                    continue;
                                }

                                writer = writerFactory.createWriter(logger, writeSchema, rawOut, flowFile);
                                writer.beginRecordSet();
                            }

                            try {
                                if (context.getProperty(ADD_ATTRIBUTES_AS_FIELDS).asBoolean()) {
                                    record.setValue(TOPIC_FIELD_KEY, mqttMessage.getTopic());
                                    record.setValue(QOS_FIELD_KEY, mqttMessage.getQos());
                                    record.setValue(IS_RETAINED_FIELD_KEY, mqttMessage.isRetained());
                                    record.setValue(IS_DUPLICATE_FIELD_KEY, mqttMessage.isDuplicate());
                                }
                                writer.write(record);
                                isWriterInitialized = true;
                                doneList.add(mqttMessage);
                            } catch (final RuntimeException re) {
                                logger.error("Failed to write message using the configured Record Writer, sending to the parse failure relationship", re);
                                transferFailure(session, mqttMessage);
                                continue;
                            }

                            session.adjustCounter(COUNTER_RECORDS_RECEIVED, 1L, false);
                            i++;
                        }
                    } catch (final IOException | MalformedRecordException | SchemaValidationException e) {
                        logger.error("Failed to write message, sending to the parse failure relationship", e);
                        transferFailure(session, mqttMessage);
                    }
                } catch (Exception e) {
                    logger.error("Failed to write message, sending to the parse failure relationship", e);
                    transferFailure(session, mqttMessage);
                }
            }

            if (writer != null) {
                final WriteResult writeResult = writer.finishRecordSet();
                attributes.put(RECORD_COUNT_KEY, String.valueOf(writeResult.getRecordCount()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.putAll(writeResult.getAttributes());
                recordCount.set(writeResult.getRecordCount());
            }

        } catch (final Exception e) {
            context.yield();

            // we try to add the messages back into the internal queue
            int numberOfMessages = 0;
            for (ReceivedMqttMessage done : doneList) {
                try {
                    mqttQueue.offer(done, 1, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    numberOfMessages++;
                    if (getLogger().isDebugEnabled()) {
                        logger.debug("Could not add message back into the internal queue, this could lead to data loss", ex);
                    }
                }
            }
            if (numberOfMessages > 0) {
                logger.error("Could not add {} message(s) back into the internal queue, this could mean data loss", numberOfMessages);
            }

            throw new ProcessException("Could not process data received from the MQTT broker(s): " + clientProperties.getRawBrokerUris(), e);
        } finally {
            closeWriter(writer);
        }

        if (recordCount.get() == 0) {
            session.remove(flowFile);
            return;
        }

        session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().receive(flowFile, getTransitUri(topicPrefix, topicFilter));
        session.transfer(flowFile, REL_MESSAGE);

        final int count = recordCount.get();
        session.adjustCounter(COUNTER_RECORDS_PROCESSED, count, false);
        logger.info("Successfully processed {} records for {}", count, flowFile);
    }

    private void closeWriter(final RecordSetWriter writer) {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (final Exception ioe) {
            logger.warn("Failed to close Record Writer", ioe);
        }
    }

    private String getTransitUri(String... appends) {
        final StringBuilder stringBuilder = new StringBuilder(clientProperties.getProvenanceFormattedBrokerUris()).append("/");
        for (String append : appends) {
            stringBuilder.append(append);
        }
        return stringBuilder.toString();
    }

    private void handleReceivedMessage(ReceivedMqttMessage message) {
        if (logger.isDebugEnabled()) {
            byte[] payload = message.getPayload();
            final String text = new String(payload, StandardCharsets.UTF_8);
            if (StringUtils.isAsciiPrintable(text)) {
                logger.debug("Message arrived from topic {}. Payload: {}", message.getTopic(), text);
            } else {
                logger.debug("Message arrived from topic {}. Binary value of size {}", message.getTopic(), payload.length);
            }
        }

        try {
            if (!mqttQueue.offer(message, 1, TimeUnit.SECONDS)) {
                throw new IllegalStateException("The subscriber queue is full, cannot receive another message until the processor is scheduled to run.");
            }
        } catch (InterruptedException e) {
            throw new MqttException("Failed to process message arrived from topic " + message.getTopic());
        }
    }
}
