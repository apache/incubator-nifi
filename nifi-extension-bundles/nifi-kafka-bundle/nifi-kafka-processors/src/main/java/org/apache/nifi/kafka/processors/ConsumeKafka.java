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
package org.apache.nifi.kafka.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kafka.processors.common.KafkaUtils;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.processors.consumer.ProcessingStrategy;
import org.apache.nifi.kafka.processors.consumer.bundle.ByteRecordBundler;
import org.apache.nifi.kafka.processors.consumer.convert.FlowFileStreamKafkaMessageConverter;
import org.apache.nifi.kafka.processors.consumer.convert.KafkaMessageConverter;
import org.apache.nifi.kafka.processors.consumer.convert.RecordStreamKafkaMessageConverter;
import org.apache.nifi.kafka.processors.consumer.convert.WrapperRecordStreamKafkaMessageConverter;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.PollingContext;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;

@CapabilityDescription("Consumes messages from Apache Kafka Consumer API. "
        + "The complementary NiFi processor for sending messages is PublishKafka. The Processor supports consumption of Kafka messages, optionally interpreted as NiFi records. "
        + "Please note that, at this time (in read record mode), the Processor assumes that "
        + "all records that are retrieved from a given partition have the same schema. For this mode, if any of the Kafka messages are pulled but cannot be parsed or written with the "
        + "configured Record Reader or Record Writer, the contents of the message will be written to a separate FlowFile, and that FlowFile will be transferred to the "
        + "'parse.failure' relationship. Otherwise, each FlowFile is sent to the 'success' relationship and may contain many individual messages within the single FlowFile. "
        + "A 'record.count' attribute is added to indicate how many messages are contained in the FlowFile. No two Kafka messages will be placed into the same FlowFile if they "
        + "have different schemas, or if they have different values for a message header that is included by the <Headers to Add as Attributes> property.")
@Tags({"Kafka", "Get", "Record", "csv", "avro", "json", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "The number of records received"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type that is provided by the configured Record Writer"),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_COUNT, description = "The number of messages written if more than one"),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_KEY, description = "The key of message if present and if single message. "
                + "How the key is encoded depends on the value of the 'Key Attribute Encoding' property."),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_OFFSET, description = "The offset of the message in the partition of the topic."),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_TIMESTAMP, description = "The timestamp of the message in the partition of the topic."),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_PARTITION, description = "The partition of the topic the message or message bundle is from"),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_TOPIC, description = "The topic the message or message bundle is from"),
        @WritesAttribute(attribute = KafkaFlowFileAttribute.KAFKA_TOMBSTONE, description = "Set to true if the consumed message is a tombstone message")
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@SeeAlso({PublishKafka.class})
public class ConsumeKafka extends AbstractProcessor implements VerifiableProcessor {

    static final AllowableValue TOPIC_NAME = new AllowableValue("names", "names", "Topic is a full topic name or comma separated list of names");
    static final AllowableValue TOPIC_PATTERN = new AllowableValue("pattern", "pattern", "Topic is a regular expression according to the Java Pattern syntax");

    static final PropertyDescriptor CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("Kafka Connection Service")
            .description("Provides connections to Kafka Broker for publishing Kafka Records")
            .identifiesControllerService(KafkaConnectionService.class)
            .expressionLanguageSupported(NONE)
            .required(true)
            .build();

    static final PropertyDescriptor GROUP_ID = new PropertyDescriptor.Builder()
            .name("Group ID")
            .description("Kafka Consumer Group Identifier corresponding to Kafka group.id property")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(NONE)
            .build();

    static final PropertyDescriptor TOPIC_FORMAT = new PropertyDescriptor.Builder()
            .name("Topic Format")
            .description("Specifies whether the Topics provided are a comma separated list of names or a single regular expression")
            .required(true)
            .allowableValues(TOPIC_NAME, TOPIC_PATTERN)
            .defaultValue(TOPIC_NAME)
            .build();

    static final PropertyDescriptor TOPICS = new PropertyDescriptor.Builder()
            .name("Topics")
            .description("The name or pattern of the Kafka Topics from which the Processor consumes Kafka Records. More than one can be supplied if comma separated.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor AUTO_OFFSET_RESET = new PropertyDescriptor.Builder()
            .name("auto.offset.reset")
            .displayName("Auto Offset Reset")
            .description("Automatic offset configuration applied when no previous consumer offset found corresponding to Kafka auto.offset.reset property")
            .required(true)
            .allowableValues(AutoOffsetReset.class)
            .defaultValue(AutoOffsetReset.LATEST.getValue())
            .expressionLanguageSupported(NONE)
            .build();

    static final PropertyDescriptor COMMIT_OFFSETS = new PropertyDescriptor.Builder()
            .name("Commit Offsets")
            .description("Specifies whether this Processor should commit the offsets to Kafka after receiving messages. Typically, this value should be set to true " +
                    "so that messages that are received are not duplicated. However, in certain scenarios, we may want to avoid committing the offsets, that the data can be " +
                    "processed and later acknowledged by PublishKafka in order to provide Exactly Once semantics.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor MAX_UNCOMMITTED_TIME = new PropertyDescriptor.Builder()
            .name("Max Uncommitted Time")
            .description("Specifies the maximum amount of time allowed to pass before offsets must be committed. "
                    + "This value impacts how often offsets will be committed. Committing offsets less often increases "
                    + "throughput but also increases the window of potential data duplication in the event of a rebalance "
                    + "or JVM restart between commits. This value is also related to maximum poll records and the use "
                    + "of a message demarcator. When using a message demarcator we can have far more uncommitted messages "
                    + "than when we're not as there is much less for us to keep track of in memory.")
            .required(true)
            .defaultValue("1 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .dependsOn(COMMIT_OFFSETS, "true")
            .build();

    static final PropertyDescriptor HEADER_ENCODING = new PropertyDescriptor.Builder()
            .name("Header Encoding")
            .description("Character encoding applied when reading Kafka Record Header values and writing FlowFile attributes")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(StandardCharsets.UTF_8.name())
            .required(true)
            .build();

    static final PropertyDescriptor HEADER_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("Header Name Pattern")
            .description("Regular Expression Pattern applied to Kafka Record Header Names for selecting Header Values to be written as FlowFile attributes")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor PROCESSING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Processing Strategy")
            .description("Strategy for processing Kafka Records and writing serialized output to FlowFiles")
            .required(true)
            .allowableValues(ProcessingStrategy.class)
            .defaultValue(ProcessingStrategy.FLOW_FILE.getValue())
            .expressionLanguageSupported(NONE)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The Record Reader to use for incoming Kafka messages")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("The Record Writer to use in order to serialize the outgoing FlowFiles")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .build();

    static final PropertyDescriptor OUTPUT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Output Strategy")
            .description("The format used to output the Kafka Record into a FlowFile Record.")
            .required(true)
            .defaultValue(OutputStrategy.USE_VALUE)
            .allowableValues(OutputStrategy.class)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .build();

    static final PropertyDescriptor KEY_ATTRIBUTE_ENCODING = new PropertyDescriptor.Builder()
            .name("Key Attribute Encoding")
            .description("Encoding for value of configured FlowFile attribute containing Kafka Record Key.")
            .required(true)
            .defaultValue(KeyEncoding.UTF8)
            .allowableValues(KeyEncoding.class)
            .dependsOn(OUTPUT_STRATEGY, OutputStrategy.USE_VALUE)
            .build();

    static final PropertyDescriptor KEY_FORMAT = new PropertyDescriptor.Builder()
            .name("Key Format")
            .description("Specifies how to represent the Kafka Record Key in the output FlowFile")
            .required(true)
            .defaultValue(KeyFormat.BYTE_ARRAY)
            .allowableValues(KeyFormat.class)
            .dependsOn(OUTPUT_STRATEGY, OutputStrategy.USE_WRAPPER)
            .build();

    static final PropertyDescriptor KEY_RECORD_READER = new PropertyDescriptor.Builder()
            .name("Key Record Reader")
            .description("The Record Reader to use for parsing the Kafka Record Key into a Record")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .dependsOn(KEY_FORMAT, KeyFormat.RECORD)
            .build();

    static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("Message Demarcator")
            .required(true)
            .addValidator(Validator.VALID)
            .description("Since KafkaConsumer receives messages in batches, this Processor has an option to output FlowFiles which contains "
                    + "all Kafka messages in a single batch for a given topic and partition and this property allows you to provide a string (interpreted as UTF-8) to use "
                    + "for demarcating apart multiple Kafka messages. This is an optional property and if not provided each Kafka message received "
                    + "will result in a single FlowFile which  "
                    + "time it is triggered. To enter special character such as 'new line' use CTRL+Enter or Shift+Enter depending on the OS")
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR)
            .build();

    static final PropertyDescriptor SEPARATE_BY_KEY = new PropertyDescriptor.Builder()
            .name("Separate By Key")
            .description("When this property is enabled, two messages will only be added to the same FlowFile if both of the Kafka Messages have identical keys.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(MESSAGE_DEMARCATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing one or more serialized Kafka Records")
            .build();

    public static final Relationship PARSE_FAILURE = new Relationship.Builder()
            .name("parse failure")
            .description("If configured to use a Record Reader, a Kafka message that cannot be parsed using the configured Record Reader will be routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_SERVICE,
            GROUP_ID,
            TOPIC_FORMAT,
            TOPICS,
            AUTO_OFFSET_RESET,
            COMMIT_OFFSETS,
            MAX_UNCOMMITTED_TIME,
            HEADER_NAME_PATTERN,
            HEADER_ENCODING,
            PROCESSING_STRATEGY,
            RECORD_READER,
            RECORD_WRITER,
            OUTPUT_STRATEGY,
            KEY_ATTRIBUTE_ENCODING,
            KEY_FORMAT,
            KEY_RECORD_READER,
            MESSAGE_DEMARCATOR,
            SEPARATE_BY_KEY
    );

    private static final Set<Relationship> SUCCESS_RELATIONSHIP = Set.of(SUCCESS);
    private static final Set<Relationship> SUCCESS_FAILURE_RELATIONSHIPS = Set.of(SUCCESS, PARSE_FAILURE);

    private volatile Charset headerEncoding;
    private volatile Pattern headerNamePattern;
    private volatile KeyEncoding keyEncoding;
    private volatile OutputStrategy outputStrategy;
    private volatile KeyFormat keyFormat;
    private volatile boolean commitOffsets;
    private volatile boolean useReader;
    private volatile PollingContext pollingContext;

    private final Queue<KafkaConsumerService> consumerServices = new LinkedBlockingQueue<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return useReader ? SUCCESS_FAILURE_RELATIONSHIPS : SUCCESS_RELATIONSHIP;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(RECORD_READER)) {
            useReader = newValue != null;
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final KafkaConnectionService connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);
        pollingContext = createPollingContext(context);
        headerEncoding = Charset.forName(context.getProperty(HEADER_ENCODING).getValue());

        final String headerNamePatternProperty = context.getProperty(HEADER_NAME_PATTERN).getValue();
        if (StringUtils.isNotBlank(headerNamePatternProperty)) {
            headerNamePattern = Pattern.compile(headerNamePatternProperty);
        } else {
            headerNamePattern = null;
        }

        keyEncoding = context.getProperty(KEY_ATTRIBUTE_ENCODING).asAllowableValue(KeyEncoding.class);
        commitOffsets = context.getProperty(COMMIT_OFFSETS).asBoolean();
        outputStrategy = context.getProperty(OUTPUT_STRATEGY).asAllowableValue(OutputStrategy.class);
        keyFormat = context.getProperty(KEY_FORMAT).asAllowableValue(KeyFormat.class);
    }

    @OnStopped
    public void onStopped() {
        // Ensure that we close all Producer services when stopped
        KafkaConsumerService service;

        while ((service = consumerServices.poll()) != null) {
            try {
                service.close();
            } catch (IOException e) {
                getLogger().warn("Failed to close Kafka Consumer Service", e);
            }
        }
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final KafkaConsumerService consumerService = getConsumerService(context);

        try {
            final Iterator<ByteRecord> consumerRecords = consumerService.poll().iterator();
            if (!consumerRecords.hasNext()) {
                getLogger().debug("No Kafka Records consumed: {}", pollingContext);
                return;
            }

            processConsumerRecords(context, session, consumerService, pollingContext, consumerRecords);
        } catch (final Exception e) {
            getLogger().error("Failed to consume Kafka Records", e);
            consumerService.rollback();

            try {
                consumerService.close();
            } catch (IOException ex) {
                getLogger().warn("Failed to close Kafka Consumer Service", ex);
            }
        } finally {
            if (!consumerService.isClosed()) {
                consumerServices.offer(consumerService);
            }
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> verificationResults = new ArrayList<>();

        final KafkaConnectionService connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);
        final PollingContext pollingContext = createPollingContext(context);
        final KafkaConsumerService consumerService = connectionService.getConsumerService(pollingContext);

        final ConfigVerificationResult.Builder verificationPartitions = new ConfigVerificationResult.Builder()
                .verificationStepName("Verify Topic Partitions");

        try {
            final List<PartitionState> partitionStates = consumerService.getPartitionStates();
            verificationPartitions
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .explanation(String.format("Partitions [%d] found for Topics %s", partitionStates.size(), pollingContext.getTopics()));
        } catch (final Exception e) {
            getLogger().error("Topics {} Partition verification failed", pollingContext.getTopics(), e);
            verificationPartitions
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation(String.format("Topics %s Partition access failed: %s", pollingContext.getTopics(), e));
        }
        verificationResults.add(verificationPartitions.build());

        return verificationResults;
    }

    private KafkaConsumerService getConsumerService(final ProcessContext context) {
        final KafkaConsumerService consumerService = consumerServices.poll();
        if (consumerService != null) {
            return consumerService;
        }

        final KafkaConnectionService connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);
        return connectionService.getConsumerService(pollingContext);
    }

    private void processConsumerRecords(final ProcessContext context, final ProcessSession session, final KafkaConsumerService consumerService,
                final PollingContext pollingContext, final Iterator<ByteRecord> consumerRecords) {

        final ProcessingStrategy processingStrategy = ProcessingStrategy.valueOf(context.getProperty(PROCESSING_STRATEGY).getValue());

        switch (processingStrategy) {
            case RECORD -> processInputRecords(context, session, consumerService, pollingContext, consumerRecords);
            case FLOW_FILE -> processInputFlowFile(session, consumerService, pollingContext, consumerRecords);
            case DEMARCATOR -> {
                final Iterator<ByteRecord> demarcatedRecords = transformDemarcator(context, consumerRecords);
                processInputFlowFile(session, consumerService, pollingContext, demarcatedRecords);
            }
        }

    }

    private Iterator<ByteRecord> transformDemarcator(final ProcessContext context, final Iterator<ByteRecord> consumerRecords) {
        final String demarcatorValue = context.getProperty(ConsumeKafka.MESSAGE_DEMARCATOR).getValue();
        if (demarcatorValue == null) {
            return consumerRecords;
        }

        final byte[] demarcator = demarcatorValue.getBytes(StandardCharsets.UTF_8);
        final boolean separateByKey = context.getProperty(SEPARATE_BY_KEY).asBoolean();
        return new ByteRecordBundler(demarcator, separateByKey, keyEncoding, headerNamePattern, headerEncoding, commitOffsets).bundle(consumerRecords);
    }

    private void processInputRecords(final ProcessContext context, final ProcessSession session, final KafkaConsumerService consumerService,
                final PollingContext pollingContext, final Iterator<ByteRecord> consumerRecords) {

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final OffsetTracker offsetTracker = new OffsetTracker();
        final Runnable onSuccess = commitOffsets
            ? () -> session.commitAsync(() -> consumerService.commit(offsetTracker.getPollingSummary(pollingContext)))
            : session::commitAsync;

        final KafkaMessageConverter converter;
        if (OutputStrategy.USE_VALUE.equals(outputStrategy)) {
            converter = new RecordStreamKafkaMessageConverter(readerFactory, writerFactory,
                headerEncoding, headerNamePattern, keyEncoding, commitOffsets, offsetTracker, onSuccess, getLogger());
        } else if (OutputStrategy.USE_WRAPPER.equals(outputStrategy)) {
            final RecordReaderFactory keyReaderFactory = context.getProperty(KEY_RECORD_READER).asControllerService(RecordReaderFactory.class);
            converter = new WrapperRecordStreamKafkaMessageConverter(readerFactory, writerFactory, keyReaderFactory,
                headerEncoding, headerNamePattern, keyFormat, keyEncoding, commitOffsets, offsetTracker, onSuccess, getLogger());
        } else {
            throw new ProcessException(String.format("Output Strategy not supported [%s]", outputStrategy));
        }

        converter.toFlowFiles(session, consumerRecords);
    }

    private void processInputFlowFile(final ProcessSession session, final KafkaConsumerService consumerService, final PollingContext pollingContext, final Iterator<ByteRecord> consumerRecords) {
        final OffsetTracker offsetTracker = new OffsetTracker();
        final Runnable onSuccess = commitOffsets
                ? () -> session.commitAsync(() -> consumerService.commit(offsetTracker.getPollingSummary(pollingContext)))
                : session::commitAsync;
        final KafkaMessageConverter converter = new FlowFileStreamKafkaMessageConverter(
                headerEncoding, headerNamePattern, keyEncoding, commitOffsets, offsetTracker, onSuccess);
        converter.toFlowFiles(session, consumerRecords);
    }

    private PollingContext createPollingContext(final ProcessContext context) {
        final String groupId = context.getProperty(GROUP_ID).getValue();
        final String offsetReset = context.getProperty(AUTO_OFFSET_RESET).getValue();
        final AutoOffsetReset autoOffsetReset = AutoOffsetReset.valueOf(offsetReset.toUpperCase());
        final String topics = context.getProperty(TOPICS).evaluateAttributeExpressions().getValue();
        final String topicFormat = context.getProperty(TOPIC_FORMAT).getValue();
        final Duration maxUncommittedTime = context.getProperty(MAX_UNCOMMITTED_TIME).asDuration();

        final PollingContext pollingContext;
        if (topicFormat.equals(TOPIC_PATTERN.getValue())) {
            final Pattern topicPattern = Pattern.compile(topics.trim());
            pollingContext = new PollingContext(groupId, topicPattern, autoOffsetReset, maxUncommittedTime);
        } else if (topicFormat.equals(TOPIC_NAME.getValue())) {
            final Collection<String> topicList = KafkaUtils.toTopicList(topics);
            pollingContext = new PollingContext(groupId, topicList, autoOffsetReset, maxUncommittedTime);
        } else {
            throw new ProcessException(String.format("Topic Format [%s] not supported", topicFormat));
        }

        return pollingContext;
    }
}
