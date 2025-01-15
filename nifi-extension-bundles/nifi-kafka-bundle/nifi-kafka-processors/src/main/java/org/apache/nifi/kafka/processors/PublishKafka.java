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
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kafka.processors.producer.PartitionStrategy;
import org.apache.nifi.kafka.processors.producer.common.PublishKafkaUtil;
import org.apache.nifi.kafka.processors.producer.config.DeliveryGuarantee;
import org.apache.nifi.kafka.processors.producer.convert.DelimitedStreamKafkaRecordConverter;
import org.apache.nifi.kafka.processors.producer.convert.FlowFileStreamKafkaRecordConverter;
import org.apache.nifi.kafka.processors.producer.convert.KafkaRecordConverter;
import org.apache.nifi.kafka.processors.producer.convert.RecordStreamKafkaRecordConverter;
import org.apache.nifi.kafka.processors.producer.convert.RecordWrapperStreamKafkaRecordConverter;
import org.apache.nifi.kafka.processors.producer.header.AttributesHeadersFactory;
import org.apache.nifi.kafka.processors.producer.header.HeadersFactory;
import org.apache.nifi.kafka.processors.producer.key.AttributeKeyFactory;
import org.apache.nifi.kafka.processors.producer.key.KeyFactory;
import org.apache.nifi.kafka.processors.producer.key.MessageKeyFactory;
import org.apache.nifi.kafka.processors.producer.wrapper.RecordMetadataStrategy;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.service.api.common.PartitionState;
import org.apache.nifi.kafka.service.api.producer.FlowFileResult;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.ProducerConfiguration;
import org.apache.nifi.kafka.service.api.producer.PublishContext;
import org.apache.nifi.kafka.service.api.producer.RecordSummary;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.component.KafkaPublishComponent;
import org.apache.nifi.kafka.shared.property.FailureStrategy;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.kafka.shared.property.PublishStrategy;
import org.apache.nifi.kafka.shared.transaction.TransactionIdSupplier;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Tags({"Apache", "Kafka", "Record", "csv", "json", "avro", "logs", "Put", "Send", "Message", "PubSub"})
@CapabilityDescription("Sends the contents of a FlowFile as either a message or as individual records to Apache Kafka using the Kafka Producer API. "
        + "The messages to send may be individual FlowFiles, may be delimited using a "
        + "user-specified delimiter (such as a new-line), or "
        + "may be record-oriented data that can be read by the configured Record Reader. "
        + "The complementary NiFi processor for fetching messages is ConsumeKafka. "
        + "To produce a kafka tombstone message while using PublishStrategy.USE_WRAPPER, simply set the value of a record to 'null'.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttribute(attribute = KafkaFlowFileAttribute.KAFKA_TOMBSTONE, description = "If this attribute is set to 'true', if the processor is not configured "
        + "with a demarcator and if the FlowFile's content is null, then a tombstone message with zero bytes will be sent to Kafka.")
@WritesAttribute(attribute = "msg.count", description = "The number of messages that were sent to Kafka for this FlowFile. This attribute is added only to "
        + "FlowFiles that are routed to success.")
@SeeAlso({ConsumeKafka.class})
public class PublishKafka extends AbstractProcessor implements KafkaPublishComponent, VerifiableProcessor {
    protected static final String MSG_COUNT = "msg.count";

    public static final PropertyDescriptor CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("Kafka Connection Service")
            .description("Provides connections to Kafka Broker for publishing Kafka Records")
            .identifiesControllerService(KafkaConnectionService.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    public static final PropertyDescriptor TOPIC_NAME = new PropertyDescriptor.Builder()
            .name("Topic Name")
            .description("Name of the Kafka Topic to which the Processor publishes Kafka Records")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor DELIVERY_GUARANTEE = new PropertyDescriptor.Builder()
            .name("acks")
            .displayName("Delivery Guarantee")
            .description("Specifies the requirement for guaranteeing that a message is sent to Kafka. Corresponds to Kafka Client acks property.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(DeliveryGuarantee.class)
            .defaultValue(DeliveryGuarantee.DELIVERY_REPLICATED)
            .build();

    static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder()
            .name("compression.type")
            .displayName("Compression Type")
            .description("Specifies the compression strategy for records sent to Kafka. Corresponds to Kafka Client compression.type property.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("none", "gzip", "snappy", "lz4")
            .defaultValue("none")
            .build();

    public static final PropertyDescriptor MAX_REQUEST_SIZE = new PropertyDescriptor.Builder()
            .name("max.request.size")
            .displayName("Max Request Size")
            .description("The maximum size of a request in bytes. Corresponds to Kafka Client max.request.size property.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();

    public static final PropertyDescriptor TRANSACTIONS_ENABLED = new PropertyDescriptor.Builder()
            .name("Transactions Enabled")
            .description("Specifies whether to provide transactional guarantees when communicating with Kafka. If there is a problem sending data to Kafka, "
                    + "and this property is set to false, then the messages that have already been sent to Kafka will continue on and be delivered to consumers. "
                    + "If this is set to true, then the Kafka transaction will be rolled back so that those messages are not available to consumers. Setting this to true "
                    + "requires that the [Delivery Guarantee] property be set to [Guarantee Replicated Delivery.]")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    static final PropertyDescriptor TRANSACTIONAL_ID_PREFIX = new PropertyDescriptor.Builder()
            .name("Transactional ID Prefix")
            .description("Specifies the KafkaProducer config transactional.id will be a generated UUID and will be prefixed with the configured string.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .dependsOn(TRANSACTIONS_ENABLED, "true")
            .required(false)
            .build();

    static final PropertyDescriptor PARTITION_CLASS = new PropertyDescriptor.Builder()
            .name("partitioner.class")
            .displayName("Partitioner Class")
            .description("Specifies which class to use to compute a partition id for a message. Corresponds to Kafka Client partitioner.class property.")
            .allowableValues(PartitionStrategy.class)
            .defaultValue(PartitionStrategy.RANDOM_PARTITIONING.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor PARTITION = new PropertyDescriptor.Builder()
            .name("partition")
            .displayName("Partition")
            .description("Specifies the Kafka Partition destination for Records.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("Message Demarcator")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages within "
                    + "a single FlowFile. If not specified, the entire content of the FlowFile will be used as a single message. If specified, the "
                    + "contents of the FlowFile will be split on this delimiter and each section sent as a separate Kafka message. "
                    + "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter, depending on your OS.")
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("The Record Writer to use in order to serialize the data before sending to Kafka")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor PUBLISH_STRATEGY = new PropertyDescriptor.Builder()
            .name("Publish Strategy")
            .description("The format used to publish the incoming FlowFile record to Kafka.")
            .required(true)
            .defaultValue(PublishStrategy.USE_VALUE)
            .dependsOn(RECORD_READER)
            .allowableValues(PublishStrategy.class)
            .build();

    public static final PropertyDescriptor MESSAGE_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("Message Key Field")
            .description("The name of a field in the Input Records that should be used as the Key for the Kafka message.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(PUBLISH_STRATEGY, PublishStrategy.USE_VALUE)
            .required(false)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_HEADER_PATTERN = new PropertyDescriptor.Builder()
            .name("FlowFile Attribute Header Pattern")
            .description("A Regular Expression that is matched against all FlowFile attribute names. "
                    + "Any attribute whose name matches the pattern will be added to the Kafka messages as a Header. "
                    + "If not specified, no FlowFile attributes will be added as headers.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(PUBLISH_STRATEGY, PublishStrategy.USE_VALUE)
            .required(false)
            .build();

    static final PropertyDescriptor HEADER_ENCODING = new PropertyDescriptor.Builder()
            .name("Header Encoding")
            .description("For any attribute that is added as a Kafka Record Header, this property indicates the Character Encoding to use for serializing the headers.")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(StandardCharsets.UTF_8.displayName())
            .required(true)
            .dependsOn(ATTRIBUTE_HEADER_PATTERN)
            .build();

    static final PropertyDescriptor KAFKA_KEY = new PropertyDescriptor.Builder()
            .name("Kafka Key")
            .description("The Key to use for the Message. "
                    + "If not specified, the FlowFile attribute 'kafka.key' is used as the message key, if it is present."
                    + "Beware that setting Kafka key and demarcating at the same time may potentially lead to many Kafka messages with the same key."
                    + "Normally this is not a problem as Kafka does not enforce or assume message and key uniqueness. Still, setting the demarcator and Kafka key at the same time poses a risk of "
                    + "data loss on Kafka. During a topic compaction on Kafka, messages will be deduplicated based on this key.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor KEY_ATTRIBUTE_ENCODING = new PropertyDescriptor.Builder()
            .name("Kafka Key Attribute Encoding")
            .description("FlowFiles that are emitted have an attribute named '" + KafkaFlowFileAttribute.KAFKA_KEY + "'. This property dictates how the value of the attribute should be encoded.")
            .required(true)
            .defaultValue(KeyEncoding.UTF8.getValue())
            .allowableValues(KeyEncoding.class)
            .dependsOn(PUBLISH_STRATEGY, PublishStrategy.USE_WRAPPER)
            .build();

    static final PropertyDescriptor RECORD_KEY_WRITER = new PropertyDescriptor.Builder()
            .name("Record Key Writer")
            .description("The Record Key Writer to use for outgoing FlowFiles")
            .required(false)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .dependsOn(PUBLISH_STRATEGY, PublishStrategy.USE_WRAPPER)
            .build();

    public static final PropertyDescriptor RECORD_METADATA_STRATEGY = new PropertyDescriptor.Builder()
            .name("Record Metadata Strategy")
            .description("Specifies whether the Record's metadata (topic and partition) should come from the Record's metadata field or if it should come from the configured " +
                    "Topic Name and Partition / Partitioner class properties")
            .required(true)
            .defaultValue(RecordMetadataStrategy.FROM_PROPERTIES)
            .allowableValues(RecordMetadataStrategy.class)
            .dependsOn(PUBLISH_STRATEGY, PublishStrategy.USE_WRAPPER)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was sent to Kafka.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to Kafka will be routed to this Relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_SERVICE,
            TOPIC_NAME,
            FAILURE_STRATEGY,
            DELIVERY_GUARANTEE,
            COMPRESSION_CODEC,
            MAX_REQUEST_SIZE,
            TRANSACTIONS_ENABLED,
            TRANSACTIONAL_ID_PREFIX,
            PARTITION_CLASS,
            PARTITION,
            MESSAGE_DEMARCATOR,
            RECORD_READER,
            RECORD_WRITER,
            PUBLISH_STRATEGY,
            MESSAGE_KEY_FIELD,
            ATTRIBUTE_HEADER_PATTERN,
            HEADER_ENCODING,
            KAFKA_KEY,
            KEY_ATTRIBUTE_ENCODING,
            RECORD_KEY_WRITER,
            RECORD_METADATA_STRATEGY
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private final Queue<KafkaProducerService> producerServices = new LinkedBlockingQueue<>();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger,
                                                 final Map<String, String> attributes) {
        final List<ConfigVerificationResult> verificationResults = new ArrayList<>();

        final KafkaConnectionService connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);

        final boolean transactionsEnabled = context.getProperty(TRANSACTIONS_ENABLED).asBoolean();
        final String transactionalIdPrefix = context.getProperty(TRANSACTIONAL_ID_PREFIX).evaluateAttributeExpressions().getValue();
        final Supplier<String> transactionalIdSupplier = new TransactionIdSupplier(transactionalIdPrefix);
        final String deliveryGuarantee = context.getProperty(DELIVERY_GUARANTEE).getValue();
        final String compressionCodec = context.getProperty(COMPRESSION_CODEC).getValue();
        final String partitionClass = context.getProperty(PARTITION_CLASS).getValue();
        final ProducerConfiguration producerConfiguration = new ProducerConfiguration(
                transactionsEnabled, transactionalIdSupplier.get(), deliveryGuarantee, compressionCodec, partitionClass);

        try (final KafkaProducerService producerService = connectionService.getProducerService(producerConfiguration)) {
            final ConfigVerificationResult.Builder verificationPartitions = new ConfigVerificationResult.Builder()
                .verificationStepName("Verify Topic Partitions");

            final String topicName = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions(attributes).getValue();
            try {
                final List<PartitionState> partitionStates = producerService.getPartitionStates(topicName);

                verificationPartitions
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .explanation(String.format("Partitions [%d] found for Topic [%s]", partitionStates.size(), topicName));
            } catch (final Exception e) {
                getLogger().error("Topic [%s] Partition verification failed", topicName, e);
                verificationPartitions
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation(String.format("Topic [%s] Partition access failed: %s", topicName, e));
            }
            verificationResults.add(verificationPartitions.build());

            return verificationResults;
        }
    }


    @OnStopped
    public void onStopped() {
        // Ensure that we close all Producer services when stopped
        KafkaProducerService service;

        while ((service = producerServices.poll()) != null) {
            service.close();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = PublishKafkaUtil.pollFlowFiles(session);
        if (flowFiles.isEmpty()) {
            return;
        }

        final KafkaProducerService producerService = getProducerService(context);
        try {
            publishFlowFiles(context, session, flowFiles, producerService);
        } catch (final Exception e) {
            final String uuids = flowFiles.stream()
                .map(ff -> ff.getAttribute(CoreAttributes.UUID.key()))
                .collect(Collectors.joining(", "));

            getLogger().error("Failed to publish {} FlowFiles to Kafka: uuids={}", flowFiles.size(), uuids, e);
            producerService.close();
        } finally {
            if (!producerService.isClosed()) {
                producerServices.offer(producerService);
            }
        }
    }

    private KafkaProducerService getProducerService(final ProcessContext context) {
        final KafkaProducerService producerService = producerServices.poll();
        if (producerService != null) {
            return producerService;
        }

        return createProducerService(context);
    }

    private KafkaProducerService createProducerService(final ProcessContext context) {
        final KafkaConnectionService connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(KafkaConnectionService.class);

        final boolean transactionsEnabled = context.getProperty(TRANSACTIONS_ENABLED).asBoolean();
        final String transactionalIdPrefix = context.getProperty(TRANSACTIONAL_ID_PREFIX).evaluateAttributeExpressions().getValue();
        final String deliveryGuarantee = context.getProperty(DELIVERY_GUARANTEE).getValue();
        final String compressionCodec = context.getProperty(COMPRESSION_CODEC).getValue();
        final String partitionClass = context.getProperty(PARTITION_CLASS).getValue();
        final ProducerConfiguration producerConfiguration = new ProducerConfiguration(
            transactionsEnabled, transactionalIdPrefix, deliveryGuarantee, compressionCodec, partitionClass);

        return connectionService.getProducerService(producerConfiguration);
    }


    private void publishFlowFiles(final ProcessContext context, final ProcessSession session,
                                  final List<FlowFile> flowFiles, final KafkaProducerService producerService) {

        // Publish all FlowFiles and ensure that we call complete() on the producer and route flowfiles as appropriate, regardless
        // of the outcome. If there are failures, the complete() method will abort the transaction (if transactions are enabled).
        // Otherwise, it will commit the transaction (if transactions are enabled). We then route the FlowFiles based on the results.
        try {
            for (final FlowFile flowFile : flowFiles) {
                publishFlowFile(context, session, flowFile, producerService);
            }
        } finally {
            RecordSummary recordSummary = null;
            try {
                recordSummary = producerService.complete();
            } catch (final Exception e) {
                getLogger().warn("Failed to complete transaction with Kafka", e);
                producerService.close();
            }

            if (recordSummary == null || recordSummary.isFailure()) {
                routeFailureStrategy(context, session, flowFiles);
            } else {
                routeResults(session, recordSummary.getFlowFileResults());
            }
        }
    }

    private void routeFailureStrategy(final ProcessContext context, final ProcessSession session, final List<FlowFile> flowFiles) {
        final FailureStrategy strategy = context.getProperty(FAILURE_STRATEGY).asAllowableValue(FailureStrategy.class);
        if (FailureStrategy.ROLLBACK == strategy) {
            session.rollback();
            context.yield();
        } else {
            session.transfer(flowFiles, REL_FAILURE);
        }
    }

    private void routeResults(final ProcessSession session, final List<FlowFileResult> flowFileResults) {
        for (final FlowFileResult flowFileResult : flowFileResults) {
            final long msgCount = flowFileResult.getSentCount();
            final FlowFile flowFile = session.putAttribute(flowFileResult.getFlowFile(), MSG_COUNT, String.valueOf(msgCount));
            session.adjustCounter("Messages Sent", msgCount, true);

            for (final Map.Entry<String, Long> entry : flowFileResult.getSentPerTopic().entrySet()) {
                session.adjustCounter("Messages Sent to " + entry.getKey(), entry.getValue(), true);
            }

            final Relationship relationship = flowFileResult.getExceptions().isEmpty() ? REL_SUCCESS : REL_FAILURE;
            session.transfer(flowFile, relationship);
            final String topicList = String.join(",", flowFileResult.getSentPerTopic().keySet());
            session.getProvenanceReporter().send(flowFile, "kafka://" + topicList);
        }
    }

    private void publishFlowFile(final ProcessContext context, final ProcessSession session,
                                 final FlowFile flowFile, final KafkaProducerService producerService) {

        final String topic = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions(flowFile.getAttributes()).getValue();
        final Integer partition = getPartition(context, flowFile);
        final PublishContext publishContext = new PublishContext(topic, partition, null, flowFile);

        final KafkaRecordConverter kafkaRecordConverter = getKafkaRecordConverter(context, flowFile);
        final PublishCallback callback = new PublishCallback(producerService, publishContext, kafkaRecordConverter, flowFile.getAttributes(), flowFile.getSize());

        session.read(flowFile, callback);
    }

    private Integer getPartition(final ProcessContext context, final FlowFile flowFile) {
        final String partitionClass = context.getProperty(PARTITION_CLASS).getValue();

        if (PartitionStrategy.EXPRESSION_LANGUAGE_PARTITIONING.getValue().equals(partitionClass)) {
            final String partition = context.getProperty(PARTITION).evaluateAttributeExpressions(flowFile).getValue();
            return Objects.hashCode(partition);
        }

        return null;
    }

    private KafkaRecordConverter getKafkaRecordConverter(final ProcessContext context, final FlowFile flowFile) {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final String attributeHeaderPatternProperty = context.getProperty(ATTRIBUTE_HEADER_PATTERN).getValue();
        final Pattern attributeHeaderPattern = (attributeHeaderPatternProperty == null) ? null : Pattern.compile(attributeHeaderPatternProperty);
        final String headerEncoding = context.getProperty(HEADER_ENCODING).evaluateAttributeExpressions().getValue();
        final Charset headerEncodingCharacterSet = Charset.forName(headerEncoding);
        final HeadersFactory headersFactory = new AttributesHeadersFactory(attributeHeaderPattern, headerEncodingCharacterSet);

        final int maxMessageSize = context.getProperty(MAX_REQUEST_SIZE).asDataSize(DataUnit.B).intValue();

        final RecordSetWriterFactory keyWriterFactory = context.getProperty(RECORD_KEY_WRITER).asControllerService(RecordSetWriterFactory.class);
        final PublishStrategy publishStrategy = PublishStrategy.valueOf(context.getProperty(PUBLISH_STRATEGY).getValue());
        final String kafkaKeyAttribute = context.getProperty(KAFKA_KEY).getValue();
        final String keyAttributeEncoding = context.getProperty(KEY_ATTRIBUTE_ENCODING).getValue();
        final String messageKeyField = context.getProperty(MESSAGE_KEY_FIELD).evaluateAttributeExpressions(flowFile).getValue();
        final KeyFactory keyFactory = ((PublishStrategy.USE_VALUE == publishStrategy) && (messageKeyField != null))
                ? new MessageKeyFactory(flowFile, messageKeyField, keyWriterFactory, getLogger())
                : new AttributeKeyFactory(kafkaKeyAttribute, keyAttributeEncoding);

        if (readerFactory != null && writerFactory != null) {
            final RecordMetadataStrategy metadataStrategy = RecordMetadataStrategy.valueOf(context.getProperty(RECORD_METADATA_STRATEGY).getValue());
            if (publishStrategy == PublishStrategy.USE_WRAPPER) {
                return new RecordWrapperStreamKafkaRecordConverter(flowFile, metadataStrategy, readerFactory, writerFactory, keyWriterFactory, maxMessageSize, getLogger());
            } else {
                return new RecordStreamKafkaRecordConverter(readerFactory, writerFactory, headersFactory, keyFactory, maxMessageSize, getLogger());
            }
        }

        final PropertyValue demarcatorValue = context.getProperty(MESSAGE_DEMARCATOR);
        if (demarcatorValue.isSet()) {
            final String demarcator = demarcatorValue.evaluateAttributeExpressions(flowFile).getValue();
            return new DelimitedStreamKafkaRecordConverter(demarcator.getBytes(StandardCharsets.UTF_8), maxMessageSize, headersFactory);
        }

        return new FlowFileStreamKafkaRecordConverter(maxMessageSize, headersFactory, keyFactory);
    }


    private static class PublishCallback implements InputStreamCallback {
        private final KafkaProducerService producerService;
        private final PublishContext publishContext;
        private final KafkaRecordConverter kafkaConverter;
        private final Map<String, String> attributes;
        private final long inputLength;

        public PublishCallback(
                final KafkaProducerService producerService,
                final PublishContext publishContext,
                final KafkaRecordConverter kafkaConverter,
                final Map<String, String> attributes,
                final long inputLength) {

            this.producerService = producerService;
            this.publishContext = publishContext;
            this.kafkaConverter = kafkaConverter;
            this.attributes = attributes;
            this.inputLength = inputLength;
        }

        @Override
        public void process(final InputStream in) {
            try (final InputStream is = new BufferedInputStream(in)) {
                final Iterator<KafkaRecord> records = kafkaConverter.convert(attributes, is, inputLength);
                producerService.send(records, publishContext);
            } catch (final Exception e) {
                publishContext.setException(e); // on data pre-process failure, indicate this to controller service
                producerService.send(Collections.emptyIterator(), publishContext);
            }
        }
    }
}
