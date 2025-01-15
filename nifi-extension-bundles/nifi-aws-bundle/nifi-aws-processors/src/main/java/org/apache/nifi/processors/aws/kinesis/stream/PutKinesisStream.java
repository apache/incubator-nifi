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
package org.apache.nifi.processors.aws.kinesis.stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.kinesis.KinesisProcessorUtils;
import org.apache.nifi.processors.aws.v2.AbstractAwsSyncProcessor;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "kinesis", "put", "stream"})
@CapabilityDescription("Sends the contents to a specified Amazon Kinesis. "
    + "In order to send data to Kinesis, the stream name has to be specified.")
@WritesAttributes({
    @WritesAttribute(attribute = "aws.kinesis.error.message", description = "Error message on posting message to AWS Kinesis"),
    @WritesAttribute(attribute = "aws.kinesis.error.code", description = "Error code for the message when posting to AWS Kinesis"),
    @WritesAttribute(attribute = "aws.kinesis.sequence.number", description = "Sequence number for the message when posting to AWS Kinesis"),
    @WritesAttribute(attribute = "aws.kinesis.shard.id", description = "Shard id of the message posted to AWS Kinesis")})
@SeeAlso(ConsumeKinesisStream.class)
public class PutKinesisStream extends AbstractAwsSyncProcessor<KinesisClient, KinesisClientBuilder> {
    /**
     * Kinesis put record response error message
     */
    public static final String AWS_KINESIS_ERROR_MESSAGE = "aws.kinesis.error.message";

    /**
     * Kinesis put record response error code
     */
    public static final String AWS_KINESIS_ERROR_CODE = "aws.kinesis.error.code";

    public static final String AWS_KINESIS_SHARD_ID = "aws.kinesis.shard.id";

    public static final String AWS_KINESIS_SEQUENCE_NUMBER = "aws.kinesis.sequence.number";

    public static final PropertyDescriptor KINESIS_PARTITION_KEY = new PropertyDescriptor.Builder()
        .displayName("Amazon Kinesis Stream Partition Key")
        .name("amazon-kinesis-stream-partition-key")
        .description("The partition key attribute.  If it is not set, a random value is used")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${kinesis.partition.key}")
        .required(false)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .displayName("Message Batch Size")
            .name("message-batch-size")
            .description("Batch size for messages (1-500).")
            .defaultValue("250")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 500, true))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_MESSAGE_BUFFER_SIZE_MB = new PropertyDescriptor.Builder()
            .name("max-message-buffer-size")
            .displayName("Max message buffer size (MB)")
            .description("Max message buffer size in Mega-bytes")
            .defaultValue("1 MB")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .sensitive(false)
            .build();

    static final PropertyDescriptor KINESIS_STREAM_NAME = new PropertyDescriptor.Builder()
            .name("kinesis-stream-name")
            .displayName("Amazon Kinesis Stream Name")
            .description("The name of Kinesis Stream")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        KINESIS_STREAM_NAME,
        REGION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        KINESIS_PARTITION_KEY,
        BATCH_SIZE,
        MAX_MESSAGE_BUFFER_SIZE_MB,
        TIMEOUT,
        PROXY_CONFIGURATION_SERVICE,
        ENDPOINT_OVERRIDE);

    /** A random number generator for cases where partition key is not available */
    protected Random randomPartitionKeyGenerator = new Random();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final long maxBufferSizeBytes = context.getProperty(MAX_MESSAGE_BUFFER_SIZE_MB).asDataSize(DataUnit.B).longValue();

        final List<FlowFile> flowFiles = KinesisProcessorUtils.filterMessagesByMaxSize(session, batchSize, maxBufferSizeBytes, AWS_KINESIS_ERROR_MESSAGE, getLogger());

        final HashMap<String, List<FlowFile>> hashFlowFiles = new HashMap<>();
        final HashMap<String, List<PutRecordsRequestEntry>> recordHash = new HashMap<>();

        final KinesisClient client = getClient(context);

        try {

            final List<FlowFile> failedFlowFiles = new ArrayList<>();
            final List<FlowFile> successfulFlowFiles = new ArrayList<>();

            // Prepare batch of records
            for (final FlowFile flowFile : flowFiles) {
                final String streamName = context.getProperty(KINESIS_STREAM_NAME).evaluateAttributeExpressions(flowFile).getValue();

                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                session.exportTo(flowFile, baos);
                final PutRecordsRequestEntry.Builder recordBuilder = PutRecordsRequestEntry.builder().data(SdkBytes.fromByteArray(baos.toByteArray()));

                final String partitionKey = context.getProperty(KINESIS_PARTITION_KEY)
                        .evaluateAttributeExpressions(flowFile).getValue();

                recordBuilder.partitionKey(StringUtils.isBlank(partitionKey) ? Integer.toString(randomPartitionKeyGenerator.nextInt()) : partitionKey);

                hashFlowFiles.computeIfAbsent(streamName, key -> new ArrayList<>()).add(flowFile);
                recordHash.computeIfAbsent(streamName, key -> new ArrayList<>()).add(recordBuilder.build());
            }

            for (final Map.Entry<String, List<PutRecordsRequestEntry>> entryRecord : recordHash.entrySet()) {
                final String streamName = entryRecord.getKey();
                final List<PutRecordsRequestEntry> records = entryRecord.getValue();

                if (!records.isEmpty()) {
                    final PutRecordsRequest putRecordRequest = PutRecordsRequest.builder()
                            .streamName(streamName)
                            .records(records)
                            .build();
                    final PutRecordsResponse response = client.putRecords(putRecordRequest);

                    final List<PutRecordsResultEntry> responseEntries = response.records();
                    for (int i = 0; i < responseEntries.size(); i++) {
                        final PutRecordsResultEntry entry = responseEntries.get(i);
                        FlowFile flowFile = hashFlowFiles.get(streamName).get(i);

                        Map<String, String> attributes = new HashMap<>();
                        attributes.put(AWS_KINESIS_SHARD_ID, entry.shardId());
                        attributes.put(AWS_KINESIS_SEQUENCE_NUMBER, entry.sequenceNumber());

                        if (StringUtils.isNotBlank(entry.errorCode())) {
                            attributes.put(AWS_KINESIS_ERROR_CODE, entry.errorCode());
                            attributes.put(AWS_KINESIS_ERROR_MESSAGE, entry.errorMessage());
                            flowFile = session.putAllAttributes(flowFile, attributes);
                            failedFlowFiles.add(flowFile);
                        } else {
                            flowFile = session.putAllAttributes(flowFile, attributes);
                            successfulFlowFiles.add(flowFile);
                        }
                    }
                }
                recordHash.get(streamName).clear();
                records.clear();
            }

            if (!failedFlowFiles.isEmpty()) {
                session.transfer(failedFlowFiles, REL_FAILURE);
                getLogger().error("Failed to publish to kinesis records {}", failedFlowFiles);
            }
            if (!successfulFlowFiles.isEmpty()) {
                session.transfer(successfulFlowFiles, REL_SUCCESS);
                getLogger().debug("Successfully published to kinesis records {}", successfulFlowFiles);
            }

        } catch (final Exception exception) {
            getLogger().error("Failed to publish due to exception {} flowfiles {} ", exception, flowFiles);
            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
        }
    }

    @Override
    protected KinesisClientBuilder createClientBuilder(final ProcessContext context) {
        return KinesisClient.builder();
    }

}
