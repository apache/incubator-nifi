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
package org.apache.nifi.processors.aws.kinesis.firehose;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
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
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.FirehoseClientBuilder;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponseEntry;
import software.amazon.awssdk.services.firehose.model.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "firehose", "kinesis", "put", "stream"})
@CapabilityDescription("Sends the contents to a specified Amazon Kinesis Firehose. "
    + "In order to send data to firehose, the firehose delivery stream name has to be specified.")
@WritesAttributes({
    @WritesAttribute(attribute = "aws.kinesis.firehose.error.message", description = "Error message on posting message to AWS Kinesis Firehose"),
    @WritesAttribute(attribute = "aws.kinesis.firehose.error.code", description = "Error code for the message when posting to AWS Kinesis Firehose"),
    @WritesAttribute(attribute = "aws.kinesis.firehose.record.id", description = "Record id of the message posted to Kinesis Firehose")})
public class PutKinesisFirehose extends AbstractAwsSyncProcessor<FirehoseClient, FirehoseClientBuilder> {

    public static final String AWS_KINESIS_FIREHOSE_ERROR_MESSAGE = "aws.kinesis.firehose.error.message";
    public static final String AWS_KINESIS_FIREHOSE_ERROR_CODE = "aws.kinesis.firehose.error.code";
    public static final String AWS_KINESIS_FIREHOSE_RECORD_ID = "aws.kinesis.firehose.record.id";

    public static final PropertyDescriptor KINESIS_FIREHOSE_DELIVERY_STREAM_NAME = new PropertyDescriptor.Builder()
            .name("Amazon Kinesis Firehose Delivery Stream Name")
            .description("The name of kinesis firehose delivery stream")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("Batch size for messages (1-500).")
            .defaultValue("250")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 500, true))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_MESSAGE_BUFFER_SIZE_MB = new PropertyDescriptor.Builder()
            .name("Max message buffer size")
            .description("Max message buffer")
            .defaultValue("1 MB")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .sensitive(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        KINESIS_FIREHOSE_DELIVERY_STREAM_NAME,
        BATCH_SIZE,
        REGION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        MAX_MESSAGE_BUFFER_SIZE_MB,
        TIMEOUT,
        PROXY_CONFIGURATION_SERVICE,
        ENDPOINT_OVERRIDE);

    public static final int MAX_MESSAGE_SIZE = KinesisProcessorUtils.MAX_MESSAGE_SIZE;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected FirehoseClientBuilder createClientBuilder(final ProcessContext context) {
        return FirehoseClient.builder();
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final long maxBufferSizeBytes = context.getProperty(MAX_MESSAGE_BUFFER_SIZE_MB).asDataSize(DataUnit.B).longValue();

        final List<FlowFile> flowFiles = KinesisProcessorUtils.filterMessagesByMaxSize(session, batchSize, maxBufferSizeBytes, AWS_KINESIS_FIREHOSE_ERROR_MESSAGE, getLogger());
        final Map<String, List<FlowFile>> hashFlowFiles = new HashMap<>();
        final Map<String, List<Record>> recordHash = new HashMap<>();

        final FirehoseClient client = getClient(context);

        try {
            final List<FlowFile> failedFlowFiles = new ArrayList<>();
            final List<FlowFile> successfulFlowFiles = new ArrayList<>();

            // Prepare batch of records
            for (final FlowFile flowFile : flowFiles) {
                final String firehoseStreamName = context.getProperty(KINESIS_FIREHOSE_DELIVERY_STREAM_NAME).evaluateAttributeExpressions(flowFile).getValue();

                recordHash.computeIfAbsent(firehoseStreamName, k -> new ArrayList<>());
                session.read(flowFile, in -> recordHash.get(firehoseStreamName).add(Record.builder().data(SdkBytes.fromInputStream(in)).build()));

                final List<FlowFile> flowFilesForStream = hashFlowFiles.computeIfAbsent(firehoseStreamName, k -> new ArrayList<>());
                flowFilesForStream.add(flowFile);
            }

            for (final Map.Entry<String, List<Record>> entry : recordHash.entrySet()) {
                final String streamName = entry.getKey();
                final List<Record> records = entry.getValue();

                if (records.size() > 0) {
                    // Send the batch
                    final PutRecordBatchRequest putRecordBatchRequest = PutRecordBatchRequest.builder()
                            .deliveryStreamName(streamName)
                            .records(records)
                            .build();
                    final PutRecordBatchResponse response = client.putRecordBatch(putRecordBatchRequest);

                    // Separate out the successful and failed flow files
                    final List<PutRecordBatchResponseEntry> responseEntries = response.requestResponses();
                    for (int i = 0; i < responseEntries.size(); i++ ) {

                        final PutRecordBatchResponseEntry responseEntry = responseEntries.get(i);
                        FlowFile flowFile = hashFlowFiles.get(streamName).get(i);

                        final Map<String, String> attributes = new HashMap<>();
                        attributes.put(AWS_KINESIS_FIREHOSE_RECORD_ID, responseEntry.recordId());
                        flowFile = session.putAttribute(flowFile, AWS_KINESIS_FIREHOSE_RECORD_ID, responseEntry.recordId());
                        if (StringUtils.isNotBlank(responseEntry.errorCode())) {
                            attributes.put(AWS_KINESIS_FIREHOSE_ERROR_CODE, responseEntry.errorCode());
                            attributes.put(AWS_KINESIS_FIREHOSE_ERROR_MESSAGE, responseEntry.errorMessage());
                            flowFile = session.putAllAttributes(flowFile, attributes);
                            failedFlowFiles.add(flowFile);
                        } else {
                            flowFile = session.putAllAttributes(flowFile, attributes);
                            successfulFlowFiles.add(flowFile);
                        }
                    }
                    recordHash.get(streamName).clear();
                    records.clear();
                }
            }

            if (failedFlowFiles.size() > 0) {
                session.transfer(failedFlowFiles, REL_FAILURE);
                getLogger().error("Failed to publish to kinesis firehose {}", failedFlowFiles);
            }
            if (successfulFlowFiles.size() > 0) {
                session.transfer(successfulFlowFiles, REL_SUCCESS);
                getLogger().info("Successfully published to kinesis firehose {}", successfulFlowFiles);
            }
        } catch (final Exception exception) {
            getLogger().error("Failed to publish to kinesis firehose {} with exception {}", flowFiles, exception);
            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
        }
    }

}
