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
package org.apache.nifi.processors.aws.dynamodb;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.SystemResourceConsiderations;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SplitRecordSetHandler;
import org.apache.nifi.serialization.SplitRecordSetHandlerException;
import org.apache.nifi.serialization.record.Record;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.utils.CollectionUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@SeeAlso({DeleteDynamoDB.class, GetDynamoDB.class, PutDynamoDB.class})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "DynamoDB", "AWS", "Put", "Insert", "Record"})
@CapabilityDescription(
        "Inserts items into DynamoDB based on record-oriented data. " +
        "The record fields are mapped into DynamoDB item fields, including partition and sort keys if set. " +
        "Depending on the number of records the processor might execute the insert in multiple chunks in order to overcome DynamoDB's limitation on batch writing. " +
        "This might result partially processed FlowFiles in which case the FlowFile will be transferred to the \"unprocessed\" relationship " +
        "with the necessary attribute to retry later without duplicating the already executed inserts."
)
@WritesAttributes({
        @WritesAttribute(attribute = PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE, description = "Number of chunks successfully inserted into DynamoDB. If not set, it is considered as 0"),
        @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_UNPROCESSED, description = "DynamoDB unprocessed keys"),
        @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR, description = "DynamoDB range key error"),
        @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_NOT_FOUND, description = "DynamoDB key not found"),
        @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE, description = "DynamoDB exception message"),
        @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_CODE, description = "DynamoDB error code"),
        @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_MESSAGE, description = "DynamoDB error message"),
        @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_SERVICE, description = "DynamoDB error service"),
        @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_RETRYABLE, description = "DynamoDB error is retryable"),
        @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_REQUEST_ID, description = "DynamoDB error request id"),
        @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_STATUS_CODE, description = "DynamoDB error status code"),
        @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ITEM_IO_ERROR, description = "IO exception message on creating item")
})
@ReadsAttribute(attribute = PutDynamoDBRecord.DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE, description = "Number of chunks successfully inserted into DynamoDB. If not set, it is considered as 0")
@SystemResourceConsiderations({
        @SystemResourceConsideration(resource = SystemResource.MEMORY),
        @SystemResourceConsideration(resource = SystemResource.NETWORK)
})
public class PutDynamoDBRecord extends AbstractDynamoDBProcessor {

    // Due to DynamoDB's hardcoded limitation on the number of items in one batch, the processor writes them in chunks.
    // Every chunk contains a number of items according to the limitations.
    private static final int MAXIMUM_CHUNK_SIZE = 25;

    static final String DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE = "dynamodb.chunks.processed";

    static final AllowableValue PARTITION_BY_FIELD = new AllowableValue("ByField", "Partition By Field",
            "Uses the value of the Record field identified by the \"Partition Key Field\" property as partition key value.");
    static final AllowableValue PARTITION_BY_ATTRIBUTE = new AllowableValue("ByAttribute", "Partition By Attribute",
            "Uses an incoming FlowFile attribute identified by \"Partition Key Attribute\" as the value of the partition key. " +
            "The incoming Records must not contain field with the same name defined by the \"Partition Key Field\".");
    static final AllowableValue PARTITION_GENERATED = new AllowableValue("Generated", "Generated UUID",
            "Uses a generated UUID as value for the partition key. The incoming Records must not contain field with the same name defined by the \"Partition Key Field\".");

    static final AllowableValue SORT_NONE = new AllowableValue("None", "None",
            "The processor will not assign sort key to the inserted Items.");
    static final AllowableValue SORT_BY_FIELD = new AllowableValue("ByField", "Sort By Field",
            "Uses the value of the Record field identified by the \"Sort Key Field\" property as sort key value.");
    static final AllowableValue SORT_BY_SEQUENCE = new AllowableValue("BySequence", "Generate Sequence",
            "The processor will assign a number for every item based on the original record's position in the incoming FlowFile. This will be used as sort key value.");

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor PARTITION_KEY_STRATEGY = new PropertyDescriptor.Builder()
            .name("partition-key-strategy")
            .displayName("Partition Key Strategy")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(PARTITION_BY_FIELD, PARTITION_BY_ATTRIBUTE, PARTITION_GENERATED)
            .defaultValue(PARTITION_BY_FIELD.getValue())
            .description("Defines the strategy the processor uses to assign partition key value to the inserted Items.")
            .build();

    static final PropertyDescriptor PARTITION_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("partition-key-field")
            .displayName("Partition Key Field")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description(
                    "Defines the name of the partition key field in the DynamoDB table. Partition key is also known as hash key. " +
                    "Depending on the \"Partition Key Strategy\" the field value might come from the incoming Record or a generated one.")
            .build();

    static final PropertyDescriptor PARTITION_KEY_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("partition-key-attribute")
            .displayName("Partition Key Attribute")
            .required(true)
            .dependsOn(PARTITION_KEY_STRATEGY, PARTITION_BY_ATTRIBUTE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Specifies the FlowFile attribute that will be used as the value of the partition key when using \"Partition by attribute\" partition key strategy.")
            .build();

    static final PropertyDescriptor SORT_KEY_STRATEGY = new PropertyDescriptor.Builder()
            .name("sort-key-strategy")
            .displayName("Sort Key Strategy")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(SORT_NONE, SORT_BY_FIELD, SORT_BY_SEQUENCE)
            .defaultValue(SORT_NONE.getValue())
            .description("Defines the strategy the processor uses to assign sort key to the inserted Items.")
            .build();

    static final PropertyDescriptor SORT_KEY_FIELD = new PropertyDescriptor.Builder()
            .name("sort-key-field")
            .displayName("Sort Key Field")
            .required(true)
            .dependsOn(SORT_KEY_STRATEGY, SORT_BY_FIELD, SORT_BY_SEQUENCE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("Defines the name of the sort key field in the DynamoDB table. Sort key is also known as range key.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        TABLE,
        REGION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        RECORD_READER,
        PARTITION_KEY_STRATEGY,
        PARTITION_KEY_FIELD,
        PARTITION_KEY_ATTRIBUTE,
        SORT_KEY_STRATEGY,
        SORT_KEY_FIELD,
        TIMEOUT,
        ENDPOINT_OVERRIDE,
        ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE,
        SSL_CONTEXT_SERVICE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final int alreadyProcessedChunks = flowFile.getAttribute(DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE) != null ? Integer.parseInt(flowFile.getAttribute(DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE)) : 0;
        final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final SplitRecordSetHandler handler = new DynamoDbSplitRecordSetHandler(MAXIMUM_CHUNK_SIZE, getClient(context), context, flowFile.getAttributes(), getLogger());
        final SplitRecordSetHandler.RecordHandlerResult result;

        try (
            final InputStream in = session.read(flowFile);
            final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger())) {
            result = handler.handle(reader.createRecordSet(), alreadyProcessedChunks);
        } catch (final Exception e) {
            getLogger().error("Error while reading records", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());
        attributes.put(DYNAMODB_CHUNKS_PROCESSED_ATTRIBUTE, String.valueOf(result.getSuccessfulChunks()));
        final FlowFile outgoingFlowFile = session.putAllAttributes(flowFile, attributes);

        if (result.isSuccess()) {
            session.transfer(outgoingFlowFile, REL_SUCCESS);
        } else {
            handleError(context, session, result, outgoingFlowFile);
        }
    }

    private void handleError(
            final ProcessContext context,
            final ProcessSession session,
            final SplitRecordSetHandler.RecordHandlerResult result,
            final FlowFile outgoingFlowFile
    ) {
        final Throwable error = result.getThrowable();
        final Throwable cause = error.getCause();

        if (cause instanceof ProvisionedThroughputExceededException) {
            // When DynamoDB returns with {@code ProvisionedThroughputExceededException}, the client reached it's write limitation and
            // should be retried at a later time. We yield the processor and the FlowFile is considered unprocessed (partially processed) due to temporary write limitations.
            // More about throughput limitations: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html
            context.yield();
            session.transfer(outgoingFlowFile, REL_UNPROCESSED);
        } else if (cause instanceof AwsServiceException) {
            getLogger().error("Could not process FlowFile due to server exception", error);
            session.transfer(processServiceException(session, Collections.singletonList(outgoingFlowFile), (AwsServiceException) cause), REL_FAILURE);
        } else if (cause instanceof SdkException) {
            getLogger().error("Could not process FlowFile due to client exception", error);
            session.transfer(processSdkException(session, Collections.singletonList(outgoingFlowFile), (SdkException) cause), REL_FAILURE);
        } else {
            getLogger().error("Could not process FlowFile", error);
            session.transfer(outgoingFlowFile, REL_FAILURE);
        }
    }

    private static class DynamoDbSplitRecordSetHandler extends SplitRecordSetHandler {
        private final DynamoDbClient client;
        private final String tableName;
        private final ProcessContext context;
        private final Map<String, String> flowFileAttributes;
        private final ComponentLog logger;
        private Collection<WriteRequest> accumulator;
        private int itemCounter = 0;

        private DynamoDbSplitRecordSetHandler(
                final int maxChunkSize,
                final DynamoDbClient client,
                final ProcessContext context,
                final Map<String, String> flowFileAttributes,
                final ComponentLog logger) {
            super(maxChunkSize);
            this.client = client;
            this.context = context;
            this.flowFileAttributes = flowFileAttributes;
            this.logger = logger;
            this.tableName = context.getProperty(TABLE).evaluateAttributeExpressions().getValue();
            accumulator = new ArrayList<>();
        }

        @Override
        protected void handleChunk(final boolean wasBatchAlreadyProcessed) throws SplitRecordSetHandlerException {
            try {
                if (!wasBatchAlreadyProcessed) {
                    final Map<String, Collection<WriteRequest>> requestItems = new HashMap<>();
                    requestItems.put(tableName, accumulator);
                    final BatchWriteItemResponse response = client.batchWriteItem(BatchWriteItemRequest.builder().requestItems(requestItems).build());

                    if (CollectionUtils.isNotEmpty(response.unprocessedItems())) {
                        throw new SplitRecordSetHandlerException("Could not insert all items. The unprocessed items are: " + response.unprocessedItems());
                    }
                } else {
                    logger.debug("Skipping chunk as was already processed");
                }

                accumulator.clear();
            } catch (final Exception e) {
                throw new SplitRecordSetHandlerException(e);
            }
        }

        @Override
        protected void addToChunk(final Record record) {
            itemCounter++;
            accumulator.add(convert(record));
        }

        private WriteRequest convert(final Record record) {
            final String partitionKeyField  = context.getProperty(PARTITION_KEY_FIELD).evaluateAttributeExpressions().getValue();
            final String sortKeyStrategy  = context.getProperty(SORT_KEY_STRATEGY).getValue();
            final String sortKeyField  = context.getProperty(SORT_KEY_FIELD).evaluateAttributeExpressions().getValue();

            final PutRequest.Builder putRequestBuilder = PutRequest.builder();
            final Map<String, AttributeValue> item = new HashMap<>();

            record.getSchema()
                    .getFields()
                    .stream()
                    .filter(field -> !field.getFieldName().equals(partitionKeyField))
                    .filter(field -> SORT_NONE.getValue().equals(sortKeyStrategy) || !field.getFieldName().equals(sortKeyField))
                    .forEach(field -> RecordToItemConverter.addField(record, item, field.getDataType().getFieldType(), field.getFieldName()));

            addPartitionKey(record, item);
            addSortKey(record, item);
            return WriteRequest.builder().putRequest(putRequestBuilder.item(item).build()).build();
        }

        private void addPartitionKey(final Record record, final Map<String, AttributeValue> item) {
            final String partitionKeyStrategy = context.getProperty(PARTITION_KEY_STRATEGY).getValue();
            final String partitionKeyField  = context.getProperty(PARTITION_KEY_FIELD).evaluateAttributeExpressions().getValue();
            final String partitionKeyAttribute = context.getProperty(PARTITION_KEY_ATTRIBUTE).evaluateAttributeExpressions().getValue();

            final Object partitionKeyValue;
            if (PARTITION_BY_FIELD.getValue().equals(partitionKeyStrategy)) {
                if (!record.getSchema().getFieldNames().contains(partitionKeyField)) {
                    throw new ProcessException("\"" + PARTITION_BY_FIELD.getDisplayName() + "\" strategy needs the \"" + PARTITION_KEY_FIELD.getDefaultValue() + "\" to present in the record");
                }

                partitionKeyValue = record.getValue(partitionKeyField);
            } else if (PARTITION_BY_ATTRIBUTE.getValue().equals(partitionKeyStrategy)) {
                if (record.getSchema().getFieldNames().contains(partitionKeyField)) {
                    throw new ProcessException("Cannot reuse existing field with " + PARTITION_KEY_STRATEGY.getDisplayName() + " \"" + PARTITION_BY_ATTRIBUTE.getDisplayName() + "\"");
                }

                if (!flowFileAttributes.containsKey(partitionKeyAttribute)) {
                    throw new ProcessException("Missing attribute \"" + partitionKeyAttribute + "\"" );
                }

                partitionKeyValue = flowFileAttributes.get(partitionKeyAttribute);
            } else if (PARTITION_GENERATED.getValue().equals(partitionKeyStrategy)) {
                if (record.getSchema().getFieldNames().contains(partitionKeyField)) {
                    throw new ProcessException("Cannot reuse existing field with " + PARTITION_KEY_STRATEGY.getDisplayName() + " \"" + PARTITION_GENERATED.getDisplayName() + "\"");
                }

                partitionKeyValue = UUID.randomUUID().toString();
            } else {
                throw new ProcessException("Unknown " + PARTITION_KEY_STRATEGY.getDisplayName() + " \"" + partitionKeyStrategy + "\"");
            }

            item.put(partitionKeyField, RecordToItemConverter.toAttributeValue(partitionKeyValue));
        }

        private void addSortKey(final Record record, final Map<String, AttributeValue> item) {
            final String sortKeyStrategy  = context.getProperty(SORT_KEY_STRATEGY).getValue();
            final String sortKeyField  = context.getProperty(SORT_KEY_FIELD).evaluateAttributeExpressions().getValue();

            final Object sortKeyValue;
            if (SORT_BY_FIELD.getValue().equals(sortKeyStrategy)) {
                if (!record.getSchema().getFieldNames().contains(sortKeyField)) {
                    throw new ProcessException(SORT_BY_FIELD.getDisplayName() + " strategy needs the \"" + SORT_KEY_FIELD.getDisplayName() + "\" to present in the record");
                }

                sortKeyValue = record.getValue(sortKeyField);
            } else if (SORT_BY_SEQUENCE.getValue().equals(sortKeyStrategy)) {
                if (record.getSchema().getFieldNames().contains(sortKeyField)) {
                    throw new ProcessException("Cannot reuse existing field with " + SORT_KEY_STRATEGY.getDisplayName() + "  \"" + SORT_BY_SEQUENCE.getDisplayName() + "\"");
                }

                sortKeyValue = itemCounter;
            } else if (SORT_NONE.getValue().equals(sortKeyStrategy)) {
                logger.debug("No {} was applied", SORT_KEY_STRATEGY.getDisplayName());
                sortKeyValue = null;
            } else {
                throw new ProcessException("Unknown " + SORT_KEY_STRATEGY.getDisplayName() + " \"" + sortKeyStrategy + "\"");
            }

            if (sortKeyValue != null) {
                item.put(sortKeyField, RecordToItemConverter.toAttributeValue(sortKeyValue));
            }
        }
    }
}
