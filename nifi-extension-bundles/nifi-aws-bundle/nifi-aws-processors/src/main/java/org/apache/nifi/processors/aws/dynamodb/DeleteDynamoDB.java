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
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SupportsBatching
@SeeAlso({GetDynamoDB.class, PutDynamoDB.class, PutDynamoDBRecord.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "DynamoDB", "AWS", "Delete", "Remove"})
@CapabilityDescription("Deletes a document from DynamoDB based on hash and range key. The key can be string or number."
        + " The request requires all the primary keys for the operation (hash or hash and range key)")
@WritesAttributes({
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_UNPROCESSED, description = "DynamoDB unprocessed keys"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_RANGE_KEY_VALUE_ERROR, description = "DynamoDB range key error"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_KEY_ERROR_NOT_FOUND, description = "DynamoDB key not found"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_EXCEPTION_MESSAGE, description = "DynamoDB exception message"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_CODE, description = "DynamoDB error code"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_MESSAGE, description = "DynamoDB error message"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_SERVICE, description = "DynamoDB error service"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_RETRYABLE, description = "DynamoDB error is retryable"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_REQUEST_ID, description = "DynamoDB error request id"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_STATUS_CODE, description = "DynamoDB status code")
    })
@ReadsAttributes({
    @ReadsAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ITEM_HASH_KEY_VALUE, description = "Items hash key value" ),
    @ReadsAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ITEM_RANGE_KEY_VALUE, description = "Items range key value" ),
    })
public class DeleteDynamoDB extends AbstractDynamoDBProcessor {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        TABLE,
        REGION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        HASH_KEY_NAME,
        RANGE_KEY_NAME,
        HASH_KEY_VALUE,
        RANGE_KEY_VALUE,
        HASH_KEY_VALUE_TYPE,
        RANGE_KEY_VALUE_TYPE,
        BATCH_SIZE,
        TIMEOUT,
        ENDPOINT_OVERRIDE,
        SSL_CONTEXT_SERVICE,
        PROXY_CONFIGURATION_SERVICE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger());
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        final Map<ItemKeys, FlowFile> keysToFlowFileMap = new HashMap<>();

        final String table = context.getProperty(TABLE).evaluateAttributeExpressions().getValue();

        final String hashKeyName = context.getProperty(HASH_KEY_NAME).evaluateAttributeExpressions().getValue();
        final String rangeKeyName = context.getProperty(RANGE_KEY_NAME).evaluateAttributeExpressions().getValue();

        final Map<String, Collection<WriteRequest>> tableNameRequestItemsMap = new HashMap<>();
        final Collection<WriteRequest> requestItems = new ArrayList<>();
        tableNameRequestItemsMap.put(table, requestItems);

        for (final FlowFile flowFile : flowFiles) {
            final AttributeValue hashKeyValue = getAttributeValue(context, HASH_KEY_VALUE_TYPE, HASH_KEY_VALUE, flowFile.getAttributes());
            final AttributeValue rangeKeyValue = getAttributeValue(context, RANGE_KEY_VALUE_TYPE, RANGE_KEY_VALUE, flowFile.getAttributes());

            if (!isHashKeyValueConsistent(hashKeyName, hashKeyValue, session, flowFile)) {
                continue;
            }

            if (!isRangeKeyValueConsistent(rangeKeyName, rangeKeyValue, session, flowFile) ) {
                continue;
            }

            final Map<String, AttributeValue> keyMap = new HashMap<>();
            keyMap.put(hashKeyName, hashKeyValue);
            if (!isBlank(rangeKeyValue)) {
                keyMap.put(rangeKeyName, rangeKeyValue);
            }
            final DeleteRequest deleteRequest = DeleteRequest.builder()
                    .key(keyMap)
                    .build();
            final WriteRequest writeRequest = WriteRequest.builder().deleteRequest(deleteRequest).build();
            requestItems.add(writeRequest);

            keysToFlowFileMap.put(new ItemKeys(hashKeyValue, rangeKeyValue), flowFile);
        }

        if (keysToFlowFileMap.isEmpty()) {
            return;
        }

        final DynamoDbClient client = getClient(context);

        try {
            final BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder().requestItems(tableNameRequestItemsMap).build();
            final BatchWriteItemResponse response = client.batchWriteItem(batchWriteItemRequest);

            if (CollectionUtils.isNotEmpty(response.unprocessedItems())) {
                // Handle unprocessed items
                final List<WriteRequest> unprocessedItems = response.unprocessedItems().get(table);
                for (final WriteRequest writeRequest : unprocessedItems) {
                    final Map<String, AttributeValue> item = writeRequest.deleteRequest().key();
                    final AttributeValue hashKeyValue = item.get(hashKeyName);
                    final AttributeValue rangeKeyValue = item.get(rangeKeyName);

                    sendUnprocessedToUnprocessedRelationship(session, keysToFlowFileMap, hashKeyValue, rangeKeyValue);
                }
            }

            // All non unprocessed items are successful
            for (final FlowFile flowFile : keysToFlowFileMap.values()) {
                getLogger().debug("Successfully deleted item from dynamodb : {}", table);
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (final AwsServiceException exception) {
            getLogger().error("Could not process flowFiles due to service exception", exception);
            List<FlowFile> failedFlowFiles = processServiceException(session, flowFiles, exception);
            session.transfer(failedFlowFiles, REL_FAILURE);
        } catch (final SdkException exception) {
            getLogger().error("Could not process flowFiles due to SDK exception", exception);
            List<FlowFile> failedFlowFiles = processSdkException(session, flowFiles, exception);
            session.transfer(failedFlowFiles, REL_FAILURE);
        } catch (final Exception exception) {
            getLogger().error("Could not process flowFiles", exception);
            List<FlowFile> failedFlowFiles = processException(session, flowFiles, exception);
            session.transfer(failedFlowFiles, REL_FAILURE);
        }
    }

}
