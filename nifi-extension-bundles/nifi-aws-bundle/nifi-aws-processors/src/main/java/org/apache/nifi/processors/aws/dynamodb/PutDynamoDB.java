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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
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
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SupportsBatching
@SeeAlso({DeleteDynamoDB.class, GetDynamoDB.class, PutDynamoDBRecord.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "DynamoDB", "AWS", "Put", "Insert"})
@CapabilityDescription("Puts a document from DynamoDB based on hash and range key.  The table can have either hash and range or hash key alone."
    + " Currently the keys supported are string and number and value can be json document. "
    + "In case of hash and range keys both key are required for the operation."
    + " The FlowFile content must be JSON. FlowFile content is mapped to the specified Json Document attribute in the DynamoDB item.")
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
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ERROR_STATUS_CODE, description = "DynamoDB error status code"),
    @WritesAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ITEM_IO_ERROR, description = "IO exception message on creating item")
})
@ReadsAttributes({
    @ReadsAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ITEM_HASH_KEY_VALUE, description = "Items hash key value"),
    @ReadsAttribute(attribute = AbstractDynamoDBProcessor.DYNAMODB_ITEM_RANGE_KEY_VALUE, description = "Items range key value")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutDynamoDB extends AbstractDynamoDBProcessor {

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        TABLE,
        REGION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        JSON_DOCUMENT,
        HASH_KEY_NAME,
        RANGE_KEY_NAME,
        HASH_KEY_VALUE,
        RANGE_KEY_VALUE,
        HASH_KEY_VALUE_TYPE,
        RANGE_KEY_VALUE_TYPE,
        DOCUMENT_CHARSET,
        BATCH_SIZE,
        TIMEOUT,
        ENDPOINT_OVERRIDE,
        SSL_CONTEXT_SERVICE,
        PROXY_CONFIGURATION_SERVICE);

    public static final int DYNAMODB_MAX_ITEM_SIZE = 400 * 1024;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        List<FlowFile> flowFiles = session.get(context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger());
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        Map<ItemKeys, FlowFile> keysToFlowFileMap = new HashMap<>();

        final String table = context.getProperty(TABLE).evaluateAttributeExpressions().getValue();

        final String hashKeyName = context.getProperty(HASH_KEY_NAME).evaluateAttributeExpressions().getValue();
        final String rangeKeyName = context.getProperty(RANGE_KEY_NAME).evaluateAttributeExpressions().getValue();
        final String jsonDocument = context.getProperty(JSON_DOCUMENT).evaluateAttributeExpressions().getValue();
        final String charset = context.getProperty(DOCUMENT_CHARSET).evaluateAttributeExpressions().getValue();

        final Map<String, Collection<WriteRequest>> tableNameRequestItemsMap = new HashMap<>();
        final Collection<WriteRequest> requestItems = new ArrayList<>();
        tableNameRequestItemsMap.put(table, requestItems);

        for (FlowFile flowFile : flowFiles) {
            final AttributeValue hashKeyValue = getAttributeValue(context, HASH_KEY_VALUE_TYPE, HASH_KEY_VALUE, flowFile.getAttributes());
            final AttributeValue rangeKeyValue = getAttributeValue(context, RANGE_KEY_VALUE_TYPE, RANGE_KEY_VALUE, flowFile.getAttributes());

            if (!isHashKeyValueConsistent(hashKeyName, hashKeyValue, session, flowFile)) {
                continue;
            }

            if (!isRangeKeyValueConsistent(rangeKeyName, rangeKeyValue, session, flowFile)) {
                continue;
            }

            if (!isDataValid(flowFile, jsonDocument)) {
                flowFile = session.putAttribute(flowFile, AWS_DYNAMO_DB_ITEM_SIZE_ERROR, "Max size of item + attribute should be 400kb but was " + flowFile.getSize() + jsonDocument.length());
                session.transfer(flowFile, REL_FAILURE);
                continue;
            }

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            session.exportTo(flowFile, baos);

            final Map<String, AttributeValue> item = new HashMap<>();
            item.put(hashKeyName, hashKeyValue);
            if (!isBlank(rangeKeyValue)) {
                item.put(rangeKeyName, rangeKeyValue);
            }
            final String jsonText = IOUtils.toString(baos.toByteArray(), charset);
            final AttributeValue jsonValue = AttributeValue.builder().s(jsonText).build();
            item.put(jsonDocument, jsonValue);

            final PutRequest putRequest = PutRequest.builder().item(item).build();
            final WriteRequest writeRequest = WriteRequest.builder().putRequest(putRequest).build();
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

            if (response.unprocessedItems() != null) {
                // Handle unprocessed items
                final List<WriteRequest> unprocessedItems = response.unprocessedItems().get(table);
                if (unprocessedItems != null) {
                    for (final WriteRequest request : unprocessedItems) {
                        final Map<String, AttributeValue> item = request.putRequest().item();
                        final AttributeValue hashKeyValue = item.get(hashKeyName);
                        final AttributeValue rangeKeyValue = item.get(rangeKeyName);

                        sendUnprocessedToUnprocessedRelationship(session, keysToFlowFileMap, hashKeyValue, rangeKeyValue);
                    }
                }
            }

            // Handle any remaining flowfiles
            for (final FlowFile flowFile : keysToFlowFileMap.values()) {
                getLogger().debug("Successful posted items to dynamodb : {}", table);
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
        } catch (Exception exception) {
            getLogger().error("Could not process flowFiles", exception);
            List<FlowFile> failedFlowFiles = processException(session, flowFiles, exception);
            session.transfer(failedFlowFiles, REL_FAILURE);
        }
    }

    private boolean isDataValid(FlowFile flowFile, String jsonDocument) {
        return (flowFile.getSize() + jsonDocument.length()) < DYNAMODB_MAX_ITEM_SIZE;
    }

}
