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
package org.apache.nifi.processors.aws.sqs;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.v2.AbstractAwsSyncProcessor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;

import java.util.List;

@SupportsBatching
@SeeAlso({GetSQS.class, PutSQS.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "AWS", "SQS", "Queue", "Delete"})
@CapabilityDescription("Deletes a message from an Amazon Simple Queuing Service Queue")
public class DeleteSQS extends AbstractAwsSyncProcessor<SqsClient, SqsClientBuilder> {

    public static final PropertyDescriptor QUEUE_URL = new PropertyDescriptor.Builder()
            .name("Queue URL")
            .description("The URL of the queue delete from")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor RECEIPT_HANDLE = new PropertyDescriptor.Builder()
            .name("Receipt Handle")
            .description("The identifier that specifies the receipt of the message")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${sqs.receipt.handle}")
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        QUEUE_URL,
        REGION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        SSL_CONTEXT_SERVICE,
        RECEIPT_HANDLE,
        TIMEOUT,
        ENDPOINT_OVERRIDE,
        PROXY_CONFIGURATION_SERVICE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String queueUrl = context.getProperty(QUEUE_URL).evaluateAttributeExpressions(flowFile).getValue();

        final SqsClient client = getClient(context);

        final String receiptHandle = context.getProperty(RECEIPT_HANDLE).evaluateAttributeExpressions(flowFile).getValue();
        final String entryId = flowFile.getAttribute(CoreAttributes.UUID.key());
        final DeleteMessageBatchRequestEntry entry = DeleteMessageBatchRequestEntry.builder()
                .receiptHandle(receiptHandle)
                .id(entryId)
                .build();

        final DeleteMessageBatchRequest request = DeleteMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(entry)
                .build();

        try {
            DeleteMessageBatchResponse response = client.deleteMessageBatch(request);

            // check for errors
            if (!response.failed().isEmpty()) {
                throw new ProcessException(response.failed().get(0).toString());
            }

            getLogger().info("Successfully deleted message from SQS for {}", flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Failed to delete message from SQS", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected SqsClientBuilder createClientBuilder(final ProcessContext context) {
        return SqsClient.builder();
    }
}
