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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SupportsBatching
@SeeAlso({ GetSQS.class, DeleteSQS.class })
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "AWS", "SQS", "Queue", "Put", "Publish"})
@CapabilityDescription("Publishes a message to an Amazon Simple Queuing Service Queue")
@DynamicProperty(name = "The name of a Message Attribute to add to the message", value = "The value of the Message Attribute",
        description = "Allows the user to add key/value pairs as Message Attributes by adding a property whose name will become the name of "
        + "the Message Attribute and value will become the value of the Message Attribute", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
public class PutSQS extends AbstractAwsSyncProcessor<SqsClient, SqsClientBuilder> {
    private static final String STRING_DATA_TYPE = "String";

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of messages to send in a single network request")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("25")
            .build();

    public static final PropertyDescriptor QUEUE_URL = new PropertyDescriptor.Builder()
            .name("Queue URL")
            .description("The URL of the queue to act upon")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor DELAY = new PropertyDescriptor.Builder()
            .name("Delay")
            .displayName("Delay")
            .description("The amount of time to delay the message before it becomes available to consumers")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 secs")
            .build();

    public static final PropertyDescriptor MESSAGEGROUPID = new PropertyDescriptor.Builder()
            .name("message-group-id")
            .displayName("Message Group ID")
            .description("If using FIFO, the message group to which the FlowFile belongs")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MESSAGEDEDUPLICATIONID = new PropertyDescriptor.Builder()
            .name("deduplication-message-id")
            .displayName("Deduplication Message ID")
            .description("The token used for deduplication of sent messages")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        QUEUE_URL,
        REGION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        SSL_CONTEXT_SERVICE,
        DELAY,
        TIMEOUT,
        ENDPOINT_OVERRIDE,
        PROXY_CONFIGURATION_SERVICE,
        MESSAGEGROUPID,
        MESSAGEDEDUPLICATIONID
    );

    private volatile List<PropertyDescriptor> userDefinedProperties = Collections.emptyList();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        userDefinedProperties = new ArrayList<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                userDefinedProperties.add(descriptor);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        final SqsClient client = getClient(context);
        final String queueUrl = context.getProperty(QUEUE_URL).evaluateAttributeExpressions(flowFile).getValue();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);
        final String flowFileContent = baos.toString();

        final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();

        for (final PropertyDescriptor descriptor : userDefinedProperties) {
            final MessageAttributeValue mav = MessageAttributeValue.builder()
                    .dataType(STRING_DATA_TYPE)
                    .stringValue(context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue())
                    .build();
            messageAttributes.put(descriptor.getName(), mav);
        }

        final SendMessageBatchRequestEntry entry = SendMessageBatchRequestEntry.builder()
                .messageBody(flowFileContent)
                .messageGroupId(context.getProperty(MESSAGEGROUPID).evaluateAttributeExpressions(flowFile).getValue())
                .messageDeduplicationId(context.getProperty(MESSAGEDEDUPLICATIONID).evaluateAttributeExpressions(flowFile).getValue())
                .id(flowFile.getAttribute(CoreAttributes.UUID.key()))
                .messageAttributes(messageAttributes)
                .delaySeconds(context.getProperty(DELAY).asTimePeriod(TimeUnit.SECONDS).intValue())
                .build();

        final SendMessageBatchRequest request = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(entry)
                .build();

        try {
            final SendMessageBatchResponse response = client.sendMessageBatch(request);

            // check for errors
            if (!response.failed().isEmpty()) {
                throw new ProcessException(response.failed().get(0).toString());
            }
        } catch (final Exception e) {
            getLogger().error("Failed to send messages to Amazon SQS; routing to failure", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        getLogger().info("Successfully published message to Amazon SQS for {}", flowFile);
        session.transfer(flowFile, REL_SUCCESS);
        final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        session.getProvenanceReporter().send(flowFile, queueUrl, transmissionMillis);
    }

    @Override
    protected SqsClientBuilder createClientBuilder(final ProcessContext context) {
        return SqsClient.builder();
    }
}
