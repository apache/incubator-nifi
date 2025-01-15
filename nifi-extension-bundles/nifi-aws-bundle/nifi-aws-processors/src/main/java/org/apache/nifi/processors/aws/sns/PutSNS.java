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
package org.apache.nifi.processors.aws.sns;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.sqs.GetSQS;
import org.apache.nifi.processors.aws.sqs.PutSQS;
import org.apache.nifi.processors.aws.v2.AbstractAwsSyncProcessor;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SupportsBatching
@SeeAlso({GetSQS.class, PutSQS.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "sns", "topic", "put", "publish", "pubsub"})
@CapabilityDescription("Sends the content of a FlowFile as a notification to the Amazon Simple Notification Service")
@DynamicProperty(name = "A name of an attribute to be added to the notification", value = "The attribute value", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "User specified dynamic Properties are added as attributes to the notification")
public class PutSNS extends AbstractAwsSyncProcessor<SnsClient, SnsClientBuilder> {

    public static final PropertyDescriptor CHARACTER_ENCODING = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The character set in which the FlowFile's content is encoded")
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor USE_JSON_STRUCTURE = new PropertyDescriptor.Builder()
            .name("Use JSON Structure")
            .description("If true, the contents of the FlowFile must be JSON with a top-level element named 'default'."
                    + " Additional elements can be used to send different messages to different protocols. See the Amazon"
                    + " SNS Documentation for more information.")
            .defaultValue("false")
            .allowableValues("true", "false")
            .required(true)
            .build();

    public static final PropertyDescriptor SUBJECT = new PropertyDescriptor.Builder()
            .name("E-mail Subject")
            .description("The optional subject to use for any subscribers that are subscribed via E-mail")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MESSAGEGROUPID = new PropertyDescriptor.Builder()
            .name("Message Group ID")
            .displayName("Message Group ID")
            .description("If using FIFO, the message group to which the flowFile belongs")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MESSAGEDEDUPLICATIONID = new PropertyDescriptor.Builder()
            .name("Deduplication Message ID")
            .displayName("Deduplication Message ID")
            .description("The token used for deduplication of sent messages")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();


    public static final PropertyDescriptor ARN = new PropertyDescriptor.Builder()
            .name("Amazon Resource Name (ARN)")
            .description("The name of the resource to which notifications should be published")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final AllowableValue ARN_TYPE_TOPIC = new AllowableValue("Topic ARN", "Topic ARN", "The ARN is the name of a topic");
    protected static final AllowableValue ARN_TYPE_TARGET = new AllowableValue("Target ARN", "Target ARN", "The ARN is the name of a particular Target, used to notify a specific subscriber");
    public static final PropertyDescriptor ARN_TYPE = new PropertyDescriptor.Builder()
            .name("ARN Type")
            .description("The type of Amazon Resource Name that is being used.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .allowableValues(ARN_TYPE_TOPIC, ARN_TYPE_TARGET)
            .defaultValue(ARN_TYPE_TOPIC.getValue())
            .build();


    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ARN,
            ARN_TYPE,
            SUBJECT,
            REGION,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            SSL_CONTEXT_SERVICE,
            TIMEOUT,
            ENDPOINT_OVERRIDE,
            PROXY_CONFIGURATION_SERVICE,
            USE_JSON_STRUCTURE,
            CHARACTER_ENCODING,
            MESSAGEGROUPID,
            MESSAGEDEDUPLICATIONID
    );

    public static final int MAX_SIZE = 256 * 1024;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        if (flowFile.getSize() > MAX_SIZE) {
            getLogger().error("Cannot publish {} to SNS because its size exceeds Amazon SNS's limit of 256KB; routing to failure", flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_ENCODING).evaluateAttributeExpressions(flowFile).getValue());

        final String message;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            session.exportTo(flowFile, baos);
            message = baos.toString(charset);
        } catch (final IOException ioe) {
            throw new ProcessException("Failed to read FlowFile content", ioe);
        }

        final SnsClient client = getClient(context);

        final PublishRequest.Builder requestBuilder = PublishRequest.builder();
        requestBuilder.message(message);

        if (context.getProperty(MESSAGEGROUPID).isSet()) {
            requestBuilder.messageGroupId(context.getProperty(MESSAGEGROUPID)
                    .evaluateAttributeExpressions(flowFile)
                    .getValue());
        }

        if (context.getProperty(MESSAGEDEDUPLICATIONID).isSet()) {
            requestBuilder.messageDeduplicationId(context.getProperty(MESSAGEDEDUPLICATIONID)
                    .evaluateAttributeExpressions(flowFile)
                    .getValue());
        }

        if (context.getProperty(USE_JSON_STRUCTURE).asBoolean()) {
            requestBuilder.messageStructure("json");
        }

        final String arn = context.getProperty(ARN).evaluateAttributeExpressions(flowFile).getValue();
        final String arnType = context.getProperty(ARN_TYPE).getValue();
        if (arnType.equalsIgnoreCase(ARN_TYPE_TOPIC.getValue())) {
            requestBuilder.topicArn(arn);
        } else {
            requestBuilder.targetArn(arn);
        }

        final String subject = context.getProperty(SUBJECT).evaluateAttributeExpressions(flowFile).getValue();
        if (subject != null) {
            requestBuilder.subject(subject);
        }

        final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic() && !StringUtils.isEmpty(entry.getValue())) {
                final MessageAttributeValue.Builder messageAttributeValueBuilder = MessageAttributeValue.builder();
                messageAttributeValueBuilder.stringValue(context.getProperty(entry.getKey()).evaluateAttributeExpressions(flowFile).getValue());
                messageAttributeValueBuilder.dataType("String");

                messageAttributes.put(entry.getKey().getName(), messageAttributeValueBuilder.build());
            }
        }
        requestBuilder.messageAttributes(messageAttributes);

        try {
            client.publish(requestBuilder.build());
            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, arn);
            getLogger().info("Publishing completed for {}", flowFile);
        } catch (final Exception e) {
            getLogger().error("Publishing failed for {}", flowFile, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected SnsClientBuilder createClientBuilder(final ProcessContext context) {
        return SnsClient.builder();
    }

}
