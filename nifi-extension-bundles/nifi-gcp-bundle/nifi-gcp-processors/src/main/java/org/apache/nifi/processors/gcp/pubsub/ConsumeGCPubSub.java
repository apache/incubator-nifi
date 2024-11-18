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
package org.apache.nifi.processors.gcp.pubsub;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.pathtemplate.ValidationException;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.pubsub.consume.OutputStrategy;
import org.apache.nifi.processors.gcp.pubsub.consume.ProcessingStrategy;
import org.apache.nifi.processors.gcp.pubsub.consume.PubSubMessageConverter;
import org.apache.nifi.processors.gcp.pubsub.consume.RecordStreamPubSubMessageConverter;
import org.apache.nifi.processors.gcp.pubsub.consume.WrapperRecordStreamPubSubMessageConverter;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.ACK_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.ACK_ID_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.DYNAMIC_ATTRIBUTES_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.DYNAMIC_ATTRIBUTES_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_ATTRIBUTES_COUNT_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_ATTRIBUTES_COUNT_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_PUBLISH_TIME_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MSG_PUBLISH_TIME_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.SERIALIZED_SIZE_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.SERIALIZED_SIZE_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.SUBSCRIPTION_NAME_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.SUBSCRIPTION_NAME_DESCRIPTION;

@SeeAlso({ PublishGCPubSub.class })
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({ "google", "google-cloud", "gcp", "message", "pubsub", "consume" })
@CapabilityDescription(
    """
     Consumes messages from the configured Google Cloud PubSub subscription. The 'Batch Size' property specified the maximum
     number of messages that will be pulled from the subscription in a single request. The 'Processing Strategy' property
     specifies if each message should be its own FlowFile or if messages should be grouped into a single FlowFile. Using the
     Demarcator strategy will provide best throughput when the format allows it. Using Record allows to convert data format
     as well as doing schema enforcement. Using the FlowFile strategy will generate one FlowFile per message and will have
     the message's attributes as FlowFile attributes.
    """
)
@WritesAttributes(
    {
            @WritesAttribute(attribute = ACK_ID_ATTRIBUTE, description = ACK_ID_DESCRIPTION),
            @WritesAttribute(attribute = SERIALIZED_SIZE_ATTRIBUTE, description = SERIALIZED_SIZE_DESCRIPTION),
            @WritesAttribute(attribute = MSG_ATTRIBUTES_COUNT_ATTRIBUTE, description = MSG_ATTRIBUTES_COUNT_DESCRIPTION),
            @WritesAttribute(attribute = MSG_PUBLISH_TIME_ATTRIBUTE, description = MSG_PUBLISH_TIME_DESCRIPTION),
            @WritesAttribute(attribute = SUBSCRIPTION_NAME_ATTRIBUTE, description = SUBSCRIPTION_NAME_DESCRIPTION),
            @WritesAttribute(attribute = DYNAMIC_ATTRIBUTES_ATTRIBUTE, description = DYNAMIC_ATTRIBUTES_DESCRIPTION)
    }
)
public class ConsumeGCPubSub extends AbstractGCPubSubWithProxyProcessor {

    private static final List<String> REQUIRED_PERMISSIONS = Collections.singletonList("pubsub.subscriptions.consume");

    public static final PropertyDescriptor SUBSCRIPTION = new PropertyDescriptor.Builder()
            .name("gcp-pubsub-subscription")
            .displayName("Subscription")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .description("Name of the Google Cloud Pub/Sub Subscription")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PROCESSING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Processing Strategy")
            .description("Strategy for processing PubSub Records and writing serialized output to FlowFiles")
            .required(true)
            .allowableValues(ProcessingStrategy.class)
            .defaultValue(ProcessingStrategy.FLOW_FILE.getValue())
            .expressionLanguageSupported(NONE)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("The Record Reader to use for incoming messages")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .description("The Record Writer to use in order to serialize the outgoing FlowFiles")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .build();

    public static final PropertyDescriptor OUTPUT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Output Strategy")
            .description("The format used to output the Kafka Record into a FlowFile Record.")
            .required(true)
            .defaultValue(OutputStrategy.USE_VALUE)
            .allowableValues(OutputStrategy.class)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.RECORD)
            .build();

    public static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("Message Demarcator")
            .required(true)
            .addValidator(Validator.VALID)
            .description(
                """
                Since the PubSub client receives messages in batches, this Processor has an option to output FlowFiles
                which contains all the messages in a single batch. This property allows you to provide a string
                (interpreted as UTF-8) to use for demarcating apart multiple messages. To enter special character
                such as 'new line' use CTRL+Enter or Shift+Enter depending on the OS.
                """)
            .dependsOn(PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR)
            .build();

    public static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse failure")
            .description("If configured to use a Record Reader, a PubSub message that cannot be parsed using the configured Record Reader will be routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GCP_CREDENTIALS_PROVIDER_SERVICE,
            PROJECT_ID,
            SUBSCRIPTION,
            BATCH_SIZE_THRESHOLD,
            PROCESSING_STRATEGY,
            RECORD_READER,
            RECORD_WRITER,
            OUTPUT_STRATEGY,
            MESSAGE_DEMARCATOR,
            API_ENDPOINT,
            PROXY_CONFIGURATION_SERVICE);

    private static final Set<Relationship> SUCCESS_RELATIONSHIP = Set.of(REL_SUCCESS);
    private static final Set<Relationship> SUCCESS_FAILURE_RELATIONSHIPS = Set.of(REL_SUCCESS, REL_PARSE_FAILURE);

    protected SubscriberStub subscriber = null;
    private PullRequest pullRequest;
    protected volatile OutputStrategy outputStrategy;
    protected volatile ProcessingStrategy processingStrategy;
    private volatile boolean useReader;
    protected volatile String demarcatorValue;

    private final AtomicReference<Exception> storedException = new AtomicReference<>();

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

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        final Integer batchSize = context.getProperty(BATCH_SIZE_THRESHOLD).asInteger();

        pullRequest = PullRequest.newBuilder()
                .setMaxMessages(batchSize)
                .setSubscription(getSubscriptionName(context, null))
                .build();

        try {
            subscriber = getSubscriber(context);
        } catch (IOException e) {
            storedException.set(e);
            getLogger().error("Failed to create Google Cloud Subscriber", e);
        }

        outputStrategy = context.getProperty(OUTPUT_STRATEGY).asAllowableValue(OutputStrategy.class);
        processingStrategy = context.getProperty(PROCESSING_STRATEGY).asAllowableValue(ProcessingStrategy.class);
        demarcatorValue = context.getProperty(MESSAGE_DEMARCATOR).getValue();
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        String subscriptionName = null;
        try {
            subscriptionName = getSubscriptionName(context, attributes);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Parse Subscription Name")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully parsed Subscription Name")
                    .build());
        } catch (final ValidationException e) {
            verificationLogger.error("Failed to parse Subscription Name", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Parse Subscription Name")
                    .outcome(Outcome.FAILED)
                    .explanation(String.format("Failed to parse Subscription Name: " + e.getMessage()))
                    .build());
        }
        SubscriberStub subscriber = null;
        try {
            subscriber = getSubscriber(context);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Create Subscriber")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully created Subscriber")
                    .build());
        } catch (final IOException e) {
            verificationLogger.error("Failed to create Subscriber", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Create Subscriber")
                    .outcome(Outcome.FAILED)
                    .explanation(String.format("Failed to create Subscriber: " + e.getMessage()))
                    .build());
        }

        if (subscriber != null && subscriptionName != null) {
            try {
                final TestIamPermissionsRequest request = TestIamPermissionsRequest.newBuilder().addAllPermissions(REQUIRED_PERMISSIONS).setResource(subscriptionName).build();
                if (subscriber.testIamPermissionsCallable().call(request).getPermissionsCount() >= REQUIRED_PERMISSIONS.size()) {
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Test IAM Permissions")
                            .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                            .explanation(String.format("Verified Subscription [%s] exists and the configured user has the correct permissions.", subscriptionName))
                            .build());
                } else {
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Test IAM Permissions")
                            .outcome(ConfigVerificationResult.Outcome.FAILED)
                            .explanation(String.format("The configured user does not have the correct permissions on Subscription [%s].", subscriptionName))
                            .build());
                }
            } catch (final ApiException e) {
                verificationLogger.error("The configured user appears to have the correct permissions, but the following error was encountered", e);
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Test IAM Permissions")
                        .outcome(ConfigVerificationResult.Outcome.FAILED)
                        .explanation(String.format("The configured user appears to have the correct permissions, but the following error was encountered: " + e.getMessage()))
                        .build());
            }
        }
        return results;
    }

    @OnStopped
    public void onStopped() {
        if (subscriber != null) {
            subscriber.shutdown();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (subscriber == null) {
            if (storedException.get() != null) {
                getLogger().error("Failed to create Google Cloud PubSub subscriber due to {}", storedException.get());
            } else {
                getLogger().error("Google Cloud PubSub Subscriber was not properly created. Yielding the processor...");
            }

            context.yield();
            return;
        }

        final PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
        final List<String> ackIds = new ArrayList<>();
        final String subscriptionName = getSubscriptionName(context, null);
        List<ReceivedMessage> receivedMessages = pullResponse.getReceivedMessagesList();

        switch (processingStrategy) {
        case RECORD -> processInputRecords(context, session, receivedMessages, subscriptionName, ackIds);
        case FLOW_FILE -> processInputFlowFile(session, receivedMessages, subscriptionName, ackIds);
        case DEMARCATOR -> processInputDemarcator(context, session, receivedMessages, subscriptionName, ackIds);
        }

        session.commitAsync(() -> acknowledgeAcks(ackIds, subscriptionName));
    }

    private void processInputDemarcator(final ProcessContext context, final ProcessSession session, final List<ReceivedMessage> receivedMessages, final String subscriptionName,
            final List<String> ackIds) {
        final byte[] demarcator = demarcatorValue == null ? new byte[0] : demarcatorValue.getBytes(StandardCharsets.UTF_8);
        FlowFile flowFile = session.create();

        try {
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    for (ReceivedMessage message : receivedMessages) {
                        if (message.hasMessage()) {
                            out.write(message.getMessage().getData().toByteArray());
                            out.write(demarcator);
                            ackIds.add(message.getAckId());
                        }
                    }
                }
            });
            session.putAttribute(flowFile, SUBSCRIPTION_NAME_ATTRIBUTE, subscriptionName);
        } catch (Exception e) {
            ackIds.clear();
            session.remove(flowFile);
            throw new ProcessException("Failed to write batch of messages in FlowFile", e);
        }

        if (flowFile.getSize() > 0) {
            session.putAttribute(flowFile, "record.count", String.valueOf(ackIds.size()));
            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().receive(flowFile, subscriptionName);
            session.adjustCounter("Records Received from " + subscriptionName, ackIds.size(), false);
        } else {
            session.remove(flowFile);
        }
    }

    private void processInputFlowFile(final ProcessSession session, final List<ReceivedMessage> receivedMessages, final String subscriptionName, final List<String> ackIds) {
        for (ReceivedMessage message : receivedMessages) {
            if (message.hasMessage()) {
                FlowFile flowFile = session.create();

                final Map<String, String> attributes = new HashMap<>();
                attributes.put(ACK_ID_ATTRIBUTE, message.getAckId());
                attributes.put(SERIALIZED_SIZE_ATTRIBUTE, String.valueOf(message.getSerializedSize()));
                attributes.put(MESSAGE_ID_ATTRIBUTE, message.getMessage().getMessageId());
                attributes.put(MSG_ATTRIBUTES_COUNT_ATTRIBUTE, String.valueOf(message.getMessage().getAttributesCount()));
                attributes.put(MSG_PUBLISH_TIME_ATTRIBUTE, String.valueOf(message.getMessage().getPublishTime().getSeconds()));
                attributes.put(SUBSCRIPTION_NAME_ATTRIBUTE, subscriptionName);
                attributes.putAll(message.getMessage().getAttributesMap());

                flowFile = session.putAllAttributes(flowFile, attributes);
                flowFile = session.write(flowFile, out -> out.write(message.getMessage().getData().toByteArray()));

                ackIds.add(message.getAckId());
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().receive(flowFile, subscriptionName);
            }
        }
    }

    private void processInputRecords(final ProcessContext context, final ProcessSession session, final List<ReceivedMessage> receivedMessages, final String subscriptionName,
            final List<String> ackIds) {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        PubSubMessageConverter converter;
        if (OutputStrategy.USE_VALUE.equals(outputStrategy)) {
            converter = new RecordStreamPubSubMessageConverter(readerFactory, writerFactory, getLogger());
        } else if (OutputStrategy.USE_WRAPPER.equals(outputStrategy)) {
            converter = new WrapperRecordStreamPubSubMessageConverter(readerFactory, writerFactory, getLogger());
        } else {
            throw new ProcessException(String.format("Output Strategy not supported [%s]", outputStrategy));
        }

        converter.toFlowFiles(session, receivedMessages, ackIds, subscriptionName);
    }

    private void acknowledgeAcks(final Collection<String> ackIds, final String subscriptionName) {
        if (ackIds == null || ackIds.isEmpty()) {
            return;
        }

        AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
                .addAllAckIds(ackIds)
                .setSubscription(subscriptionName)
                .build();
        subscriber.acknowledgeCallable().call(acknowledgeRequest);
    }

    private String getSubscriptionName(final ProcessContext context, final Map<String, String> additionalAttributes) {
        final String subscriptionName = context.getProperty(SUBSCRIPTION).evaluateAttributeExpressions(additionalAttributes).getValue();
        final String projectId = context.getProperty(PROJECT_ID).evaluateAttributeExpressions(additionalAttributes).getValue();

        if (subscriptionName.contains("/")) {
            return ProjectSubscriptionName.parse(subscriptionName).toString();
        } else {
            return ProjectSubscriptionName.of(projectId, subscriptionName).toString();
        }

    }

    private SubscriberStub getSubscriber(final ProcessContext context) throws IOException {
        final String endpoint = context.getProperty(API_ENDPOINT).getValue();

        final SubscriberStubSettings.Builder subscriberBuilder = SubscriberStubSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(getGoogleCredentials(context)))
                .setTransportChannelProvider(getTransportChannelProvider(context))
                .setEndpoint(endpoint);

        return GrpcSubscriberStub.create(subscriberBuilder.build());
    }
}
