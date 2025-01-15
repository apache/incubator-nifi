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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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

@SeeAlso({PublishGCPubSub.class})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"google", "google-cloud", "gcp", "message", "pubsub", "consume"})
@CapabilityDescription("Consumes message from the configured Google Cloud PubSub subscription. If the 'Batch Size' is set, " +
        "the configured number of messages will be pulled in a single request, else only one message will be pulled.")
@WritesAttributes({
        @WritesAttribute(attribute = ACK_ID_ATTRIBUTE, description = ACK_ID_DESCRIPTION),
        @WritesAttribute(attribute = SERIALIZED_SIZE_ATTRIBUTE, description = SERIALIZED_SIZE_DESCRIPTION),
        @WritesAttribute(attribute = MSG_ATTRIBUTES_COUNT_ATTRIBUTE, description = MSG_ATTRIBUTES_COUNT_DESCRIPTION),
        @WritesAttribute(attribute = MSG_PUBLISH_TIME_ATTRIBUTE, description = MSG_PUBLISH_TIME_DESCRIPTION),
        @WritesAttribute(attribute = DYNAMIC_ATTRIBUTES_ATTRIBUTE, description = DYNAMIC_ATTRIBUTES_DESCRIPTION)
})
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

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GCP_CREDENTIALS_PROVIDER_SERVICE,
            PROJECT_ID,
            SUBSCRIPTION,
            BATCH_SIZE_THRESHOLD,
            API_ENDPOINT,
            PROXY_CONFIGURATION_SERVICE
    );

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private SubscriberStub subscriber = null;
    private PullRequest pullRequest;

    private final AtomicReference<Exception> storedException = new AtomicReference<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

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

        for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
            if (message.hasMessage()) {
                FlowFile flowFile = session.create();

                final Map<String, String> attributes = new HashMap<>();
                ackIds.add(message.getAckId());

                attributes.put(ACK_ID_ATTRIBUTE, message.getAckId());
                attributes.put(SERIALIZED_SIZE_ATTRIBUTE, String.valueOf(message.getSerializedSize()));
                attributes.put(MESSAGE_ID_ATTRIBUTE, message.getMessage().getMessageId());
                attributes.put(MSG_ATTRIBUTES_COUNT_ATTRIBUTE, String.valueOf(message.getMessage().getAttributesCount()));
                attributes.put(MSG_PUBLISH_TIME_ATTRIBUTE, String.valueOf(message.getMessage().getPublishTime().getSeconds()));
                attributes.putAll(message.getMessage().getAttributesMap());

                flowFile = session.putAllAttributes(flowFile, attributes);
                flowFile = session.write(flowFile, out -> out.write(message.getMessage().getData().toByteArray()));

                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().receive(flowFile, subscriptionName);
            }
        }

        session.commitAsync(() -> acknowledgeAcks(ackIds, subscriptionName));
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
