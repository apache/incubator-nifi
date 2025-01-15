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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.stub.GrpcPublisherStub;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
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
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.pubsub.publish.FlowFileResult;
import org.apache.nifi.processors.gcp.pubsub.publish.MessageDerivationStrategy;
import org.apache.nifi.processors.gcp.pubsub.publish.TrackedApiFutureCallback;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StopWatch;
import org.threeten.bp.Duration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.RECORDS_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.RECORDS_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.TOPIC_NAME_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.TOPIC_NAME_DESCRIPTION;

@SeeAlso({ConsumeGCPubSub.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"google", "google-cloud", "gcp", "message", "pubsub", "publish"})
@CapabilityDescription("Publishes the content of the incoming flowfile to the configured Google Cloud PubSub topic. The processor supports dynamic properties." +
        " If any dynamic properties are present, they will be sent along with the message in the form of 'attributes'.")
@DynamicProperty(name = "Attribute name", value = "Value to be set to the attribute",
        description = "Attributes to be set for the outgoing Google Cloud PubSub message", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@WritesAttributes({
        @WritesAttribute(attribute = MESSAGE_ID_ATTRIBUTE, description = MESSAGE_ID_DESCRIPTION),
        @WritesAttribute(attribute = RECORDS_ATTRIBUTE, description = RECORDS_DESCRIPTION),
        @WritesAttribute(attribute = TOPIC_NAME_ATTRIBUTE, description = TOPIC_NAME_DESCRIPTION)
})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The entirety of the FlowFile's content "
        + "will be read into memory to be sent as a PubSub message.")
public class PublishGCPubSub extends AbstractGCPubSubWithProxyProcessor {
    private static final List<String> REQUIRED_PERMISSIONS = Collections.singletonList("pubsub.topics.publish");
    private static final String TRANSIT_URI_FORMAT_STRING = "gcp://%s";

    public static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Input Batch Size")
            .displayName("Input Batch Size")
            .description("Maximum number of FlowFiles processed for each Processor invocation")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor MESSAGE_DERIVATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Message Derivation Strategy")
            .displayName("Message Derivation Strategy")
            .description("The strategy used to publish the incoming FlowFile to the Google Cloud PubSub endpoint.")
            .required(true)
            .defaultValue(MessageDerivationStrategy.FLOWFILE_ORIENTED.getValue())
            .allowableValues(MessageDerivationStrategy.class)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(MESSAGE_DERIVATION_STRATEGY, MessageDerivationStrategy.RECORD_ORIENTED.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before sending to GCPubSub endpoint")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(MESSAGE_DERIVATION_STRATEGY, MessageDerivationStrategy.RECORD_ORIENTED.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor MAX_MESSAGE_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Message Size")
            .displayName("Maximum Message Size")
            .description("The maximum size of a Google PubSub message in bytes. Defaults to 1 MB (1048576 bytes)")
            .dependsOn(MESSAGE_DERIVATION_STRATEGY, MessageDerivationStrategy.FLOWFILE_ORIENTED.getValue())
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();

    public static final PropertyDescriptor TOPIC_NAME = new PropertyDescriptor.Builder()
            .name("gcp-pubsub-topic")
            .displayName("Topic Name")
            .description("Name of the Google Cloud PubSub Topic")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("FlowFiles are routed to this relationship if the Google Cloud Pub/Sub operation fails but attempting the operation again may succeed.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GCP_CREDENTIALS_PROVIDER_SERVICE,
            PROJECT_ID,
            TOPIC_NAME,
            MESSAGE_DERIVATION_STRATEGY,
            RECORD_READER,
            RECORD_WRITER,
            MAX_BATCH_SIZE,
            MAX_MESSAGE_SIZE,
            BATCH_SIZE_THRESHOLD,
            BATCH_BYTES_THRESHOLD,
            BATCH_DELAY_THRESHOLD,
            API_ENDPOINT,
            PROXY_CONFIGURATION_SERVICE
    );

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_RETRY
    );

    protected Publisher publisher = null;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        try {
            publisher = getPublisherBuilder(context).build();
        } catch (IOException e) {
            throw new ProcessException("Failed to create Google Cloud PubSub Publisher", e);
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        Publisher publisher = null;
        try {
            publisher = getPublisherBuilder(context).build();
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Create Publisher")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully created Publisher")
                    .build());
        } catch (final IOException e) {
            verificationLogger.error("Failed to create Publisher", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Create Publisher")
                    .outcome(Outcome.FAILED)
                    .explanation(String.format("Failed to create Publisher: " + e.getMessage()))
                    .build());
        }

        if (publisher != null) {
            try {
                final PublisherStubSettings publisherStubSettings = PublisherStubSettings.newBuilder()
                        .setCredentialsProvider(FixedCredentialsProvider.create(getGoogleCredentials(context)))
                        .setTransportChannelProvider(getTransportChannelProvider(context))
                        .build();

                try (GrpcPublisherStub publisherStub = GrpcPublisherStub.create(publisherStubSettings)) {
                    final String topicName = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions().getValue();
                    final TestIamPermissionsRequest request = TestIamPermissionsRequest.newBuilder()
                            .addAllPermissions(REQUIRED_PERMISSIONS)
                            .setResource(topicName)
                            .build();
                    final TestIamPermissionsResponse response = publisherStub.testIamPermissionsCallable().call(request);
                    if (response.getPermissionsCount() >= REQUIRED_PERMISSIONS.size()) {
                        results.add(new ConfigVerificationResult.Builder()
                                .verificationStepName("Test IAM Permissions")
                                .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                                .explanation(String.format("Verified Topic [%s] exists and the configured user has the correct permissions.", topicName))
                                .build());
                    } else {
                        results.add(new ConfigVerificationResult.Builder()
                                .verificationStepName("Test IAM Permissions")
                                .outcome(ConfigVerificationResult.Outcome.FAILED)
                                .explanation(String.format("The configured user does not have the correct permissions on Topic [%s].", topicName))
                                .build());
                    }
                }
            } catch (final ApiException e) {
                verificationLogger.error("The configured user appears to have the correct permissions, but the following error was encountered", e);
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Test IAM Permissions")
                        .outcome(ConfigVerificationResult.Outcome.FAILED)
                        .explanation(String.format("The configured user appears to have the correct permissions, but the following error was encountered: " + e.getMessage()))
                        .build());
            } catch (final IOException e) {
                verificationLogger.error("The publisher stub could not be created in order to test the permissions", e);
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Test IAM Permissions")
                        .outcome(ConfigVerificationResult.Outcome.FAILED)
                        .explanation(String.format("The publisher stub could not be created in order to test the permissions: " + e.getMessage()))
                        .build());

            }
        }
        return results;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final StopWatch stopWatch = new StopWatch(true);
        final MessageDerivationStrategy inputStrategy = MessageDerivationStrategy.valueOf(context.getProperty(MESSAGE_DERIVATION_STRATEGY).getValue());
        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
        final List<FlowFile> flowFileBatch = session.get(maxBatchSize);
        if (flowFileBatch.isEmpty()) {
            context.yield();
        } else if (MessageDerivationStrategy.FLOWFILE_ORIENTED.equals(inputStrategy)) {
            onTriggerFlowFileStrategy(context, session, stopWatch, flowFileBatch);
        } else if (MessageDerivationStrategy.RECORD_ORIENTED.equals(inputStrategy)) {
            onTriggerRecordStrategy(context, session, stopWatch, flowFileBatch);
        } else {
            throw new IllegalStateException(inputStrategy.getValue());
        }
    }

    private void onTriggerFlowFileStrategy(
            final ProcessContext context,
            final ProcessSession session,
            final StopWatch stopWatch,
            final List<FlowFile> flowFileBatch) throws ProcessException {
        final long maxMessageSize = context.getProperty(MAX_MESSAGE_SIZE).asDataSize(DataUnit.B).longValue();

        final Executor executor = MoreExecutors.directExecutor();
        final List<FlowFileResult> flowFileResults = new ArrayList<>();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (final FlowFile flowFile : flowFileBatch) {
            final List<ApiFuture<String>> futures = new ArrayList<>();
            final List<String> successes = Collections.synchronizedList(new ArrayList<>());
            final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

            if (flowFile.getSize() > maxMessageSize) {
                final String message = String.format("FlowFile size %d exceeds MAX_MESSAGE_SIZE", flowFile.getSize());
                failures.add(new IllegalArgumentException(message));
                flowFileResults.add(new FlowFileResult(flowFile, futures, successes, failures));
            } else {
                baos.reset();
                session.exportTo(flowFile, baos);

                final ApiFuture<String> apiFuture = publishOneMessage(context, flowFile, baos.toByteArray());
                futures.add(apiFuture);
                addCallback(apiFuture, new TrackedApiFutureCallback(successes, failures), executor);
                flowFileResults.add(new FlowFileResult(flowFile, futures, successes, failures));
            }
        }
        finishBatch(session, stopWatch, flowFileResults);
    }

    private void onTriggerRecordStrategy(
            final ProcessContext context,
            final ProcessSession session,
            final StopWatch stopWatch,
            final List<FlowFile> flowFileBatch) throws ProcessException {
        try {
            onTriggerRecordStrategyPublishRecords(context, session, stopWatch, flowFileBatch);
        } catch (IOException | SchemaNotFoundException | MalformedRecordException e) {
            throw new ProcessException("Record publishing failed", e);
        }
    }

    private void onTriggerRecordStrategyPublishRecords(
            final ProcessContext context,
            final ProcessSession session,
            final StopWatch stopWatch,
            final List<FlowFile> flowFileBatch)
            throws ProcessException, IOException, SchemaNotFoundException, MalformedRecordException {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final Executor executor = MoreExecutors.directExecutor();
        final List<FlowFileResult> flowFileResults = new ArrayList<>();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (final FlowFile flowFile : flowFileBatch) {
            final List<ApiFuture<String>> futures = new ArrayList<>();
            final List<String> successes = Collections.synchronizedList(new ArrayList<>());
            final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

            final Map<String, String> attributes = flowFile.getAttributes();
            try (final RecordReader reader = readerFactory.createRecordReader(
                    attributes, session.read(flowFile), flowFile.getSize(), getLogger())) {
                final RecordSet recordSet = reader.createRecordSet();
                final RecordSchema schema = writerFactory.getSchema(attributes, recordSet.getSchema());

                final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, baos, attributes);
                final PushBackRecordSet pushBackRecordSet = new PushBackRecordSet(recordSet);

                while (pushBackRecordSet.isAnotherRecord()) {
                    final ApiFuture<String> apiFuture = publishOneRecord(context, flowFile, baos, writer, pushBackRecordSet.next());
                    futures.add(apiFuture);
                    addCallback(apiFuture, new TrackedApiFutureCallback(successes, failures), executor);
                }
                flowFileResults.add(new FlowFileResult(flowFile, futures, successes, failures));
            }
        }
        finishBatch(session, stopWatch, flowFileResults);
    }

    private ApiFuture<String> publishOneRecord(
            final ProcessContext context,
            final FlowFile flowFile,
            final ByteArrayOutputStream baos,
            final RecordSetWriter writer,
            final Record record) throws IOException {
        baos.reset();
        writer.write(record);
        writer.flush();
        return publishOneMessage(context, flowFile, baos.toByteArray());
    }

    private ApiFuture<String> publishOneMessage(final ProcessContext context,
                                                final FlowFile flowFile,
                                                final byte[] content) {
        final PubsubMessage message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(content))
                .setPublishTime(Timestamp.newBuilder().build())
                .putAllAttributes(getDynamicAttributesMap(context, flowFile))
                .build();
        return publisher.publish(message);
    }

    private void finishBatch(final ProcessSession session,
                             final StopWatch stopWatch,
                             final List<FlowFileResult> flowFileResults) {
        final String topicName = publisher.getTopicNameString();
        for (final FlowFileResult flowFileResult : flowFileResults) {
            final Relationship relationship = flowFileResult.reconcile();
            final Map<String, String> attributes = flowFileResult.getAttributes();
            attributes.put(TOPIC_NAME_ATTRIBUTE, topicName);
            final FlowFile flowFile = session.putAllAttributes(flowFileResult.getFlowFile(), attributes);
            final String transitUri = String.format(TRANSIT_URI_FORMAT_STRING, topicName);
            session.getProvenanceReporter().send(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, relationship);
        }
    }

    protected void addCallback(final ApiFuture<String> apiFuture, final ApiFutureCallback<? super String> callback, Executor executor) {
        ApiFutures.addCallback(apiFuture, callback, executor);
    }

    @OnStopped
    public void onStopped() {
        shutdownPublisher();
    }

    private void shutdownPublisher() {
        try {
            if (publisher != null) {
                publisher.shutdown();
            }
        } catch (Exception e) {
            getLogger().warn("Failed to gracefully shutdown the Google Cloud PubSub Publisher", e);
        }
    }

    private ProjectTopicName getTopicName(ProcessContext context) {
        final String topic = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions().getValue();
        final String projectId = context.getProperty(PROJECT_ID).evaluateAttributeExpressions().getValue();

        if (topic.contains("/")) {
            return ProjectTopicName.parse(topic);
        } else {
            return ProjectTopicName.of(projectId, topic);
        }
    }

    private Map<String, String> getDynamicAttributesMap(ProcessContext context, FlowFile flowFile) {
        final Map<String, String> attributes = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                final String value = context.getProperty(entry.getKey()).evaluateAttributeExpressions(flowFile).getValue();
                attributes.put(entry.getKey().getName(), value);
            }
        }

        return attributes;
    }

    private Publisher.Builder getPublisherBuilder(ProcessContext context) {
        final Long batchSizeThreshold = context.getProperty(BATCH_SIZE_THRESHOLD).asLong();
        final long batchBytesThreshold = context.getProperty(BATCH_BYTES_THRESHOLD).asDataSize(DataUnit.B).longValue();
        final Long batchDelayThreshold = context.getProperty(BATCH_DELAY_THRESHOLD).asTimePeriod(TimeUnit.MILLISECONDS);
        final String endpoint = context.getProperty(API_ENDPOINT).getValue();

        final Publisher.Builder publisherBuilder = Publisher.newBuilder(getTopicName(context))
                .setCredentialsProvider(FixedCredentialsProvider.create(getGoogleCredentials(context)))
                .setChannelProvider(getTransportChannelProvider(context))
                .setEndpoint(endpoint);

        publisherBuilder.setBatchingSettings(BatchingSettings.newBuilder()
                .setElementCountThreshold(batchSizeThreshold)
                .setRequestByteThreshold(batchBytesThreshold)
                .setDelayThreshold(Duration.ofMillis(batchDelayThreshold))
                .setIsEnabled(true)
                .build());

        // Set fixed thread pool executor to number of concurrent tasks
        publisherBuilder.setExecutorProvider(FixedExecutorProvider.create(new ScheduledThreadPoolExecutor(context.getMaxConcurrentTasks())));

        return publisherBuilder;
    }
}
