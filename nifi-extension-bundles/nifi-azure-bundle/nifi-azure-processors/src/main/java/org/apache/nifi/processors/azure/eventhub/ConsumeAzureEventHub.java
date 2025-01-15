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
package org.apache.nifi.processors.azure.eventhub;

import com.azure.core.amqp.AmqpClientOptions;
import com.azure.core.amqp.AmqpTransportType;
import com.azure.core.amqp.exception.AmqpErrorCondition;
import com.azure.core.amqp.exception.AmqpException;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.core.http.ProxyOptions;
import com.azure.core.util.HttpClientOptions;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventBatchContext;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.LastEnqueuedEventProperties;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.eventhub.checkpoint.CheckpointStrategy;
import org.apache.nifi.processors.azure.eventhub.checkpoint.ComponentStateCheckpointStore;
import org.apache.nifi.processors.azure.eventhub.checkpoint.exception.ComponentStateCheckpointStoreException;
import org.apache.nifi.processors.azure.eventhub.position.EarliestEventPositionProvider;
import org.apache.nifi.processors.azure.eventhub.position.LegacyBlobStorageEventPositionProvider;
import org.apache.nifi.processors.azure.eventhub.utils.AzureEventHubUtils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubComponent;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubTransportType;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.CheckpointStoreKey.CLIENT_ID;
import static org.apache.nifi.processors.azure.eventhub.checkpoint.CheckpointStoreKey.CLUSTERED;

@Tags({"azure", "microsoft", "cloud", "eventhub", "events", "streaming", "streams"})
@CapabilityDescription("Receives messages from Microsoft Azure Event Hubs with checkpointing to ensure consistent event processing. "
        + "Checkpoint tracking avoids consuming a message multiple times and enables reliable resumption of processing in the event of intermittent network failures. "
        + "Checkpoint tracking requires external storage and provides the preferred approach to consuming messages from Azure Event Hubs. "
        + "In clustered environment, ConsumeAzureEventHub processor instances form a consumer group and the messages are distributed among the cluster nodes "
        + "(each message is processed on one cluster node only).")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER}, description = "Local state is used to store the client id. " +
        "Cluster state is used to store partition ownership and checkpoint information when component state is configured as the checkpointing strategy.")
@TriggerSerially
@WritesAttributes({
        @WritesAttribute(attribute = "eventhub.enqueued.timestamp", description = "The time (in milliseconds since epoch, UTC) at which the message was enqueued in the event hub"),
        @WritesAttribute(attribute = "eventhub.offset", description = "The offset into the partition at which the message was stored"),
        @WritesAttribute(attribute = "eventhub.sequence", description = "The sequence number associated with the message"),
        @WritesAttribute(attribute = "eventhub.name", description = "The name of the event hub from which the message was pulled"),
        @WritesAttribute(attribute = "eventhub.partition", description = "The name of the partition from which the message was pulled"),
        @WritesAttribute(attribute = "eventhub.property.*", description = "The application properties of this message. IE: 'application' would be 'eventhub.property.application'")
})
public class ConsumeAzureEventHub extends AbstractSessionFactoryProcessor implements AzureEventHubComponent {

    private static final Pattern SAS_TOKEN_PATTERN = Pattern.compile("^\\?.*$");
    private static final String FORMAT_STORAGE_CONNECTION_STRING_FOR_ACCOUNT_KEY = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.%s";
    private static final String FORMAT_STORAGE_CONNECTION_STRING_FOR_SAS_TOKEN = "BlobEndpoint=https://%s.blob.core.%s/;SharedAccessSignature=%s";

    static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("event-hub-namespace")
            .displayName("Event Hub Namespace")
            .description("The namespace that the Azure Event Hubs is assigned to. This is generally equal to <Event Hub Names>-ns.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();
    static final PropertyDescriptor EVENT_HUB_NAME = new PropertyDescriptor.Builder()
            .name("event-hub-name")
            .displayName("Event Hub Name")
            .description("The name of the event hub to pull messages from.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();
    static final PropertyDescriptor SERVICE_BUS_ENDPOINT = AzureEventHubUtils.SERVICE_BUS_ENDPOINT;
    static final PropertyDescriptor ACCESS_POLICY_NAME = new PropertyDescriptor.Builder()
            .name("event-hub-shared-access-policy-name")
            .displayName("Shared Access Policy Name")
            .description("The name of the shared access policy. This policy must have Listen claims.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .build();
    static final PropertyDescriptor POLICY_PRIMARY_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureEventHubUtils.POLICY_PRIMARY_KEY)
            .name("event-hub-shared-access-policy-primary-key")
            .build();
    static final PropertyDescriptor USE_MANAGED_IDENTITY = AzureEventHubUtils.USE_MANAGED_IDENTITY;
    static final PropertyDescriptor CONSUMER_GROUP = new PropertyDescriptor.Builder()
            .name("event-hub-consumer-group")
            .displayName("Consumer Group")
            .description("The name of the consumer group to use.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("$Default")
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for reading received messages." +
                    " The event hub name can be referred by Expression Language '${eventhub.name}' to access a schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use for serializing Records to an output FlowFile." +
                    " The event hub name can be referred by Expression Language '${eventhub.name}' to access a schema." +
                    " If not specified, each message will create a FlowFile.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    static final AllowableValue INITIAL_OFFSET_START_OF_STREAM = new AllowableValue(
            "start-of-stream", "Start of stream", "Read from the oldest message retained in the stream.");
    static final AllowableValue INITIAL_OFFSET_END_OF_STREAM = new AllowableValue(
            "end-of-stream", "End of stream",
            "Ignore old retained messages even if exist, start reading new ones from now.");
    static final PropertyDescriptor INITIAL_OFFSET = new PropertyDescriptor.Builder()
            .name("event-hub-initial-offset")
            .displayName("Initial Offset")
            .description("Specify where to start receiving messages if offset is not yet stored in the checkpoint store.")
            .required(true)
            .allowableValues(INITIAL_OFFSET_START_OF_STREAM, INITIAL_OFFSET_END_OF_STREAM)
            .defaultValue(INITIAL_OFFSET_END_OF_STREAM)
            .build();
    static final PropertyDescriptor PREFETCH_COUNT = new PropertyDescriptor.Builder()
            .name("event-hub-prefetch-count")
            .displayName("Prefetch Count")
            .defaultValue("The number of messages to fetch from the event hub before processing." +
                    " This parameter affects throughput." +
                    " The more prefetch count, the better throughput in general, but consumes more resources (RAM)." +
                    " NOTE: Even though the event hub client API provides this option," +
                    " actual number of messages can be pre-fetched is depend on the Event Hubs server implementation." +
                    " It is reported that only one event is received at a time in certain situation." +
                    " https://github.com/Azure/azure-event-hubs-java/issues/125")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("300")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();
    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("event-hub-batch-size")
            .displayName("Batch Size")
            .description("The number of messages to process within a NiFi session." +
                    " This parameter affects throughput and consistency." +
                    " NiFi commits its session and Event Hubs checkpoints after processing this number of messages." +
                    " If NiFi session is committed, but fails to create an Event Hubs checkpoint," +
                    " then it is possible that the same messages will be received again." +
                    " The higher number, the higher throughput, but possibly less consistent.")
            .defaultValue("10")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();
    static final PropertyDescriptor RECEIVE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("event-hub-message-receive-timeout")
            .displayName("Message Receive Timeout")
            .description("The amount of time this consumer should wait to receive the Batch Size before returning.")
            .defaultValue("1 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    static final PropertyDescriptor CHECKPOINT_STRATEGY = new PropertyDescriptor.Builder()
            .name("checkpoint-strategy")
            .displayName("Checkpoint Strategy")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .allowableValues(CheckpointStrategy.class)
            .defaultValue(CheckpointStrategy.AZURE_BLOB_STORAGE.getValue())
            .description("Specifies which strategy to use for storing and retrieving partition ownership and checkpoint information for each partition.")
            .build();

    static final PropertyDescriptor STORAGE_ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name("storage-account-name")
            .displayName("Storage Account Name")
            .description("Name of the Azure Storage account to store event hub consumer group state.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .dependsOn(CHECKPOINT_STRATEGY, CheckpointStrategy.AZURE_BLOB_STORAGE)
            .build();
    static final PropertyDescriptor STORAGE_ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .name("storage-account-key")
            .displayName("Storage Account Key")
            .description("The Azure Storage account key to store event hub consumer group state.")
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .dependsOn(CHECKPOINT_STRATEGY, CheckpointStrategy.AZURE_BLOB_STORAGE)
            .build();
    static final PropertyDescriptor STORAGE_SAS_TOKEN = new PropertyDescriptor.Builder()
            .name("storage-sas-token")
            .displayName("Storage SAS Token")
            .description("The Azure Storage SAS token to store Event Hub consumer group state. Always starts with a ? character.")
            .sensitive(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(SAS_TOKEN_PATTERN, true,
                    "Token must start with a ? character."))
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .dependsOn(CHECKPOINT_STRATEGY, CheckpointStrategy.AZURE_BLOB_STORAGE)
            .build();
    static final PropertyDescriptor STORAGE_CONTAINER_NAME = new PropertyDescriptor.Builder()
            .name("storage-container-name")
            .displayName("Storage Container Name")
            .description("Name of the Azure Storage container to store the event hub consumer group state." +
                    " If not specified, event hub name is used.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .dependsOn(CHECKPOINT_STRATEGY, CheckpointStrategy.AZURE_BLOB_STORAGE)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles received from Event Hub.")
            .build();

    static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse.failure")
            .description("If a message from event hub cannot be parsed using the configured Record Reader" +
                    " or failed to be written by the configured Record Writer," +
                    " the contents of the message will be routed to this Relationship as its own individual FlowFile.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private static final Set<Relationship> RECORD_RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_PARSE_FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            NAMESPACE,
            EVENT_HUB_NAME,
            SERVICE_BUS_ENDPOINT,
            TRANSPORT_TYPE,
            ACCESS_POLICY_NAME,
            POLICY_PRIMARY_KEY,
            USE_MANAGED_IDENTITY,
            CONSUMER_GROUP,
            RECORD_READER,
            RECORD_WRITER,
            INITIAL_OFFSET,
            PREFETCH_COUNT,
            BATCH_SIZE,
            RECEIVE_TIMEOUT,
            CHECKPOINT_STRATEGY,
            STORAGE_ACCOUNT_NAME,
            STORAGE_ACCOUNT_KEY,
            STORAGE_SAS_TOKEN,
            STORAGE_CONTAINER_NAME,
            PROXY_CONFIGURATION_SERVICE
    );

    private volatile ProcessSessionFactory processSessionFactory;
    private volatile EventProcessorClient eventProcessorClient;
    private volatile RecordReaderFactory readerFactory;
    private volatile RecordSetWriterFactory writerFactory;

    private volatile boolean isRecordReaderSet = false;
    private volatile boolean isRecordWriterSet = false;

    private volatile String clientId;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return isRecordReaderSet && isRecordWriterSet ? RECORD_RELATIONSHIPS : RELATIONSHIPS;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.removeProperty("event-hub-consumer-hostname");
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        final ControllerService recordReader = validationContext.getProperty(RECORD_READER).asControllerService();
        final ControllerService recordWriter = validationContext.getProperty(RECORD_WRITER).asControllerService();
        final String storageAccountKey = validationContext.getProperty(STORAGE_ACCOUNT_KEY).evaluateAttributeExpressions().getValue();
        final String storageSasToken = validationContext.getProperty(STORAGE_SAS_TOKEN).evaluateAttributeExpressions().getValue();
        final CheckpointStrategy checkpointStrategy = CheckpointStrategy.valueOf(validationContext.getProperty(CHECKPOINT_STRATEGY).getValue());

        if ((recordReader != null && recordWriter == null) || (recordReader == null && recordWriter != null)) {
            results.add(new ValidationResult.Builder()
                    .subject("Record Reader and Writer")
                    .explanation(String.format("Both %s and %s should be set in order to write FlowFiles as Records.",
                            RECORD_READER.getDisplayName(), RECORD_WRITER.getDisplayName()))
                    .valid(false)
                    .build());
        }

        if (checkpointStrategy == CheckpointStrategy.AZURE_BLOB_STORAGE) {
            if (StringUtils.isBlank(storageAccountKey) && StringUtils.isBlank(storageSasToken)) {
                results.add(new ValidationResult.Builder()
                        .subject(String.format("%s or %s",
                                STORAGE_ACCOUNT_KEY.getDisplayName(), STORAGE_SAS_TOKEN.getDisplayName()))
                        .explanation(String.format("either %s or %s should be set.",
                                STORAGE_ACCOUNT_KEY.getDisplayName(), STORAGE_SAS_TOKEN.getDisplayName()))
                        .valid(false)
                        .build());
            }

            if (StringUtils.isNotBlank(storageAccountKey) && StringUtils.isNotBlank(storageSasToken)) {
                results.add(new ValidationResult.Builder()
                        .subject(String.format("%s or %s",
                                STORAGE_ACCOUNT_KEY.getDisplayName(), STORAGE_SAS_TOKEN.getDisplayName()))
                        .explanation(String.format("%s and %s should not be set at the same time.",
                                STORAGE_ACCOUNT_KEY.getDisplayName(), STORAGE_SAS_TOKEN.getDisplayName()))
                        .valid(false)
                        .build());
            }
        }
        results.addAll(AzureEventHubUtils.customValidate(ACCESS_POLICY_NAME, POLICY_PRIMARY_KEY, validationContext));
        return results;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (RECORD_READER.equals(descriptor)) {
            isRecordReaderSet = StringUtils.isNotEmpty(newValue);
        } else if (RECORD_WRITER.equals(descriptor)) {
            isRecordWriterSet = StringUtils.isNotEmpty(newValue);
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        StateManager stateManager = context.getStateManager();

        String clientId = stateManager.getState(Scope.LOCAL).get(CLIENT_ID.key());
        if (clientId == null) {
            clientId = UUID.randomUUID().toString();

            final Map<String, String> clientState = new HashMap<>();
            clientState.put(CLIENT_ID.key(), clientId);
            clientState.put(CLUSTERED.key(), Boolean.toString(getNodeTypeProvider().isClustered()));

            stateManager.setState(clientState, Scope.LOCAL);
        }

        this.clientId = clientId;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        if (eventProcessorClient == null) {
            processSessionFactory = sessionFactory;
            readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

            eventProcessorClient = createClient(context);
            eventProcessorClient.start();
        }

        // After a EventProcessor is registered successfully, nothing has to be done at onTrigger
        // because new sessions are created when new messages are arrived by the EventProcessor.
        context.yield();
    }

    @OnStopped
    public void stopClient() {
        if (eventProcessorClient != null) {
            try {
                eventProcessorClient.stop();
            } catch (final Exception e) {
                getLogger().warn("Event Processor Client stop failed", e);
            }
            eventProcessorClient = null;
            processSessionFactory = null;
            readerFactory = null;
            writerFactory = null;
            clientId = null;
        }
    }

    protected EventProcessorClient createClient(final ProcessContext context) {
        final String eventHubNamespace = context.getProperty(NAMESPACE).evaluateAttributeExpressions().getValue();
        final String eventHubName = context.getProperty(EVENT_HUB_NAME).evaluateAttributeExpressions().getValue();
        final String serviceBusEndpoint = context.getProperty(SERVICE_BUS_ENDPOINT).getValue();
        final String consumerGroup = context.getProperty(CONSUMER_GROUP).evaluateAttributeExpressions().getValue();

        final String fullyQualifiedNamespace = String.format("%s%s", eventHubNamespace, serviceBusEndpoint);

        final CheckpointStore checkpointStore;
        final Map<String, EventPosition> legacyPartitionEventPosition;

        final CheckpointStrategy checkpointStrategy = CheckpointStrategy.valueOf(context.getProperty(CHECKPOINT_STRATEGY).getValue());

        if (checkpointStrategy == CheckpointStrategy.AZURE_BLOB_STORAGE) {
            final String containerName = defaultIfBlank(context.getProperty(STORAGE_CONTAINER_NAME).evaluateAttributeExpressions().getValue(), eventHubName);
            final String storageConnectionString = createStorageConnectionString(context);
            final BlobContainerClientBuilder blobContainerClientBuilder = new BlobContainerClientBuilder()
                    .connectionString(storageConnectionString)
                    .containerName(containerName);
            final ProxyOptions storageProxyOptions = AzureStorageUtils.getProxyOptions(context);
            if (storageProxyOptions != null) {
                blobContainerClientBuilder.clientOptions(new HttpClientOptions().setProxyOptions(storageProxyOptions));
            }
            final BlobContainerAsyncClient blobContainerAsyncClient = blobContainerClientBuilder.buildAsyncClient();
            checkpointStore = new BlobCheckpointStore(blobContainerAsyncClient);
            legacyPartitionEventPosition = getLegacyPartitionEventPosition(blobContainerAsyncClient, consumerGroup);
        } else {
            ComponentStateCheckpointStore componentStateCheckpointStore = new ComponentStateCheckpointStore(clientId, context.getStateManager());
            componentStateCheckpointStore.cleanUp(fullyQualifiedNamespace, eventHubName, consumerGroup);
            checkpointStore = componentStateCheckpointStore;
            legacyPartitionEventPosition = Collections.emptyMap();
        }

        final Long receiveTimeout = context.getProperty(RECEIVE_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final Duration maxWaitTime = Duration.ofMillis(receiveTimeout);
        final Integer maxBatchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final AmqpTransportType transportType = context.getProperty(TRANSPORT_TYPE).asAllowableValue(AzureEventHubTransportType.class).asAmqpTransportType();

        final EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
                .transportType(transportType)
                .consumerGroup(consumerGroup)
                .clientOptions(new AmqpClientOptions().setIdentifier(clientId))
                .trackLastEnqueuedEventProperties(true)
                .checkpointStore(checkpointStore)
                .processError(errorProcessor)
                .processEventBatch(eventBatchProcessor, maxBatchSize, maxWaitTime);

        final boolean useManagedIdentity = context.getProperty(USE_MANAGED_IDENTITY).asBoolean();
        if (useManagedIdentity) {
            final ManagedIdentityCredentialBuilder managedIdentityCredentialBuilder = new ManagedIdentityCredentialBuilder();
            final ManagedIdentityCredential managedIdentityCredential = managedIdentityCredentialBuilder.build();
            eventProcessorClientBuilder.credential(fullyQualifiedNamespace, eventHubName, managedIdentityCredential);
        } else {
            final String policyName = context.getProperty(ACCESS_POLICY_NAME).evaluateAttributeExpressions().getValue();
            final String policyKey = context.getProperty(POLICY_PRIMARY_KEY).getValue();
            final AzureNamedKeyCredential azureNamedKeyCredential = new AzureNamedKeyCredential(policyName, policyKey);
            eventProcessorClientBuilder.credential(fullyQualifiedNamespace, eventHubName, azureNamedKeyCredential);
        }

        final Integer prefetchCount = context.getProperty(PREFETCH_COUNT).evaluateAttributeExpressions().asInteger();
        if (prefetchCount != null && prefetchCount > 0) {
            eventProcessorClientBuilder.prefetchCount(prefetchCount);
        }

        if (legacyPartitionEventPosition.isEmpty()) {
            final String initialOffset = context.getProperty(INITIAL_OFFSET).getValue();
            // EventPosition.latest() is the default behavior is absence of existing checkpoints
            if (INITIAL_OFFSET_START_OF_STREAM.getValue().equals(initialOffset)) {
                final EarliestEventPositionProvider eventPositionProvider = new EarliestEventPositionProvider();
                final Map<String, EventPosition> partitionEventPosition = eventPositionProvider.getInitialPartitionEventPosition();
                eventProcessorClientBuilder.initialPartitionEventPosition(partitionEventPosition);
            }
        } else {
            eventProcessorClientBuilder.initialPartitionEventPosition(legacyPartitionEventPosition);
        }

        AzureEventHubUtils.getProxyOptions(context).ifPresent(eventProcessorClientBuilder::proxyOptions);

        return eventProcessorClientBuilder.buildEventProcessorClient();
    }

    protected String getTransitUri(final PartitionContext partitionContext) {
        return String.format("amqps://%s/%s/ConsumerGroups/%s/Partitions/%s",
                partitionContext.getFullyQualifiedNamespace(),
                partitionContext.getEventHubName(),
                partitionContext.getConsumerGroup(),
                partitionContext.getPartitionId()
        );
    }

    protected final Consumer<EventBatchContext> eventBatchProcessor = eventBatchContext -> {
        final ProcessSession session = processSessionFactory.createSession();

        try {
            final StopWatch stopWatch = new StopWatch(true);

            if (readerFactory == null || writerFactory == null) {
                writeFlowFiles(eventBatchContext, session, stopWatch);
            } else {
                writeRecords(eventBatchContext, session, stopWatch);
            }

            // Commit ProcessSession and then update Azure Event Hubs checkpoint status
            session.commitAsync(eventBatchContext::updateCheckpoint);
        } catch (final Exception e) {
            final PartitionContext partitionContext = eventBatchContext.getPartitionContext();
            getLogger().error("Event Batch processing failed Namespace [{}] Event Hub [{}] Consumer Group [{}] Partition [{}]",
                    partitionContext.getFullyQualifiedNamespace(),
                    partitionContext.getEventHubName(),
                    partitionContext.getConsumerGroup(),
                    partitionContext.getPartitionId(),
                    e
            );
            session.rollback();
        }
    };

    private final Consumer<ErrorContext> errorProcessor = errorContext -> {
        final PartitionContext partitionContext = errorContext.getPartitionContext();
        final Throwable throwable = errorContext.getThrowable();

        if (throwable instanceof AmqpException amqpException) {
            if (amqpException.getErrorCondition() == AmqpErrorCondition.LINK_STOLEN) {
                getLogger().info("Partition was stolen by another consumer instance from the consumer group. Namespace [{}] Event Hub [{}] Consumer Group [{}] Partition [{}]. {}",
                        partitionContext.getFullyQualifiedNamespace(),
                        partitionContext.getEventHubName(),
                        partitionContext.getConsumerGroup(),
                        partitionContext.getPartitionId(),
                        amqpException.getMessage());
                return;
            }
        }

        final String errorMessage;
        if (throwable instanceof ComponentStateCheckpointStoreException) {
            errorMessage = "Failed to access Component State Checkpoint Store";
        } else {
            errorMessage = "Receive Events failed";
        }

        getLogger().error("{}. Namespace [{}] Event Hub [{}] Consumer Group [{}] Partition [{}]",
                errorMessage,
                partitionContext.getFullyQualifiedNamespace(),
                partitionContext.getEventHubName(),
                partitionContext.getConsumerGroup(),
                partitionContext.getPartitionId(),
                throwable
        );
    };

    private void putEventHubAttributes(
            final Map<String, String> attributes,
            final PartitionContext partitionContext,
            final EventData eventData,
            final LastEnqueuedEventProperties lastEnqueuedEventProperties
    ) {
        if (lastEnqueuedEventProperties != null) {
            attributes.put("eventhub.enqueued.timestamp", String.valueOf(lastEnqueuedEventProperties.getEnqueuedTime()));
            attributes.put("eventhub.offset", String.valueOf(lastEnqueuedEventProperties.getOffset()));
            attributes.put("eventhub.sequence", String.valueOf(lastEnqueuedEventProperties.getSequenceNumber()));
        }

        final Map<String, String> applicationProperties = AzureEventHubUtils.getApplicationProperties(eventData.getProperties());
        attributes.putAll(applicationProperties);

        attributes.put("eventhub.name", partitionContext.getEventHubName());
        attributes.put("eventhub.partition", partitionContext.getPartitionId());
    }

    private void writeFlowFiles(
            final EventBatchContext eventBatchContext,
            final ProcessSession session,
            final StopWatch stopWatch
    ) {
        final PartitionContext partitionContext = eventBatchContext.getPartitionContext();
        final List<EventData> events = eventBatchContext.getEvents();
        events.forEach(eventData -> {
            final Map<String, String> attributes = new HashMap<>();
            putEventHubAttributes(attributes, partitionContext, eventData, eventBatchContext.getLastEnqueuedEventProperties());

            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, attributes);

            final byte[] body = eventData.getBody();
            flowFile = session.write(flowFile, outputStream -> outputStream.write(body));

            transferTo(REL_SUCCESS, session, stopWatch, partitionContext, flowFile);
        });
    }

    private void writeRecords(
            final EventBatchContext eventBatchContext,
            final ProcessSession session,
            final StopWatch stopWatch
    ) throws IOException {
        final PartitionContext partitionContext = eventBatchContext.getPartitionContext();
        final Map<String, String> schemaRetrievalVariables = new HashMap<>();
        schemaRetrievalVariables.put("eventhub.name", partitionContext.getEventHubName());

        final ComponentLog logger = getLogger();
        FlowFile flowFile = session.create();
        final Map<String, String> attributes = new HashMap<>();

        RecordSetWriter writer = null;
        EventData lastEventData = null;
        WriteResult lastWriteResult = null;
        int recordCount = 0;

        final LastEnqueuedEventProperties lastEnqueuedEventProperties = eventBatchContext.getLastEnqueuedEventProperties();
        final List<EventData> events = eventBatchContext.getEvents();

        try (final OutputStream out = session.write(flowFile)) {
            for (final EventData eventData : events) {
                final byte[] eventDataBytes = eventData.getBody();
                try (final InputStream in = new ByteArrayInputStream(eventDataBytes)) {
                    final RecordReader reader = readerFactory.createRecordReader(schemaRetrievalVariables, in, eventDataBytes.length, logger);

                    Record record;
                    while ((record = reader.nextRecord()) != null) {

                        if (writer == null) {
                            // Initialize the writer when the first record is read.
                            final RecordSchema readerSchema = record.getSchema();
                            final RecordSchema writeSchema = writerFactory.getSchema(schemaRetrievalVariables, readerSchema);
                            writer = writerFactory.createWriter(logger, writeSchema, out, flowFile);
                            writer.beginRecordSet();
                        }

                        lastWriteResult = writer.write(record);
                        recordCount += lastWriteResult.getRecordCount();
                    }

                    lastEventData = eventData;

                } catch (Exception e) {
                    // Write it to the parse failure relationship.
                    logger.error("Failed to parse message from Azure Event Hub using configured Record Reader and Writer", e);
                    FlowFile failed = session.create();
                    session.write(failed, o -> o.write(eventData.getBody()));
                    putEventHubAttributes(attributes, partitionContext, eventData, lastEnqueuedEventProperties);
                    failed = session.putAllAttributes(failed, attributes);
                    transferTo(REL_PARSE_FAILURE, session, stopWatch, partitionContext, failed);
                }
            }

            if (lastEventData != null) {
                putEventHubAttributes(attributes, partitionContext, lastEventData, lastEnqueuedEventProperties);

                attributes.put("record.count", String.valueOf(recordCount));
                if (writer != null) {
                    writer.finishRecordSet();
                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    if (lastWriteResult != null) {
                        attributes.putAll(lastWriteResult.getAttributes());
                    }

                    try {
                        writer.close();
                    } catch (final IOException e) {
                        logger.warn("Failed to close Record Writer", e);
                    }
                }
            }
        }

        if (lastEventData == null) {
            session.remove(flowFile);
        } else {
            flowFile = session.putAllAttributes(flowFile, attributes);
            transferTo(REL_SUCCESS, session, stopWatch, partitionContext, flowFile);
        }
    }

    private void transferTo(
            final Relationship relationship,
            final ProcessSession session,
            final StopWatch stopWatch,
            final PartitionContext partitionContext,
            final FlowFile flowFile
    ) {
        session.transfer(flowFile, relationship);
        final String transitUri = getTransitUri(partitionContext);
        session.getProvenanceReporter().receive(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
    }

    private String createStorageConnectionString(final ProcessContext context) {
        final String storageAccountName = context.getProperty(STORAGE_ACCOUNT_NAME).evaluateAttributeExpressions().getValue();
        final String serviceBusEndpoint = context.getProperty(SERVICE_BUS_ENDPOINT).getValue();
        final String domainName = serviceBusEndpoint.replace(".servicebus.", "");
        final String storageAccountKey = context.getProperty(STORAGE_ACCOUNT_KEY).evaluateAttributeExpressions().getValue();
        final String storageSasToken = context.getProperty(STORAGE_SAS_TOKEN).evaluateAttributeExpressions().getValue();

        if (storageAccountKey != null) {
            return String.format(FORMAT_STORAGE_CONNECTION_STRING_FOR_ACCOUNT_KEY, storageAccountName, storageAccountKey, domainName);
        }
        return String.format(FORMAT_STORAGE_CONNECTION_STRING_FOR_SAS_TOKEN, storageAccountName, domainName, storageSasToken);
    }

    private Map<String, EventPosition> getLegacyPartitionEventPosition(
            final BlobContainerAsyncClient blobContainerAsyncClient,
            final String consumerGroup
    ) {
        final LegacyBlobStorageEventPositionProvider legacyBlobStorageEventPositionProvider = new LegacyBlobStorageEventPositionProvider(
                blobContainerAsyncClient,
                consumerGroup
        );
        final Map<String, EventPosition> partitionEventPosition = legacyBlobStorageEventPositionProvider.getInitialPartitionEventPosition();

        for (final Map.Entry<String, EventPosition> partition : partitionEventPosition.entrySet()) {
            final String partitionId = partition.getKey();
            final EventPosition eventPosition = partition.getValue();
            getLogger().info("Loaded Event Position [{}] for Partition [{}] from Legacy Checkpoint Storage", eventPosition, partitionId);
        }

        return partitionEventPosition;
    }
}
