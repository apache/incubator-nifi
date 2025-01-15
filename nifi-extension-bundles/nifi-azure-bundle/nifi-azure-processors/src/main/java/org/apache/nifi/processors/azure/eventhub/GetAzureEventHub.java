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
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.eventhub.utils.AzureEventHubUtils;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubComponent;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubTransportType;
import org.apache.nifi.util.StopWatch;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"azure", "microsoft", "cloud", "eventhub", "events", "streaming", "streams"})
@CapabilityDescription("Receives messages from Microsoft Azure Event Hubs without reliable checkpoint tracking. "
        + "In clustered environment, GetAzureEventHub processor instances work independently and all cluster nodes process all messages "
        + "(unless running the processor in Primary Only mode). "
        + "ConsumeAzureEventHub offers the recommended approach to receiving messages from Azure Event Hubs. "
        + "This processor creates a thread pool for connections to Azure Event Hubs.")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = "eventhub.enqueued.timestamp", description = "The time (in milliseconds since epoch, UTC) at which the message was enqueued in the event hub"),
        @WritesAttribute(attribute = "eventhub.offset", description = "The offset into the partition at which the message was stored"),
        @WritesAttribute(attribute = "eventhub.sequence", description = "The Azure sequence number associated with the message"),
        @WritesAttribute(attribute = "eventhub.name", description = "The name of the event hub from which the message was pulled"),
        @WritesAttribute(attribute = "eventhub.partition", description = "The name of the event hub partition from which the message was pulled"),
        @WritesAttribute(attribute = "eventhub.property.*", description = "The application properties of this message. IE: 'application' would be 'eventhub.property.application'")
})
@SeeAlso(ConsumeAzureEventHub.class)
public class GetAzureEventHub extends AbstractProcessor implements AzureEventHubComponent {
    private static final String TRANSIT_URI_FORMAT_STRING = "amqps://%s/%s/ConsumerGroups/%s/Partitions/%s";
    private static final Duration DEFAULT_FETCH_TIMEOUT = Duration.ofSeconds(60);
    private static final int DEFAULT_FETCH_SIZE = 100;

    private static final String NODE_CLIENT_IDENTIFIER_FORMAT = "%s-%s";

    static final PropertyDescriptor EVENT_HUB_NAME = new PropertyDescriptor.Builder()
            .name("Event Hub Name")
            .description("Name of Azure Event Hubs source")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("Event Hub Namespace")
            .description("Namespace of Azure Event Hubs prefixed to Service Bus Endpoint domain")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();
    static final PropertyDescriptor SERVICE_BUS_ENDPOINT = AzureEventHubUtils.SERVICE_BUS_ENDPOINT;
    static final PropertyDescriptor ACCESS_POLICY = new PropertyDescriptor.Builder()
            .name("Shared Access Policy Name")
            .description("The name of the shared access policy. This policy must have Listen claims.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();
    static final PropertyDescriptor POLICY_PRIMARY_KEY = AzureEventHubUtils.POLICY_PRIMARY_KEY;
    static final PropertyDescriptor USE_MANAGED_IDENTITY = AzureEventHubUtils.USE_MANAGED_IDENTITY;

    static final PropertyDescriptor CONSUMER_GROUP = new PropertyDescriptor.Builder()
            .name("Event Hub Consumer Group")
            .displayName("Consumer Group")
            .description("The name of the consumer group to use when pulling events")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue("$Default")
            .required(true)
            .build();

    static final PropertyDescriptor ENQUEUE_TIME = new PropertyDescriptor.Builder()
            .name("Event Hub Message Enqueue Time")
            .displayName("Message Enqueue Time")
            .description("A timestamp (ISO-8601 Instant) formatted as YYYY-MM-DDThhmmss.sssZ (2016-01-01T01:01:01.000Z) from which messages "
                    + "should have been enqueued in the Event Hub to start reading from")
            .addValidator(StandardValidators.ISO8601_INSTANT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();
    static final PropertyDescriptor RECEIVER_FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Partition Recivier Fetch Size")
            .displayName("Partition Receiver Fetch Size")
            .description("The number of events that a receiver should fetch from an Event Hubs partition before returning. The default is " + DEFAULT_FETCH_SIZE)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();
    static final PropertyDescriptor RECEIVER_FETCH_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Partition Receiver Timeout (millseconds)")
            .displayName("Partition Receiver Timeout")
            .description("The amount of time in milliseconds a Partition Receiver should wait to receive the Fetch Size before returning. The default is " + DEFAULT_FETCH_TIMEOUT.toMillis())
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully received from the event hub will be transferred to this Relationship.")
            .build();

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            NAMESPACE,
            EVENT_HUB_NAME,
            SERVICE_BUS_ENDPOINT,
            TRANSPORT_TYPE,
            ACCESS_POLICY,
            POLICY_PRIMARY_KEY,
            USE_MANAGED_IDENTITY,
            CONSUMER_GROUP,
            ENQUEUE_TIME,
            RECEIVER_FETCH_SIZE,
            RECEIVER_FETCH_TIMEOUT,
            PROXY_CONFIGURATION_SERVICE
    );

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private final Map<String, EventPosition> partitionEventPositions = new ConcurrentHashMap<>();

    private final BlockingQueue<String> partitionIds = new LinkedBlockingQueue<>();

    private final AtomicReference<ExecutionNode> configuredExecutionNode = new AtomicReference<>(ExecutionNode.ALL);

    private volatile int receiverFetchSize;

    private volatile Duration receiverFetchTimeout;

    private EventHubClientBuilder configuredClientBuilder;

    private EventHubConsumerClient eventHubConsumerClient;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        return AzureEventHubUtils.customValidate(ACCESS_POLICY, POLICY_PRIMARY_KEY, context);
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeStateChange(final PrimaryNodeState primaryNodeState) {
        final ExecutionNode executionNode = configuredExecutionNode.get();
        if (executionNode == ExecutionNode.PRIMARY) {
            if (PrimaryNodeState.PRIMARY_NODE_REVOKED == primaryNodeState) {
                closeClient();
                getLogger().info("Consumer Client closed based on Execution Node [{}] and Primary Node State [{}]", executionNode, primaryNodeState);
            } else {
                createClient();
                getLogger().info("Consumer Client created based on Execution Node [{}] and Primary Node State [{}]", executionNode, primaryNodeState);
            }
        } else {
            getLogger().debug("Consumer Client not changed based on Execution Node [{}]", executionNode);
        }
    }

    @OnStopped
    public void closeClient() {
        partitionIds.clear();
        partitionEventPositions.clear();

        if (eventHubConsumerClient == null) {
            getLogger().debug("Consumer Client not configured");
        } else {
            eventHubConsumerClient.close();
            getLogger().info("Consumer Client for Event Hub [{}] closed", eventHubConsumerClient.getEventHubName());
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        configuredExecutionNode.set(context.getExecutionNode());
        configuredClientBuilder = createEventHubClientBuilder(context);
        createClient();

        if (context.getProperty(RECEIVER_FETCH_SIZE).isSet()) {
            receiverFetchSize = context.getProperty(RECEIVER_FETCH_SIZE).asInteger();
        } else {
            receiverFetchSize = DEFAULT_FETCH_SIZE;
        }
        if (context.getProperty(RECEIVER_FETCH_TIMEOUT).isSet()) {
            receiverFetchTimeout = Duration.ofMillis(context.getProperty(RECEIVER_FETCH_TIMEOUT).asLong());
        } else {
            receiverFetchTimeout = DEFAULT_FETCH_TIMEOUT;
        }

        final PropertyValue enqueuedTimeProperty = context.getProperty(ENQUEUE_TIME);
        final Instant initialEnqueuedTime;
        if (enqueuedTimeProperty.isSet()) {
            initialEnqueuedTime = Instant.parse(enqueuedTimeProperty.getValue());
        } else {
            initialEnqueuedTime = Instant.now();
        }
        final EventPosition initialEventPosition = EventPosition.fromEnqueuedTime(initialEnqueuedTime);
        for (final String partitionId : partitionIds) {
            partitionEventPositions.put(partitionId, initialEventPosition);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String partitionId = partitionIds.poll();
        if (partitionId == null) {
            getLogger().debug("No partitions available");
            return;
        }

        Long lastSequenceNumber = null;
        final StopWatch stopWatch = new StopWatch(true);
        try {
            final Iterable<PartitionEvent> events = receiveEvents(partitionId);

            for (final PartitionEvent partitionEvent : events) {
                final Map<String, String> attributes = getAttributes(partitionEvent);

                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, attributes);

                final EventData eventData = partitionEvent.getData();
                final byte[] body = eventData.getBody();
                flowFile = session.write(flowFile, outputStream -> outputStream.write(body));

                session.transfer(flowFile, REL_SUCCESS);

                final String transitUri = getTransitUri(partitionId);
                session.getProvenanceReporter().receive(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));

                lastSequenceNumber = eventData.getSequenceNumber();
            }

            if (lastSequenceNumber == null) {
                getLogger().debug("Partition [{}] Event Position not updated: Last Sequence Number not found", partitionId);
            } else {
                final EventPosition eventPosition = EventPosition.fromSequenceNumber(lastSequenceNumber);
                partitionEventPositions.put(partitionId, eventPosition);
                getLogger().debug("Partition [{}] Event Position updated: Sequence Number [{}]", partitionId, lastSequenceNumber);
            }
        } finally {
            partitionIds.offer(partitionId);
        }
    }

    /**
     * Get Partition Identifiers from Event Hub Consumer Client for polling
     *
     * @return Queue of Partition Identifiers
     */
    protected BlockingQueue<String> getPartitionIds() {
        final BlockingQueue<String> configuredPartitionIds = new LinkedBlockingQueue<>();
        for (final String partitionId : eventHubConsumerClient.getPartitionIds()) {
            configuredPartitionIds.add(partitionId);
        }
        return configuredPartitionIds;
    }

    /**
     * Receive Events from specified partition is synchronized to avoid concurrent requests for the same partition
     *
     * @param partitionId Partition Identifier
     * @return Iterable of Partition Events or empty when none received
     */
    protected synchronized Iterable<PartitionEvent> receiveEvents(final String partitionId) {
        final EventPosition eventPosition = partitionEventPositions.getOrDefault(partitionId, EventPosition.fromEnqueuedTime(Instant.now()));
        getLogger().debug("Receiving Events for Partition [{}] from Position [{}]", partitionId, eventPosition);
        return eventHubConsumerClient.receiveFromPartition(partitionId, receiverFetchSize, eventPosition, receiverFetchTimeout);
    }

    private void createClient() {
        if (isCreateClientEnabled()) {
            closeClient();
            eventHubConsumerClient = configuredClientBuilder.buildConsumerClient();
            partitionIds.addAll(getPartitionIds());
            getLogger().info("Consumer Client created for Event Hub [{}] Partitions {}", eventHubConsumerClient.getEventHubName(), partitionIds);
        }
    }

    private boolean isCreateClientEnabled() {
        final boolean enabled;

        final ExecutionNode executionNode = configuredExecutionNode.get();
        if (ExecutionNode.PRIMARY == executionNode) {
            final NodeTypeProvider nodeTypeProvider = getNodeTypeProvider();
            enabled = nodeTypeProvider.isPrimary();
        } else {
            enabled = true;
        }

        return enabled;
    }

    private EventHubClientBuilder createEventHubClientBuilder(final ProcessContext context) {
        final String namespace = context.getProperty(NAMESPACE).getValue();
        final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();
        final String serviceBusEndpoint = context.getProperty(SERVICE_BUS_ENDPOINT).getValue();
        final boolean useManagedIdentity = context.getProperty(USE_MANAGED_IDENTITY).asBoolean();
        final String fullyQualifiedNamespace = String.format("%s%s", namespace, serviceBusEndpoint);
        final AmqpTransportType transportType = context.getProperty(TRANSPORT_TYPE).asAllowableValue(AzureEventHubTransportType.class).asAmqpTransportType();

        final EventHubClientBuilder eventHubClientBuilder = new EventHubClientBuilder();
        eventHubClientBuilder.transportType(transportType);

        final String consumerGroup = context.getProperty(CONSUMER_GROUP).getValue();
        eventHubClientBuilder.consumerGroup(consumerGroup);

        if (useManagedIdentity) {
            final ManagedIdentityCredentialBuilder managedIdentityCredentialBuilder = new ManagedIdentityCredentialBuilder();
            final ManagedIdentityCredential managedIdentityCredential = managedIdentityCredentialBuilder.build();
            eventHubClientBuilder.credential(fullyQualifiedNamespace, eventHubName, managedIdentityCredential);
        } else {
            final String policyName = context.getProperty(ACCESS_POLICY).getValue();
            final String policyKey = context.getProperty(POLICY_PRIMARY_KEY).getValue();
            final AzureNamedKeyCredential azureNamedKeyCredential = new AzureNamedKeyCredential(policyName, policyKey);
            eventHubClientBuilder.credential(fullyQualifiedNamespace, eventHubName, azureNamedKeyCredential);
        }

        // Set Azure Event Hub Client Identifier using Processor Identifier instead of default random UUID
        final AmqpClientOptions clientOptions = new AmqpClientOptions();
        final String clientIdentifier = getClientIdentifier();
        clientOptions.setIdentifier(clientIdentifier);
        eventHubClientBuilder.clientOptions(clientOptions);

        AzureEventHubUtils.getProxyOptions(context).ifPresent(eventHubClientBuilder::proxyOptions);

        return eventHubClientBuilder;
    }

    private String getTransitUri(final String partitionId) {
        return String.format(TRANSIT_URI_FORMAT_STRING,
                eventHubConsumerClient.getFullyQualifiedNamespace(),
                eventHubConsumerClient.getEventHubName(),
                eventHubConsumerClient.getConsumerGroup(),
                partitionId
        );
    }

    private String getClientIdentifier() {
        final String clientIdentifier;

        final String componentIdentifier = getIdentifier();

        final NodeTypeProvider nodeTypeProvider = getNodeTypeProvider();
        if (nodeTypeProvider.isClustered()) {
            final Optional<String> currentNode = nodeTypeProvider.getCurrentNode();
            if (currentNode.isPresent()) {
                final String currentNodeId = currentNode.get();
                clientIdentifier = String.format(NODE_CLIENT_IDENTIFIER_FORMAT, currentNodeId, componentIdentifier);
            } else {
                clientIdentifier = componentIdentifier;
            }
        } else {
            clientIdentifier = componentIdentifier;
        }

        return clientIdentifier;
    }

    private Map<String, String> getAttributes(final PartitionEvent partitionEvent) {
        final Map<String, String> attributes = new LinkedHashMap<>();

        final EventData eventData = partitionEvent.getData();

        attributes.put("eventhub.enqueued.timestamp", String.valueOf(eventData.getEnqueuedTime()));
        attributes.put("eventhub.offset", String.valueOf(eventData.getOffset()));
        attributes.put("eventhub.sequence", String.valueOf(eventData.getSequenceNumber()));

        final PartitionContext partitionContext = partitionEvent.getPartitionContext();
        attributes.put("eventhub.name", partitionContext.getEventHubName());
        attributes.put("eventhub.partition", partitionContext.getPartitionId());

        final Map<String, String> applicationProperties = AzureEventHubUtils.getApplicationProperties(eventData.getProperties());
        attributes.putAll(applicationProperties);

        return attributes;
    }
}
