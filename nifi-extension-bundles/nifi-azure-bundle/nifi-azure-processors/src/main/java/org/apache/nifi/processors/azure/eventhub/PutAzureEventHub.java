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

import com.azure.core.amqp.AmqpTransportType;
import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.SendOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.eventhub.utils.AzureEventHubUtils;
import org.apache.nifi.processors.azure.storage.utils.FlowFileResultCarrier;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubComponent;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubTransportType;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SupportsBatching
@Tags({"microsoft", "azure", "cloud", "eventhub", "events", "streams", "streaming"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Send FlowFile contents to Azure Event Hubs")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The Processor buffers FlowFile contents in memory before sending")
public class PutAzureEventHub extends AbstractProcessor implements AzureEventHubComponent {
    private static final String TRANSIT_URI_FORMAT_STRING = "amqps://%s%s/%s";

    static final PropertyDescriptor EVENT_HUB_NAME = new PropertyDescriptor.Builder()
            .name("Event Hub Name")
            .description("Name of Azure Event Hubs destination")
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
            .description("The name of the shared access policy. This policy must have Send claims.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();
    static final PropertyDescriptor POLICY_PRIMARY_KEY = AzureEventHubUtils.POLICY_PRIMARY_KEY;
    static final PropertyDescriptor USE_MANAGED_IDENTITY = AzureEventHubUtils.USE_MANAGED_IDENTITY;

    static final PropertyDescriptor PARTITIONING_KEY_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("partitioning-key-attribute-name")
            .displayName("Partitioning Key Attribute Name")
            .description("If specified, the value from argument named by this field will be used as a partitioning key to be used by event hub.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .build();
    static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("max-batch-size")
            .displayName("Maximum Batch Size")
            .description("Maximum number of FlowFiles processed for each Processor invocation")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .defaultValue("100")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully sent to the event hubs will be transferred to this Relationship.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that could not be sent to the event hub will be transferred to this Relationship.")
            .build();

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            NAMESPACE,
            EVENT_HUB_NAME,
            SERVICE_BUS_ENDPOINT,
            TRANSPORT_TYPE,
            ACCESS_POLICY,
            POLICY_PRIMARY_KEY,
            USE_MANAGED_IDENTITY,
            PARTITIONING_KEY_ATTRIBUTE_NAME,
            MAX_BATCH_SIZE,
            PROXY_CONFIGURATION_SERVICE
    );

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private EventHubProducerClient eventHubProducerClient;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public final void createClient(final ProcessContext context) {
        eventHubProducerClient = createEventHubProducerClient(context);
    }

    @OnStopped
    public void closeClient() {
        if (eventHubProducerClient == null) {
            getLogger().info("Azure Event Hub Producer Client not configured");
        } else {
            eventHubProducerClient.close();
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        return AzureEventHubUtils.customValidate(ACCESS_POLICY, POLICY_PRIMARY_KEY, context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final StopWatch stopWatch = new StopWatch(true);

        final String partitioningKeyAttributeName = context.getProperty(PARTITIONING_KEY_ATTRIBUTE_NAME).getValue();

        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
        final List<FlowFile> flowFileBatch = session.get(maxBatchSize);

        final List<FlowFileResultCarrier<Relationship>> flowFileResults = new ArrayList<>();
        for (final FlowFile flowFile : flowFileBatch) {
            final FlowFileResultCarrier<Relationship> flowFileResult = handleFlowFile(flowFile, partitioningKeyAttributeName, session);
            flowFileResults.add(flowFileResult);
        }

        processFlowFileResults(context, session, stopWatch, flowFileResults);
    }

    protected EventHubProducerClient createEventHubProducerClient(final ProcessContext context) throws ProcessException {
        final boolean useManagedIdentity = context.getProperty(USE_MANAGED_IDENTITY).asBoolean();
        final String namespace = context.getProperty(NAMESPACE).getValue();
        final String serviceBusEndpoint = context.getProperty(SERVICE_BUS_ENDPOINT).getValue();
        final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();
        final AmqpTransportType transportType = context.getProperty(TRANSPORT_TYPE).asAllowableValue(AzureEventHubTransportType.class).asAmqpTransportType();

        try {
            final EventHubClientBuilder eventHubClientBuilder = new EventHubClientBuilder();
            eventHubClientBuilder.transportType(transportType);

            final String fullyQualifiedNamespace = String.format("%s%s", namespace, serviceBusEndpoint);
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
            AzureEventHubUtils.getProxyOptions(context).ifPresent(eventHubClientBuilder::proxyOptions);
            return eventHubClientBuilder.buildProducerClient();
        } catch (final Exception e) {
            throw new ProcessException("EventHubClient creation failed", e);
        }
    }

    private void processFlowFileResults(
            final ProcessContext context,
            final ProcessSession session,
            final StopWatch stopWatch,
            final List<FlowFileResultCarrier<Relationship>> flowFileResults
    ) {
        try {
            for (final FlowFileResultCarrier<Relationship> flowFileResult : flowFileResults) {
                final FlowFile flowFile = flowFileResult.flowFile();

                if (flowFileResult.result() == REL_SUCCESS) {
                    final String namespace = context.getProperty(NAMESPACE).getValue();
                    final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();
                    final String serviceBusEndpoint = context.getProperty(SERVICE_BUS_ENDPOINT).getValue();
                    final String transitUri = String.format(TRANSIT_URI_FORMAT_STRING, namespace, serviceBusEndpoint, eventHubName);
                    session.getProvenanceReporter().send(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                    session.transfer(flowFile, REL_SUCCESS);
                } else {
                    final Throwable processException = flowFileResult.exception();
                    getLogger().error("Send failed {}", flowFile, processException);
                    session.transfer(session.penalize(flowFile), REL_FAILURE);
                }
            }
        } catch (final Exception e) {
            session.rollback();
            getLogger().error("FlowFile Batch Size [{}] processing failed", flowFileResults.size());
        }
    }

    private FlowFileResultCarrier<Relationship> handleFlowFile(FlowFile flowFile, final String partitioningKeyAttributeName, final ProcessSession session) {
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));

        final String partitioningKey;
        if (StringUtils.isNotBlank(partitioningKeyAttributeName)) {
            partitioningKey = flowFile.getAttribute(partitioningKeyAttributeName);
        } else {
            partitioningKey = null;
        }

        final Map<String, String> attributes = flowFile.getAttributes();
        final Map<String, ?> userProperties = attributes == null ? Collections.emptyMap() : attributes;

        try {
            sendMessage(buffer, partitioningKey, userProperties);
            return new FlowFileResultCarrier<>(flowFile, REL_SUCCESS);
        } catch (final Exception processException) {
            return new FlowFileResultCarrier<>(flowFile, REL_FAILURE, processException);
        }
    }

    private void sendMessage(final byte[] buffer, String partitioningKey, Map<String, ?> userProperties) {
        final EventData eventData = new EventData(buffer);
        eventData.getProperties().putAll(userProperties);
        final SendOptions sendOptions = new SendOptions();
        if (StringUtils.isNotBlank(partitioningKey)) {
            sendOptions.setPartitionKey(partitioningKey);
        }

        eventHubProducerClient.send(Collections.singleton(eventData), sendOptions);
    }
}
