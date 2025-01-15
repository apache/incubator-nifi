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
package org.apache.nifi.processors.standard;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportsBatching
@Tags({"map", "cache", "notify", "distributed", "signal", "release"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Caches a release signal identifier in the distributed cache, optionally along with "
        + "the FlowFile's attributes.  Any flow files held at a corresponding Wait processor will be "
        + "released once this signal in the cache is discovered.")
@WritesAttribute(attribute = "notified", description = "All FlowFiles will have an attribute 'notified'. The value of this " +
        "attribute is true, is the FlowFile is notified, otherwise false.")
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService", "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "org.apache.nifi.processors.standard.Wait"})
public class Notify extends AbstractProcessor {

    public static final String NOTIFIED_ATTRIBUTE_NAME = "notified";

    // Identifies the distributed map cache client
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("distributed-cache-service")
            .displayName("Distributed Cache Service")
            .description("The Controller Service that is used to cache release signals in order to release files queued at a corresponding Wait processor")
            .required(true)
            .identifiesControllerService(AtomicDistributedMapCacheClient.class)
            .build();

    // Selects the FlowFile attribute or expression, whose value is used as cache key
    public static final PropertyDescriptor RELEASE_SIGNAL_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("release-signal-id")
            .displayName("Release Signal Identifier")
            .description("A value, or the results of an Attribute Expression Language statement, which will " +
                "be evaluated against a FlowFile in order to determine the release signal cache key")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SIGNAL_COUNTER_NAME = new PropertyDescriptor.Builder()
            .name("signal-counter-name")
            .displayName("Signal Counter Name")
            .description("A value, or the results of an Attribute Expression Language statement, which will " +
                "be evaluated against a FlowFile in order to determine the signal counter name. " +
                "Signal counter name is useful when a corresponding Wait processor needs to know the number of occurrences " +
                "of different types of events, such as success or failure, or destination data source names, etc.")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue(WaitNotifyProtocol.DEFAULT_COUNT_NAME)
            .build();

    public static final PropertyDescriptor SIGNAL_COUNTER_DELTA = new PropertyDescriptor.Builder()
            .name("signal-counter-delta")
            .displayName("Signal Counter Delta")
            .description("A value, or the results of an Attribute Expression Language statement, which will " +
                "be evaluated against a FlowFile in order to determine the signal counter delta. " +
                "Specify how much the counter should increase. " +
                "For example, if multiple signal events are processed at upstream flow in batch oriented way, " +
                "the number of events processed can be notified with this property at once. " +
                "Zero (0) has a special meaning, it clears target count back to 0, which is especially useful when used with Wait " +
                Wait.RELEASABLE_FLOWFILE_COUNT.getDisplayName() + " = Zero (0) mode, to provide 'open-close-gate' type of flow control. " +
                "One (1) can open a corresponding Wait processor, and Zero (0) can negate it as if closing a gate.")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor SIGNAL_BUFFER_COUNT = new PropertyDescriptor.Builder()
            .name("signal-buffer-count")
            .displayName("Signal Buffer Count")
            .description("Specify the maximum number of incoming flow files that can be buffered until signals are notified to cache service. " +
                "The more buffer can provide the better performance, as it reduces the number of interactions with cache service " +
                "by grouping signals by signal identifier when multiple incoming flow files share the same signal identifier.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    // Specifies an optional regex used to identify which attributes to cache
    public static final PropertyDescriptor ATTRIBUTE_CACHE_REGEX = new PropertyDescriptor.Builder()
            .name("attribute-cache-regex")
            .displayName("Attribute Cache Regex")
            .description("Any attributes whose names match this regex will be stored in the distributed cache to be "
                    + "copied to any FlowFiles released from a corresponding Wait processor.  Note that the "
                    + "uuid attribute will not be cached regardless of this value.  If blank, no attributes "
                    + "will be cached.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RELEASE_SIGNAL_IDENTIFIER,
            SIGNAL_COUNTER_NAME,
            SIGNAL_COUNTER_DELTA,
            SIGNAL_BUFFER_COUNT,
            DISTRIBUTED_CACHE_SERVICE,
            ATTRIBUTE_CACHE_REGEX
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles where the release signal has been successfully entered in the cache will be routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("When the cache cannot be reached, or if the Release Signal Identifier evaluates to null or empty, FlowFiles will be routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private class SignalBuffer {

        final Map<String, Integer> deltas = new HashMap<>();
        final Map<String, String> attributesToCache = new HashMap<>();
        final List<FlowFile> flowFiles = new ArrayList<>();

        int incrementDelta(final String counterName, final int delta) {
            int current = deltas.getOrDefault(counterName, 0);
            // Zero (0) clears count.
            int updated = delta == 0 ? 0 : current + delta;
            deltas.put(counterName, updated);
            return updated;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final ComponentLog logger = getLogger();
        final PropertyValue signalIdProperty = context.getProperty(RELEASE_SIGNAL_IDENTIFIER);
        final PropertyValue counterNameProperty = context.getProperty(SIGNAL_COUNTER_NAME);
        final PropertyValue deltaProperty = context.getProperty(SIGNAL_COUNTER_DELTA);
        final String attributeCacheRegex = context.getProperty(ATTRIBUTE_CACHE_REGEX).getValue();
        final Integer bufferCount = context.getProperty(SIGNAL_BUFFER_COUNT).asInteger();

        // the cache client used to interact with the distributed cache.
        final AtomicDistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(AtomicDistributedMapCacheClient.class);
        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        final Map<String, SignalBuffer> signalBuffers = new HashMap<>();

        for (int i = 0; i < bufferCount; i++) {

            final FlowFile flowFile = session.get();
            if (flowFile == null) {
                break;
            }

            // Signal id is computed from attribute 'RELEASE_SIGNAL_IDENTIFIER' with expression language support
            final String signalId = signalIdProperty.evaluateAttributeExpressions(flowFile).getValue();

            // if the computed value is null, or empty, we transfer the flow file to failure relationship
            if (StringUtils.isBlank(signalId)) {
                logger.error("FlowFile {} has no attribute for given Release Signal Identifier", flowFile);
                // set 'notified' attribute
                session.transfer(session.putAttribute(flowFile, NOTIFIED_ATTRIBUTE_NAME, String.valueOf(false)), REL_FAILURE);
                continue;
            }

            String counterName = counterNameProperty.evaluateAttributeExpressions(flowFile).getValue();
            if (StringUtils.isEmpty(counterName)) {
                counterName = WaitNotifyProtocol.DEFAULT_COUNT_NAME;
            }

            int delta = 1;
            if (deltaProperty.isSet()) {
                final String deltaStr = deltaProperty.evaluateAttributeExpressions(flowFile).getValue();
                try {
                    delta = Integer.parseInt(deltaStr);
                } catch (final NumberFormatException e) {
                    logger.error("Failed to calculate delta for FlowFile {} due to {}", flowFile, e, e);
                    session.transfer(session.putAttribute(flowFile, NOTIFIED_ATTRIBUTE_NAME, String.valueOf(false)), REL_FAILURE);
                    continue;
                }
            }

            if (!signalBuffers.containsKey(signalId)) {
                signalBuffers.put(signalId, new SignalBuffer());
            }
            final SignalBuffer signalBuffer = signalBuffers.get(signalId);

            if (StringUtils.isNotEmpty(attributeCacheRegex)) {
                flowFile.getAttributes().entrySet()
                        .stream().filter(e -> (!e.getKey().equals("uuid") && e.getKey().matches(attributeCacheRegex)))
                        .forEach(e -> signalBuffer.attributesToCache.put(e.getKey(), e.getValue()));
            }

            signalBuffer.incrementDelta(counterName, delta);
            signalBuffer.flowFiles.add(flowFile);

            if (logger.isDebugEnabled()) {
                logger.debug("Cached release signal identifier {} counterName {} from FlowFile {}", signalId, counterName, flowFile);
            }
        }

        signalBuffers.forEach((signalId, signalBuffer) -> {
            // In case of Exception, just throw the exception so that processor can
            // retry after yielding for a while.
            try {
                protocol.notify(signalId, signalBuffer.deltas, signalBuffer.attributesToCache);
                signalBuffer.flowFiles.forEach(flowFile ->
                        session.transfer(session.putAttribute(flowFile, NOTIFIED_ATTRIBUTE_NAME, String.valueOf(true)), REL_SUCCESS));
            } catch (IOException e) {
                throw new RuntimeException(String.format("Unable to communicate with cache when processing %s due to %s", signalId, e), e);
            }
        });
    }
}
