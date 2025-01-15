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
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.WaitNotifyProtocol.Signal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_TERMINATE;
import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;

@SupportsBatching
@Tags({"map", "cache", "wait", "hold", "distributed", "signal", "release"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Routes incoming FlowFiles to the 'wait' relationship until a matching release signal "
        + "is stored in the distributed cache from a corresponding Notify processor. "
        + "When a matching release signal is identified, a waiting FlowFile is routed to the 'success' relationship. "
        + "The release signal entry is then removed from the cache. "
        + "The attributes of the FlowFile that produced the release signal are copied to the waiting FlowFile if the Attribute Cache Regex "
        + "property of the corresponding Notify processor is set properly. "
        + "If there are multiple release signals in the cache identified by the Release Signal Identifier, "
        + "and the Notify processor is configured to copy the FlowFile attributes to the cache, "
        + "then the FlowFile passing the Wait processor receives the union of the attributes of the FlowFiles "
        + "that produced the release signals in the cache (identified by Release Signal Identifier). "
        + "Waiting FlowFiles will be routed to 'expired' if they exceed the Expiration Duration. "

        + "If you need to wait for more than one signal, specify the desired number of signals via the 'Target Signal Count' property. "
        + "This is particularly useful with processors that split a source FlowFile into multiple fragments, such as SplitText. "
        + "In order to wait for all fragments to be processed, connect the 'original' relationship to a Wait processor, and the 'splits' relationship to "
        + "a corresponding Notify processor. Configure the Notify and Wait processors to use the '${fragment.identifier}' as the value "
        + "of 'Release Signal Identifier', and specify '${fragment.count}' as the value of 'Target Signal Count' in the Wait processor."

        + "It is recommended to use a prioritizer (for instance First In First Out) when using the 'wait' relationship as a loop."
)
@WritesAttributes({
        @WritesAttribute(attribute = "wait.start.timestamp", description = "All FlowFiles will have an attribute 'wait.start.timestamp', which sets the "
        + "initial epoch timestamp when the file first entered this processor.  This is used to determine the expiration time of the FlowFile.  "
        + "This attribute is not written when the FlowFile is transferred to failure, expired or success"),
        @WritesAttribute(attribute = "wait.counter.<counterName>", description = "The name of each counter for which at least one signal "
        + "has been present in the cache since the last time the cache was empty "
        + "gets copied to the current FlowFile as an attribute.")
})
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService", "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "org.apache.nifi.processors.standard.Notify"})
public class Wait extends AbstractProcessor {

    public static final String WAIT_START_TIMESTAMP = "wait.start.timestamp";

    // Identifies the distributed map cache client
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("distributed-cache-service")
            .displayName("Distributed Cache Service")
            .description("The Controller Service that is used to check for release signals from a corresponding Notify processor")
            .required(true)
            .identifiesControllerService(AtomicDistributedMapCacheClient.class)
            .build();

    // Selects the FlowFile attribute or expression, whose value is used as cache key
    public static final PropertyDescriptor RELEASE_SIGNAL_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("release-signal-id")
            .displayName("Release Signal Identifier")
            .description("A value that specifies the key to a specific release signal cache. "
                + "To decide whether the FlowFile that is being processed by the Wait processor should be sent to the 'success' "
                + "or the 'wait' relationship, the processor checks the signals in the cache specified by this key.")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor TARGET_SIGNAL_COUNT = new PropertyDescriptor.Builder()
            .name("target-signal-count")
            .displayName("Target Signal Count")
            .description("The number of signals that need to be in the cache (specified by the Release Signal Identifier) "
                + "in order for the FlowFile processed by the Wait processor to be sent to the ‘success’ relationship. "
                + "If the number of signals in the cache has reached this number, the FlowFile is routed to the "
                + "'success' relationship and the number of signals in the cache is decreased by this value. "
                + "If Signal Counter Name is specified, this processor checks a particular counter, "
                + "otherwise checks against the total number of signals in the cache.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor SIGNAL_COUNTER_NAME = new PropertyDescriptor.Builder()
            .name("signal-counter-name")
            .displayName("Signal Counter Name")
            .description("Within the cache (specified by the Release Signal Identifier) the signals may belong to different counters. "
                + "If this property is specified, the processor checks the number of signals in the cache that belong to this particular counter. "
                + "If not specified, the processor checks the total number of signals in the cache.")
            .required(false)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor WAIT_BUFFER_COUNT = new PropertyDescriptor.Builder()
            .name("wait-buffer-count")
            .displayName("Wait Buffer Count")
            .description("Specify the maximum number of incoming FlowFiles that can be buffered to check whether it can move forward. " +
                    "The more buffer can provide the better performance, as it reduces the number of interactions with cache service " +
                    "by grouping FlowFiles by signal identifier. " +
                    "Only a signal identifier can be processed at a processor execution.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor RELEASABLE_FLOWFILE_COUNT = new PropertyDescriptor.Builder()
            .name("releasable-flowfile-count")
            .displayName("Releasable FlowFile Count")
            .description("A value, or the results of an Attribute Expression Language statement, which will " +
                    "be evaluated against a FlowFile in order to determine the releasable FlowFile count. " +
                    "This specifies how many FlowFiles can be released when a target count reaches target signal count. " +
                    "Zero (0) has a special meaning, any number of FlowFiles can be released as long as signal count matches target.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("1")
            .build();

    // Selects the FlowFile attribute or expression, whose value is used as cache key
    public static final PropertyDescriptor EXPIRATION_DURATION = new PropertyDescriptor.Builder()
            .name("expiration-duration")
            .displayName("Expiration Duration")
            .description("Indicates the duration after which waiting FlowFiles will be routed to the 'expired' relationship")
            .required(true)
            .defaultValue("10 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final AllowableValue ATTRIBUTE_COPY_REPLACE = new AllowableValue("replace", "Replace if present",
            "When cached attributes are copied onto released FlowFiles, they replace any matching attributes.");

    public static final AllowableValue ATTRIBUTE_COPY_KEEP_ORIGINAL = new AllowableValue("keeporiginal", "Keep original",
            "Attributes on released FlowFiles are not overwritten by copied cached attributes.");

    public static final PropertyDescriptor ATTRIBUTE_COPY_MODE = new PropertyDescriptor.Builder()
            .name("attribute-copy-mode")
            .displayName("Attribute Copy Mode")
            .description("Specifies how to handle attributes copied from FlowFiles entering the Notify processor")
            .defaultValue(ATTRIBUTE_COPY_KEEP_ORIGINAL.getValue())
            .required(true)
            .allowableValues(ATTRIBUTE_COPY_REPLACE, ATTRIBUTE_COPY_KEEP_ORIGINAL)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final AllowableValue WAIT_MODE_TRANSFER_TO_WAIT = new AllowableValue("wait", "Transfer to wait relationship",
            "Transfer a FlowFile to the 'wait' relationship when whose release signal has not been notified yet." +
                    " This mode allows other incoming FlowFiles to be enqueued by moving FlowFiles into the wait relationship." +
                    " It is recommended to set a prioritizer (for instance First In First Out) on the 'wait' relationship.");

    public static final AllowableValue WAIT_MODE_KEEP_IN_UPSTREAM = new AllowableValue("keep", "Keep in the upstream connection",
            "Transfer a FlowFile to the upstream connection where it comes from when whose release signal has not been notified yet." +
                    " This mode helps keeping upstream connection being full so that the upstream source processor" +
                    " will not be scheduled while back-pressure is active and limit incoming FlowFiles. ");

    public static final PropertyDescriptor WAIT_MODE = new PropertyDescriptor.Builder()
            .name("wait-mode")
            .displayName("Wait Mode")
            .description("Specifies how to handle a FlowFile waiting for a notify signal")
            .defaultValue(WAIT_MODE_TRANSFER_TO_WAIT.getValue())
            .required(true)
            .allowableValues(WAIT_MODE_TRANSFER_TO_WAIT, WAIT_MODE_KEEP_IN_UPSTREAM)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor WAIT_PENALTY_DURATION = new PropertyDescriptor.Builder()
        .name("wait-penalty-duration")
        .displayName("Wait Penalty Duration")
        .description("If configured, after a signal identifier got processed but did not meet the release criteria," +
            " the signal identifier is penalized and FlowFiles having the signal identifier" +
            " will not be processed again for the specified period of time," +
            " so that the signal identifier will not block others to be processed." +
            " This can be useful for use cases where a Wait processor is expected to process multiple signal identifiers," +
            " and each signal identifier has multiple FlowFiles," +
            " and also the order of releasing FlowFiles is important within a signal identifier." +
            " The FlowFile order can be configured with Prioritizers." +
            " IMPORTANT: There is a limitation of number of queued signals can be processed," +
            " and Wait processor may not be able to check all queued signal ids. See additional details for the best practice.")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RELEASE_SIGNAL_IDENTIFIER,
            TARGET_SIGNAL_COUNT,
            SIGNAL_COUNTER_NAME,
            WAIT_BUFFER_COUNT,
            RELEASABLE_FLOWFILE_COUNT,
            EXPIRATION_DURATION,
            DISTRIBUTED_CACHE_SERVICE,
            ATTRIBUTE_COPY_MODE,
            WAIT_MODE,
            WAIT_PENALTY_DURATION
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile with a matching release signal in the cache will be routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("When the cache cannot be reached, or if the Release Signal Identifier evaluates to null or empty, FlowFiles will be routed to this relationship")
            .build();

    public static final Relationship REL_WAIT = new Relationship.Builder()
            .name("wait")
            .description("A FlowFile with no matching release signal in the cache will be routed to this relationship")
            .build();

    public static final Relationship REL_EXPIRED = new Relationship.Builder()
            .name("expired")
            .description("A FlowFile that has exceeded the configured Expiration Duration will be routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_WAIT,
            REL_EXPIRED,
            REL_FAILURE
    );

    private final Map<String, Long> signalIdPenalties = new HashMap<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final ComponentLog logger = getLogger();

        // Signal id is computed from attribute 'RELEASE_SIGNAL_IDENTIFIER' with expression language support
        final PropertyValue signalIdProperty = context.getProperty(RELEASE_SIGNAL_IDENTIFIER);
        final Integer bufferCount = context.getProperty(WAIT_BUFFER_COUNT).asInteger();

        final Map<Relationship, List<FlowFile>> processedFlowFiles = new HashMap<>();
        final Function<Relationship, List<FlowFile>> getFlowFilesFor = r -> processedFlowFiles.computeIfAbsent(r, k -> new ArrayList<>());

        final AtomicReference<String> targetSignalId = new AtomicReference<>();
        final AtomicInteger bufferedCount = new AtomicInteger(0);
        final List<FlowFile> failedFilteringFlowFiles = new ArrayList<>();
        final Supplier<FlowFileFilter.FlowFileFilterResult> acceptResultSupplier =
                () -> bufferedCount.incrementAndGet() == bufferCount ? ACCEPT_AND_TERMINATE : ACCEPT_AND_CONTINUE;

        // Clear expired penalties.
        if (!signalIdPenalties.isEmpty()) {
            final Iterator<Entry<String, Long>> penaltyIterator = signalIdPenalties.entrySet().iterator();
            final long now = System.currentTimeMillis();
            while (penaltyIterator.hasNext()) {
                final Entry<String, Long> penalty = penaltyIterator.next();
                if (penalty.getValue() < now) {
                    penaltyIterator.remove();
                }
            }
        }

        final List<FlowFile> flowFiles = session.get(f -> {

            final String fSignalId = signalIdProperty.evaluateAttributeExpressions(f).getValue();

            // if the computed value is null, or empty, we transfer the FlowFile to failure relationship
            if (StringUtils.isBlank(fSignalId)) {
                // We can't penalize f before getting it from session, so keep it in a temporal list.
                logger.error("FlowFile {} has no attribute for given Release Signal Identifier", f);
                failedFilteringFlowFiles.add(f);
                return ACCEPT_AND_CONTINUE;
            }

            if (signalIdPenalties.containsKey(fSignalId)) {
                // This id is penalized.
                return REJECT_AND_CONTINUE;
            }

            final String targetSignalIdStr = targetSignalId.get();
            if (targetSignalIdStr == null) {
                // This is the first one.
                targetSignalId.set(fSignalId);
                return acceptResultSupplier.get();
            }

            if (targetSignalIdStr.equals(fSignalId)) {
                return acceptResultSupplier.get();
            }

            return REJECT_AND_CONTINUE;

        });

        final String attributeCopyMode = context.getProperty(ATTRIBUTE_COPY_MODE).getValue();
        final boolean replaceOriginalAttributes = ATTRIBUTE_COPY_REPLACE.getValue().equals(attributeCopyMode);
        final AtomicReference<Signal> signalRef = new AtomicReference<>();
        // This map contains original counts before those are consumed to release incoming FlowFiles.
        final HashMap<String, Long> originalSignalCounts = new HashMap<>();

        final Consumer<FlowFile> transferToFailure = flowFile -> {
            flowFile = session.penalize(flowFile);
            // This flowFile is now failed, our tracking is done, clear the timer
            flowFile = clearWaitState(session, flowFile);
            getFlowFilesFor.apply(REL_FAILURE).add(flowFile);
        };

        final Consumer<Entry<Relationship, List<FlowFile>>> transferFlowFiles = routedFlowFiles -> {
            Relationship relationship = routedFlowFiles.getKey();

            if (REL_WAIT.equals(relationship)) {
                final String waitMode = context.getProperty(WAIT_MODE).getValue();

                if (WAIT_MODE_KEEP_IN_UPSTREAM.getValue().equals(waitMode)) {
                    // Transfer to self.
                    relationship = Relationship.SELF;
                }
            }
            final Relationship finalRelationship = relationship;
            final List<FlowFile> flowFilesWithSignalAttributes = routedFlowFiles.getValue().stream()
                    .map(f -> {
                        if (REL_SUCCESS.equals(finalRelationship) || REL_EXPIRED.equals(finalRelationship)) {
                            // These flowFiles will be exiting the wait, clear the timer
                            f = clearWaitState(session, f);
                        }
                        return copySignalAttributes(session, f, signalRef.get(),
                            originalSignalCounts,
                            replaceOriginalAttributes);
                    })
                    .collect(Collectors.toList());

            session.transfer(flowFilesWithSignalAttributes, relationship);
        };

        failedFilteringFlowFiles.forEach(f -> {
            flowFiles.remove(f);
            transferToFailure.accept(f);
        });

        if (flowFiles.isEmpty()) {
            // If there was nothing but failed FlowFiles while filtering, transfer those and end immediately.
            processedFlowFiles.entrySet().forEach(transferFlowFiles);
            return;
        }

        // the cache client used to interact with the distributed cache
        final AtomicDistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(AtomicDistributedMapCacheClient.class);
        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        final String signalId = targetSignalId.get();
        final Signal signal;

        // get notifying signal
        try {
            signal = protocol.getSignal(signalId);
            if (signal != null) {
                originalSignalCounts.putAll(signal.getCounts());
            }
            signalRef.set(signal);
        } catch (final IOException e) {
            throw new ProcessException(String.format("Failed to get signal for %s due to %s", signalId, e), e);
        }

        String targetCounterName = null;
        long targetCount = 1;
        int releasableFlowFileCount = 1;

        final List<FlowFile> candidates = new ArrayList<>();

        for (FlowFile flowFile : flowFiles) {
            // Set wait start timestamp if it's not set yet
            String waitStartTimestamp = flowFile.getAttribute(WAIT_START_TIMESTAMP);
            if (waitStartTimestamp == null) {
                waitStartTimestamp = String.valueOf(System.currentTimeMillis());
                flowFile = session.putAttribute(flowFile, WAIT_START_TIMESTAMP, waitStartTimestamp);
            }

            long lWaitStartTimestamp;
            try {
                lWaitStartTimestamp = Long.parseLong(waitStartTimestamp);
            } catch (NumberFormatException nfe) {
                logger.error("{} has an invalid value '{}' on FlowFile {}", WAIT_START_TIMESTAMP, waitStartTimestamp, flowFile);
                transferToFailure.accept(flowFile);
                continue;
            }

            // check for expiration
            long expirationDuration = context.getProperty(EXPIRATION_DURATION)
                    .asTimePeriod(TimeUnit.MILLISECONDS);
            long now = System.currentTimeMillis();
            if (now > (lWaitStartTimestamp + expirationDuration)) {
                logger.info("FlowFile {} expired after {}ms", flowFile, (now - lWaitStartTimestamp));
                getFlowFilesFor.apply(REL_EXPIRED).add(flowFile);
                continue;
            }

            // If there's no signal yet, then we don't have to evaluate target counts. Return immediately.
            if (signal == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("No release signal found for {} on FlowFile {} yet", signalId, flowFile);
                }
                getFlowFilesFor.apply(REL_WAIT).add(flowFile);
                continue;
            }

            // Fix target counter name and count from current FlowFile, if those are not set yet.
            if (candidates.isEmpty()) {
                targetCounterName = context.getProperty(SIGNAL_COUNTER_NAME).evaluateAttributeExpressions(flowFile).getValue();
                try {
                    targetCount = Long.parseLong(context.getProperty(TARGET_SIGNAL_COUNT).evaluateAttributeExpressions(flowFile).getValue());
                } catch (final NumberFormatException e) {
                    transferToFailure.accept(flowFile);
                    logger.error("Failed to parse targetCount when processing {} due to {}", flowFile, e, e);
                    continue;
                }
                try {
                    releasableFlowFileCount = Integer.parseInt(context.getProperty(RELEASABLE_FLOWFILE_COUNT).evaluateAttributeExpressions(flowFile).getValue());
                } catch (final NumberFormatException e) {
                    transferToFailure.accept(flowFile);
                    logger.error("Failed to parse releasableFlowFileCount when processing {} due to {}", flowFile, e, e);
                    continue;
                }
            }

            // FlowFile is now validated and added to candidates.
            candidates.add(flowFile);
        }

        boolean waitCompleted = false;
        boolean waitProgressed = false;
        if (signal != null && !candidates.isEmpty()) {

            if (releasableFlowFileCount > 0) {
                signal.releaseCandidates(targetCounterName, targetCount, releasableFlowFileCount, candidates,
                        released -> getFlowFilesFor.apply(REL_SUCCESS).addAll(released),
                        waiting -> getFlowFilesFor.apply(REL_WAIT).addAll(waiting));
                waitCompleted = signal.getTotalCount() == 0 && signal.getReleasableCount() == 0;
                waitProgressed = !getFlowFilesFor.apply(REL_SUCCESS).isEmpty();

            } else {
                boolean reachedTargetCount = StringUtils.isBlank(targetCounterName)
                        ? signal.isTotalCountReached(targetCount)
                        : signal.isCountReached(targetCounterName, targetCount);

                if (reachedTargetCount) {
                    getFlowFilesFor.apply(REL_SUCCESS).addAll(candidates);
                } else {
                    getFlowFilesFor.apply(REL_WAIT).addAll(candidates);
                }
            }
        }

        // Transfer FlowFiles.
        processedFlowFiles.entrySet().forEach(transferFlowFiles);

        // Penalize signal id if no FlowFile transferred to success.
        final PropertyValue waitPenaltyDuration = context.getProperty(WAIT_PENALTY_DURATION);
        if (waitPenaltyDuration.isSet() && getFlowFilesFor.apply(REL_SUCCESS).isEmpty()) {
            signalIdPenalties.put(signalId, System.currentTimeMillis() + waitPenaltyDuration.asTimePeriod(TimeUnit.MILLISECONDS));
        }

        // Update signal if needed.
        try {
            if (waitCompleted) {
                protocol.complete(signalId);
            } else if (waitProgressed) {
                protocol.replace(signal);
            }

        } catch (final IOException e) {
            session.rollback();
            throw new ProcessException(String.format("Unable to communicate with cache while updating %s due to %s", signalId, e), e);
        }

    }

    private FlowFile clearWaitState(final ProcessSession session, final FlowFile flowFile) {
        return session.removeAttribute(flowFile, WAIT_START_TIMESTAMP);
    }

    private FlowFile copySignalAttributes(final ProcessSession session, final FlowFile flowFile, final Signal signal, final Map<String, Long> originalCount, final boolean replaceOriginal) {
        if (signal == null) {
            return flowFile;
        }

        // copy over attributes from release signal FlowFile, if provided
        final Map<String, String> attributesToCopy;
        if (replaceOriginal) {
            attributesToCopy = new HashMap<>(signal.getAttributes());
            attributesToCopy.remove("uuid");
        } else {
            // if the current FlowFile does *not* have the cached attribute, copy it
            attributesToCopy = signal.getAttributes().entrySet().stream()
                    .filter(e -> flowFile.getAttribute(e.getKey()) == null)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }

        // Copy counter attributes
        final long totalCount = originalCount.entrySet().stream().mapToLong(e -> {
            final Long count = e.getValue();
            attributesToCopy.put("wait.counter." + e.getKey(), String.valueOf(count));
            return count;
        }).sum();
        attributesToCopy.put("wait.counter.total", String.valueOf(totalCount));

        return session.putAllAttributes(flowFile, attributesToCopy);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        signalIdPenalties.clear();
    }

    Map<String, Long> getSignalIdPenalties() {
        return signalIdPenalties;
    }
}
