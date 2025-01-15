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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Tags({"sort", "order"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@TriggerSerially
@CapabilityDescription("Enforces expected ordering of FlowFiles that belong to the same data group within a single node. " +
        " Although PriorityAttributePrioritizer can be used on a connection to ensure that flow files going through that connection are in priority order," +
        " depending on error-handling, branching, and other flow designs, it is possible for FlowFiles to get out-of-order." +
        " EnforceOrder can be used to enforce original ordering for those FlowFiles." +
        " [IMPORTANT] In order to take effect of EnforceOrder, FirstInFirstOutPrioritizer should be used at EVERY downstream relationship" +
        " UNTIL the order of FlowFiles physically get FIXED by operation such as MergeContent or being stored to the final destination.")
@Stateful(scopes = Scope.LOCAL, description = "EnforceOrder uses following states per ordering group:" +
        " '<groupId>.target' is a order number which is being waited to arrive next." +
        " When a FlowFile with a matching order arrives, or a FlowFile overtakes the FlowFile being waited for because of wait timeout," +
        " target order will be updated to (FlowFile.order + 1)." +
        " '<groupId>.max is the maximum order number for a group." +
        " '<groupId>.updatedAt' is a timestamp when the order of a group was updated last time." +
        " These managed states will be removed automatically once a group is determined as inactive, see 'Inactive Timeout' for detail.")
@WritesAttributes({
    @WritesAttribute(attribute = EnforceOrder.ATTR_STARTED_AT,
        description = "All FlowFiles going through this processor will have this attribute. This value is used to determine wait timeout."),
    @WritesAttribute(attribute = EnforceOrder.ATTR_RESULT,
        description = "All FlowFiles going through this processor will have this attribute denoting which relationship it was routed to."),
    @WritesAttribute(attribute = EnforceOrder.ATTR_DETAIL,
        description = "FlowFiles routed to 'failure' or 'skipped' relationship will have this attribute describing details."),
    @WritesAttribute(attribute = EnforceOrder.ATTR_EXPECTED_ORDER,
        description = "FlowFiles routed to 'wait' or 'skipped' relationship will have this attribute denoting expected order when the FlowFile was processed.")
})
public class EnforceOrder extends AbstractProcessor {

    public static final String ATTR_STARTED_AT = "EnforceOrder.startedAt";
    public static final String ATTR_EXPECTED_ORDER = "EnforceOrder.expectedOrder";
    public static final String ATTR_RESULT = "EnforceOrder.result";
    public static final String ATTR_DETAIL = "EnforceOrder.detail";
    private static final Function<String, String> STATE_TARGET_ORDER = groupId -> groupId + ".target";
    private static final String STATE_SUFFIX_UPDATED_AT = ".updatedAt";
    private static final Function<String, String> STATE_UPDATED_AT = groupId -> groupId + STATE_SUFFIX_UPDATED_AT;
    private static final Function<String, String> STATE_MAX_ORDER = groupId -> groupId + ".max";

    public static final PropertyDescriptor GROUP_IDENTIFIER = new PropertyDescriptor.Builder()
        .name("group-id")
        .displayName("Group Identifier")
        .description("EnforceOrder is capable of multiple ordering groups." +
                " 'Group Identifier' is used to determine which group a FlowFile belongs to." +
                " This property will be evaluated with each incoming FlowFile." +
                " If evaluated result is empty, the FlowFile will be routed to failure.")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${filename}")
        .build();

    public static final PropertyDescriptor ORDER_ATTRIBUTE = new PropertyDescriptor.Builder()
        .name("order-attribute")
        .displayName("Order Attribute")
        .description("A name of FlowFile attribute whose value will be used to enforce order of FlowFiles within a group." +
                " If a FlowFile does not have this attribute, or its value is not an integer, the FlowFile will be routed to failure.")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    public static final PropertyDescriptor INITIAL_ORDER = new PropertyDescriptor.Builder()
        .name("initial-order")
        .displayName("Initial Order")
        .description("When the first FlowFile of a group arrives, initial target order will be computed and stored in the managed state." +
                " After that, target order will start being tracked by EnforceOrder and stored in the state management store." +
                " If Expression Language is used but evaluated result was not an integer, then the FlowFile will be routed to failure," +
                " and initial order will be left unknown until consecutive FlowFiles provide a valid initial order.")
        .required(true)
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("0")
        .build();

    public static final PropertyDescriptor MAX_ORDER = new PropertyDescriptor.Builder()
        .name("maximum-order")
        .displayName("Maximum Order")
        .description("If specified, any FlowFiles that have larger order will be routed to failure." +
                " This property is computed only once for a given group." +
                " After a maximum order is computed, it will be persisted in the state management store and used for other FlowFiles belonging to the same group." +
                " If Expression Language is used but evaluated result was not an integer, then the FlowFile will be routed to failure," +
                " and maximum order will be left unknown until consecutive FlowFiles provide a valid maximum order.")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final PropertyDescriptor WAIT_TIMEOUT = new PropertyDescriptor.Builder()
        .name("wait-timeout")
        .displayName("Wait Timeout")
        .description("Indicates the duration after which waiting FlowFiles will be routed to the 'overtook' relationship.")
        .required(true)
        .defaultValue("10 min")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    public static final PropertyDescriptor INACTIVE_TIMEOUT = new PropertyDescriptor.Builder()
        .name("inactive-timeout")
        .displayName("Inactive Timeout")
        .description("Indicates the duration after which state for an inactive group will be cleared from managed state." +
                " Group is determined as inactive if any new incoming FlowFile has not seen for a group for specified duration." +
                " Inactive Timeout must be longer than Wait Timeout." +
                " If a FlowFile arrives late after its group is already cleared, it will be treated as a brand new group," +
                " but will never match the order since expected preceding FlowFiles are already gone." +
                " The FlowFile will eventually timeout for waiting and routed to 'overtook'." +
                " To avoid this, group states should be kept long enough, however, shorter duration would be helpful for reusing the same group identifier again.")
        .required(true)
        .defaultValue("30 min")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    public static final PropertyDescriptor BATCH_COUNT = new PropertyDescriptor.Builder()
        .name("batch-count")
        .displayName("Batch Count")
        .description("The maximum number of FlowFiles that EnforceOrder can process at an execution.")
        .required(true)
        .defaultValue("1000")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GROUP_IDENTIFIER,
            ORDER_ATTRIBUTE,
            INITIAL_ORDER,
            MAX_ORDER,
            BATCH_COUNT,
            WAIT_TIMEOUT,
            INACTIVE_TIMEOUT
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("A FlowFile with a matching order number will be routed to this relationship.")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("A FlowFiles which does not have required attributes, or fails to compute those will be routed to this relationship")
        .build();

    public static final Relationship REL_WAIT = new Relationship.Builder()
        .name("wait")
        .description("A FlowFile with non matching order will be routed to this relationship")
        .build();

    public static final Relationship REL_OVERTOOK = new Relationship.Builder()
        .name("overtook")
        .description("A FlowFile that waited for preceding FlowFiles longer than Wait Timeout and overtook those FlowFiles, will be routed to this relationship.")
        .build();

    public static final Relationship REL_SKIPPED = new Relationship.Builder()
        .name("skipped")
        .description("A FlowFile that has an order younger than current, which means arrived too late and skipped, will be routed to this relationship.")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_WAIT,
            REL_OVERTOOK,
            REL_FAILURE,
            REL_SKIPPED
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final Long waitTimeoutMillis = validationContext.getProperty(WAIT_TIMEOUT).asTimePeriod(TimeUnit.MICROSECONDS);
        final Long inactiveTimeoutMillis = validationContext.getProperty(INACTIVE_TIMEOUT).asTimePeriod(TimeUnit.MICROSECONDS);

        if (waitTimeoutMillis >= inactiveTimeoutMillis) {
            results.add(new ValidationResult.Builder().input(validationContext.getProperty(INACTIVE_TIMEOUT).getValue())
                    .subject(INACTIVE_TIMEOUT.getDisplayName())
                    .explanation(String.format("%s should be longer than %s",
                            INACTIVE_TIMEOUT.getDisplayName(), WAIT_TIMEOUT.getDisplayName()))
                    .valid(false)
                    .build());
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final Integer batchCount = context.getProperty(BATCH_COUNT).asInteger();

        final List<FlowFile> flowFiles = session.get(batchCount);
        if (flowFiles == null || flowFiles.isEmpty()) {
            return;
        }

        final StateMap stateMap;
        try {
            stateMap = session.getState(Scope.LOCAL);
        } catch (final IOException e) {
            getLogger().error("Failed to retrieve state from StateManager", e);
            context.yield();
            return;
        }

        final OrderingContext oc = new OrderingContext(context, session);

        oc.groupStates.putAll(stateMap.toMap());

        for (final FlowFile flowFile : flowFiles) {
            oc.setFlowFile(flowFile);
            if (oc.flowFile == null) {
                break;
            }

            if (!oc.computeGroupId()
                    || !oc.computeOrder()
                    || !oc.computeInitialOrder()
                    || !oc.computeMaxOrder()) {
                continue;
            }

            // At this point, the flow file is confirmed to be valid.
            oc.markFlowFileValid();
        }

        oc.transferFlowFiles();

        oc.cleanupInactiveStates();

        try {
            session.setState(oc.groupStates, Scope.LOCAL);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to update state due to " + e
                    + ". Session will be rollback and processor will be yielded for a while.", e);
        }

    }

    private class OrderingContext {

        private final ComponentLog logger = getLogger();
        private final ProcessSession processSession;
        private final ProcessContext processContext;

        // Following properties are static global setting for all groups.
        private final String orderAttribute;
        private final Long waitTimeoutMillis;
        private final Function<FlowFile, Integer> getOrder;

        private final Map<String, String> groupStates = new HashMap<>();
        private final long now = System.currentTimeMillis();

        // Following properties are computed per flow file.
        private final PropertyValue groupIdentifierProperty;

        // Followings are per group objects.
        private final PropertyValue initOrderProperty;
        private final PropertyValue maxOrderProperty;
        private final Map<String, List<FlowFile>> flowFileGroups = new TreeMap<>();

        // Current variables within incoming FlowFiles loop.
        private FlowFile flowFile;
        private String groupId;
        private Integer order;

        private OrderingContext(final ProcessContext processContext, final ProcessSession processSession) {
            this.processContext = processContext;
            this.processSession = processSession;

            orderAttribute = processContext.getProperty(ORDER_ATTRIBUTE).getValue();
            waitTimeoutMillis = processContext.getProperty(WAIT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
            getOrder = flowFile -> Integer.parseInt(flowFile.getAttribute(orderAttribute));


            groupIdentifierProperty = processContext.getProperty(GROUP_IDENTIFIER);

            initOrderProperty = processContext.getProperty(INITIAL_ORDER);
            maxOrderProperty = processContext.getProperty(MAX_ORDER);
        }

        private void setFlowFile(final FlowFile flowFile) {
            this.flowFile = flowFile;
            this.groupId = null;
            this.order = null;
        }

        private boolean computeGroupId() {
            groupId = groupIdentifierProperty.evaluateAttributeExpressions(flowFile).getValue();
            if (isBlank(groupId)) {
                transferToFailure(flowFile, "Failed to get Group Identifier.");
                return false;
            }
            return true;
        }

        private boolean computeOrder() {
            try {
                order = getOrder.apply(flowFile);
            } catch (final NumberFormatException e) {
                transferToFailure(flowFile, "Failed to parse order attribute due to " + e, e);
                return false;
            }
            return true;
        }

        private boolean computeMaxOrder() {
            if (maxOrderProperty.isSet()) {
                // Compute maxOrder for this group if it's not there yet.
                final String maxOrderStr = groupStates.computeIfAbsent(STATE_MAX_ORDER.apply(groupId),
                        k -> maxOrderProperty.evaluateAttributeExpressions(flowFile).getValue());
                if (isBlank(maxOrderStr)) {
                    transferToFailure(flowFile, String.format("%s was specified but result was empty.", MAX_ORDER.getDisplayName()));
                    return false;
                }

                final Integer maxOrder;
                try {
                    maxOrder = Integer.parseInt(maxOrderStr);
                } catch (final NumberFormatException e) {
                    final String msg = String.format("Failed to get Maximum Order for group [%s] due to %s", groupId, e);
                    transferToFailure(flowFile, msg, e);
                    return false;
                }

                // Check max order.
                if (order > maxOrder) {
                    final String msg = String.format("Order (%d) is greater than the Maximum Order (%d) for Group [%s]", order, maxOrder, groupId);
                    transferToFailure(flowFile, msg);
                    return false;
                }
            }
            return true;
        }

        private boolean computeInitialOrder() {
            // Compute initial order. Use asInteger() to check if it's a valid integer.
            final String stateKeyOrder = STATE_TARGET_ORDER.apply(groupId);
            try {
                final AtomicReference<String> computedInitOrder = new AtomicReference<>();
                groupStates.computeIfAbsent(stateKeyOrder, k -> {
                    final String initOrderStr = initOrderProperty.evaluateAttributeExpressions(flowFile).getValue();
                    // Parse it to check if it is a valid integer.
                    Integer.parseInt(initOrderStr);
                    computedInitOrder.set(initOrderStr);
                    return initOrderStr;
                });
                // If these map modification is in the computeIfAbsent function, it causes this issue.
                // JDK-8071667 : HashMap.computeIfAbsent() adds entry that HashMap.get() does not find.
                // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=8071667
                if (!isBlank(computedInitOrder.get())) {
                    groupStates.put(STATE_UPDATED_AT.apply(groupId), String.valueOf(now));
                }

            } catch (final NumberFormatException e) {
                final String msg = String.format("Failed to get Initial Order for Group [%s] due to %s", groupId, e);
                transferToFailure(flowFile, msg, e);
                return false;
            }
            return true;
        }

        private void markFlowFileValid() {
            final List<FlowFile> groupedFlowFiles = flowFileGroups.computeIfAbsent(groupId, k -> new ArrayList<>());

            final FlowFile validFlowFile;
            if (isBlank(flowFile.getAttribute(ATTR_STARTED_AT))) {
                validFlowFile = processSession.putAttribute(flowFile, ATTR_STARTED_AT, String.valueOf(now));
            } else {
                validFlowFile = flowFile;
            }

            groupedFlowFiles.add(validFlowFile);
        }

        private void transferFlowFiles() {
            flowFileGroups.entrySet().stream()
                    .filter(entry -> !entry.getValue().isEmpty())
                    .peek(entry -> {
                        // Sort flow files within each group.
                        final List<FlowFile> groupedFlowFiles = entry.getValue();
                        groupedFlowFiles.sort(Comparator.comparing(getOrder));
                    })
                    .forEach(entry -> {
                        // Check current state.
                        final String groupId = entry.getKey();
                        final String stateKeyOrder = STATE_TARGET_ORDER.apply(groupId);
                        final int previousTargetOrder = Integer.parseInt(groupStates.get(stateKeyOrder));
                        final AtomicInteger targetOrder = new AtomicInteger(previousTargetOrder);
                        final List<FlowFile> groupedFlowFiles = entry.getValue();
                        final String maxOrderStr = groupStates.get(STATE_MAX_ORDER.apply(groupId));

                        groupedFlowFiles.forEach(f -> {
                            final Integer order = getOrder.apply(f);
                            final boolean isMaxOrder = !isBlank(maxOrderStr) && order.equals(Integer.parseInt(maxOrderStr));

                            if (order == targetOrder.get()) {
                                transferResult(f, REL_SUCCESS, null, null);
                                if (!isMaxOrder) {
                                    // If max order is specified and this FlowFile has the max order, don't increment target anymore.
                                    targetOrder.incrementAndGet();
                                }

                            } else if (order > targetOrder.get()) {

                                if (now - Long.parseLong(f.getAttribute(ATTR_STARTED_AT)) > waitTimeoutMillis) {
                                    transferResult(f, REL_OVERTOOK, null, targetOrder.get());
                                    targetOrder.set(isMaxOrder ? order : order + 1);
                                } else {
                                    transferResult(f, REL_WAIT, null, targetOrder.get());
                                }

                            } else {
                                final String msg = String.format("Skipped, FlowFile order was %d but current target is %d", order, targetOrder.get());
                                logger.warn("{}. {}", msg, f);
                                transferResult(f, REL_SKIPPED, msg, targetOrder.get());
                            }

                        });

                        if (previousTargetOrder != targetOrder.get()) {
                            groupStates.put(stateKeyOrder, String.valueOf(targetOrder.get()));
                            groupStates.put(STATE_UPDATED_AT.apply(groupId), String.valueOf(now));
                        }
                    });
        }

        private void transferResult(final FlowFile flowFile, final Relationship result, final String detail, final Integer expectedOrder) {
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(ATTR_RESULT, result.getName());
            if (expectedOrder != null) {
                attributes.put(ATTR_EXPECTED_ORDER, expectedOrder.toString());
            }
            if (!isBlank(detail)) {
                attributes.put(ATTR_DETAIL, detail);
            }

            FlowFile resultFlowFile = processSession.putAllAttributes(flowFile, attributes);
            // Remove
            if (expectedOrder == null) {
                resultFlowFile = processSession.removeAttribute(resultFlowFile, ATTR_EXPECTED_ORDER);
            }
            if (detail == null) {
                resultFlowFile = processSession.removeAttribute(resultFlowFile, ATTR_DETAIL);
            }
            processSession.transfer(resultFlowFile, result);
        }

        private void transferToFailure(final FlowFile flowFile, final String message) {
            transferToFailure(flowFile, message, null);
        }

        private void transferToFailure(final FlowFile flowFile, final String message, final Throwable cause) {
            if (cause != null) {
                getLogger().warn("{} {}", message, flowFile, cause);
            } else {
                getLogger().warn("{} {}", message, flowFile);
            }
            transferResult(flowFile, REL_FAILURE, message, null);
        }

        private void cleanupInactiveStates() {
            final Long inactiveTimeout = processContext.getProperty(INACTIVE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
            final List<String> inactiveGroups = groupStates.keySet().stream()
                    .filter(k -> k.endsWith(STATE_SUFFIX_UPDATED_AT) && (now - Long.parseLong(groupStates.get(k)) > inactiveTimeout))
                    .map(k -> k.substring(0, k.length() - STATE_SUFFIX_UPDATED_AT.length()))
                    .toList();
            inactiveGroups.forEach(groupId -> {
                groupStates.remove(STATE_TARGET_ORDER.apply(groupId));
                groupStates.remove(STATE_UPDATED_AT.apply(groupId));
                groupStates.remove(STATE_MAX_ORDER.apply(groupId));
            });
        }

    }


}
