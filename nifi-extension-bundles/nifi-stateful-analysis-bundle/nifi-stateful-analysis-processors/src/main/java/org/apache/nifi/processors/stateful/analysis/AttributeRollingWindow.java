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
package org.apache.nifi.processors.stateful.analysis;

import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow.ROLLING_WINDOW_COUNT_KEY;
import static org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow.ROLLING_WINDOW_MEAN_KEY;
import static org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow.ROLLING_WINDOW_STDDEV_KEY;
import static org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow.ROLLING_WINDOW_VALUE_KEY;
import static org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow.ROLLING_WINDOW_VARIANCE_KEY;

@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Attribute Expression Language", "state", "data science", "rolling", "window"})
@CapabilityDescription("Track a Rolling Window based on evaluating an Expression Language expression on each FlowFile and add that value to the processor's state. Each FlowFile will be emitted " +
        "with the count of FlowFiles and total aggregate value of values processed in the current time window.")
@WritesAttributes({
        @WritesAttribute(attribute = ROLLING_WINDOW_VALUE_KEY, description = "The rolling window value (sum of all the values stored)."),
        @WritesAttribute(attribute = ROLLING_WINDOW_COUNT_KEY, description = "The count of the number of FlowFiles seen in the rolling window."),
        @WritesAttribute(attribute = ROLLING_WINDOW_MEAN_KEY, description = "The mean of the FlowFiles seen in the rolling window."),
        @WritesAttribute(attribute = ROLLING_WINDOW_VARIANCE_KEY, description = "The variance of the FlowFiles seen in the rolling window."),
        @WritesAttribute(attribute = ROLLING_WINDOW_STDDEV_KEY, description = "The standard deviation (positive square root of the variance) of the FlowFiles seen in the rolling window.")
})
@Stateful(scopes = {Scope.LOCAL}, description = "Store the values backing the rolling window. This includes storing the individual values and their time-stamps or the batches of values and their " +
        "counts.")
public class AttributeRollingWindow extends AbstractProcessor {

    public static final String COUNT_KEY = "count";
    public static final String ROLLING_WINDOW_VALUE_KEY = "rolling_window_value";
    public static final String ROLLING_WINDOW_COUNT_KEY = "rolling_window_count";
    public static final String ROLLING_WINDOW_MEAN_KEY = "rolling_window_mean";
    public static final String ROLLING_WINDOW_VARIANCE_KEY = "rolling_window_variance";
    public static final String ROLLING_WINDOW_STDDEV_KEY = "rolling_window_stddev";

    public static final String CURRENT_MICRO_BATCH_STATE_TS_KEY = "start_curr_batch_ts";
    public static final String BATCH_APPEND_KEY = "_batch";
    public static final String COUNT_APPEND_KEY = "_count";
    public static final int COUNT_APPEND_KEY_LENGTH = 6;

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("All FlowFiles are successfully processed are routed here")
            .name("success")
            .build();
    public static final Relationship REL_FAILED_SET_STATE = new Relationship.Builder()
            .name("set state fail")
            .description("When state fails to save when processing a FlowFile, the FlowFile is routed here.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("When a FlowFile fails for a reason other than failing to set state it is routed here.")
            .build();

    static final PropertyDescriptor VALUE_TO_TRACK = new PropertyDescriptor.Builder()
            .displayName("Value to track")
            .name("Value to track")
            .description("The expression on which to evaluate each FlowFile. The result of the expression will be added to the rolling window value.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor TIME_WINDOW = new PropertyDescriptor.Builder()
            .displayName("Time window")
            .name("Time window")
            .description("The time window on which to calculate the rolling window.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor SUB_WINDOW_LENGTH = new PropertyDescriptor.Builder()
            .displayName("Sub-window length")
            .name("Sub-window length")
            .description("When set, values will be batched into sub-windows of the set length. This allows for much larger length total windows to be set but sacrifices some precision. If this is " +
                    "not set (or is 0) then each value is stored in state with the timestamp of when it was received. After the length of time stated in " + TIME_WINDOW.getDisplayName() +
                    " elaspes the value will be removed. If this is set, values will be batched together every X amount of time (where X is the time period set for this property) and removed " +
                    "all at once.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(false)
            .build();

    private static final Scope SCOPE = Scope.LOCAL;

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
                REL_SUCCESS,
                REL_FAILED_SET_STATE,
                REL_FAILURE);

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            VALUE_TO_TRACK,
            TIME_WINDOW,
            SUB_WINDOW_LENGTH
    );

    private Long timeWindow;
    private Long microBatchTime;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        timeWindow = context.getProperty(TIME_WINDOW).asTimePeriod(TimeUnit.MILLISECONDS);
        microBatchTime = context.getProperty(SUB_WINDOW_LENGTH).asTimePeriod(TimeUnit.MILLISECONDS);

        if (microBatchTime == null || microBatchTime == 0) {
            StateManager stateManager = context.getStateManager();
            StateMap state = stateManager.getState(SCOPE);
            HashMap<String, String> tempMap = new HashMap<>();
            tempMap.putAll(state.toMap());
            if (!tempMap.containsKey(COUNT_KEY)) {
                tempMap.put(COUNT_KEY, "0");
                context.getStateManager().setState(tempMap, SCOPE);
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            Long currTime = System.currentTimeMillis();
            if (microBatchTime == null) {
                noMicroBatch(context, session, flowFile, currTime);
            } else {
                microBatch(context, session, flowFile, currTime);
            }

        } catch (Exception e) {
            getLogger().error("Ran into an error while processing {}.", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void noMicroBatch(ProcessContext context, ProcessSession session, FlowFile flowFile, Long currTime) {
        Map<String, String> state = null;
        try {
            state = new HashMap<>(session.getState(SCOPE).toMap());
        } catch (IOException e) {
            getLogger().error("Failed to get the initial state when processing {}; transferring FlowFile back to its incoming queue", flowFile, e);
            session.transfer(flowFile);
            context.yield();
            return;
        }

        Long count = Long.valueOf(state.get(COUNT_KEY));
        count++;

        Set<String> keysToRemove = new HashSet<>();

        for (String key: state.keySet()) {
            if (!key.equals(COUNT_KEY)) {
                Long timeStamp = Long.decode(key);

                if (currTime - timeStamp > timeWindow) {
                    keysToRemove.add(key);
                    count--;

                }
            }
        }
        String countString = String.valueOf(count);

        for (String key: keysToRemove) {
            state.remove(key);
        }

        Double aggregateValue = 0.0D;
        Variance variance = new Variance();

        for (Map.Entry<String, String> entry: state.entrySet()) {
            if (!entry.getKey().equals(COUNT_KEY)) {
                final Double value = Double.valueOf(entry.getValue());
                variance.increment(value);
                aggregateValue += value;
            }
        }

        final Double currentFlowFileValue = context.getProperty(VALUE_TO_TRACK).evaluateAttributeExpressions(flowFile).asDouble();
        variance.increment(currentFlowFileValue);
        aggregateValue += currentFlowFileValue;

        state.put(String.valueOf(currTime), String.valueOf(currentFlowFileValue));
        state.put(COUNT_KEY, countString);

        try {
            session.setState(state, SCOPE);
        } catch (IOException e) {
            getLogger().error("Failed to set the state after successfully processing {} due a failure when setting the state. Transferring to '{}'",
                    flowFile, REL_FAILED_SET_STATE.getName(), e);

            session.transfer(flowFile, REL_FAILED_SET_STATE);
            context.yield();
            return;
        }

        Double mean = aggregateValue / count;

        Map<String, String> attributesToAdd = new HashMap<>();
        attributesToAdd.put(ROLLING_WINDOW_VALUE_KEY, String.valueOf(aggregateValue));
        attributesToAdd.put(ROLLING_WINDOW_COUNT_KEY, String.valueOf(count));
        attributesToAdd.put(ROLLING_WINDOW_MEAN_KEY, String.valueOf(mean));
        double varianceValue = variance.getResult();
        attributesToAdd.put(ROLLING_WINDOW_VARIANCE_KEY, String.valueOf(varianceValue));
        attributesToAdd.put(ROLLING_WINDOW_STDDEV_KEY, String.valueOf(Math.sqrt(varianceValue)));

        flowFile = session.putAllAttributes(flowFile, attributesToAdd);

        session.transfer(flowFile, REL_SUCCESS);
    }

    private void microBatch(ProcessContext context, ProcessSession session, FlowFile flowFile, Long currTime) {
        Map<String, String> state = null;
        try {
            state = new HashMap<>(session.getState(SCOPE).toMap());
        } catch (IOException e) {
            getLogger().error("Failed to get the initial state when processing {}; transferring FlowFile back to its incoming queue", flowFile, e);
            session.transfer(flowFile);
            context.yield();
            return;
        }

        String currBatchStart = state.get(CURRENT_MICRO_BATCH_STATE_TS_KEY);
        boolean newBatch = false;
        if (currBatchStart != null) {
            if (currTime - Long.valueOf(currBatchStart) > microBatchTime) {
                newBatch = true;
                currBatchStart = String.valueOf(currTime);
                state.put(CURRENT_MICRO_BATCH_STATE_TS_KEY, currBatchStart);
            }
        } else {
            newBatch = true;
            currBatchStart = String.valueOf(currTime);
            state.put(CURRENT_MICRO_BATCH_STATE_TS_KEY, currBatchStart);
        }

        Long count = 0L;
        count += 1;

        Set<String> keysToRemove = new HashSet<>();

        for (String key : state.keySet()) {
            String timeStampString;
            if (key.endsWith(BATCH_APPEND_KEY)) {
                timeStampString = key.substring(0, key.length() - COUNT_APPEND_KEY_LENGTH);
                Long timeStamp = Long.decode(timeStampString);

                if (currTime - timeStamp  > timeWindow) {
                    keysToRemove.add(key);
                }
            } else if (key.endsWith(COUNT_APPEND_KEY)) {
                timeStampString = key.substring(0, key.length() - COUNT_APPEND_KEY_LENGTH);
                Long timeStamp = Long.decode(timeStampString);

                if (currTime - timeStamp > timeWindow) {
                    keysToRemove.add(key);
                } else {
                    count += Long.valueOf(state.get(key));
                }
            }
        }

        for (String key:keysToRemove) {
            state.remove(key);
        }
        keysToRemove.clear();

        Double aggregateValue = 0.0D;
        Double currentBatchValue =  0.0D;
        Long currentBatchCount = 0L;
        Variance variance = new Variance();

        for (Map.Entry<String, String> entry: state.entrySet()) {
            String key = entry.getKey();
            if (key.endsWith(BATCH_APPEND_KEY)) {
                String timeStampString = key.substring(0, key.length() - COUNT_APPEND_KEY_LENGTH);

                Double batchValue = Double.valueOf(entry.getValue());
                Long batchCount = Long.valueOf(state.get(timeStampString + COUNT_APPEND_KEY));
                if (!newBatch && timeStampString.equals(currBatchStart)) {

                    final Double currentFlowFileValue = context.getProperty(VALUE_TO_TRACK).evaluateAttributeExpressions(flowFile).asDouble();
                    batchCount++;

                    batchValue += currentFlowFileValue;
                    currentBatchValue = batchValue;
                    currentBatchCount = batchCount;
                }

                aggregateValue += batchValue;
                variance.increment(batchValue);
            }
        }

        if (newBatch) {
            final Double currentFlowFileValue = context.getProperty(VALUE_TO_TRACK).evaluateAttributeExpressions(flowFile).asDouble();

            currentBatchValue += currentFlowFileValue;
            currentBatchCount = 1L;

            aggregateValue += currentBatchValue;
            variance.increment(currentBatchValue);
        }

        state.put(currBatchStart + BATCH_APPEND_KEY, String.valueOf(currentBatchValue));
        state.put(currBatchStart + COUNT_APPEND_KEY, String.valueOf(currentBatchCount));

        try {
            session.setState(state, SCOPE);
        } catch (IOException e) {
            getLogger().error("Failed to get the initial state when processing {}; transferring FlowFile back to its incoming queue", flowFile, e);
            session.transfer(flowFile);
            context.yield();
            return;
        }

        Double mean = aggregateValue / count;

        Map<String, String> attributesToAdd = new HashMap<>();
        attributesToAdd.put(ROLLING_WINDOW_VALUE_KEY, String.valueOf(aggregateValue));
        attributesToAdd.put(ROLLING_WINDOW_COUNT_KEY, String.valueOf(count));
        attributesToAdd.put(ROLLING_WINDOW_MEAN_KEY, String.valueOf(mean));
        double varianceValue = variance.getResult();
        attributesToAdd.put(ROLLING_WINDOW_VARIANCE_KEY, String.valueOf(varianceValue));
        attributesToAdd.put(ROLLING_WINDOW_STDDEV_KEY, String.valueOf(Math.sqrt(varianceValue)));

        flowFile = session.putAllAttributes(flowFile, attributesToAdd);

        session.transfer(flowFile, REL_SUCCESS);
    }
}
