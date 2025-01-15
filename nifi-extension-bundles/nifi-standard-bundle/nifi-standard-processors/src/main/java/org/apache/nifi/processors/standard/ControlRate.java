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
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.timebuffer.LongEntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;
import org.apache.nifi.util.timebuffer.TimestampedLong;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

@SideEffectFree
@TriggerSerially
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"rate control", "throttle", "rate", "throughput"})
@CapabilityDescription("Controls the rate at which data is transferred to follow-on processors."
        + " If you configure a very small Time Duration, then the accuracy of the throttle gets worse."
        + " You can improve this accuracy by decreasing the Yield Duration, at the expense of more Tasks given to the processor.")
@UseCase(description = "Limit the rate at which data is sent to a downstream system with little to no bursts",
    keywords = {"throttle", "limit", "slow down", "data rate"},
    configuration = """
        Set the "Rate Control Criteria" to `data rate`.
        Set the "Time Duration" property to `1 sec`.
        Configure the "Maximum Rate" property to specify how much data should be allowed through each second.

        For example, to allow through 8 MB per second, set "Maximum Rate" to `8 MB`.
        """
)
@UseCase(description = "Limit the rate at which FlowFiles are sent to a downstream system with little to no bursts",
    keywords = {"throttle", "limit", "slow down", "flowfile rate"},
    configuration = """
        Set the "Rate Control Criteria" to `flowfile count`.
        Set the "Time Duration" property to `1 sec`.
        Configure the "Maximum Rate" property to specify how many FlowFiles should be allowed through each second.

        For example, to allow through 100 FlowFiles per second, set "Maximum Rate" to `100`.
        """
)
@UseCase(description = "Reject requests that exceed a specific rate with little to no bursts",
    keywords = {"throttle", "limit", "slow down", "request rate"},
    configuration = """
        Set the "Rate Control Criteria" to `flowfile count`.
        Set the "Time Duration" property to `1 sec`.
        Set the "Rate Exceeded Strategy" property to `Route to 'rate exceeded'`.
        Configure the "Maximum Rate" property to specify how many requests should be allowed through each second.

        For example, to allow through 100 requests per second, set "Maximum Rate" to `100`.
        If more than 100 requests come in during any one second, the additional requests will be routed to `rate exceeded` instead of `success`.
        """
)
@UseCase(description = "Reject requests that exceed a specific rate, allowing for bursts",
    keywords = {"throttle", "limit", "slow down", "request rate"},
    configuration = """
        Set the "Rate Control Criteria" to `flowfile count`.
        Set the "Time Duration" property to `1 min`.
        Set the "Rate Exceeded Strategy" property to `Route to 'rate exceeded'`.
        Configure the "Maximum Rate" property to specify how many requests should be allowed through each minute.

        For example, to allow through 100 requests per second, set "Maximum Rate" to `6000`.
        This will allow through 6,000 FlowFiles per minute, which averages to 100 FlowFiles per second. However, those 6,000 FlowFiles may come all within the first couple of
        seconds, or they may come in over a period of 60 seconds. As a result, this gives us an average rate of 100 FlowFiles per second but allows for bursts of data.
        If more than 6,000 requests come in during any one minute, the additional requests will be routed to `rate exceeded` instead of `success`.
        """
)
public class ControlRate extends AbstractProcessor {

    public static final String DATA_RATE = "data rate";
    public static final String FLOWFILE_RATE = "flowfile count";
    public static final String ATTRIBUTE_RATE = "attribute value";
    public static final String DATA_OR_FLOWFILE_RATE = "data rate or flowfile count";

    public static final AllowableValue DATA_RATE_VALUE = new AllowableValue(DATA_RATE, DATA_RATE,
            "Rate is controlled by counting bytes transferred per time duration.");
    public static final AllowableValue FLOWFILE_RATE_VALUE = new AllowableValue(FLOWFILE_RATE, FLOWFILE_RATE,
            "Rate is controlled by counting FlowFiles transferred per time duration");
    public static final AllowableValue ATTRIBUTE_RATE_VALUE = new AllowableValue(ATTRIBUTE_RATE, ATTRIBUTE_RATE,
            "Rate is controlled by accumulating the value of a specified attribute that is transferred per time duration");
    public static final AllowableValue DATA_OR_FLOWFILE_RATE_VALUE = new AllowableValue(DATA_OR_FLOWFILE_RATE, DATA_OR_FLOWFILE_RATE,
            "Rate is controlled by counting bytes and FlowFiles transferred per time duration; if either threshold is met, throttling is enforced");

    static final AllowableValue HOLD_FLOWFILE = new AllowableValue("Hold FlowFile", "Hold FlowFile",
        "The FlowFile will be held in its input queue until the rate of data has fallen below the configured maximum and will then be allowed through.");
    static final AllowableValue ROUTE_TO_RATE_EXCEEDED = new AllowableValue("Route to 'rate exceeded'", "Route to 'rate exceeded'",
        "The FlowFile will be routed to the 'rate exceeded' Relationship.");

    // based on testing to balance commits and 10,000 FF swap limit
    public static final int MAX_FLOW_FILES_PER_BATCH = 1000;
    private static final long DEFAULT_ACCRUAL_COUNT = -1L;

    public static final PropertyDescriptor RATE_CONTROL_CRITERIA = new PropertyDescriptor.Builder()
            .name("Rate Control Criteria")
            .displayName("Rate Control Criteria")
            .description("Indicates the criteria that is used to control the throughput rate. Changing this value resets the rate counters.")
            .required(true)
            .allowableValues(DATA_RATE_VALUE, FLOWFILE_RATE_VALUE, ATTRIBUTE_RATE_VALUE, DATA_OR_FLOWFILE_RATE_VALUE)
            .defaultValue(DATA_RATE)
            .build();
    public static final PropertyDescriptor MAX_RATE = new PropertyDescriptor.Builder()
            .name("Maximum Rate")
            .displayName("Maximum Rate")
            .description("The maximum rate at which data should pass through this processor. The format of this property is expected to be a "
                    + "positive integer, or a Data Size (such as '1 MB') if Rate Control Criteria is set to 'data rate'.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR) // validated in customValidate b/c dependent on Rate Control Criteria
            .dependsOn(RATE_CONTROL_CRITERIA, DATA_RATE_VALUE, FLOWFILE_RATE_VALUE, ATTRIBUTE_RATE_VALUE)
            .build();
    public static final PropertyDescriptor MAX_DATA_RATE = new PropertyDescriptor.Builder()
            .name("Maximum Data Rate")
            .displayName("Maximum Data Rate")
            .description("The maximum rate at which data should pass through this processor. The format of this property is expected to be a "
                    + "Data Size (such as '1 MB') representing bytes per Time Duration.")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .dependsOn(RATE_CONTROL_CRITERIA, DATA_OR_FLOWFILE_RATE)
            .build();

    public static final PropertyDescriptor MAX_COUNT_RATE = new PropertyDescriptor.Builder()
            .name("Maximum FlowFile Rate")
            .displayName("Maximum FlowFile Rate")
            .description("The maximum rate at which FlowFiles should pass through this processor. The format of this property is expected to be a "
                    + "positive integer representing FlowFiles count per Time Duration")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .dependsOn(RATE_CONTROL_CRITERIA, DATA_OR_FLOWFILE_RATE)
            .build();

    public static final PropertyDescriptor RATE_EXCEEDED_STRATEGY = new PropertyDescriptor.Builder()
        .name("Rate Exceeded Strategy")
        .description("Specifies how to handle an incoming FlowFile when the maximum data rate has been exceeded.")
        .required(true)
        .allowableValues(HOLD_FLOWFILE, ROUTE_TO_RATE_EXCEEDED)
        .defaultValue(HOLD_FLOWFILE.getValue())
        .build();

    public static final PropertyDescriptor RATE_CONTROL_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Rate Controlled Attribute")
            .description("The name of an attribute whose values build toward the rate limit if Rate Control Criteria is set to 'attribute value'. "
                    + "The value of the attribute referenced by this property must be a positive long, or the FlowFile will be routed to failure. "
                    + "This value is ignored if Rate Control Criteria is not set to 'attribute value'. Changing this value resets the rate counters.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(RATE_CONTROL_CRITERIA, ATTRIBUTE_RATE)
            .build();
    public static final PropertyDescriptor TIME_PERIOD = new PropertyDescriptor.Builder()
            .name("Time Duration")
            .description("The amount of time to which the Maximum Rate pertains. Changing this value resets the rate counters.")
            .required(true)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .defaultValue("1 min")
            .build();
    public static final PropertyDescriptor GROUPING_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Grouping Attribute")
            .description("By default, a single \"throttle\" is used for all FlowFiles. If this value is specified, a separate throttle is used for "
                    + "each value specified by the attribute with this name. Changing this value resets the rate counters.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RATE_CONTROL_CRITERIA,
            TIME_PERIOD,
            MAX_RATE,
            MAX_DATA_RATE,
            MAX_COUNT_RATE,
            RATE_EXCEEDED_STRATEGY,
            RATE_CONTROL_ATTRIBUTE_NAME,
            GROUPING_ATTRIBUTE_NAME
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are transferred to this relationship under normal conditions")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles will be routed to this relationship if they are missing a necessary Rate Controlled Attribute or the attribute is not in the expected format")
            .build();
    static final Relationship REL_RATE_EXCEEDED = new Relationship.Builder()
        .name("rate exceeded")
        .description("A FlowFile will be routed to this Relationship if it results in exceeding the maximum threshold allowed based on the Processor's configuration and if the Rate Exceeded " +
            "Strategy is configured to use this Relationship.")
        .build();

    private static final Set<Relationship> DEFAULT_RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );
    private static final Set<Relationship> RATE_EXCEEDED_RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_RATE_EXCEEDED
    );

    private static final Pattern POSITIVE_LONG_PATTERN = Pattern.compile("0*[1-9][0-9]*");
    private static final String DEFAULT_GROUP_ATTRIBUTE = ControlRate.class.getName() + "###____DEFAULT_GROUP_ATTRIBUTE___###";

    private volatile Set<Relationship> relationships = DEFAULT_RELATIONSHIPS;

    private final ConcurrentMap<String, Throttle> dataThrottleMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Throttle> countThrottleMap = new ConcurrentHashMap<>();
    private final AtomicLong lastThrottleClearTime = new AtomicLong(getCurrentTimeMillis());
    private volatile String rateControlCriteria = null;
    private volatile String rateControlAttribute = null;
    private volatile String maximumRateStr = null;
    private volatile String maximumCountRateStr = null;
    private volatile String groupingAttributeName = null;
    private volatile int timePeriodSeconds = 1;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        switch (context.getProperty(RATE_CONTROL_CRITERIA).getValue().toLowerCase()) {
            case DATA_OR_FLOWFILE_RATE:
                // enforce validators to be sure properties are configured; they are only required for DATA_OR_FLOWFILE_RATE criteria
                validationResults.add(StandardValidators.DATA_SIZE_VALIDATOR.validate(MAX_DATA_RATE.getDisplayName(), context.getProperty(MAX_DATA_RATE).getValue(), context));
                validationResults.add(StandardValidators.POSITIVE_LONG_VALIDATOR.validate(MAX_COUNT_RATE.getDisplayName(), context.getProperty(MAX_COUNT_RATE).getValue(), context));
                break;
            case DATA_RATE:
                validationResults.add(StandardValidators.DATA_SIZE_VALIDATOR.validate("Maximum Rate", context.getProperty(MAX_RATE).getValue(), context));
                break;

            case ATTRIBUTE_RATE:
                final String rateAttr = context.getProperty(RATE_CONTROL_ATTRIBUTE_NAME).getValue();
                if (rateAttr == null) {
                    validationResults.add(new ValidationResult.Builder()
                            .subject(RATE_CONTROL_ATTRIBUTE_NAME.getName())
                            .explanation("property must be set if using <Rate Control Criteria> of 'attribute value'")
                            .build());
                }
            case FLOWFILE_RATE:
                validationResults.add(StandardValidators.POSITIVE_LONG_VALIDATOR.validate("Maximum Rate", context.getProperty(MAX_RATE).getValue(), context));
                break;
            default:
                // no custom validation required
                break;
        }

        return validationResults;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);

        if (descriptor.equals(RATE_EXCEEDED_STRATEGY)) {
            if (ROUTE_TO_RATE_EXCEEDED.getValue().equalsIgnoreCase(newValue)) {
                this.relationships = RATE_EXCEEDED_RELATIONSHIPS;
            } else {
                this.relationships = DEFAULT_RELATIONSHIPS;
            }
        }

        if (descriptor.equals(RATE_CONTROL_CRITERIA)
                || descriptor.equals(RATE_CONTROL_ATTRIBUTE_NAME)
                || descriptor.equals(GROUPING_ATTRIBUTE_NAME)
                || descriptor.equals(TIME_PERIOD)) {
            // if the criteria that is being used to determine limits/throttles is changed, we must clear our throttle map.
            dataThrottleMap.clear();
            countThrottleMap.clear();
        } else if (descriptor.equals(MAX_RATE) || descriptor.equals(MAX_DATA_RATE)) {
            // MAX_RATE could affect either throttle map; MAX_DATA_RATE only affects data throttle map
            final long newRate;
            if (newValue != null) {
                if (DataUnit.DATA_SIZE_PATTERN.matcher(newValue.toUpperCase()).matches()) {
                    newRate = DataUnit.parseDataSize(newValue, DataUnit.B).longValue();
                } else {
                    newRate = Long.parseLong(newValue);
                }
                if (dataThrottleRequired()) {
                    for (final Throttle throttle : dataThrottleMap.values()) {
                        throttle.setMaxRate(newRate);
                    }
                }
                if (countThrottleRequired()) {
                    for (final Throttle throttle : countThrottleMap.values()) {
                        throttle.setMaxRate(newRate);
                    }
                }
            }
        } else if (descriptor.equals(MAX_COUNT_RATE)) {
            // MAX_COUNT_RATE only affects count throttle map
            long newRate;
            try {
                newRate = Long.parseLong(newValue);
            } catch (NumberFormatException nfe) {
                newRate = -1;
            }
            for (final Throttle throttle : countThrottleMap.values()) {
                throttle.setMaxRate(newRate);
            }
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        rateControlCriteria = context.getProperty(RATE_CONTROL_CRITERIA).getValue().toLowerCase();
        rateControlAttribute = context.getProperty(RATE_CONTROL_ATTRIBUTE_NAME).getValue();
        if (dataThrottleRequired()) {
            // Use MAX_DATA_RATE only for DATA_OR_FLOWFILE_RATE criteria
            maximumRateStr = rateControlCriteria.equals(DATA_OR_FLOWFILE_RATE)
                    ? context.getProperty(MAX_DATA_RATE).getValue().toUpperCase() : context.getProperty(MAX_RATE).getValue().toUpperCase();
        }
        if (countThrottleRequired()) {
            // Use MAX_COUNT_RATE only for DATA_OR_FLOWFILE_RATE criteria
            maximumCountRateStr = rateControlCriteria.equals(DATA_OR_FLOWFILE_RATE)
                    ? context.getProperty(MAX_COUNT_RATE).getValue() : context.getProperty(MAX_RATE).getValue();
        }
        groupingAttributeName = context.getProperty(GROUPING_ATTRIBUTE_NAME).getValue();
        timePeriodSeconds = context.getProperty(TIME_PERIOD).asTimePeriod(TimeUnit.SECONDS).intValue();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String strategy = context.getProperty(RATE_EXCEEDED_STRATEGY).getValue();
        if (ROUTE_TO_RATE_EXCEEDED.getValue().equalsIgnoreCase(strategy)) {
            routeFlowFilesExceedingRate(context, session);
        } else {
            holdFlowFilesExceedingRate(context, session);
        }
    }

    private void routeFlowFilesExceedingRate(final ProcessContext context, final ProcessSession session) {
        clearExpiredThrottles(context);

        final List<FlowFile> flowFiles = session.get(MAX_FLOW_FILES_PER_BATCH);
        if (flowFiles.isEmpty()) {
            context.yield();
            return;
        }

        final ThrottleFilter filter = new ThrottleFilter(MAX_FLOW_FILES_PER_BATCH, this::getCurrentTimeMillis);
        for (final FlowFile flowFile : flowFiles) {
            final Relationship relationship;
            if (!isRateAttributeValid(flowFile)) {
                relationship = REL_FAILURE;
            } else {
                final FlowFileFilterResult result = filter.filter(flowFile);
                relationship = result.isAccept() ? REL_SUCCESS : REL_RATE_EXCEEDED;
            }

            session.transfer(flowFile, relationship);
            getLogger().info("Routing {} to {}", flowFile, relationship.getName());
            session.getProvenanceReporter().route(flowFile, relationship);
        }
    }


    private void holdFlowFilesExceedingRate(final ProcessContext context, final ProcessSession session) {
        clearExpiredThrottles(context);

        final List<FlowFile> flowFiles = session.get(new ThrottleFilter(MAX_FLOW_FILES_PER_BATCH, this::getCurrentTimeMillis));
        if (flowFiles.isEmpty()) {
            context.yield();
            return;
        }

        final ComponentLog logger = getLogger();
        for (FlowFile flowFile : flowFiles) {
            // call this to capture potential error
            if (isRateAttributeValid(flowFile)) {
                logger.info("transferring {} to 'success'", flowFile);
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                logger.error("Routing {} to 'failure' due to missing or invalid attribute", flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    private void clearExpiredThrottles(final ProcessContext context) {
        // Periodically clear any Throttle that has not been used in more than 2 throttling periods
        final long lastClearTime = lastThrottleClearTime.get();
        final long throttleExpirationMillis = getCurrentTimeMillis() - 2 * context.getProperty(TIME_PERIOD).asTimePeriod(TimeUnit.MILLISECONDS);
        if (lastClearTime < throttleExpirationMillis) {
            if (lastThrottleClearTime.compareAndSet(lastClearTime, getCurrentTimeMillis())) {
                final Set<Map.Entry<String, Throttle>> throttleSet = new HashSet<>();
                if (dataThrottleRequired()) {
                    throttleSet.addAll(dataThrottleMap.entrySet());
                }
                if (countThrottleRequired()) {
                    throttleSet.addAll(countThrottleMap.entrySet());
                }
                final Iterator<Map.Entry<String, Throttle>> itr = throttleSet.iterator();
                while (itr.hasNext()) {
                    final Map.Entry<String, Throttle> entry = itr.next();
                    final Throttle throttle = entry.getValue();
                    if (throttle.tryLock()) {
                        try {
                            if (throttle.lastUpdateTime() < lastClearTime) {
                                itr.remove();
                            }
                        } finally {
                            throttle.unlock();
                        }
                    }
                }
            }
        }
    }

    /**
     * Get current time in milliseconds
     *
     * @return Current time in milliseconds from System
     */
    protected long getCurrentTimeMillis() {
        return System.currentTimeMillis();
    }

    /*
     * Determine if the accrual amount is valid for the type of throttle being applied. For example, if throttling based on
     * flowfile attribute, the specified attribute must be present and must be a long integer.
     */
    private boolean isRateAttributeValid(FlowFile flowFile) {
        if (rateControlCriteria.equals(ATTRIBUTE_RATE)) {
            final String attributeValue = flowFile.getAttribute(rateControlAttribute);
            return attributeValue != null && POSITIVE_LONG_PATTERN.matcher(attributeValue).matches();
        }
        return true;
    }

    /*
     * Determine the amount this FlowFile will incur against the maximum allowed rate.
     * This is applicable to data size accrual only
     */
    private long getDataSizeAccrual(FlowFile flowFile) {
        return flowFile.getSize();
    }

    /*
     * Determine the amount this FlowFile will incur against the maximum allowed rate.
     * This is applicable to counting accruals, flowfiles or attributes
     */
    private long getCountAccrual(FlowFile flowFile) {
        long rateValue = DEFAULT_ACCRUAL_COUNT;
        if (rateControlCriteria.equals(FLOWFILE_RATE) || rateControlCriteria.equals(DATA_OR_FLOWFILE_RATE)) {
            rateValue = 1;
        }
        if (rateControlCriteria.equals(ATTRIBUTE_RATE)) {
            final String attributeValue = flowFile.getAttribute(rateControlAttribute);
            if (attributeValue == null) {
                return DEFAULT_ACCRUAL_COUNT;
            }

            if (!POSITIVE_LONG_PATTERN.matcher(attributeValue).matches()) {
                return DEFAULT_ACCRUAL_COUNT;
            }
            rateValue = Long.parseLong(attributeValue);
        }
        return rateValue;
    }

    private boolean dataThrottleRequired() {
        return rateControlCriteria != null && (rateControlCriteria.equals(DATA_RATE) || rateControlCriteria.equals(DATA_OR_FLOWFILE_RATE));
    }

    private boolean countThrottleRequired() {
        return rateControlCriteria != null && (rateControlCriteria.equals(FLOWFILE_RATE) || rateControlCriteria.equals(ATTRIBUTE_RATE) || rateControlCriteria.equals(DATA_OR_FLOWFILE_RATE));
    }

    private static class Throttle extends ReentrantLock {

        private final AtomicLong maxRate = new AtomicLong(1L);
        private final long timePeriodMillis;
        private final TimedBuffer<TimestampedLong> timedBuffer;
        private final ComponentLog logger;
        private final LongSupplier currentTimeSupplier;

        private volatile long penalizationPeriod = 0;
        private volatile long penalizationExpired = 0;
        private volatile long lastUpdateTime;

        private Throttle(final int timePeriod, final TimeUnit unit, final ComponentLog logger, final LongSupplier currentTimeSupplier) {
            this.timePeriodMillis = TimeUnit.MILLISECONDS.convert(timePeriod, unit);
            this.timedBuffer = new TimedBuffer<>(unit, timePeriod, new LongEntityAccess(), currentTimeSupplier);
            this.logger = logger;
            this.currentTimeSupplier = currentTimeSupplier;
        }

        public void setMaxRate(final long maxRate) {
            this.maxRate.set(maxRate);
        }

        public long lastUpdateTime() {
            return lastUpdateTime;
        }

        public boolean tryAdd(final long value) {
            // value should never be negative, but if it is return immediately
            if (value < 0) {
                return false;
            }
            final long now = currentTimeSupplier.getAsLong();
            if (penalizationExpired > now) {
                return false;
            }

            final long maxRateValue = maxRate.get();

            final TimestampedLong sum = timedBuffer.getAggregateValue(timePeriodMillis);
            if (sum != null && sum.getValue() >= maxRateValue) {
                if (logger.isDebugEnabled()) {
                    logger.debug("current sum for throttle is {} at time {}, so not allowing rate of {} through", sum.getValue(), sum.getTimestamp(), value);
                }
                return false;
            }

            // Implement the Throttle penalization based on how much extra 'amountOver' was allowed through
            if (penalizationPeriod > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Starting Throttle penalization, expiring {} milliseconds from now", penalizationPeriod);
                }
                penalizationExpired = now + penalizationPeriod;
                penalizationPeriod = 0;
                return false;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("current sum for throttle is {} at time {}, so allowing rate of {} through",
                        sum == null ? 0 : sum.getValue(), sum == null ? 0 : sum.getTimestamp(), value);
            }

            final long transferred = timedBuffer.add(new TimestampedLong(value)).getValue();
            if (transferred > maxRateValue) {
                final long amountOver = transferred - maxRateValue;
                // determine how long it should take to transfer 'amountOver' and 'penalize' the Throttle for that long
                final double pct = (double) amountOver / (double) maxRateValue;
                this.penalizationPeriod = (long) (timePeriodMillis * pct);

                if (logger.isDebugEnabled()) {
                    logger.debug("allowing rate of {} through but penalizing Throttle for {} milliseconds", value, penalizationPeriod);
                }
            }

            lastUpdateTime = now;
            return true;
        }
    }

    private class ThrottleFilter implements FlowFileFilter {

        private final int flowFilesPerBatch;
        private final LongSupplier currentTimeSupplier;
        private int flowFilesInBatch = 0;

        ThrottleFilter(final int maxFFPerBatch, final LongSupplier currentTimeSupplier) {
            this.flowFilesPerBatch = maxFFPerBatch;
            this.currentTimeSupplier = currentTimeSupplier;
        }

        @Override
        public FlowFileFilterResult filter(FlowFile flowFile) {
            if (!isRateAttributeValid(flowFile)) {
                // this FlowFile is invalid for this configuration so let the processor deal with it
                return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
            }

            String groupName = (groupingAttributeName == null) ? DEFAULT_GROUP_ATTRIBUTE : flowFile.getAttribute(groupingAttributeName);

            // the flow file may not have the required attribute: in this case it is considered part
            // of the DEFAULT_GROUP_ATTRIBUTE
            if (groupName == null) {
                groupName = DEFAULT_GROUP_ATTRIBUTE;
            }

            Throttle dataThrottle = dataThrottleMap.get(groupName);
            Throttle countThrottle = countThrottleMap.get(groupName);

            boolean dataThrottlingActive = false;
            if (dataThrottleRequired()) {
                if (dataThrottle == null) {
                    dataThrottle = new Throttle(timePeriodSeconds, TimeUnit.SECONDS, getLogger(), currentTimeSupplier);
                    dataThrottle.setMaxRate(DataUnit.parseDataSize(maximumRateStr, DataUnit.B).longValue());
                    dataThrottleMap.put(groupName, dataThrottle);
                }

                dataThrottle.lock();
                try {
                    if (dataThrottle.tryAdd(getDataSizeAccrual(flowFile))) {
                        flowFilesInBatch++;
                        if (flowFilesInBatch >= flowFilesPerBatch) {
                            flowFilesInBatch = 0;
                            return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
                        } else {
                            // only accept flowfile if additional count throttle does not need to run
                            if (!countThrottleRequired()) {
                                return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                            }
                        }
                    } else {
                        dataThrottlingActive = true;
                    }
                } finally {
                    dataThrottle.unlock();
                }
            }

            // continue processing count throttle only if required and if data throttle is not already limiting flowfiles
            if (countThrottleRequired() && !dataThrottlingActive) {
                if (countThrottle == null) {
                    countThrottle = new Throttle(timePeriodSeconds, TimeUnit.SECONDS, getLogger(), currentTimeSupplier);
                    countThrottle.setMaxRate(Long.parseLong(maximumCountRateStr));
                    countThrottleMap.put(groupName, countThrottle);
                }
                countThrottle.lock();
                try {
                    if (countThrottle.tryAdd(getCountAccrual(flowFile))) {
                        flowFilesInBatch++;
                        if (flowFilesInBatch >= flowFilesPerBatch) {
                            flowFilesInBatch = 0;
                            return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
                        } else {
                            return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                        }
                    }
                } finally {
                    countThrottle.unlock();
                }
            }

            // If we are not using a grouping attribute, then no FlowFile will be able to continue on. So we can
            // just TERMINATE the iteration over FlowFiles.
            // However, if we are using a grouping attribute, then another FlowFile in the queue may be able to proceed,
            // so we want to continue our iteration.
            if (groupingAttributeName == null) {
                return FlowFileFilterResult.REJECT_AND_TERMINATE;
            }

            return FlowFileFilterResult.REJECT_AND_CONTINUE;
        }
    }
}
