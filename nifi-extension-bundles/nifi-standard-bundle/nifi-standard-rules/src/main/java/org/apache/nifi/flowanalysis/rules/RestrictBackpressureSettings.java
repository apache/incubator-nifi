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
package org.apache.nifi.flowanalysis.rules;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tags({"connection", "backpressure"})
@CapabilityDescription("This rule will generate a violation if backpressure settings of a connection exceed configured thresholds. "
        + "Improper configuration of backpressure settings can lead to decreased performance because of excessive swapping and can "
        + "fill up the content repository with too much in-flight data.")
public class RestrictBackpressureSettings extends AbstractFlowAnalysisRule {

    public static final PropertyDescriptor COUNT_MIN = new PropertyDescriptor.Builder()
            .name("Minimum Backpressure Object Count Threshold")
            .description("This is the minimum value that should be set for the Object Count backpressure setting on connections. "
                    + "This can be used to prevent a user from setting a value of 0 which disables backpressure based on count.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor COUNT_MAX = new PropertyDescriptor.Builder()
            .name("Maximum Backpressure Object Count Threshold")
            .description("This is the maximum value that should be set for the Object Count backpressure setting on connections. "
                    + "This can be used to prevent a user from setting a very high value that may be leading to a lot of swapping.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .defaultValue("10000")
            .build();

    public static final PropertyDescriptor SIZE_MIN = new PropertyDescriptor.Builder()
            .name("Minimum Backpressure Data Size Threshold")
            .description("This is the minimum value that should be set for the Data Size backpressure setting on connections. "
                    + "This can be used to prevent a user from setting a value of 0 which disables backpressure based on size.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();

    public static final PropertyDescriptor SIZE_MAX = new PropertyDescriptor.Builder()
            .name("Maximum Backpressure Data Size Threshold")
            .description("This is the maximum value that should be set for the Data Size backpressure setting on connections. "
                    + "This can be used to prevent a user from setting a very high value that may be filling up the content repo.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 GB")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            COUNT_MIN,
            COUNT_MAX,
            SIZE_MIN,
            SIZE_MAX
    );

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final long minCount = validationContext.getProperty(COUNT_MIN).asLong();
        final long maxCount = validationContext.getProperty(COUNT_MAX).asLong();
        final double minSize = validationContext.getProperty(SIZE_MIN).asDataSize(DataUnit.B);
        final double maxSize = validationContext.getProperty(SIZE_MAX).asDataSize(DataUnit.B);

        if (minCount > maxCount) {
            results.add(
                    new ValidationResult.Builder()
                    .subject(COUNT_MIN.getName())
                    .valid(false)
                    .explanation("Value of '" + COUNT_MIN.getName() + "' cannot be strictly greater than '" + COUNT_MAX.getName() + "'")
                    .build());
        }
        if (Double.compare(minSize, maxSize) > 0) {
            results.add(
                    new ValidationResult.Builder()
                    .subject(SIZE_MIN.getName())
                    .valid(false)
                    .explanation("Value of '" + SIZE_MIN.getName() + "' cannot be strictly greater than '" + SIZE_MAX.getName() + "'")
                    .build());
        }

        return results;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup pg, FlowAnalysisRuleContext context) {
        final Collection<GroupAnalysisResult> results = new HashSet<>();

        final long minCount = context.getProperty(COUNT_MIN).asLong();
        final long maxCount = context.getProperty(COUNT_MAX).asLong();
        final double minSize = context.getProperty(SIZE_MIN).asDataSize(DataUnit.B);
        final double maxSize = context.getProperty(SIZE_MAX).asDataSize(DataUnit.B);

        // Map of all id/components to generate more human readable violations
        final Map<String, VersionedComponent> idComponent = Stream.of(
                pg.getFunnels().stream(),
                pg.getProcessors().stream(),
                pg.getInputPorts().stream(),
                pg.getOutputPorts().stream())
                .flatMap(c -> c)
                .collect(Collectors.toMap(c -> c.getIdentifier(), Function.identity()));

        pg.getConnections().stream().forEach(
                connection -> {
                    if (connection.getBackPressureObjectThreshold() < minCount) {
                        results.add(buildViolation(connection,
                                idComponent.get(connection.getSource().getId()),
                                idComponent.get(connection.getDestination().getId()),
                                BackpressureViolationType.BP_COUNT_THRESHOLD_BELOW_LIMIT,
                                getViolationMessage(BackpressureViolationType.BP_COUNT_THRESHOLD_BELOW_LIMIT, connection.getBackPressureObjectThreshold().toString(), Long.toString(minCount))));
                    }
                    if (connection.getBackPressureObjectThreshold() > maxCount) {
                        results.add(buildViolation(connection,
                                idComponent.get(connection.getSource().getId()),
                                idComponent.get(connection.getDestination().getId()),
                                BackpressureViolationType.BP_COUNT_THRESHOLD_ABOVE_LIMIT,
                                getViolationMessage(BackpressureViolationType.BP_COUNT_THRESHOLD_ABOVE_LIMIT, connection.getBackPressureObjectThreshold().toString(), Long.toString(maxCount))));
                    }
                    final double sizeThreshold = DataUnit.parseDataSize(connection.getBackPressureDataSizeThreshold(), DataUnit.B);
                    if (Double.compare(sizeThreshold, minSize) < 0) {
                        results.add(buildViolation(connection,
                                idComponent.get(connection.getSource().getId()),
                                idComponent.get(connection.getDestination().getId()),
                                BackpressureViolationType.BP_SIZE_THRESHOLD_BELOW_LIMIT,
                                getViolationMessage(BackpressureViolationType.BP_SIZE_THRESHOLD_BELOW_LIMIT, connection.getBackPressureDataSizeThreshold(), context.getProperty(SIZE_MIN).getValue())));
                    }
                    if (Double.compare(sizeThreshold, maxSize) > 0) {
                        results.add(buildViolation(connection,
                                idComponent.get(connection.getSource().getId()),
                                idComponent.get(connection.getDestination().getId()),
                                BackpressureViolationType.BP_SIZE_THRESHOLD_ABOVE_LIMIT,
                                getViolationMessage(BackpressureViolationType.BP_SIZE_THRESHOLD_ABOVE_LIMIT, connection.getBackPressureDataSizeThreshold(), context.getProperty(SIZE_MAX).getValue())));
                    }
                }
            );

        return results;
    }

    private GroupAnalysisResult buildViolation(final VersionedConnection connection, final VersionedComponent source,
            final VersionedComponent destination, final BackpressureViolationType backpressureViolationType, final String violationMessage) {
        // The reason why we want the violation to be on the processor when we have one
        // (either as source or destination) is because in case the rule is "enforced"
        // we want the corresponding component to be invalid. If this is not a processor
        // (funnel, process group, etc) we cannot make it invalid so we put the
        // violation on the connection itself.
        if (!(source instanceof VersionedProcessor) && !(destination instanceof VersionedProcessor)) {
            // connection between two components that are not processors and cannot be invalid, setting violation on connection
            return GroupAnalysisResult.forComponent(connection,
                    connection.getIdentifier() + "_" + backpressureViolationType.getId(),
                    getLocationMessage(connection, source, destination) + violationMessage).build();
        } else if (source instanceof VersionedProcessor) {
            // defining violation on source processor
            return GroupAnalysisResult.forComponent(source,
                    connection.getIdentifier() + "_" + backpressureViolationType.getId(),
                    getLocationMessage(connection, source, destination) + violationMessage).build();
        } else {
            // defining violation on destination processor
            return GroupAnalysisResult.forComponent(destination,
                    connection.getIdentifier() + "_" + backpressureViolationType.getId(),
                    getLocationMessage(connection, source, destination) + violationMessage).build();
        }
    }

    private String getLocationMessage(final VersionedConnection connection, final VersionedComponent source, final VersionedComponent destination) {
        if (source == null || destination == null) {
            return "The connection [" + connection.getIdentifier() + "] is violating the rule for backpressure settings. ";
        }
        return "The connection [" + connection.getIdentifier() + "] connecting " + source.getName() + " [" + source.getIdentifier() + "] to "
                + destination.getName() + " [" + destination.getIdentifier() + "] is violating the rule for backpressure settings. ";
    }

    private String getViolationMessage(final BackpressureViolationType backpressureViolationType, final String configured, final String limit) {
        return switch (backpressureViolationType) {
        case BP_COUNT_THRESHOLD_ABOVE_LIMIT -> "The connection is configured with a Backpressure Count Threshold of " + configured + " and it should be lesser or equal than " + limit + ".";
        case BP_COUNT_THRESHOLD_BELOW_LIMIT -> "The connection is configured with a Backpressure Count Threshold of " + configured + " and it should be greater or equal than " + limit + ".";
        case BP_SIZE_THRESHOLD_ABOVE_LIMIT -> "The connection is configured with a Backpressure Data Size Threshold of " + configured + " and it should be lesser or equal than " + limit + ".";
        case BP_SIZE_THRESHOLD_BELOW_LIMIT -> "The connection is configured with a Backpressure Data Size Threshold of " + configured + " and it should be greater or equal than " + limit + ".";
        };
    }

    private enum BackpressureViolationType {

        BP_COUNT_THRESHOLD_BELOW_LIMIT("BackpressureCountThresholdTooLow"),
        BP_COUNT_THRESHOLD_ABOVE_LIMIT("BackpressureCountThresholdTooHigh"),
        BP_SIZE_THRESHOLD_BELOW_LIMIT("BackpressureSizeThresholdTooLow"),
        BP_SIZE_THRESHOLD_ABOVE_LIMIT("BackpressureSizeThresholdTooHigh");

        private final String id;

        BackpressureViolationType(String id) {
            this.id = id;
        }

        public String getId() {
            return this.id;
        }

    }
}
