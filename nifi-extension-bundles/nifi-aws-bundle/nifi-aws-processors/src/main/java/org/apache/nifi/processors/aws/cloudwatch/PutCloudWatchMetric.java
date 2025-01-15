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
package org.apache.nifi.processors.aws.cloudwatch;

import com.amazonaws.AmazonClientException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.v2.AbstractAwsSyncProcessor;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClientBuilder;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatch.model.StatisticSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Publishes metrics to Amazon CloudWatch. Metric can be either a single value, or a StatisticSet comprised of " +
        "minimum, maximum, sum and sample count.")
@DynamicProperty(name = "Dimension Name", value = "Dimension Value",
        description = "Allows dimension name/value pairs to be added to the metric. AWS supports a maximum of 10 dimensions.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@Tags({"amazon", "aws", "cloudwatch", "metrics", "put", "publish"})
public class PutCloudWatchMetric extends AbstractAwsSyncProcessor<CloudWatchClient, CloudWatchClientBuilder> {

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    public static final Set<String> units = Arrays.stream(StandardUnit.values())
            .map(StandardUnit::toString).collect(Collectors.toSet());

    private static final Validator UNIT_VALIDATOR = (subject, input, context) -> {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
        } else {
            String reason = null;

            if (!units.contains(input)) {
                reason = "not a valid Unit";
            }
            return (new ValidationResult.Builder()).subject(subject).input(input).explanation(reason).valid(reason == null).build();
        }
    };

    private static final Validator DOUBLE_VALIDATOR = (subject, input, context) -> {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return (new ValidationResult.Builder()).subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
        } else {
            String reason = null;

            try {
                Double.parseDouble(input);
            } catch (NumberFormatException e) {
                reason = "not a valid Double";
            }

            return (new ValidationResult.Builder()).subject(subject).input(input).explanation(reason).valid(reason == null).build();
        }
    };

    public static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("Namespace")
            .displayName("Namespace")
            .description("The namespace for the metric data for CloudWatch")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor METRIC_NAME = new PropertyDescriptor.Builder()
            .name("MetricName")
            .displayName("Metric Name")
            .description("The name of the metric")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .build();

    public static final PropertyDescriptor VALUE = new PropertyDescriptor.Builder()
            .name("Value")
            .displayName("Value")
            .description("The value for the metric. Must be a double")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(DOUBLE_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIMESTAMP = new PropertyDescriptor.Builder()
            .name("Timestamp")
            .displayName("Timestamp")
            .description("A point in time expressed as the number of milliseconds since Jan 1, 1970 00:00:00 UTC. If not specified, the default value is set to the time the metric data was received")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor UNIT = new PropertyDescriptor.Builder()
            .name("Unit")
            .displayName("Unit")
            .description("The unit of the metric. (e.g Seconds, Bytes, Megabytes, Percent, Count,  Kilobytes/Second, Terabits/Second, Count/Second) For details see http://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(UNIT_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAXIMUM = new PropertyDescriptor.Builder()
            .name("maximum")
            .displayName("Maximum")
            .description("The maximum value of the sample set. Must be a double")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(DOUBLE_VALIDATOR)
            .build();

    public static final PropertyDescriptor MINIMUM = new PropertyDescriptor.Builder()
            .name("minimum")
            .displayName("Minimum")
            .description("The minimum value of the sample set. Must be a double")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(DOUBLE_VALIDATOR)
            .build();

    public static final PropertyDescriptor SAMPLECOUNT = new PropertyDescriptor.Builder()
            .name("sampleCount")
            .displayName("Sample Count")
            .description("The number of samples used for the statistic set. Must be a double")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(DOUBLE_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUM = new PropertyDescriptor.Builder()
            .name("sum")
            .displayName("Sum")
            .description("The sum of values for the sample set. Must be a double")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(DOUBLE_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        NAMESPACE,
        METRIC_NAME,
        REGION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        VALUE,
        MAXIMUM,
        MINIMUM,
        SAMPLECOUNT,
        SUM,
        TIMESTAMP,
        UNIT,
        TIMEOUT,
        SSL_CONTEXT_SERVICE,
        ENDPOINT_OVERRIDE,
        PROXY_CONFIGURATION_SERVICE);

    private volatile Set<String> dynamicPropertyNames = new HashSet<>();

    @Override
    protected CloudWatchClientBuilder createClientBuilder(final ProcessContext context) {
        return CloudWatchClient.builder();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isDynamic()) {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {           // removing a property
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {    // adding a new property
                newDynamicPropertyNames.add(descriptor.getName());
            }
            this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        final boolean valueSet = validationContext.getProperty(VALUE).isSet();
        final boolean maxSet = validationContext.getProperty(MAXIMUM).isSet();
        final boolean minSet = validationContext.getProperty(MINIMUM).isSet();
        final boolean sampleCountSet = validationContext.getProperty(SAMPLECOUNT).isSet();
        final boolean sumSet = validationContext.getProperty(SUM).isSet();

        final boolean completeStatisticSet = (maxSet && minSet && sampleCountSet && sumSet);
        final boolean anyStatisticSetValue = (maxSet || minSet || sampleCountSet || sumSet);

        if (valueSet && anyStatisticSetValue) {
            problems.add(new ValidationResult.Builder()
                .subject("Metric")
                .valid(false)
                .explanation("Cannot set both Value and StatisticSet(Maximum, Minimum, SampleCount, Sum) properties")
                .build());
        } else if (!valueSet && !completeStatisticSet) {
            problems.add(new ValidationResult.Builder()
                .subject("Metric")
                .valid(false)
                .explanation("Must set either Value or complete StatisticSet(Maximum, Minimum, SampleCount, Sum) properties")
                .build());
        }

        if (dynamicPropertyNames.size() > 10) {
            problems.add(new ValidationResult.Builder()
                .subject("Metric")
                .valid(false)
                .explanation("Cannot set more than 10 dimensions")
                .build());
        }

        return problems;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final MetricDatum.Builder datumBuilder = MetricDatum.builder();

        try {
            datumBuilder.metricName(context.getProperty(METRIC_NAME).evaluateAttributeExpressions(flowFile).getValue());
            final String valueString = context.getProperty(VALUE).evaluateAttributeExpressions(flowFile).getValue();
            if (valueString != null) {
                datumBuilder.value(Double.parseDouble(valueString));
            } else {
                final StatisticSet statisticSet = StatisticSet.builder()
                        .maximum(Double.parseDouble(context.getProperty(MAXIMUM).evaluateAttributeExpressions(flowFile).getValue()))
                        .minimum(Double.parseDouble(context.getProperty(MINIMUM).evaluateAttributeExpressions(flowFile).getValue()))
                        .sampleCount(Double.parseDouble(context.getProperty(SAMPLECOUNT).evaluateAttributeExpressions(flowFile).getValue()))
                        .sum(Double.parseDouble(context.getProperty(SUM).evaluateAttributeExpressions(flowFile).getValue()))
                        .build();

                datumBuilder.statisticValues(statisticSet);
            }

            final String timestamp = context.getProperty(TIMESTAMP).evaluateAttributeExpressions(flowFile).getValue();
            if (timestamp != null) {
                datumBuilder.timestamp(new Date(Long.parseLong(timestamp)).toInstant());
            }

            final String unit = context.getProperty(UNIT).evaluateAttributeExpressions(flowFile).getValue();
            if (unit != null) {
                datumBuilder.unit(unit);
            }

            // add dynamic properties as dimensions
            if (!dynamicPropertyNames.isEmpty()) {
                final List<Dimension> dimensions = new ArrayList<>(dynamicPropertyNames.size());
                for (final String propertyName : dynamicPropertyNames) {
                    final String propertyValue = context.getProperty(propertyName).evaluateAttributeExpressions(flowFile).getValue();
                    if (StringUtils.isNotBlank(propertyValue)) {
                        dimensions.add(Dimension.builder().name(propertyName).value(propertyValue).build());
                    }
                }
                datumBuilder.dimensions(dimensions);
            }

            final PutMetricDataRequest metricDataRequest = PutMetricDataRequest.builder()
                    .namespace(context.getProperty(NAMESPACE).evaluateAttributeExpressions(flowFile).getValue())
                    .metricData(datumBuilder.build())
                    .build();

            putMetricData(context, metricDataRequest);
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().info("Successfully published cloudwatch metric for {}", flowFile);
        } catch (final Exception e) {
            getLogger().error("Failed to publish cloudwatch metric for {} due to {}", flowFile, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    protected PutMetricDataResponse putMetricData(final ProcessContext context, final PutMetricDataRequest metricDataRequest) throws AmazonClientException {
        final CloudWatchClient client = getClient(context);
        final PutMetricDataResponse result = client.putMetricData(metricDataRequest);
        return result;
    }

}
