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
package org.apache.nifi.processors.aws.kinesis.stream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean2;
import org.apache.commons.beanutils.FluentPropertyBeanIntrospector;
import org.apache.commons.beanutils.PropertyUtilsBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.kinesis.stream.record.AbstractKinesisRecordProcessor;
import org.apache.nifi.processors.aws.kinesis.stream.record.KinesisRecordProcessorRaw;
import org.apache.nifi.processors.aws.kinesis.stream.record.KinesisRecordProcessorRecord;
import org.apache.nifi.processors.aws.v2.AbstractAwsAsyncProcessor;
import org.apache.nifi.processors.aws.v2.AbstractAwsProcessor;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordFieldType;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClientBuilder;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.coordinator.WorkerStateChangeListener;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@Tags({"amazon", "aws", "kinesis", "consume", "stream"})
@CapabilityDescription("Reads data from the specified AWS Kinesis stream and outputs a FlowFile for every processed Record (raw) " +
        " or a FlowFile for a batch of processed records if a Record Reader and Record Writer are configured. " +
        "At-least-once delivery of all Kinesis Records within the Stream while the processor is running. " +
        "AWS Kinesis Client Library can take several seconds to initialise before starting to fetch data. " +
        "Uses DynamoDB for check pointing and CloudWatch (optional) for metrics. " +
        "Ensure that the credentials provided have access to DynamoDB and CloudWatch (optional) along with Kinesis.")
@WritesAttributes({
        @WritesAttribute(attribute = AbstractKinesisRecordProcessor.AWS_KINESIS_PARTITION_KEY,
                description = "Partition key of the (last) Kinesis Record read from the Shard"),
        @WritesAttribute(attribute = AbstractKinesisRecordProcessor.AWS_KINESIS_SHARD_ID,
                description = "Shard ID from which the Kinesis Record was read"),
        @WritesAttribute(attribute = AbstractKinesisRecordProcessor.AWS_KINESIS_SEQUENCE_NUMBER,
                description = "The unique identifier of the (last) Kinesis Record within its Shard"),
        @WritesAttribute(attribute = AbstractKinesisRecordProcessor.AWS_KINESIS_APPROXIMATE_ARRIVAL_TIMESTAMP,
                description = "Approximate arrival timestamp of the (last) Kinesis Record read from the stream"),
        @WritesAttribute(attribute = "mime.type",
                description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer (if configured)"),
        @WritesAttribute(attribute = "record.count",
                description = "Number of records written to the FlowFiles by the Record Writer (if configured)"),
        @WritesAttribute(attribute = "record.error.message",
                description = "This attribute provides on failure the error message encountered by the Record Reader or Record Writer (if configured)")
})
@DynamicProperties({
        @DynamicProperty(name = "Kinesis Client Library (KCL) Configuration property name",
                description = "Override default KCL Configuration ConfigsBuilder properties with required values. Supports setting of values directly on " +
                        "the ConfigsBuilder, such as 'namespace', as well as properties on nested builders. For example, to set configsBuilder.retrievalConfig().maxListShardsRetryAttempts(value), " +
                        "name the property as 'retrievalConfig.maxListShardsRetryAttempts'. Only supports setting of simple property values, e.g. String, " +
                        "int, long and boolean. Does not allow override of KCL Configuration settings handled by non-dynamic processor properties.",
                expressionLanguageScope = ExpressionLanguageScope.NONE, value = "Value to set in the KCL Configuration property")
})
@SystemResourceConsideration(resource = SystemResource.CPU, description = "Kinesis Client Library is used to create a Worker thread for consumption of Kinesis Records. " +
        "The Worker is initialised and started when this Processor has been triggered. It runs continually, spawning Kinesis Record Processors as required " +
        "to fetch Kinesis Records. The Worker Thread (and any child Record Processor threads) are not controlled by the normal NiFi scheduler as part of the " +
        "Concurrent Thread pool and are not released until this processor is stopped.")
@SystemResourceConsideration(resource = SystemResource.NETWORK, description = "Kinesis Client Library will continually poll for new Records, " +
        "requesting up to a maximum number of Records/bytes per call. This can result in sustained network usage.")
@SeeAlso(PutKinesisStream.class)
public class ConsumeKinesisStream extends AbstractAwsAsyncProcessor<KinesisAsyncClient, KinesisAsyncClientBuilder> {

    private static final String CHECKPOINT_CONFIG = "checkpointConfig";
    private static final String COORDINATOR_CONFIG = "coordinatorConfig";
    private static final String LEASE_MANAGEMENT_CONFIG = "leaseManagementConfig";
    private static final String LIFECYCLE_CONFIG = "lifecycleConfig";
    private static final String METRICS_CONFIG = "metricsConfig";
    private static final String PROCESSOR_CONFIG = "processorConfig";
    private static final String RETRIEVAL_CONFIG = "retrievalConfig";

    static final AllowableValue TRIM_HORIZON = new AllowableValue(
            InitialPositionInStream.TRIM_HORIZON.toString(),
            InitialPositionInStream.TRIM_HORIZON.toString(),
            "Start reading at the last untrimmed record in the shard in the system, which is the oldest data record in the shard."
    );
    static final AllowableValue LATEST = new AllowableValue(
            InitialPositionInStream.LATEST.toString(),
            InitialPositionInStream.LATEST.toString(),
            "Start reading just after the most recent record in the shard, so that you always read the most recent data in the shard."
    );
    static final AllowableValue AT_TIMESTAMP = new AllowableValue(
            InitialPositionInStream.AT_TIMESTAMP.toString(),
            InitialPositionInStream.AT_TIMESTAMP.toString(), "Start reading from the position denoted by a specific time stamp, provided in the value Timestamp."
    );

    static final PropertyDescriptor KINESIS_STREAM_NAME = new PropertyDescriptor.Builder()
            .name("kinesis-stream-name")
            .displayName("Amazon Kinesis Stream Name")
            .description("The name of Kinesis Stream")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor APPLICATION_NAME = new PropertyDescriptor.Builder()
            .displayName("Application Name")
            .name("amazon-kinesis-stream-application-name")
            .description("The Kinesis stream reader application name.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true).build();

    public static final PropertyDescriptor INITIAL_STREAM_POSITION = new PropertyDescriptor.Builder()
            .displayName("Initial Stream Position")
            .name("amazon-kinesis-stream-initial-position")
            .description("Initial position to read Kinesis streams.")
            .allowableValues(LATEST, TRIM_HORIZON, AT_TIMESTAMP)
            .defaultValue(LATEST.getValue())
            .required(true).build();

    public static final PropertyDescriptor STREAM_POSITION_TIMESTAMP = new PropertyDescriptor.Builder()
            .displayName("Stream Position Timestamp")
            .name("amazon-kinesis-stream-position-timestamp")
            .description("Timestamp position in stream from which to start reading Kinesis Records. " +
                    "Required if " + INITIAL_STREAM_POSITION.getDescription() + " is " + AT_TIMESTAMP.getDisplayName() + ". " +
                    "Uses the Timestamp Format to parse value into a Date.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR) // customValidate checks the value against TIMESTAMP_FORMAT
            .dependsOn(INITIAL_STREAM_POSITION, AT_TIMESTAMP)
            .required(false).build();

    public static final PropertyDescriptor TIMESTAMP_FORMAT = new PropertyDescriptor.Builder()
            .displayName("Timestamp Format")
            .name("amazon-kinesis-stream-timestamp-format")
            .description("Format to use for parsing the " + STREAM_POSITION_TIMESTAMP.getDisplayName() + " into a Date " +
                    "and converting the Kinesis Record's Approximate Arrival Timestamp into a FlowFile attribute.")
            .addValidator((subject, input, context) -> {
                if (StringUtils.isNotBlank(input)) {
                    try {
                        DateTimeFormatter.ofPattern(input);
                    } catch (Exception e) {
                        return new ValidationResult.Builder().valid(false).subject(subject).input(input)
                                .explanation("Must be a valid java.time.DateTimeFormatter pattern, e.g. " + RecordFieldType.TIMESTAMP.getDefaultFormat())
                                .build();
                    }
                }
                return new ValidationResult.Builder().valid(true).subject(subject).build();
            })
            .defaultValue(RecordFieldType.TIMESTAMP.getDefaultFormat())
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true).build();

    public static final PropertyDescriptor FAILOVER_TIMEOUT = new PropertyDescriptor.Builder()
            .displayName("Failover Timeout")
            .name("amazon-kinesis-stream-failover-timeout")
            .description("Kinesis Client Library failover timeout")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 secs")
            .required(true).build();

    public static final PropertyDescriptor GRACEFUL_SHUTDOWN_TIMEOUT = new PropertyDescriptor.Builder()
            .displayName("Graceful Shutdown Timeout")
            .name("amazon-kinesis-stream-graceful-shutdown-timeout")
            .description("Kinesis Client Library graceful shutdown timeout")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("20 secs")
            .required(true).build();

    public static final PropertyDescriptor CHECKPOINT_INTERVAL = new PropertyDescriptor.Builder()
            .displayName("Checkpoint Interval")
            .name("amazon-kinesis-stream-checkpoint-interval")
            .description("Interval between Kinesis checkpoints")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("3 secs")
            .required(true).build();

    public static final PropertyDescriptor NUM_RETRIES = new PropertyDescriptor.Builder()
            .displayName("Retry Count")
            .name("amazon-kinesis-stream-retry-count")
            .description("Number of times to retry a Kinesis operation (process record, checkpoint, shutdown)")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("10")
            .required(true).build();

    public static final PropertyDescriptor RETRY_WAIT = new PropertyDescriptor.Builder()
            .displayName("Retry Wait")
            .name("amazon-kinesis-stream-retry-wait")
            .description("Interval between Kinesis operation retries (process record, checkpoint, shutdown)")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 sec")
            .required(true).build();

    public static final PropertyDescriptor DYNAMODB_ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .displayName("DynamoDB Override")
            .name("amazon-kinesis-stream-dynamodb-override")
            .description("DynamoDB override to use non-AWS deployments")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false).build();

    public static final PropertyDescriptor REPORT_CLOUDWATCH_METRICS = new PropertyDescriptor.Builder()
            .displayName("Report Metrics to CloudWatch")
            .name("amazon-kinesis-stream-cloudwatch-flag")
            .description("Whether to report Kinesis usage metrics to CloudWatch.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true).build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("amazon-kinesis-stream-record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for reading received messages." +
                    " The Kinesis Stream name can be referred to by Expression Language '${" +
                    AbstractKinesisRecordProcessor.KINESIS_RECORD_SCHEMA_KEY + "}' to access a schema." +
                    " If Record Reader/Writer are not specified, each Kinesis Record will create a FlowFile.")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("amazon-kinesis-stream-record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use for serializing Records to an output FlowFile." +
                    " The Kinesis Stream name can be referred to by Expression Language '${" +
                    AbstractKinesisRecordProcessor.KINESIS_RECORD_SCHEMA_KEY + "}' to access a schema." +
                    " If Record Reader/Writer are not specified, each Kinesis Record will create a FlowFile.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .build();

    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractAwsProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE)
            .required(true)
            .build();

    public static final Relationship REL_PARSE_FAILURE = new Relationship.Builder()
            .name("parse.failure")
            .description("If a message from Kinesis cannot be parsed using the configured Record Reader" +
                    " or failed to be written by the configured Record Writer," +
                    " the contents of the message will be routed to this Relationship as its own individual FlowFile.")
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            // Kinesis Stream specific properties
            KINESIS_STREAM_NAME,
            APPLICATION_NAME,
            RECORD_READER,
            RECORD_WRITER,
            REGION,
            ENDPOINT_OVERRIDE,
            DYNAMODB_ENDPOINT_OVERRIDE,
            INITIAL_STREAM_POSITION,
            STREAM_POSITION_TIMESTAMP,
            TIMESTAMP_FORMAT,
            FAILOVER_TIMEOUT,
            GRACEFUL_SHUTDOWN_TIMEOUT,
            CHECKPOINT_INTERVAL,
            NUM_RETRIES,
            RETRY_WAIT,
            REPORT_CLOUDWATCH_METRICS,

            // generic AWS processor properties
            TIMEOUT,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            PROXY_CONFIGURATION_SERVICE
    );

    private static final Map<String, PropertyDescriptor> DISALLOWED_DYNAMIC_KCL_PROPERTIES = Map.of(
            "leaseManagementConfig.initialPositionInStream", INITIAL_STREAM_POSITION,
            "leaseManagementConfig.failoverTimeMillis", FAILOVER_TIMEOUT
    );

    private static final Object WORKER_LOCK = new Object();
    private static final String SCHEDULER_THREAD_NAME_TEMPLATE = ConsumeKinesisStream.class.getSimpleName() + "-" + Scheduler.class.getSimpleName() + "-";

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private static final Set<Relationship> RECORD_RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_PARSE_FAILURE
    );

    private static final PropertyUtilsBean PROPERTY_UTILS_BEAN;
    private static final BeanUtilsBean BEAN_UTILS_BEAN;

    static {
        PROPERTY_UTILS_BEAN = new PropertyUtilsBean();
        PROPERTY_UTILS_BEAN.addBeanIntrospector(new FluentPropertyBeanIntrospector(""));

        final ConvertUtilsBean2 convertUtilsBean2 = new ConvertUtilsBean2() {
            @SuppressWarnings("unchecked") // generic Enum conversion from String property values
            @Override
            public Object convert(final String value, final Class clazz) {
                if (clazz.isEnum()) {
                    return Enum.valueOf(clazz, value);
                } else {
                    return super.convert(value, clazz);
                }
            }
        };

        BEAN_UTILS_BEAN = new BeanUtilsBean(convertUtilsBean2, PROPERTY_UTILS_BEAN);
    }

    private volatile boolean isRecordReaderSet;
    private volatile boolean isRecordWriterSet;

    private volatile Scheduler scheduler;
    final AtomicReference<WorkerStateChangeListener.WorkerState> workerState = new AtomicReference<>(null);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return isRecordReaderSet && isRecordWriterSet ? RECORD_RELATIONSHIPS : RELATIONSHIPS;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (RECORD_READER.equals(descriptor)) {
            isRecordReaderSet = StringUtils.isNotEmpty(newValue);
        } else if (RECORD_WRITER.equals(descriptor)) {
            isRecordWriterSet = StringUtils.isNotEmpty(newValue);
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .addValidator(this::validateDynamicKCLConfigProperty)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE);

        return builder.build();
    }

    @Override
    @OnStopped
    public void onStopped() {
        super.onStopped();
    }

    private ValidationResult validateDynamicKCLConfigProperty(final String subject, final String input, final ValidationContext context) {
        final ValidationResult.Builder validationResult = new ValidationResult.Builder().subject(subject).input(input);

        if (!subject.matches("^(?!with)[a-zA-Z]\\w*(\\.[a-zA-Z]\\w*)?$")) {
            return validationResult
                    .explanation("Property name must not have a prefix of \"with\", must start with a letter and contain only letters, numbers, periods, or underscores")
                    .valid(false).build();
        }

        if (DISALLOWED_DYNAMIC_KCL_PROPERTIES.keySet().stream().anyMatch(k -> k.equalsIgnoreCase(subject))) {
            return validationResult
                    .explanation(String.format("Use \"%s\" instead of a dynamic property", DISALLOWED_DYNAMIC_KCL_PROPERTIES.get(subject).getDisplayName()))
                    .valid(false).build();
        }

        final Region region = Region.of(context.getProperty(REGION).getValue());
        // This is a temporary builder that is not used outside of validation
        final ConfigsBuilder configsBuilderTemp = new ConfigsBuilder(
                getStreamName(context),
                getApplicationName(context),
                KinesisAsyncClient.builder().region(region).build(),
                DynamoDbAsyncClient.builder().region(region).build(),
                CloudWatchAsyncClient.builder().region(region).build(),
                UUID.randomUUID().toString(),
                () -> null
        );
        try {
            if (subject.contains(".")) {
                final String[] beanParts = subject.split("\\.");
                if (beanParts.length != 2) {
                    throw new IllegalArgumentException("Kinesis Client Configuration Builder properties only support one level of nesting");
                }
                final String configurationMethod = beanParts[0];
                final String setterMethod = StringUtils.uncapitalize(beanParts[1]);
                final Object configurationObject = configsBuilderTemp.getClass().getMethod(configurationMethod).invoke(configsBuilderTemp);
                if (!PROPERTY_UTILS_BEAN.isWriteable(configurationObject, setterMethod)) {
                    return validationResult
                            .explanation(String.format("Kinesis Client Configuration Builder property with name %s does not exist or is not writable", StringUtils.capitalize(subject)))
                            .valid(false).build();
                }
                BEAN_UTILS_BEAN.setProperty(configurationObject, setterMethod, input);
            } else {
                final String propName = StringUtils.uncapitalize(subject);
                if (!PROPERTY_UTILS_BEAN.isWriteable(configsBuilderTemp, propName)) {
                    return validationResult
                            .explanation(String.format("Kinesis Client Configuration Builder property with name %s does not exist or is not writable", StringUtils.capitalize(subject)))
                            .valid(false).build();
                }
                BEAN_UTILS_BEAN.setProperty(configsBuilderTemp, propName, input);
            }
        } catch (final IllegalAccessException | NoSuchMethodException e) {
            return validationResult
                    .explanation(String.format("Kinesis Client Configuration Builder property with name %s is not accessible", StringUtils.capitalize(subject)))
                    .valid(false).build();
        } catch (final InvocationTargetException e) {
            return buildDynamicPropertyBeanValidationResult(validationResult, subject, input, e.getTargetException().getLocalizedMessage());
        } catch (final IllegalArgumentException e) {
            return buildDynamicPropertyBeanValidationResult(validationResult, subject, input, e.getLocalizedMessage());
        }

        return validationResult.valid(true).build();
    }

    private ValidationResult buildDynamicPropertyBeanValidationResult(final ValidationResult.Builder validationResult,
                                                                      final String subject, final String input, final String message) {
        return validationResult
                .input(input)
                .subject(subject)
                .explanation("Kinesis Client Configuration Builder property with name %s cannot be used with value \"%s\" : %s".formatted(StringUtils.capitalize(subject), input, message))
                .valid(false)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final Set<ValidationResult> validationResults = new HashSet<>(super.customValidate(validationContext));

        DateTimeFormatter dateTimeFormatter = null;
        try {
            dateTimeFormatter = getDateTimeFormatter(validationContext);
        } catch (IllegalArgumentException iae) {
            validationResults.add(new ValidationResult.Builder().valid(false)
                    .subject(TIMESTAMP_FORMAT.getName())
                    .explanation(String.format("%s must be a valid java.time.DateTimeFormatter format", TIMESTAMP_FORMAT.getDisplayName()))
                    .build()
            );
        }

        if (InitialPositionInStream.AT_TIMESTAMP == getInitialPositionInStream(validationContext)) {
            if (!validationContext.getProperty(STREAM_POSITION_TIMESTAMP).isSet()) {
                validationResults.add(new ValidationResult.Builder().valid(false)
                        .subject(STREAM_POSITION_TIMESTAMP.getName())
                        .explanation(String.format("%s must be provided when %s is %s", STREAM_POSITION_TIMESTAMP.getDisplayName(),
                                INITIAL_STREAM_POSITION.getDisplayName(), AT_TIMESTAMP.getDisplayName()))
                        .build()
                );
            } else if (dateTimeFormatter != null) {
                try {
                    // check the streamTimestamp can be formatted
                    getStartStreamTimestamp(validationContext, dateTimeFormatter);
                } catch (Exception e) {
                    validationResults.add(new ValidationResult.Builder().valid(false)
                            .subject(STREAM_POSITION_TIMESTAMP.getName())
                            .explanation(String.format("%s must be parsable by %s", STREAM_POSITION_TIMESTAMP.getDisplayName(),
                                    TIMESTAMP_FORMAT.getDisplayName()))
                            .build());
                }
            }
        }

        if (isRecordReaderSet && !isRecordWriterSet) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(RECORD_WRITER.getName())
                    .explanation(String.format("%s must be set if %s is set in order to write FlowFiles as Records.",
                            RECORD_WRITER.getDisplayName(), RECORD_READER.getDisplayName()))
                    .valid(false)
                    .build());
        } else if (isRecordWriterSet && !isRecordReaderSet) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(RECORD_READER.getName())
                    .explanation(String.format("%s must be set if %s is set in order to write FlowFiles as Records.",
                            RECORD_READER.getDisplayName(), RECORD_WRITER.getDisplayName()))
                    .valid(false)
                    .build());
        }

        return validationResults;
    }

    @OnScheduled
    @Override
    public void onScheduled(ProcessContext context) {
        stopped.set(false);
        workerState.set(null);
        super.onScheduled(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        if (scheduler == null) {
            synchronized (WORKER_LOCK) {
                if (scheduler == null) {
                    final String schedulerId = generateSchedulerId();
                    getLogger().info("Starting Kinesis Scheduler {}", schedulerId);
                    // create scheduler (WorkerState will be CREATED)
                    scheduler = prepareScheduler(context, sessionFactory, schedulerId);
                    // initialise and start Scheduler (will set WorkerState to INITIALIZING and attempt to start)
                    new Thread(scheduler, SCHEDULER_THREAD_NAME_TEMPLATE + schedulerId).start();
                }
            }
        } else {
            // after a Scheduler is registered successfully, nothing has to be done at onTrigger
            // new sessions are created when new messages are consumed by the Scheduler
            // and if the WorkerState is unexpectedly SHUT_DOWN, then we don't want to immediately re-enter onTrigger
            context.yield();

            if (!stopped.get() && WorkerStateChangeListener.WorkerState.SHUT_DOWN == workerState.get()) {
                throw new ProcessException("Scheduler has shutdown unexpectedly, possibly due to a configuration issue; check logs for details");
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // intentionally blank (using onTrigger with ProcessSessionFactory above instead)
    }

    @OnStopped
    public void stopConsuming(final ProcessContext context) {
        if (scheduler != null) {
            synchronized (WORKER_LOCK) {
                if (scheduler != null) {
                    // indicate whether the processor has been Stopped; the Worker can be marked as SHUT_DOWN but still be waiting
                    // for ShardConsumers/RecordProcessors to complete, etc.
                    stopped.set(true);

                    final boolean success = shutdownWorker(context);
                    scheduler = null;
                    workerState.set(null);

                    if (!success) {
                        getLogger().warn("One or more problems while shutting down Kinesis Worker, see logs for details");
                    }
                }
            }
        }
    }

    @VisibleForTesting
    synchronized Scheduler prepareScheduler(final ProcessContext context, final ProcessSessionFactory sessionFactory, final String schedulerId) {
        final KinesisAsyncClient kinesisClient = getClient(context);
        final ConfigsBuilder configsBuilder = prepareConfigsBuilder(context, schedulerId, sessionFactory);
        final MetricsConfig metricsConfig = configsBuilder.metricsConfig();
        if (!isReportCloudWatchMetrics(context)) {
            metricsConfig.metricsFactory(new NullMetricsFactory());
        }

        final String streamName = getStreamName(context);
        final LeaseManagementConfig leaseManagementConfig = configsBuilder.leaseManagementConfig()
                .failoverTimeMillis(getFailoverTimeMillis(context))
                .streamName(streamName);
        final CoordinatorConfig coordinatorConfig = configsBuilder.coordinatorConfig().workerStateChangeListener(workerState::set);

        final List<PropertyDescriptor> dynamicProperties = context.getProperties()
                .keySet()
                .stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toList());


        final RetrievalConfig retrievalConfig = configsBuilder.retrievalConfig()
                .retrievalSpecificConfig(new PollingConfig(streamName, kinesisClient));

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(CHECKPOINT_CONFIG, configsBuilder.checkpointConfig());
        configMap.put(COORDINATOR_CONFIG, coordinatorConfig);
        configMap.put(LEASE_MANAGEMENT_CONFIG, leaseManagementConfig);
        configMap.put(LIFECYCLE_CONFIG, configsBuilder.lifecycleConfig());
        configMap.put(METRICS_CONFIG, metricsConfig);
        configMap.put(PROCESSOR_CONFIG, configsBuilder.processorConfig());
        configMap.put(RETRIEVAL_CONFIG, retrievalConfig);

        dynamicProperties.forEach(descriptor -> {
            final String name = descriptor.getName();
            final String value = context.getProperty(descriptor).getValue();
            try {
                if (name.contains(".")) {
                    final String[] beanParts = name.split("\\.");
                    if (beanParts.length != 2) {
                        throw new IllegalArgumentException("Kinesis Client Configuration Builder properties only support one level of nesting");
                    }
                    final String configurationMethod = beanParts[0];
                    final String setterMethod = beanParts[1];
                    final Object configurationObject = configMap.get(configurationMethod);
                    if (configurationObject == null) {
                        throw new IllegalArgumentException("Kinesis Client Configuration Builder does not have a configuration method named " + configurationMethod);
                    }

                    BEAN_UTILS_BEAN.setProperty(configurationObject, StringUtils.uncapitalize(setterMethod), value);
                } else {
                    BEAN_UTILS_BEAN.setProperty(configsBuilder, StringUtils.uncapitalize(name), value);
                }
            } catch (final IllegalAccessException | InvocationTargetException e) {
                throw new ProcessException(String.format("Unable to set Kinesis Client Configuration Builder property %s with value %s", StringUtils.capitalize(name), value), e);
            }
        });

        getLogger().info("Kinesis Scheduler prepared for application {} to process stream {} as scheduler ID {}...",
                getApplicationName(context), streamName, schedulerId);

        return new Scheduler(
                (CheckpointConfig) configMap.get(CHECKPOINT_CONFIG),
                (CoordinatorConfig) configMap.get(COORDINATOR_CONFIG),
                (LeaseManagementConfig) configMap.get(LEASE_MANAGEMENT_CONFIG),
                (LifecycleConfig) configMap.get(LIFECYCLE_CONFIG),
                (MetricsConfig) configMap.get(METRICS_CONFIG),
                (ProcessorConfig) configMap.get(PROCESSOR_CONFIG),
                (RetrievalConfig) configMap.get(RETRIEVAL_CONFIG)
        );
    }

    private ShardRecordProcessorFactory prepareRecordProcessorFactory(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        return () -> {
            if (isRecordReaderSet && isRecordWriterSet) {
                return new KinesisRecordProcessorRecord(
                        sessionFactory, getLogger(), getStreamName(context), getEndpointPrefix(context),
                        getKinesisEndpoint(context).orElse(null), getCheckpointIntervalMillis(context),
                        getRetryWaitMillis(context), getNumRetries(context), getDateTimeFormatter(context),
                        getReaderFactory(context), getWriterFactory(context)
                );
            } else {
                return new KinesisRecordProcessorRaw(
                        sessionFactory, getLogger(), getStreamName(context), getEndpointPrefix(context),
                        getKinesisEndpoint(context).orElse(null), getCheckpointIntervalMillis(context),
                        getRetryWaitMillis(context), getNumRetries(context), getDateTimeFormatter(context)
                );
            }
        };
    }

    private String getEndpointPrefix(final ProcessContext context) {
        return "kinesis." + getRegion(context).id().toLowerCase();
    }

    /*
     *  Developer note: if setting KCL configuration explicitly from processor properties, be sure to add them to the
     *  DISALLOWED_DYNAMIC_KCL_PROPERTIES list so Dynamic Properties can't be used to override the static properties
     */
    @VisibleForTesting
    ConfigsBuilder prepareConfigsBuilder(final ProcessContext context, final String workerId, final ProcessSessionFactory sessionFactory) {
        final InitialPositionInStream initialPositionInStream = getInitialPositionInStream(context);
        final InitialPositionInStreamExtended initialPositionInStreamValue = (InitialPositionInStream.AT_TIMESTAMP == initialPositionInStream)
                ? InitialPositionInStreamExtended.newInitialPositionAtTimestamp(getStartStreamTimestamp(context))
                : InitialPositionInStreamExtended.newInitialPosition(initialPositionInStream);

        return new ConfigsBuilder(
                new SingleStreamTracker(getStreamName(context), initialPositionInStreamValue),
                getApplicationName(context),
                getClient(context),
                getDynamoClient(context),
                getCloudwatchClient(context),
                workerId,
                prepareRecordProcessorFactory(context, sessionFactory)
        );
    }

    private CloudWatchAsyncClient getCloudwatchClient(final ProcessContext context) {
        final CloudWatchAsyncClientBuilder builder = CloudWatchAsyncClient.builder();
        configureClientBuilder(builder, getRegion(context), context, null);
        return builder.build();
    }

    private DynamoDbAsyncClient getDynamoClient(final ProcessContext context) {
        final DynamoDbAsyncClientBuilder dynamoClientBuilder = DynamoDbAsyncClient.builder();
        configureClientBuilder(dynamoClientBuilder, getRegion(context), context, DYNAMODB_ENDPOINT_OVERRIDE);
        return dynamoClientBuilder.build();
    }

    private boolean shutdownWorker(final ProcessContext context) {
        boolean success = true;
        try {
            if (!scheduler.hasGracefulShutdownStarted()) {
                getLogger().info("Requesting Kinesis Worker shutdown");
                final Future<Boolean> shutdown = scheduler.startGracefulShutdown();
                // allow 2 seconds longer than the graceful period for shutdown before cancelling the task
                if (Boolean.FALSE.equals(shutdown.get(getGracefulShutdownMillis(context) + 2_000L, TimeUnit.MILLISECONDS))) {
                    getLogger().warn("Kinesis Worker shutdown did not complete in time, cancelling");
                    success = false;
                } else {
                    getLogger().info("Kinesis Worker shutdown");
                }
            }
        } catch (final InterruptedException | TimeoutException | ExecutionException e) {
            getLogger().warn("Problem while shutting down Kinesis Worker: {}", e.getLocalizedMessage(), e);
            success = false;
        }
        return success;
    }

    private String generateSchedulerId() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            throw new ProcessException(e);
        }
    }

    private String getApplicationName(final PropertyContext context) {
        return StringUtils.trimToEmpty(context.getProperty(APPLICATION_NAME).getValue());
    }

    private String getStreamName(final PropertyContext context) {
        return StringUtils.trimToEmpty(context.getProperty(KINESIS_STREAM_NAME).getValue());
    }

    private long getFailoverTimeMillis(final PropertyContext context) {
        return context.getProperty(FAILOVER_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
    }

    private long getGracefulShutdownMillis(final PropertyContext context) {
        return context.getProperty(GRACEFUL_SHUTDOWN_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
    }

    private long getCheckpointIntervalMillis(final PropertyContext context) {
        return context.getProperty(CHECKPOINT_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
    }

    private int getNumRetries(final PropertyContext context) {
        return context.getProperty(NUM_RETRIES).asInteger();
    }

    private long getRetryWaitMillis(final PropertyContext context) {
        return context.getProperty(RETRY_WAIT).asTimePeriod(TimeUnit.MILLISECONDS);
    }

    private boolean isReportCloudWatchMetrics(final PropertyContext context) {
        return context.getProperty(REPORT_CLOUDWATCH_METRICS).asBoolean();
    }

    private Optional<String> getKinesisEndpoint(final PropertyContext context) {
        return context.getProperty(ENDPOINT_OVERRIDE).isSet()
                ? Optional.of(StringUtils.trimToEmpty(context.getProperty(ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue()))
                : Optional.empty();
    }

    private RecordReaderFactory getReaderFactory(final PropertyContext context) {
        return context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
    }

    private RecordSetWriterFactory getWriterFactory(final PropertyContext context) {
        return context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
    }

    private InitialPositionInStream getInitialPositionInStream(final PropertyContext context) {
        return InitialPositionInStream.valueOf(StringUtils.trimToEmpty(context.getProperty(INITIAL_STREAM_POSITION).getValue()));
    }

    private DateTimeFormatter getDateTimeFormatter(final PropertyContext context) {
        return DateTimeFormatter.ofPattern(context.getProperty(TIMESTAMP_FORMAT).evaluateAttributeExpressions().getValue());
    }

    private Date getStartStreamTimestamp(final PropertyContext context) {
        return getStartStreamTimestamp(context, getDateTimeFormatter(context));
    }

    private Date getStartStreamTimestamp(final PropertyContext context, final DateTimeFormatter dateTimeFormatter) {
        final String streamTimestamp = context.getProperty(STREAM_POSITION_TIMESTAMP).getValue();
        return new Date(
                LocalDateTime.parse(streamTimestamp, dateTimeFormatter).atZone(ZoneId.systemDefault()) // parse date/time with system timezone
                .withZoneSameInstant(ZoneOffset.UTC) // convert to UTC
                .toInstant().toEpochMilli() // convert to epoch milliseconds for creating Date
        );
    }

    @Override
    protected KinesisAsyncClientBuilder createClientBuilder(final ProcessContext context) {
        return KinesisAsyncClient.builder();
    }
}
