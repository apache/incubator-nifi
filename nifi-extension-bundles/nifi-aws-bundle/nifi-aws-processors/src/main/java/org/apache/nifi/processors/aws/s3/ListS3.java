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
package org.apache.nifi.processors.aws.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.VersionListing;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.ListableEntityWrapper;
import org.apache.nifi.processor.util.list.ListedEntity;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.FormatUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.aws.util.RegionUtilV1.REGION;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"Amazon", "S3", "AWS", "list"})
@SeeAlso({FetchS3Object.class, PutS3Object.class, DeleteS3Object.class, CopyS3Object.class, GetS3ObjectMetadata.class, TagS3Object.class})
@CapabilityDescription("Retrieves a listing of objects from an S3 bucket. For each object that is listed, creates a FlowFile that represents "
        + "the object so that it can be fetched in conjunction with FetchS3Object. This Processor is designed to run on Primary Node only "
        + "in a cluster. If the primary node changes, the new Primary Node will pick up where the previous node left off without duplicating "
        + "all of the data.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a listing of keys, the timestamp of the newest key is stored, "
        + "along with the keys that share that same timestamp. This allows the Processor to list only keys that have been added or modified after "
        + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary "
        + "Node is selected, the new node can pick up where the previous node left off, without duplicating the data.")
@WritesAttributes({
        @WritesAttribute(attribute = "s3.bucket", description = "The name of the S3 bucket"),
        @WritesAttribute(attribute = "s3.region", description = "The region of the S3 bucket"),
        @WritesAttribute(attribute = "filename", description = "The name of the file"),
        @WritesAttribute(attribute = "s3.etag", description = "The ETag that can be used to see if the file has changed"),
        @WritesAttribute(attribute = "s3.isLatest", description = "A boolean indicating if this is the latest version of the object"),
        @WritesAttribute(attribute = "s3.lastModified", description = "The last modified time in milliseconds since epoch in UTC time"),
        @WritesAttribute(attribute = "s3.length", description = "The size of the object in bytes"),
        @WritesAttribute(attribute = "s3.storeClass", description = "The storage class of the object"),
        @WritesAttribute(attribute = "s3.version", description = "The version of the object, if applicable"),
        @WritesAttribute(attribute = "s3.tag.___", description = "If 'Write Object Tags' is set to 'True', the tags associated to the S3 object that is being listed " +
                "will be written as part of the flowfile attributes"),
        @WritesAttribute(attribute = "s3.user.metadata.___", description = "If 'Write User Metadata' is set to 'True', the user defined metadata associated to the S3 object that is being listed " +
                "will be written as part of the flowfile attributes")})
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListS3 extends AbstractS3Processor implements VerifiableProcessor {

    public static final AllowableValue BY_TIMESTAMPS = new AllowableValue("timestamps", "Tracking Timestamps",
        "This strategy tracks the latest timestamp of listed entity to determine new/updated entities." +
            " Since it only tracks few timestamps, it can manage listing state efficiently." +
            " This strategy will not pick up any newly added or modified entity if their timestamps are older than the tracked latest timestamp." +
            " Also may miss files when multiple subdirectories are being written at the same time while listing is running.");

    public static final AllowableValue BY_ENTITIES = new AllowableValue("entities", "Tracking Entities",
        "This strategy tracks information of all the listed entities within the latest 'Entity Tracking Time Window' to determine new/updated entities." +
            " This strategy can pick entities having old timestamp that can be missed with 'Tracing Timestamps'." +
            " Works even when multiple subdirectories are being written at the same time while listing is running." +
            " However an additional DistributedMapCache controller service is required and more JVM heap memory is used." +
            " For more information on how the 'Entity Tracking Time Window' property works, see the description.");

    public static final AllowableValue NO_TRACKING = new AllowableValue("none", "No Tracking",
            "This strategy lists all entities without any tracking. The same entities will be listed each time" +
                    " this processor is scheduled. It is recommended to change the default run schedule value." +
                    " Any property that relates to the persisting state will be ignored.");

    public static final PropertyDescriptor LISTING_STRATEGY = new Builder()
        .name("listing-strategy")
        .displayName("Listing Strategy")
        .description("Specify how to determine new/updated entities. See each strategy descriptions for detail.")
        .required(true)
        .allowableValues(BY_TIMESTAMPS, BY_ENTITIES, NO_TRACKING)
        .defaultValue(BY_TIMESTAMPS.getValue())
        .build();

    public static final PropertyDescriptor TRACKING_STATE_CACHE = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(ListedEntityTracker.TRACKING_STATE_CACHE)
        .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
        .required(true)
        .build();

    public static final PropertyDescriptor INITIAL_LISTING_TARGET = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(ListedEntityTracker.INITIAL_LISTING_TARGET)
        .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
        .required(true)
        .build();

    public static final PropertyDescriptor TRACKING_TIME_WINDOW = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(ListedEntityTracker.TRACKING_TIME_WINDOW)
        .dependsOn(ListedEntityTracker.TRACKING_STATE_CACHE)
        .required(true)
        .build();

    public static final PropertyDescriptor DELIMITER = new Builder()
            .name("delimiter")
            .displayName("Delimiter")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The string used to delimit directories within the bucket. Please consult the AWS documentation " +
                    "for the correct use of this field.")
            .build();

    public static final PropertyDescriptor PREFIX = new Builder()
            .name("prefix")
            .displayName("Prefix")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .description("The prefix used to filter the object list. Do not begin with a forward slash '/'. In most cases, it should end with a forward slash '/'.")
            .build();

    public static final PropertyDescriptor USE_VERSIONS = new Builder()
            .name("use-versions")
            .displayName("Use Versions")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Specifies whether to use S3 versions, if applicable.  If false, only the latest version of each object will be returned.")
            .build();

    public static final PropertyDescriptor LIST_TYPE = new Builder()
            .name("list-type")
            .displayName("List Type")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .allowableValues(
                    new AllowableValue("1", "List Objects V1"),
                    new AllowableValue("2", "List Objects V2"))
            .defaultValue("1")
            .description("Specifies whether to use the original List Objects or the newer List Objects Version 2 endpoint.")
            .build();

    public static final PropertyDescriptor MIN_AGE = new Builder()
            .name("min-age")
            .displayName("Minimum Object Age")
            .description("The minimum age that an S3 object must be in order to be considered; any object younger than this amount of time (according to last modification date) will be ignored")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final PropertyDescriptor MAX_AGE = new Builder()
            .name("max-age")
            .displayName("Maximum Object Age")
            .description("The maximum age that an S3 object can be in order to be considered; any object older than this amount of time (according to last modification date) will be ignored")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .addValidator(createMaxAgeValidator())
            .build();

    public static final PropertyDescriptor WRITE_OBJECT_TAGS = new Builder()
            .name("write-s3-object-tags")
            .displayName("Write Object Tags")
            .description("If set to 'True', the tags associated with the S3 object will be written as FlowFile attributes")
            .required(true)
            .allowableValues(new AllowableValue("true", "True"), new AllowableValue("false", "False"))
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor REQUESTER_PAYS = new Builder()
            .name("requester-pays")
            .displayName("Requester Pays")
            .required(true)
            .description("If true, indicates that the requester consents to pay any charges associated with listing "
                    + "the S3 bucket.  This sets the 'x-amz-request-payer' header to 'requester'.  Note that this "
                    + "setting is not applicable when 'Use Versions' is 'true'.")
            .addValidator(createRequesterPaysValidator())
            .allowableValues(
                new AllowableValue("true", "True", "Indicates that the requester consents to pay any charges associated with listing the S3 bucket."),
                new AllowableValue("false", "False", "Does not consent to pay requester charges for listing the S3 bucket."))
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor WRITE_USER_METADATA = new Builder()
            .name("write-s3-user-metadata")
            .displayName("Write User Metadata")
            .description("If set to 'True', the user defined metadata associated with the S3 object will be added to FlowFile attributes/records")
            .required(true)
            .allowableValues(new AllowableValue("true", "True"), new AllowableValue("false", "False"))
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Record Writer to use for creating the listing. If not specified, one FlowFile will be created for each entity that is listed. If the Record Writer is specified, " +
            "all entities will be written to a single FlowFile instead of adding attributes to individual FlowFiles.")
        .required(false)
        .identifiesControllerService(RecordSetWriterFactory.class)
        .build();

    static final PropertyDescriptor BATCH_SIZE = new Builder()
        .name("Listing Batch Size")
        .displayName("Listing Batch Size")
        .description("If not using a Record Writer, this property dictates how many S3 objects should be listed in a single batch. Once this number is reached, the FlowFiles that have been created " +
            "will be transferred out of the Processor. Setting this value lower may result in lower latency by sending out the FlowFiles before the complete listing has finished. However, it can " +
            "significantly reduce performance. Larger values may take more memory to store all of the information before sending the FlowFiles out. This property is ignored if using a Record " +
            "Writer, as one of the main benefits of the Record Writer is being able to emit the entire listing as a single FlowFile.")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("100")
        .build();


    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        BUCKET_WITHOUT_DEFAULT_VALUE,
        REGION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        LISTING_STRATEGY,
        TRACKING_STATE_CACHE,
        TRACKING_TIME_WINDOW,
        INITIAL_LISTING_TARGET,
        RECORD_WRITER,
        MIN_AGE,
        MAX_AGE,
        BATCH_SIZE,
        WRITE_OBJECT_TAGS,
        WRITE_USER_METADATA,
        TIMEOUT,
        SSL_CONTEXT_SERVICE,
        ENDPOINT_OVERRIDE,
        SIGNER_OVERRIDE,
        S3_CUSTOM_SIGNER_CLASS_NAME,
        S3_CUSTOM_SIGNER_MODULE_LOCATION,
        PROXY_CONFIGURATION_SERVICE,
        DELIMITER,
        PREFIX,
        USE_VERSIONS,
        LIST_TYPE,
        REQUESTER_PAYS);

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private static final Set<PropertyDescriptor> TRACKING_RESET_PROPERTY_DESCRIPTORS = Set.of(
            BUCKET_WITHOUT_DEFAULT_VALUE,
            REGION,
            PREFIX,
            LISTING_STRATEGY
    );

    public static final String CURRENT_TIMESTAMP = "currentTimestamp";
    public static final String CURRENT_KEY_PREFIX = "key-";

    // used by Tracking Timestamps tracking strategy
    private final AtomicReference<ListingSnapshot> listing = new AtomicReference<>(ListingSnapshot.empty());

    // used by Tracking Entities tracking strategy
    private volatile ListedEntityTracker<ListableEntityWrapper<S3VersionSummary>> listedEntityTracker;

    private volatile boolean justElectedPrimaryNode = false;
    private volatile boolean resetTracking = false;

    private volatile Long minObjectAgeMilliseconds;
    private volatile Long maxObjectAgeMilliseconds;

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange(final PrimaryNodeState newState) {
        justElectedPrimaryNode = (newState == PrimaryNodeState.ELECTED_PRIMARY_NODE);
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (isConfigurationRestored() && TRACKING_RESET_PROPERTY_DESCRIPTORS.contains(descriptor)) {
            resetTracking = true;
        }
    }

    @OnScheduled
    public void initTrackingStrategy(ProcessContext context) throws IOException {
        final String listingStrategy = context.getProperty(LISTING_STRATEGY).getValue();
        final boolean isTrackingTimestampsStrategy = BY_TIMESTAMPS.getValue().equals(listingStrategy);
        final boolean isTrackingEntityStrategy = BY_ENTITIES.getValue().equals(listingStrategy);

        if (resetTracking || !isTrackingTimestampsStrategy) {
            context.getStateManager().clear(Scope.CLUSTER);
            listing.set(ListingSnapshot.empty());
        }

        if (listedEntityTracker != null && (resetTracking || !isTrackingEntityStrategy)) {
            try {
                listedEntityTracker.clearListedEntities();
                listedEntityTracker = null;
            } catch (IOException e) {
                throw new RuntimeException("Failed to reset previously listed entities", e);
            }
        }

        if (isTrackingEntityStrategy && listedEntityTracker == null) {
            listedEntityTracker = createListedEntityTracker();
        }

        resetTracking = false;
    }

    @OnScheduled
    public void initObjectAgeThresholds(ProcessContext context) {
        minObjectAgeMilliseconds = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        maxObjectAgeMilliseconds = context.getProperty(MAX_AGE) != null ? context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS) : null;
    }

    protected ListedEntityTracker<ListableEntityWrapper<S3VersionSummary>> createListedEntityTracker() {
        return new ListedS3VersionSummaryTracker();
    }

    private static Validator createRequesterPaysValidator() {
        return (subject, input, context) -> {
            boolean requesterPays = Boolean.parseBoolean(input);
            boolean useVersions = context.getProperty(USE_VERSIONS).asBoolean();
            boolean valid = !requesterPays || !useVersions;
            return new ValidationResult.Builder()
                    .input(input)
                    .subject(subject)
                    .valid(valid)
                    .explanation(valid ? null : "'Requester Pays' cannot be used when listing object versions.")
                    .build();
        };
    }
    private static Validator createMaxAgeValidator() {
        return (subject, input, context) -> {
            Double  maxAge = input != null ? FormatUtils.getPreciseTimeDuration(input, TimeUnit.MILLISECONDS) : null;
            long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
            boolean valid = input != null && maxAge > minAge;
            return new ValidationResult.Builder()
                    .input(input)
                    .subject(subject)
                    .valid(valid)
                    .explanation(valid ? null : "'Maximum Age' must be greater than 'Minimum Age' ")
                    .build();
        };
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private Set<String> extractKeys(final StateMap stateMap) {
        Set<String> keys = new HashSet<>();
        for (Map.Entry<String, String>  entry : stateMap.toMap().entrySet()) {
            if (entry.getKey().startsWith(CURRENT_KEY_PREFIX)) {
                keys.add(entry.getValue());
            }
        }
        return keys;
    }

    private void restoreState(final ProcessSession session) throws IOException {
        final StateMap stateMap = session.getState(Scope.CLUSTER);
        if (stateMap.getStateVersion().isEmpty() || stateMap.get(CURRENT_TIMESTAMP) == null || stateMap.get(CURRENT_KEY_PREFIX + "0") == null) {
            forcefullyUpdateListing(0L, Collections.emptySet());
        } else {
            final long timestamp = Long.parseLong(stateMap.get(CURRENT_TIMESTAMP));
            final Set<String> keys = extractKeys(stateMap);
            forcefullyUpdateListing(timestamp, keys);
        }
    }

    private void updateListingIfNewer(final long timestamp, final Set<String> keys) {
        final ListingSnapshot updatedListing = new ListingSnapshot(timestamp, keys);
        listing.getAndUpdate(current -> current.getTimestamp() > timestamp ? current : updatedListing);
    }

    private void forcefullyUpdateListing(final long timestamp, final Set<String> keys) {
        final ListingSnapshot updatedListing = new ListingSnapshot(timestamp, keys);
        listing.set(updatedListing);
    }

    private void persistState(final ProcessSession session, final long timestamp, final Collection<String> keys) {
        final Map<String, String> state = new HashMap<>();
        state.put(CURRENT_TIMESTAMP, String.valueOf(timestamp));

        int i = 0;
        for (final String key : keys) {
            state.put(CURRENT_KEY_PREFIX + i, key);
            i++;
        }

        try {
            session.setState(state, Scope.CLUSTER);
        } catch (IOException ioe) {
            getLogger().error("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", ioe);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final String listingStrategy = context.getProperty(LISTING_STRATEGY).getValue();

        if (BY_TIMESTAMPS.equals(listingStrategy)) {
            listByTrackingTimestamps(context, session);
        } else if (BY_ENTITIES.equals(listingStrategy)) {
            listByTrackingEntities(context, session);
        } else if (NO_TRACKING.equals(listingStrategy)) {
            listNoTracking(context, session);
        } else {
            throw new ProcessException("Unknown listing strategy: " + listingStrategy);
        }
    }

    private void listNoTracking(ProcessContext context, ProcessSession session) {
        final AmazonS3 client = getClient(context);

        S3BucketLister bucketLister = getS3BucketLister(context, client);

        final long startNanos = System.nanoTime();
        final long minAgeMilliseconds = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAgeMilliseconds = context.getProperty(MAX_AGE) != null ? context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS) : null;
        final long listingTimestamp = System.currentTimeMillis();

        final String bucket = context.getProperty(BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions().getValue();
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

        int listCount = 0;
        int totalListCount = 0;

        getLogger().trace("Start listing, listingTimestamp={}", listingTimestamp);

        final S3ObjectWriter writer;
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        if (writerFactory == null) {
            writer = new AttributeObjectWriter(session);
        } else {
            writer = new RecordObjectWriter(session, writerFactory, getLogger(), context.getProperty(REGION).getValue());
        }

        try {
            writer.beginListing();

            do {
                VersionListing versionListing = bucketLister.listVersions();
                for (S3VersionSummary versionSummary : versionListing.getVersionSummaries()) {
                    long lastModified = versionSummary.getLastModified().getTime();
                    if ((maxAgeMilliseconds != null && (lastModified < (listingTimestamp - maxAgeMilliseconds)))
                        || lastModified > (listingTimestamp - minAgeMilliseconds)) {
                        continue;
                    }

                    getLogger().trace("Listed key={}, lastModified={}", versionSummary.getKey(), lastModified);

                    GetObjectTaggingResult taggingResult = getTaggingResult(context, client, versionSummary);

                    ObjectMetadata objectMetadata = getObjectMetadata(context, client, versionSummary);

                    // Write the entity to the listing
                    writer.addToListing(versionSummary, taggingResult, objectMetadata, context.getProperty(REGION).getValue());

                    listCount++;
                }
                bucketLister.setNextMarker();

                totalListCount += listCount;

                if (listCount >= batchSize && writer.isCheckpoint()) {
                    getLogger().info("Successfully listed {} new files from S3; routing to success", listCount);
                    session.commitAsync();
                }

                listCount = 0;
            } while (bucketLister.isTruncated());

            writer.finishListing();
        } catch (final Exception e) {
            getLogger().error("Failed to list contents of bucket", e);
            writer.finishListingExceptionally(e);
            session.rollback();
            context.yield();
            return;
        }

        final long listMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully listed S3 bucket {} in {} millis", bucket, listMillis);

        if (totalListCount == 0) {
            getLogger().debug("No new objects in S3 bucket {} to list. Yielding.", bucket);
            context.yield();
        }
    }

    private void listByTrackingTimestamps(ProcessContext context, ProcessSession session) {
        try {
            restoreState(session);
        } catch (IOException ioe) {
            getLogger().error("Failed to restore processor state; yielding", ioe);
            context.yield();
            return;
        }

        final AmazonS3 client = getClient(context);
        final S3BucketLister bucketLister = getS3BucketLister(context, client);
        final String bucket = context.getProperty(BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions().getValue();
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

        final ListingSnapshot currentListing = listing.get();
        final long startNanos = System.nanoTime();
        final long currentTimestamp = System.currentTimeMillis();
        final long listingTimestamp = currentListing.getTimestamp();
        final Set<String> currentKeys = currentListing.getKeys();
        int listCount = 0;
        int totalListCount = 0;
        long latestListedTimestampInThisCycle = listingTimestamp;

        final Set<String> listedKeys = new HashSet<>();
        getLogger().trace("Start listing, listingTimestamp={}, currentTimestamp={}, currentKeys={}", currentTimestamp, listingTimestamp, currentKeys);

        final S3ObjectWriter writer;
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        if (writerFactory == null) {
            writer = new AttributeObjectWriter(session);
        } else {
            writer = new RecordObjectWriter(session, writerFactory, getLogger(), context.getProperty(REGION).getValue());
        }

        try {
            writer.beginListing();

            do {
                final VersionListing versionListing = bucketLister.listVersions();
                for (S3VersionSummary versionSummary : versionListing.getVersionSummaries()) {
                    final long lastModified = versionSummary.getLastModified().getTime();
                    if (lastModified < listingTimestamp
                            || lastModified == listingTimestamp && currentKeys.contains(versionSummary.getKey())
                            || !includeObjectInListing(versionSummary, currentTimestamp)) {
                        continue;
                    }

                    getLogger().trace("Listed key={}, lastModified={}, currentKeys={}", versionSummary.getKey(), lastModified, currentKeys);

                    // Write the entity to the listing
                    final GetObjectTaggingResult taggingResult = getTaggingResult(context, client, versionSummary);
                    final ObjectMetadata objectMetadata = getObjectMetadata(context, client, versionSummary);
                    writer.addToListing(versionSummary, taggingResult, objectMetadata, context.getProperty(REGION).getValue());

                    // Track the latest lastModified timestamp and keys having that timestamp.
                    // NOTE: Amazon S3 lists objects in UTF-8 character encoding in lexicographical order. Not ordered by timestamps.
                    if (lastModified > latestListedTimestampInThisCycle) {
                        latestListedTimestampInThisCycle = lastModified;
                        listedKeys.clear();
                        listedKeys.add(versionSummary.getKey());

                    } else if (lastModified == latestListedTimestampInThisCycle) {
                        listedKeys.add(versionSummary.getKey());
                    }

                    listCount++;
                }

                bucketLister.setNextMarker();
                totalListCount += listCount;

                if (listCount >= batchSize && writer.isCheckpoint()) {
                    getLogger().info("Successfully listed {} new files from S3; routing to success", listCount);
                    session.commitAsync();
                }

                listCount = 0;
            } while (bucketLister.isTruncated());

            writer.finishListing();
        } catch (final Exception e) {
            getLogger().error("Failed to list contents of bucket", e);
            writer.finishListingExceptionally(e);
            session.rollback();
            context.yield();
            return;
        }

        final Set<String> updatedKeys = new HashSet<>();
        if (latestListedTimestampInThisCycle <= listingTimestamp) {
            updatedKeys.addAll(currentKeys);
        }
        updatedKeys.addAll(listedKeys);

        persistState(session, latestListedTimestampInThisCycle, updatedKeys);

        final long latestListed = latestListedTimestampInThisCycle;
        session.commitAsync(() -> {
            updateListingIfNewer(latestListed, updatedKeys);
        });

        final long listMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully listed S3 bucket {} in {} millis", bucket, listMillis);

        if (totalListCount == 0) {
            getLogger().debug("No new objects in S3 bucket {} to list. Yielding.", bucket);
            context.yield();
        }
    }

    private void listByTrackingEntities(ProcessContext context, ProcessSession session) {
        listedEntityTracker.trackEntities(context, session, justElectedPrimaryNode, Scope.CLUSTER, minTimestampToList -> {
            S3BucketLister bucketLister = getS3BucketLister(context, getClient(context));
            final long currentTime = System.currentTimeMillis();

            return bucketLister.listVersions().getVersionSummaries()
                .stream()
                .filter(s3VersionSummary -> s3VersionSummary.getLastModified().getTime() >= minTimestampToList
                        && includeObjectInListing(s3VersionSummary, currentTime))
                .map(s3VersionSummary -> new ListableEntityWrapper<>(
                        s3VersionSummary,
                        S3VersionSummary::getKey,
                        summary -> summary.getKey() + "_" + summary.getVersionId(),
                        summary -> summary.getLastModified().getTime(),
                        S3VersionSummary::getSize
                ))
                .collect(Collectors.toList());
        }, null);

        justElectedPrimaryNode = false;
    }

    private class ListedS3VersionSummaryTracker extends ListedEntityTracker<ListableEntityWrapper<S3VersionSummary>> {
        public ListedS3VersionSummaryTracker() {
            super(getIdentifier(), getLogger(), RecordObjectWriter.RECORD_SCHEMA);
        }

        @Override
        protected void createRecordsForEntities(
            ProcessContext context,
            ProcessSession session,
            List<ListableEntityWrapper<S3VersionSummary>> updatedEntities
        ) {
            publishListing(context, session, updatedEntities);
        }

        @Override
        protected void createFlowFilesForEntities(
            ProcessContext context,
            ProcessSession session,
            List<ListableEntityWrapper<S3VersionSummary>> updatedEntities,
            Function<ListableEntityWrapper<S3VersionSummary>, Map<String, String>> createAttributes
        ) {
            publishListing(context, session, updatedEntities);
        }

        private void publishListing(ProcessContext context, ProcessSession session, List<ListableEntityWrapper<S3VersionSummary>> updatedEntities) {
            final S3ObjectWriter writer;
            final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
            if (writerFactory == null) {
                writer = new AttributeObjectWriter(session);
            } else {
                writer = new RecordObjectWriter(session, writerFactory, getLogger(), context.getProperty(REGION).getValue());
            }

            try {
                writer.beginListing();
                final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

                int listCount = 0;
                for (ListableEntityWrapper<S3VersionSummary> updatedEntity : updatedEntities) {
                    S3VersionSummary s3VersionSummary = updatedEntity.getRawEntity();

                    final AmazonS3Client s3Client = getClient(context);
                    final GetObjectTaggingResult taggingResult = getTaggingResult(context, s3Client, s3VersionSummary);
                    final ObjectMetadata objectMetadata = getObjectMetadata(context, s3Client, s3VersionSummary);
                    writer.addToListing(s3VersionSummary, taggingResult, objectMetadata, context.getProperty(REGION).getValue());

                    listCount++;

                    if (listCount >= batchSize && writer.isCheckpoint()) {
                        getLogger().info("Successfully listed {} new files from S3; routing to success", listCount);
                        session.commitAsync();
                    }

                    final ListedEntity listedEntity = new ListedEntity(updatedEntity.getTimestamp(), updatedEntity.getSize());
                    alreadyListedEntities.put(updatedEntity.getIdentifier(), listedEntity);
                }

                writer.finishListing();
            } catch (final Exception e) {
                getLogger().error("Failed to list contents of bucket", e);
                writer.finishListingExceptionally(e);
                session.rollback();
                context.yield();
            }
        }
    }

    private GetObjectTaggingResult getTaggingResult(ProcessContext context, AmazonS3 client, S3VersionSummary versionSummary) {
        GetObjectTaggingResult taggingResult = null;
        if (context.getProperty(WRITE_OBJECT_TAGS).asBoolean()) {
            try {
                taggingResult = client.getObjectTagging(new GetObjectTaggingRequest(versionSummary.getBucketName(), versionSummary.getKey()));
            } catch (final Exception e) {
                getLogger().warn("Failed to obtain Object Tags for S3 Object {} in bucket {}. Will list S3 Object without the object tags",
                        versionSummary.getKey(), versionSummary.getBucketName(), e);
            }
        }
        return taggingResult;
    }

    private ObjectMetadata getObjectMetadata(ProcessContext context, AmazonS3 client, S3VersionSummary versionSummary) {
        ObjectMetadata objectMetadata = null;
        if (context.getProperty(WRITE_USER_METADATA).asBoolean()) {
            try {
                objectMetadata = client.getObjectMetadata(new GetObjectMetadataRequest(versionSummary.getBucketName(), versionSummary.getKey()));
            } catch (final Exception e) {
                getLogger().warn("Failed to obtain User Metadata for S3 Object {} in bucket {}. Will list S3 Object without the user metadata",
                        versionSummary.getKey(), versionSummary.getBucketName(), e);
            }
        }
        return objectMetadata;
    }

    private S3BucketLister getS3BucketLister(ProcessContext context, AmazonS3 client) {
        final boolean requesterPays = context.getProperty(REQUESTER_PAYS).asBoolean();
        final boolean useVersions = context.getProperty(USE_VERSIONS).asBoolean();

        final String bucket = context.getProperty(BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions().getValue();
        final String delimiter = context.getProperty(DELIMITER).getValue();
        final String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions().getValue();

        final int listType = context.getProperty(LIST_TYPE).asInteger();

        final S3BucketLister bucketLister = useVersions
            ? new S3VersionBucketLister(client)
            : listType == 2
            ? new S3ObjectBucketListerVersion2(client)
            : new S3ObjectBucketLister(client);

        bucketLister.setBucketName(bucket);
        bucketLister.setRequesterPays(requesterPays);

        if (delimiter != null && !delimiter.isEmpty()) {
            bucketLister.setDelimiter(delimiter);
        }
        if (prefix != null && !prefix.isEmpty()) {
            bucketLister.setPrefix(prefix);
        }
        return bucketLister;
    }

    private interface S3BucketLister {
        void setBucketName(String bucketName);
        void setPrefix(String prefix);
        void setDelimiter(String delimiter);
        void setRequesterPays(boolean requesterPays);
        // Versions have a superset of the fields that Objects have, so we'll use
        // them as a common interface
        VersionListing listVersions();
        void setNextMarker();
        boolean isTruncated();
    }

    public class S3ObjectBucketLister implements S3BucketLister {
        private final AmazonS3 client;
        private ListObjectsRequest listObjectsRequest;
        private ObjectListing objectListing;

        public S3ObjectBucketLister(AmazonS3 client) {
            this.client = client;
        }

        @Override
        public void setBucketName(String bucketName) {
            listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
        }

        @Override
        public void setPrefix(String prefix) {
            listObjectsRequest.setPrefix(prefix);
        }

        @Override
        public void setDelimiter(String delimiter) {
            listObjectsRequest.setDelimiter(delimiter);
        }

        @Override
        public void setRequesterPays(boolean requesterPays) {
            listObjectsRequest.setRequesterPays(requesterPays);
        }

        @Override
        public VersionListing listVersions() {
            objectListing = client.listObjects(listObjectsRequest);
            final VersionListing versionListing = new VersionListing();
            versionListing.setVersionSummaries(objectListing.getObjectSummaries().stream()
                    .map(ListS3.this::objectSummaryToVersionSummary)
                    .collect(Collectors.toList()));
            return versionListing;
        }

        @Override
        public void setNextMarker() {
            listObjectsRequest.setMarker(objectListing.getNextMarker());
        }

        @Override
        public boolean isTruncated() {
            return objectListing != null && objectListing.isTruncated();
        }
    }

    public class S3ObjectBucketListerVersion2 implements S3BucketLister {
        private final AmazonS3 client;
        private ListObjectsV2Request listObjectsRequest;
        private ListObjectsV2Result objectListing;

        public S3ObjectBucketListerVersion2(AmazonS3 client) {
            this.client = client;
        }

        @Override
        public void setBucketName(String bucketName) {
            listObjectsRequest = new ListObjectsV2Request().withBucketName(bucketName);
        }

        @Override
        public void setPrefix(String prefix) {
            listObjectsRequest.setPrefix(prefix);
        }

        @Override
        public void setDelimiter(String delimiter) {
            listObjectsRequest.setDelimiter(delimiter);
        }

        @Override
        public void setRequesterPays(boolean requesterPays) {
            listObjectsRequest.setRequesterPays(requesterPays);
        }

        @Override
        public VersionListing listVersions() {
            objectListing = client.listObjectsV2(listObjectsRequest);
            final VersionListing versionListing = new VersionListing();
            versionListing.setVersionSummaries(objectListing.getObjectSummaries().stream()
                    .map(ListS3.this::objectSummaryToVersionSummary)
                    .collect(Collectors.toList()));
            return versionListing;
        }

        @Override
        public void setNextMarker() {
            listObjectsRequest.setContinuationToken(objectListing.getNextContinuationToken());
        }

        @Override
        public boolean isTruncated() {
            return objectListing != null && objectListing.isTruncated();
        }
    }

    public class S3VersionBucketLister implements S3BucketLister {
        private final AmazonS3 client;
        private ListVersionsRequest listVersionsRequest;
        private VersionListing versionListing;

        public S3VersionBucketLister(AmazonS3 client) {
            this.client = client;
        }

        @Override
        public void setBucketName(String bucketName) {
            listVersionsRequest = new ListVersionsRequest().withBucketName(bucketName);
        }

        @Override
        public void setPrefix(String prefix) {
            listVersionsRequest.setPrefix(prefix);
        }

        @Override
        public void setDelimiter(String delimiter) {
            listVersionsRequest.setDelimiter(delimiter);
        }

        @Override
        public void setRequesterPays(boolean requesterPays) {
            // Not supported in versionListing, so this does nothing.
        }

        @Override
        public VersionListing listVersions() {
            versionListing = client.listVersions(listVersionsRequest);
            return versionListing;
        }

        @Override
        public void setNextMarker() {
            listVersionsRequest.setKeyMarker(versionListing.getNextKeyMarker());
            listVersionsRequest.setVersionIdMarker(versionListing.getNextVersionIdMarker());
        }

        @Override
        public boolean isTruncated() {
            return versionListing != null && versionListing.isTruncated();
        }
    }

    interface S3ObjectWriter {
        void beginListing() throws IOException, SchemaNotFoundException;

        void addToListing(S3VersionSummary summary, GetObjectTaggingResult taggingResult, ObjectMetadata objectMetadata, String region) throws IOException;

        void finishListing() throws IOException;

        void finishListingExceptionally(Exception cause);

        boolean isCheckpoint();
    }

    static class RecordObjectWriter implements S3ObjectWriter {
        private static final RecordSchema RECORD_SCHEMA;

        private static final String KEY = "key";
        private static final String BUCKET = "bucket";
        private static final String OWNER = "owner";
        private static final String ETAG = "etag";
        private static final String LAST_MODIFIED = "lastModified";
        private static final String SIZE = "size";
        private static final String STORAGE_CLASS = "storageClass";
        private static final String IS_LATEST = "latest";
        private static final String VERSION_ID = "versionId";
        private static final String TAGS = "tags";
        private static final String USER_METADATA = "userMetadata";

        static {
            final List<RecordField> fields = new ArrayList<>();
            fields.add(new RecordField(KEY, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(BUCKET, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(OWNER, RecordFieldType.STRING.getDataType(), true));
            fields.add(new RecordField(ETAG, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(LAST_MODIFIED, RecordFieldType.TIMESTAMP.getDataType(), false));
            fields.add(new RecordField(SIZE, RecordFieldType.LONG.getDataType(), false));
            fields.add(new RecordField(STORAGE_CLASS, RecordFieldType.STRING.getDataType(), false));
            fields.add(new RecordField(IS_LATEST, RecordFieldType.BOOLEAN.getDataType(), false));
            fields.add(new RecordField(VERSION_ID, RecordFieldType.STRING.getDataType(), true));
            fields.add(new RecordField(TAGS, RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), true));
            fields.add(new RecordField(USER_METADATA, RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), true));

            RECORD_SCHEMA = new SimpleRecordSchema(fields);
        }

        private final ProcessSession session;
        private final RecordSetWriterFactory writerFactory;
        private final ComponentLog logger;
        private final String region;
        private RecordSetWriter recordWriter;
        private FlowFile flowFile;

        public RecordObjectWriter(final ProcessSession session, final RecordSetWriterFactory writerFactory, final ComponentLog logger, final String region) {
            this.session = session;
            this.writerFactory = writerFactory;
            this.logger = logger;
            this.region = region;
        }

        @Override
        public void beginListing() throws IOException, SchemaNotFoundException {
            flowFile = session.create();

            final OutputStream out = session.write(flowFile);
            recordWriter = writerFactory.createWriter(logger, RECORD_SCHEMA, out, flowFile);
            recordWriter.beginRecordSet();
        }

        @Override
        public void addToListing(final S3VersionSummary summary, final GetObjectTaggingResult taggingResult, final ObjectMetadata objectMetadata, String region) throws IOException {
            recordWriter.write(createRecordForListing(summary, taggingResult, objectMetadata));
        }

        @Override
        public void finishListing() throws IOException {
            final WriteResult writeResult = recordWriter.finishRecordSet();
            recordWriter.close();

            if (writeResult.getRecordCount() == 0) {
                session.remove(flowFile);
            } else {
                final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                attributes.put("s3.region", region);
                flowFile = session.putAllAttributes(flowFile, attributes);

                session.transfer(flowFile, REL_SUCCESS);
            }
        }

        @Override
        public void finishListingExceptionally(final Exception cause) {
            try {
                recordWriter.close();
            } catch (IOException e) {
                logger.error("Failed to write listing as Records", e);
            }

            session.remove(flowFile);
        }

        @Override
        public boolean isCheckpoint() {
            return false;
        }

        private Record createRecordForListing(final S3VersionSummary versionSummary, final GetObjectTaggingResult taggingResult, final ObjectMetadata objectMetadata) {
            final Map<String, Object> values = new HashMap<>();
            values.put(KEY, versionSummary.getKey());
            values.put(BUCKET, versionSummary.getBucketName());

            if (versionSummary.getOwner() != null) { // We may not have permission to read the owner
                values.put(OWNER, versionSummary.getOwner().getId());
            }

            values.put(ETAG, versionSummary.getETag());
            values.put(LAST_MODIFIED, new Timestamp(versionSummary.getLastModified().getTime()));
            values.put(SIZE, versionSummary.getSize());
            values.put(STORAGE_CLASS, versionSummary.getStorageClass());
            values.put(IS_LATEST, versionSummary.isLatest());
            final String versionId = versionSummary.getVersionId();
            if (versionId != null && !versionId.equals(Constants.NULL_VERSION_ID)) {
                values.put(VERSION_ID, versionSummary.getVersionId());
            }

            if (taggingResult != null) {
                final Map<String, String> tags = new HashMap<>();
                taggingResult.getTagSet().forEach(tag -> {
                    tags.put(tag.getKey(), tag.getValue());
                });

                values.put(TAGS, tags);
            }

            if (objectMetadata != null) {
                values.put(USER_METADATA, objectMetadata.getUserMetadata());
            }

            return new MapRecord(RECORD_SCHEMA, values);
        }
    }

    static class AttributeObjectWriter implements S3ObjectWriter {
        private final ProcessSession session;

        public AttributeObjectWriter(final ProcessSession session) {
            this.session = session;
        }

        @Override
        public void beginListing() {
        }

        @Override
        public void addToListing(final S3VersionSummary versionSummary, final GetObjectTaggingResult taggingResult, final ObjectMetadata objectMetadata, final String region) {
            // Create the attributes
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), versionSummary.getKey());
            attributes.put("s3.bucket", versionSummary.getBucketName());
            attributes.put("s3.region", region);

            if (versionSummary.getOwner() != null) { // We may not have permission to read the owner
                attributes.put("s3.owner", versionSummary.getOwner().getId());
            }

            attributes.put("s3.etag", versionSummary.getETag());
            attributes.put("s3.lastModified", String.valueOf(versionSummary.getLastModified().getTime()));
            attributes.put("s3.length", String.valueOf(versionSummary.getSize()));
            attributes.put("s3.storeClass", versionSummary.getStorageClass());
            attributes.put("s3.isLatest", String.valueOf(versionSummary.isLatest()));

            if (versionSummary.getVersionId() != null) {
                attributes.put("s3.version", versionSummary.getVersionId());
            }

            if (taggingResult != null) {
                final List<Tag> tags = taggingResult.getTagSet();
                for (final Tag tag : tags) {
                    attributes.put("s3.tag." + tag.getKey(), tag.getValue());
                }
            }

            if (objectMetadata != null) {
                for (Map.Entry<String, String> e : objectMetadata.getUserMetadata().entrySet()) {
                    attributes.put("s3.user.metadata." + e.getKey(), e.getValue());
                }
            }

            // Create the flowfile
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);
        }

        @Override
        public void finishListing() {
        }

        @Override
        public void finishListingExceptionally(final Exception cause) {
        }

        @Override
        public boolean isCheckpoint() {
            return true;
        }
    }

    static class ListingSnapshot {
        private final long timestamp;
        private final Set<String> keys;

        public ListingSnapshot(final long timestamp, final Set<String> keys) {
            this.timestamp = timestamp;
            this.keys = keys;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Set<String> getKeys() {
            return keys;
        }

        public static ListingSnapshot empty() {
            return new ListingSnapshot(0L, Collections.emptySet());
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog logger, final Map<String, String> attributes) {
        final AmazonS3Client client = createClient(context);

        final List<ConfigVerificationResult> results = new ArrayList<>(super.verify(context, logger, attributes));
        final String bucketName = context.getProperty(BUCKET_WITHOUT_DEFAULT_VALUE).evaluateAttributeExpressions(attributes).getValue();

        if (bucketName == null || bucketName.trim().isEmpty()) {
            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Perform Listing")
                .outcome(Outcome.FAILED)
                .explanation("Bucket Name must be specified")
                .build());

            return results;
        }

        // Attempt to perform a listing of objects in the S3 bucket
        try {
            final S3BucketLister bucketLister = getS3BucketLister(context, client);
            int totalItems = 0;
            int totalMatchingItems = 0;
            do {
                final VersionListing versionListing = bucketLister.listVersions();
                final long currentTime = System.currentTimeMillis();
                for (final S3VersionSummary versionSummary : versionListing.getVersionSummaries()) {
                    totalItems++;
                    if (includeObjectInListing(versionSummary, currentTime)) {
                        totalMatchingItems++;
                    }
                }
                bucketLister.setNextMarker();
            } while (bucketLister.isTruncated());

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Perform Listing")
                .outcome(Outcome.SUCCESSFUL)
                .explanation("Successfully listed contents of bucket '" + bucketName + "', finding " + totalItems + " total object(s). "
                        + totalMatchingItems + " objects matched the filter.")
                .build());

            logger.info("Successfully verified configuration");
        } catch (final Exception e) {
            logger.warn("Failed to verify configuration. Could not list contents of bucket '{}'", bucketName, e);

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Perform Listing")
                .outcome(Outcome.FAILED)
                .explanation("Failed to list contents of bucket '" + bucketName + "': " + e.getMessage())
                .build());
        }

        return results;
    }

    /**
     * Return whether to include the entity in the listing, based on the minimum and maximum object age (if configured).
     */
    private boolean includeObjectInListing(final S3VersionSummary versionSummary, final long currentTimeMillis) {
        final long lastModifiedTime = versionSummary.getLastModified().getTime();

        return (minObjectAgeMilliseconds == null || currentTimeMillis >= lastModifiedTime + minObjectAgeMilliseconds)
                && (maxObjectAgeMilliseconds == null || currentTimeMillis <= lastModifiedTime + maxObjectAgeMilliseconds);
    }

    private S3VersionSummary objectSummaryToVersionSummary(final S3ObjectSummary objectSummary) {
        final S3VersionSummary versionSummary = new S3VersionSummary();
        versionSummary.setBucketName(objectSummary.getBucketName());
        versionSummary.setETag(objectSummary.getETag());
        versionSummary.setKey(objectSummary.getKey());
        versionSummary.setLastModified(objectSummary.getLastModified());
        versionSummary.setOwner(objectSummary.getOwner());
        versionSummary.setSize(objectSummary.getSize());
        versionSummary.setStorageClass(objectSummary.getStorageClass());
        versionSummary.setIsLatest(true);
        return versionSummary;
    }

    ListingSnapshot getListingSnapshot() {
        return listing.get();
    }

    ListedEntityTracker<ListableEntityWrapper<S3VersionSummary>> getListedEntityTracker() {
        return listedEntityTracker;
    }

    boolean isResetTracking() {
        return resetTracking;
    }
}
