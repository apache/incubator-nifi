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

package org.apache.nifi.processor.util.list;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 * An Abstract Processor that is intended to simplify the coding required in order to perform Listing operations of remote or local resources.
 * Those resources may be files, "objects", "messages", or any other sort of entity that may need to be listed in such a way that
 * we identity the entity only once. Each of these objects, messages, etc. is referred to as an "entity" for the scope of this Processor.
 * </p>
 * <p>
 * This class is responsible for triggering the listing to occur, filtering the results returned such that only new (unlisted) entities
 * or entities that have been modified will be emitted from the Processor.
 * </p>
 * <p>
 * In order to make use of this abstract class, the entities listed must meet the following criteria:
 * </p>
 * <ul>
 * <li>
 * Entity must have a timestamp associated with it. This timestamp is used to determine if entities are "new" or not. Any entity that is
 * returned by the listing will be considered "new" if the timestamp is later than the latest timestamp pulled.
 * </li>
 * <li>
 * If the timestamp of an entity is before OR equal to the latest timestamp pulled, then the entity is not considered new. If the timestamp is later
 * than the last timestamp pulled, then the entity is considered new.
 * </li>
 * <li>
 * With 'Tracking Entities' strategy, the size of entity content is also used to determine if an entity is "new". If the size changes the entity is considered "new".
 * </li>
 * <li>
 * Entity must have a user-readable name that can be used for logging purposes.
 * </li>
 * </ul>
 * <p>
 * This class persists state across restarts so that even if NiFi is restarted, duplicates will not be pulled from the target system given the above criteria. This is
 * performed using the {@link StateManager}. This allows the system to be restarted and begin processing where it left off. The state that is stored is the latest timestamp
 * that has been pulled (as determined by the timestamps of the entities that are returned). See the section above for information about how this information isused in order to
 * determine new entities.
 * </p>
 * <p>
 * NOTE: This processor performs migrations of legacy state mechanisms inclusive of locally stored, file-based state and the optional utilization of the <code>Distributed Cache
 * Service</code> property to the new {@link StateManager} functionality. Upon successful migration, the associated data from one or both of the legacy mechanisms is purged.
 * </p>
 *
 * <p>
 * For each new entity that is listed, the Processor will send a FlowFile to the 'success' relationship. The FlowFile will have no content but will have some set
 * of attributes (defined by the concrete implementation) that can be used to fetch those resources or interact with them in whatever way makes sense for
 * the configured dataflow.
 * </p>
 * <p>
 * Subclasses are responsible for the following:
 * </p>
 * <ul>
 * <li>
 * Perform a listing of resources. The subclass will implement the {@link #performListing(ProcessContext, Long, ListingMode)} method, which creates a listing of all
 * entities on the target system that have timestamps later than the provided timestamp. If the entities returned have a timestamp before the provided one, those
 * entities will be filtered out. It is therefore not necessary to perform the filtering of timestamps but is provided in order to give the implementation the ability
 * to filter those resources on the server side rather than pulling back all of the information, if it makes sense to do so in the concrete implementation.
 * </li>
 * <li>
 * Creating a Map of attributes that are applicable for an entity. The attributes that are assigned to each FlowFile are exactly those returned by the
 * {@link #createAttributes(ListableEntity, ProcessContext)}.
 * </li>
 * <li>
 * Returning the configured path. Many resources can be comprised of a "path" (or a "container" or "bucket", etc.) as well as name or identifier that is unique only
 * within that path. The {@link #getPath(ProcessContext)} method is responsible for returning the path that is currently being polled for entities. If this does concept
 * does not apply for the concrete implementation, it is recommended that the concrete implementation return "." or "/" for all invocations of this method.
 * </li>
 * <li>
 * Determining when the listing must be cleared. It is sometimes necessary to clear state about which entities have already been ingested, as the result of a user
 * changing a property value. The {@link #isListingResetNecessary(PropertyDescriptor)} method is responsible for determining when the listing needs to be reset by returning
 * a boolean indicating whether or not a change in the value of the provided property should trigger the timestamp and identifier information to be cleared.
 * </li>
 * <li>
 * Provide the target system timestamp precision. By either letting user to choose the right one by adding TARGET_SYSTEM_TIMESTAMP_PRECISION to the return value of
 * getSupportedPropertyDescriptors method or, overriding getDefaultTimePrecision method in case the target system has a fixed time precision.
 * </li>
 * </ul>
 */
@TriggerSerially
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER}, description = "After a listing of resources is performed, the latest timestamp of any of the resources is stored in the component's state. "
    + "The scope used depends on the implementation.")
public abstract class AbstractListProcessor<T extends ListableEntity> extends AbstractProcessor implements VerifiableProcessor {

    /**
     * Indicates the mode when performing a listing.
     */
    protected enum ListingMode {
        /**
         * Indicates the listing is being performed during normal processor execution.  May use configuration cached in the Processor object.
         */
        EXECUTION,
        /**
         * Indicates the listing is being performed during configuration verification.  Only use configuration provided in the ProcessContext argument, since the configuration may not
         * have been applied to the processor yet.
         */
        CONFIGURATION_VERIFICATION
    }

    private static final Long IGNORE_MIN_TIMESTAMP_VALUE = 0L;

    public static final AllowableValue PRECISION_AUTO_DETECT = new AllowableValue("auto-detect", "Auto Detect",
            "Automatically detect time unit deterministically based on candidate entries timestamp."
                    + " Please note that this option may take longer to list entities unnecessarily, if none of entries has a precise precision timestamp."
                    + " E.g. even if a target system supports millis, if all entries only have timestamps without millis, such as '2017-06-16 09:06:34.000', "
                    + "then its precision is determined as 'seconds'.");
    public static final AllowableValue PRECISION_MILLIS = new AllowableValue("millis", "Milliseconds",
            "This option provides the minimum latency for an entry from being available to being listed if target system supports millis, if not, use other options.");
    public static final AllowableValue PRECISION_SECONDS = new AllowableValue("seconds", "Seconds",
            "For a target system that does not have millis precision, but has in seconds.");
    public static final AllowableValue PRECISION_MINUTES = new AllowableValue("minutes", "Minutes", "For a target system that only supports precision in minutes.");

    public static final PropertyDescriptor TARGET_SYSTEM_TIMESTAMP_PRECISION = new Builder()
            .name("target-system-timestamp-precision")
            .displayName("Target System Timestamp Precision")
            .description("Specify timestamp precision at the target system."
                    + " Since this processor uses timestamp of entities to decide which should be listed, it is crucial to use the right timestamp precision.")
            .required(true)
            .allowableValues(PRECISION_AUTO_DETECT, PRECISION_MILLIS, PRECISION_SECONDS, PRECISION_MINUTES)
            .defaultValue(PRECISION_AUTO_DETECT.getValue())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received are routed to success")
            .build();

    public static final AllowableValue BY_TIMESTAMPS = new AllowableValue("timestamps", "Tracking Timestamps",
            "This strategy tracks the latest timestamp of listed entity to determine new/updated entities." +
                    " Since it only tracks few timestamps, it can manage listing state efficiently." +
                    " However, any newly added, or updated entity having timestamp older than the tracked latest timestamp can not be picked by this strategy." +
                    " For example, such situation can happen in a file system if a file with old timestamp" +
                    " is copied or moved into the target directory without its last modified timestamp being updated." +
                    " Also may miss files when multiple subdirectories are being written at the same time while listing is running.");

    public static final AllowableValue BY_ENTITIES = new AllowableValue("entities", "Tracking Entities",
            "This strategy tracks information of all the listed entities within the latest 'Entity Tracking Time Window' to determine new/updated entities." +
                    " This strategy can pick entities having old timestamp that can be missed with 'Tracking Timestamps'." +
                    " Works even when multiple subdirectories are being written at the same time while listing is running." +
                    " However additional DistributedMapCache controller service is required and more JVM heap memory is used." +
                    " See the description of 'Entity Tracking Time Window' property for further details on how it works.");

    public static final AllowableValue NO_TRACKING = new AllowableValue("none", "No Tracking",
            "This strategy lists an entity without any tracking. The same entity will be listed each time" +
                    " on executing this processor. It is recommended to change the default run schedule value." +
                    " Any property that related to the persisting state will be disregarded.");

    public static final AllowableValue BY_TIME_WINDOW = new AllowableValue("time-window", "Time Window",
            "This strategy uses a sliding time window. The window starts where the previous window ended and ends with the 'current time'." +
                    " One cycle will list files with modification time falling within the time window." +
                    " Works even when multiple subdirectories are being written at the same time while listing is running." +
                    " IMPORTANT: This strategy works properly only if the time on both the system hosting NiFi and the one hosting the files" +
                    " are accurate.");

    public static final PropertyDescriptor LISTING_STRATEGY = new Builder()
            .name("listing-strategy")
            .displayName("Listing Strategy")
            .description("Specify how to determine new/updated entities. See each strategy descriptions for detail.")
            .required(true)
            .allowableValues(BY_TIMESTAMPS, BY_ENTITIES, NO_TRACKING)
            .defaultValue(BY_TIMESTAMPS.getValue())
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Record Writer to use for creating the listing. If not specified, one FlowFile will be created for each entity that is listed. " +
                    "If the Record Writer is specified, all entities will be written to a single FlowFile instead of adding attributes to individual FlowFiles.")
            .required(false)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS
    );

    /**
     * Represents the timestamp of an entity which was the latest one within those listed at the previous cycle.
     * It does not necessary mean it has been processed as well.
     * Whether it was processed or not depends on target system time precision and how old the entity timestamp was.
     */
    private volatile Long lastListedLatestEntryTimestampMillis = null;
    /**
     * Represents the timestamp of an entity which was the latest one
     * within those picked up and written to the output relationship at the previous cycle.
     */
    private volatile Long lastProcessedLatestEntryTimestampMillis = 0L;
    private volatile Long lastRunTimeNanos = 0L;
    private volatile boolean justElectedPrimaryNode = false;
    private volatile boolean resetState = false;
    private volatile boolean resetEntityTrackingState = false;
    private volatile List<String> latestIdentifiersProcessed = new ArrayList<>();
    private volatile ListedEntityTracker<T> listedEntityTracker;

    /*
     * A constant used in determining an internal "yield" of processing files. Given the logic to provide a pause on the newest
     * files according to timestamp, it is ensured that at least the specified millis has been eclipsed to avoid getting scheduled
     * near instantaneously after the prior iteration effectively voiding the built in buffer
     */
    public static final Map<TimeUnit, Long> LISTING_LAG_MILLIS;

    static {
        final Map<TimeUnit, Long> nanos = new HashMap<>();
        nanos.put(TimeUnit.MILLISECONDS, 100L);
        nanos.put(TimeUnit.SECONDS, 1_000L);
        nanos.put(TimeUnit.MINUTES, 60_000L);
        LISTING_LAG_MILLIS = Collections.unmodifiableMap(nanos);
    }

    static final String LATEST_LISTED_ENTRY_TIMESTAMP_KEY = "listing.timestamp";
    static final String LAST_PROCESSED_LATEST_ENTRY_TIMESTAMP_KEY = "processed.timestamp";
    static final String IDENTIFIER_PREFIX = "id";

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.removeProperty("Distributed Cache Service");
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (isConfigurationRestored() && isListingResetNecessary(descriptor)) {
            resetTimeStates(); // clear lastListingTime so that we have to fetch new time
            resetState = true;
            resetEntityTrackingState = true;
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
       return RELATIONSHIPS;
    }

    /**
     * In order to add custom validation at sub-classes, implement {@link #customValidate(ValidationContext, Collection)} method.
     */
    @Override
    protected final Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String listingStrategy = context.getProperty(LISTING_STRATEGY).getValue();
        if (BY_ENTITIES.equals(listingStrategy)) {
            ListedEntityTracker.validateProperties(context, results, getStateScope(context));
        }

        customValidate(context, results);
        return results;
    }

    /**
     * Sub-classes can add custom validation by implementing this method.
     *
     * @param validationContext the validation context
     * @param validationResults add custom validation result to this collection
     */
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> validationResults) {

    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog logger, final Map<String, String> attributes) {

        final List<ConfigVerificationResult> results = new ArrayList<>();

        final String containerName = getListingContainerName(context);
        try {
            final Integer unfilteredListingCount = countUnfilteredListing(context);
            final int matchingCount = performListing(context, IGNORE_MIN_TIMESTAMP_VALUE, ListingMode.CONFIGURATION_VERIFICATION).size();

            final String countExplanation;
            if (unfilteredListingCount == null) {
                if (matchingCount == 0) {
                    countExplanation = "Found no objects matching the filter.";
                } else {
                    final String matchingCountText = matchingCount == 1 ? matchingCount + " object" : matchingCount + " objects";
                    countExplanation = String.format("Found %s matching the filter.", matchingCountText);
                }
            } else if (unfilteredListingCount == 0) {
                countExplanation = "Found no objects.";
            } else {
                final String unfilteredListingCountText = unfilteredListingCount == 1 ? unfilteredListingCount + " object" : unfilteredListingCount + " objects";
                final String unfilteredDemonstrativePronoun = unfilteredListingCount == 1 ? "that" : "those";
                final String matchingCountText = matchingCount == 1 ? matchingCount + " matches" : matchingCount + " match";
                countExplanation = String.format("Found %s.  Of %s, %s the filter.",
                        unfilteredListingCountText, unfilteredDemonstrativePronoun, matchingCountText);
            }

            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Perform Listing")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation(String.format("Successfully listed contents of %s.  %s", containerName, countExplanation))
                    .build());

            logger.info("Successfully verified configuration");
        } catch (final IOException e) {
            logger.warn("Failed to verify configuration. Could not list contents of {}", containerName, e);

            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Perform Listing")
                    .outcome(Outcome.FAILED)
                    .explanation(String.format("Failed to list contents of %s: %s", containerName, e.getMessage()))
                    .build());
        }

        return results;
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange(final PrimaryNodeState newState) {
        justElectedPrimaryNode = (newState == PrimaryNodeState.ELECTED_PRIMARY_NODE);
    }

    @OnScheduled
    public final void updateState(final ProcessContext context) throws IOException {
        final StateMap stateMap = context.getStateManager().getState(getStateScope(context));

        // When scheduled to run, check if the associated timestamp is null, signifying a clearing of state and reset the internal timestamp
        if (lastListedLatestEntryTimestampMillis != null && stateMap.get(LATEST_LISTED_ENTRY_TIMESTAMP_KEY) == null) {
            getLogger().info("Detected that state was cleared for this component.  Resetting internal values.");
            resetTimeStates();
        }

        if (resetState) {
            context.getStateManager().clear(getStateScope(context));
            resetState = false;
        }
    }

    private Map<String, String> createStateMap(final long latestListedEntryTimestampThisCycleMillis,
                                               final long lastProcessedLatestEntryTimestampMillis,
                                               final List<String> processedIdentifiesWithLatestTimestamp) {
        final Map<String, String> updatedState = new HashMap<>(processedIdentifiesWithLatestTimestamp.size() + 2);
        updatedState.put(LATEST_LISTED_ENTRY_TIMESTAMP_KEY, String.valueOf(latestListedEntryTimestampThisCycleMillis));
        updatedState.put(LAST_PROCESSED_LATEST_ENTRY_TIMESTAMP_KEY, String.valueOf(lastProcessedLatestEntryTimestampMillis));
        for (int i = 0; i < processedIdentifiesWithLatestTimestamp.size(); i++) {
            updatedState.put(IDENTIFIER_PREFIX + "." + i, processedIdentifiesWithLatestTimestamp.get(i));
        }

        return updatedState;
    }

    private void persist(final long latestListedEntryTimestampThisCycleMillis,
                         final long lastProcessedLatestEntryTimestampMillis,
                         final List<String> processedIdentifiesWithLatestTimestamp,
                         final ProcessSession session, final Scope scope) throws IOException {
        final Map<String, String> updatedState = createStateMap(latestListedEntryTimestampThisCycleMillis, lastProcessedLatestEntryTimestampMillis, processedIdentifiesWithLatestTimestamp);
        session.setState(updatedState, scope);
    }

    protected String getKey(final String directory) {
        return getIdentifier() + ".lastListingTime." + directory;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final String listingStrategy = context.getProperty(LISTING_STRATEGY).getValue();
        if (BY_TIMESTAMPS.equals(listingStrategy)) {
            listByTrackingTimestamps(context, session);

        } else if (BY_ENTITIES.equals(listingStrategy)) {
            listByTrackingEntities(context, session);

        } else if (NO_TRACKING.equals(listingStrategy)) {
            listByNoTracking(context, session);

        } else if (BY_TIME_WINDOW.equals(listingStrategy)) {
            listByTimeWindow(context, session);

        } else {
            throw new ProcessException("Unknown listing strategy: " + listingStrategy);
        }
    }

    protected long getCurrentTime() {
        return System.currentTimeMillis();
    }

    protected long getCurrentNanoTime() {
        return System.nanoTime();
    }

    public void listByNoTracking(final ProcessContext context, final ProcessSession session) {
        final List<T> entityList;

        try {
            // Remove any previous state from the state manager before use a No Tracking Strategy.
            context.getStateManager().clear(getStateScope(context));

        } catch (final IOException re) {
            getLogger().error("Failed to remove previous state from the State Manager.", re.getMessage(), re);
            context.yield();
            return;
        }

        try {
            // minTimestamp = 0L by default on this strategy to ignore any future
            // comparision in lastModifiedMap to the same entity.
            entityList = performListing(context, IGNORE_MIN_TIMESTAMP_VALUE, ListingMode.EXECUTION);
        } catch (final IOException pe) {
            getLogger().error("Failed to perform listing on remote host due to {}", pe.getMessage(), pe);
            context.yield();
            return;
        }

        if (entityList == null || entityList.isEmpty()) {
            getLogger().debug("No data found: yielding");
            context.yield();
            return;
        }

        final TreeMap<Long, List<T>> orderedEntries = new TreeMap<>();
        for (final T entity : entityList) {
            List<T> entitiesForTimestamp = orderedEntries.computeIfAbsent(entity.getTimestamp(), k -> new ArrayList<>());
            entitiesForTimestamp.add(entity);
        }

        final boolean writerSet = context.getProperty(RECORD_WRITER).isSet();
        if (writerSet) {
            try {
                createRecordsForEntities(context, session, orderedEntries);
            } catch (final IOException | SchemaNotFoundException e) {
                getLogger().error("Failed to write listing to FlowFile", e);
                context.yield();
                return;
            }
        } else {
            createFlowFilesForEntities(context, session, orderedEntries);
        }
    }

    public void listByTimeWindow(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (lastListedLatestEntryTimestampMillis == null || justElectedPrimaryNode) {
            try {
                final StateMap stateMap = context.getStateManager().getState(getStateScope(context));
                Optional.ofNullable(stateMap.get(LATEST_LISTED_ENTRY_TIMESTAMP_KEY))
                        .map(Long::parseLong)
                        .ifPresent(lastTimestamp -> lastListedLatestEntryTimestampMillis = lastTimestamp);

                justElectedPrimaryNode = false;
            } catch (final IOException ioe) {
                getLogger().error("Failed to retrieve timestamp of last listing from the State Manager. Will not perform listing until this is accomplished.");
                context.yield();
                return;
            }
        }

        long lowerBoundInclusiveTimestamp = Optional.ofNullable(lastListedLatestEntryTimestampMillis).orElse(IGNORE_MIN_TIMESTAMP_VALUE);
        long upperBoundExclusiveTimestamp;

        long currentTime = getCurrentTime();

        final TreeMap<Long, List<T>> orderedEntries = new TreeMap<>();
        try {
            List<T> entityList = performListing(context, lowerBoundInclusiveTimestamp, ListingMode.EXECUTION);

            boolean targetSystemHasMilliseconds = false;
            boolean targetSystemHasSeconds = false;
            for (final T entity : entityList) {
                final long entityTimestampMillis = entity.getTimestamp();
                if (!targetSystemHasMilliseconds) {
                    targetSystemHasMilliseconds = entityTimestampMillis % 1000 > 0;
                }
                if (!targetSystemHasSeconds) {
                    targetSystemHasSeconds = entityTimestampMillis % 60_000 > 0;
                }
            }

            // Determine target system time precision.
            String specifiedPrecision = context.getProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION).getValue();
            if (StringUtils.isBlank(specifiedPrecision)) {
                // If TARGET_SYSTEM_TIMESTAMP_PRECISION is not supported by the Processor, then specifiedPrecision can be null, instead of its default value.
                specifiedPrecision = getDefaultTimePrecision();
            }
            final TimeUnit targetSystemTimePrecision
                    = PRECISION_AUTO_DETECT.getValue().equals(specifiedPrecision)
                    ? targetSystemHasMilliseconds ? TimeUnit.MILLISECONDS : targetSystemHasSeconds ? TimeUnit.SECONDS : TimeUnit.MINUTES
                    : PRECISION_MILLIS.getValue().equals(specifiedPrecision) ? TimeUnit.MILLISECONDS
                    : PRECISION_SECONDS.getValue().equals(specifiedPrecision) ? TimeUnit.SECONDS : TimeUnit.MINUTES;
            final Long listingLagMillis = LISTING_LAG_MILLIS.get(targetSystemTimePrecision);

            upperBoundExclusiveTimestamp = currentTime - listingLagMillis;

            if (getLogger().isTraceEnabled()) {
                getLogger().trace("interval: {} - {}", lowerBoundInclusiveTimestamp, upperBoundExclusiveTimestamp);
                getLogger().trace("entityList: {}", entityList.stream().map(entity -> entity.getName() + "_" + entity.getTimestamp()).collect(Collectors.joining(", ")));
            }
            entityList
                    .stream()
                    .filter(entity -> entity.getTimestamp() >= lowerBoundInclusiveTimestamp)
                    .filter(entity -> entity.getTimestamp() < upperBoundExclusiveTimestamp)
                    .forEach(entity -> orderedEntries
                            .computeIfAbsent(entity.getTimestamp(), __ -> new ArrayList<>())
                            .add(entity)
                    );

            if (getLogger().isTraceEnabled()) {
                getLogger().trace("orderedEntries: {}",
                        orderedEntries.values().stream()
                                .flatMap(List::stream)
                                .map(entity -> entity.getName() + "_" + entity.getTimestamp())
                                .collect(Collectors.joining(", "))
                );
            }
        } catch (final IOException e) {
            getLogger().error("Failed to perform listing on remote host due to {}", e.getMessage(), e);
            context.yield();
            return;
        }

        if (orderedEntries.isEmpty()) {
            getLogger().debug("There is no data to list: yielding");
            context.yield();
            return;
        }

        final boolean writerSet = context.getProperty(RECORD_WRITER).isSet();
        if (writerSet) {
            try {
                createRecordsForEntities(context, session, orderedEntries);
            } catch (final IOException | SchemaNotFoundException e) {
                getLogger().error("Failed to write listing to FlowFile", e);
                context.yield();
                return;
            }
        } else {
            createFlowFilesForEntities(context, session, orderedEntries);
        }

        try {
            if (getLogger().isTraceEnabled()) {
                getLogger().info("this.lastListedLatestEntryTimestampMillis = upperBoundExclusiveTimestamp: {} = {}", lastListedLatestEntryTimestampMillis, upperBoundExclusiveTimestamp);
            }
            lastListedLatestEntryTimestampMillis = upperBoundExclusiveTimestamp;
            persist(upperBoundExclusiveTimestamp, upperBoundExclusiveTimestamp, latestIdentifiersProcessed, session, getStateScope(context));
        } catch (final IOException ioe) {
            getLogger().warn("Unable to save state due to {}. If NiFi is restarted before state is saved, or if another node begins executing this Processor, data duplication may occur.", ioe);
        }
    }

    public void listByTrackingTimestamps(final ProcessContext context, final ProcessSession session) throws ProcessException {
        Long minTimestampToListMillis = lastListedLatestEntryTimestampMillis;

        if (lastListedLatestEntryTimestampMillis == null || lastProcessedLatestEntryTimestampMillis == null || justElectedPrimaryNode) {
            try {
                boolean noUpdateRequired = false;
                // Attempt to retrieve state from the state manager if a last listing was not yet established or
                // if just elected the primary node
                final StateMap stateMap = session.getState(getStateScope(context));
                latestIdentifiersProcessed.clear();
                for (final Map.Entry<String, String> state : stateMap.toMap().entrySet()) {
                    final String k = state.getKey();
                    final String v = state.getValue();
                    if (v == null || v.isEmpty()) {
                        continue;
                    }

                    if (LATEST_LISTED_ENTRY_TIMESTAMP_KEY.equals(k)) {
                        minTimestampToListMillis = Long.parseLong(v);
                        // If our determined timestamp is the same as that of our last listing, skip this execution as there are no updates
                        if (minTimestampToListMillis.equals(lastListedLatestEntryTimestampMillis)) {
                            noUpdateRequired = true;
                        } else {
                            lastListedLatestEntryTimestampMillis = minTimestampToListMillis;
                        }
                    } else if (LAST_PROCESSED_LATEST_ENTRY_TIMESTAMP_KEY.equals(k)) {
                        lastProcessedLatestEntryTimestampMillis = Long.parseLong(v);
                    } else if (k.startsWith(IDENTIFIER_PREFIX)) {
                        latestIdentifiersProcessed.add(v);
                    }
                }
                justElectedPrimaryNode = false;
                if (noUpdateRequired) {
                    getLogger().debug("No update required for last listed entity: yielding");
                    context.yield();
                    return;
                }
            } catch (final IOException ioe) {
                getLogger().error("Failed to retrieve timestamp of last listing from the State Manager. Will not perform listing until this is accomplished.");
                context.yield();
                return;
            }
        }

        final List<T> entityList;
        final long currentRunTimeNanos = getCurrentNanoTime();
        final long currentRunTimeMillis = getCurrentTime();
        try {
            // track of when this last executed for consideration of the lag nanos
            entityList = performListing(context, minTimestampToListMillis, ListingMode.EXECUTION);
        } catch (final IOException e) {
            getLogger().error("Failed to perform listing on remote host due to {}", e.getMessage(), e);
            context.yield();
            return;
        }

        if (entityList == null || entityList.isEmpty()) {
            getLogger().debug("No data found matching minimum timestamp [{}]: yielding", minTimestampToListMillis);
            context.yield();
            return;
        }

        Long latestListedEntryTimestampThisCycleMillis = null;
        final TreeMap<Long, List<T>> orderedEntries = new TreeMap<>();

        // Build a sorted map to determine the latest possible entries
        boolean targetSystemHasMilliseconds = false;
        boolean targetSystemHasSeconds = false;
        for (final T entity : entityList) {
            final long entityTimestampMillis = entity.getTimestamp();
            if (!targetSystemHasMilliseconds) {
                targetSystemHasMilliseconds = entityTimestampMillis % 1000 > 0;
            }
            if (!targetSystemHasSeconds) {
                targetSystemHasSeconds = entityTimestampMillis % 60_000 > 0;
            }
            // New entries are all those that occur at or after the associated timestamp
            final boolean newEntry = minTimestampToListMillis == null || entityTimestampMillis >= minTimestampToListMillis && entityTimestampMillis >= lastProcessedLatestEntryTimestampMillis;

            if (newEntry) {
                List<T> entitiesForTimestamp = orderedEntries.computeIfAbsent(entity.getTimestamp(), k -> new ArrayList<>());
                entitiesForTimestamp.add(entity);
            }
        }

        int entitiesListed = 0;

        if (orderedEntries.size() > 0) {
            latestListedEntryTimestampThisCycleMillis = orderedEntries.lastKey();

            // Determine target system time precision.
            String specifiedPrecision = context.getProperty(TARGET_SYSTEM_TIMESTAMP_PRECISION).getValue();
            if (StringUtils.isBlank(specifiedPrecision)) {
                // If TARGET_SYSTEM_TIMESTAMP_PRECISION is not supported by the Processor, then specifiedPrecision can be null, instead of its default value.
                specifiedPrecision = getDefaultTimePrecision();
            }
            final TimeUnit targetSystemTimePrecision
                    = PRECISION_AUTO_DETECT.getValue().equals(specifiedPrecision)
                    ? targetSystemHasMilliseconds ? TimeUnit.MILLISECONDS : targetSystemHasSeconds ? TimeUnit.SECONDS : TimeUnit.MINUTES
                    : PRECISION_MILLIS.getValue().equals(specifiedPrecision) ? TimeUnit.MILLISECONDS
                    : PRECISION_SECONDS.getValue().equals(specifiedPrecision) ? TimeUnit.SECONDS : TimeUnit.MINUTES;
            final Long listingLagMillis = LISTING_LAG_MILLIS.get(targetSystemTimePrecision);

            // If the last listing time is equal to the newest entries previously seen,
            // another iteration has occurred without new files and special handling is needed to avoid starvation
            if (latestListedEntryTimestampThisCycleMillis.equals(lastListedLatestEntryTimestampMillis)) {
                /* We need to wait for another cycle when either:
                 *   - If we have not eclipsed the minimal listing lag needed due to being triggered too soon after the last run
                 *   - The latest listed entity timestamp is equal to the last processed time, meaning we handled those items originally passed over. No need to process it again.
                 */
                final long listingLagNanos = TimeUnit.MILLISECONDS.toNanos(listingLagMillis);
                final boolean minimalListingLagNotPassed = currentRunTimeNanos - lastRunTimeNanos < listingLagNanos;

                if (minimalListingLagNotPassed) {
                    getLogger().debug("Minimal listing lag not passed: yielding");
                    context.yield();
                    return;
                }

                final boolean latestListedEntryIsUpToDate = latestListedEntryTimestampThisCycleMillis.equals(lastProcessedLatestEntryTimestampMillis)
                        && orderedEntries.get(latestListedEntryTimestampThisCycleMillis).stream().allMatch(entity -> latestIdentifiersProcessed.contains(entity.getIdentifier()));

                if (latestListedEntryIsUpToDate) {
                    getLogger().debug("Latest entry already listed with timestamp [{}]: yielding", latestListedEntryTimestampThisCycleMillis);
                    context.yield();
                    return;
                }

            } else {
                // Convert minimum reliable timestamp into target system time unit, in order to truncate unreliable digits.
                final long minimumReliableTimestampInFilesystemTimeUnit = targetSystemTimePrecision.convert(currentRunTimeMillis - listingLagMillis, TimeUnit.MILLISECONDS);
                final long minimumReliableTimestampMillis = targetSystemTimePrecision.toMillis(minimumReliableTimestampInFilesystemTimeUnit);
                // If the latest listed entity is not old enough, compared with the minimum timestamp, then wait for another cycle.
                // The minimum timestamp should be reliable to determine that no further entries will be added with the same timestamp based on the target system time precision.
                if (minimumReliableTimestampMillis < latestListedEntryTimestampThisCycleMillis) {
                    // Otherwise, newest entries are held back one cycle to avoid issues in writes occurring exactly when the listing is being performed to avoid missing data
                    orderedEntries.remove(latestListedEntryTimestampThisCycleMillis);
                }
            }


            final boolean writerSet = context.getProperty(RECORD_WRITER).isSet();
            if (writerSet) {
                try {
                    entitiesListed = createRecordsForEntities(context, session, orderedEntries);
                } catch (final IOException | SchemaNotFoundException e) {
                    getLogger().error("Failed to write listing to FlowFile", e);
                    context.yield();
                    return;
                }
            } else {
                entitiesListed = createFlowFilesForEntities(context, session, orderedEntries);
            }
        }

        // As long as we have a listing timestamp, there is meaningful state to capture regardless of any outputs generated
        if (latestListedEntryTimestampThisCycleMillis != null) {
            final boolean processedNewFiles = entitiesListed > 0;

            if (processedNewFiles) {
                // If there have been files created, update the last timestamp we processed.
                // Retrieving lastKey instead of using latestListedEntryTimestampThisCycleMillis is intentional here,
                // because latestListedEntryTimestampThisCycleMillis might be removed if it's not old enough.
                if (!orderedEntries.lastKey().equals(lastProcessedLatestEntryTimestampMillis)) {
                    // If the latest timestamp at this cycle becomes different than the previous one, we need to clear identifiers.
                    // If it didn't change, we need to add identifiers.
                    latestIdentifiersProcessed.clear();
                }
                // Capture latestIdentifierProcessed.
                latestIdentifiersProcessed.addAll(orderedEntries.lastEntry().getValue().stream().map(T::getIdentifier).collect(Collectors.toList()));
                lastProcessedLatestEntryTimestampMillis = orderedEntries.lastKey();
            }

            if (!latestListedEntryTimestampThisCycleMillis.equals(lastListedLatestEntryTimestampMillis) || processedNewFiles) {
                // We have performed a listing and pushed any FlowFiles out that may have been generated
                // Now, we need to persist state about the Last Modified timestamp of the newest file
                // that we evaluated. We do this in order to avoid pulling in the same file twice.
                // However, we want to save the state both locally and remotely.
                // We store the state remotely so that if a new Primary Node is chosen, it can pick up where the
                // previously Primary Node left off.
                // We also store the state locally so that if the node is restarted, and the node cannot contact
                // the distributed state cache, the node can continue to run (if it is primary node).
                try {
                    lastListedLatestEntryTimestampMillis = latestListedEntryTimestampThisCycleMillis;
                    persist(latestListedEntryTimestampThisCycleMillis, lastProcessedLatestEntryTimestampMillis, latestIdentifiersProcessed, session, getStateScope(context));
                } catch (final IOException ioe) {
                    getLogger().warn("Unable to save state due to {}. If NiFi is restarted before state is saved, or "
                            + "if another node begins executing this Processor, data duplication may occur.", ioe);
                }
            }

            if (processedNewFiles) {
                getLogger().info("Successfully created listing with {} new objects", entitiesListed);
                session.commitAsync();
            }

            lastRunTimeNanos = currentRunTimeNanos;
        } else {
            getLogger().debug("There is no data to list: yielding");
            context.yield();

            // lastListingTime = 0 so that we don't continually poll the distributed cache / local file system
            if (lastListedLatestEntryTimestampMillis == null) {
                lastListedLatestEntryTimestampMillis = 0L;
            }
        }
    }

    private int createRecordsForEntities(final ProcessContext context, final ProcessSession session, final Map<Long, List<T>> orderedEntries) throws IOException, SchemaNotFoundException {
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        int entitiesListed = 0;
        FlowFile flowFile = session.create();
        final WriteResult writeResult;

        final Map<String, String> attributes = new HashMap<>();

        try (final OutputStream out = session.write(flowFile);
            final RecordSetWriter recordSetWriter = writerFactory.createWriter(getLogger(), getRecordSchema(), out, Collections.emptyMap())) {
            attributes.put(CoreAttributes.MIME_TYPE.key(), recordSetWriter.getMimeType());

            recordSetWriter.beginRecordSet();
            for (final Map.Entry<Long, List<T>> timestampEntities : orderedEntries.entrySet()) {
                List<T> entities = timestampEntities.getValue();
                if (timestampEntities.getKey().equals(lastProcessedLatestEntryTimestampMillis)) {
                    // Filter out previously processed entities.
                    entities = entities.stream().filter(entity -> !latestIdentifiersProcessed.contains(entity.getIdentifier())).collect(Collectors.toList());
                }

                for (final T entity : entities) {
                    entitiesListed++;
                    recordSetWriter.write(entity.toRecord());
                }
            }

            writeResult = recordSetWriter.finishRecordSet();
        }

        if (entitiesListed == 0) {
            session.remove(flowFile);
            return 0;
        }

        attributes.putAll(writeResult.getAttributes());
        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
        flowFile = session.putAllAttributes(flowFile, attributes);

        session.transfer(flowFile, REL_SUCCESS);
        return entitiesListed;
    }

    private int createFlowFilesForEntities(final ProcessContext context, final ProcessSession session, final Map<Long, List<T>> orderedEntries) {
        int entitiesListed = 0;
        for (final Map.Entry<Long, List<T>> timestampEntities : orderedEntries.entrySet()) {
            List<T> entities = timestampEntities.getValue();
            if (timestampEntities.getKey().equals(lastProcessedLatestEntryTimestampMillis)) {
                // Filter out previously processed entities.
                entities = entities.stream().filter(entity -> !latestIdentifiersProcessed.contains(entity.getIdentifier())).collect(Collectors.toList());
            }

            for (final T entity : entities) {
                entitiesListed++;

                // Create the FlowFile for this path.
                final Map<String, String> attributes = createAttributes(entity, context);
                FlowFile flowFile = session.create();
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_SUCCESS);
            }
        }

        return entitiesListed;
    }

    /**
     * This method is intended to be overridden by SubClasses those do not support TARGET_SYSTEM_TIMESTAMP_PRECISION property.
     * So that it use return different precisions than PRECISION_AUTO_DETECT.
     * If TARGET_SYSTEM_TIMESTAMP_PRECISION is supported as a valid Processor property,
     * then PRECISION_AUTO_DETECT will be the default value when not specified by a user.
     *
     * @return Default Time Precision
     */
    protected String getDefaultTimePrecision() {
        return TARGET_SYSTEM_TIMESTAMP_PRECISION.getDefaultValue();
    }

    private void resetTimeStates() {
        lastListedLatestEntryTimestampMillis = null;
        lastProcessedLatestEntryTimestampMillis = 0L;
        lastRunTimeNanos = 0L;
        latestIdentifiersProcessed.clear();
    }

    /**
     * Creates a Map of attributes that should be applied to the FlowFile to represent this entity. This processor will emit a FlowFile for each "new" entity
     * (see the documentation for this class for a discussion of how this class determines whether or not an entity is "new"). The FlowFile will contain no
     * content. The attributes that will be included are exactly the attributes that are returned by this method.
     *
     * @param entity  the entity represented by the FlowFile
     * @param context the ProcessContext for obtaining configuration information
     * @return a Map of attributes for this entity
     */
    protected abstract Map<String, String> createAttributes(T entity, ProcessContext context);

    /**
     * Returns the path to perform a listing on.
     * Many resources can be comprised of a "path" (or a "container" or "bucket", etc.) as well as name or identifier that is unique only
     * within that path. This method is responsible for returning the path that is currently being polled for entities. If this concept
     * does not apply for the concrete implementation, it is recommended that the concrete implementation return "." or "/" for all invocations of this method.
     *
     * @param context the ProcessContext to use in order to obtain configuration
     * @return the path that is to be used to perform the listing, or <code>null</code> if not applicable.
     */
    protected abstract String getPath(final ProcessContext context);

    /**
     * Performs a listing of the remote entities that can be pulled. If any entity that is returned has already been "discovered" or "emitted"
     * by this Processor, it will be ignored. A discussion of how the Processor determines those entities that have already been emitted is
     * provided above in the documentation for this class. Any entity that is returned by this method with a timestamp prior to the minTimestamp
     * will be filtered out by the Processor. Therefore, it is not necessary that implementations perform this filtering but can be more efficient
     * if the filtering can be performed on the server side prior to retrieving the information.
     *
     * @param context      the ProcessContext to use in order to pull the appropriate entities
     * @param minTimestamp the minimum timestamp of entities that should be returned
     * @param listingMode  the listing mode, indicating whether the listing is being performed during configuration verification or normal processor execution
     * @return a Listing of entities that have a timestamp >= minTimestamp
     */
    protected abstract List<T> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode)
            throws IOException;

    /**
     * Determines whether or not the listing must be reset if the value of the given property is changed
     *
     * @param property the property that has changed
     * @return <code>true</code> if a change in value of the given property necessitates that the listing be reset, <code>false</code> otherwise.
     */
    protected abstract boolean isListingResetNecessary(final PropertyDescriptor property);

    /**
     * Returns a Scope that specifies where the state should be managed for this Processor
     *
     * @param context the ProcessContext to use in order to make a determination
     * @return a Scope that specifies where the state should be managed for this Processor
     */
    protected abstract Scope getStateScope(final PropertyContext context);

    /**
     * @return the RecordSchema that will be used for any Records that are produced by the Processor
     */
    protected abstract RecordSchema getRecordSchema();

    /**
     * Performs an unfiltered listing and returns the count, or null if this operation is not supported.
     *
     * @param context the ProcessContext to use in order to pull the appropriate entities
     * @return The number of unfiltered entities in the listing, or null if this processor does not support an unfiltered listing
     */
    protected abstract Integer countUnfilteredListing(final ProcessContext context)
            throws IOException;

    /**
     * Provides a human-readable name for the container being listed, for the purpose of displaying readable verification messages during processor configuration verification.
     *
     * @param context The process context
     * @return The user-friendly name for the container
     */
    protected abstract String getListingContainerName(final ProcessContext context);

    @OnScheduled
    public void initListedEntityTracker(ProcessContext context) {
        final boolean isTrackingEntityStrategy = BY_ENTITIES.getValue().equals(context.getProperty(LISTING_STRATEGY).getValue());
        if (listedEntityTracker != null && (resetEntityTrackingState || !isTrackingEntityStrategy)) {
            try {
                listedEntityTracker.clearListedEntities();
            } catch (IOException e) {
                throw new RuntimeException("Failed to reset previously listed entities due to " + e, e);
            }
        }
        resetEntityTrackingState = false;

        if (isTrackingEntityStrategy) {
            if (listedEntityTracker == null) {
                listedEntityTracker = createListedEntityTracker();
            }
        } else {
            listedEntityTracker = null;
        }
    }

    protected ListedEntityTracker<T> createListedEntityTracker() {
        return new ListedEntityTracker<>(getIdentifier(), getLogger(), this::getCurrentTime, getRecordSchema());
    }

    private void listByTrackingEntities(ProcessContext context, ProcessSession session) throws ProcessException {
        listedEntityTracker.trackEntities(context, session, justElectedPrimaryNode, getStateScope(context), minTimestampToList -> {
            try {
                return performListing(context, minTimestampToList, ListingMode.EXECUTION);
            } catch (final IOException e) {
                getLogger().error("Failed to perform listing on remote host due to {}", e.getMessage(), e);
                return Collections.emptyList();
            }
        }, entity -> createAttributes(entity, context));
        justElectedPrimaryNode = false;
    }
}
