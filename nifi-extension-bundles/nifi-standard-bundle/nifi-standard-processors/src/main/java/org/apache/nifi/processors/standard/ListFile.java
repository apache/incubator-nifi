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
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.io.File;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileStore;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.apache.nifi.expression.ExpressionLanguageScope.ENVIRONMENT;
import static org.apache.nifi.processor.util.StandardValidators.POSITIVE_INTEGER_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.TIME_PERIOD_VALIDATOR;

@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"file", "get", "list", "ingest", "source", "filesystem"})
@CapabilityDescription("Retrieves a listing of files from the input directory. For each file listed, creates a FlowFile " +
        "that represents the file so that it can be fetched in conjunction with FetchFile. This Processor is designed " +
        "to run on Primary Node only in a cluster when 'Input Directory Location' is set to 'Remote'. If the primary node " +
        "changes, the new Primary Node will pick up where the previous node left off without duplicating all the data. " +
        "When 'Input Directory Location' is 'Local', the 'Execution' mode can be anything, and synchronization won't happen. " +
        "Unlike GetFile, this Processor does not delete any data from the local filesystem.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file that was read from filesystem."),
        @WritesAttribute(attribute = "path", description = "The path is set to the relative path of the file's directory " +
                "on filesystem compared to the Input Directory property. For example, if Input Directory is set to " +
                "/tmp, then files picked up from /tmp will have the path attribute set to \"/\". If the Recurse " +
                "Subdirectories property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path " +
                "attribute will be set to \"abc/1/2/3/\"."),
        @WritesAttribute(attribute = "absolute.path", description = "The absolute.path is set to the absolute path of " +
                "the file's directory on filesystem. For example, if the Input Directory property is set to /tmp, " +
                "then files picked up from /tmp will have the path attribute set to \"/tmp/\". If the Recurse " +
                "Subdirectories property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path " +
                "attribute will be set to \"/tmp/abc/1/2/3/\"."),
        @WritesAttribute(attribute = ListFile.FILE_OWNER_ATTRIBUTE, description = "The user that owns the file in filesystem"),
        @WritesAttribute(attribute = ListFile.FILE_GROUP_ATTRIBUTE, description = "The group that owns the file in filesystem"),
        @WritesAttribute(attribute = ListFile.FILE_SIZE_ATTRIBUTE, description = "The number of bytes in the file in filesystem"),
        @WritesAttribute(attribute = ListFile.FILE_PERMISSIONS_ATTRIBUTE, description = "The permissions for the file in filesystem. This " +
                "is formatted as 3 characters for the owner, 3 for the group, and 3 for other users. For example " +
                "rw-rw-r--"),
        @WritesAttribute(attribute = ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description = "The timestamp of when the file in filesystem was " +
                "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute = ListFile.FILE_LAST_ACCESS_TIME_ATTRIBUTE, description = "The timestamp of when the file in filesystem was " +
                "last accessed as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute = ListFile.FILE_CREATION_TIME_ATTRIBUTE, description = "The timestamp of when the file in filesystem was " +
                "created as 'yyyy-MM-dd'T'HH:mm:ssZ'")
})
@SeeAlso({GetFile.class, PutFile.class, FetchFile.class})
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
    + "This allows the Processor to list only files that have been added or modified after "
    + "this date the next time that the Processor is run. Whether the state is stored with a Local or Cluster scope depends on the value of the "
    + "<Input Directory Location> property.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListFile extends AbstractListProcessor<FileInfo> {
    static final AllowableValue LOCATION_LOCAL = new AllowableValue("Local", "Local", "Input Directory is located on a local disk. State will be stored locally on each node in the cluster.");
    static final AllowableValue LOCATION_REMOTE = new AllowableValue("Remote", "Remote", "Input Directory is located on a remote system. State will be stored across the cluster so that "
        + "the listing can be performed on Primary Node Only and another node can pick up where the last node left off, if the Primary Node changes");

    public static final PropertyDescriptor DIRECTORY = new Builder()
            .name("Input Directory")
            .description("The input directory from which files to pull files")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ENVIRONMENT)
            .build();

    public static final PropertyDescriptor RECURSE = new Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether to list files from subdirectories of the directory")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor DIRECTORY_LOCATION = new Builder()
            .name("Input Directory Location")
            .description("Specifies where the Input Directory is located. This is used to determine whether state should be stored locally or across the cluster.")
            .allowableValues(LOCATION_LOCAL, LOCATION_REMOTE)
            .defaultValue(LOCATION_LOCAL.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor FILE_FILTER = new Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue("[^\\.].*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor PATH_FILTER = new Builder()
            .name("Path Filter")
            .description("When " + RECURSE.getName() + " is true, then only subdirectories whose path matches the given regular expression will be scanned")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_FILE_ATTRIBUTES = new Builder()
        .name("Include File Attributes")
        .description("Whether or not to include information such as the file's Last Modified Time and Owner as FlowFile Attributes. "
            + "Depending on the File System being used, gathering this information can be expensive and as a result should be disabled. This is especially true of remote file shares.")
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();

    public static final PropertyDescriptor MIN_AGE = new Builder()
            .name("Minimum File Age")
            .description("The minimum age that a file must be in order to be pulled; any file younger than this amount of time (according to last modification date) will be ignored")
            .required(true)
            .addValidator(TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final PropertyDescriptor MAX_AGE = new Builder()
            .name("Maximum File Age")
            .description("The maximum age that a file must be in order to be pulled; any file older than this amount of time (according to last modification date) will be ignored")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
            .build();

    public static final PropertyDescriptor MIN_SIZE = new Builder()
            .name("Minimum File Size")
            .description("The minimum size that a file must be in order to be pulled")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("0 B")
            .build();

    public static final PropertyDescriptor MAX_SIZE = new Builder()
            .name("Maximum File Size")
            .description("The maximum size that a file can be in order to be pulled")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor IGNORE_HIDDEN_FILES = new Builder()
            .name("Ignore Hidden Files")
            .description("Indicates whether or not hidden files should be ignored")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor TRACK_PERFORMANCE = new Builder()
        .name("track-performance")
        .displayName("Track Performance")
        .description("Whether or not the Processor should track the performance of disk access operations. If true, all accesses to disk will be recorded, including the file being accessed, the " +
            "information being obtained, and how long it takes. This is then logged periodically at a DEBUG level. While the amount of data will be capped, " +
            "this option may still consume a significant amount of heap (controlled by the 'Maximum Number of Files to Track' property), " +
            "but it can be very useful for troubleshooting purposes if performance is poor is degraded.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    public static final PropertyDescriptor MAX_TRACKED_FILES = new Builder()
        .name("max-performance-metrics")
        .displayName("Maximum Number of Files to Track")
        .description("If the 'Track Performance' property is set to 'true', this property indicates the maximum number of files whose performance metrics should be held onto. A smaller value for " +
            "this property will result in less heap utilization, while a larger value may provide more accurate insights into how the disk access operations are performing")
        .required(true)
        .addValidator(POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .defaultValue("100000")
        .build();

    public static final PropertyDescriptor MAX_DISK_OPERATION_TIME = new Builder()
        .name("max-operation-time")
        .displayName("Max Disk Operation Time")
        .description("The maximum amount of time that any single disk operation is expected to take. If any disk operation takes longer than this amount of time, a warning bulletin will be " +
            "generated for each operation that exceeds this amount of time.")
        .required(false)
        .addValidator(TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ENVIRONMENT)
        .defaultValue("10 secs")
        .build();

    public static final PropertyDescriptor MAX_LISTING_TIME = new Builder()
        .name("max-listing-time")
        .displayName("Max Directory Listing Time")
        .description("The maximum amount of time that listing any single directory is expected to take. If the listing for the directory specified by the 'Input Directory' property, " +
            "or the listing of any subdirectory (if 'Recurse' is set to true) takes longer than this amount of time, a warning bulletin will be generated for each directory listing " +
            "that exceeds this amount of time.")
        .required(false)
        .addValidator(TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ENVIRONMENT)
        .defaultValue("3 mins")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            DIRECTORY,
            LISTING_STRATEGY,
            RECURSE,
            RECORD_WRITER,
            DIRECTORY_LOCATION,
            FILE_FILTER,
            PATH_FILTER,
            INCLUDE_FILE_ATTRIBUTES,
            MIN_AGE,
            MAX_AGE,
            MIN_SIZE,
            MAX_SIZE,
            IGNORE_HIDDEN_FILES,
            TARGET_SYSTEM_TIMESTAMP_PRECISION,
            ListedEntityTracker.TRACKING_STATE_CACHE,
            ListedEntityTracker.TRACKING_TIME_WINDOW,
            ListedEntityTracker.INITIAL_LISTING_TARGET,
            ListedEntityTracker.NODE_IDENTIFIER,
            TRACK_PERFORMANCE,
            MAX_TRACKED_FILES,
            MAX_DISK_OPERATION_TIME,
            MAX_LISTING_TIME
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private volatile ScheduledExecutorService monitoringThreadPool;
    private volatile Future<?> monitoringFuture;

    private volatile boolean includeFileAttributes;
    private volatile PerformanceTracker performanceTracker;
    private volatile long performanceLoggingTimestamp = System.currentTimeMillis();

    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_SIZE_ATTRIBUTE = "file.size";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        monitoringThreadPool = Executors.newScheduledThreadPool(1, r -> {
            final Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("Monitor ListFile Performance [UUID=" + context.getIdentifier() + "]");
            t.setDaemon(true);

            return t;
        });
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        includeFileAttributes = context.getProperty(INCLUDE_FILE_ATTRIBUTES).asBoolean();

        final long maxDiskOperationMillis = context.getProperty(MAX_DISK_OPERATION_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final long maxListingMillis = context.getProperty(MAX_LISTING_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        final boolean trackPerformance = context.getProperty(TRACK_PERFORMANCE).asBoolean();
        if (trackPerformance) {
            final int maxEntries = context.getProperty(MAX_TRACKED_FILES).evaluateAttributeExpressions().asInteger();
            performanceTracker = new RollingMetricPerformanceTracker(getLogger(), maxDiskOperationMillis, maxEntries);
        } else {
            performanceTracker = new UntrackedPerformanceTracker(getLogger(), maxDiskOperationMillis);
        }

        final long millisToKeepStats = TimeUnit.MINUTES.toMillis(15);
        final MonitorActiveTasks monitorTask = new MonitorActiveTasks(performanceTracker, getLogger(), maxDiskOperationMillis, maxListingMillis, millisToKeepStats);
        monitoringFuture = monitoringThreadPool.scheduleAtFixedRate(monitorTask, 15, 15, TimeUnit.SECONDS);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        if (monitoringFuture != null) {
            monitoringFuture.cancel(true);
        }

        final boolean trackPerformance = context.getProperty(TRACK_PERFORMANCE).asBoolean();
        if (trackPerformance) {
            logPerformance();
        }
    }

    protected PerformanceTracker getPerformanceTracker() {
        return performanceTracker;
    }

    public void logPerformance() {
        final ComponentLog logger = getLogger();
        if (!logger.isDebugEnabled()) {
            return;
        }

        final long earliestTimestamp = performanceTracker.getEarliestTimestamp();
        final long millis = System.currentTimeMillis() - earliestTimestamp;
        final long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);

        for (final DiskOperation operation : DiskOperation.values()) {
            final OperationStatistics stats = performanceTracker.getOperationStatistics(operation);

            final StringBuilder sb = new StringBuilder();
            if (stats.getCount() == 0) {
                sb.append("Over the past ").append(seconds).append(" seconds, for Operation '").append(operation).append("' there were no operations performed");
            } else {
                sb.append("Over the past ").append(seconds).append(" seconds, For Operation '").append(operation).append("' there were ")
                    .append(stats.getCount()).append(" operations performed with an average time of ")
                    .append(stats.getAverage()).append(" milliseconds; Standard Deviation = ").append(stats.getStandardDeviation()).append(" millis; Min Time = ")
                    .append(stats.getMin()).append(" millis, Max Time = ").append(stats.getMax()).append(" millis");

                if (logger.isDebugEnabled()) {
                    final Map<String, Long> outliers = stats.getOutliers();

                    sb.append("; ").append(stats.getOutliers().size()).append(" significant outliers: ");
                    sb.append(outliers);
                }
            }

            logger.debug("{}", sb);
        }

        performanceLoggingTimestamp = System.currentTimeMillis();
    }


    @Override
    protected Map<String, String> createAttributes(final FileInfo fileInfo, final ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();

        final String fullPath = fileInfo.getFullPathFileName();
        final File file = new File(fullPath);
        final Path filePath = file.toPath();
        final Path directoryPath = new File(getPath(context)).toPath();

        final Path relativePath = directoryPath.toAbsolutePath().relativize(filePath.getParent());
        String relativePathString = relativePath.toString();
        relativePathString = relativePathString.isEmpty() ? "." + File.separator : relativePathString + File.separator;

        final Path absPath = filePath.toAbsolutePath();
        final String absPathString = absPath.getParent().toString() + File.separator;

        attributes.put(CoreAttributes.PATH.key(), relativePathString);
        attributes.put(CoreAttributes.FILENAME.key(), fileInfo.getFileName());
        attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), absPathString);
        attributes.put(FILE_SIZE_ATTRIBUTE, Long.toString(fileInfo.getSize()));
        attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatDateTime(fileInfo.getLastModifiedTime()));

        if (includeFileAttributes) {
            final TimingInfo timingInfo = performanceTracker.getTimingInfo(relativePath.toString(), file.getName());

            try {
                FileStore store = Files.getFileStore(filePath);

                timingInfo.timeOperation(DiskOperation.RETRIEVE_BASIC_ATTRIBUTES, () -> {
                    if (store.supportsFileAttributeView("basic")) {
                        try {
                            BasicFileAttributeView view = Files.getFileAttributeView(filePath, BasicFileAttributeView.class);
                            BasicFileAttributes attrs = view.readAttributes();
                            attributes.put(FILE_CREATION_TIME_ATTRIBUTE, formatDateTime(attrs.creationTime().toMillis()));
                            attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, formatDateTime(attrs.lastAccessTime().toMillis()));
                        } catch (Exception ignore) {
                        } // allow other attributes if these fail
                    }
                });

                timingInfo.timeOperation(DiskOperation.RETRIEVE_OWNER_ATTRIBUTES, () -> {
                    if (store.supportsFileAttributeView("owner")) {
                        try {
                            FileOwnerAttributeView view = Files.getFileAttributeView(filePath, FileOwnerAttributeView.class);
                            attributes.put(FILE_OWNER_ATTRIBUTE, view.getOwner().getName());
                        } catch (Exception ignore) {
                        } // allow other attributes if these fail
                    }
                });

                timingInfo.timeOperation(DiskOperation.RETRIEVE_POSIX_ATTRIBUTES, () -> {
                    if (store.supportsFileAttributeView("posix")) {
                        try {
                            PosixFileAttributeView view = Files.getFileAttributeView(filePath, PosixFileAttributeView.class);
                            attributes.put(FILE_PERMISSIONS_ATTRIBUTE, PosixFilePermissions.toString(view.readAttributes().permissions()));
                            attributes.put(FILE_GROUP_ATTRIBUTE, view.readAttributes().group().getName());
                        } catch (Exception ignore) {
                        } // allow other attributes if these fail
                    }
                });
            } catch (IOException ioe) {
                // well then this FlowFile gets none of these attributes
                getLogger().warn("Error collecting attributes for file {}", absPathString, ioe);
            }
        }

        return attributes;
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        final String location = context.getProperty(DIRECTORY_LOCATION).getValue();
        if (LOCATION_REMOTE.getValue().equalsIgnoreCase(location)) {
            return Scope.CLUSTER;
        }

        return Scope.LOCAL;
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return FileInfo.getRecordSchema();
    }

    @Override
    protected Integer countUnfilteredListing(final ProcessContext context) throws IOException {
        return performListing(context, 0L, ListingMode.CONFIGURATION_VERIFICATION, false).size();
    }
    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode)
            throws IOException {
        return performListing(context, minTimestamp, listingMode, true);
    }

    private List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode, final boolean applyFilters)
            throws IOException {
        final String evaluatedPath = getPath(context);
        if (evaluatedPath == null || evaluatedPath.isBlank()) {
            throw new IOException("Blank Directory is not valid: review configured expression for Directory property");
        }

        final Path basePath = new File(evaluatedPath).toPath();
        final Boolean recurse = context.getProperty(RECURSE).asBoolean();
        final Map<Path, BasicFileAttributes> lastModifiedMap = new HashMap<>();

        final BiPredicate<Path, BasicFileAttributes> fileFilter;
        final PerformanceTracker performanceTracker;
        if (listingMode == ListingMode.EXECUTION) {
            performanceTracker = this.performanceTracker;
            fileFilter = createFileFilter(context, performanceTracker, applyFilters, basePath);
        } else {
            final long maxDiskOperationMillis = context.getProperty(MAX_DISK_OPERATION_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
            performanceTracker = new UntrackedPerformanceTracker(getLogger(), maxDiskOperationMillis);
            fileFilter = createFileFilter(context, performanceTracker, applyFilters, basePath);
        }

        int maxDepth = recurse ? Integer.MAX_VALUE : 1;

        final BiPredicate<Path, BasicFileAttributes> matcher = new BiPredicate<>() {
            private long lastTimestamp = System.currentTimeMillis();

            @Override
            public boolean test(final Path path, final BasicFileAttributes attributes) {
                if (!isScheduled() && listingMode == ListingMode.EXECUTION) {
                    throw new ProcessorStoppedException();
                }

                final long now = System.currentTimeMillis();
                final long timeToList = now - lastTimestamp;
                lastTimestamp = now;

                final Path relativeDirectory = basePath.relativize(path).getParent();
                final String relativePath = relativeDirectory == null ? "" : relativeDirectory.toString();
                final String filename = path.getFileName().toString();
                performanceTracker.acceptOperation(DiskOperation.RETRIEVE_NEXT_FILE_FROM_OS, relativePath, filename, timeToList);

                final boolean isDirectory = attributes.isDirectory();
                if (isDirectory) {
                    performanceTracker.setActiveDirectory(relativePath);
                }

                final TimedOperationKey operationKey = performanceTracker.beginOperation(DiskOperation.FILTER, relativePath, filename);

                try {
                    final boolean matchesFilters = (minTimestamp == null || attributes.lastModifiedTime().toMillis() >= minTimestamp)
                            && fileFilter.test(path, attributes);
                    if (!isDirectory && (!applyFilters || matchesFilters)) {
                        // We store the attributes for each Path we are returning in order to avoid
                        // retrieving them again later when creating the FileInfo
                        lastModifiedMap.put(path, attributes);

                        return true;
                    }

                    return false;
                } finally {
                    performanceTracker.completeOperation(operationKey);

                    if (TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - performanceLoggingTimestamp) >= 5) {
                        logPerformance();
                    }
                }
            }
        };

        try {
            final long start = System.currentTimeMillis();
            final List<FileInfo> result = new LinkedList<>();

            Files.walkFileTree(basePath, Set.of(FileVisitOption.FOLLOW_LINKS), maxDepth, new FileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attributes) {
                    if (Files.isReadable(dir)) {
                        return FileVisitResult.CONTINUE;
                    } else {
                        getLogger().debug("The following directory is not readable: {}", dir);
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                }

                @Override
                public FileVisitResult visitFile(final Path path, final BasicFileAttributes attributes) {
                    if (matcher.test(path, attributes)) {
                        final File file = path.toFile();
                        final BasicFileAttributes fileAttributes = lastModifiedMap.get(path);
                        final FileInfo fileInfo = new FileInfo.Builder()
                                .directory(false)
                                .filename(file.getName())
                                .fullPathFileName(file.getAbsolutePath())
                                .lastModifiedTime(fileAttributes.lastModifiedTime().toMillis())
                                .size(fileAttributes.size())
                                .build();

                        result.add(fileInfo);
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(final Path path, final IOException e) {
                    if (e instanceof AccessDeniedException) {
                        getLogger().debug("The following file is not readable: {}", path);
                        return FileVisitResult.SKIP_SUBTREE;
                    } else {
                        getLogger().error("Error during visiting file {}", path, e);
                        return FileVisitResult.TERMINATE;
                    }
                }

                @Override
                public FileVisitResult postVisitDirectory(final Path dir, final IOException e) {
                    if (e != null) {
                        getLogger().error("Error during visiting directory {}", dir, e);
                    }

                    return FileVisitResult.CONTINUE;
                }
            });

            final long millis = System.currentTimeMillis() - start;

            getLogger().debug("Took {} milliseconds to perform listing and gather {} entries", millis, result.size());
            return result;
        } catch (final ProcessorStoppedException pse) {
            getLogger().info("Processor was stopped so will not complete listing of Files");
            return Collections.emptyList();
        } finally {
            if (performanceTracker != null) {
                performanceTracker.completeActiveDirectory();
            }
        }
    }

    @Override
    protected String getListingContainerName(final ProcessContext context) {
        return String.format("%s Directory [%s]", context.getProperty(DIRECTORY_LOCATION).getValue(), getPath(context));
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return DIRECTORY.equals(property)
                || RECURSE.equals(property)
                || FILE_FILTER.equals(property)
                || PATH_FILTER.equals(property)
                || MIN_AGE.equals(property)
                || MAX_AGE.equals(property)
                || MIN_SIZE.equals(property)
                || MAX_SIZE.equals(property)
                || IGNORE_HIDDEN_FILES.equals(property);
    }

    private String formatDateTime(final long dateTime) {
        final ZonedDateTime zonedDateTime = Instant.ofEpochMilli(dateTime).atZone(ZoneId.systemDefault());
        return DATE_TIME_FORMATTER.format(zonedDateTime);
    }

    private BiPredicate<Path, BasicFileAttributes> createFileFilter(final ProcessContext context, final PerformanceTracker performanceTracker,
                                                                    final boolean applyFilters, final Path basePath) {
        final long minSize = context.getProperty(MIN_SIZE).asDataSize(DataUnit.B).longValue();
        final Double maxSize = context.getProperty(MAX_SIZE).asDataSize(DataUnit.B);
        final long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAge = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final boolean ignoreHidden = context.getProperty(IGNORE_HIDDEN_FILES).asBoolean();
        final String fileFilter = context.getProperty(FILE_FILTER).getValue();
        final Pattern filePattern = Pattern.compile(fileFilter);
        final boolean recurseDirs = context.getProperty(RECURSE).asBoolean();
        final String pathPatternStr = context.getProperty(PATH_FILTER).getValue();
        final Pattern pathPattern = (!recurseDirs || pathPatternStr == null) ? null : Pattern.compile(pathPatternStr);

        return (path, attributes) -> {
            if (!applyFilters) {
                return true;
            }

            if (minSize > attributes.size()) {
                return false;
            }
            if (maxSize != null && maxSize < attributes.size()) {
                return false;
            }
            final long fileAge = System.currentTimeMillis() - attributes.lastModifiedTime().toMillis();
            if (minAge > fileAge) {
                return false;
            }
            if (maxAge != null && maxAge < fileAge) {
                return false;
            }

            final Path relativePath = basePath.relativize(path);
            final Path relativePathParent = relativePath.getParent();
            final String relativeDir = relativePathParent == null ? "" : relativePathParent.toString();
            final String filename = path.getFileName().toString();
            final TimingInfo timingInfo = performanceTracker.getTimingInfo(relativeDir, filename);

            final File file = path.toFile();

            if ((pathPattern != null) && (!pathPattern.matcher(relativeDir).matches())) {
                return false;
            }

            final boolean matchesFilter = filePattern.matcher(filename).matches();
            if (!matchesFilter) {
                return false;
            }

            // Verify that we have at least read permissions on the file we're considering grabbing
            if (!timingInfo.timeOperation(DiskOperation.CHECK_READABLE, () -> Files.isReadable(path))) {
                return false;
            }

            if (ignoreHidden && timingInfo.timeOperation(DiskOperation.CHECK_HIDDEN, file::isHidden)) {
                return false;
            }

            return true;
        };
    }

    /**
     * A PerformanceTracker that is capable of tracking which disk access operation is active and which directory is actively being listed,
     * as well as timing specific operations, but does not track metrics over any amount of time. This implementation does not provide the ability
     * to glean information such as which operations or files are taking the longest to operate on but uses very little heap.
     */
    public static class UntrackedPerformanceTracker implements PerformanceTracker {
        private TimedOperationKey activeOperation = null;
        private String activeDirectory;
        private long activeDirectoryStartTime = -1L;

        private final ComponentLog logger;
        private final long maxDiskOperationMillis;

        public UntrackedPerformanceTracker(final ComponentLog logger, final long maxDiskOperationMillis) {
            this.logger = logger;
            this.maxDiskOperationMillis = maxDiskOperationMillis;
        }

        @Override
        public TimedOperationKey beginOperation(final DiskOperation operation, final String directory, final String filename) {
            return null;
        }

        @Override
        public void completeOperation(final TimedOperationKey operationKey) {
        }

        @Override
        public void acceptOperation(final DiskOperation operation, final String directory, final String filename, final long millis) {
        }

        @Override
        public TimingInfo getTimingInfo(final String directory, final String filename) {
            return new TimingInfo(directory, filename, this, logger, maxDiskOperationMillis);
        }

        @Override
        public OperationStatistics getOperationStatistics(final DiskOperation operation) {
            return OperationStatistics.EMPTY;
        }

        @Override
        public synchronized void setActiveOperation(final TimedOperationKey operationKey) {
            this.activeOperation = operationKey;
        }

        @Override
        public synchronized void completeActiveOperation() {
            this.activeOperation = null;
        }

        @Override
        public synchronized TimedOperationKey getActiveOperation() {
            return activeOperation;
        }

        @Override
        public void purgeTimingInfo(final long cutoff) {
        }

        @Override
        public long getEarliestTimestamp() {
            return System.currentTimeMillis();
        }

        @Override
        public synchronized void setActiveDirectory(final String directory) {
            activeDirectory = directory;
            activeDirectoryStartTime = System.currentTimeMillis();
        }

        @Override
        public synchronized void completeActiveDirectory() {
            activeDirectory = null;
            activeDirectoryStartTime = -1L;
        }

        @Override
        public synchronized long getActiveDirectoryStartTime() {
            return activeDirectoryStartTime;
        }

        @Override
        public synchronized String getActiveDirectory() {
            return activeDirectory;
        }

        @Override
        public int getTrackedFileCount() {
            return 0;
        }
    }

    /**
     * Tracks metrics using a rolling window of time, in which older metrics are 'aged off' by calling {@link #purgeTimingInfo(long)}. Tracking these metrics allows information
     * to be gleaned, such as which files are expensive to operate on or which operations are most expensive. However, the heap utilization is significant.
     */
    public static final class RollingMetricPerformanceTracker implements PerformanceTracker {
        private final Map<String, String> directoryCanonicalization = new HashMap<>();
        private final Map<Tuple<String, String>, TimingInfo> directoryToTimingInfo;
        private TimedOperationKey activeOperation;
        private long earliestTimestamp = System.currentTimeMillis();
        private final long maxDiskOperationMillis;
        private final ComponentLog logger;

        private String activeDirectory;
        private long activeDirectoryStartTime = -1L;

        public RollingMetricPerformanceTracker(final ComponentLog logger, final long maxDiskOperationMillis, final int maxEntries) {
            this.logger = logger;
            this.maxDiskOperationMillis = maxDiskOperationMillis;

            directoryToTimingInfo = new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(final Map.Entry<Tuple<String, String>, TimingInfo> eldest) {
                    return size() > maxEntries;
                }
            };
        }

        @Override
        public synchronized TimedOperationKey beginOperation(final DiskOperation operation, final String directory, final String filename) {
            return new TimedOperationKey(operation, directory, filename, System.currentTimeMillis());
        }

        @Override
        public synchronized void completeOperation(final TimedOperationKey operationKey) {
            final TimingInfo timingInfo = getTimingInfo(operationKey.getDirectory(), operationKey.getFilename());
            timingInfo.accept(operationKey.getOperation(), System.currentTimeMillis() - operationKey.getStartTime());
        }

        @Override
        public synchronized void acceptOperation(final DiskOperation operation, final String directory, final String filename, final long millis) {
            final String canonicalDirectory = directoryCanonicalization.computeIfAbsent(directory, key -> directory);
            final Tuple<String, String> key = new Tuple<>(canonicalDirectory, filename);
            final TimingInfo timingInfo = directoryToTimingInfo.computeIfAbsent(key, k -> new TimingInfo(directory, filename, this, logger, maxDiskOperationMillis));
            timingInfo.accept(operation, millis);
        }

        @Override
        public synchronized TimingInfo getTimingInfo(final String directory, final String filename) {
            final String canonicalDirectory = directoryCanonicalization.computeIfAbsent(directory, key -> directory);
            final Tuple<String, String> key = new Tuple<>(canonicalDirectory, filename);
            final TimingInfo timingInfo = directoryToTimingInfo.computeIfAbsent(key, k -> new TimingInfo(directory, filename, this, logger, maxDiskOperationMillis));

            return timingInfo;
        }

        @Override
        public void setActiveOperation(final TimedOperationKey activeOperation) {
            this.activeOperation = activeOperation;
        }

        @Override
        public void completeActiveOperation() {
            this.activeOperation = null;
        }

        @Override
        public synchronized TimedOperationKey getActiveOperation() {
            return activeOperation;
        }

        @Override
        public synchronized void setActiveDirectory(final String directory) {
            activeDirectory = directory;
            activeDirectoryStartTime = System.currentTimeMillis();
        }

        @Override
        public synchronized void completeActiveDirectory() {
            activeDirectory = null;
            activeDirectoryStartTime = -1L;
        }

        @Override
        public synchronized long getActiveDirectoryStartTime() {
            return activeDirectoryStartTime;
        }

        @Override
        public synchronized String getActiveDirectory() {
            return activeDirectory;
        }

        @Override
        public synchronized int getTrackedFileCount() {
            return directoryToTimingInfo.size();
        }

        @Override
        public synchronized void purgeTimingInfo(final long cutoff) {
            logger.debug("Purging any entries from Performance Tracker that is older than {}", new Date(cutoff));
            final Iterator<Map.Entry<Tuple<String, String>, TimingInfo>> itr = directoryToTimingInfo.entrySet().iterator();

            int purgedCount = 0;
            long earliestTimestamp = System.currentTimeMillis();
            while (itr.hasNext()) {
                final Map.Entry<Tuple<String, String>, TimingInfo> entry = itr.next();
                final TimingInfo timingInfo = entry.getValue();
                final long creationTime = timingInfo.getCreationTimestamp();

                if (creationTime < cutoff) {
                    itr.remove();
                    purgedCount++;

                    directoryCanonicalization.remove(entry.getKey().getKey());
                } else {
                    earliestTimestamp = Math.min(earliestTimestamp, creationTime);
                }
            }

            this.earliestTimestamp = earliestTimestamp;
            logger.debug("Purged {} entries from Performance Tracker; now holding {} entries", purgedCount, directoryToTimingInfo.size());
        }

        @Override
        public long getEarliestTimestamp() {
            return earliestTimestamp;
        }

        @Override
        public synchronized OperationStatistics getOperationStatistics(final DiskOperation operation) {
            long count = 0L;
            long sum = 0L;
            long min = 0L;
            long max = 0L;

            // Calculate min/max/mean
            for (final TimingInfo timingInfo : directoryToTimingInfo.values()) {
                final long operationTime = timingInfo.getOperationTime(operation);

                if (operationTime < 0) { // operation not conducted
                    continue;
                }

                sum += operationTime;

                if (count++ == 0) {
                    min = operationTime;
                    max = operationTime;
                } else {
                    min = Math.min(min, operationTime);
                    max = Math.max(max, operationTime);
                }
            }

            if (count == 0) {
                return OperationStatistics.EMPTY;
            }

            double average = (double) sum / (double) count;

            // Calculate Standard Deviation
            final double stdDeviation = calculateStdDev(average, count, operation);
            final double outlierCutoff = average + 2 * stdDeviation;

            final Map<String, Long> outliers = new HashMap<>();
            for (final TimingInfo timingInfo : directoryToTimingInfo.values()) {
                final long operationTime = timingInfo.getOperationTime(operation);

                if (operationTime > 2 && operationTime > outlierCutoff) {
                    final String directory = timingInfo.getDirectory();
                    final String filename = timingInfo.getFilename();
                    final String fullPath = directory.endsWith("/") ? directory + filename : directory + "/" + filename;
                    outliers.put(fullPath, operationTime);
                }
            }

            return new StandardOperationStatistics(min, max, count, average, stdDeviation, outliers);
        }

        private double calculateStdDev(final double average, final double count, final DiskOperation operation) {
            double squaredDifferenceSum = 0D;
            for (final TimingInfo timingInfo : directoryToTimingInfo.values()) {
                final long operationTime = timingInfo.getOperationTime(operation);
                if (operationTime < 0) {
                    continue;
                }

                final double differenceSquared = Math.pow((operationTime - average), 2);
                squaredDifferenceSum += differenceSquared;
            }

            final double squaredDifferenceAverage = squaredDifferenceSum / count;
            final double stdDeviation = Math.pow(squaredDifferenceAverage, 0.5);
            return stdDeviation;
        }
    }

    /**
     * Provides a mechanism for timing how long a particular operation takes to complete, logging if it takes longer than the configured threshold.
     */
    private static class TimingInfo {
        private final String directory;
        private final String filename;
        private final int[] operationTimes;
        private final PerformanceTracker tracker;
        private final long creationTimestamp;
        private final ComponentLog logger;
        private final long maxDiskOperationMillis;

        public TimingInfo(final String directory, final String filename, final PerformanceTracker tracker, final ComponentLog logger, final long maxDiskOperationMillis) {
            this.directory = directory;
            this.filename = filename;
            this.tracker = tracker;
            this.logger = logger;
            this.maxDiskOperationMillis = maxDiskOperationMillis;

            this.creationTimestamp = System.currentTimeMillis();

            operationTimes = new int[DiskOperation.values().length];
            Arrays.fill(operationTimes, -1);
        }

        public String getDirectory() {
            return directory;
        }

        public String getFilename() {
            return filename;
        }

        public void accept(final DiskOperation operation, final long duration) {
            operationTimes[operation.ordinal()] = (int) duration;

            if (duration > maxDiskOperationMillis) {
                final String fullPath = getFullPath();
                logger.warn("This Processor completed action {} on {} in {} milliseconds, which exceeds the configured threshold of {} milliseconds",
                        operation, fullPath, duration, maxDiskOperationMillis);
            }

            if (logger.isTraceEnabled()) {
                logger.trace("Performing operation {} on {} took {} milliseconds", operation, getFullPath(), duration);
            }
        }

        private String getFullPath() {
            if (directory.isEmpty()) {
                return filename;
            } else {
                return directory.endsWith("/") ? directory + filename : directory + "/" + filename;
            }
        }

        public long getOperationTime(final DiskOperation operation) {
            return operationTimes[operation.ordinal()];
        }

        private <T> T timeOperation(final DiskOperation operation, final Supplier<T> function) {
            final long start = System.currentTimeMillis();
            final TimedOperationKey operationKey = new TimedOperationKey(operation, directory, filename, start);
            tracker.setActiveOperation(operationKey);

            try {
                final T value = function.get();
                final long millis = System.currentTimeMillis() - start;
                accept(operation, millis);
                return value;
            } finally {
                tracker.completeActiveOperation();
            }
        }

        private void timeOperation(final DiskOperation operation, final Runnable task) {
            final long start = System.currentTimeMillis();
            final TimedOperationKey operationKey = new TimedOperationKey(operation, directory, filename, start);
            tracker.setActiveOperation(operationKey);

            try {
                task.run();
                final long millis = System.currentTimeMillis() - start;
                accept(operation, millis);
            } finally {
                tracker.completeActiveOperation();
            }
        }

        public long getCreationTimestamp() {
            return creationTimestamp;
        }
    }

    /**
     * PerformanceTracker is responsible for providing a mechanism by which any disk operation can be timed and the timing information
     * can both be used to issue warnings as well as be aggregated for some amount of time, in order to understand how long certain disk operations
     * take and which files may be responsible for causing longer-than-usual operations to be performed.
     */
    interface PerformanceTracker {
        TimedOperationKey beginOperation(DiskOperation operation, String directory, String filename);

        void completeOperation(TimedOperationKey operationKey);

        void acceptOperation(DiskOperation operation, String directory, String filename, long millis);

        TimingInfo getTimingInfo(String directory, String filename);

        OperationStatistics getOperationStatistics(DiskOperation operation);

        void setActiveOperation(TimedOperationKey operationKey);

        void completeActiveOperation();

        TimedOperationKey getActiveOperation();

        void purgeTimingInfo(long cutoff);

        long getEarliestTimestamp();

        void setActiveDirectory(String directory);

        void completeActiveDirectory();

        String getActiveDirectory();

        long getActiveDirectoryStartTime();

        int getTrackedFileCount();
    }


    interface OperationStatistics {
        long getMin();
        long getMax();
        long getCount();
        double getAverage();
        double getStandardDeviation();

        Map<String, Long> getOutliers();

        OperationStatistics EMPTY = new OperationStatistics() {
            @Override
            public long getMin() {
                return 0;
            }

            @Override
            public long getMax() {
                return 0;
            }

            @Override
            public long getCount() {
                return 0;
            }

            @Override
            public double getAverage() {
                return 0;
            }

            @Override
            public double getStandardDeviation() {
                return 0;
            }

            @Override
            public Map<String, Long> getOutliers() {
                return Collections.emptyMap();
            }
        };
    }

    private static class StandardOperationStatistics implements OperationStatistics {
        private final long min;
        private final long max;
        private final long count;
        private final double average;
        private final double stdDev;
        private final Map<String, Long> outliers;

        public StandardOperationStatistics(final long min, final long max, final long count, final double average, final double stdDev, final Map<String, Long> outliers) {
            this.min = min;
            this.max = max;
            this.count = count;
            this.average = average;
            this.stdDev = stdDev;
            this.outliers = outliers;
        }

        @Override
        public long getMin() {
            return min;
        }

        @Override
        public long getMax() {
            return max;
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public double getAverage() {
            return average;
        }

        @Override
        public double getStandardDeviation() {
            return stdDev;
        }

        @Override
        public Map<String, Long> getOutliers() {
            return outliers;
        }
    }


    private static class TimedOperationKey {
        private final DiskOperation operation;
        private final String directory;
        private final String filename;
        private final long startTime;

        public TimedOperationKey(final DiskOperation operation, final String directory, final String filename, final long startTime) {
            this.operation = operation;
            this.startTime = startTime;
            this.directory = directory;
            this.filename = filename;
        }

        public DiskOperation getOperation() {
            return operation;
        }

        public String getDirectory() {
            return directory;
        }

        public String getFilename() {
            return filename;
        }

        public long getStartTime() {
            return startTime;
        }
    }

    private enum DiskOperation {
        RETRIEVE_BASIC_ATTRIBUTES,
        RETRIEVE_OWNER_ATTRIBUTES,
        RETRIEVE_POSIX_ATTRIBUTES,
        CHECK_HIDDEN,
        CHECK_READABLE,
        FILTER,
        RETRIEVE_NEXT_FILE_FROM_OS
    }

    private static class ProcessorStoppedException extends RuntimeException {
    }

    static class MonitorActiveTasks implements Runnable {
        private final PerformanceTracker performanceTracker;
        private final ComponentLog logger;
        private final long maxDiskOperationMillis;
        private final long maxListingMillis;
        private final long millisToKeepStats;
        private long lastPurgeTimestamp = 0L;

        public MonitorActiveTasks(final PerformanceTracker tracker, final ComponentLog logger, final long maxDiskOperationMillis, final long maxListingMillis, final long millisToKeepStats) {
            this.performanceTracker = tracker;
            this.logger = logger;
            this.maxDiskOperationMillis = maxDiskOperationMillis;
            this.maxListingMillis = maxListingMillis;
            this.millisToKeepStats = millisToKeepStats;
        }

        @Override
        public void run() {
            monitorActiveOperation();
            monitorActiveDirectory();

            final long now = System.currentTimeMillis();
            final long millisSincePurge = now - lastPurgeTimestamp;
            if (millisSincePurge > TimeUnit.SECONDS.toMillis(60)) {
                performanceTracker.purgeTimingInfo(now - millisToKeepStats);
                lastPurgeTimestamp = System.currentTimeMillis();
            }
        }

        private void monitorActiveOperation() {
            final TimedOperationKey activeOperation = performanceTracker.getActiveOperation();
            if (activeOperation == null) {
                return;
            }

            final long activeTime = System.currentTimeMillis() - activeOperation.getStartTime();
            if (activeTime > maxDiskOperationMillis) {
                final String directory = activeOperation.getDirectory();
                final String filename = activeOperation.getFilename();

                final String fullPath;
                if (directory.isEmpty()) {
                    fullPath = filename;
                } else {
                    fullPath = directory.endsWith("/") ? directory + filename : directory + "/" + filename;
                }

                logger.warn("This Processor has currently spent {} milliseconds performing the {} action on {}, which exceeds the configured threshold of {} milliseconds",
                        activeTime, activeOperation.getOperation(), fullPath, maxDiskOperationMillis);
            }
        }

        private void monitorActiveDirectory() {
            final String activeDirectory = performanceTracker.getActiveDirectory();
            final long startTime = performanceTracker.getActiveDirectoryStartTime();
            if (startTime <= 0) {
                return;
            }

            final long activeMillis = System.currentTimeMillis() - startTime;
            if (activeMillis > maxListingMillis) {
                final String fullPath = activeDirectory.isEmpty() ? "the base directory" : activeDirectory;
                logger.warn("This processor has currently spent {} milliseconds performing the listing of {}, which exceeds the configured threshold of {} milliseconds",
                        activeMillis, fullPath, maxListingMillis);
            }
        }
    }
}
