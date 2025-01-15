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
package org.apache.nifi.processors.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.util.GSSExceptionRollbackYieldSessionHandler;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"hadoop", "HCFS", "HDFS", "get", "fetch", "ingest", "source", "filesystem"})
@CapabilityDescription("Fetch files from Hadoop Distributed File System (HDFS) into FlowFiles. This Processor will delete the file from HDFS after fetching it.")
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "The name of the file that was read from HDFS."),
    @WritesAttribute(attribute = "path", description = "The path is set to the relative path of the file's directory on HDFS. For example, if the Directory property "
            + "is set to /tmp, then files picked up from /tmp will have the path attribute set to \"./\". If the Recurse Subdirectories property is set to true and "
            + "a file is picked up from /tmp/abc/1/2/3, then the path attribute will be set to \"abc/1/2/3\".") })
@SeeAlso({PutHDFS.class, ListHDFS.class})
@Restricted(restrictions = {
    @Restriction(
        requiredPermission = RequiredPermission.READ_DISTRIBUTED_FILESYSTEM,
        explanation = "Provides operator the ability to retrieve any file that NiFi has access to in HDFS or the local filesystem."),
    @Restriction(
        requiredPermission = RequiredPermission.WRITE_DISTRIBUTED_FILESYSTEM,
        explanation = "Provides operator the ability to delete any file that NiFi has access to in HDFS or the local filesystem.")
})
public class GetHDFS extends AbstractHadoopProcessor {

    public static final String BUFFER_SIZE_KEY = "io.file.buffer.size";
    public static final int BUFFER_SIZE_DEFAULT = 4096;
    public static final int MAX_WORKING_QUEUE_SIZE = 25000;

    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
    .name("success")
    .description("All files retrieved from HDFS are transferred to this relationship")
    .build();

    // properties
    public static final PropertyDescriptor RECURSE_SUBDIRS = new PropertyDescriptor.Builder()
    .name("Recurse Subdirectories")
    .description("Indicates whether to pull files from subdirectories of the HDFS directory")
    .required(true)
    .allowableValues("true", "false")
    .defaultValue("true")
    .build();

    public static final PropertyDescriptor KEEP_SOURCE_FILE = new PropertyDescriptor.Builder()
    .name("Keep Source File")
    .description("Determines whether to delete the file from HDFS after it has been successfully transferred. If true, the file will be fetched repeatedly. This is intended for testing only.")
    .required(true)
    .allowableValues("true", "false")
    .defaultValue("false")
    .build();

    public static final PropertyDescriptor FILE_FILTER_REGEX = new PropertyDescriptor.Builder()
    .name("File Filter Regex")
    .description("A Java Regular Expression for filtering Filenames; if a filter is supplied then only files whose names match that Regular "
            + "Expression will be fetched, otherwise all files will be fetched")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor FILTER_MATCH_NAME_ONLY = new PropertyDescriptor.Builder()
    .name("Filter Match Name Only")
    .description("If true then File Filter Regex will match on just the filename, otherwise subdirectory names will be included with filename "
            + "in the regex comparison")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor IGNORE_DOTTED_FILES = new PropertyDescriptor.Builder()
    .name("Ignore Dotted Files")
    .description("If true, files whose names begin with a dot (\".\") will be ignored")
    .required(true)
    .allowableValues("true", "false")
    .defaultValue("true")
    .build();

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
    .name("Minimum File Age")
    .description("The minimum age that a file must be in order to be pulled; any file younger than this amount of time (based on last modification date) will be ignored")
    .required(true)
    .addValidator(StandardValidators.createTimePeriodValidator(0, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
    .defaultValue("0 sec")
    .build();

    public static final PropertyDescriptor MAX_AGE = new PropertyDescriptor.Builder()
    .name("Maximum File Age")
    .description("The maximum age that a file must be in order to be pulled; any file older than this amount of time (based on last modification date) will be ignored")
    .required(false)
    .addValidator(StandardValidators.createTimePeriodValidator(100, TimeUnit.MILLISECONDS, Long.MAX_VALUE, TimeUnit.NANOSECONDS))
    .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
    .name("Batch Size")
    .description("The maximum number of files to pull in each iteration, based on run schedule.")
    .required(true)
    .defaultValue("100")
    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
    .build();

    public static final PropertyDescriptor POLLING_INTERVAL = new PropertyDescriptor.Builder()
    .name("Polling Interval")
    .description("Indicates how long to wait between performing directory listings")
    .required(true)
    .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
    .defaultValue("0 sec")
    .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
    .name("IO Buffer Size")
    .description("Amount of memory to use to buffer file contents during IO. This overrides the Hadoop Configuration")
    .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
    .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                    DIRECTORY,
                    RECURSE_SUBDIRS,
                    KEEP_SOURCE_FILE,
                    FILE_FILTER_REGEX,
                    FILTER_MATCH_NAME_ONLY,
                    IGNORE_DOTTED_FILES,
                    MIN_AGE,
                    MAX_AGE,
                    POLLING_INTERVAL,
                    BATCH_SIZE,
                    BUFFER_SIZE,
                    COMPRESSION_CODEC
            )
    ).toList();

    protected ProcessorConfiguration processorConfig;
    private final AtomicLong logEmptyListing = new AtomicLong(2L);

    private final AtomicLong lastPollTime = new AtomicLong(0L);
    private final Lock listingLock = new ReentrantLock();
    private final Lock queueLock = new ReentrantLock();

    private final BlockingQueue<Path> filePathQueue = new LinkedBlockingQueue<>(MAX_WORKING_QUEUE_SIZE);
    private final BlockingQueue<Path> processing = new LinkedBlockingQueue<>();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(context));

        final Long minAgeProp = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAgeProp = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final long minimumAge = (minAgeProp == null) ? 0L : minAgeProp;
        final long maximumAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;
        if (minimumAge > maximumAge) {
            problems.add(new ValidationResult.Builder().valid(false).subject("GetHDFS Configuration")
                    .explanation(MIN_AGE.getName() + " cannot be greater than " + MAX_AGE.getName()).build());
        }

        try {
            new Path(context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue());
        } catch (Exception e) {
            problems.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject("Directory")
                    .explanation(e.getMessage())
                    .build());
        }

        return problems;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        abstractOnScheduled(context);
        // copy configuration values to pass them around cleanly
        processorConfig = new ProcessorConfiguration(context);

        // forget the state of the queue in case HDFS contents changed while this processor was turned off
        queueLock.lock();
        try {
            filePathQueue.clear();
            processing.clear();
        } finally {
            queueLock.unlock();
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final List<Path> files = new ArrayList<>(batchSize);

        // retrieve new file names from HDFS and place them into work queue
        if (filePathQueue.size() < MAX_WORKING_QUEUE_SIZE / 2) {
            try {
                final StopWatch stopWatch = new StopWatch(true);
                Set<Path> listedFiles = performListing(context);
                stopWatch.stop();
                final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);

                if (listedFiles != null) {
                    // place files into the work queue
                    int newItems = 0;
                    queueLock.lock();
                    try {
                        for (Path file : listedFiles) {
                            if (!filePathQueue.contains(file) && !processing.contains(file)) {
                                if (!filePathQueue.offer(file)) {
                                    break;
                                }
                                newItems++;
                            }
                        }
                    } catch (Exception e) {
                        getLogger().warn("Could not add to processing queue", e);
                    } finally {
                        queueLock.unlock();
                    }
                    if (listedFiles.size() > 0) {
                        logEmptyListing.set(3L);
                    }
                    if (logEmptyListing.getAndDecrement() > 0) {
                        getLogger().info("Obtained file listing in {} milliseconds; listing had {} items, {} of which were new",
                                millis, listedFiles.size(), newItems);
                    }
                }
            } catch (IOException e) {
                handleAuthErrors(e, session, context, new GSSExceptionRollbackYieldSessionHandler());
                getLogger().warn("Error while retrieving list of files due to {}", e.getMessage(), e);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                context.yield();
                getLogger().warn("Interrupted while retrieving files", e);
                return;
            }
        }

        // prepare to process a batch of files in the queue
        queueLock.lock();
        try {
            filePathQueue.drainTo(files, batchSize);
            if (files.isEmpty()) {
                // nothing to do!
                context.yield();
                return;
            }
            processing.addAll(files);
        } finally {
            queueLock.unlock();
        }

        processBatchOfFiles(files, context, session);

        session.commitAsync(() -> {
            queueLock.lock();
            try {
                processing.removeAll(files);
            } finally {
                queueLock.unlock();
            }
        });
    }

    protected void processBatchOfFiles(final List<Path> files, final ProcessContext context, final ProcessSession session) {
        // process the batch of files
        InputStream stream = null;
        CompressionCodec codec = null;
        Configuration conf = getConfiguration();
        FileSystem hdfs = getFileSystem();
        final boolean keepSourceFiles = context.getProperty(KEEP_SOURCE_FILE).asBoolean();
        final Double bufferSizeProp = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B);
        int bufferSize = bufferSizeProp != null ? bufferSizeProp.intValue() : conf.getInt(BUFFER_SIZE_KEY,
                BUFFER_SIZE_DEFAULT);
        final Path rootDir = getNormalizedPath(context, DIRECTORY);

        final CompressionType compressionType = CompressionType.valueOf(context.getProperty(COMPRESSION_CODEC).toString());
        final boolean inferCompressionCodec = compressionType == CompressionType.AUTOMATIC;
        if (inferCompressionCodec || compressionType != CompressionType.NONE) {
            codec = getCompressionCodec(context, getConfiguration());
        }
        final CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);
        for (final Path file : files) {
            try {
                if (!getUserGroupInformation().doAs((PrivilegedExceptionAction<Boolean>) () -> hdfs.exists(file))) {
                    continue; // if file is no longer there then move on
                }
                final String originalFilename = file.getName();
                final String relativePath = getPathDifference(rootDir, file);

                stream = getUserGroupInformation().doAs((PrivilegedExceptionAction<FSDataInputStream>) () -> hdfs.open(file, bufferSize));

                final String outputFilename;
                // Check if we should infer compression codec
                if (inferCompressionCodec) {
                    codec = compressionCodecFactory.getCodec(file);
                }
                // Check if compression codec is defined (inferred or otherwise)
                if (codec != null) {
                    stream = codec.createInputStream(stream);
                    outputFilename = StringUtils.removeEnd(originalFilename, codec.getDefaultExtension());
                } else {
                    outputFilename = originalFilename;
                }

                FlowFile flowFile = session.create();

                final StopWatch stopWatch = new StopWatch(true);
                flowFile = session.importFrom(stream, flowFile);
                stopWatch.stop();
                final String dataRate = stopWatch.calculateDataRate(flowFile.getSize());
                final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);

                flowFile = session.putAttribute(flowFile, CoreAttributes.PATH.key(), relativePath.isEmpty() ? "." : relativePath);
                flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), outputFilename);

                if (!keepSourceFiles && !getUserGroupInformation().doAs((PrivilegedExceptionAction<Boolean>) () -> hdfs.delete(file, false))) {
                    getLogger().warn("Could not remove {} from HDFS. Not ingesting this file ...", file);
                    session.remove(flowFile);
                    continue;
                }

                session.getProvenanceReporter().receive(flowFile, file.toString());
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().info("retrieved {} from HDFS {} in {} milliseconds at a rate of {}", flowFile, file, millis, dataRate);
            } catch (final Throwable t) {
                if (!handleAuthErrors(t, session, context, new GSSExceptionRollbackYieldSessionHandler())) {
                    getLogger().error("Error retrieving file {} from HDFS due to {}", file, t);
                    session.rollback();
                    context.yield();
                }
            } finally {
                IOUtils.closeQuietly(stream);
                stream = null;
            }
        }
    }

    /**
     * Do a listing of HDFS if the POLLING_INTERVAL has lapsed.
     *
     * Will return null if POLLING_INTERVAL has not lapsed. Will return an empty set if no files were found on HDFS that matched the configured filters.
     *
     * @param context context
     * @return null if POLLING_INTERVAL has not lapsed. Will return an empty set if no files were found on HDFS that matched the configured filters
     * @throws java.io.IOException ex
     */
    protected Set<Path> performListing(final ProcessContext context) throws IOException, InterruptedException {

        final long pollingIntervalMillis = context.getProperty(POLLING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
        final long nextPollTime = lastPollTime.get() + pollingIntervalMillis;
        Set<Path> listing = null;

        if (System.currentTimeMillis() >= nextPollTime && listingLock.tryLock()) {
            try {
                final FileSystem hdfs = getFileSystem();
                final Path directoryPath = getNormalizedPath(context, DIRECTORY);

                final boolean directoryExists = getUserGroupInformation().doAs((PrivilegedExceptionAction<Boolean>) () -> hdfs.exists(directoryPath));
                if (!directoryExists) {
                    context.yield();
                    getLogger().warn("The directory {} does not exist.", directoryPath);
                } else {
                    // get listing
                    listing = selectFiles(hdfs, directoryPath, null);
                }

                lastPollTime.set(System.currentTimeMillis());
            } finally {
                listingLock.unlock();
            }
        }

        return listing;
    }

    /**
     * Poll HDFS for files to process that match the configured file filters.
     *
     * @param hdfs hdfs
     * @param dir dir
     * @param filesVisited filesVisited
     * @return files to process
     * @throws java.io.IOException ex
     */
    protected Set<Path> selectFiles(final FileSystem hdfs, final Path dir, Set<Path> filesVisited) throws IOException, InterruptedException {
        if (null == filesVisited) {
            filesVisited = new HashSet<>();
        }

        final Set<Path> files = new HashSet<>();

        FileStatus[] fileStatuses = getUserGroupInformation().doAs((PrivilegedExceptionAction<FileStatus[]>) () -> hdfs.listStatus(dir));
        for (final FileStatus file : fileStatuses) {
            if (files.size() >= MAX_WORKING_QUEUE_SIZE) {
                // no need to make the files set larger than what we would queue anyway
                break;
            }

            final Path canonicalFile = file.getPath();

            if (!filesVisited.add(canonicalFile)) { // skip files we've already seen (may be looping directory links)
                continue;
            }

            if (file.isDirectory() && processorConfig.getRecurseSubdirs()) {
                files.addAll(selectFiles(hdfs, canonicalFile, filesVisited));

            } else if (!file.isDirectory() && processorConfig.getPathFilter(dir).accept(canonicalFile)) {
                final long fileAge = System.currentTimeMillis() - file.getModificationTime();
                if (processorConfig.getMinimumAge() < fileAge && fileAge < processorConfig.getMaximumAge()) {
                    files.add(canonicalFile);

                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("{} selected file at path: {}", this, canonicalFile);
                    }

                }
            }
        }
        return files;
    }

    /**
     * Holder for a snapshot in time of some processor properties that are passed around.
     */
    protected static class ProcessorConfiguration {

        final private Pattern fileFilterPattern;
        final private boolean ignoreDottedFiles;
        final private boolean filterMatchBasenameOnly;
        final private long minimumAge;
        final private long maximumAge;
        final private boolean recurseSubdirs;

        ProcessorConfiguration(final ProcessContext context) {
            ignoreDottedFiles = context.getProperty(IGNORE_DOTTED_FILES).asBoolean();
            final String fileFilterRegex = context.getProperty(FILE_FILTER_REGEX).getValue();
            fileFilterPattern = (fileFilterRegex == null) ? null : Pattern.compile(fileFilterRegex);
            filterMatchBasenameOnly = context.getProperty(FILTER_MATCH_NAME_ONLY).asBoolean();
            final Long minAgeProp = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
            minimumAge = (minAgeProp == null) ? 0L : minAgeProp;
            final Long maxAgeProp = context.getProperty(MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
            maximumAge = (maxAgeProp == null) ? Long.MAX_VALUE : maxAgeProp;
            recurseSubdirs = context.getProperty(RECURSE_SUBDIRS).asBoolean();
        }

        protected long getMinimumAge() {
            return minimumAge;
        }

        protected long getMaximumAge() {
            return maximumAge;
        }

        public boolean getRecurseSubdirs() {
            return recurseSubdirs;
        }

        protected PathFilter getPathFilter(final Path dir) {
            return path -> {
                if (ignoreDottedFiles && path.getName().startsWith(".")) {
                    return false;
                }
                final String pathToCompare;
                if (filterMatchBasenameOnly) {
                    pathToCompare = path.getName();
                } else {
                    // figure out portion of path that does not include the provided root dir.
                    String relativePath = getPathDifference(dir, path);
                    if (relativePath.length() == 0) {
                        pathToCompare = path.getName();
                    } else {
                        pathToCompare = relativePath + Path.SEPARATOR + path.getName();
                    }
                }

                if (fileFilterPattern != null && !fileFilterPattern.matcher(pathToCompare).matches()) {
                    return false;
                }
                return true;
            };
        }
    }
}
