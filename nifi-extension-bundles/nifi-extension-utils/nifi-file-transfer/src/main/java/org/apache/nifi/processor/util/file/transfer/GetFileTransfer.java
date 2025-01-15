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
package org.apache.nifi.processor.util.file.transfer;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.util.StopWatch;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base class for GetSFTP and GetFTP
 */
public abstract class GetFileTransfer extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received are routed to success")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS
    );

    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_SIZE_ATTRIBUTE = "file.size";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    protected static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(FILE_MODIFY_DATE_ATTR_FORMAT);

    private final AtomicLong lastPollTime = new AtomicLong(-1L);
    private final Lock listingLock = new ReentrantLock();
    private final AtomicReference<BlockingQueue<FileInfo>> fileQueueRef = new AtomicReference<>();
    private final Set<FileInfo> processing = Collections.synchronizedSet(new HashSet<>());

    // Used when transferring filenames from the File Queue to the processing queue; multiple threads can do this
    // simultaneously using the sharableTransferLock; however, in order to check if either has a given file, the
    // mutually exclusive lock is required.
    private final ReadWriteLock transferLock = new ReentrantReadWriteLock();
    private final Lock sharableTransferLock = transferLock.readLock();
    private final Lock mutuallyExclusiveTransferLock = transferLock.writeLock();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    protected abstract FileTransfer getFileTransfer(final ProcessContext context);

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        listingLock.lock();
        try {
            final BlockingQueue<FileInfo> fileQueue = fileQueueRef.get();
            if (fileQueue != null) {
                fileQueue.clear();
            }
            fileQueueRef.set(null); // create new queue on next listing, in case queue type needs to change
        } finally {
            listingLock.unlock();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final long pollingIntervalMillis = context.getProperty(FileTransfer.POLLING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
        final long nextPollTime = lastPollTime.get() + pollingIntervalMillis;
        BlockingQueue<FileInfo> fileQueue = fileQueueRef.get();
        final ComponentLog logger = getLogger();

        // do not do the listing if there are already 100 or more items in our queue
        // 100 is really just a magic number that seems to work out well in practice
        FileTransfer transfer = null;
        if (System.currentTimeMillis() >= nextPollTime && (fileQueue == null || fileQueue.size() < 100) && listingLock.tryLock()) {
            try {
                transfer = getFileTransfer(context);
                try {
                    fetchListing(context, session, transfer);
                    lastPollTime.set(System.currentTimeMillis());
                } catch (final IOException e) {
                    context.yield();

                    try {
                        transfer.close();
                    } catch (final IOException e1) {
                        logger.warn("Unable to close connection", e1);
                    }

                    logger.error("Unable to fetch listing from remote server", e);
                    return;
                }
            } finally {
                listingLock.unlock();
            }
        }

        fileQueue = fileQueueRef.get();
        if (fileQueue == null || fileQueue.isEmpty()) {
            // nothing to do!
            context.yield();
            if (transfer != null) {
                try {
                    transfer.close();
                } catch (final IOException e1) {
                    logger.warn("Unable to close connection", e1);
                }
            }
            return;
        }

        final String hostname = context.getProperty(FileTransfer.HOSTNAME).evaluateAttributeExpressions().getValue();
        final boolean deleteOriginal = context.getProperty(FileTransfer.DELETE_ORIGINAL).asBoolean();
        final int maxSelects = context.getProperty(FileTransfer.MAX_SELECTS).asInteger();

        if (transfer == null) {
            transfer = getFileTransfer(context);
        }

        final Map<FlowFile, String> flowFilesReceived = new HashMap<>();

        try {
            for (int i = 0; i < maxSelects && isScheduled(); i++) {
                final FileInfo file;
                sharableTransferLock.lock();
                try {
                    file = fileQueue.poll();
                    if (file == null) {
                        break;
                    }
                    processing.add(file);
                } finally {
                    sharableTransferLock.unlock();
                }

                File relativeFile = new File(file.getFullPathFileName());
                final String parentRelativePath = (null == relativeFile.getParent()) ? "" : relativeFile.getParent();
                final String parentRelativePathString = parentRelativePath + "/";

                final Path absPath = relativeFile.toPath().toAbsolutePath();
                final String absPathString = absPath.getParent().toString() + "/";

                try {
                    FlowFile flowFile = session.create();
                    final StopWatch stopWatch = new StopWatch(false);
                    stopWatch.start();
                    flowFile = transfer.getRemoteFile(file.getFullPathFileName(), flowFile, session);
                    stopWatch.stop();
                    final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                    final String dataRate = stopWatch.calculateDataRate(flowFile.getSize());

                    final Map<String, String> attributes = getAttributesFromFile(file);
                    attributes.put(this.getClass().getSimpleName().toLowerCase() + ".remote.source", hostname);
                    attributes.put(CoreAttributes.PATH.key(), parentRelativePathString);
                    attributes.put(CoreAttributes.FILENAME.key(), relativeFile.getName());
                    attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), absPathString);

                    flowFile = session.putAllAttributes(flowFile, attributes);

                    session.getProvenanceReporter().receive(flowFile, transfer.getProtocolName() + "://" + hostname + "/" + file.getFullPathFileName(), millis);
                    session.transfer(flowFile, REL_SUCCESS);
                    logger.info("Successfully retrieved {} from {} in {} milliseconds at a rate of {} and transferred to success", flowFile, hostname, millis, dataRate);

                    flowFilesReceived.put(flowFile, file.getFullPathFileName());
                } catch (final IOException e) {
                    context.yield();
                    logger.error("Unable to retrieve file {}", file.getFullPathFileName(), e);
                    try {
                        transfer.close();
                    } catch (IOException e1) {
                        logger.warn("Unable to close connection to remote host", e1);
                    }

                    session.rollback();
                    return;
                } catch (final FlowFileAccessException e) {
                    context.yield();
                    logger.error("Unable to retrieve file {} due to {}", file.getFullPathFileName(), e.getCause(), e);

                    try {
                        transfer.close();
                    } catch (IOException e1) {
                        logger.warn("Unable to close connection to remote host due to {}", e1);
                    }

                    session.rollback();
                    return;
                } finally {
                    processing.remove(file);
                }
            }

            final FileTransfer fileTransfer = transfer;
            session.commitAsync(() -> {
                if (deleteOriginal) {
                    deleteRemote(fileTransfer, flowFilesReceived);
                }

                closeTransfer(fileTransfer, hostname);
            }, t -> {
                closeTransfer(fileTransfer, hostname);
            });
        } catch (final Throwable t) {
            closeTransfer(transfer, hostname);
        }
    }

    private void deleteRemote(final FileTransfer fileTransfer, final Map<FlowFile, String> flowFileToRemoteFileMapping) {
        for (final Map.Entry<FlowFile, String> entry : flowFileToRemoteFileMapping.entrySet()) {
            final FlowFile receivedFlowFile = entry.getKey();
            final String remoteFilename = entry.getValue();

            try {
                fileTransfer.deleteFile(receivedFlowFile, null, remoteFilename);
            } catch (final IOException e) {
                getLogger().error("Failed to remove remote file {} due to {}. This file may be duplicated in a subsequent run", remoteFilename, e, e);
            }
        }
    }

    private void closeTransfer(final FileTransfer transfer, final String hostname) {
        try {
            transfer.close();
        } catch (final IOException e) {
            getLogger().warn("Failed to close connection to {}", hostname, e);
        }
    }

    protected Map<String, String> getAttributesFromFile(FileInfo info) {
        Map<String, String> attributes = new HashMap<>();
        if (info != null) {
            attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(info.getLastModifiedTime()).atZone(ZoneId.systemDefault())));
            attributes.put(FILE_PERMISSIONS_ATTRIBUTE, info.getPermissions());
            attributes.put(FILE_OWNER_ATTRIBUTE, info.getOwner());
            attributes.put(FILE_GROUP_ATTRIBUTE, info.getGroup());
        }
        return attributes;
    }

    // must be called while holding the listingLock
    private void fetchListing(final ProcessContext context, final ProcessSession session, final FileTransfer transfer) throws IOException {
        BlockingQueue<FileInfo> queue = fileQueueRef.get();
        if (queue == null) {
            final boolean useNaturalOrdering = context.getProperty(FileTransfer.USE_NATURAL_ORDERING).asBoolean();
            queue = useNaturalOrdering ? new PriorityBlockingQueue<>(25000) : new LinkedBlockingQueue<>(25000);
            fileQueueRef.set(queue);
        }

        final StopWatch stopWatch = new StopWatch(true);
        final List<FileInfo> listing = transfer.getListing(true);
        final long millis = stopWatch.getElapsed(TimeUnit.MILLISECONDS);

        int newItems = 0;
        mutuallyExclusiveTransferLock.lock();
        try {
            for (final FileInfo file : listing) {
                if (!queue.contains(file) && !processing.contains(file)) {
                    if (!queue.offer(file)) {
                        break;
                    }
                    newItems++;
                }
            }
        } finally {
            mutuallyExclusiveTransferLock.unlock();
        }

        getLogger().info("Obtained file listing in {} milliseconds; listing had {} items, {} of which were new",
                millis, listing.size(), newItems);
    }
}
