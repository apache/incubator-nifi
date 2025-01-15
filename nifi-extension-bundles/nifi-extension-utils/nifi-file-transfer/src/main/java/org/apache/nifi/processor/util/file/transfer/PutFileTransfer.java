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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for PutFTP & PutSFTP
 *
 * @param <T> type of transfer
 */
public abstract class PutFileTransfer<T extends FileTransfer> extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully sent will be routed to success")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to the remote system; failure is usually looped back to this processor")
            .build();
    public static final Relationship REL_REJECT = new Relationship.Builder()
            .name("reject")
            .description("FlowFiles that were rejected by the destination system")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_REJECT
    );

    public PutFileTransfer() {
        super();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    protected abstract T getFileTransfer(final ProcessContext context);

    protected void beforePut(final FlowFile flowFile, final ProcessContext context, final T transfer) throws IOException {

    }

    protected void afterPut(final FlowFile flowFile, final ProcessContext context, final T transfer) throws IOException {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        String hostname = context.getProperty(FileTransfer.HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();

        final int maxNumberOfFiles = context.getProperty(FileTransfer.BATCH_SIZE).asInteger();
        int fileCount = 0;
        try (final T transfer = getFileTransfer(context)) {
            do {
                //evaluate again inside the loop as each flowfile can have a different hostname
                hostname = context.getProperty(FileTransfer.HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
                final String rootPath = context.getProperty(FileTransfer.REMOTE_PATH).evaluateAttributeExpressions(flowFile).getValue();
                final String workingDirPath;
                if (StringUtils.isBlank(rootPath)) {
                    workingDirPath = transfer.getHomeDirectory(flowFile);
                } else {
                    workingDirPath = transfer.getAbsolutePath(flowFile, rootPath);
                }

                final boolean rejectZeroByteFiles = context.getProperty(FileTransfer.REJECT_ZERO_BYTE).asBoolean();
                final ConflictResult conflictResult
                        = identifyAndResolveConflictFile(context.getProperty(FileTransfer.CONFLICT_RESOLUTION).getValue(), transfer, workingDirPath, flowFile, rejectZeroByteFiles, logger);

                if (conflictResult.isTransfer()) {
                    final StopWatch stopWatch = new StopWatch();
                    stopWatch.start();

                    beforePut(flowFile, context, transfer);
                    final FlowFile flowFileToTransfer = flowFile;
                    final AtomicReference<String> fullPathRef = new AtomicReference<>(null);
                    session.read(flowFile, in -> {
                        try (final InputStream bufferedIn = new BufferedInputStream(in)) {
                            if (workingDirPath != null && context.getProperty(FileTransfer.CREATE_DIRECTORY).asBoolean()) {
                                transfer.ensureDirectoryExists(flowFileToTransfer, new File(workingDirPath));
                            }

                            fullPathRef.set(transfer.put(flowFileToTransfer, workingDirPath, conflictResult.getFileName(), bufferedIn));
                        }
                    });
                    afterPut(flowFile, context, transfer);

                    stopWatch.stop();
                    final String dataRate = stopWatch.calculateDataRate(flowFile.getSize());
                    final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                    logger.info("Successfully transferred {} to {} on remote host {} in {} milliseconds at a rate of {}",
                            flowFile, fullPathRef.get(), hostname, millis, dataRate);

                    String fullPathWithSlash = fullPathRef.get();
                    if (!fullPathWithSlash.startsWith("/")) {
                        fullPathWithSlash = "/" + fullPathWithSlash;
                    }
                    final String destinationUri = transfer.getProtocolName() + "://" + hostname + fullPathWithSlash;
                    session.getProvenanceReporter().send(flowFile, destinationUri, millis);
                }

                if (conflictResult.isPenalize()) {
                    flowFile = session.penalize(flowFile);
                }

                session.transfer(flowFile, conflictResult.getRelationship());
                session.commitAsync();
            } while (isScheduled()
                    && (getRelationships().size() == context.getAvailableRelationships().size())
                    && (++fileCount < maxNumberOfFiles)
                    && ((flowFile = session.get()) != null));
        } catch (final IOException e) {
            context.yield();
            logger.error("Unable to transfer {} to remote host {} ", flowFile, hostname, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        } catch (final FlowFileAccessException e) {
            context.yield();
            logger.error("Unable to transfer {} to remote host {}", flowFile, hostname, e.getCause());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        } catch (final ProcessException e) {
            context.yield();
            logger.error("Routing to failure since unable to transfer {} to remote host {}", flowFile, hostname, e.getCause());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    //Attempts to identify naming or content issues with files before they are transferred.
    private ConflictResult identifyAndResolveConflictFile(
            final String conflictResolutionType,
            final T transfer,
            final String path,
            final FlowFile flowFile,
            final boolean rejectZeroByteFiles,
            final ComponentLog logger)
            throws IOException {
        Relationship destinationRelationship = REL_SUCCESS;
        String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        boolean transferFile = true;
        boolean penalizeFile = false;

        //First, check if the file is empty
        //Reject files that are zero bytes or less
        if (rejectZeroByteFiles) {
            final long sizeInBytes = flowFile.getSize();
            if (sizeInBytes == 0) {
                logger.warn("Rejecting {} because it is zero bytes", flowFile);
                return new ConflictResult(REL_REJECT, false, fileName, true);
            }
        }

        //Second, check if the user doesn't care about detecting naming conflicts ahead of time
        if (conflictResolutionType.equalsIgnoreCase(FileTransfer.CONFLICT_RESOLUTION_NONE)) {
            return new ConflictResult(destinationRelationship, transferFile, fileName, penalizeFile);
        }

        final FileInfo remoteFileInfo = transfer.getRemoteFileInfo(flowFile, path, fileName);
        if (remoteFileInfo == null) {
            return new ConflictResult(destinationRelationship, transferFile, fileName, penalizeFile);
        }

        if (remoteFileInfo.isDirectory()) {
            logger.warn("Resolving conflict by rejecting {} due to conflicting filename with a directory or file already on remote server", flowFile);
            return new ConflictResult(REL_REJECT, false, fileName, false);
        }

        logger.info("Discovered a filename conflict on the remote server for {} so handling using configured Conflict Resolution of {}", flowFile, conflictResolutionType);

        switch (conflictResolutionType.toUpperCase()) {
            case FileTransfer.CONFLICT_RESOLUTION_REJECT:
                destinationRelationship = REL_REJECT;
                transferFile = false;
                penalizeFile = false;
                logger.warn("Resolving conflict by rejecting {} due to conflicting filename with a directory or file already on remote server", flowFile);
                break;
            case FileTransfer.CONFLICT_RESOLUTION_REPLACE:
                destinationRelationship = REL_SUCCESS;
                transferFile = true;
                penalizeFile = false;
                logger.info("Resolving filename conflict for {} with remote server by deleting remote file and replacing with flow file", flowFile);
                break;
            case FileTransfer.CONFLICT_RESOLUTION_RENAME:
                boolean uniqueNameGenerated = false;
                for (int i = 1; i < 100 && !uniqueNameGenerated; i++) {
                    String possibleFileName = i + "." + fileName;

                    final FileInfo renamedFileInfo = transfer.getRemoteFileInfo(flowFile, path, possibleFileName);
                    uniqueNameGenerated = (renamedFileInfo == null);
                    if (uniqueNameGenerated) {
                        fileName = possibleFileName;
                        logger.info("Attempting to resolve filename conflict for {} on the remote server by using a newly generated filename of: {}", flowFile, fileName);
                        destinationRelationship = REL_SUCCESS;
                        transferFile = true;
                        penalizeFile = false;
                        break;
                    }
                }
                if (!uniqueNameGenerated) {
                    destinationRelationship = REL_REJECT;
                    transferFile = false;
                    penalizeFile = false;
                    logger.warn("Could not determine a unique name after 99 attempts for.  Switching resolution mode to REJECT for {}", flowFile);
                }
                break;
            case FileTransfer.CONFLICT_RESOLUTION_IGNORE:
                destinationRelationship = REL_SUCCESS;
                transferFile = false;
                penalizeFile = false;
                logger.info("Resolving conflict for {}  by not transferring file and and still considering the process a success.", flowFile);
                break;
            case FileTransfer.CONFLICT_RESOLUTION_FAIL:
                destinationRelationship = REL_FAILURE;
                transferFile = false;
                penalizeFile = true;
                logger.warn("Resolved filename conflict for {} as configured by routing to FAILURE relationship.", flowFile);
            default:
                break;
        }

        return new ConflictResult(destinationRelationship, transferFile, fileName, penalizeFile);
    }

    /**
     * static inner class to hold conflict data
     */
    private static class ConflictResult {

        final Relationship relationship;
        final boolean transferFile;
        final String newFileName;
        final boolean penalizeFile;

        public ConflictResult(final Relationship relationship, final boolean transferFileVal, final String newFileNameVal, final boolean penalizeFileVal) {
            this.relationship = relationship;
            this.transferFile = transferFileVal;
            this.newFileName = newFileNameVal;
            this.penalizeFile = penalizeFileVal;
        }

        public boolean isTransfer() {
            return transferFile;
        }

        public boolean isPenalize() {
            return penalizeFile;
        }

        public String getFileName() {
            return newFileName;
        }

        public Relationship getRelationship() {
            return relationship;
        }
    }
}
