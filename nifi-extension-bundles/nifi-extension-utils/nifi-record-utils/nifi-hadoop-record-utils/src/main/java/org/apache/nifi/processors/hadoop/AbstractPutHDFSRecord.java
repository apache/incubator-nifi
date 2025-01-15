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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.exception.FailureException;
import org.apache.nifi.processors.hadoop.exception.RecordReaderFactoryException;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StopWatch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for processors that write Records to HDFS.
 */
@TriggerWhenEmpty // trigger when empty so we have a chance to perform a Kerberos re-login
@DefaultSettings(yieldDuration = "100 ms") // decrease the default yield since we are triggering when empty
public abstract class AbstractPutHDFSRecord extends AbstractHadoopProcessor {


    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("compression-type")
            .displayName("Compression Type")
            .description("The type of compression for the file being written.")
            .required(true)
            .build();

    public static final PropertyDescriptor OVERWRITE = new PropertyDescriptor.Builder()
            .name("overwrite")
            .displayName("Overwrite Files")
            .description("Whether or not to overwrite existing files in the same directory with the same name. When set to false, " +
                    "flow files will be routed to failure when a file exists in the same directory with the same name.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor UMASK = new PropertyDescriptor.Builder()
            .name("permissions-umask")
            .displayName("Permissions umask")
            .description("A umask represented as an octal number which determines the permissions of files written to HDFS. " +
                    "This overrides the Hadoop Configuration dfs.umaskmode")
            .addValidator(HadoopValidators.UMASK_VALIDATOR)
            .build();

    public static final PropertyDescriptor REMOTE_OWNER = new PropertyDescriptor.Builder()
            .name("remote-owner")
            .displayName("Remote Owner")
            .description("Changes the owner of the HDFS file to this value after it is written. " +
                    "This only works if NiFi is running as a user that has HDFS super user privilege to change owner")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REMOTE_GROUP = new PropertyDescriptor.Builder()
            .name("remote-group")
            .displayName("Remote Group")
            .description("Changes the group of the HDFS file to this value after it is written. " +
                    "This only works if NiFi is running as a user that has HDFS super user privilege to change group")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The service for reading records from incoming flow files.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Flow Files that have been successfully processed are transferred to this relationship")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("Flow Files that could not be processed due to issues that can be retried are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Flow Files that could not be processed due to issue that cannot be retried are transferred to this relationship")
            .build();

    public static final String RECORD_COUNT_ATTR = "record.count";

    private volatile String remoteOwner;
    private volatile String remoteGroup;

    private volatile Set<Relationship> putHdfsRecordRelationships;
    private volatile List<PropertyDescriptor> putHdfsRecordProperties;


    @Override
    protected final void init(final ProcessorInitializationContext context) {
        super.init(context);
        this.putHdfsRecordRelationships = Set.of(
                REL_SUCCESS,
                REL_RETRY,
                REL_FAILURE
        );

        final List<PropertyDescriptor> props = new ArrayList<>(getCommonPropertyDescriptors());
        props.add(RECORD_READER);

        props.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(DIRECTORY)
                .description("The parent directory to which files should be written. Will be created if it doesn't exist.")
                .build());

        final AllowableValue[] compressionTypes = getCompressionTypes(context).toArray(new AllowableValue[0]);

        props.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(COMPRESSION_TYPE)
                .allowableValues(compressionTypes)
                .defaultValue(getDefaultCompressionType(context))
                .build());

        props.add(OVERWRITE);
        props.add(UMASK);
        props.add(REMOTE_GROUP);
        props.add(REMOTE_OWNER);
        props.addAll(getAdditionalProperties());
        this.putHdfsRecordProperties = Collections.unmodifiableList(props);
    }

    /**
     * @param context the initialization context
     * @return the possible compression types
     */
    public abstract List<AllowableValue> getCompressionTypes(final ProcessorInitializationContext context);

    /**
     * @param context the initialization context
     * @return the default compression type
     */
    public abstract String getDefaultCompressionType(final ProcessorInitializationContext context);

    /**
     * Allows sub-classes to add additional properties, called from initialize.
     *
     * @return additional properties to add to the overall list
     */
    public List<PropertyDescriptor> getAdditionalProperties() {
        return Collections.emptyList();
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return putHdfsRecordRelationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
       return putHdfsRecordProperties;
    }

    @Override
    protected void preProcessConfiguration(Configuration config, ProcessContext context) {
        // Set umask once, to avoid thread safety issues doing it in onTrigger
        final PropertyValue umaskProp = context.getProperty(UMASK);
        final short dfsUmask;
        if (umaskProp.isSet()) {
            dfsUmask = Short.parseShort(umaskProp.getValue(), 8);
        } else {
            dfsUmask = FsPermission.DEFAULT_UMASK;
        }

        FsPermission.setUMask(config, new FsPermission(dfsUmask));
    }

    @OnScheduled
    public final void onScheduled(final ProcessContext context) throws IOException {
        super.abstractOnScheduled(context);

        this.remoteOwner = context.getProperty(REMOTE_OWNER).getValue();
        this.remoteGroup = context.getProperty(REMOTE_GROUP).getValue();
    }

    /**
     * Sub-classes provide the appropriate HDFSRecordWriter.
     *
     * @param context the process context to obtain additional configuration
     * @param flowFile the flow file being written
     * @param conf the Configuration instance
     * @param path the path to write to
     * @param schema the schema for writing
     * @return the HDFSRecordWriter
     * @throws IOException if an error occurs creating the writer or processing the schema
     */
    public abstract HDFSRecordWriter createHDFSRecordWriter(
            final ProcessContext context,
            final FlowFile flowFile,
            final Configuration conf,
            final Path path,
            final RecordSchema schema) throws IOException, SchemaNotFoundException;

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // do this before getting a flow file so that we always get a chance to attempt Kerberos relogin
        final FileSystem fileSystem = getFileSystem();
        final Configuration configuration = getConfiguration();
        final UserGroupInformation ugi = getUserGroupInformation();

        if (configuration == null || fileSystem == null || ugi == null) {
            getLogger().error("Processor not configured properly because Configuration, FileSystem, or UserGroupInformation was null");
            context.yield();
            return;
        }

        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            context.yield();
            return;
        }

        ugi.doAs((PrivilegedAction<Object>) () -> {
            Path tempDotCopyFile = null;
            FlowFile putFlowFile = flowFile;
            try {
                final String filenameValue = putFlowFile.getAttribute(CoreAttributes.FILENAME.key()); // TODO codec extension

                // create the directory if it doesn't exist
                final Path directoryPath = getNormalizedPath(context, DIRECTORY, putFlowFile);
                createDirectory(fileSystem, directoryPath, remoteOwner, remoteGroup);

                // write to tempFile first and on success rename to destFile
                final Path tempFile = new Path(directoryPath, "." + filenameValue) {
                    @Override
                    public FileSystem getFileSystem(Configuration conf) throws IOException {
                        return fileSystem;
                    }
                };
                final Path destFile = new Path(directoryPath, filenameValue) {
                    @Override
                    public FileSystem getFileSystem(Configuration conf) throws IOException {
                        return fileSystem;
                    }
                };

                final boolean destinationOrTempExists = fileSystem.exists(destFile) || fileSystem.exists(tempFile);
                final boolean shouldOverwrite = context.getProperty(OVERWRITE).asBoolean();

                // if the tempFile or destFile already exist, and overwrite is set to false, then transfer to failure
                if (destinationOrTempExists && !shouldOverwrite) {
                    session.transfer(session.penalize(putFlowFile), REL_FAILURE);
                    getLogger().warn("penalizing {} and routing to failure because file with same name already exists", putFlowFile);
                    return null;
                }

                final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);
                final AtomicReference<WriteResult> writeResult = new AtomicReference<>();
                final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

                final FlowFile flowFileIn = putFlowFile;
                final StopWatch stopWatch = new StopWatch(true);

                // Read records from the incoming FlowFile and write them the tempFile
                session.read(putFlowFile, (final InputStream in) -> {
                    RecordReader recordReader = null;
                    HDFSRecordWriter recordWriter = null;

                    try {
                        // if we fail to create the RecordReader then we want to route to failure, so we need to
                        // handle this separately from the other IOExceptions which normally route to retry
                        try {
                            recordReader = recordReaderFactory.createRecordReader(flowFileIn, in, getLogger());
                        } catch (Exception e) {
                            final RecordReaderFactoryException rrfe = new RecordReaderFactoryException("Unable to create RecordReader", e);
                            exceptionHolder.set(rrfe);
                            return;
                        }

                        final RecordSet recordSet = recordReader.createRecordSet();

                        recordWriter = createHDFSRecordWriter(context, flowFile, configuration, tempFile, recordReader.getSchema());
                        writeResult.set(recordWriter.write(recordSet));
                    } catch (Exception e) {
                        exceptionHolder.set(e);
                    } finally {
                        IOUtils.closeQuietly(recordReader);
                        IOUtils.closeQuietly(recordWriter);
                    }
                });
                stopWatch.stop();

                final String dataRate = stopWatch.calculateDataRate(putFlowFile.getSize());
                final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                tempDotCopyFile = tempFile;

                // if any errors happened within the session.read then throw the exception so we jump
                // into one of the appropriate catch blocks below
                if (exceptionHolder.get() != null) {
                    throw exceptionHolder.get();
                }

                final boolean destinationExists = fileSystem.exists(destFile);

                // If destination file already exists, resolve that based on processor configuration
                if (destinationExists && shouldOverwrite) {
                    if (fileSystem.delete(destFile, false)) {
                        getLogger().info("deleted {} in order to replace with the contents of {}", destFile, putFlowFile);
                    }
                }

                // Attempt to rename from the tempFile to destFile, and change owner if successfully renamed
                rename(fileSystem, tempFile, destFile);
                changeOwner(fileSystem, destFile, remoteOwner, remoteGroup);

                getLogger().info("Wrote {} to {} in {} milliseconds at a rate of {}", putFlowFile, destFile, millis, dataRate);

                putFlowFile = postProcess(context, session, putFlowFile, destFile);

                final String newFilename = destFile.getName();
                final String hdfsPath = destFile.getParent().toString();

                // Update the filename and absolute path attributes
                final Map<String, String> attributes = new HashMap<>(writeResult.get().getAttributes());
                attributes.put(CoreAttributes.FILENAME.key(), newFilename);
                attributes.put(ABSOLUTE_HDFS_PATH_ATTRIBUTE, hdfsPath);
                attributes.put(RECORD_COUNT_ATTR, String.valueOf(writeResult.get().getRecordCount()));
                putFlowFile = session.putAllAttributes(putFlowFile, attributes);

                // Send a provenance event and transfer to success
                final Path qualifiedPath = destFile.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
                putFlowFile = session.putAttribute(putFlowFile, HADOOP_FILE_URL_ATTRIBUTE, qualifiedPath.toString());
                session.getProvenanceReporter().send(putFlowFile, qualifiedPath.toString());
                session.transfer(putFlowFile, REL_SUCCESS);

            } catch (IOException | FlowFileAccessException e) {
                deleteQuietly(fileSystem, tempDotCopyFile);
                getLogger().error("Failed to write", e);
                session.transfer(session.penalize(putFlowFile), REL_RETRY);
                context.yield();
            } catch (Throwable t) {
                deleteQuietly(fileSystem, tempDotCopyFile);
                getLogger().error("Failed to write", t);
                session.transfer(putFlowFile, REL_FAILURE);
            }

            return null;
        });
    }

    /**
     * This method will be called after successfully writing to the destination file and renaming the file to it's final name
     * in order to give sub-classes a chance to take action before transferring to success.
     *
     * @param context the context
     * @param session the session
     * @param flowFile the flow file being processed
     * @param destFile the destination file written to
     * @return an updated FlowFile reference
     */
    protected FlowFile postProcess(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Path destFile) {
        return flowFile;
    }

    /**
     * Attempts to rename srcFile to destFile up to 10 times, with a 200ms sleep in between each attempt.
     *
     * If the file has not been renamed after 10 attempts, a FailureException is thrown.
     *
     * @param fileSystem the file system where the files are located
     * @param srcFile the source file
     * @param destFile the destination file to rename the source to
     * @throws IOException if IOException happens while attempting to rename
     * @throws InterruptedException if renaming is interrupted
     * @throws FailureException if the file couldn't be renamed after 10 attempts
     */
    protected void rename(final FileSystem fileSystem, final Path srcFile, final Path destFile) throws IOException, InterruptedException, FailureException {
        boolean renamed = false;
        for (int i = 0; i < 10; i++) { // try to rename multiple times.
            if (fileSystem.rename(srcFile, destFile)) {
                renamed = true;
                break; // rename was successful
            }
            Thread.sleep(200L); // try waiting to let whatever might cause rename failure to resolve
        }
        if (!renamed) {
            fileSystem.delete(srcFile, false);
            throw new FailureException("Could not rename file " + srcFile + " to its final filename");
        }
    }

    /**
     * Deletes the given file from the given filesystem. Any exceptions that are encountered will be caught and logged, but not thrown.
     *
     * @param fileSystem the filesystem where the file exists
     * @param file the file to delete
     */
    protected void deleteQuietly(final FileSystem fileSystem, final Path file) {
        if (file != null) {
            try {
                fileSystem.delete(file, false);
            } catch (Exception e) {
                getLogger().error("Unable to remove file {}", file, e);
            }
        }
    }

    /**
     * Changes the ownership of the given file.
     *
     * @param fileSystem the filesystem where the file exists
     * @param path the file to change ownership on
     * @param remoteOwner the new owner for the file
     * @param remoteGroup the new group for the file
     */
    protected void changeOwner(final FileSystem fileSystem, final Path path, final String remoteOwner, final String remoteGroup) {
        try {
            // Change owner and group of file if configured to do so
            if (remoteOwner != null || remoteGroup != null) {
                fileSystem.setOwner(path, remoteOwner, remoteGroup);
            }
        } catch (Exception e) {
            getLogger().warn("Could not change owner or group of {} on", path, e);
        }
    }

    /**
     * Creates the given directory and changes the ownership to the specified owner/group.
     *
     * @param fileSystem the filesystem to create the directory on
     * @param directory the directory to create
     * @param remoteOwner the owner for changing ownership of the directory
     * @param remoteGroup the group for changing ownership of the directory
     * @throws IOException if an error occurs obtaining the file status or issuing the mkdir command
     * @throws FailureException if the directory could not be created
     */
    protected void createDirectory(final FileSystem fileSystem, final Path directory, final String remoteOwner, final String remoteGroup) throws IOException, FailureException {
        try {
            if (!fileSystem.getFileStatus(directory).isDirectory()) {
                throw new FailureException(directory.toString() + " already exists and is not a directory");
            }
        } catch (FileNotFoundException fe) {
            if (!fileSystem.mkdirs(directory)) {
                throw new FailureException(directory.toString() + " could not be created");
            }
            changeOwner(fileSystem, directory, remoteOwner, remoteGroup);
        }
    }

}
