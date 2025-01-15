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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.record.HDFSRecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StopWatch;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
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
 * Base processor for reading a data from HDFS that can be fetched into records.
 */
@TriggerWhenEmpty // trigger when empty so we have a chance to perform a Kerberos re-login
@DefaultSettings(yieldDuration = "100 ms") // decrease the default yield since we are triggering when empty
public abstract class AbstractFetchHDFSRecord extends AbstractHadoopProcessor {

    public static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
            .name("filename")
            .displayName("Filename")
            .description("The name of the file to retrieve")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${path}/${filename}")
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The service for writing records to the FlowFile content")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles will be routed to this relationship once they have been updated with the content of the file")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles will be routed to this relationship if the content of the file cannot be retrieved and trying again will likely not be helpful. "
                    + "This would occur, for instance, if the file is not found or if there is a permissions issue")
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("FlowFiles will be routed to this relationship if the content of the file cannot be retrieved, but might be able to be in the future if tried again. "
                    + "This generally indicates that the Fetch should be tried again.")
            .build();

    public static final String FETCH_FAILURE_REASON_ATTR = "fetch.failure.reason";
    public static final String RECORD_COUNT_ATTR = "record.count";

    private volatile Set<Relationship> fetchHdfsRecordRelationships;
    private volatile List<PropertyDescriptor> fetchHdfsRecordProperties;

    @Override
    protected final void init(final ProcessorInitializationContext context) {
        super.init(context);

        this.fetchHdfsRecordRelationships = Set.of(
                REL_SUCCESS,
                REL_RETRY,
                REL_FAILURE
        );

        final List<PropertyDescriptor> props = new ArrayList<>(getCommonPropertyDescriptors());
        props.add(FILENAME);
        props.add(RECORD_WRITER);
        props.addAll(getAdditionalProperties());
        this.fetchHdfsRecordProperties = Collections.unmodifiableList(props);
    }

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
        return fetchHdfsRecordRelationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return fetchHdfsRecordProperties;
    }

    /**
     * Sub-classes provide the appropriate HDFSRecordReader.
     *
     * @param context the process context to obtain additional configuration
     * @param flowFile the flow file being written
     * @param conf the Configuration instance
     * @param path the path to write to
     * @return the HDFSRecordWriter
     * @throws IOException if an error occurs creating the writer
     */
    public abstract HDFSRecordReader createHDFSRecordReader(final ProcessContext context, final FlowFile flowFile, final Configuration conf, final Path path)
            throws IOException;

    @SuppressWarnings("PMD.IdenticalCatchBranches")
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

        final FlowFile originalFlowFile = session.get();
        if (originalFlowFile == null ) {
            context.yield();
            return;
        }


        ugi.doAs((PrivilegedAction<Object>) () -> {
            FlowFile child = null;
            final String filenameValue = context.getProperty(FILENAME).evaluateAttributeExpressions(originalFlowFile).getValue();
            try {
                final Path path = getNormalizedPath(context, FILENAME, originalFlowFile);
                final AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);
                final AtomicReference<WriteResult> writeResult = new AtomicReference<>();

                final RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

                final StopWatch stopWatch = new StopWatch(true);

                // use a child FlowFile so that if any error occurs we can route the original untouched FlowFile to retry/failure
                child = session.create(originalFlowFile);

                final AtomicReference<String> mimeTypeRef = new AtomicReference<>();
                child = session.write(child, (final OutputStream rawOut) -> {
                    try (final BufferedOutputStream out = new BufferedOutputStream(rawOut);
                         final HDFSRecordReader recordReader = createHDFSRecordReader(context, originalFlowFile, configuration, path)) {

                        Record record = recordReader.nextRecord();
                        final RecordSchema schema = recordSetWriterFactory.getSchema(originalFlowFile.getAttributes(),
                                record == null ? null : record.getSchema());

                        try (final RecordSetWriter recordSetWriter = recordSetWriterFactory.createWriter(getLogger(), schema, out, originalFlowFile)) {
                            recordSetWriter.beginRecordSet();
                            if (record != null) {
                                recordSetWriter.write(record);
                            }

                            while ((record = recordReader.nextRecord()) != null) {
                                recordSetWriter.write(record);
                            }

                            writeResult.set(recordSetWriter.finishRecordSet());
                            mimeTypeRef.set(recordSetWriter.getMimeType());
                        }
                    } catch (Exception e) {
                        exceptionHolder.set(e);
                    }
                });

                stopWatch.stop();

                // if any errors happened within the session.write then throw the exception so we jump
                // into one of the appropriate catch blocks below
                if (exceptionHolder.get() != null) {
                    throw exceptionHolder.get();
                }

                FlowFile successFlowFile = postProcess(context, session, child, path);

                final Map<String, String> attributes = new HashMap<>(writeResult.get().getAttributes());
                attributes.put(RECORD_COUNT_ATTR, String.valueOf(writeResult.get().getRecordCount()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), mimeTypeRef.get());
                successFlowFile = session.putAllAttributes(successFlowFile, attributes);


                final Path qualifiedPath = path.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
                successFlowFile = session.putAttribute(successFlowFile, HADOOP_FILE_URL_ATTRIBUTE, qualifiedPath.toString());
                getLogger().info("Successfully received content from {} for {} in {} milliseconds", qualifiedPath, successFlowFile, stopWatch.getDuration());
                session.getProvenanceReporter().fetch(successFlowFile, qualifiedPath.toString(), stopWatch.getDuration(TimeUnit.MILLISECONDS));
                session.transfer(successFlowFile, REL_SUCCESS);
                session.remove(originalFlowFile);
                return null;

            } catch (final FileNotFoundException | AccessControlException e) {
                getLogger().error("Routing to failure since failed to retrieve content from {} for {}", filenameValue, originalFlowFile, e);
                final FlowFile failureFlowFile = session.putAttribute(originalFlowFile, FETCH_FAILURE_REASON_ATTR, e.getMessage() == null ? e.toString() : e.getMessage());
                session.transfer(failureFlowFile, REL_FAILURE);
            } catch (final IOException | FlowFileAccessException e) {
                getLogger().error("Routing to retry since failed to retrieve content from {} for {}", filenameValue, originalFlowFile, e);
                session.transfer(session.penalize(originalFlowFile), REL_RETRY);
                context.yield();
            } catch (final Throwable t) {
                getLogger().error("Routing to failure since failed to retrieve content from {} for {}", filenameValue, originalFlowFile, t);
                final FlowFile failureFlowFile = session.putAttribute(originalFlowFile, FETCH_FAILURE_REASON_ATTR, t.getMessage() == null ? t.toString() : t.getMessage());
                session.transfer(failureFlowFile, REL_FAILURE);
            }

            // if we got this far then we weren't successful so we need to clean up the child flow file if it got initialized
            if (child != null) {
                session.remove(child);
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
     * @param fetchPath the path that was fetched
     * @return an updated FlowFile reference
     */
    protected FlowFile postProcess(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Path fetchPath) {
        return flowFile;
    }
}
