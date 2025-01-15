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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.record.sink.RetryableIOException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"record", "put", "sink"})
@CapabilityDescription("The PutRecord processor uses a specified RecordReader to input (possibly multiple) records from an incoming flow file, and sends them "
        + "to a destination specified by a Record Destination Service (i.e. record sink).")
public class PutRecord extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("put-record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_SINK = new PropertyDescriptor.Builder()
            .name("put-record-sink")
            .displayName("Record Destination Service")
            .description("Specifies the Controller Service to use for writing out the query result records to some destination.")
            .identifiesControllerService(RecordSinkService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor INCLUDE_ZERO_RECORD_RESULTS = new PropertyDescriptor.Builder()
            .name("put-record-include-zero-record-results")
            .displayName("Include Zero Record Results")
            .description("If no records are read from the incoming FlowFile, this property specifies whether or not an empty record set will be transmitted. The original "
                    + "FlowFile will still be routed to success, but if no transmission occurs, no provenance SEND event will be generated.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_READER,
            RECORD_SINK,
            INCLUDE_ZERO_RECORD_RESULTS
    );

    // Relationships
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The original FlowFile will be routed to this relationship if the records were transmitted successfully")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("The original FlowFile is routed to this relationship if the records could not be transmitted but attempting the operation again may succeed")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the records could not be transmitted and retrying the operation will also fail")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_RETRY
    );

    private volatile RecordSinkService recordSinkService;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        recordSinkService = context.getProperty(RECORD_SINK).asControllerService(RecordSinkService.class);
        recordSinkService.reset();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final StopWatch stopWatch = new StopWatch(true);

        RecordSet recordSet;
        try (final InputStream in = session.read(flowFile)) {

            final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER)
                    .asControllerService(RecordReaderFactory.class);
            final RecordReader recordParser = recordParserFactory.createRecordReader(flowFile, in, getLogger());
            recordSet = recordParser.createRecordSet();

            final boolean transmitZeroRecords = context.getProperty(INCLUDE_ZERO_RECORD_RESULTS).asBoolean();
            final WriteResult writeResult = recordSinkService.sendData(recordSet, new HashMap<>(flowFile.getAttributes()), transmitZeroRecords);
            String recordSinkURL = writeResult.getAttributes().get("record.sink.url");
            if (StringUtils.isEmpty(recordSinkURL)) {
                recordSinkURL = "unknown://";
            }

            final long transmissionMillis = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            // Only record provenance if we sent any records
            if (writeResult.getRecordCount() > 0 || transmitZeroRecords) {
                session.getProvenanceReporter().send(flowFile, recordSinkURL, transmissionMillis);
            }

        } catch (RetryableIOException rioe) {
            getLogger().warn("Error during transmission of records due to {}, routing to retry", rioe.getMessage(), rioe);
            session.transfer(flowFile, REL_RETRY);
            return;
        } catch (SchemaNotFoundException snfe) {
            throw new ProcessException("Error determining schema of flowfile records: " + snfe.getMessage(), snfe);
        } catch (MalformedRecordException e) {
            getLogger().error("Error reading records from {} due to {}, routing to failure", flowFile, e.getMessage(), e);
            session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (IOException ioe) {
            // The cause might be a MalformedRecordException (RecordReader wraps it in an IOException), send to failure in that case
            if (ioe.getCause() instanceof MalformedRecordException) {
                getLogger().error("Error reading records from {} due to {}, routing to failure", flowFile, ioe.getMessage(), ioe);
                session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
            throw new ProcessException("Error reading from flowfile input stream: " + ioe.getMessage(), ioe);
        } catch (Exception e) {
            getLogger().error("Error during transmission of records due to {}, routing to failure", e.getMessage(), e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        session.transfer(flowFile, REL_SUCCESS);
    }
}
