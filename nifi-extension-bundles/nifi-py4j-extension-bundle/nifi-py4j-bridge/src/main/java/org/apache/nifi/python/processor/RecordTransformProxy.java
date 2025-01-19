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

package org.apache.nifi.python.processor;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.NullSuppression;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.JsonParserFactory;
import org.apache.nifi.json.JsonRecordSource;
import org.apache.nifi.json.JsonSchemaInference;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.json.OutputGrouping;
import org.apache.nifi.json.WriteJsonResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.NopSchemaAccessWriter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

@InputRequirement(Requirement.INPUT_REQUIRED)
public class RecordTransformProxy extends PythonProcessorProxy<RecordTransform> {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("Record Reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for reading incoming data")
        .required(true)
        .identifiesControllerService(RecordReaderFactory.class)
        .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("Record Writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing out the records")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();

    private static final JsonParserFactory jsonParserFactory = new JsonParserFactory();

    public RecordTransformProxy(final String processorType, final Supplier<PythonProcessorBridge> bridgeFactory, final boolean initialize) {
        super(processorType, bridgeFactory, initialize);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.addAll(super.getSupportedPropertyDescriptors());
        return properties;
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final Map<RecordGroupingKey, DestinationTuple> destinationTuples = new HashMap<>();
        final AttributeMap attributeMap = new FlowFileAttributeMap(flowFile);

        long recordsRead = 0L;
        long recordsWritten = 0L;

        final RecordTransform transform = getTransform();
        Map<Relationship, List<FlowFile>> flowFilesPerRelationship;
        try (final InputStream in = session.read(flowFile);
             final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

            final RecordSchema recordSchema = reader.getSchema();
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                final WriteJsonResult writeJsonResult = new WriteJsonResult(getLogger(), recordSchema, new NopSchemaAccessWriter(), baos, false, NullSuppression.NEVER_SUPPRESS,
                    OutputGrouping.OUTPUT_ARRAY, null, null, null);

                int writtenSinceFlush = 0;
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    recordsRead++;
                    if (writtenSinceFlush == 0) {
                        writeJsonResult.beginRecordSet();
                    }

                    writeJsonResult.writeRawRecord(record);
                    writtenSinceFlush++;

                    if (baos.size() >= 1_000_000) {
                        writeJsonResult.finishRecordSet();
                        writeJsonResult.flush();
                        final String json = baos.toString();
                        baos.reset();

                        final List<RecordTransformResult> results = transform.transformRecord(json, recordSchema, attributeMap);
                        try {
                            for (final RecordTransformResult result : results) {
                                writeResult(result, destinationTuples, writerFactory, session, flowFile);
                                recordsWritten++;
                            }
                        } finally {
                            results.forEach(RecordTransformResult::free);
                        }

                        writtenSinceFlush = 0;
                    }
                }

                if (writtenSinceFlush > 0) {
                    writeJsonResult.finishRecordSet();
                    writeJsonResult.flush();
                    final String json = baos.toString();
                    baos.reset();

                    final List<RecordTransformResult> results = transform.transformRecord(json, recordSchema, attributeMap);
                    try {
                        for (final RecordTransformResult result : results) {
                            writeResult(result, destinationTuples, writerFactory, session, flowFile);
                            recordsWritten++;
                        }
                    } finally {
                        results.forEach(RecordTransformResult::free);
                    }
                }
            }

            // Update FlowFile attributes, close Record Writers, and map FlowFiles to their appropriate relationships
            flowFilesPerRelationship = mapResults(destinationTuples, session);

            session.adjustCounter("Records Read", recordsRead, false);
            session.adjustCounter("Record Written", recordsWritten, false);
        } catch (final Exception e) {
            getLogger().error("Failed to transform {}; routing to failure", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);

            destinationTuples.values().forEach(tuple -> {
                session.remove(tuple.flowFile());

                try {
                    tuple.writer().close();
                } catch (final IOException ioe) {
                    getLogger().warn("Failed to close Record Writer for FlowFile created in this session", ioe);
                }
            });

            return;
        }

        // Transfer FlowFiles to the appropriate relationships.
        // This must be done outside of the try/catch because we need to close the InputStream before transferring the FlowFile
        flowFilesPerRelationship.forEach((rel, flowFiles) -> session.transfer(flowFiles, rel));
        session.transfer(flowFile, REL_ORIGINAL);
    }


    /**
     * Create mapping of each Relationship to all FlowFiles that go to that Relationship.
     * This gives us a way to efficiently transfer FlowFiles and allows us to ensure that we are able
     * to finish the Record Sets and close the Writers (flushing results, etc.) appropriately before
     * transferring any FlowFiles. This way, if there is any error, we can cleanup easily.
     *
     * @param destinationTuples a mapping of RecordGroupingKey (relationship and optional partition) to a DestinationTuple (FlowFile and RecordSetWriter)
     * @param session the process session
     * @return a mapping of all Relationships to which a FlowFile should be routed to those FlowFiles that are to be routed to the given Relationship
     *
     * @throws IOException if unable to create a RecordSetWriter
     */
    private Map<Relationship, List<FlowFile>> mapResults(final Map<RecordGroupingKey, DestinationTuple> destinationTuples, final ProcessSession session) throws IOException {
        final Map<Relationship, List<FlowFile>> flowFilesPerRelationship = new HashMap<>();
        for (final Map.Entry<RecordGroupingKey, DestinationTuple> entry : destinationTuples.entrySet()) {
            final DestinationTuple destinationTuple = entry.getValue();
            final RecordSetWriter writer = destinationTuple.writer();

            final WriteResult writeResult = writer.finishRecordSet();
            writer.close();

            // Create attribute map
            final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
            attributes.put("mime.type", writer.getMimeType());

            final RecordGroupingKey groupingKey = entry.getKey();
            final Map<String, Object> partition = groupingKey.partition();
            if (partition != null) {
                partition.forEach((key, value) -> attributes.put(key, Objects.toString(value)));
            }

            // Update the FlowFile and add to the appropriate Relationship and grouping
            final FlowFile outputFlowFile = session.putAllAttributes(destinationTuple.flowFile(), attributes);
            final Relationship destinationRelationship = new Relationship.Builder().name(groupingKey.relationship()).build();
            final List<FlowFile> flowFiles = flowFilesPerRelationship.computeIfAbsent(destinationRelationship, key -> new ArrayList<>());
            flowFiles.add(outputFlowFile);
        }

        return flowFilesPerRelationship;
    }

    /**
     * Writes the RecordTransformResult to the appropriate RecordSetWriter
     *
     * @param result the result to write out
     * @param destinationTuples a mapping of RecordGroupingKey (relationship and optional partition) to a DestinationTuple (FlowFile and RecordSetWriter)
     * @param writerFactory RecordSetWriterFactory to use for creating a RecordSetWriter if necessary
     * @param session the ProcessSession
     * @param originalFlowFile the original FlowFile
     *
     * @throws SchemaNotFoundException if unable to find the appropriate schema when attempting to create a new RecordSetWriter
     * @throws IOException if unable to create a new RecordSetWriter
     */
    private void writeResult(final RecordTransformResult result, final Map<RecordGroupingKey, DestinationTuple> destinationTuples,
                             final RecordSetWriterFactory writerFactory, final ProcessSession session, final FlowFile originalFlowFile)
        throws SchemaNotFoundException, IOException, MalformedRecordException {

        final Record transformed = createRecordFromJson(result);
        if (transformed == null) {
            getLogger().debug("Received null result from RecordTransform; will not write result to output for {}", originalFlowFile);
            return;
        }

        // Get the DestinationTuple for the specified relationship
        final RecordGroupingKey key = new RecordGroupingKey(result.getRelationship(), result.getPartition());
        DestinationTuple destinationTuple = destinationTuples.get(key);
        if (destinationTuple == null) {
            final FlowFile destinationFlowFile = session.create(originalFlowFile);

            final RecordSetWriter writer;
            OutputStream out = null;
            try {
                out = session.write(destinationFlowFile);
                final Map<String, String> originalAttributes = originalFlowFile.getAttributes();
                final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, transformed.getSchema());
                writer = writerFactory.createWriter(getLogger(), writeSchema, out, originalAttributes);
                writer.beginRecordSet();
            } catch (final Exception e) {
                // If we failed to create the RecordSetWriter, ensure that we close the Output Stream
                IOUtils.closeQuietly(out);
                session.remove(destinationFlowFile);
                throw e;
            }

            destinationTuple = new DestinationTuple(destinationFlowFile, writer);
            destinationTuples.put(key, destinationTuple);
        }

        // Transform the result into a Record and write it out
        destinationTuple.writer().write(transformed);
    }


    private Record createRecordFromJson(final RecordTransformResult transformResult) throws IOException, MalformedRecordException {
        final String json = transformResult.getRecordJson();
        final byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);

        final RecordSchema returnedSchema = transformResult.getSchema();
        final RecordSchema schema;
        if (returnedSchema == null) {
            schema = inferSchema(jsonBytes);
        } else {
            schema = returnedSchema;
        }

        try (final InputStream in = new ByteArrayInputStream(jsonBytes)) {
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, getLogger(), schema, null, null, null, null,
                    null, null, null, jsonParserFactory);
            final Record record = reader.nextRecord(false, false);
            return record;
        }
    }

    private RecordSchema inferSchema(final byte[] jsonBytes) throws IOException {
        try (final InputStream in = new ByteArrayInputStream(jsonBytes)) {
            final JsonRecordSource recordSource = new JsonRecordSource(in);
            final TimeValueInference timeValueInference = new TimeValueInference(null, null, null);
            final JsonSchemaInference schemaInference = new JsonSchemaInference(timeValueInference);
            return schemaInference.inferSchema(recordSource);
        }
    }


    /**
     * A tuple representing the name of a Relationship to which a Record should be transferred and an optional Partition that may distinguish
     * a Record from other Records going to the same Relationship
     */
    private record RecordGroupingKey(String relationship, Map<String, Object> partition) {

    }

    /**
     * A tuple of a FlowFile and the RecordSetWriter to use for writing to that FlowFile
     */
    private record DestinationTuple(FlowFile flowFile, RecordSetWriter writer) {
    }
}
