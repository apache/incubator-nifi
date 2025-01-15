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
package org.apache.nifi.processors.script;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SideEffectFree
@Tags({"record", "partition", "script", "groovy", "segment", "split", "group", "organize"})
@CapabilityDescription("Receives Record-oriented data (i.e., data that can be read by the configured Record Reader) and evaluates the user provided script against "
        + "each record in the incoming flow file. Each record is then grouped with other records sharing the same partition and a FlowFile is created for each groups of records. " +
        "Two records shares the same partition if the evaluation of the script results the same return value for both. Those will be considered as part of the same partition.")
@Restricted(restrictions = {
        @Restriction(requiredPermission = RequiredPermission.EXECUTE_CODE,
                explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "partition", description = "The partition of the outgoing flow file. If the script indicates that the partition has a null value, the attribute will be set to " +
            "the literal string \"<null partition>\" (without quotes). Otherwise, the attribute is set to the String representation of whatever value is returned by the script."),
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records within the flow file."),
        @WritesAttribute(attribute = "record.error.message", description = "This attribute provides on failure the error message encountered by the Reader or Writer."),
        @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the partitioned FlowFiles that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count", description = "The number of partitioned FlowFiles generated from the parent FlowFile")
})
@SeeAlso(classNames = {
        "org.apache.nifi.processors.script.ScriptedTransformRecord",
        "org.apache.nifi.processors.script.ScriptedValidateRecord",
        "org.apache.nifi.processors.script.ScriptedFilterRecord"
})
public class ScriptedPartitionRecord extends ScriptedRecordProcessor {

    static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully partitioned will be routed to this relationship")
            .build();
    static final Relationship RELATIONSHIP_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Once all records in an incoming FlowFile have been partitioned, the original FlowFile is routed to this relationship.")
            .build();
    static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be partitioned from the configured input format to the configured output format, "
                    + "the unchanged FlowFile will be routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            RELATIONSHIP_ORIGINAL,
            RELATIONSHIP_SUCCESS,
            RELATIONSHIP_FAILURE
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ScriptRunner scriptRunner = pollScriptRunner();
        boolean success;

        try {
            final ScriptEvaluator evaluator;

            try {
                final ScriptEngine scriptEngine = scriptRunner.getScriptEngine();
                evaluator = createEvaluator(scriptEngine, flowFile);
            } catch (final ScriptException se) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Script as executed by NiFi with preloads {}", scriptRunner.getScript());
                }
                getLogger().error("Failed to initialize script engine", se);
                session.transfer(flowFile, RELATIONSHIP_FAILURE);
                return;
            }

            success = partition(context, session, flowFile, evaluator);
        } finally {
            offerScriptRunner(scriptRunner);
        }

        session.transfer(flowFile, success ? RELATIONSHIP_ORIGINAL : RELATIONSHIP_FAILURE);
    }

    private boolean partition(
            final ProcessContext context,
            final ProcessSession session,
            final FlowFile incomingFlowFile,
            final ScriptEvaluator evaluator
    ) {
        final long startMillis = System.currentTimeMillis();

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final Map<String, String> originalAttributes = incomingFlowFile.getAttributes();
        final RecordCounts counts = new RecordCounts();

        try {
            session.read(incomingFlowFile, in -> {
                try (
                    final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, incomingFlowFile.getSize(), getLogger())
                ) {
                    final RecordSchema schema = writerFactory.getSchema(originalAttributes, reader.getSchema());
                    final RecordSet recordSet = reader.createRecordSet();
                    final PushBackRecordSet pushBackSet = new PushBackRecordSet(recordSet);

                    final Map<Object, FlowFile> outgoingFlowFiles = new HashMap<>();
                    final Map<Object, RecordSetWriter> recordSetWriters = new HashMap<>();

                    // Reading in records and evaluate script
                    while (pushBackSet.isAnotherRecord()) {
                        final Record record = pushBackSet.next();
                        final Object evaluatedValue = evaluator.evaluate(record, counts.getRecordCount());
                        getLogger().debug("Evaluated scripted against {} (index {}), producing result of {}", record, counts.getRecordCount(), evaluatedValue);
                        counts.incrementRecordCount();

                        final Object partition = (evaluatedValue instanceof Object[]) ? Arrays.asList((Object[]) evaluatedValue) : evaluatedValue;
                        RecordSetWriter writer = recordSetWriters.get(partition);

                        if (writer == null) {
                            final FlowFile outgoingFlowFile = session.create(incomingFlowFile);
                            final OutputStream out = session.write(outgoingFlowFile);
                            writer = writerFactory.createWriter(getLogger(), schema, out, outgoingFlowFile);

                            writer.beginRecordSet();
                            outgoingFlowFiles.put(partition, outgoingFlowFile);
                            recordSetWriters.put(partition, writer);
                        }

                        writer.write(record);
                    }

                    // Sending outgoing flow files
                    int fragmentIndex = 0;

                    for (final Object partition : outgoingFlowFiles.keySet()) {
                        final RecordSetWriter writer = recordSetWriters.get(partition);
                        final FlowFile outgoingFlowFile = outgoingFlowFiles.get(partition);

                        final Map<String, String> attributes = new HashMap<>(incomingFlowFile.getAttributes());
                        attributes.put("mime.type", writer.getMimeType());
                        attributes.put("partition", partition == null ? "<null partition>" : partition.toString());
                        attributes.put("fragment.index", String.valueOf(fragmentIndex));
                        attributes.put("fragment.count", String.valueOf(outgoingFlowFiles.size()));

                        try {
                            final WriteResult finalResult = writer.finishRecordSet();
                            final int outgoingFlowFileRecords = finalResult.getRecordCount();
                            attributes.put("record.count", String.valueOf(outgoingFlowFileRecords));
                            writer.close();
                        } catch (final IOException e) {
                            throw new ProcessException("Resources used for record writing might not be closed", e);
                        }

                        session.putAllAttributes(outgoingFlowFile, attributes);
                        session.transfer(outgoingFlowFile, RELATIONSHIP_SUCCESS);
                        fragmentIndex++;
                    }

                    final long millis = System.currentTimeMillis() - startMillis;
                    session.adjustCounter("Records Processed", counts.getRecordCount(), true);
                    session.getProvenanceReporter().fork(incomingFlowFile, outgoingFlowFiles.values(), "Processed " + counts.getRecordCount() + " Records", millis);

                } catch (final ScriptException | SchemaNotFoundException | MalformedRecordException e) {
                    throw new ProcessException("After processing " + counts.getRecordCount() +  " Records, encountered failure when attempting to process " + incomingFlowFile, e);
                }
            });

            return true;
        } catch (final Exception e) {
            getLogger().error("Failed to partition records", e);
            return false;
        }
    }
}
