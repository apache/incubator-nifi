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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@SupportsBatching
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Restricted(restrictions = {
        @Restriction(requiredPermission = RequiredPermission.EXECUTE_CODE,
                explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records within the flow file."),
        @WritesAttribute(attribute = "record.error.message", description = "This attribute provides on failure the error message encountered by the Reader or Writer.")
})
public abstract class ScriptedRouterProcessor<T> extends ScriptedRecordProcessor {
    private final Class<T> scriptResultType;

    /**
     * @param scriptResultType Defines the expected result type of the user-provided script.
     */
    protected ScriptedRouterProcessor(final Class<T> scriptResultType) {
        this.scriptResultType = scriptResultType;
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
        boolean success = false;

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
                session.transfer(flowFile, getFailureRelationship());
                return;
            }

            success = route(context, session, flowFile, evaluator);
        } finally {
            offerScriptRunner(scriptRunner);
        }

        session.transfer(flowFile, success ? getOriginalRelationship() : getFailureRelationship());
    }

    private boolean route(
        final ProcessContext context,
        final ProcessSession session,
        final FlowFile incomingFlowFile,
        final ScriptEvaluator evaluator
    ) {
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
                    final Map<Relationship, FlowFile> outgoingFlowFiles = new HashMap<>();
                    final Map<Relationship, RecordSetWriter> recordSetWriters = new HashMap<>();

                    // Reading in records and evaluate script
                    while (pushBackSet.isAnotherRecord()) {
                        final Record record = pushBackSet.next();
                        final Object evaluatedValue = evaluator.evaluate(record, counts.getRecordCount());
                        getLogger().debug("Evaluated scripted against {} (index {}), producing result of {}", record, counts.getRecordCount(), evaluatedValue);
                        counts.incrementRecordCount();

                        if (evaluatedValue != null && scriptResultType.isInstance(evaluatedValue)) {
                            final Optional<Relationship> outgoingRelationship = resolveRelationship(scriptResultType.cast(evaluatedValue));

                            if (outgoingRelationship.isPresent()) {
                                if (!outgoingFlowFiles.containsKey(outgoingRelationship.get())) {
                                    final FlowFile outgoingFlowFile = session.create(incomingFlowFile);
                                    final OutputStream out = session.write(outgoingFlowFile);
                                    final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, outgoingFlowFile);
                                    writer.beginRecordSet();

                                    outgoingFlowFiles.put(outgoingRelationship.get(), outgoingFlowFile);
                                    recordSetWriters.put(outgoingRelationship.get(), writer);
                                }

                                recordSetWriters.get(outgoingRelationship.get()).write(record);
                            } else {
                                getLogger().debug("Record with evaluated value {} has no outgoing relationship determined", String.valueOf(evaluatedValue));
                            }
                        } else {
                            throw new ProcessException("Script returned a value of " + evaluatedValue
                                    + " but this Processor requires that the object returned be an instance of " + scriptResultType.getSimpleName());
                        }
                    }

                    // Sending outgoing flow files
                    for (final Relationship relationship : outgoingFlowFiles.keySet()) {
                        final RecordSetWriter writer = recordSetWriters.get(relationship);
                        final FlowFile outgoingFlowFile = outgoingFlowFiles.get(relationship);

                        final Map<String, String> attributes = new HashMap<>(incomingFlowFile.getAttributes());
                        attributes.put("mime.type", writer.getMimeType());

                        try {
                            final WriteResult finalResult = writer.finishRecordSet();
                            final int outgoingFlowFileRecords = finalResult.getRecordCount();
                            attributes.put("record.count", String.valueOf(outgoingFlowFileRecords));
                            writer.close();
                        } catch (final IOException e) {
                            throw new ProcessException("Resources used for record writing might not be closed", e);
                        }

                        session.putAllAttributes(outgoingFlowFile, attributes);
                        session.transfer(outgoingFlowFile, relationship);
                    }
                } catch (final ScriptException | SchemaNotFoundException | MalformedRecordException e) {
                    throw new ProcessException("After processing " + counts.getRecordCount() +  " Records, encountered failure when attempting to process " + incomingFlowFile, e);
                }
            });

            session.adjustCounter("Records Processed", counts.getRecordCount(), true);
            return true;
        } catch (final Exception e) {
            getLogger().error("Failed to route records", e);
            return false;
        }
    }

    /**
     * @return Returns with the relationship used for route the incoming FlowFile in case of successful processing.
     */
    protected abstract Relationship getOriginalRelationship();

    /**
     * @return Returns with the relationship used for route the incoming FlowFile in case of unsuccessful processing.
     */
    protected abstract Relationship getFailureRelationship();

    /**
     * Returns a relationship based on the script's result value. As the script uses a given record as input, this helps
     * to dissolve the result value for the routing.
     *
     * @param scriptResult The value returned by the script.
     *
     * @return Returns with a relationship if there is one to determine based on the value. If it is not possible to determine
     * an {code Optional#empty} is expected. Records with empty relationship will not be routed into any relationship (except for
     * the original or failed).
     */
    protected abstract Optional<Relationship> resolveRelationship(final T scriptResult);
}
