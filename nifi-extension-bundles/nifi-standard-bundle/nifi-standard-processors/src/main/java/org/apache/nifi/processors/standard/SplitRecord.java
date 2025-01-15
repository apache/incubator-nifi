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
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
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

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"split", "generic", "schema", "json", "csv", "avro", "log", "logs", "freeform", "text"})
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer for the FlowFiles routed to the 'splits' Relationship."),
    @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile. This is added to FlowFiles that are routed to the 'splits' Relationship."),
    @WritesAttribute(attribute = "fragment.identifier", description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
    @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
    @WritesAttribute(attribute = "fragment.count", description = "The number of split FlowFiles generated from the parent FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})
@CapabilityDescription("Splits up an input FlowFile that is in a record-oriented data format into multiple smaller FlowFiles")
public class SplitRecord extends AbstractProcessor {

    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("Record Reader")
        .description("Specifies the Controller Service to use for reading incoming data")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("Record Writer")
        .description("Specifies the Controller Service to use for writing out the records")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor RECORDS_PER_SPLIT = new PropertyDescriptor.Builder()
        .name("Records Per Split")
        .description("Specifies how many records should be written to each 'split' or 'segment' FlowFile")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_READER,
            RECORD_WRITER,
            RECORDS_PER_SPLIT
    );

    static final Relationship REL_SPLITS = new Relationship.Builder()
        .name("splits")
        .description("The individual 'segments' of the original FlowFile will be routed to this relationship.")
        .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("Upon successfully splitting an input FlowFile, the original FlowFile will be sent to this relationship.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
            + "the unchanged FlowFile will be routed to this relationship.")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SPLITS,
            REL_ORIGINAL,
            REL_FAILURE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final int maxRecords = Math.max(1, context.getProperty(RECORDS_PER_SPLIT).evaluateAttributeExpressions(original).asInteger());

        final List<FlowFile> splits = new ArrayList<>();
        final Map<String, String> originalAttributes = original.getAttributes();
        final String fragmentId = UUID.randomUUID().toString();
        try {
            session.read(original, in -> {
                try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, original.getSize(), getLogger())) {

                    final RecordSchema schema = writerFactory.getSchema(originalAttributes, reader.getSchema());

                    final RecordSet recordSet = reader.createRecordSet();
                    final PushBackRecordSet pushbackSet = new PushBackRecordSet(recordSet);

                    int fragmentIndex = 0;
                    while (pushbackSet.isAnotherRecord()) {
                        FlowFile split = session.create(original);

                        try {
                            final Map<String, String> attributes = new HashMap<>();
                            final WriteResult writeResult;

                            try (final OutputStream out = session.write(split);
                                final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, split)) {
                                    if (maxRecords == 1) {
                                        final Record record = pushbackSet.next();
                                        writeResult = writer.write(record);
                                    } else {
                                        final RecordSet limitedSet = pushbackSet.limit(maxRecords);
                                        writeResult = writer.write(limitedSet);
                                    }

                                    attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                                    attributes.put(FRAGMENT_INDEX, String.valueOf(fragmentIndex));
                                    attributes.put(FRAGMENT_ID, fragmentId);
                                    attributes.put(SEGMENT_ORIGINAL_FILENAME, original.getAttribute(CoreAttributes.FILENAME.key()));
                                    attributes.putAll(writeResult.getAttributes());

                                    session.adjustCounter("Records Split", writeResult.getRecordCount(), false);
                            }

                            split = session.putAllAttributes(split, attributes);
                        } finally {
                            splits.add(split);
                        }
                        fragmentIndex++;
                    }
                } catch (final SchemaNotFoundException | MalformedRecordException e) {
                    throw new ProcessException("Failed to parse incoming data", e);
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to split {}", original, pe);
            session.remove(splits);
            session.transfer(original, REL_FAILURE);
            return;
        }

        final FlowFile originalFlowFile = FragmentAttributes.copyAttributesToOriginal(session, original, fragmentId, splits.size());
        session.transfer(originalFlowFile, REL_ORIGINAL);
        // Add the fragment count to each split
        for (FlowFile split : splits) {
            session.putAttribute(split, FRAGMENT_COUNT, String.valueOf(splits.size()));
        }
        session.transfer(splits, REL_SPLITS);
        getLogger().info("Successfully split {} into {} FlowFiles, each containing up to {} records", original, splits.size(), maxRecords);
    }

}
