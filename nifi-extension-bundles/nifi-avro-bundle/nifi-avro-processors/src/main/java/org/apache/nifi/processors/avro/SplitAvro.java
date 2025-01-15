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
package org.apache.nifi.processors.avro;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.copyAttributesToOriginal;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@SideEffectFree
@SupportsBatching
@Tags({ "avro", "split" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits a binary encoded Avro datafile into smaller files based on the configured Output Size. The Output Strategy determines if " +
        "the smaller files will be Avro datafiles, or bare Avro records with metadata in the FlowFile attributes. The output will always be binary encoded.")
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier",
                description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
        @WritesAttribute(attribute = "fragment.index",
                description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
        @WritesAttribute(attribute = "fragment.count",
                description = "The number of split FlowFiles generated from the parent FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class SplitAvro extends AbstractProcessor {

    public static final String RECORD_SPLIT_VALUE = "Record";
    public static final AllowableValue RECORD_SPLIT = new AllowableValue(RECORD_SPLIT_VALUE, RECORD_SPLIT_VALUE, "Split at Record boundaries");

    public static final PropertyDescriptor SPLIT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Split Strategy")
            .description("The strategy for splitting the incoming datafile. The Record strategy will read the incoming datafile by de-serializing each record.")
            .required(true)
            .allowableValues(RECORD_SPLIT)
            .defaultValue(RECORD_SPLIT.getValue())
            .build();

    public static final PropertyDescriptor OUTPUT_SIZE = new PropertyDescriptor.Builder()
            .name("Output Size")
            .description("The number of Avro records to include per split file. In cases where the incoming file has less records than the Output Size, or " +
                    "when the total number of records does not divide evenly by the Output Size, it is possible to get a split file with less records.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("1")
            .build();

    public static final String DATAFILE_OUTPUT_VALUE = "Datafile";
    public static final String BARE_RECORD_OUTPUT_VALUE = "Bare Record";

    public static final AllowableValue DATAFILE_OUTPUT = new AllowableValue(DATAFILE_OUTPUT_VALUE, DATAFILE_OUTPUT_VALUE, "Avro's object container file format");
    public static final AllowableValue BARE_RECORD_OUTPUT = new AllowableValue(BARE_RECORD_OUTPUT_VALUE, BARE_RECORD_OUTPUT_VALUE, "Bare Avro records");

    public static final PropertyDescriptor OUTPUT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Output Strategy")
            .description("Determines the format of the output. Either Avro Datafile, or bare record. Bare record output is only intended for use with systems " +
                    "that already require it, and shouldn't be needed for normal use.")
            .required(true)
            .allowableValues(DATAFILE_OUTPUT, BARE_RECORD_OUTPUT)
            .defaultValue(DATAFILE_OUTPUT.getValue())
            .build();

    public static final PropertyDescriptor TRANSFER_METADATA = new PropertyDescriptor.Builder()
            .name("Transfer Metadata")
            .description("Whether or not to transfer metadata from the parent datafile to the children. If the Output Strategy is Bare Record, " +
                    "then the metadata will be stored as FlowFile attributes, otherwise it will be in the Datafile header.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split. If the FlowFile fails processing, nothing will be sent to " +
                    "this relationship")
            .build();
    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("All new files split from the original FlowFile will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid Avro), " +
                    "it will be routed to this relationship")
            .build();

    // Metadata keys that are not transferred to split files when output strategy is datafile
    // Avro will write this key/values pairs on its own
    static final Set<String> RESERVED_METADATA = Set.of(
            "avro.schema",
            "avro.codec"
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SPLIT_STRATEGY,
            OUTPUT_SIZE,
            OUTPUT_STRATEGY,
            TRANSFER_METADATA
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_SPLIT,
            REL_FAILURE
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
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final int splitSize = context.getProperty(OUTPUT_SIZE).asInteger();
        final boolean transferMetadata = context.getProperty(TRANSFER_METADATA).asBoolean();

        SplitWriter splitWriter;
        final String outputStrategy = context.getProperty(OUTPUT_STRATEGY).getValue();
        splitWriter = switch (outputStrategy) {
            case DATAFILE_OUTPUT_VALUE -> new DatafileSplitWriter(transferMetadata);
            case BARE_RECORD_OUTPUT_VALUE -> new BareRecordSplitWriter();
            default -> throw new AssertionError();
        };

        Splitter splitter;
        final String splitStrategy = context.getProperty(SPLIT_STRATEGY).getValue();
        if (splitStrategy.equals(RECORD_SPLIT_VALUE)) {
            splitter = new RecordSplitter(splitSize, transferMetadata);
        } else {
            throw new AssertionError();
        }

        try {
            final List<FlowFile> splits = splitter.split(session, flowFile, splitWriter);
            final Map<String, String> attributes = new HashMap<>();
            final String fragmentIdentifier = UUID.randomUUID().toString();
            attributes.put(FRAGMENT_ID.key(), fragmentIdentifier);
            attributes.put(SEGMENT_ORIGINAL_FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()));
            attributes.put(FRAGMENT_COUNT.key(), Integer.toString(splits.size()));
            IntStream.range(0, splits.size()).forEach((i) -> {
                FlowFile split = splits.get(i);
                attributes.put(FRAGMENT_INDEX.key(), Integer.toString(i));
                split = session.putAllAttributes(split, attributes);
                session.transfer(split, REL_SPLIT);
            });
            final FlowFile originalFlowFile = copyAttributesToOriginal(session, flowFile, fragmentIdentifier, splits.size());
            session.transfer(originalFlowFile, REL_ORIGINAL);
        } catch (ProcessException e) {
            getLogger().error("Failed to split {} due to {}", flowFile, e.getMessage(), e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Able to split an incoming Avro datafile into multiple smaller FlowFiles.
     */
    private interface Splitter {
        List<FlowFile> split(final ProcessSession session, final FlowFile originalFlowFile, final SplitWriter splitWriter);
    }

    /**
     * Splits the incoming Avro datafile into batches of records by reading and de-serializing each record.
     */
    static private class RecordSplitter implements Splitter {

        private final int splitSize;
        private final boolean transferMetadata;

        public RecordSplitter(final int splitSize, final boolean transferMetadata) {
            this.splitSize = splitSize;
            this.transferMetadata = transferMetadata;
        }

        @Override
        public List<FlowFile> split(final ProcessSession session, final FlowFile originalFlowFile, final SplitWriter splitWriter) {
            final List<FlowFile> childFlowFiles = new ArrayList<>();
            final AtomicReference<GenericRecord> recordHolder = new AtomicReference<>(null);

            session.read(originalFlowFile, rawIn -> {
                try (final InputStream in = new BufferedInputStream(rawIn);
                     final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<>())) {

                    final AtomicReference<String> codec = new AtomicReference<>(reader.getMetaString(DataFileConstants.CODEC));
                    if (codec.get() == null) {
                        codec.set(DataFileConstants.NULL_CODEC);
                    }

                    // while records are left, start a new split by spawning a FlowFile
                    final AtomicReference<Boolean> hasNextHolder = new AtomicReference<>(reader.hasNext());
                    while (hasNextHolder.get()) {
                        FlowFile childFlowFile = session.create(originalFlowFile);
                        childFlowFile = session.write(childFlowFile, rawOut -> {
                            try (final BufferedOutputStream out = new BufferedOutputStream(rawOut)) {
                                splitWriter.init(reader, codec.get(), out);

                                // append to the current FlowFile until no more records, or splitSize is reached
                                int recordCount = 0;
                                while (hasNextHolder.get() && recordCount < splitSize) {
                                    recordHolder.set(reader.next(recordHolder.get()));
                                    splitWriter.write(recordHolder.get());
                                    recordCount++;
                                    hasNextHolder.set(reader.hasNext());
                                }

                                splitWriter.flush();
                            } finally {
                                splitWriter.close();
                            }
                        });

                        // would prefer this to be part of the SplitWriter, but putting the metadata in FlowFile attributes
                        // can't be done inside of an OutputStream callback which is where the splitWriter is used
                        if (splitWriter instanceof BareRecordSplitWriter && transferMetadata) {
                            final Map<String, String> metadata = new HashMap<>();
                            for (String metaKey : reader.getMetaKeys()) {
                                metadata.put(metaKey, reader.getMetaString(metaKey));
                            }
                            childFlowFile = session.putAllAttributes(childFlowFile, metadata);
                        }

                        childFlowFiles.add(childFlowFile);
                    }
                }
            });

            return childFlowFiles;
        }
    }

    /**
     * Writes records from the reader to the given output stream.
     */
    private interface SplitWriter {
        void init(final DataFileStream<GenericRecord> reader, final String codec, final OutputStream out) throws IOException;
        void write(final GenericRecord datum) throws IOException;
        void flush() throws IOException;
        void close() throws IOException;
    }

    /**
     * Writes a binary Avro Datafile to the OutputStream.
     */
    static private class DatafileSplitWriter implements SplitWriter {

        private final boolean transferMetadata;
        private DataFileWriter<GenericRecord> writer;

        public DatafileSplitWriter(final boolean transferMetadata) {
            this.transferMetadata = transferMetadata;
        }

        @Override
        public void init(final DataFileStream<GenericRecord> reader, final String codec, final OutputStream out) throws IOException {
            writer = new DataFileWriter<>(new GenericDatumWriter<>());

            if (transferMetadata) {
                for (String metaKey : reader.getMetaKeys()) {
                    if (!RESERVED_METADATA.contains(metaKey)) {
                        writer.setMeta(metaKey, reader.getMeta(metaKey));
                    }
                }
            }

            writer.setCodec(CodecFactory.fromString(codec));
            writer.create(reader.getSchema(), out);
        }

        @Override
        public void write(final GenericRecord datum) throws IOException {
            writer.append(datum);
        }

        @Override
        public void flush() throws IOException {
            writer.flush();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    /**
     * Writes bare Avro records to the OutputStream.
     */
    static private class BareRecordSplitWriter implements SplitWriter {
        private Encoder encoder;
        private DatumWriter<GenericRecord> writer;

        @Override
        public void init(final DataFileStream<GenericRecord> reader, final String codec, final OutputStream out) {
            writer = new GenericDatumWriter<>(reader.getSchema());
            encoder = EncoderFactory.get().binaryEncoder(out, null);
        }

        @Override
        public void write(GenericRecord datum) throws IOException {
            writer.write(datum, encoder);
        }

        @Override
        public void flush() throws IOException {
            encoder.flush();
        }

        @Override
        public void close() {
            // nothing to do
        }
    }

}
