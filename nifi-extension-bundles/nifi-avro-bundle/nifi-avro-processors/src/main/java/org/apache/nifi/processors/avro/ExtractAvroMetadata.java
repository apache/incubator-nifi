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

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@SideEffectFree
@SupportsBatching
@Tags({ "avro", "schema", "metadata" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Extracts metadata from the header of an Avro datafile.")
@WritesAttributes({
        @WritesAttribute(attribute = "schema.type", description = "The type of the schema (i.e. record, enum, etc.)."),
        @WritesAttribute(attribute = "schema.name", description = "Contains the name when the type is a record, enum or fixed, " +
                "otherwise contains the name of the primitive type."),
        @WritesAttribute(attribute = "schema.fingerprint", description = "The result of the Fingerprint Algorithm as a Hex string."),
        @WritesAttribute(attribute = "item.count", description = "The total number of items in the datafile, only written if Count Items " +
                "is set to true.")
})
public class ExtractAvroMetadata extends AbstractProcessor {

    static final AllowableValue CRC_64_AVRO = new AllowableValue("CRC-64-AVRO");
    static final AllowableValue MD5 = new AllowableValue("MD5");
    static final AllowableValue SHA_256 = new AllowableValue("SHA-256");

    static final PropertyDescriptor FINGERPRINT_ALGORITHM = new PropertyDescriptor.Builder()
            .name("Fingerprint Algorithm")
            .description("The algorithm used to generate the schema fingerprint. Available choices are based on the Avro recommended practices for " +
                    "fingerprint generation.")
            .allowableValues(CRC_64_AVRO, MD5, SHA_256)
            .defaultValue(CRC_64_AVRO.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor METADATA_KEYS = new PropertyDescriptor.Builder()
            .name("Metadata Keys")
            .description("A comma-separated list of keys indicating key/value pairs to extract from the Avro file header. The key 'avro.schema' can " +
                    "be used to extract the full schema in JSON format, and 'avro.codec' can be used to extract the codec name if one exists.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor COUNT_ITEMS = new PropertyDescriptor.Builder()
            .name("Count Items")
            .description("If true the number of items in the datafile will be counted and stored in a FlowFile attribute 'item.count'. The counting is done " +
                    "by reading blocks and getting the number of items for each block, thus avoiding de-serializing. The items being counted will be the top-level " +
                    "items in the datafile. For example, with a schema of type record the items will be the records, and for a schema of type Array the items will " +
                    "be the arrays (not the number of entries in each array).")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after metadata has been extracted.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be parsed as Avro or metadata cannot be extracted for any reason")
            .build();

    static final String SCHEMA_TYPE_ATTR = "schema.type";
    static final String SCHEMA_NAME_ATTR = "schema.name";
    static final String SCHEMA_FINGERPRINT_ATTR = "schema.fingerprint";
    static final String ITEM_COUNT_ATTR = "item.count";

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FINGERPRINT_ALGORITHM,
            METADATA_KEYS,
            COUNT_ITEMS
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
    }

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
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Map<String, String> avroMetadata = new HashMap<>();
        final Set<String> requestedMetadataKeys = new HashSet<>();

        final boolean countRecords = context.getProperty(COUNT_ITEMS).asBoolean();
        final String fingerprintAlgorithm = context.getProperty(FINGERPRINT_ALGORITHM).getValue();
        final String metadataKeysValue = context.getProperty(METADATA_KEYS).getValue();

        if (!StringUtils.isEmpty(metadataKeysValue)) {
            final String[] keys = metadataKeysValue.split("\\s*,\\s*");
            for (final String key : keys) {
                requestedMetadataKeys.add(key.trim());
            }
        }

        try {
            session.read(flowFile, rawIn -> {
                try (final InputStream in = new BufferedInputStream(rawIn);
                     final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<>())) {

                    final Schema schema = reader.getSchema();
                    if (schema == null) {
                        throw new ProcessException("Avro schema was null");
                    }

                    for (String key : reader.getMetaKeys()) {
                        if (requestedMetadataKeys.contains(key)) {
                            avroMetadata.put(key, reader.getMetaString(key));
                        }
                    }

                    try {
                        final byte[] rawFingerprint = SchemaNormalization.parsingFingerprint(fingerprintAlgorithm, schema);
                        avroMetadata.put(SCHEMA_FINGERPRINT_ATTR, Hex.encodeHexString(rawFingerprint));
                        avroMetadata.put(SCHEMA_TYPE_ATTR, schema.getType().getName());
                        avroMetadata.put(SCHEMA_NAME_ATTR, schema.getName());
                    } catch (NoSuchAlgorithmException e) {
                        // shouldn't happen since allowable values are valid algorithms
                        throw new ProcessException(e);
                    }

                    if (countRecords) {
                        long recordCount = reader.getBlockCount();
                        try {
                            while (reader.nextBlock() != null) {
                                recordCount += reader.getBlockCount();
                            }
                        } catch (NoSuchElementException e) {
                            // happens at end of file
                        }
                        avroMetadata.put(ITEM_COUNT_ATTR, String.valueOf(recordCount));
                    }
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Transferring to failure since failed to extract Avro metadata for {}", flowFile, pe);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAllAttributes(flowFile, avroMetadata);
        session.transfer(flowFile, REL_SUCCESS);
    }

}
