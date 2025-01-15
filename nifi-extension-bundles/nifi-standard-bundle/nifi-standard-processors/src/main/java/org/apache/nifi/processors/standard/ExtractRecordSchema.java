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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"record", "generic", "schema", "json", "csv", "avro", "freeform", "text", "xml"})
@WritesAttributes({
        @WritesAttribute(attribute = "record.error.message", description = "This attribute provides on failure the error message encountered by the Reader."),
        @WritesAttribute(attribute = "avro.schema", description = "This attribute provides the schema extracted from the input FlowFile using the provided RecordReader."),
})
@CapabilityDescription("Extracts the record schema from the FlowFile using the supplied Record Reader and writes it to the `avro.schema` attribute.")
public class ExtractRecordSchema extends AbstractProcessor {

    public static final String SCHEMA_ATTRIBUTE_NAME = "avro.schema";

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor SCHEMA_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Schema Cache Size")
            .description("Specifies the number of schemas to cache. This value should reflect the expected number of different schemas "
                    + "that may be in the incoming FlowFiles. This ensures more efficient retrieval of the schemas and thus the processor performance.")
            .defaultValue("10")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_READER,
            SCHEMA_CACHE_SIZE
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles whose record schemas are successfully extracted will be routed to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile's record schema cannot be extracted from the configured input format, "
                    + "the FlowFile will be routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private LoadingCache<RecordSchema, String> avroSchemaTextCache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        final int cacheSize = context.getProperty(SCHEMA_CACHE_SIZE).asInteger();
        avroSchemaTextCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .build(schema -> AvroTypeUtil.extractAvroSchema(schema).toString());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final Map<String, String> originalAttributes = flowFile.getAttributes();
        final RecordSchema recordSchema;
        try (final InputStream inputStream = session.read(flowFile);
             final RecordReader reader = readerFactory.createRecordReader(originalAttributes, inputStream, flowFile.getSize(), getLogger())) {
            recordSchema = reader.getSchema();

        } catch (final Exception e) {
            getLogger().error("Failed to process {}; will route to failure", flowFile, e);
            // Since we are wrapping the exceptions above there should always be a cause
            // but it's possible it might not have a message. This handles that by logging
            // the name of the class thrown.
            Throwable c = e.getCause();
            if (c == null) {
                session.putAttribute(flowFile, "record.error.message", e.getClass().getCanonicalName() + " Thrown");
            } else {
                session.putAttribute(flowFile, "record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
            }
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.putAttribute(flowFile, SCHEMA_ATTRIBUTE_NAME, avroSchemaTextCache.get(recordSchema));
        session.transfer(flowFile, REL_SUCCESS);
    }
}
