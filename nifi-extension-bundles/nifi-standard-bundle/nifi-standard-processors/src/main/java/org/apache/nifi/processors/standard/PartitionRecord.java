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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits, or partitions, record-oriented data based on the configured fields in the data. One or more properties must be added. The name of the property is the name " +
    "of an attribute to add. The value of the property is a RecordPath to evaluate against each Record. Two records will go to the same outbound FlowFile only if they have the same value for each " +
    "of the given RecordPaths. Because we know that all records in a given output FlowFile have the same value for the fields that are specified by the RecordPath, an attribute is added for each " +
    "field. See Additional Details on the Usage page for more information and examples.")
@DynamicProperty(name = "The name given to the dynamic property is the name of the attribute that will be used to denote the value of the associated RecordPath.",
    value = "A RecordPath that points to a field in the Record.",
    description = "Each dynamic property represents a RecordPath that will be evaluated against each record in an incoming FlowFile. When the value of the RecordPath is determined "
        + "for a Record, an attribute is added to the outgoing FlowFile. The name of the attribute is the same as the name of this property. The value of the attribute is the same as "
        + "the value of the field in the Record that the RecordPath points to. Note that no attribute will be added if the value returned for the RecordPath is null or is not a scalar "
        + "value (i.e., the value is an Array, Map, or Record).",
    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@WritesAttributes({
    @WritesAttribute(attribute = "record.count", description = "The number of records in an outgoing FlowFile"),
    @WritesAttribute(attribute = "mime.type", description = "The MIME Type that the configured Record Writer indicates is appropriate"),
    @WritesAttribute(attribute = "fragment.identifier", description = "All partitioned FlowFiles produced from the same parent FlowFile will have the same randomly "
            + "generated UUID added for this attribute"),
    @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the partitioned FlowFiles that were created from a single parent FlowFile"),
    @WritesAttribute(attribute = "fragment.count", description = "The number of partitioned FlowFiles generated from the parent FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile"),
    @WritesAttribute(attribute = "<dynamic property name>",
        description = "For each dynamic property that is added, an attribute may be added to the FlowFile. See the description for Dynamic Properties for more information.")
})
@Tags({"record", "partition", "recordpath", "rpath", "segment", "split", "group", "bin", "organize"})
@SeeAlso({ConvertRecord.class, SplitRecord.class, UpdateRecord.class, QueryRecord.class})
@UseCase(
    description = "Separate records into separate FlowFiles so that all of the records in a FlowFile have the same value for a given field or set of fields.",
    keywords = {"separate", "split", "partition", "break apart", "colocate", "segregate", "record", "field", "recordpath"},
    configuration = """
        Choose a RecordReader that is appropriate based on the format of the incoming data.
        Choose a RecordWriter that writes the data in the desired output format.

        Add a single additional property. The name of the property should describe the type of data that is being used to partition the data. \
        The property's value should be a RecordPath that specifies which output FlowFile the Record belongs to.

        For example, if we want to separate records based on their `transactionType` field, we could add a new property named `transactionType`. \
        The value of the property might be `/transaction/type`. An input FlowFile will then be separated into as few FlowFiles as possible such that each \
        output FlowFile has the same value for the `transactionType` field.
        """
)
@UseCase(
    description = "Separate records based on whether or not they adhere to a specific criteria",
    keywords = {"separate", "split", "partition", "break apart", "segregate", "record", "field", "recordpath", "criteria"},
    configuration = """
        Choose a RecordReader that is appropriate based on the format of the incoming data.
        Choose a RecordWriter that writes the data in the desired output format.

        Add a single additional property. The name of the property should describe the criteria. \
        The property's value should be a RecordPath that returns `true` if the Record meets the criteria or `false` otherwise.

        For example, if we want to separate records based on whether or not they have a transaction total of more than $1,000 we could add a new property named \
        `largeTransaction` with a value of `/transaction/total > 1000`. This will create two FlowFiles. In the first, all records will have a total over `1000`. \
        In the second, all records will have a transaction less than or equal to 1000. Each FlowFile will have an attribute named `largeTransaction` with a value \
        of `true` or `false`.
        """
)
public class PartitionRecord extends AbstractProcessor {
    private final RecordPathCache recordPathCache = new RecordPathCache(25);

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for reading incoming data")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing out the records")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_READER,
            RECORD_WRITER
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles that are successfully partitioned will be routed to this relationship")
        .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("Once all records in an incoming FlowFile have been partitioned, the original FlowFile is routed to this relationship.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a FlowFile cannot be partitioned from the configured input format to the configured output format, "
            + "the unchanged FlowFile will be routed to this relationship")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_ORIGINAL
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
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final boolean hasDynamic = validationContext.getProperties().keySet().stream()
            .anyMatch(PropertyDescriptor::isDynamic);

        if (hasDynamic) {
            return Collections.emptyList();
        }

        return Set.of(new ValidationResult.Builder()
            .subject("User-defined Properties")
            .valid(false)
            .explanation("At least one RecordPath must be added to this processor by adding a user-defined property")
            .build());
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .dynamic(true)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new RecordPathValidator())
            .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final Map<String, RecordPath> recordPaths;
        try {
            recordPaths = context.getProperties().keySet().stream()
                    .filter(PropertyDescriptor::isDynamic)
                    .collect(Collectors.toMap(
                            PropertyDescriptor::getName,
                            prop -> getRecordPath(context, prop, flowFile)));
        } catch (final Exception e) {
            getLogger().error("Failed to compile RecordPath for {}; routing to failure", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<RecordValueMap, RecordSetWriter> writerMap = new HashMap<>();

        try (final InputStream in = session.read(flowFile)) {
            final Map<String, String> originalAttributes = flowFile.getAttributes();
            final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, flowFile.getSize(), getLogger());

            final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, reader.getSchema());

            Record record;
            while ((record = reader.nextRecord()) != null) {
                final Map<String, List<ValueWrapper>> recordMap = new HashMap<>();

                // Evaluate all of the RecordPath's for this Record
                for (final Map.Entry<String, RecordPath> entry : recordPaths.entrySet()) {
                    final String propName = entry.getKey();
                    final RecordPath recordPath = entry.getValue();

                    final Stream<FieldValue> fieldValueStream = recordPath.evaluate(record).getSelectedFields();
                    final List<ValueWrapper> fieldValues = fieldValueStream
                        .map(fieldVal -> new ValueWrapper(fieldVal.getValue()))
                        .collect(Collectors.toList());
                    recordMap.put(propName, fieldValues);
                }

                final RecordValueMap recordValueMap = new RecordValueMap(recordMap);

                // Get the RecordSetWriter that contains the same values for all RecordPaths - or create one if none exists.
                RecordSetWriter writer = writerMap.get(recordValueMap);
                if (writer == null) {
                    final FlowFile childFlowFile = session.create(flowFile);
                    recordValueMap.setFlowFile(childFlowFile);

                    final OutputStream out = session.write(childFlowFile);

                    writer = writerFactory.createWriter(getLogger(), writeSchema, out, childFlowFile);
                    writer.beginRecordSet();
                    writerMap.put(recordValueMap, writer);
                }

                writer.write(record);
            }

            // For each RecordSetWriter, finish the record set and close the writer.
            int fragmentIndex = 0;
            final String fragmentId = UUID.randomUUID().toString();
            for (final Map.Entry<RecordValueMap, RecordSetWriter> entry : writerMap.entrySet()) {
                final RecordValueMap valueMap = entry.getKey();
                final RecordSetWriter writer = entry.getValue();

                final WriteResult writeResult = writer.finishRecordSet();
                writer.close();

                final Map<String, String> attributes = new HashMap<>();
                attributes.putAll(valueMap.getAttributes());
                attributes.putAll(writeResult.getAttributes());
                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                attributes.put(FragmentAttributes.FRAGMENT_INDEX.key(), String.valueOf(fragmentIndex));
                attributes.put(FragmentAttributes.FRAGMENT_ID.key(), fragmentId);
                attributes.put(FragmentAttributes.FRAGMENT_COUNT.key(), String.valueOf(writerMap.size()));
                attributes.put(FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()));

                FlowFile childFlowFile = valueMap.getFlowFile();
                childFlowFile = session.putAllAttributes(childFlowFile, attributes);

                session.adjustCounter("Record Processed", writeResult.getRecordCount(), false);
                fragmentIndex++;
            }

        } catch (final Exception e) {
            for (final Map.Entry<RecordValueMap, RecordSetWriter> entry : writerMap.entrySet()) {
                final RecordValueMap valueMap = entry.getKey();
                final RecordSetWriter writer = entry.getValue();

                try {
                    writer.close();
                } catch (final IOException e1) {
                    getLogger().warn("Failed to close Record Writer for {}; some resources may not be cleaned up appropriately", flowFile, e1);
                }

                session.remove(valueMap.getFlowFile());
            }


            getLogger().error("Failed to partition {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Transfer the FlowFiles. We wait until the end to do this, in case any IOException is thrown above,
        // because we want to ensure that we are able to remove the child flowfiles in case of a failure.
        for (final RecordValueMap valueMap : writerMap.keySet()) {
            session.transfer(valueMap.getFlowFile(), REL_SUCCESS);
        }

        session.transfer(flowFile, REL_ORIGINAL);
    }

    private RecordPath getRecordPath(final ProcessContext context, final PropertyDescriptor prop, final FlowFile flowFile) {
        final String pathText = context.getProperty(prop).evaluateAttributeExpressions(flowFile).getValue();
        return recordPathCache.getCompiled(pathText);
    }

    /**
     * We have this ValueWrapper class here because we want to use it as part of the key to a Map and we
     * want two values that may or may not be arrays. Since calling a.equals(b) returns false when a and b
     * are arrays, we need to wrap our values in a class that can handle comparisons appropriately.
     */
    static class ValueWrapper {
        private final Object value;

        public ValueWrapper(final Object value) {
            this.value = value;
        }

        public Object get() {
            return value;
        }

        @Override
        public int hashCode() {
            if (value == null) {
                return 31;
            }

            if (value instanceof Object[]) {
                return 31 + Arrays.deepHashCode((Object[]) value);
            }

            return 31 + value.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof ValueWrapper other)) {
                return false;
            }
            if (value == null && other.value == null) {
                return true;
            }
            if (value == null || other.value == null) {
                return false;
            }
            if (value instanceof Object[] && other.value instanceof Object[]) {
                return Arrays.equals((Object[]) value, (Object[]) other.value);
            }
            return value.equals(other.value);
        }
    }

    private static class RecordValueMap {
        private final Map<String, List<ValueWrapper>> values;
        private FlowFile flowFile;

        public RecordValueMap(final Map<String, List<ValueWrapper>> values) {
            this.values = values;
        }

        public Map<String, String> getAttributes() {
            final Map<String, String> attributes = new HashMap<>();
            for (final Map.Entry<String, List<ValueWrapper>> entry : values.entrySet()) {
                final List<ValueWrapper> values = entry.getValue();

                // If there are no values or there are multiple values, don't create an attribute.
                if (values.size() != 1) {
                    continue;
                }

                // If value is null, don't create an attribute
                final Object value = values.getFirst().get();
                if (value == null) {
                    continue;
                }

                // If value is not scalar, don't create an attribute
                if (value instanceof Object[] || value instanceof Map || value instanceof Record) {
                    continue;
                }

                // There exists a single value that is scalar. Create attribute using the property name as the attribute name
                final String attributeValue = DataTypeUtils.toString(value, (String) null);
                attributes.put(entry.getKey(), attributeValue);
            }

            return attributes;
        }

        public FlowFile getFlowFile() {
            return flowFile;
        }

        public void setFlowFile(final FlowFile flowFile) {
            this.flowFile = flowFile;
        }

        @Override
        public int hashCode() {
            return 41 + 37 * values.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof RecordValueMap other)) {
                return false;
            }
            return values.equals(other.values);
        }

        @Override
        public String toString() {
            return "RecordMapValue[" + values + "]";
        }
    }
}