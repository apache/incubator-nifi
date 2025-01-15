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

import net.datafaker.Faker;
import org.apache.avro.Schema;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.avro.AvroSchemaValidator;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.faker.FakerUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.StringUtils;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.standard.faker.FakerUtils.DEFAULT_DATE_PROPERTY_NAME;

@SupportsBatching
@Tags({"test", "random", "generate", "fake"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile"),
})
@CapabilityDescription("This processor creates FlowFiles with records having random value for the specified fields. GenerateRecord is useful " +
        "for testing, configuration, and simulation. It uses either user-defined properties to define a record schema or a provided schema and generates the specified number of records using " +
        "random data for the fields in the schema.")
@DynamicProperties({
        @DynamicProperty(
                name = "Field name in generated record",
                value = "Faker category for generated record values",
                description = "Custom properties define the generated record schema using configured field names and value data types in absence of the Schema Text property"
        )
})
public class GenerateRecord extends AbstractProcessor {

    private static final AllowableValue[] fakerDatatypeValues = FakerUtils.createFakerPropertyList();

    // Fake keys when generating a map
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";
    private static final String KEY3 = "key3";
    private static final String KEY4 = "key4";

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor NUM_RECORDS = new PropertyDescriptor.Builder()
            .name("number-of-records")
            .displayName("Number of Records")
            .description("Specifies how many records will be generated for each outgoing FlowFile.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor NULLABLE_FIELDS = new PropertyDescriptor.Builder()
            .name("nullable-fields")
            .displayName("Nullable Fields")
            .description("Whether the generated fields will be nullable. Note that this property is ignored if Schema Text is set. Also it only affects the schema of the generated data, " +
                    "not whether any values will be null. If this property is true, see 'Null Value Percentage' to set the probability that any generated field will be null.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();
    static final PropertyDescriptor NULL_PERCENTAGE = new PropertyDescriptor.Builder()
            .name("null-percentage")
            .displayName("Null Value Percentage")
            .description("The percent probability (0-100%) that a generated value for any nullable field will be null. Set this property to zero to have no null values, or 100 to have all " +
                    "null values.")
            .addValidator(StandardValidators.createLongValidator(0L, 100L, true))
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .defaultValue("0")
            .dependsOn(NULLABLE_FIELDS, "true")
            .build();

    static final PropertyDescriptor SCHEMA_TEXT = new PropertyDescriptor.Builder()
            .name("schema-text")
            .displayName("Schema Text")
            .description("The text of an Avro-formatted Schema used to generate record data. If this property is set, any user-defined properties are ignored.")
            .addValidator(new AvroSchemaValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_WRITER,
            NUM_RECORDS,
            NULLABLE_FIELDS,
            NULL_PERCENTAGE,
            SCHEMA_TEXT
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully created will be routed to this relationship")
            .build();

    static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private volatile Faker faker = new Faker();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .allowableValues(fakerDatatypeValues)
                .defaultValue("Address.fullAddress")
                .required(false)
                .dynamic(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Force the en-US Locale for more predictable results
        faker = new Faker(Locale.US);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final String schemaText = context.getProperty(SCHEMA_TEXT).evaluateAttributeExpressions().getValue();
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final int numRecords = context.getProperty(NUM_RECORDS).evaluateAttributeExpressions().asInteger();
        FlowFile flowFile = session.create();
        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();

        try {
            flowFile = session.write(flowFile, out -> {
                final RecordSchema recordSchema;
                final boolean usingSchema;
                final int nullPercentage = context.getProperty(NULL_PERCENTAGE).evaluateAttributeExpressions().asInteger();
                if (StringUtils.isNotEmpty(schemaText)) {
                    final Schema avroSchema = new Schema.Parser().parse(schemaText);
                    recordSchema = AvroTypeUtil.createSchema(avroSchema);
                    usingSchema = true;
                } else {
                    // Generate RecordSchema from user-defined properties
                    final boolean nullable = context.getProperty(NULLABLE_FIELDS).asBoolean();
                    final Map<String, String> fields = getFields(context);
                    recordSchema = generateRecordSchema(fields, nullable);
                    usingSchema = false;
                }
                try {
                    final RecordSchema writeSchema = writerFactory.getSchema(attributes, recordSchema);
                    try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, attributes)) {
                        writer.beginRecordSet();

                        Record record;
                        List<RecordField> writeFieldNames = writeSchema.getFields();
                        Map<String, Object> recordEntries = new HashMap<>();
                        for (int i = 0; i < numRecords; i++) {
                            for (RecordField writeRecordField : writeFieldNames) {
                                final String writeFieldName = writeRecordField.getFieldName();
                                final Object writeFieldValue;
                                if (usingSchema) {
                                    writeFieldValue = generateValueFromRecordField(writeRecordField, faker, nullPercentage);
                                } else {
                                    final boolean nullValue;
                                    if (!context.getProperty(GenerateRecord.NULLABLE_FIELDS).asBoolean() || nullPercentage == 0) {
                                        nullValue = false;
                                    } else {
                                        nullValue = (faker.number().numberBetween(0, 100) <= nullPercentage);
                                    }
                                    if (nullValue) {
                                        writeFieldValue = null;
                                    } else {
                                        final String propertyValue = context.getProperty(writeFieldName).getValue();
                                        writeFieldValue = FakerUtils.getFakeData(propertyValue, faker);
                                    }
                                }

                                recordEntries.put(writeFieldName, writeFieldValue);
                            }
                            record = new MapRecord(recordSchema, recordEntries);
                            writer.write(record);
                        }

                        final WriteResult writeResult = writer.finishRecordSet();
                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        attributes.putAll(writeResult.getAttributes());
                        recordCount.set(writeResult.getRecordCount());
                    }
                } catch (final SchemaNotFoundException e) {
                    throw new ProcessException("Schema not found while writing records", e);
                }
            });
        } catch (final Exception e) {
            if (e instanceof ProcessException) {
                throw e;
            } else {
                throw new ProcessException("Record generation failed", e);
            }
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);

        final int count = recordCount.get();
        session.adjustCounter("Records Processed", count, false);

        getLogger().info("Generated records [{}] for {}", count, flowFile);
    }

    protected Map<String, String> getFields(ProcessContext context) {
        return context.getProperties().entrySet().stream()
                // filter non-null dynamic properties
                .filter(e -> e.getKey().isDynamic() && e.getValue() != null)
                // convert to Map of user-defined field names and types
                .collect(Collectors.toMap(
                        e -> e.getKey().getName(),
                        e -> context.getProperty(e.getKey()).getValue()
                ));
    }

    private Object generateValueFromRecordField(RecordField recordField, Faker faker, int nullPercentage) {
        if (recordField.isNullable() && faker.number().numberBetween(0, 100) < nullPercentage) {
            return null;
        }
        switch (recordField.getDataType().getFieldType()) {
            case BIGINT:
                return new BigInteger(String.valueOf(faker.number().numberBetween(Long.MIN_VALUE, Long.MAX_VALUE)));
            case BOOLEAN:
                return FakerUtils.getFakeData("Bool.bool", faker);
            case BYTE:
                return (byte) faker.number().numberBetween(Byte.MIN_VALUE, Byte.MAX_VALUE);
            case CHAR:
                return (char) faker.number().numberBetween(Character.MIN_VALUE, Character.MAX_VALUE);
            case DATE:
                return FakerUtils.getFakeData(DEFAULT_DATE_PROPERTY_NAME, faker);
            case DOUBLE:
                return faker.number().randomDouble(6, Long.MIN_VALUE, Long.MAX_VALUE);
            case FLOAT:
                final double randomDouble = faker.number().randomDouble(6, Long.MIN_VALUE, Long.MAX_VALUE);
                return (float) randomDouble;
            case DECIMAL:
                return faker.number().randomDouble(((DecimalDataType) recordField.getDataType()).getScale(), Long.MIN_VALUE, Long.MAX_VALUE);
            case INT:
                return faker.number().numberBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
            case LONG:
                return faker.number().numberBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            case SHORT:
                return faker.number().numberBetween(Short.MIN_VALUE, Short.MAX_VALUE);
            case ENUM:
                List<String> enums = ((EnumDataType) recordField.getDataType()).getEnums();
                return enums.get(faker.number().numberBetween(0, enums.size() - 1));
            case TIME:
                Date fakeDate = (Date) FakerUtils.getFakeData(DEFAULT_DATE_PROPERTY_NAME, faker);
                LocalDate fakeLocalDate = fakeDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                return fakeLocalDate.format(DateTimeFormatter.ISO_LOCAL_TIME);
            case TIMESTAMP:
                return ((Date) FakerUtils.getFakeData(DEFAULT_DATE_PROPERTY_NAME, faker)).getTime();
            case UUID:
                return UUID.randomUUID();
            case ARRAY:
                final ArrayDataType arrayDataType = (ArrayDataType) recordField.getDataType();
                final DataType elementType = arrayDataType.getElementType();
                final int numElements = faker.number().numberBetween(0, 10);
                Object[] returnValue = new Object[numElements];
                for (int i = 0; i < numElements; i++) {
                    RecordField tempRecordField = new RecordField(recordField.getFieldName() + "[" + i + "]", elementType, arrayDataType.isElementsNullable());
                    // If the array elements are non-nullable, use zero as the nullPercentage
                    returnValue[i] = generateValueFromRecordField(tempRecordField, faker, arrayDataType.isElementsNullable() ? nullPercentage : 0);
                }
                return returnValue;
            case MAP:
                final MapDataType mapDataType = (MapDataType) recordField.getDataType();
                final DataType valueType = mapDataType.getValueType();
                // Create 4-element fake map
                Map<String, Object> returnMap = new HashMap<>(4);
                returnMap.put(KEY1, generateValueFromRecordField(new RecordField(KEY1, valueType), faker, nullPercentage));
                returnMap.put(KEY2, generateValueFromRecordField(new RecordField(KEY2, valueType), faker, nullPercentage));
                returnMap.put(KEY3, generateValueFromRecordField(new RecordField(KEY3, valueType), faker, nullPercentage));
                returnMap.put(KEY4, generateValueFromRecordField(new RecordField(KEY4, valueType), faker, nullPercentage));
                return returnMap;
            case RECORD:
                final RecordDataType recordType = (RecordDataType) recordField.getDataType();
                final RecordSchema childSchema = recordType.getChildSchema();
                final Map<String, Object> recordValues = new HashMap<>();
                for (RecordField writeRecordField : childSchema.getFields()) {
                    final String writeFieldName = writeRecordField.getFieldName();
                    final Object writeFieldValue = generateValueFromRecordField(writeRecordField, faker, nullPercentage);
                    recordValues.put(writeFieldName, writeFieldValue);
                }
                return new MapRecord(childSchema, recordValues);
            case CHOICE:
                final ChoiceDataType choiceDataType = (ChoiceDataType) recordField.getDataType();
                List<DataType> subTypes = choiceDataType.getPossibleSubTypes();
                // Pick one at random and generate a value for it
                DataType chosenType = subTypes.get(faker.number().numberBetween(0, subTypes.size() - 1));
                RecordField tempRecordField = new RecordField(recordField.getFieldName(), chosenType);
                return generateValueFromRecordField(tempRecordField, faker, nullPercentage);
            case STRING:
            default:
                return generateRandomString();
        }
    }

    private String generateRandomString() {
        final int categoryChoice = faker.number().numberBetween(0, 10);
        return switch (categoryChoice) {
            case 0 -> faker.name().fullName();
            case 1 -> faker.lorem().word();
            case 2 -> faker.shakespeare().romeoAndJulietQuote();
            case 3 -> faker.educator().university();
            case 4 -> faker.zelda().game();
            case 5 -> faker.company().name();
            case 6 -> faker.chuckNorris().fact();
            case 7 -> faker.book().title();
            case 8 -> faker.dog().breed();
            default -> faker.animal().name();
        };
    }

    protected RecordSchema generateRecordSchema(final Map<String, String> fields, final boolean nullable) {
        final List<RecordField> recordFields = new ArrayList<>(fields.size());
        for (Map.Entry<String, String> field : fields.entrySet()) {
            final String fieldName = field.getKey();
            final String fieldType = field.getValue();
            final DataType fieldDataType = FakerUtils.getDataType(fieldType);
            RecordField recordField = new RecordField(fieldName, fieldDataType, nullable);
            recordFields.add(recordField);
        }
        return new SimpleRecordSchema(recordFields);
    }
}
