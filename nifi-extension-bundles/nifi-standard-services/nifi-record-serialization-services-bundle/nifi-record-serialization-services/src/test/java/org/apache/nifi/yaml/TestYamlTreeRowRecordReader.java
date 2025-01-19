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

package org.apache.nifi.yaml;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.json.JsonRecordSource;
import org.apache.nifi.json.JsonSchemaInference;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.json.SchemaApplicationStrategy;
import org.apache.nifi.json.StartingFieldStrategy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.inference.InferSchemaAccessStrategy;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.util.EqualsWrapper;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestYamlTreeRowRecordReader {
    private static final Path BANK_ACCOUNT_ARRAY = Paths.get("src/test/resources/yaml/bank-account-array.yaml");
    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();

    private List<RecordField> getDefaultFields() {
        return getFields(RecordFieldType.DOUBLE.getDataType());
    }

    private List<RecordField> getFields(final DataType balanceDataType) {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("balance", balanceDataType));
        fields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("city", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("state", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("zipCode", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("country", RecordFieldType.STRING.getDataType()));
        return fields;
    }

    private RecordSchema getAccountSchema() {
        final List<RecordField> accountFields = new ArrayList<>();
        accountFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        accountFields.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));

        return new SimpleRecordSchema(accountFields);
    }

    @Test
    void testReadChoiceOfStringOrArrayOfRecords() throws IOException, MalformedRecordException {
        final File schemaFile = new File("src/test/resources/json/choice-of-string-or-array-record.avsc");
        final File jsonFile = new File("src/test/resources/yaml/choice-of-string-or-array-record.yaml");

        final Schema avroSchema = new Schema.Parser().parse(schemaFile);
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        try (final InputStream fis = Files.newInputStream(jsonFile.toPath());
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(fis, new MockComponentLog("id", "id"), recordSchema, dateFormat, timeFormat, timestampFormat)) {

            final Record record = reader.nextRecord();
            final Object[] fieldsArray = record.getAsArray("fields");
            assertEquals(2, fieldsArray.length);

            final Object firstElement = fieldsArray[0];
            assertInstanceOf(Record.class, firstElement);
            assertEquals("string", ((Record) firstElement).getAsString("type"));

            final Object secondElement = fieldsArray[1];
            assertInstanceOf(Record.class, secondElement);
            final Object[] typeArray = ((Record) secondElement).getAsArray("type");
            assertEquals(1, typeArray.length);

            final Object firstType = typeArray[0];
            assertInstanceOf(Record.class, firstType);
            final Record firstTypeRecord = (Record) firstType;
            assertEquals("string", firstTypeRecord.getAsString("type"));
        }
    }

    @Test
    void testChoiceOfRecordTypes() throws IOException, MalformedRecordException {
        final Schema avroSchema = new Schema.Parser().parse(new File("src/test/resources/json/record-choice.avsc"));
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/elements-for-record-choice.yaml"));
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), recordSchema, dateFormat, timeFormat, timestampFormat)) {

            // evaluate first record
            final Record firstRecord = reader.nextRecord();
            assertNotNull(firstRecord);
            final RecordSchema firstOuterSchema = firstRecord.getSchema();
            assertEquals(Arrays.asList("id", "child"), firstOuterSchema.getFieldNames());
            assertEquals("1234", firstRecord.getValue("id"));

            // record should have a schema that indicates that the 'child' is a CHOICE of 2 different record types
            assertSame(RecordFieldType.CHOICE, firstOuterSchema.getDataType("child").get().getFieldType());
            final List<DataType> firstSubTypes = ((ChoiceDataType) firstOuterSchema.getDataType("child").get()).getPossibleSubTypes();
            assertEquals(2, firstSubTypes.size());
            assertEquals(2L, firstSubTypes.stream().filter(type -> type.getFieldType() == RecordFieldType.RECORD).count());

            // child record should have a schema with "id" as the only field
            final Object childObject = firstRecord.getValue("child");
            assertInstanceOf(Record.class, childObject);
            final Record firstChildRecord = (Record) childObject;
            final RecordSchema firstChildSchema = firstChildRecord.getSchema();

            assertEquals(Collections.singletonList("id"), firstChildSchema.getFieldNames());

            // evaluate second record
            final Record secondRecord = reader.nextRecord();
            assertNotNull(secondRecord);

            final RecordSchema secondOuterSchema = secondRecord.getSchema();
            assertEquals(Arrays.asList("id", "child"), secondOuterSchema.getFieldNames());
            assertEquals("1234", secondRecord.getValue("id"));

            // record should have a schema that indicates that the 'child' is a CHOICE of 2 different record types
            assertSame(RecordFieldType.CHOICE, secondOuterSchema.getDataType("child").get().getFieldType());
            final List<DataType> secondSubTypes = ((ChoiceDataType) secondOuterSchema.getDataType("child").get()).getPossibleSubTypes();
            assertEquals(2, secondSubTypes.size());
            assertEquals(2L, secondSubTypes.stream().filter(type -> type.getFieldType() == RecordFieldType.RECORD).count());

            // child record should have a schema with "name" as the only field
            final Object secondChildObject = secondRecord.getValue("child");
            assertInstanceOf(Record.class, secondChildObject);
            final Record secondChildRecord = (Record) secondChildObject;
            final RecordSchema secondChildSchema = secondChildRecord.getSchema();

            assertEquals(Collections.singletonList("name"), secondChildSchema.getFieldNames());

            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testReadArray() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = Files.newInputStream(BANK_ACCOUNT_ARRAY);
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList("id", "name", "balance", "address", "city", "state", "zipCode", "country");
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(DataType::getFieldType).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING,
                    RecordFieldType.DOUBLE, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING);
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            final Object[] secondRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {2, "Jane Doe", 4820.09, "321 Your Street", "Your City", "NY", "33333", "USA"}, secondRecordValues);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testReadYamlComments() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/bank-account-comments.yaml"))) {
            final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat,
                    null, null, null, null);
            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList("id", "name", "balance", "address", "city", "state", "zipCode", "country");
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(DataType::getFieldType).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING,
                    RecordFieldType.DOUBLE, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING);
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            final Object[] secondRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {2, "Jane Doe", 4820.09, "321 Your Street", "Your City", "NY", "33333", "USA"}, secondRecordValues);

            assertNull(reader.nextRecord());
        }
    }


    @Test
    void testReadRawRecordIncludesFieldsNotInSchema() throws IOException, MalformedRecordException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = Files.newInputStream(BANK_ACCOUNT_ARRAY);
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Record schemaValidatedRecord = reader.nextRecord(true, true);
            assertEquals(1, schemaValidatedRecord.getValue("id"));
            assertEquals("John Doe", schemaValidatedRecord.getValue("name"));
            assertNull(schemaValidatedRecord.getValue("balance"));
        }

        try (final InputStream in = Files.newInputStream(BANK_ACCOUNT_ARRAY);
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Record rawRecord = reader.nextRecord(false, false);
            assertEquals(1, rawRecord.getValue("id"));
            assertEquals("John Doe", rawRecord.getValue("name"));
            assertEquals(4750.89, rawRecord.getValue("balance"));
            assertEquals("123 My Street", rawRecord.getValue("address"));
            assertEquals("My City", rawRecord.getValue("city"));
            assertEquals("MS", rawRecord.getValue("state"));
            assertEquals("11111", rawRecord.getValue("zipCode"));
            assertEquals("USA", rawRecord.getValue("country"));
        }
    }

    @Test
    void testReadRawRecordFieldOrderPreserved() throws IOException, MalformedRecordException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String expectedRecordToString = """
            {"id":1,"name":"John Doe","address":"123 My Street","city":"My City","state":"MS","zipCode":"11111","country":"USA","account":{"id":42,"balance":4750.89}}""";

        final String expectedMap = "{id=1, name=John Doe, address=123 My Street, city=My City, state=MS, zipCode=11111, country=USA, account={\"id\":42,\"balance\":4750.89}}";

        try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/single-element-nested.yaml"));
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Record rawRecord = reader.nextRecord(false, false);
            assertEquals(expectedRecordToString, rawRecord.toString());

            final Map<String, Object> map = rawRecord.toMap();
            assertEquals(expectedMap, map.toString());
        }
    }

    @Test
    void testReadRawRecordTypeCoercion() throws IOException, MalformedRecordException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = Files.newInputStream(BANK_ACCOUNT_ARRAY);
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Record schemaValidatedRecord = reader.nextRecord(true, true);
            assertEquals("1", schemaValidatedRecord.getValue("id")); // will be coerced into a STRING as per the schema
            assertEquals("John Doe", schemaValidatedRecord.getValue("name"));
            assertNull(schemaValidatedRecord.getValue("balance"));

            assertEquals(2, schemaValidatedRecord.getRawFieldNames().size());
        }

        try (final InputStream in = Files.newInputStream(BANK_ACCOUNT_ARRAY);
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Record rawRecord = reader.nextRecord(false, false);
            assertEquals(1, rawRecord.getValue("id")); // will return raw value of (int) 1
            assertEquals("John Doe", rawRecord.getValue("name"));
            assertEquals(4750.89, rawRecord.getValue("balance"));
            assertEquals("123 My Street", rawRecord.getValue("address"));
            assertEquals("My City", rawRecord.getValue("city"));
            assertEquals("MS", rawRecord.getValue("state"));
            assertEquals("11111", rawRecord.getValue("zipCode"));
            assertEquals("USA", rawRecord.getValue("country"));

            assertEquals(8, rawRecord.getRawFieldNames().size());
        }
    }

    @Test
    void testDateCoercedFromString() throws IOException, MalformedRecordException {
        final String dateField = "date";
        final List<RecordField> recordFields = Collections.singletonList(new RecordField(dateField, RecordFieldType.DATE.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(recordFields);

        final String date = "2000-01-01";
        final String datePattern = "yyyy-MM-dd";
        final String yaml = String.format("%s: %s", dateField, date);
        for (final boolean coerceTypes : new boolean[] {true, false}) {
            try (final InputStream in = new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8));
                 final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, datePattern, timeFormat, timestampFormat)) {

                final Record record = reader.nextRecord(coerceTypes, false);
                final Object value = record.getValue(dateField);
                assertInstanceOf(Date.class, value, "With coerceTypes set to " + coerceTypes + ", value is not a Date");
                assertEquals(date, value.toString());
            }
        }
    }

    @Test
    void testTimestampCoercedFromString() throws IOException, MalformedRecordException {
        final List<RecordField> recordFields = Collections.singletonList(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(recordFields);

        for (final boolean coerceTypes : new boolean[] {true, false}) {
            try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/timestamp.yaml"));
                 final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, "yyyy/MM/dd HH:mm:ss")) {

                final Record record = reader.nextRecord(coerceTypes, false);
                final Object value = record.getValue("timestamp");
                assertInstanceOf(Timestamp.class, value, "With coerceTypes set to " + coerceTypes + ", value is not a Timestamp");
            }
        }
    }

    @Test
    void testSingleJsonElement() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/single-bank-account.yaml"));
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList("id", "name", "balance", "address", "city", "state", "zipCode", "country");
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(DataType::getFieldType).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING,
                    RecordFieldType.DOUBLE, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING);
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testSingleJsonElementWithChoiceFields() throws IOException, MalformedRecordException {
        // Wraps default fields by Choice data type to test mapping to a Choice type.
        final List<RecordField> choiceFields = getDefaultFields().stream()
                .map(f -> new RecordField(f.getFieldName(), RecordFieldType.CHOICE.getChoiceDataType(f.getDataType()))).collect(Collectors.toList());
        final RecordSchema schema = new SimpleRecordSchema(choiceFields);

        try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/single-bank-account.yaml"));
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList("id", "name", "balance", "address", "city", "state", "zipCode", "country");
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING,
                    RecordFieldType.DOUBLE, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING);
            final List<RecordField> fields = schema.getFields();
            for (int i = 0; i < schema.getFields().size(); i++) {
                assertInstanceOf(ChoiceDataType.class, fields.get(i).getDataType());
                final ChoiceDataType choiceDataType = (ChoiceDataType) fields.get(i).getDataType();
                assertEquals(expectedTypes.get(i), choiceDataType.getPossibleSubTypes().get(0).getFieldType());
            }

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testElementWithNestedData() throws IOException, MalformedRecordException {
        final DataType accountType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("account", accountType));
        fields.remove(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/single-element-nested.yaml"));
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(DataType::getFieldType).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING,
                    RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.RECORD);
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            final Object[] allButLast = Arrays.copyOfRange(firstRecordValues, 0, firstRecordValues.length - 1);
            assertArrayEquals(new Object[] {1, "John Doe", "123 My Street", "My City", "MS", "11111", "USA"}, allButLast);

            final Object last = firstRecordValues[firstRecordValues.length - 1];
            assertTrue(Record.class.isAssignableFrom(last.getClass()));
            final Record record = (Record) last;
            assertEquals(42, record.getValue("id"));
            assertEquals(4750.89, record.getValue("balance"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testElementWithNestedArray() throws IOException, MalformedRecordException {
        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        fields.remove(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/single-element-nested-array.yaml"));
             final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList("id", "name", "address", "city", "state", "zipCode", "country", "accounts");
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(DataType::getFieldType).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING,
                    RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.ARRAY);
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            final Object[] nonArrayValues = Arrays.copyOfRange(firstRecordValues, 0, firstRecordValues.length - 1);
            assertArrayEquals(new Object[] {1, "John Doe", "123 My Street", "My City", "MS", "11111", "USA"}, nonArrayValues);

            final Object lastRecord = firstRecordValues[firstRecordValues.length - 1];
            assertTrue(Object[].class.isAssignableFrom(lastRecord.getClass()));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testReadArrayDifferentSchemas() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/bank-account-array-different-schemas.yaml"));
             final JsonTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList("id", "name", "balance", "address", "city", "state", "zipCode", "country");
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(DataType::getFieldType).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING,
                    RecordFieldType.DOUBLE, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING);
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            final Object[] secondRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {2, "Jane Doe", 4820.09, "321 Your Street", "Your City", "NY", "33333", null}, secondRecordValues);

            final Object[] thirdRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {3, "Jake Doe", 4751.89, "124 My Street", "My City", "MS", "11111", "USA"}, thirdRecordValues);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testReadArrayDifferentSchemasWithOptionalElementOverridden() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/bank-account-array-optional-balance.yaml"));
             final JsonTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList("id", "name", "balance", "address", "city", "state", "zipCode", "country");
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(DataType::getFieldType).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING, RecordFieldType.DOUBLE, RecordFieldType.STRING,
                    RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING);
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            final Object[] secondRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {2, "Jane Doe", null, "321 Your Street", "Your City", "NY", "33333", "USA"}, secondRecordValues);

            final Object[] thirdRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {3, "Jimmy Doe", null, "321 Your Street", "Your City", "NY", "33333", "USA"}, thirdRecordValues);

            assertNull(reader.nextRecord());
        }
    }


    @Test
    void testReadUnicodeCharacters() throws IOException, MalformedRecordException {

        final List<RecordField> fromFields = new ArrayList<>();
        fromFields.add(new RecordField("id", RecordFieldType.LONG.getDataType()));
        fromFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema fromSchema = new SimpleRecordSchema(fromFields);
        final DataType fromType = RecordFieldType.RECORD.getRecordDataType(fromSchema);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("created_at", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("id", RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField("unicode", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("from", fromType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/yaml-with-unicode.yaml"));
             final JsonTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Object[] firstRecordValues = reader.nextRecord().getValues();

            final Object secondValue = firstRecordValues[1];
            assertInstanceOf(Long.class, secondValue);
            assertEquals(832036744985577473L, secondValue);

            final Object unicodeValue = firstRecordValues[2];
            assertEquals("\u3061\u3083\u6ce3\u304d\u305d\u3046", unicodeValue);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testIncorrectSchema() {
        final DataType accountType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("account", accountType));
        fields.remove(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        MalformedRecordException mre = assertThrows(MalformedRecordException.class, () -> {
            try (final InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/single-bank-account-wrong-field-type.yaml"));
                 final YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

                reader.nextRecord().getValues();
            }
        });

        final String msg = mre.getCause().getMessage();
        assertTrue(msg.contains("account.balance"));
        assertTrue(msg.contains("true"));
        assertTrue(msg.contains("Double"));
        assertTrue(msg.contains("Boolean"));
    }

    @Test
    void testMergeOfSimilarRecords() throws Exception {
        String yamlPath = "src/test/resources/yaml/similar-records.yaml";

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType()),
            new RecordField("booleanOrString", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.BOOLEAN.getDataType(),
                RecordFieldType.STRING.getDataType()
            )),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));

        List<Object> expected = Arrays.asList(
            new MapRecord(expectedSchema, new HashMap<>() {{
                put("integer", 1);
                put("boolean", true);
                put("booleanOrString", true);
            }}),
            new MapRecord(expectedSchema, new HashMap<>() {{
                put("integer", 2);
                put("string", "stringValue2");
                put("booleanOrString", "booleanOrStringValue2");
            }})
        );

        testReadRecords(yamlPath, expected);
    }

    @Test
    void testChoiceOfEmbeddedSimilarRecords() throws Exception {
        String yamlPath = "src/test/resources/yaml/choice-of-embedded-similar-records.yaml";

        final SimpleRecordSchema expectedRecordSchema1 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType())
        ));
        final SimpleRecordSchema expectedRecordSchema2 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));
        RecordSchema expectedRecordChoiceSchema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("record", RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema1),
                        RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema2)
                ))
        ));

        List<Object> expected = Arrays.asList(
            new MapRecord(expectedRecordChoiceSchema, new HashMap<>() {{
                put("record", new MapRecord(expectedRecordSchema1, new HashMap<>() {{
                    put("integer", 1);
                    put("boolean", true);
                }}));
            }}),
            new MapRecord(expectedRecordChoiceSchema, new HashMap<>() {{
                put("record", new MapRecord(expectedRecordSchema2, new HashMap<>() {{
                    put("integer", 2);
                    put("string", "stringValue2");
                }}));
            }})
        );

        testReadRecords(yamlPath, expected);
    }


    @Test
    void testChoseSuboptimalSchemaWhenDataHasExtraFields() throws Exception {
        String yamlPath = "src/test/resources/yaml/choice-of-different-arrays-with-extra-fields.yaml";

        SimpleRecordSchema recordSchema1 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType())
        ));
        SimpleRecordSchema recordSchema2 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));

        RecordSchema recordChoiceSchema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("record", RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(recordSchema1)),
                        RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(recordSchema2))
                ))
        ));

        RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("dataCollection", RecordFieldType.ARRAY.getArrayDataType(
                        RecordFieldType.RECORD.getRecordDataType(recordChoiceSchema)
                )
                )));

        SimpleRecordSchema expectedChildSchema1 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType())
        ));
        SimpleRecordSchema expectedChildSchema2 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));
        RecordSchema expectedRecordChoiceSchema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("record", RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(expectedChildSchema1)),
                        RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(expectedChildSchema2))
                ))
        ));

        // Since the actual arrays have records with either (INT, BOOLEAN, STRING) or (INT, STRING, STRING)
        //  while the explicit schema defines only (INT, BOOLEAN) and (INT, STRING) we can't tell which record schema to chose
        //  so we take the first one (INT, BOOLEAN) - as best effort - for both cases
        List<Object> expected = Arrays.asList(
            new MapRecord(expectedRecordChoiceSchema, new HashMap<>() {{
                put("record", new Object[]{
                        new MapRecord(expectedChildSchema1, new HashMap<>() {{
                            put("integer", 11);
                            put("boolean", true);
                            put("extraString", "extraStringValue11");
                        }}),
                        new MapRecord(expectedChildSchema1, new HashMap<>() {{
                            put("integer", 12);
                            put("boolean", false);
                            put("extraString", "extraStringValue12");
                        }})
                });
            }}),
            new MapRecord(expectedRecordChoiceSchema, new HashMap<>() {{
                put("record", new Object[]{
                        new MapRecord(expectedChildSchema1, new HashMap<>() {{
                            put("integer", 21);
                            put("extraString", "extraStringValue21");
                            put("string", "stringValue21");
                        }}),
                        new MapRecord(expectedChildSchema1, new HashMap<>() {{
                            put("integer", 22);
                            put("extraString", "extraStringValue22");
                            put("string", "stringValue22");
                        }})
                });
            }})
        );

        testReadRecords(yamlPath, schema, expected);
    }

    @Test
    void testStartFromNestedArray() throws IOException, MalformedRecordException {
        String yamlPath = "src/test/resources/yaml/single-element-nested-array.yaml";

        SimpleRecordSchema expectedRecordSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("balance", RecordFieldType.DOUBLE.getDataType())
        ));

        List<Object> expected = Arrays.asList(
                new MapRecord(expectedRecordSchema, new HashMap<>() {{
                    put("id", 42);
                    put("balance", 4750.89);
                }}),
                new MapRecord(expectedRecordSchema, new HashMap<>() {{
                    put("id", 43);
                    put("balance", 48212.38);
                }})
        );

        testNestedReadRecords(yamlPath, expected, "accounts");
    }

    @Test
    void testStartFromNestedObject() throws IOException, MalformedRecordException {
        String yamlPath = "src/test/resources/yaml/single-element-nested.yaml";

        SimpleRecordSchema expectedRecordSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("balance", RecordFieldType.DOUBLE.getDataType())
        ));

        List<Object> expected = Collections.singletonList(
                new MapRecord(expectedRecordSchema, new HashMap<>() {{
                    put("id", 42);
                    put("balance", 4750.89);
                }})
        );

        testNestedReadRecords(yamlPath, expected, "account");
    }

    @Test
    void testStartFromMultipleNestedField() throws IOException, MalformedRecordException {
        String yamlPath = "src/test/resources/yaml/multiple-nested-field.yaml";

        SimpleRecordSchema expectedRecordSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("id", RecordFieldType.STRING.getDataType()),
                new RecordField("type", RecordFieldType.STRING.getDataType())
        ));

        List<Object> expected = Arrays.asList(
                    new MapRecord(expectedRecordSchema, new HashMap<>() {{
                        put("id", "n312kj3");
                        put("type", "employee");
                    }}),
                    new MapRecord(expectedRecordSchema, new HashMap<>() {{
                        put("id", "dl2kdff");
                        put("type", "security");
                    }})
        );

        testNestedReadRecords(yamlPath, expected, "accountIds");
    }

    @Test
    void testStartFromSimpleFieldReturnsEmptyJson() throws IOException, MalformedRecordException {
        String yamlPath = "src/test/resources/yaml/single-element-nested.yaml";

        testNestedReadRecords(yamlPath, Collections.emptyList(), "name");
    }

    @Test
    void testStartFromNonExistentFieldWithDefinedSchema() throws IOException, MalformedRecordException {
        String yamlPath = "src/test/resources/yaml/single-element-nested.yaml";

        SimpleRecordSchema expectedRecordSchema = new SimpleRecordSchema(getDefaultFields());
        List<Object> expected = Collections.emptyList();

        testNestedReadRecords(yamlPath, expectedRecordSchema, expected, "notfound", SchemaApplicationStrategy.SELECTED_PART);
    }

    @Test
    void testStartFromNestedFieldThenStartObject() throws IOException, MalformedRecordException {
        String yamlPath = "src/test/resources/yaml/nested-array-then-start-object.yaml";

        SimpleRecordSchema expectedRecordSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("balance", RecordFieldType.DOUBLE.getDataType())
        ));

        List<Object> expected = Arrays.asList(
                new MapRecord(expectedRecordSchema, new HashMap<>() {{
                    put("id", 42);
                    put("balance", 4750.89);
                }}),
                new MapRecord(expectedRecordSchema, new HashMap<>() {{
                    put("id", 43);
                    put("balance", 48212.38);
                }})
        );

        testNestedReadRecords(yamlPath, expectedRecordSchema, expected, "accounts", SchemaApplicationStrategy.SELECTED_PART);
    }

    @Test
    void testStartFromNestedObjectWithWholeJsonSchemaScope() throws IOException, MalformedRecordException {
        String yamlPath = "src/test/resources/yaml/single-element-nested.yaml";

        RecordSchema accountSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("balance", RecordFieldType.DOUBLE.getDataType())
        ));

        RecordSchema recordSchema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("account", RecordFieldType.RECORD.getRecordDataType(accountSchema))
        ));

        List<Object> expected = Collections.singletonList(
                new MapRecord(accountSchema, new HashMap<>() {{
                    put("id", 42);
                    put("balance", 4750.89);
                }})
        );

        testNestedReadRecords(yamlPath, recordSchema, expected, "account", SchemaApplicationStrategy.WHOLE_JSON);
    }

    @Test
    void testStartFromNestedArrayWithWholeJsonSchemaScope() throws IOException, MalformedRecordException {
        String yamlPath = "src/test/resources/yaml/single-element-nested-array.yaml";

        RecordSchema accountSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("balance", RecordFieldType.DOUBLE.getDataType())
        ));

        RecordSchema recordSchema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("accounts", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(accountSchema)))
        ));

        List<Object> expected = Arrays.asList(
                new MapRecord(accountSchema, new HashMap<>() {{
                    put("id", 42);
                    put("balance", 4750.89);
                }}),
                new MapRecord(accountSchema, new HashMap<>() {{
                    put("id", 43);
                    put("balance", 48212.38);
                }})
        );

        testNestedReadRecords(yamlPath, recordSchema, expected, "accounts", SchemaApplicationStrategy.WHOLE_JSON);
    }

    @Test
    void testStartFromDeepNestedObject() throws IOException, MalformedRecordException {
        String yamlPath = "src/test/resources/yaml/single-element-deep-nested.yaml";

        RecordSchema recordSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("rootInt", RecordFieldType.INT.getDataType()),
                new RecordField("rootString", RecordFieldType.STRING.getDataType()),
                new RecordField("nestedLevel1Record", RecordFieldType.RECORD.getRecordDataType(
                        new SimpleRecordSchema(Arrays.asList(
                                new RecordField("nestedLevel1Int", RecordFieldType.INT.getDataType()),
                                new RecordField("nestedLevel1String", RecordFieldType.STRING.getDataType()),
                                new RecordField("nestedLevel2Record", RecordFieldType.RECORD.getRecordDataType(
                                        new SimpleRecordSchema(Arrays.asList(
                                                new RecordField("nestedLevel2Int", RecordFieldType.INT.getDataType()),
                                                new RecordField("nestedLevel2String", RecordFieldType.STRING.getDataType())
                                        ))
                                ))
                        ))
                ))
        ));

        SimpleRecordSchema expectedRecordSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("nestedLevel2Int", RecordFieldType.INT.getDataType()),
                new RecordField("nestedLevel2String", RecordFieldType.STRING.getDataType())
        ));

        List<Object> expected = Collections.singletonList(
                new MapRecord(expectedRecordSchema, new HashMap<>() {{
                    put("nestedLevel2Int", 111);
                    put("nestedLevel2String", "root.level1.level2:string");
                }})
        );

        testNestedReadRecords(yamlPath, recordSchema, expected, "nestedLevel2Record", SchemaApplicationStrategy.WHOLE_JSON);
    }

    @Test
    void testCaptureFields() throws IOException, MalformedRecordException {
        Map<String, String> expectedCapturedFields = new HashMap<>();
        expectedCapturedFields.put("id", "1");
        expectedCapturedFields.put("zipCode", "11111");
        expectedCapturedFields.put("country", "USA");
        expectedCapturedFields.put("job", null);
        Set<String> fieldsToCapture = expectedCapturedFields.keySet();
        BiPredicate<String, String> capturePredicate = (fieldName, fieldValue) -> fieldsToCapture.contains(fieldName);
        String startingFieldName = "accounts";


        SimpleRecordSchema accountRecordSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("balance", RecordFieldType.DOUBLE.getDataType())
        ));

        SimpleRecordSchema jobRecordSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("salary", RecordFieldType.INT.getDataType()),
                new RecordField("position", RecordFieldType.STRING.getDataType())
        ));

        SimpleRecordSchema recordSchema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("accounts", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(accountRecordSchema))),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("address", RecordFieldType.STRING.getDataType()),
                new RecordField("city", RecordFieldType.STRING.getDataType()),
                new RecordField("job", RecordFieldType.RECORD.getRecordDataType(jobRecordSchema)),
                new RecordField("state", RecordFieldType.STRING.getDataType()),
                new RecordField("zipCode", RecordFieldType.STRING.getDataType()),
                new RecordField("country", RecordFieldType.STRING.getDataType())
        ));

        try (InputStream in = Files.newInputStream(Paths.get("src/test/resources/yaml/capture-fields.yaml"))) {
            YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(
                    in, mock(ComponentLog.class), recordSchema,
                    dateFormat, timeFormat, timestampFormat,
                    StartingFieldStrategy.NESTED_FIELD, startingFieldName,
                    SchemaApplicationStrategy.SELECTED_PART, capturePredicate);

            int records = 0;
            while (reader.nextRecord() != null) {
                records++;
            }
            assertNotEquals(0, records);

            Map<String, String> capturedFields = reader.getCapturedFields();

            assertEquals(expectedCapturedFields, capturedFields);
        }
    }

    private void testReadRecords(String yamlPath, List<Object> expected) throws IOException, MalformedRecordException {
        final File jsonFile = new File(yamlPath);
        try (
            InputStream jsonStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(jsonFile))
        ) {
            RecordSchema schema = inferSchema(jsonStream, StartingFieldStrategy.ROOT_NODE, null);
            testReadRecords(jsonStream, schema, expected);
        }
    }

    private void testNestedReadRecords(String yamlPath, List<Object> expected, String startingFieldName) throws IOException, MalformedRecordException {

        final File jsonFile = new File(yamlPath);
        try (InputStream jsonStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(jsonFile))) {
            RecordSchema schema = inferSchema(jsonStream, StartingFieldStrategy.NESTED_FIELD, startingFieldName);
            testNestedReadRecords(jsonStream, schema, expected, startingFieldName, SchemaApplicationStrategy.SELECTED_PART);
        }
    }

    private void testReadRecords(String yamlPath, RecordSchema schema, List<Object> expected) throws IOException, MalformedRecordException {
        final File jsonFile = new File(yamlPath);
        try (InputStream jsonStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(jsonFile))) {
            testReadRecords(jsonStream, schema, expected);
        }
    }

    private void testNestedReadRecords(String yamlPath,
                                 RecordSchema schema,
                                 List<Object> expected,
                                 String startingFieldName,
                                 SchemaApplicationStrategy schemaApplicationStrategy
    ) throws IOException, MalformedRecordException {

        final File yamlFile = new File(yamlPath);
        try (InputStream jsonStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(yamlFile))) {
            testNestedReadRecords(jsonStream, schema, expected, startingFieldName, schemaApplicationStrategy);
        }
    }

    private void testReadRecords(InputStream yamlStream, RecordSchema schema, List<Object> expected) throws IOException, MalformedRecordException {
        try (YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(yamlStream, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {
            List<Object> actual = new ArrayList<>();
            Record record;
            while ((record = reader.nextRecord()) != null) {
                List<Object> dataCollection = Arrays.asList((Object[]) record.getValue("dataCollection"));
                actual.addAll(dataCollection);
            }

            List<Function<Object, Object>> propertyProviders = Arrays.asList(
                _object -> ((Record) _object).getSchema(),
                _object -> Arrays.stream(((Record) _object).getValues()).map(value -> {
                    if (value != null && value.getClass().isArray()) {
                        return Arrays.asList((Object[]) value);
                    } else {
                        return value;
                    }
                }).collect(Collectors.toList())
            );

            List<EqualsWrapper<Object>> wrappedExpected = EqualsWrapper.wrapList(expected, propertyProviders);
            List<EqualsWrapper<Object>> wrappedActual = EqualsWrapper.wrapList(actual, propertyProviders);

            assertEquals(wrappedExpected, wrappedActual);
        }
    }

    private void testNestedReadRecords(InputStream yamlStream,
                                 RecordSchema schema,
                                 List<Object> expected,
                                 String startingFieldName,
                                 SchemaApplicationStrategy schemaApplicationStrategy)
            throws IOException, MalformedRecordException {

        try (YamlTreeRowRecordReader reader = new YamlTreeRowRecordReader(yamlStream, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat,
                StartingFieldStrategy.NESTED_FIELD, startingFieldName, schemaApplicationStrategy, null)) {
            List<Object> actual = new ArrayList<>();
            Record record;

            while ((record = reader.nextRecord()) != null) {
                actual.add(record);
            }

            List<Function<Object, Object>> propertyProviders = Arrays.asList(
                    _object -> ((Record) _object).getSchema(),
                    _object -> Arrays.stream(((Record) _object).getValues()).map(value -> {
                        if (value != null && value.getClass().isArray()) {
                            return Arrays.asList((Object[]) value);
                        } else {
                            return value;
                        }
                    }).collect(Collectors.toList())
            );

            List<EqualsWrapper<Object>> wrappedExpected = EqualsWrapper.wrapList(expected, propertyProviders);
            List<EqualsWrapper<Object>> wrappedActual = EqualsWrapper.wrapList(actual, propertyProviders);

            assertEquals(wrappedExpected, wrappedActual);
        }
    }

    private RecordSchema inferSchema(InputStream jsonStream, StartingFieldStrategy strategy, String startingFieldName) throws IOException {
        RecordSchema schema = new InferSchemaAccessStrategy<>(
            (__, inputStream) -> new JsonRecordSource(inputStream, strategy, startingFieldName, new YamlParserFactory()),
            new JsonSchemaInference(new TimeValueInference(null, null, null)),
            mock(ComponentLog.class)
        ).getSchema(Collections.emptyMap(), jsonStream, null);

        jsonStream.reset();

        return schema;
    }
}
