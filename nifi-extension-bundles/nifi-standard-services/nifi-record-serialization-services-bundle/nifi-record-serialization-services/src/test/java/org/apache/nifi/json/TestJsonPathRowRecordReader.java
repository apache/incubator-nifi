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

package org.apache.nifi.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestJsonPathRowRecordReader {
    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();

    private final LinkedHashMap<String, JsonPath> allJsonPaths = new LinkedHashMap<>();

    private final ObjectMapper mapper = new ObjectMapper();
    private final TokenParserFactory parserFactory = new JsonParserFactory();

    @BeforeEach
    public void populateJsonPaths() {
        allJsonPaths.clear();

        allJsonPaths.put("id", JsonPath.compile("$.id"));
        allJsonPaths.put("name", JsonPath.compile("$.name"));
        allJsonPaths.put("balance", JsonPath.compile("$.balance"));
        allJsonPaths.put("address", JsonPath.compile("$.address"));
        allJsonPaths.put("city", JsonPath.compile("$.city"));
        allJsonPaths.put("state", JsonPath.compile("$.state"));
        allJsonPaths.put("zipCode", JsonPath.compile("$.zipCode"));
        allJsonPaths.put("country", JsonPath.compile("$.country"));
    }


    private List<RecordField> getDefaultFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
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
    void testReadArray() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream("src/test/resources/json/bank-account-array.json");
             final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(allJsonPaths, schema, in, mock(ComponentLog.class), dateFormat, timeFormat, timestampFormat, mapper, parserFactory)) {

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
    void testReadOneLine() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream("src/test/resources/json/bank-account-oneline.json");
             final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(allJsonPaths, schema, in, mock(ComponentLog.class), dateFormat, timeFormat, timestampFormat, mapper, parserFactory)) {

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
    void testSingleJsonElement() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream("src/test/resources/json/single-bank-account.json");
             final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(allJsonPaths, schema, in, mock(ComponentLog.class), dateFormat, timeFormat, timestampFormat, mapper, parserFactory)) {

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
    void testTimestampCoercedFromString() throws IOException, MalformedRecordException {
        final List<RecordField> recordFields = Collections.singletonList(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(recordFields);

        final LinkedHashMap<String, JsonPath> jsonPaths = new LinkedHashMap<>();
        jsonPaths.put("timestamp", JsonPath.compile("$.timestamp"));
        jsonPaths.put("field_not_in_schema", JsonPath.compile("$.field_not_in_schema"));

        final String customFormat = "yyyy/MM/dd HH:mm:ss";
        for (final boolean coerceTypes : new boolean[] {true, false}) {
            try (final InputStream in = new FileInputStream("src/test/resources/json/timestamp.json");
                 final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(jsonPaths, schema, in, mock(ComponentLog.class), dateFormat, timeFormat, customFormat, mapper, parserFactory)) {

                final Record record = reader.nextRecord(coerceTypes, false);
                final Object value = record.getValue("timestamp");
                assertInstanceOf(Timestamp.class, value, "With coerceTypes set to " + coerceTypes + ", value is not a Timestamp");

                final Object valueNotInSchema = record.getValue("field_not_in_schema");
                assertInstanceOf(String.class, valueNotInSchema, "field_not_in_schema should be String");
            }
        }
    }



    @Test
    void testElementWithNestedData() throws IOException, MalformedRecordException {
        final LinkedHashMap<String, JsonPath> jsonPaths = new LinkedHashMap<>(allJsonPaths);
        jsonPaths.put("account", JsonPath.compile("$.account"));

        final DataType accountType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("account", accountType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = new FileInputStream("src/test/resources/json/single-element-nested.json");
             final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(jsonPaths, schema, in, mock(ComponentLog.class), dateFormat, timeFormat, timestampFormat, mapper, parserFactory)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList("id", "name", "balance", "address", "city", "state", "zipCode", "country", "account");
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(DataType::getFieldType).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING, RecordFieldType.DOUBLE,
                    RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.RECORD);
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            final Object[] simpleElements = Arrays.copyOfRange(firstRecordValues, 0, firstRecordValues.length - 1);
            assertArrayEquals(new Object[] {1, "John Doe", null, "123 My Street", "My City", "MS", "11111", "USA"}, simpleElements);

            final Object lastElement = firstRecordValues[firstRecordValues.length - 1];
            assertInstanceOf(Record.class, lastElement);
            final Record record = (Record) lastElement;
            assertEquals(42, record.getValue("id"));
            assertEquals(4750.89D, record.getValue("balance"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testElementWithNestedArray() throws IOException, MalformedRecordException {
        final LinkedHashMap<String, JsonPath> jsonPaths = new LinkedHashMap<>(allJsonPaths);
        jsonPaths.put("accounts", JsonPath.compile("$.accounts"));

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);
        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);


        try (final InputStream in = new FileInputStream("src/test/resources/json/single-element-nested-array.json");
             final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(jsonPaths, schema, in, mock(ComponentLog.class), dateFormat, timeFormat, timestampFormat, mapper, parserFactory)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList("id", "name", "balance", "address", "city", "state", "zipCode", "country", "accounts");
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(DataType::getFieldType).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING, RecordFieldType.DOUBLE,
                    RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.ARRAY);
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            final Object[] nonArrayValues = Arrays.copyOfRange(firstRecordValues, 0, firstRecordValues.length - 1);
            assertArrayEquals(new Object[] {1, "John Doe", null, "123 My Street", "My City", "MS", "11111", "USA"}, nonArrayValues);

            final Object lastRecord = firstRecordValues[firstRecordValues.length - 1];
            assertTrue(Object[].class.isAssignableFrom(lastRecord.getClass()));

            final Object[] array = (Object[]) lastRecord;
            assertEquals(2, array.length);
            final Object firstElement = array[0];
            assertInstanceOf(Record.class, firstElement);

            final Record firstRecord = (Record) firstElement;
            assertEquals(42, firstRecord.getValue("id"));
            assertEquals(4750.89D, firstRecord.getValue("balance"));

            final Object secondElement = array[1];
            assertInstanceOf(Record.class, secondElement);
            final Record secondRecord = (Record) secondElement;
            assertEquals(43, secondRecord.getValue("id"));
            assertEquals(48212.38D, secondRecord.getValue("balance"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testReadArrayDifferentSchemas() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream("src/test/resources/json/bank-account-array-different-schemas.json");
             final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(allJsonPaths, schema, in, mock(ComponentLog.class), dateFormat, timeFormat, timestampFormat, mapper, parserFactory)) {

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
    void testReadArrayDifferentSchemasWithOverride() throws IOException, MalformedRecordException {
        final LinkedHashMap<String, JsonPath> jsonPaths = new LinkedHashMap<>(allJsonPaths);
        jsonPaths.put("address2", JsonPath.compile("$.address2"));

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("address2", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);


        try (final InputStream in = new FileInputStream("src/test/resources/json/bank-account-array-different-schemas.json");
             final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(jsonPaths, schema, in, mock(ComponentLog.class), dateFormat, timeFormat, timestampFormat, mapper, parserFactory)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList("id", "name", "balance", "address", "city", "state", "zipCode", "country", "address2");
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(DataType::getFieldType).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING, RecordFieldType.DOUBLE, RecordFieldType.STRING,
                    RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING);
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA", null}, firstRecordValues);

            final Object[] secondRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {2, "Jane Doe", 4820.09, "321 Your Street", "Your City", "NY", "33333", null, null}, secondRecordValues);

            final Object[] thirdRecordValues = reader.nextRecord().getValues();
            assertArrayEquals(new Object[] {3, "Jake Doe", 4751.89, "124 My Street", "My City", "MS", "11111", "USA", "Apt. #12"}, thirdRecordValues);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    void testPrimitiveTypeArrays() throws IOException, MalformedRecordException {
        final LinkedHashMap<String, JsonPath> jsonPaths = new LinkedHashMap<>(allJsonPaths);
        jsonPaths.put("accountIds", JsonPath.compile("$.accountIds"));

        final List<RecordField> fields = getDefaultFields();
        final DataType idsType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType());
        fields.add(new RecordField("accountIds", idsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = new FileInputStream("src/test/resources/json/primitive-type-array.json");
             final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(jsonPaths, schema, in, mock(ComponentLog.class), dateFormat, timeFormat, timestampFormat, mapper, parserFactory)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList("id", "name", "balance", "address", "city", "state", "zipCode", "country", "accountIds");
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(DataType::getFieldType).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(RecordFieldType.INT, RecordFieldType.STRING, RecordFieldType.DOUBLE, RecordFieldType.STRING,
                    RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.ARRAY);
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();

            final Object[] nonArrayValues = Arrays.copyOfRange(firstRecordValues, 0, firstRecordValues.length - 1);
            assertArrayEquals(new Object[] {1, "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"}, nonArrayValues);

            final Object lastRecord = firstRecordValues[firstRecordValues.length - 1];
            assertNotNull(lastRecord);
            assertTrue(Object[].class.isAssignableFrom(lastRecord.getClass()));

            final Object[] array = (Object[]) lastRecord;
            assertArrayEquals(new Object[] {1, 2, 3}, array);

            assertNull(reader.nextRecord());
            assertNull(reader.nextRecord());
        }
    }
}
