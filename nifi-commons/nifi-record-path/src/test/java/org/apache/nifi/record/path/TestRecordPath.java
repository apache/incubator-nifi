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

package org.apache.nifi.record.path;

import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.nifi.record.path.exception.RecordPathException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.uuid5.Uuid5Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public class TestRecordPath {

    private static final String USER_TIMEZONE_PROPERTY = "user.timezone";

    private static final String SYSTEM_TIMEZONE = System.getProperty(USER_TIMEZONE_PROPERTY);

    private static final String TEST_TIMEZONE = "America/Phoenix";

    private static final int TEST_OFFSET_HOURS = 2;

    private static final String TEST_TIMEZONE_OFFSET = String.format("GMT+0%d:00", TEST_OFFSET_HOURS);

    @BeforeAll
    public static void setTestTimezone() {
        System.setProperty(USER_TIMEZONE_PROPERTY, TEST_TIMEZONE);
    }

    @AfterAll
    public static void setSystemTimezone() {
        if (SYSTEM_TIMEZONE != null) {
            System.setProperty(USER_TIMEZONE_PROPERTY, SYSTEM_TIMEZONE);
        }
    }

    @Test
    public void testCompile() {
        RecordPath.compile("/person/name/last");
        RecordPath.compile("/person[2]");
        RecordPath.compile("//person[2]");
        RecordPath.compile("/person/child[1]//sibling/name");

        // contains is a 'filter function' so can be used as the predicate
        RecordPath.compile("/name[contains(., 'hello')]");

        // substring is not a filter function so cannot be used as a predicate
        assertThrows(RecordPathException.class, () -> RecordPath.compile("/name[substring(., 1, 2)]"));

        // substring is not a filter function so can be used as *part* of a predicate but not as the entire predicate
        RecordPath.compile("/name[substring(., 1, 2) = 'e']");
    }

    @Test
    public void testChildField() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        final Record record = new MapRecord(schema, values);

        assertEquals(48, evaluateSingleFieldValue("/id", record).getValue());
        assertEquals(record, evaluateSingleFieldValue("/id", record).getParentRecord().get());

        assertEquals("John Doe", evaluateSingleFieldValue("/name", record).getValue());
        assertEquals(record, evaluateSingleFieldValue("/name", record).getParentRecord().get());

        assertEquals(accountRecord, evaluateSingleFieldValue("/mainAccount", record).getValue());
        assertEquals(record, evaluateSingleFieldValue("/mainAccount", record).getParentRecord().get());

        assertEquals(1, evaluateSingleFieldValue("/mainAccount/id", record).getValue());
        assertEquals(accountRecord, evaluateSingleFieldValue("/mainAccount/id", record).getParentRecord().get());

        assertEquals(123.45D, evaluateSingleFieldValue("/mainAccount/balance", record).getValue());
        assertEquals(accountRecord, evaluateSingleFieldValue("/mainAccount/id", record).getParentRecord().get());
    }

    @Test
    public void testRootRecord() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = evaluateSingleFieldValue("/", record);
        assertEquals(Optional.empty(), fieldValue.getParent());
        assertEquals(record, fieldValue.getValue());
    }

    @Test
    public void testWildcardChild() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/mainAccount/*").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(2, fieldValues.size());

        for (final FieldValue fieldValue : fieldValues) {
            assertEquals(accountRecord, fieldValue.getParentRecord().get());
        }

        assertEquals("id", fieldValues.get(0).getField().getFieldName());
        assertEquals(1, fieldValues.get(0).getValue());

        assertEquals("balance", fieldValues.get(1).getField().getFieldName());
        assertEquals(123.45D, fieldValues.get(1).getValue());

        RecordPath.compile("/mainAccount/*[. > 100]").evaluate(record).getSelectedFields().forEach(field -> field.updateValue(122.44D));
        assertEquals(1, accountValues.get("id"));
        assertEquals(122.44D, accountValues.get("balance"));
    }

    @Test
    public void testWildcardWithArray() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("accounts", new Object[] {accountRecord});
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/*[0]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("accounts", fieldValue.getField().getFieldName());
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals(accountRecord, fieldValue.getValue());

        final Map<String, Object> updatedAccountValues = new HashMap<>(accountValues);
        updatedAccountValues.put("balance", 122.44D);
        final Record updatedAccountRecord = new MapRecord(getAccountSchema(), updatedAccountValues);
        RecordPath.compile("/*[0]").evaluate(record).getSelectedFields().forEach(field -> field.updateValue(updatedAccountRecord));

        final Object[] accountRecords = (Object[]) record.getValue("accounts");
        assertEquals(1, accountRecords.length);
        final Record recordToVerify = (Record) accountRecords[0];
        assertEquals(122.44D, recordToVerify.getValue("balance"));
        assertEquals(48, record.getValue("id"));
        assertEquals("John Doe", record.getValue("name"));
    }

    @Test
    public void testDescendantField() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("//id").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(2, fieldValues.size());

        final FieldValue first = fieldValues.get(0);
        final FieldValue second = fieldValues.get(1);

        assertEquals(RecordFieldType.INT, first.getField().getDataType().getFieldType());
        assertEquals(RecordFieldType.INT, second.getField().getDataType().getFieldType());

        assertEquals(48, first.getValue());
        assertEquals(1, second.getValue());
    }

    @Test
    public void testParent() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("//id/..").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(2, fieldValues.size());

        final FieldValue first = fieldValues.get(0);
        final FieldValue second = fieldValues.get(1);

        assertEquals(RecordFieldType.RECORD, first.getField().getDataType().getFieldType());
        assertEquals(RecordFieldType.RECORD, second.getField().getDataType().getFieldType());

        assertEquals(record, first.getValue());
        assertEquals(accountRecord, second.getValue());
    }

    @Test
    public void testMapKey() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("city", "New York");
        attributes.put("state", "NY");

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("attributes", attributes);
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = evaluateSingleFieldValue("/attributes['city']", record);
        assertEquals("attributes", fieldValue.getField().getFieldName());
        assertEquals("New York", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    public void testMapKeyReferencedWithCurrentField() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("city", "New York");
        attributes.put("state", "NY");

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("attributes", attributes);
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = evaluateSingleFieldValue("/attributes/.['city']", record);
        assertEquals("attributes", fieldValue.getField().getFieldName());
        assertEquals("New York", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpdateMap() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("city", "New York");
        attributes.put("state", "NY");

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("attributes", attributes);
        final Record record = new MapRecord(schema, values);

        evaluateSingleFieldValue("/attributes['city']", record).updateValue("Boston");
        assertEquals("Boston", ((Map<String, Object>) record.getValue("attributes")).get("city"));
    }

    @Test
    public void testMapWildcard() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("city", "New York");
        attributes.put("state", "NY");

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("attributes", attributes);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/attributes[*]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(2, fieldValues.size());

        assertEquals("New York", fieldValues.get(0).getValue());
        assertEquals("NY", fieldValues.get(1).getValue());

        for (final FieldValue fieldValue : fieldValues) {
            assertEquals("attributes", fieldValue.getField().getFieldName());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        RecordPath.compile("/attributes[*]").evaluate(record).getSelectedFields().forEach(field -> field.updateValue("Unknown"));
        assertEquals("Unknown", attributes.get("city"));
        assertEquals("Unknown", attributes.get("state"));

        RecordPath.compile("/attributes[*][fieldName(.) = 'attributes']").evaluate(record).getSelectedFields().forEach(field -> field.updateValue("Unknown"));
        assertEquals("Unknown", attributes.get("city"));
        assertEquals("Unknown", attributes.get("state"));

    }

    @Test
    public void testMapMultiKey() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("city", "New York");
        attributes.put("state", "NY");

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("attributes", attributes);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/attributes['city', 'state']").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(2, fieldValues.size());

        assertEquals("New York", fieldValues.get(0).getValue());
        assertEquals("NY", fieldValues.get(1).getValue());

        for (final FieldValue fieldValue : fieldValues) {
            assertEquals("attributes", fieldValue.getField().getFieldName());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        RecordPath.compile("/attributes['city', 'state']").evaluate(record).getSelectedFields().forEach(field -> field.updateValue("Unknown"));
        assertEquals("Unknown", attributes.get("city"));
        assertEquals("Unknown", attributes.get("state"));
    }

    @Test
    public void testEscapedFieldName() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name,date", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name,date", "John Doe");
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = evaluateSingleFieldValue("/'name,date'", record);
        assertEquals("name,date", fieldValue.getField().getFieldName());
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    public void testSingleArrayIndex() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("numbers", new Object[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = evaluateSingleFieldValue("/numbers[3]", record);
        assertEquals("numbers", fieldValue.getField().getFieldName());
        assertEquals(3, fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    public void testSingleArrayRange() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("numbers", new Object[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/numbers[0..1]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        for (final FieldValue fieldValue : fieldValues) {
            assertEquals("numbers", fieldValue.getField().getFieldName());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        assertEquals(2, fieldValues.size());
        for (int i = 0; i < 1; i++) {
            assertEquals(i, fieldValues.get(0).getValue());
        }
    }


    @Test
    public void testMultiArrayIndex() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("numbers", new Object[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/numbers[3,6, -1, -2]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        int i = 0;
        final int[] expectedValues = new int[] {3, 6, 9, 8};
        for (final FieldValue fieldValue : fieldValues) {
            assertEquals("numbers", fieldValue.getField().getFieldName());
            assertEquals(expectedValues[i++], fieldValue.getValue());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        RecordPath.compile("/numbers[3,6, -1, -2]").evaluate(record).getSelectedFields().forEach(field -> field.updateValue(99));
        assertArrayEquals(new Object[] {0, 1, 2, 99, 4, 5, 99, 7, 99, 99}, (Object[]) values.get("numbers"));
    }

    @Test
    public void testMultiArrayIndexWithRanges() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("numbers", new Object[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        final Record record = new MapRecord(schema, values);

        List<FieldValue> fieldValues = RecordPath.compile("/numbers[0, 2, 4..7, 9]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        for (final FieldValue fieldValue : fieldValues) {
            assertEquals("numbers", fieldValue.getField().getFieldName());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        int[] expectedValues = new int[] {0, 2, 4, 5, 6, 7, 9};
        assertEquals(expectedValues.length, fieldValues.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], fieldValues.get(i).getValue());
        }

        fieldValues = RecordPath.compile("/numbers[0..-1]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        for (final FieldValue fieldValue : fieldValues) {
            assertEquals("numbers", fieldValue.getField().getFieldName());
            assertEquals(record, fieldValue.getParentRecord().get());
        }
        expectedValues = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        assertEquals(expectedValues.length, fieldValues.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], fieldValues.get(i).getValue());
        }


        fieldValues = RecordPath.compile("/numbers[-1..-1]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        for (final FieldValue fieldValue : fieldValues) {
            assertEquals("numbers", fieldValue.getField().getFieldName());
            assertEquals(record, fieldValue.getParentRecord().get());
        }
        expectedValues = new int[] {9};
        assertEquals(expectedValues.length, fieldValues.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], fieldValues.get(i).getValue());
        }

        fieldValues = RecordPath.compile("/numbers[*]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        for (final FieldValue fieldValue : fieldValues) {
            assertEquals("numbers", fieldValue.getField().getFieldName());
            assertEquals(record, fieldValue.getParentRecord().get());
        }
        expectedValues = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        assertEquals(expectedValues.length, fieldValues.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], fieldValues.get(i).getValue());
        }

        fieldValues = RecordPath.compile("/xx[1,2,3]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(0, fieldValues.size());
    }

    @Test
    public void testEqualsPredicate() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        values.put("numbers", new Object[] {1, 2, 3, 4, 4, 4, 5});
        final Record record = new MapRecord(schema, values);


        List<FieldValue> fieldValues = RecordPath.compile("/numbers[0..-1][. = 4]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(3, fieldValues.size());

        for (final FieldValue fieldValue : fieldValues) {
            final String fieldName = fieldValue.getField().getFieldName();
            assertEquals("numbers", fieldName);
            assertEquals(RecordFieldType.INT, fieldValue.getField().getDataType().getFieldType());
            assertEquals(4, fieldValue.getValue());
            assertEquals(record, fieldValue.getParentRecord().get());
        }

        fieldValues = RecordPath.compile("//id[. = 48]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());
        final FieldValue fieldValue = fieldValues.get(0);

        assertEquals("id", fieldValue.getField().getFieldName());
        assertEquals(RecordFieldType.INT.getDataType(), fieldValue.getField().getDataType());
        assertEquals(48, fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
    }

    @Test
    public void testRelativePath() {
        final Map<String, Object> accountValues = new HashMap<>();
        accountValues.put("id", 1);
        accountValues.put("balance", 123.45D);
        final Record accountRecord = new MapRecord(getAccountSchema(), accountValues);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("mainAccount", accountRecord);
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/mainAccount/././balance/.").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals(accountRecord, fieldValue.getParentRecord().get());
        assertEquals(123.45D, fieldValue.getValue());
        assertEquals("balance", fieldValue.getField().getFieldName());

        RecordPath.compile("/mainAccount/././balance/.").evaluate(record).getSelectedFields().forEach(field -> field.updateValue(123.44D));
        assertEquals(123.44D, accountValues.get("balance"));
    }

    @Test
    public void testCompareToLiteral() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("numbers", new Object[] {0, 1, 2});
        final Record record = new MapRecord(schema, values);

        List<FieldValue> fieldValues = RecordPath.compile("/id[. > 42]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        fieldValues = RecordPath.compile("/id[. < 42]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(0, fieldValues.size());
    }

    @Test
    public void testCompareToAbsolute() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("numbers", new Object[] {0, 1, 2});
        final Record record = new MapRecord(schema, values);

        List<FieldValue> fieldValues = RecordPath.compile("/numbers[0..-1][. < /id]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(3, fieldValues.size());

        fieldValues = RecordPath.compile("/id[. > /numbers[-1]]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());
    }

    @Test
    public void testCompareWithEmbeddedPaths() {
        final Map<String, Object> accountValues1 = new HashMap<>();
        accountValues1.put("id", 1);
        accountValues1.put("balance", 10_000.00D);
        final Record accountRecord1 = new MapRecord(getAccountSchema(), accountValues1);

        final Map<String, Object> accountValues2 = new HashMap<>();
        accountValues2.put("id", 2);
        accountValues2.put("balance", 48.02D);
        final Record accountRecord2 = new MapRecord(getAccountSchema(), accountValues2);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("accounts", new Object[] {accountRecord1, accountRecord2});
        final Record record = new MapRecord(schema, values);

        final RecordPath recordPath = RecordPath.compile("/accounts[0..-1][./balance > 100]");
        List<FieldValue> fieldValues = recordPath.evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("accounts", fieldValue.getField().getFieldName());
        assertEquals(0, ((ArrayIndexFieldValue) fieldValue).getArrayIndex());
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals(accountRecord1, fieldValue.getValue());
    }

    @Test
    public void testPredicateInMiddleOfPath() {
        final Map<String, Object> accountValues1 = new HashMap<>();
        accountValues1.put("id", 1);
        accountValues1.put("balance", 10_000.00D);
        final Record accountRecord1 = new MapRecord(getAccountSchema(), accountValues1);

        final Map<String, Object> accountValues2 = new HashMap<>();
        accountValues2.put("id", 2);
        accountValues2.put("balance", 48.02D);
        final Record accountRecord2 = new MapRecord(getAccountSchema(), accountValues2);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("accounts", new Object[] {accountRecord1, accountRecord2});
        final Record record = new MapRecord(schema, values);

        final RecordPath recordPath = RecordPath.compile("/accounts[0..-1][./balance > 100]/id");
        List<FieldValue> fieldValues = recordPath.evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("id", fieldValue.getField().getFieldName());
        assertEquals(accountRecord1, fieldValue.getParentRecord().get());
        assertEquals(1, fieldValue.getValue());
    }

    @Test
    public void testUpdateValueOnMatchingFields() {
        final Map<String, Object> accountValues1 = new HashMap<>();
        accountValues1.put("id", 1);
        accountValues1.put("balance", 10_000.00D);
        final Record accountRecord1 = new MapRecord(getAccountSchema(), accountValues1);

        final Map<String, Object> accountValues2 = new HashMap<>();
        accountValues2.put("id", 2);
        accountValues2.put("balance", 48.02D);
        final Record accountRecord2 = new MapRecord(getAccountSchema(), accountValues2);

        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        values.put("accounts", new Object[] {accountRecord1, accountRecord2});
        final Record record = new MapRecord(schema, values);

        evaluateSingleFieldValue("/accounts[0..-1][./balance > 100]/id", record).updateValue(100);

        assertEquals(48, record.getValue("id"));
        assertEquals(100, accountRecord1.getValue("id"));
        assertEquals(2, accountRecord2.getValue("id"));
    }

    @Test
    public void testPredicateDoesNotIncludeFieldsThatDontHaveRelativePath() {
        final List<RecordField> addressFields = new ArrayList<>();
        addressFields.add(new RecordField("city", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("state", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("zip", RecordFieldType.STRING.getDataType()));
        final RecordSchema addressSchema = new SimpleRecordSchema(addressFields);

        final List<RecordField> detailsFields = new ArrayList<>();
        detailsFields.add(new RecordField("position", RecordFieldType.STRING.getDataType()));
        detailsFields.add(new RecordField("managerName", RecordFieldType.STRING.getDataType()));
        final RecordSchema detailsSchema = new SimpleRecordSchema(detailsFields);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("address", RecordFieldType.RECORD.getRecordDataType(addressSchema)));
        fields.add(new RecordField("details", RecordFieldType.RECORD.getRecordDataType(detailsSchema)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final Record record = new MapRecord(recordSchema, new HashMap<>());
        record.setValue("name", "John Doe");

        final Record addressRecord = new MapRecord(addressSchema, new HashMap<>());
        addressRecord.setValue("city", "San Francisco");
        addressRecord.setValue("state", "CA");
        addressRecord.setValue("zip", "12345");
        record.setValue("address", addressRecord);

        final Record detailsRecord = new MapRecord(detailsSchema, new HashMap<>());
        detailsRecord.setValue("position", "Developer");
        detailsRecord.setValue("managerName", "Jane Doe");
        record.setValue("details", detailsRecord);

        final RecordPath recordPath = RecordPath.compile("/*[./state != 'NY']");
        final RecordPathResult result = recordPath.evaluate(record);
        final List<FieldValue> fieldValues = result.getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("address", fieldValue.getField().getFieldName());

        assertEquals("12345", evaluateSingleFieldValue("/*[./state != 'NY']/zip", record).getValue());
    }

    @Test
    public void testPredicateWithAbsolutePath() {
        final List<RecordField> addressFields = new ArrayList<>();
        addressFields.add(new RecordField("city", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("state", RecordFieldType.STRING.getDataType()));
        addressFields.add(new RecordField("zip", RecordFieldType.STRING.getDataType()));
        final RecordSchema addressSchema = new SimpleRecordSchema(addressFields);

        final List<RecordField> detailsFields = new ArrayList<>();
        detailsFields.add(new RecordField("position", RecordFieldType.STRING.getDataType()));
        detailsFields.add(new RecordField("preferredState", RecordFieldType.STRING.getDataType()));
        final RecordSchema detailsSchema = new SimpleRecordSchema(detailsFields);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("address1", RecordFieldType.RECORD.getRecordDataType(addressSchema)));
        fields.add(new RecordField("address2", RecordFieldType.RECORD.getRecordDataType(addressSchema)));
        fields.add(new RecordField("details", RecordFieldType.RECORD.getRecordDataType(detailsSchema)));
        final RecordSchema recordSchema = new SimpleRecordSchema(fields);

        final Record record = new MapRecord(recordSchema, new HashMap<>());
        record.setValue("name", "John Doe");

        final Record address1Record = new MapRecord(addressSchema, new HashMap<>());
        address1Record.setValue("city", "San Francisco");
        address1Record.setValue("state", "CA");
        address1Record.setValue("zip", "12345");
        record.setValue("address1", address1Record);

        final Record address2Record = new MapRecord(addressSchema, new HashMap<>());
        address2Record.setValue("city", "New York");
        address2Record.setValue("state", "NY");
        address2Record.setValue("zip", "01234");
        record.setValue("address2", address2Record);

        final Record detailsRecord = new MapRecord(detailsSchema, new HashMap<>());
        detailsRecord.setValue("position", "Developer");
        detailsRecord.setValue("preferredState", "NY");
        record.setValue("details", detailsRecord);

        final RecordPath recordPath = RecordPath.compile("/*[./state = /details/preferredState]");
        final RecordPathResult result = recordPath.evaluate(record);
        final List<FieldValue> fieldValues = result.getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("address2", fieldValue.getField().getFieldName());
    }

    @Test
    public void testRelativePathOnly() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        final FieldValue recordFieldValue = new StandardFieldValue(record, new RecordField("record", RecordFieldType.RECORD.getDataType()), null);

        final List<FieldValue> fieldValues = RecordPath.compile("./name").evaluate(record, recordFieldValue).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals("name", fieldValue.getField().getFieldName());
    }

    @Test
    public void testRelativePathAgainstNonRecordField() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());
        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        final FieldValue recordFieldValue = new StandardFieldValue(record, new RecordField("root", RecordFieldType.RECORD.getRecordDataType(record.getSchema())), null);
        final FieldValue nameFieldValue = new StandardFieldValue("John Doe", new RecordField("name", RecordFieldType.STRING.getDataType()), recordFieldValue);

        final List<FieldValue> fieldValues = RecordPath.compile(".").evaluate(record, nameFieldValue).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());

        final FieldValue fieldValue = fieldValues.get(0);
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals(record, fieldValue.getParentRecord().get());
        assertEquals("name", fieldValue.getField().getFieldName());

        fieldValue.updateValue("Jane Doe");
        assertEquals("Jane Doe", record.getValue("name"));
    }

    @Test
    public void testSubstringFunction() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = evaluateSingleFieldValue("substring(/name, 0, 4)", record);
        assertEquals("John", fieldValue.getValue());

        assertEquals("John", evaluateSingleFieldValue("substring(/name, 0, -5)", record).getValue());
        assertEquals("", evaluateSingleFieldValue("substring(/name, 1000, 1005)", record).getValue());
        assertEquals("", evaluateSingleFieldValue("substring(/name, 4, 3)", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("substring(/name, 0, 10000)", record).getValue());
        assertEquals("", evaluateSingleFieldValue("substring(/name, -50, -1)", record).getValue());
    }

    @Test
    public void testSubstringBeforeFunction() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("John", evaluateSingleFieldValue("substringBefore(/name, ' ')", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("substringBefore(/name, 'XYZ')", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("substringBefore(/name, '')", record).getValue());

        assertEquals("John D", evaluateSingleFieldValue("substringBeforeLast(/name, 'o')", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("substringBeforeLast(/name, 'XYZ')", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("substringBeforeLast(/name, '')", record).getValue());
    }

    @Test
    public void testSubstringAfterFunction() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("hn Doe", evaluateSingleFieldValue("substringAfter(/name, 'o')", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("substringAfter(/name, 'XYZ')", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("substringAfter(/name, '')", record).getValue());
        assertEquals("n Doe", evaluateSingleFieldValue("substringAfter(/name, 'oh')", record).getValue());

        assertEquals("e", evaluateSingleFieldValue("substringAfterLast(/name, 'o')", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("substringAfterLast(/name, 'XYZ')", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("substringAfterLast(/name, '')", record).getValue());
        assertEquals("n Doe", evaluateSingleFieldValue("substringAfterLast(/name, 'oh')", record).getValue());
    }

    @Test
    public void testContains() {
        final Record record = createSimpleRecord();
        assertEquals("John Doe", evaluateSingleFieldValue("/name[contains(., 'o')]", record).getValue());
        assertEquals(0L, RecordPath.compile("/name[contains(., 'x')]").evaluate(record).getSelectedFields().count());

        record.setValue("name", "John Doe 48");
        assertEquals("John Doe 48", evaluateSingleFieldValue("/name[contains(., /id)]", record).getValue());
    }

    @Test
    public void testStartsWith() {
        final Record record = createSimpleRecord();
        assertEquals("John Doe", evaluateSingleFieldValue("/name[startsWith(., 'J')]", record).getValue());
        assertEquals(0L, RecordPath.compile("/name[startsWith(., 'x')]").evaluate(record).getSelectedFields().count());
        assertEquals("John Doe", evaluateSingleFieldValue("/name[startsWith(., '')]", record).getValue());
    }

    @Test
    public void testEndsWith() {
        final Record record = createSimpleRecord();
        assertEquals("John Doe", evaluateSingleFieldValue("/name[endsWith(., 'e')]", record).getValue());
        assertEquals(0L, RecordPath.compile("/name[endsWith(., 'x')]").evaluate(record).getSelectedFields().count());
        assertEquals("John Doe", evaluateSingleFieldValue("/name[endsWith(., '')]", record).getValue());
    }

    @Test
    public void testIsEmpty() {
        final Record record = createSimpleRecord();
        assertEquals("John Doe", evaluateSingleFieldValue("/name[isEmpty(../missing)]", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("/name[isEmpty(/missing)]", record).getValue());
        assertEquals(0L, RecordPath.compile("/name[isEmpty(../id)]").evaluate(record).getSelectedFields().count());

        record.setValue("missing", "   ");
        assertEquals(0L, RecordPath.compile("/name[isEmpty(/missing)]").evaluate(record).getSelectedFields().count());
    }


    @Test
    public void testIsBlank() {
        final Record record = createSimpleRecord();
        assertEquals("John Doe", evaluateSingleFieldValue("/name[isBlank(../missing)]", record).getValue());

        record.setValue("missing", "   ");
        assertEquals("John Doe", evaluateSingleFieldValue("/name[isBlank(../missing)]", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("/name[isBlank(/missing)]", record).getValue());
        assertEquals(0L, RecordPath.compile("/name[isBlank(../id)]").evaluate(record).getSelectedFields().count());
    }

    @Test
    public void testContainsRegex() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Doe", evaluateSingleFieldValue("/name[containsRegex(., 'o')]", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("/name[containsRegex(., '[xo]')]", record).getValue());
        assertEquals(0L, RecordPath.compile("/name[containsRegex(., 'x')]").evaluate(record).getSelectedFields().count());
    }

    @Test
    public void testNot() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Doe", evaluateSingleFieldValue("/name[not(contains(., 'x'))]", record).getValue());
        assertEquals(0L, RecordPath.compile("/name[not(. = 'John Doe')]").evaluate(record).getSelectedFields().count());
        assertEquals("John Doe", evaluateSingleFieldValue("/name[not(. = 'Jane Doe')]", record).getValue());
    }

    @Test
    public void testChainingFunctions() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Doe", evaluateSingleFieldValue("/name[contains(substringAfter(., 'o'), 'h')]", record).getValue());
    }



    @Test
    public void testMatchesRegex() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals(0L, RecordPath.compile("/name[matchesRegex(., 'John D')]").evaluate(record).getSelectedFields().count());
        assertEquals("John Doe", evaluateSingleFieldValue("/name[matchesRegex(., '[John Doe]{8}')]", record).getValue());
    }

    @Test
    public void testReplace() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Doe", evaluateSingleFieldValue("/name[replace(../id, 48, 18) = 18]", record).getValue());
        assertEquals(0L, RecordPath.compile("/name[replace(../id, 48, 18) = 48]").evaluate(record).getSelectedFields().count());

        assertEquals("Jane Doe", evaluateSingleFieldValue("replace(/name, 'ohn', 'ane')", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("replace(/name, 'ohnny', 'ane')", record).getValue());

        assertEquals("John 48", evaluateSingleFieldValue("replace(/name, 'Doe', /id)", record).getValue());
        assertEquals("23", evaluateSingleFieldValue("replace(/id, 48, 23)", record).getValue());
    }

    @Test
    public void testReplaceRegex() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("ohn oe", evaluateSingleFieldValue("replaceRegex(/name, '[JD]', '')", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("replaceRegex(/name, 'ohnny', 'ane')", record).getValue());

        assertEquals("11", evaluateSingleFieldValue("replaceRegex(/id, '[0-9]', 1)", record).getValue());

        assertEquals("Jxohn Dxoe", evaluateSingleFieldValue("replaceRegex(/name, '([JD])', '$1x')", record).getValue());

        assertEquals("Jxohn Dxoe", evaluateSingleFieldValue("replaceRegex(/name, '(?<hello>[JD])', '${hello}x')", record).getValue());

        assertEquals("48ohn 48oe", evaluateSingleFieldValue("replaceRegex(/name, '(?<hello>[JD])', /id)", record).getValue());

    }

    @Test
    public void testReplaceRegexEscapedCharacters() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        final Record record = new MapRecord(schema, values);

        // Special character cases
        values.put("name", "John Doe");
        assertEquals("John\nDoe", evaluateSingleFieldValue("replaceRegex(/name, '[\\s]', '\\n')", record).getValue(),
                "Replacing whitespace to new line");

        values.put("name", "John\nDoe");
        assertEquals("John Doe", evaluateSingleFieldValue("replaceRegex(/name, '\\n', ' ')", record).getValue(),
                "Replacing new line to whitespace");

        values.put("name", "John Doe");
        assertEquals("John\tDoe", evaluateSingleFieldValue("replaceRegex(/name, '[\\s]', '\\t')", record).getValue(),
                "Replacing whitespace to tab");

        values.put("name", "John\tDoe");
        assertEquals("John Doe", evaluateSingleFieldValue("replaceRegex(/name, '\\t', ' ')", record).getValue(),
                "Replacing tab to whitespace");

    }

    @Test
    public void testReplaceRegexEscapedQuotes() {

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        final Record record = new MapRecord(schema, values);

        // Quotes
        // NOTE: At Java code, a single back-slash needs to be escaped with another-back slash, but needn't do so at NiFi UI.
        //       The test record path is equivalent to replaceRegex(/name, '\'', '"')
        values.put("name", "'John' 'Doe'");
        assertEquals("\"John\" \"Doe\"", evaluateSingleFieldValue("replaceRegex(/name, '\\'', '\"')", record).getValue(),
                "Replacing quote to double-quote");

        values.put("name", "\"John\" \"Doe\"");
        assertEquals("'John' 'Doe'", evaluateSingleFieldValue("replaceRegex(/name, '\"', '\\'')", record).getValue(),
                "Replacing double-quote to single-quote");

        values.put("name", "'John' 'Doe'");
        assertEquals("\"John\" \"Doe\"", evaluateSingleFieldValue("replaceRegex(/name, \"'\", \"\\\"\")", record).getValue(),
                "Replacing quote to double-quote, the function arguments are wrapped by double-quote");

        values.put("name", "\"John\" \"Doe\"");
        assertEquals("'John' 'Doe'", evaluateSingleFieldValue("replaceRegex(/name, \"\\\"\", \"'\")", record).getValue(),
                "Replacing double-quote to single-quote, the function arguments are wrapped by double-quote");

    }

    @Test
    public void testReplaceRegexEscapedBackSlashes() {

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        final Record record = new MapRecord(schema, values);

        // Back-slash
        // NOTE: At Java code, a single back-slash needs to be escaped with another-back slash, but needn't do so at NiFi UI.
        //       The test record path is equivalent to replaceRegex(/name, '\\', '/')
        values.put("name", "John\\Doe");
        assertEquals("John/Doe", evaluateSingleFieldValue("replaceRegex(/name, '\\\\', '/')", record).getValue(),
                "Replacing a back-slash to forward-slash");

        values.put("name", "John/Doe");
        assertEquals("John\\Doe", evaluateSingleFieldValue("replaceRegex(/name, '/', '\\\\')", record).getValue(),
                "Replacing a forward-slash to back-slash");

    }

    @Test
    public void testReplaceRegexEscapedBrackets() {

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        final Record record = new MapRecord(schema, values);

        // Brackets
        values.put("name", "J[o]hn Do[e]");
        assertEquals("J(o)hn Do(e)", evaluateSingleFieldValue("replaceRegex(replaceRegex(/name, '\\[', '('), '\\]', ')')", record).getValue(),
                "Square brackets can be escaped with back-slash");

        values.put("name", "J(o)hn Do(e)");
        assertEquals("J[o]hn Do[e]", evaluateSingleFieldValue("replaceRegex(replaceRegex(/name, '\\(', '['), '\\)', ']')", record).getValue(),
                "Brackets can be escaped with back-slash");
    }

    @Test
    public void testReplaceNull() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("missing", RecordFieldType.LONG.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals(48, evaluateSingleFieldValue("replaceNull(/missing, /id)", record).getValue());
        assertEquals(14, evaluateSingleFieldValue("replaceNull(/missing, 14)", record).getValue());
        assertEquals(48, evaluateSingleFieldValue("replaceNull(/id, 14)", record).getValue());
    }

    @Test
    public void testConcat() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("fullName", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("firstName", RecordFieldType.LONG.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("lastName", "Doe");
        values.put("firstName", "John");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Doe: 48", evaluateSingleFieldValue("concat(/firstName, ' ', /lastName, ': ', 48)", record).getValue());
    }

    @Test
    public void testMapOf() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("fullName", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("firstName", RecordFieldType.LONG.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("lastName", "Doe");
        values.put("firstName", "John");
        final Record record = new MapRecord(schema, values);
        final FieldValue fv = evaluateSingleFieldValue("mapOf('firstName', /firstName, 'lastName', /lastName)", record);
        assertTrue(fv.getField().getDataType().getFieldType().equals(RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()).getFieldType()));
        assertEquals("MapRecord[{firstName=John, lastName=Doe}]", fv.getValue().toString());

        assertThrows(RecordPathException.class, () -> RecordPath.compile("mapOf('firstName', /firstName, 'lastName')").evaluate(record));
    }


    @Test
    public void testCoalesce() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", "1234");
        values.put("name", null);
        Record record = new MapRecord(schema, values);

        final RecordPath recordPath = RecordPath.compile("coalesce(/id, /name)");

        // Test where the first value is populated
        FieldValue fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertEquals("1234", fieldValue.getValue());
        assertEquals("id", fieldValue.getField().getFieldName());

        // Test different value populated
        values.clear();
        values.put("id", null);
        values.put("name", "John Doe");

        record = new MapRecord(schema, values);
        fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals("name", fieldValue.getField().getFieldName());

        // Test all null
        values.clear();
        values.put("id", null);
        values.put("name", null);

        record = new MapRecord(schema, values);
        assertFalse(recordPath.evaluate(record).getSelectedFields().findFirst().isPresent());

        // Test none is null
        values.clear();
        values.put("id", "1234");
        values.put("name", "John Doe");

        record = new MapRecord(schema, values);
        fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertEquals("1234", fieldValue.getValue());
        assertEquals("id", fieldValue.getField().getFieldName());

        // Test missing field
        values.clear();
        values.put("name", "John Doe");

        record = new MapRecord(schema, values);
        fieldValue = evaluateSingleFieldValue(recordPath, record);
        assertEquals("John Doe", fieldValue.getValue());
        assertEquals("name", fieldValue.getField().getFieldName());
    }

    private Record getCaseTestRecord() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("middleName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("firstName", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("lastName", "Doe");
        values.put("firstName", "John");
        values.put("middleName", "Smith");
        return new MapRecord(schema, values);
    }

    @Test
    public void testToUpperCase() {
        final Record record = getCaseTestRecord();

        assertEquals("JOHN SMITH DOE", evaluateSingleFieldValue("toUpperCase(concat(/firstName, ' ', /middleName, ' ', /lastName))", record).getValue());
        assertEquals("", evaluateSingleFieldValue("toLowerCase(/notDefined)", record).getValue());
    }

    @Test
    public void testToLowerCase() {
        final Record record = getCaseTestRecord();

        assertEquals("john smith doe", evaluateSingleFieldValue("toLowerCase(concat(/firstName, ' ', /middleName, ' ', /lastName))", record).getValue());
        assertEquals("", evaluateSingleFieldValue("toLowerCase(/notDefined)", record).getValue());
    }

    @Test
    public void testTrimString() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("fullName", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("fullName", "   John Smith     ");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Smith", evaluateSingleFieldValue("trim(/fullName)", record).getValue());
        assertEquals("", evaluateSingleFieldValue("trim(/missing)", record).getValue());
    }

    @Test
    public void testTrimArray() {
        final List<RecordField> fields = new ArrayList<>();
        final DataType dataType = new ArrayDataType(RecordFieldType.STRING.getDataType());
        fields.add(new RecordField("names", dataType));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("names", new String[]{"   John Smith     ", "   Jane Smith     "});
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> results = RecordPath.compile("trim(/names[*])").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals("John Smith", results.get(0).getValue());
        assertEquals("Jane Smith", results.get(1).getValue());
    }

    @Test
    public void testFieldName() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("name", evaluateSingleFieldValue("fieldName(/name)", record).getValue());
        assertEquals("name", evaluateSingleFieldValue("fieldName(/*)", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("//*[startsWith(fieldName(.), 'na')]", record).getValue());
        assertEquals("name", evaluateSingleFieldValue("fieldName(//*[startsWith(fieldName(.), 'na')])", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("//name[not(startsWith(fieldName(.), 'xyz'))]", record).getValue());
        assertEquals(0L, RecordPath.compile("//name[not(startsWith(fieldName(.), 'n'))]").evaluate(record).getSelectedFields().count());
    }

    @Test
    public void testRecursiveWithMap() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("map", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, String> mapValues = new HashMap<>();
        mapValues.put("a", "z");
        mapValues.put("b", "Y");
        mapValues.put("c", "x");

        final Map<String, Object> values = new HashMap<>();
        values.put("map", mapValues);

        final Record record = new MapRecord(schema, values);
        assertEquals("Y", evaluateSingleFieldValue("//*[. = toUpperCase(.)]", record).getValue());
    }

    @Test
    public void testRecursiveWithChoiceThatIncludesRecord() {
        final RecordSchema personSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("name", RecordFieldType.STRING.getDataType()),
            new RecordField("age", RecordFieldType.INT.getDataType())
        ));

        final DataType personDataType = RecordFieldType.RECORD.getRecordDataType(personSchema);
        final DataType stringDataType = RecordFieldType.STRING.getDataType();

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("person", RecordFieldType.CHOICE.getChoiceDataType(stringDataType, personDataType)));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> personValueMap = new HashMap<>();
        personValueMap.put("name", "John Doe");
        personValueMap.put("age", 30);
        final Record personRecord = new MapRecord(personSchema, personValueMap);

        final Map<String, Object> values = new HashMap<>();
        values.put("person", personRecord);

        final Record record = new MapRecord(schema, values);
        final List<Object> expectedValues = List.of(personRecord, "John Doe", 30);
        assertEquals(expectedValues, RecordPath.compile("//*").evaluate(record).getSelectedFields().map(FieldValue::getValue).toList());
    }


    @Test
    public void testToDateFromString() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("date", "2017-10-20T11:00:00Z");
        final Record record = new MapRecord(schema, values);

        final Object evaluated = evaluateSingleFieldValue("toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")", record).getValue();
        assertInstanceOf(java.util.Date.class, evaluated);

        final Object evaluatedTimeZone = evaluateSingleFieldValue("toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", \"GMT+8:00\")", record).getValue();
        assertInstanceOf(java.util.Date.class, evaluatedTimeZone);
    }

    @Test
    public void testToDateFromLong() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("date", RecordFieldType.LONG.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final long dateValue = 0L;

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("date", dateValue);
        final Record record = new MapRecord(schema, values);

        // since the field is a long it shouldn't do the conversion and should return the value unchanged
        assertTrue(evaluateSingleFieldValue("toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")", record).getValue() instanceof Long);
        assertTrue(evaluateSingleFieldValue("toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", \"GMT+8:00\")", record).getValue() instanceof Long);
    }

    @Test
    public void testToDateFromNonDateString() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.DATE.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        // since the field is a string it shouldn't do the conversion and should return the value unchanged
        final FieldValue fieldValue = evaluateSingleFieldValue("toDate(/name, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")", record);
        assertEquals("John Doe", fieldValue.getValue());
        final FieldValue fieldValue2 = evaluateSingleFieldValue("toDate(/name, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", \"GMT+8:00\")", record);
        assertEquals("John Doe", fieldValue2.getValue());
    }

    @Test
    public void testFormatDateFromString() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);

        final String localDateFormatted = "2017-10-20";
        final String localDateTimeFormatted = String.format("%sT12:45:30", localDateFormatted);
        final LocalDateTime localDateTime = LocalDateTime.parse(localDateTimeFormatted);

        values.put("date", localDateTimeFormatted);
        final Record record = new MapRecord(schema, values);

        final FieldValue fieldValue = evaluateSingleFieldValue("format( toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\"), 'yyyy-MM-dd' )", record);
        assertEquals(localDateFormatted, fieldValue.getValue());
        final FieldValue fieldValue2 = evaluateSingleFieldValue(String.format("format( toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\"), 'yyyy-MM-dd' , '%s')", TEST_TIMEZONE_OFFSET), record);
        assertEquals(localDateFormatted, fieldValue2.getValue());

        final FieldValue fieldValue3 = evaluateSingleFieldValue(String.format("format( toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\"), \"yyyy-MM-dd'T'HH:mm:ss\", '%s')", TEST_TIMEZONE_OFFSET), record);

        final ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneOffset.systemDefault());
        final ZonedDateTime adjustedZoneDateTime = zonedDateTime.withZoneSameInstant(ZoneOffset.ofHours(TEST_OFFSET_HOURS));
        final LocalDateTime adjustedLocalDateTime = adjustedZoneDateTime.toLocalDateTime();
        final String adjustedDateTime = adjustedLocalDateTime.toString();
        assertEquals(adjustedDateTime, fieldValue3.getValue());

        final FieldValue fieldValueUnchanged = evaluateSingleFieldValue("format( toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\"), 'INVALID' )", record);
        assertInstanceOf(java.util.Date.class, fieldValueUnchanged.getValue());
        final FieldValue fieldValueUnchanged2 = evaluateSingleFieldValue("format( toDate(/date, \"yyyy-MM-dd'T'HH:mm:ss\"), 'INVALID' , 'INVALID')", record);
        assertInstanceOf(java.util.Date.class, fieldValueUnchanged2.getValue());
    }

    @Test
    public void testFormatDateFromLong() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("date", RecordFieldType.LONG.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String localDate = "2017-10-20";
        final String instantFormatted = String.format("%sT12:30:45Z", localDate);
        final long epochMillis = Instant.parse(instantFormatted).toEpochMilli();

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("date", epochMillis);
        final Record record = new MapRecord(schema, values);

        assertEquals(localDate, evaluateSingleFieldValue("format(/date, 'yyyy-MM-dd' )", record).getValue());
        assertEquals(instantFormatted, evaluateSingleFieldValue("format(/date, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", 'GMT')", record).getValue());

        final FieldValue fieldValueUnchanged = evaluateSingleFieldValue("format(/date, 'INVALID' )", record);
        assertEquals(epochMillis, fieldValueUnchanged.getValue());
        final FieldValue fieldValueUnchanged2 = evaluateSingleFieldValue("format(/date, 'INVALID', 'INVALID' )", record);
        assertEquals(epochMillis, fieldValueUnchanged2.getValue());
    }

    @Test
    public void testFormatDateFromDate() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("date", RecordFieldType.DATE.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final String localDate = "2017-10-20";
        final String instantFormatted = String.format("%sT12:30:45Z", localDate);
        final Instant instant = Instant.parse(instantFormatted);
        final Date dateValue = new Date(instant.toEpochMilli());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("date", dateValue);
        final Record record = new MapRecord(schema, values);

        assertEquals(localDate, evaluateSingleFieldValue("format(/date, 'yyyy-MM-dd')", record).getValue());
        assertEquals(instantFormatted, evaluateSingleFieldValue("format(/date, \"yyyy-MM-dd'T'HH:mm:ss'Z'\", 'GMT')", record).getValue());

        final FieldValue fieldValueUnchanged = evaluateSingleFieldValue("format(/date, 'INVALID')", record);
        assertEquals(dateValue, fieldValueUnchanged.getValue());
        final FieldValue fieldValueUnchanged2 = evaluateSingleFieldValue("format(/date, 'INVALID', 'INVALID' )", record);
        assertEquals(dateValue, fieldValueUnchanged2.getValue());
    }

    @Test
    public void testFormatDateWhenNotDate() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        final Record record = new MapRecord(schema, values);

        assertEquals("John Doe", evaluateSingleFieldValue("format(/name, 'yyyy-MM')", record).getValue());
        assertEquals("John Doe", evaluateSingleFieldValue("format(/name, 'yyyy-MM', 'GMT+8:00')", record).getValue());
    }

    @Test
    public void testToString() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("bytes", RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()))));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("bytes", "Hello World!".getBytes(StandardCharsets.UTF_16));
        final Record record = new MapRecord(schema, values);

        assertEquals("Hello World!", evaluateSingleFieldValue("toString(/bytes, \"UTF-16\")", record).getValue());
    }

    @Test
    public void testToStringBadCharset() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("bytes", RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()))));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("bytes", "Hello World!".getBytes(StandardCharsets.UTF_16));
        final Record record = new MapRecord(schema, values);

        assertThrows(IllegalCharsetNameException.class, () ->
                evaluateSingleFieldValue("toString(/bytes, \"NOT A REAL CHARSET\")", record).getValue());
    }

    @Test
    public void testToBytes() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("s", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("s", "Hello World!");
        final Record record = new MapRecord(schema, values);

        assertArrayEquals("Hello World!".getBytes(StandardCharsets.UTF_16LE),
                (byte[]) evaluateSingleFieldValue("toBytes(/s, \"UTF-16LE\")", record).getValue());
    }

    @Test
    public void testToBytesBadCharset() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("s", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("s", "Hello World!");
        final Record record = new MapRecord(schema, values);

        assertThrows(IllegalCharsetNameException.class, () -> evaluateSingleFieldValue("toBytes(/s, \"NOT A REAL CHARSET\")", record).getValue());
    }

    @Test
    public void testBase64Encode() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("firstName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("b", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<Object> expectedValues = Arrays.asList(
                Base64.getEncoder().encodeToString("John".getBytes(StandardCharsets.UTF_8)),
                Base64.getEncoder().encodeToString("Doe".getBytes(StandardCharsets.UTF_8)),
                Base64.getEncoder().encode("xyz".getBytes(StandardCharsets.UTF_8))
        );
        final Map<String, Object> values = new HashMap<>();
        values.put("firstName", "John");
        values.put("lastName", "Doe");
        values.put("b", "xyz".getBytes(StandardCharsets.UTF_8));
        final Record record = new MapRecord(schema, values);

        assertEquals(Base64.getEncoder().encodeToString("John".getBytes(StandardCharsets.UTF_8)),
                evaluateSingleFieldValue("base64Encode(/firstName)", record).getValue());
        assertEquals(Base64.getEncoder().encodeToString("Doe".getBytes(StandardCharsets.UTF_8)),
                evaluateSingleFieldValue("base64Encode(/lastName)", record).getValue());
        assertArrayEquals(Base64.getEncoder().encode("xyz".getBytes(StandardCharsets.UTF_8)),
                (byte[]) evaluateSingleFieldValue("base64Encode(/b)", record).getValue());
        List<Object> actualValues = RecordPath.compile("base64Encode(/*)").evaluate(record).getSelectedFields().map(FieldValue::getValue).collect(Collectors.toList());
        IntStream.range(0, 3).forEach(i -> {
            Object expectedObject = expectedValues.get(i);
            Object actualObject = actualValues.get(i);
            if (actualObject instanceof String) {
                assertEquals(expectedObject, actualObject);
            } else if (actualObject instanceof byte[]) {
                assertArrayEquals((byte[]) expectedObject, (byte[]) actualObject);
            }
        });
    }

    @Test
    public void testBase64Decode() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("firstName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("lastName", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("b", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType())));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<Object> expectedValues = Arrays.asList("John", "Doe", "xyz".getBytes(StandardCharsets.UTF_8));
        final Map<String, Object> values = new HashMap<>();
        values.put("firstName", Base64.getEncoder().encodeToString("John".getBytes(StandardCharsets.UTF_8)));
        values.put("lastName", Base64.getEncoder().encodeToString("Doe".getBytes(StandardCharsets.UTF_8)));
        values.put("b", Base64.getEncoder().encode("xyz".getBytes(StandardCharsets.UTF_8)));
        final Record record = new MapRecord(schema, values);

        assertEquals("John", evaluateSingleFieldValue("base64Decode(/firstName)", record).getValue());
        assertEquals("Doe", evaluateSingleFieldValue("base64Decode(/lastName)", record).getValue());
        assertArrayEquals("xyz".getBytes(StandardCharsets.UTF_8), (byte[]) evaluateSingleFieldValue("base64Decode(/b)", record).getValue());
        List<Object> actualValues = RecordPath.compile("base64Decode(/*)").evaluate(record).getSelectedFields().map(FieldValue::getValue).collect(Collectors.toList());
        IntStream.range(0, 3).forEach(i -> {
            Object expectedObject = expectedValues.get(i);
            Object actualObject = actualValues.get(i);
            if (actualObject instanceof String) {
                assertEquals(expectedObject, actualObject);
            } else if (actualObject instanceof byte[]) {
                assertArrayEquals((byte[]) expectedObject, (byte[]) actualObject);
            }
        });
    }

    @Test
    public void testEscapeJson() {
        final RecordSchema address = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("address_1", RecordFieldType.STRING.getDataType())
        ));

        final RecordSchema person = new SimpleRecordSchema(Arrays.asList(
                new RecordField("firstName", RecordFieldType.STRING.getDataType()),
                new RecordField("age", RecordFieldType.INT.getDataType()),
                new RecordField("nicknames", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())),
                new RecordField("addresses", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(address)))
        ));

        final RecordSchema schema = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("person", RecordFieldType.RECORD.getRecordDataType(person))
        ));

        final Map<String, Object> values = new HashMap<String, Object>(){{
            put("person", new MapRecord(person, new HashMap<String, Object>(){{
                put("firstName", "John");
                put("age", 30);
                put("nicknames", new String[] {"J", "Johnny"});
                put("addresses", new MapRecord[]{
                        new MapRecord(address, Collections.singletonMap("address_1", "123 Somewhere Street")),
                        new MapRecord(address, Collections.singletonMap("address_1", "456 Anywhere Road"))
                });
            }}));
        }};

        final Record record = new MapRecord(schema, values);

        assertEquals("\"John\"", RecordPath.compile("escapeJson(/person/firstName)").evaluate(record).getSelectedFields().findFirst().orElseThrow(IllegalStateException::new).getValue());
        assertEquals("30", RecordPath.compile("escapeJson(/person/age)").evaluate(record).getSelectedFields().findFirst().orElseThrow(IllegalStateException::new).getValue());
        assertEquals(
                "{\"firstName\":\"John\",\"age\":30,\"nicknames\":[\"J\",\"Johnny\"],\"addresses\":[{\"address_1\":\"123 Somewhere Street\"},{\"address_1\":\"456 Anywhere Road\"}]}",
                RecordPath.compile("escapeJson(/person)").evaluate(record).getSelectedFields().findFirst().orElseThrow(IllegalStateException::new).getValue()
        );
    }

    @Test
    public void testUnescapeJson() {
        final RecordSchema address = new SimpleRecordSchema(Collections.singletonList(
                new RecordField("address_1", RecordFieldType.STRING.getDataType())
        ));

        final RecordSchema person = new SimpleRecordSchema(Arrays.asList(
                new RecordField("firstName", RecordFieldType.STRING.getDataType()),
                new RecordField("age", RecordFieldType.INT.getDataType()),
                new RecordField("nicknames", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())),
                new RecordField("addresses", RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(address)),
                        RecordFieldType.RECORD.getRecordDataType(address)
                ))
        ));

        final RecordSchema schema = new SimpleRecordSchema(Arrays.asList(
                new RecordField("person", RecordFieldType.RECORD.getRecordDataType(person)),
                new RecordField("json_str", RecordFieldType.STRING.getDataType())
        ));

        // test CHOICE resulting in nested ARRAY of Records
        final Record mapAddressesArray = new MapRecord(schema,
                Collections.singletonMap(
                        "json_str",
                        "{\"firstName\":\"John\",\"age\":30,\"nicknames\":[\"J\",\"Johnny\"],\"addresses\":[{\"address_1\":\"123 Somewhere Street\"},{\"address_1\":\"456 Anywhere Road\"}]}")
        );
        assertEquals(
                new HashMap<String, Object>(){{
                    put("firstName", "John");
                    put("age", 30);
                    put("nicknames", Arrays.asList("J", "Johnny"));
                    put("addresses", Arrays.asList(
                            Collections.singletonMap("address_1", "123 Somewhere Street"),
                            Collections.singletonMap("address_1", "456 Anywhere Road")
                    ));
                }},
                RecordPath.compile("unescapeJson(/json_str)").evaluate(mapAddressesArray).getSelectedFields().findFirst().orElseThrow(IllegalStateException::new).getValue()
        );

        // test CHOICE resulting in nested single RECORD
        final Record mapAddressesSingle = new MapRecord(schema,
                Collections.singletonMap(
                        "json_str",
                        "{\"firstName\":\"John\",\"age\":30,\"nicknames\":[\"J\",\"Johnny\"],\"addresses\":{\"address_1\":\"123 Somewhere Street\"}}")
        );
        assertEquals(
                new HashMap<String, Object>(){{
                    put("firstName", "John");
                    put("age", 30);
                    put("nicknames", Arrays.asList("J", "Johnny"));
                    put("addresses", Collections.singletonMap("address_1", "123 Somewhere Street"));
                }},
                RecordPath.compile("unescapeJson(/json_str, 'false')").evaluate(mapAddressesSingle).getSelectedFields().findFirst().orElseThrow(IllegalStateException::new).getValue()
        );

        // test single Record converted from Map Object
        final Record recordFromMap = new MapRecord(schema,
                Collections.singletonMap(
                        "json_str",
                        "{\"firstName\":\"John\",\"age\":30}")
        );
        assertEquals(
                DataTypeUtils.toRecord(new HashMap<String, Object>(){{
                    put("firstName", "John");
                    put("age", 30);
                }}, "json_str"),
                RecordPath.compile("unescapeJson(/json_str, 'true')").evaluate(recordFromMap).getSelectedFields().findFirst().orElseThrow(IllegalStateException::new).getValue()
        );

        // test collection of Record converted from Map collection
        final Record recordCollectionFromMaps = new MapRecord(schema,
                Collections.singletonMap(
                        "json_str",
                        "[{\"address_1\":\"123 Somewhere Street\"},{\"address_1\":\"456 Anywhere Road\"}]")
        );
        assertEquals(
                Arrays.asList(
                        DataTypeUtils.toRecord(Collections.singletonMap("address_1", "123 Somewhere Street"), "json_str"),
                        DataTypeUtils.toRecord(Collections.singletonMap("address_1", "456 Anywhere Road"), "json_str")
                ),
                RecordPath.compile("unescapeJson(/json_str, 'true')").evaluate(recordCollectionFromMaps).getSelectedFields().findFirst().orElseThrow(IllegalStateException::new).getValue()
        );

        // test simple String field
        final Record recordJustName = new MapRecord(schema, Collections.singletonMap("json_str", "{\"firstName\":\"John\"}"));
        assertEquals(
                new HashMap<String, Object>(){{put("firstName", "John");}},
                RecordPath.compile("unescapeJson(/json_str)").evaluate(recordJustName).getSelectedFields().findFirst().orElseThrow(IllegalStateException::new).getValue()
        );

        // test simple String
        final Record recordJustString = new MapRecord(schema, Collections.singletonMap("json_str", "\"John\""));
        assertEquals("John", RecordPath.compile("unescapeJson(/json_str)").evaluate(recordJustString).getSelectedFields().findFirst().orElseThrow(IllegalStateException::new).getValue());

        // test simple Int
        final Record recordJustInt = new MapRecord(schema, Collections.singletonMap("json_str", "30"));
        assertEquals(30, RecordPath.compile("unescapeJson(/json_str)").evaluate(recordJustInt).getSelectedFields().findFirst().orElseThrow(IllegalStateException::new).getValue());

        // test invalid JSON
        final Record recordInvalidJson = new MapRecord(schema, Collections.singletonMap("json_str", "{\"invalid\": \"json"));

        RecordPathException rpe = assertThrows(RecordPathException.class,
                () -> RecordPath.compile("unescapeJson(/json_str)")
                        .evaluate(recordInvalidJson).getSelectedFields()
                        .findFirst().orElseThrow(IllegalStateException::new).getValue());
        assertEquals("Unable to deserialise JSON String into Record Path value", rpe.getMessage());

        // test not String
        final Record recordNotString = new MapRecord(schema, Collections.singletonMap("person", new MapRecord(person, Collections.singletonMap("age", 30))));
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> RecordPath.compile("unescapeJson(/person/age)")
                        .evaluate(recordNotString).getSelectedFields()
                        .findFirst().orElseThrow(IllegalStateException::new).getValue());
        assertEquals("Argument supplied to unescapeJson must be a String", iae.getMessage());
    }

    @Test
    public void testHash() {
        final Record record = getCaseTestRecord();
        assertEquals("61409aa1fd47d4a5332de23cbf59a36f", evaluateSingleFieldValue("hash(/firstName, 'MD5')", record).getValue());
        assertEquals("5753a498f025464d72e088a9d5d6e872592d5f91", evaluateSingleFieldValue("hash(/firstName, 'SHA-1')", record).getValue());
    }

    @Test
    public void testHashFailure() {
        final Record record = getCaseTestRecord();
        assertThrows(RecordPathException.class, () -> evaluateSingleFieldValue("hash(/firstName, 'NOT_A_ALGO')", record).getValue());
    }

    @Test
    public void testPadLeft() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("someString", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("emptyString", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("nullString", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("someString", "MyString");
        values.put("emptyString", "");
        values.put("nullString", null);
        final Record record = new MapRecord(schema, values);

        assertEquals("##MyString", evaluateSingleFieldValue("padLeft(/someString, 10, '#')", record).getValue());
        assertEquals("__MyString", evaluateSingleFieldValue("padLeft(/someString, 10)", record).getValue());
        assertEquals("MyString", evaluateSingleFieldValue("padLeft(/someString, 3)", record).getValue());
        assertEquals("MyString", evaluateSingleFieldValue("padLeft(/someString, -10)", record).getValue());
        assertEquals("@@@@@@@@@@", evaluateSingleFieldValue("padLeft(/emptyString, 10, '@')", record).getValue());
        assertNull(evaluateSingleFieldValue("padLeft(/nullString, 10, '@')", record).getValue());
        assertEquals("xyMyString", evaluateSingleFieldValue("padLeft(/someString, 10, \"xy\")", record).getValue());
        assertEquals("aVMyString", evaluateSingleFieldValue("padLeft(/someString, 10, \"aVeryLongPadding\")", record).getValue());
        assertEquals("fewfewfewfewMyString", evaluateSingleFieldValue("padLeft(/someString, 20, \"few\")", record).getValue());
    }

    @Test
    public void testPadRight() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("someString", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("emptyString", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("nullString", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("someString", "MyString");
        values.put("emptyString", "");
        values.put("nullString", null);
        final Record record = new MapRecord(schema, values);

        assertEquals("MyString##", evaluateSingleFieldValue("padRight(/someString, 10, \"#\")", record).getValue());
        assertEquals("MyString__", evaluateSingleFieldValue("padRight(/someString, 10)", record).getValue());
        assertEquals("MyString", evaluateSingleFieldValue("padRight(/someString, 3)", record).getValue());
        assertEquals("MyString", evaluateSingleFieldValue("padRight(/someString, -10)", record).getValue());
        assertEquals("@@@@@@@@@@", evaluateSingleFieldValue("padRight(/emptyString, 10, '@')", record).getValue());
        assertNull(evaluateSingleFieldValue("padRight(/nullString, 10, '@')", record).getValue());
        assertEquals("MyStringxy", evaluateSingleFieldValue("padRight(/someString, 10, \"xy\")", record).getValue());
        assertEquals("MyStringaV", evaluateSingleFieldValue("padRight(/someString, 10, \"aVeryLongPadding\")", record).getValue());
        assertEquals("MyStringfewfewfewfew", evaluateSingleFieldValue("padRight(/someString, 20, \"few\")", record).getValue());
    }

    @Test
    public void testUuidV5() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("input", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("namespace", RecordFieldType.STRING.getDataType(), true));
        final RecordSchema schema = new SimpleRecordSchema(fields);
        final UUID namespace = UUID.fromString("67eb2232-f06e-406a-b934-e17f5fa31ae4");
        final String input = "testing NiFi functionality";
        final Map<String, Object> values = new HashMap<>();
        values.put("input", input);
        values.put("namespace", namespace.toString());
        final Record record = new MapRecord(schema, values);

        /*
         * Test with a namespace
         */

        RecordPath path = RecordPath.compile("uuid5(/input, /namespace)");
        RecordPathResult result = path.evaluate(record);

        Optional<FieldValue> fieldValueOpt = result.getSelectedFields().findFirst();
        assertTrue(fieldValueOpt.isPresent());

        String value = fieldValueOpt.get().getValue().toString();
        assertEquals(Uuid5Util.fromString(input, namespace.toString()), value);

        /*
         * Test with no namespace
         */
        final Map<String, Object> values2 = new HashMap<>();
        values2.put("input", input);
        final Record record2 = new MapRecord(schema, values2);

        path = RecordPath.compile("uuid5(/input)");
        result = path.evaluate(record2);
        fieldValueOpt = result.getSelectedFields().findFirst();
        assertTrue(fieldValueOpt.isPresent());

        value = fieldValueOpt.get().getValue().toString();
        assertEquals(Uuid5Util.fromString(input, null), value);
    }

    @Test
    public void testPredicateAsPath() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", null);
        final Record record = new MapRecord(schema, values);

        assertEquals(Boolean.TRUE, evaluateSingleFieldValue("isEmpty( /name )", record).getValue());
        assertEquals(Boolean.FALSE, evaluateSingleFieldValue("isEmpty( /id )", record).getValue());

        assertEquals(Boolean.TRUE, evaluateSingleFieldValue("/id = 48", record).getValue());
        assertEquals(Boolean.FALSE, evaluateSingleFieldValue("/id > 48", record).getValue());

        assertEquals(Boolean.FALSE, evaluateSingleFieldValue("not(/id = 48)", record).getValue());
    }

    @Test
    public void testCountArrayElements() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("numbers", new Object[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("count(/numbers[*])").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());
        assertEquals(10L, fieldValues.get(0).getValue());
    }

    @Test
    public void testCountComparison() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("numbers", new Object[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("count(/numbers[*]) > 9").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());
        assertEquals(true, fieldValues.get(0).getValue());
    }

    @Test
    public void testCountAsFilter() {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("numbers", new Object[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        final Record record = new MapRecord(schema, values);

        final List<FieldValue> fieldValues = RecordPath.compile("/id[count(/numbers[*]) > 2]").evaluate(record).getSelectedFields().collect(Collectors.toList());
        assertEquals(1, fieldValues.size());
        assertEquals(48, fieldValues.get(0).getValue());
    }

    @Test
    public void testRecordRootReferenceInFunction() {
        final Record record = createSimpleRecord();

        final FieldValue singleArgumentFieldValue = evaluateSingleFieldValue("escapeJson(/)", record);
        assertEquals("{\"id\":48,\"name\":\"John Doe\",\"missing\":null}", singleArgumentFieldValue.getValue());
        final FieldValue multipleArgumentsFieldValue = evaluateSingleFieldValue("mapOf(\"copy\",/)", record);
        assertInstanceOf(MapRecord.class, multipleArgumentsFieldValue.getValue());
        assertEquals(record.toString(), ((MapRecord) multipleArgumentsFieldValue.getValue()).getValue("copy"));
    }

    private List<RecordField> getDefaultFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("attributes", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField("mainAccount", RecordFieldType.RECORD.getRecordDataType(getAccountSchema())));
        fields.add(new RecordField("numbers", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));

        final DataType accountDataType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountDataType);
        final RecordField accountsField = new RecordField("accounts", accountsType);
        fields.add(accountsField);
        return fields;
    }

    private RecordSchema getAccountSchema() {
        final List<RecordField> accountFields = new ArrayList<>();
        accountFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        accountFields.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));

        return new SimpleRecordSchema(accountFields);
    }

    private Record createSimpleRecord() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("missing", RecordFieldType.STRING.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values = new HashMap<>();
        values.put("id", 48);
        values.put("name", "John Doe");
        return new MapRecord(schema, values);
    }

    private static FieldValue evaluateSingleFieldValue(RecordPath recordPath, Record record) {
        return recordPath.evaluate(record).getSelectedFields().findFirst().get();
    }

    private static FieldValue evaluateSingleFieldValue(String path, Record record) {
        return evaluateSingleFieldValue(RecordPath.compile(path), record);
    }
}
