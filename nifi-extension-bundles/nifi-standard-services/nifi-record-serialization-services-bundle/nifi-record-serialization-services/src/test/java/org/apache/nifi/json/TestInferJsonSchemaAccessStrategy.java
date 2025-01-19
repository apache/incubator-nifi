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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.inference.InferSchemaAccessStrategy;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestInferJsonSchemaAccessStrategy {

    private static final Logger logger = LoggerFactory.getLogger(TestInferJsonSchemaAccessStrategy.class);
    private final SchemaInferenceEngine<JsonNode> timestampInference = new JsonSchemaInference(new TimeValueInference("yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    private final SchemaInferenceEngine<JsonNode> noTimestampInference = new JsonSchemaInference(new TimeValueInference("yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));

    @Test
    @Disabled("Intended only for manual testing to determine performance before/after modifications")
    void testPerformanceOfSchemaInferenceWithTimestamp() throws IOException {
        final File file = new File("src/test/resources/json/prov-events.json");
        final byte[] data = Files.readAllBytes(file.toPath());

        final byte[] manyCopies = new byte[data.length * 20];
        for (int i = 0; i < 20; i++) {
            System.arraycopy(data, 0, manyCopies, data.length * i, data.length);
        }

        final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                (var, content) -> new JsonRecordSource(content), timestampInference, Mockito.mock(ComponentLog.class)
        );

        for (int j = 0; j < 10; j++) {
            final long start = System.nanoTime();

            for (int i = 0; i < 10_000; i++) {
                try (final InputStream in = new ByteArrayInputStream(manyCopies)) {
                    accessStrategy.getSchema(null, in, null);
                }
            }

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            logger.info("{}", millis);
        }
    }

    @Test
    @Disabled("Intended only for manual testing to determine performance before/after modifications")
    void testPerformanceOfSchemaInferenceWithoutTimestamp() throws IOException {
        final File file = new File("src/test/resources/json/prov-events.json");
        final byte[] data = Files.readAllBytes(file.toPath());

        final byte[] manyCopies = new byte[data.length * 20];
        for (int i = 0; i < 20; i++) {
            System.arraycopy(data, 0, manyCopies, data.length * i, data.length);
        }

        for (int j = 0; j < 10; j++) {
            final long start = System.nanoTime();

            for (int i = 0; i < 10_000; i++) {
                try (final InputStream in = new ByteArrayInputStream(manyCopies)) {
                    final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>((var, content) -> new JsonRecordSource(content),
                            noTimestampInference, Mockito.mock(ComponentLog.class));

                    accessStrategy.getSchema(null, in, null);
                }
            }

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            logger.info("{}", millis);
        }
    }

    @Test
    void testInferenceIncludesAllRecords() throws IOException {
        final File file = new File("src/test/resources/json/prov-events.json");
        final RecordSchema schema = inferSchema(file, StartingFieldStrategy.ROOT_NODE, null);

        final RecordField extraField1 = schema.getField("extra field 1").get();
        assertSame(RecordFieldType.STRING, extraField1.getDataType().getFieldType());

        final RecordField extraField2 = schema.getField("extra field 2").get();
        assertSame(RecordFieldType.STRING, extraField2.getDataType().getFieldType());

        final RecordField updatedAttributesField = schema.getField("updatedAttributes").get();
        final DataType updatedAttributesDataType = updatedAttributesField.getDataType();
        assertSame(RecordFieldType.RECORD, updatedAttributesDataType.getFieldType());

        final List<String> expectedAttributeNames = Arrays.asList("path", "filename", "drop reason", "uuid", "reporting.task.type", "s2s.address", "schema.cache.identifier", "reporting.task.uuid",
                "record.count", "s2s.host", "reporting.task.transaction.id", "reporting.task.name", "mime.type");

        final RecordSchema updatedAttributesSchema = ((RecordDataType) updatedAttributesDataType).getChildSchema();
        assertEquals(expectedAttributeNames.size(), updatedAttributesSchema.getFieldCount());

        for (final String attributeName : expectedAttributeNames) {
            assertSame(RecordFieldType.STRING, updatedAttributesSchema.getDataType(attributeName).get().getFieldType());
        }
    }

    @Test
    void testDateAndTimestampsInferred() throws IOException {
        final File file = new File("src/test/resources/json/prov-events.json");
        final RecordSchema schema = inferSchema(file, StartingFieldStrategy.ROOT_NODE, null);

        final RecordField timestampField = schema.getField("timestamp").get();
        assertEquals(RecordFieldType.TIMESTAMP.getDataType("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"), timestampField.getDataType());

        final RecordField dateField = schema.getField("eventDate").get();
        assertEquals(RecordFieldType.DATE.getDataType("yyyy-MM-dd"), dateField.getDataType());

        final RecordField timeField = schema.getField("eventTime").get();
        assertEquals(RecordFieldType.TIME.getDataType("HH:mm:ss"), timeField.getDataType());

        // TIME value and a STRING should be inferred as a STRING field
        final RecordField maybeTimeField = schema.getField("maybeTime").get();
        assertEquals(
                RecordFieldType.CHOICE.getChoiceDataType().getFieldType(),
                maybeTimeField.getDataType().getFieldType());

        // DATE value and a null value should be inferred as a DATE field
        final RecordField maybeDateField = schema.getField("maybeDate").get();
        assertEquals(RecordFieldType.DATE.getDataType("yyyy-MM-dd"), maybeDateField.getDataType());
    }

    /**
     * Test is intended to ensure that all inference rules that are explained in the readers' additionalDetails.md are correct
     */
    @Test
    void testDocsExample() throws IOException {
        final File file = new File("src/test/resources/json/docs-example.json");
        final RecordSchema schema = inferSchema(file, StartingFieldStrategy.ROOT_NODE, null);

        assertSame(RecordFieldType.STRING, schema.getDataType("name").get().getFieldType());
        assertSame(RecordFieldType.CHOICE, schema.getDataType("age").get().getFieldType());

        final DataType valuesDataType = schema.getDataType("values").get();
        assertSame(RecordFieldType.CHOICE, valuesDataType.getFieldType());

        final ChoiceDataType valuesChoiceType = (ChoiceDataType) valuesDataType;
        final List<DataType> possibleTypes = valuesChoiceType.getPossibleSubTypes();
        assertEquals(2, possibleTypes.size());
        assertTrue(possibleTypes.contains(RecordFieldType.STRING.getDataType()));
        assertTrue(possibleTypes.contains(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.STRING.getDataType()
        ))));

        assertSame(RecordFieldType.STRING, schema.getDataType("nullValue").get().getFieldType());
    }

    @ParameterizedTest(name = "{index} {2}")
    @MethodSource("startingFieldNameArgumentProvider")
    void testInferenceStartsFromArray(final String jsonPath, final StartingFieldStrategy strategy, final String startingFieldName, String testName) throws IOException {
        final File file = new File(jsonPath);
        final RecordSchema schema = inferSchema(file, strategy, startingFieldName);

        assertEquals(2, schema.getFieldCount());

        final RecordField field1 = schema.getField("id").get();
        assertSame(RecordFieldType.INT, field1.getDataType().getFieldType());

        final RecordField field2 = schema.getField("balance").get();
        assertSame(RecordFieldType.DOUBLE, field2.getDataType().getFieldType());
    }

    @Test
    void testInferenceStartsFromSimpleFieldAndNoNestedObjectOrArrayFound() throws IOException {
        final File file = new File("src/test/resources/json/single-element-nested-array-middle.json");
        final RecordSchema schema = inferSchema(file, StartingFieldStrategy.NESTED_FIELD, "name");

        assertEquals(0, schema.getFieldCount());
    }

    @Test
    void testInferenceStartFromNonExistentField() throws IOException {
        final File file = new File("src/test/resources/json/single-element-nested-array.json");
        final RecordSchema recordSchema = inferSchema(file, StartingFieldStrategy.NESTED_FIELD, "notfound");
        assertEquals(0, recordSchema.getFieldCount());
    }

    @Test
    void testInferenceStartFromMultipleNestedField() throws IOException {
        final File file = new File("src/test/resources/json/multiple-nested-field.json");
        final RecordSchema schema = inferSchema(file, StartingFieldStrategy.NESTED_FIELD, "accountIds");

        final RecordField field1 = schema.getField("id").get();
        assertSame(RecordFieldType.STRING, field1.getDataType().getFieldType());

        final RecordField field2 = schema.getField("type").get();
        assertSame(RecordFieldType.STRING, field2.getDataType().getFieldType());
    }

    private RecordSchema inferSchema(final File file, final StartingFieldStrategy strategy, final String startingFieldName) throws IOException {
        try (final InputStream in = new FileInputStream(file);
             final InputStream bufferedIn = new BufferedInputStream(in)) {

            final InferSchemaAccessStrategy<?> accessStrategy = new InferSchemaAccessStrategy<>(
                    (var, content) -> new JsonRecordSource(content, strategy, startingFieldName, new JsonParserFactory()),
                    timestampInference, Mockito.mock(ComponentLog.class)
            );

            return accessStrategy.getSchema(null, bufferedIn, null);
        }
    }

    private static Stream<Arguments> startingFieldNameArgumentProvider() {
        final StartingFieldStrategy strategy = StartingFieldStrategy.NESTED_FIELD;
        return Stream.of(
                Arguments.of("src/test/resources/json/single-element-nested-array.json", strategy, "accounts", "testInferenceSkipsToNestedArray"),
                Arguments.of("src/test/resources/json/single-element-nested.json", strategy, "account", "testInferenceSkipsToNestedObject"),
                Arguments.of("src/test/resources/json/single-element-nested-array.json", strategy, "name", "testInferenceSkipsToSimpleFieldFindsNextNestedArray"),
                Arguments.of("src/test/resources/json/single-element-nested.json", strategy, "name", "testInferenceSkipsToSimpleFieldFindsNextNestedObject"),
                Arguments.of("src/test/resources/json/single-element-nested-array-middle.json", strategy, "accounts", "testInferenceSkipsToNestedArrayInMiddle"),
                Arguments.of("src/test/resources/json/nested-array-then-start-object.json", strategy, "accounts", "testInferenceSkipsToNestedThenStartObject")
        );
    }
}
