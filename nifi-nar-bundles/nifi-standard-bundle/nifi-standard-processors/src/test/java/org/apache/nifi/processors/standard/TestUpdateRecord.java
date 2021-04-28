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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUpdateRecord {

    private TestRunner runner;
    private MockRecordParser readerService;
    private MockRecordWriter writerService;

    //Apparently pretty printing is not portable as these tests fail on windows
    @BeforeClass
    public static void setUpSuite() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Before
    public void setup() throws InitializationException {
        readerService = new MockRecordParser();
        writerService = new MockRecordWriter("header", false);

        runner = TestRunners.newTestRunner(UpdateRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(UpdateRecord.RECORD_READER, "reader");
        runner.setProperty(UpdateRecord.RECORD_WRITER, "writer");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
    }


    @Test
    public void testLiteralReplacementValue() {
        runner.setProperty("/name", "Jane Doe");
        runner.enqueue("");

        readerService.addRecord("John Doe", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\nJane Doe,35\n");
    }

    @Test
    public void testLiteralReplacementValueExpressionLanguage() {
        runner.setProperty("/name", "${newName}");
        runner.enqueue("", Collections.singletonMap("newName", "Jane Doe"));

        readerService.addRecord("John Doe", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\nJane Doe,35\n");
    }

    @Test
    public void testRecordPathReplacementValue() {
        runner.setProperty("/name", "/age");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES.getValue());
        runner.enqueue("");

        readerService.addRecord("John Doe", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\n35,35\n");
    }

    @Test
    public void testInvalidRecordPathUsingExpressionLanguage() {
        runner.setProperty("/name", "${recordPath}");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES.getValue());
        runner.enqueue("", Collections.singletonMap("recordPath", "hello"));

        readerService.addRecord("John Doe", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_FAILURE, 1);
    }

    @Test
    public void testLiteralReplacementRowIndexValueExpressionLanguage() throws InitializationException {
        readerService = new MockRecordParser();
        readerService.addSchemaField("id", RecordFieldType.LONG);
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);

        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.LITERAL_VALUES);
        runner.setProperty("/id", "${record.index}");

        runner.enqueue("");

        readerService.addRecord(null, "John Doe", 35);
        readerService.addRecord(null, "Jane Doe", 36);
        readerService.addRecord(null, "John Smith", 37);
        readerService.addRecord(null, "Jane Smith", 38);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\n1,John Doe,35\n2,Jane Doe,36\n3,John Smith,37\n4,Jane Smith,38\n");
    }

    @Test
    public void testReplaceWithMissingRecordPath() throws InitializationException {
        readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("siblings", RecordFieldType.ARRAY);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);

        runner.setProperty("/name", "/siblings[0]/name");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES.getValue());

        runner.enqueue("", Collections.singletonMap("recordPath", "hello"));

        readerService.addRecord("John Doe", null);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        mff.assertContentEquals("header\n,\n");
    }

    @Test
    public void testRelativePath() throws InitializationException {
        readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("nickname", RecordFieldType.STRING);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);

        runner.setProperty("/name", "../nickname");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES.getValue());

        runner.enqueue("", Collections.singletonMap("recordPath", "hello"));

        readerService.addRecord("John Doe", "Johnny");
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        mff.assertContentEquals("header\nJohnny,Johnny\n");
    }

    @Test
    public void testConcatWithArrayInferredSchema() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/addresses.json"));
        runner.setProperty("/addresses[*]/full", "concat(../street, ' ', ../city, ' ', ../state)");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/full-addresses.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testChangingSchema() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/name", "/name/first");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/person-with-firstname.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testUpdateInArray() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-address.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-address.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person-address.json"));
        runner.setProperty("/address[*]/city", "newCity");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.LITERAL_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/person-with-new-city.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testUpdateInNullArray() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-address.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-address.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person-with-null-array.json"));
        runner.setProperty("/address[*]/city", "newCity");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.LITERAL_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/person-with-null-array.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testAddFieldNotInInputRecord() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string-fields.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/firstName", "/name/first");
        runner.setProperty("/lastName", "/name/last");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/person-with-firstname-lastname.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testFieldValuesInEL() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/name/last", "${field.value:toUpper()}");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.LITERAL_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/person-with-capital-lastname.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testSetRootPathAbsoluteWithMultipleValues() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/name-fields-only.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/", "/name/*");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/name-fields-only.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testSetRootPathAbsoluteWithSingleValue() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/name-fields-only.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/", "/name");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/name-fields-only.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }


    @Test
    public void testSetRootPathRelativeWithMultipleValues() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/name-fields-only.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/name/..", "/name/*");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/name-fields-only.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testSetRootPathRelativeWithSingleValue() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/name-fields-only.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/name/..", "/name");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/name-fields-only.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testSetAbsolutePathWithAnotherRecord() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-and-mother.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-and-mother.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/name", "/mother");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/name-and-mother-same.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testUpdateSimpleArray() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/multi-arrays.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/multi-arrays.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.LITERAL_VALUES);
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/numbers[*]", "8");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json")));
        expectedOutput = expectedOutput.replaceFirst("1, null, 4", "8, 8, 8");
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
        runner.removeProperty("/numbers[*]");

        runner.clearTransferState();
        runner.enqueue("{\"numbers\":null}");
        runner.setProperty("/numbers[*]", "8");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        String content = new String(runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).toByteArray());
        assertTrue(content.contains("\"numbers\" : null"));
        runner.removeProperty("/numbers[*]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/numbers[1]", "8");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json")));
        expectedOutput = expectedOutput.replaceFirst("1, null, 4", "1, 8, 4");
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
        runner.removeProperty("/numbers[1]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/numbers[0..1]", "8");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json")));
        expectedOutput = expectedOutput.replaceFirst("1, null, 4", "8, 8, 4");
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
        runner.removeProperty("/numbers[0..1]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/numbers[0,2]", "8");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json")));
        expectedOutput = expectedOutput.replaceFirst("1, null, 4", "8, null, 8");
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
        runner.removeProperty("/numbers[0,2]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/numbers[0,1..2]", "8");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json")));
        expectedOutput = expectedOutput.replaceFirst("1, null, 4", "8, 8, 8");
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
        runner.removeProperty("/numbers[0,1..2]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/numbers[0..-1][. = 4]", "8");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json")));
        expectedOutput = expectedOutput.replaceFirst("1, null, 4", "1, null, 8");
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
        runner.removeProperty("/numbers[0..-1][. = 4]");
    }

    @Test
    public void testUpdateComplexArrays() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/multi-arrays.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/multi-arrays.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/peoples[*]", "/peoples[3]");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        String content = new String(runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).toByteArray());
        int count = StringUtils.countMatches(content, "Mary Doe");
        assertEquals(4, count);
        runner.removeProperty("/peoples[*]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/peoples[1]", "/peoples[3]");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        content = new String(runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).toByteArray());
        count = StringUtils.countMatches(content, "Mary Doe");
        assertEquals(2, count);
        runner.removeProperty("/peoples[1]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/peoples[0..1]", "/peoples[3]");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/updateArrays/multi-arrays-0and1.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
        runner.removeProperty("/peoples[0..1]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/peoples[0,2]", "/peoples[3]");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/updateArrays/multi-arrays-0and2.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
        runner.removeProperty("/peoples[0,2]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/peoples[0,1..2]", "/peoples[3]");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        content = new String(runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).toByteArray());
        count = StringUtils.countMatches(content, "Mary Doe");
        assertEquals(4, count);
        runner.removeProperty("/peoples[0,1..2]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/peoples[0..-1][./name != 'Mary Doe']", "/peoples[3]");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        content = new String(runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).toByteArray());
        count = StringUtils.countMatches(content, "Mary Doe");
        assertEquals(4, count);
        runner.removeProperty("/peoples[0..-1][./name != 'Mary Doe']");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/peoples[0..-1][./name != 'Mary Doe']/addresses[*]", "/peoples[3]/addresses[0]");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        content = new String(runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).toByteArray());
        count = StringUtils.countMatches(content, "1 nifi road");
        assertEquals(13, count);
        runner.removeProperty("/peoples[0..-1][./name != 'Mary Doe']/addresses[*]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/peoples[0..-1][./name != 'Mary Doe']/addresses[0,1..2]", "/peoples[3]/addresses[0]");
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/updateArrays/multi-arrays-streets.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
        runner.removeProperty("/peoples[0..-1][./name != 'Mary Doe']/addresses[0,1..2]");

        runner.clearTransferState();
        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/multi-arrays.json"));
        runner.setProperty("/peoples[0..-1][./name != 'Mary Doe']/addresses[0,1..2]/city", "newCity");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.LITERAL_VALUES);
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        content = new String(runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).toByteArray());
        count = StringUtils.countMatches(content, "newCity");
        assertEquals(9, count);
        runner.removeProperty("/peoples[0..-1][./name != 'Mary Doe']/addresses[0,1..2]/city");
    }

    @Test
    public void testRemoveSimpleFieldThatIsMissingFromOneRecord() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestUpdateRecord/schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestUpdateRecord/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestUpdateRecord/schema/complex-person-no-dateOfBirth.avsc";
        String outPutFlowFile = "src/test/resources/TestUpdateRecord/output/complex-person-no-dateOfBirth.json";
        String fieldToRemove = "/dateOfBirth";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveComplexFieldThatIsMissingFromOneRecord() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestUpdateRecord/schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestUpdateRecord/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestUpdateRecord/schema/complex-person-no-workAddress.avsc";
        String outPutFlowFile = "src/test/resources/TestUpdateRecord/output/complex-person-no-workAddress.json";
        String fieldToRemove = "/workAddress";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFromDeepStructure() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestUpdateRecord/schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestUpdateRecord/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestUpdateRecord/schema/complex-person-no-workAddress-zip.avsc";
        String outPutFlowFile = "src/test/resources/TestUpdateRecord/output/complex-person-no-workAddress-zip.json";
        String fieldToRemove = "/workAddress/zip";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFromDeepStructureWithRelativePath() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestUpdateRecord/schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestUpdateRecord/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestUpdateRecord/schema/complex-person-no-zip.avsc";
        String outPutFlowFile = "src/test/resources/TestUpdateRecord/output/complex-person-no-zip.json";
        String fieldToRemove = "//zip";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFrom3LevelDeepStructure() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestUpdateRecord/schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestUpdateRecord/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestUpdateRecord/schema/complex-person-no-workAddress-building-letter.avsc";
        String outPutFlowFile = "src/test/resources/TestUpdateRecord/output/complex-person-no-workAddress-building-letter.json";
        String fieldToRemove = "/workAddress/building/letter";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveComplexFieldFromDeepStructureWithRelativePath() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestUpdateRecord/schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestUpdateRecord/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestUpdateRecord/schema/complex-person-no-building.avsc";
        String outPutFlowFile = "src/test/resources/TestUpdateRecord/output/complex-person-no-building.json";
        String fieldToRemove = "//building";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveNonExistentField() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestUpdateRecord/schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestUpdateRecord/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestUpdateRecord/schema/complex-person.avsc";
        String outPutFlowFile = "src/test/resources/TestUpdateRecord/output/complex-person.json";
        String fieldToRemove = "/nonExistentField";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveNonExistentFieldFromDeepStructure() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestUpdateRecord/schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestUpdateRecord/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestUpdateRecord/schema/complex-person.avsc";
        String outPutFlowFile = "src/test/resources/TestUpdateRecord/output/complex-person.json";
        String fieldToRemove = "/workAddress/nonExistentField";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveMultipleFields() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestUpdateRecord/schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestUpdateRecord/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestUpdateRecord/schema/complex-person-multiple-fields-removed.avsc";
        String outPutFlowFile = "src/test/resources/TestUpdateRecord/output/complex-person-multiple-fields-removed.json";
        String fieldToRemove1 = "/name";
        String fieldToRemove2 = "/dateOfBirth";
        String fieldToRemove3 = "/workAddress/building";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove1, fieldToRemove2, fieldToRemove3);
    }

    private void executeRemovalTest(String inputSchema, String inputFlowFile, String outPutSchema, String outPutFlowFile, String... fieldsToRemove)
            throws IOException, InitializationException {
        // GIVEN
        executePreparation(inputSchema, inputFlowFile, fieldsToRemove);

        // WHEN
        runner.run();

        // THEN
        assertOutput(outPutSchema, outPutFlowFile);
    }

    private void executePreparation(String inputSchema, String inputFlowFile, String... fieldsToRemove) throws IOException, InitializationException {
        setUpJsonReader(inputSchema);
        setUpJsonWriter();

        Map<String, String> properties = new HashMap<>(1);
        for (String fieldToRemove : fieldsToRemove) {
            properties.put(fieldToRemove, "");
        }
        setUpRunner(inputFlowFile, properties);
    }

    private void assertOutput(String outPutSchema, String outPutFlowFile) throws IOException {
        String expectedOutput = new String(Files.readAllBytes(Paths.get(outPutFlowFile)));
        String expectedSchema = new String(Files.readAllBytes(Paths.get(outPutSchema)));

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        out.assertContentEquals(expectedOutput);
        out.assertAttributeEquals("avro.schema", expectedSchema);
    }

    private void setUpJsonReader(String schemaFilePath) throws IOException, InitializationException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get(schemaFilePath)));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);
    }

    private void setUpJsonWriter() throws InitializationException {
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
    }

    private void setUpRunner(String flowFilePath, Map<String, String> properties) throws IOException {
        runner.enqueue(Paths.get(flowFilePath));
        properties.forEach((propertyName,propertyValue) -> runner.setProperty(propertyName, propertyValue));
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);
    }
}
