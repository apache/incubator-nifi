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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.json.JsonParserFactory;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@DisabledOnOs(value = OS.WINDOWS, disabledReason = "Pretty printing is not portable as these fail on windows")
public class TestForkRecord {

    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();

    private List<RecordField> getDefaultFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
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

    private RecordSchema getAccountWithTransactionSchema() {
        final List<RecordField> accountFields = new ArrayList<>();
        accountFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        accountFields.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));

        final DataType transactionRecordType = RecordFieldType.RECORD.getRecordDataType(getTransactionSchema());
        final DataType transactionsType = RecordFieldType.ARRAY.getArrayDataType(transactionRecordType);
        accountFields.add(new RecordField("transactions", transactionsType));

        return new SimpleRecordSchema(accountFields);
    }

    private RecordSchema getTransactionSchema() {
        final List<RecordField> transactionFields = new ArrayList<>();
        transactionFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        transactionFields.add(new RecordField("amount", RecordFieldType.DOUBLE.getDataType()));
        return new SimpleRecordSchema(transactionFields);
    }

    @Test
    public void testForkExtractSimpleWithoutParentFields() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, getAccountSchema());

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty("my-path", "/accounts");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "2");
        mff.assertContentEquals("header\n42,4750.89\n43,48212.38\n");
    }

    @Test
    public void testForkExtractSimpleWithParentFields() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = getDefaultFields();
        fieldsWrite.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("my-path", "/accounts");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "2");
        mff.assertContentEquals("header\n42,John Doe,123 My Street,My City,MS,11111,USA,4750.89\n43,John Doe,123 My Street,My City,MS,11111,USA,48212.38\n");
    }

    @Test
    public void testForkExtractNotAnArray() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = getDefaultFields();
        fieldsWrite.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("my-path", "/country");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "0");
    }

    @Test
    public void testForkExtractNotAnArrayOfRecords() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.STRING.getDataType();
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = getDefaultFields();
        fieldsWrite.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("my-path", "/accounts");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-array-strings.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "0");
    }

    @Test
    public void testForkExtractComplexWithParentFields() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountWithTransactionSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = getDefaultFields();
        fieldsWrite.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        fieldsWrite.add(new RecordField("amount", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("my-path", "/accounts[*]/transactions");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "4");
        mff.assertContentEquals("header\n5,John Doe,123 My Street,My City,MS,11111,USA,4750.89,150.31\n6,John Doe,123 My Street,My City,MS,11111,USA,4750.89,-15.31\n"
                + "7,John Doe,123 My Street,My City,MS,11111,USA,48212.38,36.78\n8,John Doe,123 My Street,My City,MS,11111,USA,48212.38,-21.34\n");
    }

    @Test
    public void testForkExtractComplexWithoutParentFields() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountWithTransactionSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = new ArrayList<>();
        fieldsWrite.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fieldsWrite.add(new RecordField("amount", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "false");
        runner.setProperty("my-path", "/accounts[*]/transactions");

        runner.enqueue(new File("src/test/resources/TestForkRecord/single-element-nested-nested-array.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "4");
        mff.assertContentEquals("header\n5,150.31\n6,-15.31\n7,36.78\n8,-21.34\n");
    }

    @Test
    public void testForkExtractComplexWithParentFieldsAndNull() throws IOException, MalformedRecordException, InitializationException {
        TestRunner runner = TestRunners.newTestRunner(new ForkRecord());

        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountWithTransactionSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        final List<RecordField> fieldsWrite = getDefaultFields();
        fieldsWrite.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        fieldsWrite.add(new RecordField("amount", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schemaWrite = new SimpleRecordSchema(fieldsWrite);

        final JsonRecordReader readerService = new JsonRecordReader(schema);
        final MockRecordWriter writerService = new CustomRecordWriter("header", false, schemaWrite);

        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(ForkRecord.RECORD_READER, "reader");
        runner.setProperty(ForkRecord.RECORD_WRITER, "writer");
        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");
        runner.setProperty("my-path", "/accounts[*]/transactions");

        runner.enqueue(new File("src/test/resources/TestForkRecord/two-elements-nested-nested-array-null.json").toPath());
        runner.run(1);
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);

        final MockFlowFile mff = runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0);
        mff.assertAttributeEquals("record.count", "4");
        mff.assertContentEquals("header\n5,John Doe,123 My Street,My City,MS,11111,USA,4750.89,150.31\n6,John Doe,123 My Street,My City,MS,11111,USA,4750.89,-15.31\n"
                + "7,John Doe,123 My Street,My City,MS,11111,USA,48212.38,36.78\n8,John Doe,123 My Street,My City,MS,11111,USA,48212.38,-21.34\n");
    }

    @Test
    public void testSplitMode() throws InitializationException, IOException {
        String expectedOutput = null;
        final TestRunner runner = TestRunners.newTestRunner(new ForkRecord());
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("record-reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestForkRecord/schema/schema.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestForkRecord/schema/schema.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);
        runner.setProperty(ForkRecord.RECORD_READER, "record-reader");

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
        runner.setProperty(ForkRecord.RECORD_WRITER, "record-writer");

        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_SPLIT);

        runner.setProperty("my-path", "/address");
        runner.enqueue(Paths.get("src/test/resources/TestForkRecord/input/complex-input-json.json"));
        runner.run();
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);
        expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestForkRecord/output/split-address.json")));
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0).assertContentEquals(expectedOutput);
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0).assertAttributeEquals("record.count", "5");

        runner.clearTransferState();
        runner.setProperty("my-path", "/bankAccounts[*]/last5Transactions");
        runner.enqueue(Paths.get("src/test/resources/TestForkRecord/input/complex-input-json.json"));
        runner.run();
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);
        expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestForkRecord/output/split-transactions.json")));
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0).assertContentEquals(expectedOutput);
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0).assertAttributeEquals("record.count", "6");
    }

    @Test
    public void testExtractMode() throws InitializationException, IOException {
        String expectedOutput = null;
        final TestRunner runner = TestRunners.newTestRunner(new ForkRecord());
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("record-reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestForkRecord/schema/schema.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestForkRecord/schema/extract-schema.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);
        runner.setProperty(ForkRecord.RECORD_READER, "record-reader");

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("record-writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
        runner.setProperty(ForkRecord.RECORD_WRITER, "record-writer");

        runner.setProperty(ForkRecord.MODE, ForkRecord.MODE_EXTRACT);
        runner.setProperty(ForkRecord.INCLUDE_PARENT_FIELDS, "true");

        runner.setProperty("my-path", "/bankAccounts[*]/last5Transactions");
        runner.enqueue(Paths.get("src/test/resources/TestForkRecord/input/complex-input-json.json"));
        runner.run();
        runner.assertTransferCount(ForkRecord.REL_ORIGINAL, 1);
        runner.assertTransferCount(ForkRecord.REL_FORK, 1);
        expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestForkRecord/output/extract-transactions.json")));
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0).assertContentEquals(expectedOutput);
        runner.getFlowFilesForRelationship(ForkRecord.REL_FORK).get(0).assertAttributeEquals("record.count", "6");
    }

    private class JsonRecordReader extends AbstractControllerService implements RecordReaderFactory {

        private static final JsonParserFactory jsonParserFactory = new JsonParserFactory();

        RecordSchema schema;

        public JsonRecordReader(RecordSchema schema) {
            this.schema = schema;
        }

        @Override
        public RecordReader createRecordReader(FlowFile flowFile, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException {
            return new JsonTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat, null, null, null, null, jsonParserFactory);
        }

        @Override
        public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger)
                throws MalformedRecordException, IOException {
            return new JsonTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat, null, null, null, null, jsonParserFactory);
        }
    }

    private class CustomRecordWriter extends MockRecordWriter {

        RecordSchema schema;

        public CustomRecordWriter(final String header, final boolean quoteValues, RecordSchema schema) {
            super(header, quoteValues);
            this.schema = schema;
        }

        @Override
        public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
            return this.schema;
        }

    }

}
