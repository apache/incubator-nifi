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
package org.apache.nifi.parquet;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;
import static org.apache.nifi.parquet.utils.ParquetUtils.AVRO_ADD_LIST_ELEMENT_RECORDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parquet.utils.ParquetAttribute;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

@DisabledOnOs({ OS.WINDOWS })
public class TestParquetReader {

    private static final String PARQUET_PATH = "src/test/resources/TestParquetReader.parquet";

    private ParquetReader parquetReaderFactory;
    private ComponentLog componentLog;

    @BeforeEach
    public void setup() {
        Map<PropertyDescriptor, String> readerFactoryProperties = new HashMap<>();
        ConfigurationContext readerFactoryConfigContext = new MockConfigurationContext(readerFactoryProperties, null, null);

        parquetReaderFactory = new ParquetReader();
        parquetReaderFactory.abstractStoreConfigContext(readerFactoryConfigContext);

        componentLog = new MockComponentLog("1234", parquetReaderFactory);
    }

    @Test
    public void testReadUsers() throws IOException, MalformedRecordException {
        final int numUsers = 10;
        final File parquetFile = ParquetTestUtils.createUsersParquetFile(numUsers);
        final List<Record> results = getRecords(parquetFile, emptyMap());

        assertEquals(numUsers, results.size());
        IntStream.range(0, numUsers)
                .forEach(i -> assertEquals(ParquetTestUtils.createUser(i), convertRecordToUser(results.get(i))));
    }

    @Test
    public void testReadUsersPartiallyWithOffsetAndLimitedRecordCount() throws IOException, MalformedRecordException {
        final int numUsers = 1000025; // intentionally so large, to test input with many record groups
        final int expectedRecords = 2;
        final File parquetFile = ParquetTestUtils.createUsersParquetFile(numUsers);
        final List<Record> results = getRecords(parquetFile, Map.of(ParquetAttribute.RECORD_OFFSET, "1000020",
                ParquetAttribute.RECORD_COUNT, "2"));

        assertEquals(expectedRecords, results.size());
        IntStream.range(0, expectedRecords)
                .forEach(i -> assertEquals(ParquetTestUtils.createUser(i + 1000020), convertRecordToUser(results.get(i))));
    }

    @Test
    public void testReadUsersPartiallyWithOffsetWithinFileRange() throws IOException, MalformedRecordException {
        final int numUsers = 1000;
        final int expectedRecords = 5;
        final File parquetFile = ParquetTestUtils.createUsersParquetFile(numUsers);
        final List<Record> results = getRecords(
                parquetFile,
                Map.of(ParquetAttribute.RECORD_OFFSET, "321",
                        ParquetAttribute.FILE_RANGE_START_OFFSET, "16543",
                        ParquetAttribute.FILE_RANGE_END_OFFSET, "24784")
        );

        assertEquals(expectedRecords, results.size());
        IntStream.range(0, expectedRecords)
                .forEach(i -> assertEquals(ParquetTestUtils.createUser(i + 984), convertRecordToUser(results.get(i))));
    }

    @Test
    public void testReadUsersPartiallyWithOffsetAndLimitedRecordCountWithinFileRange()
            throws IOException, MalformedRecordException {
        final int numUsers = 1000;
        final int expectedRecords = 2;
        final File parquetFile = ParquetTestUtils.createUsersParquetFile(numUsers);
        final List<Record> results = getRecords(
                parquetFile,
                Map.of(ParquetAttribute.RECORD_OFFSET, "321",
                        ParquetAttribute.RECORD_COUNT, "2",
                        ParquetAttribute.FILE_RANGE_START_OFFSET, "16543",
                        ParquetAttribute.FILE_RANGE_END_OFFSET, "24784")
        );

        assertEquals(expectedRecords, results.size());
        IntStream.range(0, expectedRecords)
                .forEach(i -> assertEquals(ParquetTestUtils.createUser(i + 984), convertRecordToUser(results.get(i))));
    }

    @Test
    public void testReader() throws InitializationException, IOException  {
        final TestRunner runner = TestRunners.newTestRunner(TestParquetProcessor.class);
        final ParquetReader parquetReader = new ParquetReader();

        runner.addControllerService("reader", parquetReader);
        runner.enableControllerService(parquetReader);

        runner.enqueue(Paths.get(PARQUET_PATH));

        runner.setProperty(TestParquetProcessor.READER, "reader");

        runner.run();
        runner.assertAllFlowFilesTransferred(TestParquetProcessor.SUCCESS, 1);
        runner.getFlowFilesForRelationship(TestParquetProcessor.SUCCESS).get(0).assertContentEquals(
                "MapRecord[{name=Bob0, favorite_number=0, favorite_color=blue0}]\n" +
                "MapRecord[{name=Bob1, favorite_number=1, favorite_color=blue1}]\n" +
                "MapRecord[{name=Bob2, favorite_number=2, favorite_color=blue2}]\n" +
                "MapRecord[{name=Bob3, favorite_number=3, favorite_color=blue3}]\n" +
                "MapRecord[{name=Bob4, favorite_number=4, favorite_color=blue4}]\n" +
                "MapRecord[{name=Bob5, favorite_number=5, favorite_color=blue5}]\n" +
                "MapRecord[{name=Bob6, favorite_number=6, favorite_color=blue6}]\n" +
                "MapRecord[{name=Bob7, favorite_number=7, favorite_color=blue7}]\n" +
                "MapRecord[{name=Bob8, favorite_number=8, favorite_color=blue8}]\n" +
                "MapRecord[{name=Bob9, favorite_number=9, favorite_color=blue9}]");
    }

    @Test
    public void testPartialReaderWithOffsetAndLimitedRecordCount() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestParquetProcessor.class);
        final ParquetReader parquetReader = new ParquetReader();

        runner.addControllerService("reader", parquetReader);
        runner.enableControllerService(parquetReader);

        runner.enqueue(Paths.get(PARQUET_PATH), Map.of(ParquetAttribute.RECORD_OFFSET, "6",
                ParquetAttribute.RECORD_COUNT, "2"));

        runner.setProperty(TestParquetProcessor.READER, "reader");

        runner.run();
        runner.assertAllFlowFilesTransferred(TestParquetProcessor.SUCCESS, 1);
        runner.getFlowFilesForRelationship(TestParquetProcessor.SUCCESS).get(0).assertContentEquals(
                "MapRecord[{name=Bob6, favorite_number=6, favorite_color=blue6}]\n" +
                "MapRecord[{name=Bob7, favorite_number=7, favorite_color=blue7}]");
    }

    @Test
    public void testReaderWithAddListElementRecordsEnabled() throws IOException, InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestParquetProcessor.class);
        final ParquetReader parquetReader = new ParquetReader();

        runner.addControllerService("reader", parquetReader);
        runner.setProperty(parquetReader, AVRO_ADD_LIST_ELEMENT_RECORDS, "true");
        runner.enableControllerService(parquetReader);

        runner.enqueue(Paths.get("src/test/resources/TestParquetReaderWithArray.parquet"));

        runner.setProperty(TestParquetProcessor.READER, "reader");

        runner.run();
        runner.assertAllFlowFilesTransferred(TestParquetProcessor.SUCCESS, 1);
        runner.getFlowFilesForRelationship(TestParquetProcessor.SUCCESS).getFirst().assertContentEquals(
                "MapRecord[{name=Bob, favorite_number=1, favorite_colors=[MapRecord[{element=blue}], MapRecord[{element=red}], MapRecord[{element=yellow}]]}]");
    }

    @Test
    public void testReaderWithAddListElementRecordsDisabled() throws IOException, InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestParquetProcessor.class);
        final ParquetReader parquetReader = new ParquetReader();

        runner.addControllerService("reader", parquetReader);
        runner.setProperty(parquetReader, AVRO_ADD_LIST_ELEMENT_RECORDS, "false");
        runner.enableControllerService(parquetReader);

        runner.enqueue(Paths.get("src/test/resources/TestParquetReaderWithArray.parquet"));

        runner.setProperty(TestParquetProcessor.READER, "reader");

        runner.run();
        runner.assertAllFlowFilesTransferred(TestParquetProcessor.SUCCESS, 1);
        runner.getFlowFilesForRelationship(TestParquetProcessor.SUCCESS).getFirst().assertContentEquals(
                "MapRecord[{name=Bob, favorite_number=1, favorite_colors=[blue, red, yellow]}]");
    }

    private List<Record> getRecords(File parquetFile, Map<String, String> variables)
            throws IOException, MalformedRecordException {
        final List<Record> results = new ArrayList<>();
        // read the parquet file into bytes since we can't use a FileInputStream since it doesn't support mark/reset
        final byte[] parquetBytes = IOUtils.toByteArray(parquetFile.toURI());

        // read the users in using the record reader...
        try (final InputStream in = new ByteArrayInputStream(parquetBytes);
                final RecordReader recordReader = parquetReaderFactory.createRecordReader(
                        variables, in, parquetFile.length(), componentLog)) {

            Record record;
            while ((record = recordReader.nextRecord()) != null) {
                results.add(record);
            }
        }
        return results;
    }

    private Map<String, Object> convertRecordToUser(Record record) {
        return record.getRawFieldNames()
                .stream()
                .collect(toMap(
                        fieldName -> fieldName,
                        record::getValue
                ));
    }
}
