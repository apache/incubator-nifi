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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.db.SimpleCommerceDataSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestExecuteSQLRecord {

    private final Logger LOGGER = LoggerFactory.getLogger(TestExecuteSQLRecord.class);

    final static String DB_LOCATION = "target/db";

    final static String QUERY_WITH_EL = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID = ${person.id}";

    final static String QUERY_WITHOUT_EL = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID = 10";

    final static String QUERY_WITHOUT_EL_WITH_PARAMS = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID < ? AND REL.ID < ?";


    @BeforeAll
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @AfterAll
    public static void cleanupClass() {
        System.clearProperty("derby.stream.error.file");
    }

    private TestRunner runner;

    @BeforeEach
    public void setup() throws InitializationException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl("derby");
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(ExecuteSQLRecord.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(AbstractExecuteSQL.DBCP_SERVICE, "dbcp");
    }

    @Test
    public void testIncomingConnectionWithNoFlowFile() throws InitializationException {
        runner.setIncomingConnection(true);
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM persons");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();
        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 0);
    }

    @Test
    public void testIncomingConnectionWithNoFlowFileAndNoQuery() throws InitializationException {
        runner.setIncomingConnection(true);
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();
        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 0);
    }

    @Test
    public void testNoIncomingConnectionAndNoQuery() throws InitializationException {
        runner.setIncomingConnection(false);
        assertThrows(AssertionError.class, () -> {
            runner.run();
        });
    }

    @Test
    public void testNoIncomingConnection() throws ClassNotFoundException, SQLException, InitializationException, IOException {
        runner.setIncomingConnection(false);
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, false, null, true);
        assertEquals(ProvenanceEventType.RECEIVE, runner.getProvenanceEvents().get(0).getEventType());
    }

    @Test
    public void testSelectQueryInFlowFile() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, true, null, false);
        assertEquals(ProvenanceEventType.FORK, runner.getProvenanceEvents().get(0).getEventType());
        assertEquals(ProvenanceEventType.FETCH, runner.getProvenanceEvents().get(1).getEventType());
    }

    @Test
    public void testAutoCommitFalse() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        runner.setProperty(ExecuteSQL.AUTO_COMMIT, "false");
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, true, null, false);
    }

    @Test
    public void testAutoCommitTrue() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        runner.setProperty(ExecuteSQL.AUTO_COMMIT, "true");
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, true, null, false);
    }

    @Test
    public void testWithOutputBatching() throws InitializationException, SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        for (int i = 0; i < 1000; i++) {
            stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (" + i + ", 1, 1)");
        }

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQLRecord.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(ExecuteSQLRecord.OUTPUT_BATCH_SIZE, "5");
        runner.setProperty(ExecuteSQLRecord.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, 200);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).get(0);

        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).get(199);

        lastFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULTSET_INDEX, "0");
    }

    @Test
    public void testWithOutputBatchingAndIncomingFlowFile() throws InitializationException, SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        for (int i = 0; i < 1000; i++) {
            stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (" + i + ", 1, 1)");
        }

        Map<String, String> attrMap = new HashMap<>();
        String testAttrName = "attr1";
        String testAttrValue = "value1";
        attrMap.put(testAttrName, testAttrValue);

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(ExecuteSQLRecord.OUTPUT_BATCH_SIZE, "1");
        MockFlowFile inputFlowFile = runner.enqueue("SELECT * FROM TEST_NULL_INT", attrMap);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, 200);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).get(0);

        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).get(199);

        lastFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULTSET_INDEX, "0");
        lastFlowFile.assertAttributeEquals(testAttrName, testAttrValue);
        lastFlowFile.assertAttributeEquals(AbstractExecuteSQL.INPUT_FLOWFILE_UUID, inputFlowFile.getAttribute(CoreAttributes.UUID.key()));
    }

    @Test
    public void testWithOutputBatchingLastBatchFails() throws InitializationException, SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 varchar(50), constraint my_pk primary key (id))");

        // Insert some valid numeric values (for TO_NUMBER call later)
        for (int i = 0; i < 11; i++) {
            stmt.execute("insert into TEST_NULL_INT (id, val1) VALUES (" + i + ", '" + i + "')");
        }
        // Insert invalid numeric value
        stmt.execute("insert into TEST_NULL_INT (id, val1) VALUES (100, 'abc')");

        Map<String, String> attrMap = new HashMap<>();
        String testAttrName = "attr1";
        String testAttrValue = "value1";
        attrMap.put(testAttrName, testAttrValue);

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.enqueue("SELECT ID, CAST(VAL1 AS INTEGER) AS TN FROM TEST_NULL_INT", attrMap);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_FAILURE, 1);
        runner.assertTransferCount(ExecuteSQLRecord.REL_SUCCESS, 0);
    }

    @Test
    public void testMaxRowsPerFlowFile() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        for (int i = 0; i < 1000; i++) {
            stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (" + i + ", 1, 1)");
        }

        runner.setIncomingConnection(false);
        runner.setProperty(AbstractExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(AbstractExecuteSQL.OUTPUT_BATCH_SIZE, "0");
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 200);
        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 0);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_COUNT.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).get(0);

        firstFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeEquals("record.count", "5");
        firstFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain"); // MockRecordWriter has text/plain MIME type
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).get(199);

        lastFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals("record.count", "5");
        lastFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain"); // MockRecordWriter has text/plain MIME type
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(AbstractExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testInsertStatementCreatesFlowFile() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(false);
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).get(0).assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "0");
    }

    @Test
    public void testWriteLOBsToAvro() throws Exception {
        final DBCPService dbcp = new DBCPServiceSimpleImpl("h2");
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(ExecuteSQLRecord.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(AbstractExecuteSQL.DBCP_SERVICE, "dbcp");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, image blob(1K), words clob(1K), "
                + "natwords nclob(1K), constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_NULL_INT (id, val1, val2, image, words, natwords) VALUES (0, NULL, 1, CAST (X'DEADBEEF' AS BLOB), "
                + "CAST ('Hello World' AS CLOB), CAST ('I am an NCLOB' AS NCLOB))");

        runner.setIncomingConnection(false);
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        AvroRecordSetWriter recordWriter = new AvroRecordSetWriter();
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(recordWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(AbstractExecuteSQL.RESULT_ROW_COUNT, "1");

        ByteArrayInputStream bais = new ByteArrayInputStream(flowFile.toByteArray());
        try (DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(bais, new GenericDatumReader<>())) {
            final Schema avroSchema = dataFileStream.getSchema();
            GenericData.setStringType(avroSchema, GenericData.StringType.String);
            final GenericRecord avroRecord = dataFileStream.next();

            Object imageObj = avroRecord.get("IMAGE");
            assertNotNull(imageObj);
            assertInstanceOf(ByteBuffer.class, imageObj);
            assertArrayEquals(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}, ((ByteBuffer) imageObj).array());

            Object wordsObj = avroRecord.get("WORDS");
            assertNotNull(wordsObj);
            assertInstanceOf(Utf8.class, wordsObj);
            assertEquals("Hello World", wordsObj.toString());

            Object natwordsObj = avroRecord.get("NATWORDS");
            assertNotNull(natwordsObj);
            assertInstanceOf(Utf8.class, natwordsObj);
            assertEquals("I am an NCLOB", natwordsObj.toString());
        }
    }

    @Test
    public void testNoRowsStatementCreatesEmptyFlowFile() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("Hello".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).get(0);
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "0");
        firstFlowFile.assertContentEquals("");
    }

    @Test
    public void testNoResultCreatesEmptyFlowFile() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.SQL_SELECT_QUERY, "drop table TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("Hello".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).get(0);
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "0");
        firstFlowFile.assertContentEquals("");
    }

    @Test
    public void testWithSqlException() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NO_ROWS");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NO_ROWS (id integer)");

        runner.setIncomingConnection(false);
        // Try a valid SQL statement that will generate an error (val1 does not exist, e.g.)
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "SELECT val1 FROM TEST_NO_ROWS");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        //No incoming flow file containing a query, and an exception causes no outbound flowfile.
        // There should be no flow files on either relationship
        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_FAILURE, 0);
        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 0);
    }

    public void invokeOnTriggerRecords(final Integer queryTimeout, final String query, final boolean incomingFlowFile, final Map<String, String> attrs, final boolean setQueryProperty)
            throws InitializationException, ClassNotFoundException, SQLException, IOException {

        if (queryTimeout != null) {
            runner.setProperty(AbstractExecuteSQL.QUERY_TIMEOUT, queryTimeout.toString() + " secs");
        }

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        SimpleCommerceDataSet.loadTestData2Database(con, 100, 200, 100);
        LOGGER.info("test data loaded");

        //commit loaded data if auto-commit is dissabled
        if (!con.getAutoCommit()) {
            con.commit();
        }

        // ResultSet size will be 1x200x100 = 20 000 rows
        // because of where PER.ID = ${person.id}

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        if (incomingFlowFile) {
            // incoming FlowFile content is not used, but attributes are used
            final Map<String, String> attributes = (attrs == null) ? new HashMap<>() : attrs;
            attributes.put("person.id", "10");
            if (!setQueryProperty) {
                runner.enqueue(query.getBytes(), attributes);
            } else {
                runner.enqueue("Hello".getBytes(), attributes);
            }
        }

        if (setQueryProperty) {
            runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, query);
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(AbstractExecuteSQL.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_QUERY_DURATION);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_QUERY_EXECUTION_TIME);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_QUERY_FETCH_TIME);
        runner.assertAllFlowFilesContainAttribute(AbstractExecuteSQL.REL_SUCCESS, AbstractExecuteSQL.RESULT_ROW_COUNT);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_SUCCESS);
        final long executionTime = Long.parseLong(flowfiles.get(0).getAttribute(AbstractExecuteSQL.RESULT_QUERY_EXECUTION_TIME));
        final long fetchTime = Long.parseLong(flowfiles.get(0).getAttribute(AbstractExecuteSQL.RESULT_QUERY_FETCH_TIME));
        final long durationTime = Long.parseLong(flowfiles.get(0).getAttribute(AbstractExecuteSQL.RESULT_QUERY_DURATION));
        assertEquals(durationTime, fetchTime + executionTime);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWithSqlExceptionErrorProcessingResultSet() throws Exception {
        DBCPService dbcp = mock(DBCPService.class);
        Connection conn = mock(Connection.class);
        when(dbcp.getConnection(any(Map.class))).thenReturn(conn);
        when(dbcp.getIdentifier()).thenReturn("mockdbcp");
        PreparedStatement statement = mock(PreparedStatement.class);
        when(conn.prepareStatement(anyString())).thenReturn(statement);
        when(statement.execute()).thenReturn(true);
        ResultSet rs = mock(ResultSet.class);
        when(statement.getResultSet()).thenReturn(rs);
        // Throw an exception the first time you access the ResultSet, this is after the flow file to hold the results has been created.
        when(rs.getMetaData()).thenThrow(new SQLException("test execute statement failed"));

        runner.addControllerService("mockdbcp", dbcp, new HashMap<>());
        runner.enableControllerService(dbcp);
        runner.setProperty(AbstractExecuteSQL.DBCP_SERVICE, "mockdbcp");

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.setIncomingConnection(true);
        runner.enqueue("SELECT 1");
        runner.run();

        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 0);

        // Assert exception message has been put to flow file attribute
        MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(AbstractExecuteSQL.REL_FAILURE).get(0);
        assertEquals("java.sql.SQLException: test execute statement failed", failedFlowFile.getAttribute(AbstractExecuteSQL.RESULT_ERROR_MESSAGE));
    }

    @Test
    public void testPreQuery() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_NULL_INT values(1,2,3)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        runner.setProperty(ExecuteSQLRecord.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).get(0);
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "1");
    }

    @Test
    public void testPostQuery() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");
        stmt.execute("insert into TEST_NULL_INT values(1,2,3)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        runner.setProperty(ExecuteSQLRecord.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        runner.setProperty(ExecuteSQLRecord.SQL_POST_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_SUCCESS).get(0);
        firstFlowFile.assertAttributeEquals(ExecuteSQLRecord.RESULT_ROW_COUNT, "1");
    }

    @Test
    public void testPreQueryFail() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        // Simulate failure by not provide parameter
        runner.setProperty(ExecuteSQLRecord.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS()");
        runner.setProperty(ExecuteSQLRecord.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_FAILURE, 1);
    }

    @Test
    public void testPostQueryFail() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQLRecord.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        runner.setProperty(ExecuteSQLRecord.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        // Simulate failure by not provide parameter
        runner.setProperty(ExecuteSQLRecord.SQL_POST_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS()");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLRecord.REL_FAILURE, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLRecord.REL_FAILURE).get(0);
        firstFlowFile.assertContentEquals("test");
    }


    /**
     * Simple implementation only for ExecuteSQL processor testing.
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        private final String type;

        public DBCPServiceSimpleImpl(String type) {
            this.type = type;

        }

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                final Connection con;
                if ("h2".equalsIgnoreCase(type)) {
                    con = DriverManager.getConnection("jdbc:h2:file:" + "./target/testdb7");
                } else {
                    Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                    con = DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
                }
                return con;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

}
