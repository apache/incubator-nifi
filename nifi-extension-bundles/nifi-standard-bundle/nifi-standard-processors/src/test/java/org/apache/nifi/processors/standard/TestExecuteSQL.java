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

import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.db.AvroUtil;
import org.apache.nifi.util.db.SimpleCommerceDataSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestExecuteSQL {

    private final Logger LOGGER = LoggerFactory.getLogger(TestExecuteSQL.class);

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
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(ExecuteSQL.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(ExecuteSQL.DBCP_SERVICE, "dbcp");
    }

    @Test
    public void testIncomingConnectionWithNoFlowFile() {
        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM persons");
        runner.run();
        runner.assertTransferCount(ExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteSQL.REL_FAILURE, 0);
    }

    @Test
    public void testIncomingConnectionWithNoFlowFileAndNoQuery() {
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertTransferCount(ExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteSQL.REL_FAILURE, 0);
    }

    @Test
    public void testNoIncomingConnectionAndNoQuery() {
        runner.setIncomingConnection(false);
        assertThrows(AssertionError.class, () -> runner.run());
    }

    @Test
    public void testNoIncomingConnection() throws SQLException, IOException {
        runner.setIncomingConnection(false);
        invokeOnTrigger(null, QUERY_WITHOUT_EL, false, null, true);
    }

    @Test
    public void testNoTimeLimit() throws SQLException, IOException {
        invokeOnTrigger(null, QUERY_WITH_EL, true, null, true);
    }

    @Test
    public void testSelectQueryInFlowFile() throws SQLException, IOException {
        invokeOnTrigger(null, QUERY_WITHOUT_EL, true, null, false);
    }

    @Test
    public void testSelectQueryInFlowFileWithParameters() throws SQLException, IOException {
        Map<String, String> sqlParams = new HashMap<>();
        sqlParams.put("sql.args.1.type", "4");
        sqlParams.put("sql.args.1.value", "20");
        sqlParams.put("sql.args.2.type", "4");
        sqlParams.put("sql.args.2.value", "5");

        invokeOnTrigger(null, QUERY_WITHOUT_EL_WITH_PARAMS, true, sqlParams, false);
    }

    @Test
    public void testQueryTimeout() throws SQLException, IOException {
        // Does to seem to have any effect when using embedded Derby
        invokeOnTrigger(1, QUERY_WITH_EL, true, null, true); // 1 second max time
    }

    @Test
    public void testAutoCommitFalse() throws SQLException, IOException {
        runner.setProperty(ExecuteSQL.AUTO_COMMIT, "false");
        invokeOnTrigger(null, QUERY_WITHOUT_EL, true, null, false);
    }

    @Test
    public void testAutoCommitTrue() throws SQLException, IOException {
        runner.setProperty(ExecuteSQL.AUTO_COMMIT, "true");
        invokeOnTrigger(null, QUERY_WITHOUT_EL, true, null, false);
    }

    @Test
    public void testWithNullIntColumn() throws SQLException {
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

        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst().assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "2");
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst().assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testDropTableWithOverwrite() throws SQLException, IOException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_DROP_TABLE");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_DROP_TABLE (id integer not null, val1 integer, val2 integer)");

        stmt.execute("insert into TEST_DROP_TABLE (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_DROP_TABLE (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "DROP TABLE TEST_DROP_TABLE");
        runner.enqueue("some data");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS);
        final InputStream in = new ByteArrayInputStream(flowfiles.getFirst().toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
            assertFalse(dataFileReader.hasNext());
        }
    }

    @Test
    public void testDropTableNoOverwrite() throws SQLException, IOException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_TRUNCATE_TABLE");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table TEST_TRUNCATE_TABLE (id integer not null, val1 integer, val2 integer)");

        stmt.execute("insert into TEST_TRUNCATE_TABLE (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_TRUNCATE_TABLE (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.OVERWRITE_FLOW_FILE_CONTENT, "false");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "TRUNCATE TABLE TEST_TRUNCATE_TABLE");
        runner.enqueue("some data");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.assertContents(ExecuteSQL.REL_SUCCESS, List.of("some data"));
    }

    @Test
    public void testCompression() throws SQLException, IOException {
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

        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.COMPRESSION_FORMAT, AvroUtil.CodecType.BZIP2.name());
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();

        try (DataFileStream<GenericRecord> dfs = new DataFileStream<>(new ByteArrayInputStream(flowFile.toByteArray()), new GenericDatumReader<>())) {
            assertEquals(AvroUtil.CodecType.BZIP2.name().toLowerCase(), dfs.getMetaString(DataFileConstants.CODEC).toLowerCase());
        }
    }

    @Test
    public void testWithOutputBatching() throws SQLException {
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
        runner.setProperty(ExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(ExecuteSQL.OUTPUT_BATCH_SIZE, "5");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 200);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();

        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(199);

        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testWithOutputBatchingAndIncomingFlowFile() throws SQLException {
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
        attrMap.put("max.rows", "5");
        attrMap.put("batch.size", "1");
        runner.setIncomingConnection(true);
        runner.setProperty(ExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "${max.rows}");
        runner.setProperty(ExecuteSQL.OUTPUT_BATCH_SIZE, "${batch.size}");
        MockFlowFile inputFlowFile = runner.enqueue("SELECT * FROM TEST_NULL_INT", attrMap);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 200);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();

        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(199);


        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(testAttrName, testAttrValue);
        lastFlowFile.assertAttributeEquals(AbstractExecuteSQL.INPUT_FLOWFILE_UUID, inputFlowFile.getAttribute(CoreAttributes.UUID.key()));
    }

    @Test
    public void testMaxRowsPerFlowFile() throws SQLException {
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
        runner.setProperty(ExecuteSQL.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(AbstractExecuteSQL.FETCH_SIZE, "5");
        runner.setProperty(ExecuteSQL.OUTPUT_BATCH_SIZE, "0");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 200);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_COUNT.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();

        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).get(199);

        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(ExecuteSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testInsertStatementCreatesFlowFile() throws SQLException {
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
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst().assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "0");
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
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        runner.enqueue("Hello".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "0");
        final InputStream in = new ByteArrayInputStream(firstFlowFile.toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
            GenericRecord record = null;
            long recordsFromStream = 0;
            while (dataFileReader.hasNext()) {
                // Reuse record object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                record = dataFileReader.next(record);
                recordsFromStream += 1;
            }

            assertEquals(0, recordsFromStream);
        }
    }

    @Test
    public void testWithDuplicateColumns() throws SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table host1");
            stmt.execute("drop table host2");
        } catch (final SQLException ignored) {
        }

        stmt.execute("create table host1 (id integer not null, host varchar(45))");
        stmt.execute("create table host2 (id integer not null, host varchar(45))");
        stmt.execute("insert into host1 values(1,'host1')");
        stmt.execute("insert into host2 values(1,'host2')");
        stmt.execute("select a.host as hostA,b.host as hostB from host1 a join host2 b on b.id=a.id");
        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select a.host as hostA,b.host as hostB from host1 a join host2 b on b.id=a.id");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst().assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "1");
    }

    @Test
    public void testWithSqlException() throws SQLException {
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
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "SELECT val1 FROM TEST_NO_ROWS");
        runner.run();

        //No incoming flow file containing a query, and an exception causes no outbound flowfile.
        // There should be no flow files on either relationship
        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_FAILURE, 0);
        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 0);
    }

    @Test
    @SuppressWarnings("unchecked")
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
        runner.setProperty(ExecuteSQL.DBCP_SERVICE, "mockdbcp");

        runner.setIncomingConnection(true);
        runner.enqueue("SELECT 1");
        runner.run();

        runner.assertTransferCount(ExecuteSQL.REL_FAILURE, 1);
        runner.assertTransferCount(ExecuteSQL.REL_SUCCESS, 0);

        // Assert exception message has been put to flow file attribute
        MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_FAILURE).getFirst();
        assertEquals("java.sql.SQLException: test execute statement failed", failedFlowFile.getAttribute(ExecuteSQL.RESULT_ERROR_MESSAGE));
    }

    public void invokeOnTrigger(final Integer queryTimeout, final String query, final boolean incomingFlowFile, final Map<String, String> attrs, final boolean setQueryProperty)
        throws SQLException, IOException {

        if (queryTimeout != null) {
            runner.setProperty(ExecuteSQL.QUERY_TIMEOUT, queryTimeout + " secs");
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
        final int nrOfRows = 20000;

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
            runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, query);
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_QUERY_DURATION);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_QUERY_EXECUTION_TIME);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_QUERY_FETCH_TIME);
        runner.assertAllFlowFilesContainAttribute(ExecuteSQL.REL_SUCCESS, ExecuteSQL.RESULT_ROW_COUNT);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS);
        final long executionTime = Long.parseLong(flowfiles.getFirst().getAttribute(ExecuteSQL.RESULT_QUERY_EXECUTION_TIME));
        final long fetchTime = Long.parseLong(flowfiles.getFirst().getAttribute(ExecuteSQL.RESULT_QUERY_FETCH_TIME));
        final long durationTime = Long.parseLong(flowfiles.getFirst().getAttribute(ExecuteSQL.RESULT_QUERY_DURATION));

        assertEquals(durationTime, fetchTime + executionTime);

        final InputStream in = new ByteArrayInputStream(flowfiles.getFirst().toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
            GenericRecord record = null;
            long recordsFromStream = 0;
            while (dataFileReader.hasNext()) {
                // Reuse record object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                record = dataFileReader.next(record);
                recordsFromStream += 1;
            }

            LOGGER.info("total nr of records from stream: {}", recordsFromStream);
            assertEquals(nrOfRows, recordsFromStream);
        }
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
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "1");

        final InputStream in = new ByteArrayInputStream(firstFlowFile.toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
            GenericRecord record = null;
            long recordsFromStream = 0;
            while (dataFileReader.hasNext()) {
                // Reuse record object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                record = dataFileReader.next(record);
                recordsFromStream += 1;
            }

            assertEquals(1, recordsFromStream);
        }
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
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        runner.setProperty(ExecuteSQL.SQL_POST_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_SUCCESS, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_SUCCESS).getFirst();
        firstFlowFile.assertAttributeEquals(ExecuteSQL.RESULT_ROW_COUNT, "1");

        final InputStream in = new ByteArrayInputStream(firstFlowFile.toByteArray());
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
            GenericRecord record = null;
            long recordsFromStream = 0;
            while (dataFileReader.hasNext()) {
                // Reuse record object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                record = dataFileReader.next(record);
                recordsFromStream += 1;
            }

            assertEquals(1, recordsFromStream);
        }
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
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS()");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_FAILURE, 1);
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
        runner.setProperty(ExecuteSQL.SQL_PRE_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        runner.setProperty(ExecuteSQL.SQL_SELECT_QUERY, "select * from TEST_NULL_INT");
        // Simulate failure by not provide parameter
        runner.setProperty(ExecuteSQL.SQL_POST_QUERY, "CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS()");
        runner.enqueue("test".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQL.REL_FAILURE, 1);
        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQL.REL_FAILURE).getFirst();
        firstFlowFile.assertContentEquals("test");
    }

    /**
     * Simple implementation only for ExecuteSQL processor testing.
     *
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                final Connection con = DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
                return Mockito.spy(con);
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

}
