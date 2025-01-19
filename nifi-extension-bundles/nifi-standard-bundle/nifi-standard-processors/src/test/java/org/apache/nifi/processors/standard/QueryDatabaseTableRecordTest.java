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

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.db.JdbcProperties;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the QueryDatabaseTableRecord processor
 */
public class QueryDatabaseTableRecordTest {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    MockQueryDatabaseTableRecord processor;
    protected TestRunner runner;
    private final static String DB_LOCATION = "target/db_qdt";
    private final static String TABLE_NAME_KEY = "tableName";
    private final static String MAX_ROWS_KEY = "maxRows";


    @BeforeAll
    public static void setupBeforeClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ioe) {
            // Do nothing, may not have existed
        }
    }

    @AfterAll
    public static void cleanUpAfterClass() {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true");
        } catch (Exception e) {
            // Do nothing, this is what happens at Derby shutdown
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        try {
            FileUtils.deleteFile(dbLocation, true);
        } catch (IOException ioe) {
            // Do nothing, may not have existed
        }
        System.clearProperty("derby.stream.error.file");
    }

    public String getDatabaseType() {
        return "Generic";
    }

    public void createDbcpControllerService() throws InitializationException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
    }

    @BeforeEach
    public void setup() throws InitializationException, IOException {
        processor = new MockQueryDatabaseTableRecord();
        runner = TestRunners.newTestRunner(processor);
        createDbcpControllerService();
        runner.setProperty(QueryDatabaseTableRecord.DBCP_SERVICE, "dbcp");
        runner.setProperty(QueryDatabaseTableRecord.DB_TYPE, getDatabaseType());
        runner.getStateManager().clear(Scope.CLUSTER);
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(QueryDatabaseTableRecord.RECORD_WRITER_FACTORY, "writer");
        runner.setProperty(JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION, "8");
        runner.setProperty(JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE, "2");
        runner.enableControllerService(recordWriter);
    }

    @AfterEach
    public void teardown() throws IOException {
        runner.getStateManager().clear(Scope.CLUSTER);
        runner = null;
    }

    @Test
    public void testAddedRows() throws SQLException, IOException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 2);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTableRecord.RESULT_TABLENAME));
        assertEquals(flowFile.getAttribute("maxvalue.id"), "2");
        runner.setProperty(QueryDatabaseTableRecord.FETCH_SIZE, "2");
        flowFile.assertAttributeEquals("record.count", "2");

        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(1);
        assertEquals(flowFile.getAttribute("maxvalue.id"), "2");
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        //Remove Max Rows Per Flow File
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "0");

        // Add a new row with a higher ID and run, one flowfile with one new row should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals(flowFile.getAttribute("maxvalue.id"), "3");
        flowFile.assertAttributeEquals("record.count", "1");

        // Sanity check - run again, this time no flowfiles/rows should be transferred
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add timestamp as a max value column name
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "id, created_on");

        // Add a new row with a higher ID and run, one flow file will be transferred because no max value for the timestamp has been stored
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (4, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals(flowFile.getAttribute("maxvalue.id"), "4");
        assertEquals(flowFile.getAttribute("maxvalue.created_on"), "2011-01-01 03:23:34.234");
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Add a new row with a higher ID but lower timestamp and run, no flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (5, 'NO NAME', 15.0, '2001-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (6, 'Mr. NiFi', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Set name as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "name");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "7");
        runner.clearTransferState();

        // Add a new row with a "higher" name than the max but lower than "NULL" (to test that null values are skipped), one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (7, 'NULK', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "scale");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "8");
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (8, 'NULK', 100.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "bignum");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "9");
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on, bignum) VALUES (9, 'Alice Bob', 100.0, '2012-01-01 03:23:34.234', 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();
    }

    @Test
    public void testAddedRowsAutoCommitTrue() throws SQLException, IOException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "2");
        runner.setProperty(QueryDatabaseTableRecord.FETCH_SIZE, "2");
        runner.setProperty(QueryDatabaseTable.AUTO_COMMIT, "true");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 2);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTableRecord.RESULT_TABLENAME));
        assertEquals(flowFile.getAttribute("maxvalue.id"), "2");
        flowFile.assertAttributeEquals("record.count", "2");

        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(1);
        assertEquals(flowFile.getAttribute("maxvalue.id"), "2");
        flowFile.assertAttributeEquals("record.count", "1");
    }

    @Test
    public void testAddedRowsAutoCommitFalse() throws SQLException, IOException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "2");
        runner.setProperty(QueryDatabaseTableRecord.FETCH_SIZE, "2");
        runner.setProperty(QueryDatabaseTable.AUTO_COMMIT, "false");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 2);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTableRecord.RESULT_TABLENAME));
        assertEquals(flowFile.getAttribute("maxvalue.id"), "2");
        flowFile.assertAttributeEquals("record.count", "2");

        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(1);
        assertEquals(flowFile.getAttribute("maxvalue.id"), "2");
        flowFile.assertAttributeEquals("record.count", "1");
    }

    @Test
    public void testAddedRowsTwoTables() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE2");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 2);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTableRecord.RESULT_TABLENAME));
        assertEquals(flowFile.getAttribute("maxvalue.id"), "2");
        flowFile.assertAttributeEquals("record.count", "2");
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(1);
        assertEquals(flowFile.getAttribute("maxvalue.id"), "2");
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Populate a second table and set
        stmt.execute("create table TEST_QUERY_DB_TABLE2 (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE2");
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "0");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);

        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals("TEST_QUERY_DB_TABLE2", flowFile.getAttribute(QueryDatabaseTableRecord.RESULT_TABLENAME));
        assertEquals(flowFile.getAttribute("maxvalue.id"), "2");
        flowFile.assertAttributeEquals("record.count", "3");
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flowfile with one new row should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals(flowFile.getAttribute("maxvalue.id"), "3");
        flowFile.assertAttributeEquals("record.count", "1");

        // Sanity check - run again, this time no flowfiles/rows should be transferred
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testMultiplePartitions() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, bucket integer not null)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (0, 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (1, 0)");

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "ID, BUCKET");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        assertEquals("2",
                runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0).getAttribute(QueryDatabaseTableRecord.RESULT_ROW_COUNT)
        );
        runner.clearTransferState();

        // Add a new row in the same bucket
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (2, 0)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        assertEquals("1",
                runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0).getAttribute(QueryDatabaseTableRecord.RESULT_ROW_COUNT)
        );
        runner.clearTransferState();

        // Add a new row in a new bucket
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (3, 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        assertEquals("1",
                runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0).getAttribute(QueryDatabaseTableRecord.RESULT_ROW_COUNT)
        );
        runner.clearTransferState();

        // Add a new row in an old bucket, it should not be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (4, 0)");
        runner.run();
        runner.assertTransferCount(QueryDatabaseTableRecord.REL_SUCCESS, 0);

        // Add a new row in the second bucket, only the new row should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (5, 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        assertEquals("1",
                runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0).getAttribute(QueryDatabaseTableRecord.RESULT_ROW_COUNT)
        );
        runner.clearTransferState();
    }

    @Test
    public void testTimestampNanos() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.000123456')");

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "created_on");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a lower timestamp (but same millisecond value), no flow file should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.000')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a higher timestamp, one flow file should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.0003')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testWithNullIntColumn() throws SQLException {
        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
            // Ignore, usually due to Derby not having DROP TABLE IF EXISTS
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (0, NULL, 1)");
        stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (1, 1, 1)");

        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0).assertAttributeEquals(QueryDatabaseTableRecord.RESULT_ROW_COUNT, "2");
    }

    @Test
    public void testWithSqlException() throws SQLException {
        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NO_ROWS");
        } catch (final SQLException sqle) {
            // Ignore, usually due to Derby not having DROP TABLE IF EXISTS
        }

        stmt.execute("create table TEST_NO_ROWS (id integer)");

        runner.setIncomingConnection(false);
        // Try a valid SQL statement that will generate an error (val1 does not exist, e.g.)
        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_NO_ROWS");
        runner.setProperty(QueryDatabaseTableRecord.COLUMN_NAMES, "val1");
        runner.run();

        assertTrue(runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).isEmpty());
    }

    @Test
    public void testOutputBatchSize() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();
        MockFlowFile mff;

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        int rowCount = 0;
        // Create larger row set
        for (int batch = 0; batch < 100; batch++) {
            stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
            rowCount++;
        }

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "${" + MAX_ROWS_KEY + "}");
        runner.setEnvironmentVariableValue(MAX_ROWS_KEY, "7");
        runner.setProperty(QueryDatabaseTableRecord.OUTPUT_BATCH_SIZE, "${outputBatchSize}");
        runner.setEnvironmentVariableValue("outputBatchSize", "4");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 15);

        // Ensure all but the last file have 7 records each
        for (int ff = 0; ff < 14; ff++) {
            mff = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(ff);
            mff.assertAttributeEquals("record.count", "7");

            mff.assertAttributeExists("fragment.identifier");
            assertEquals(Integer.toString(ff), mff.getAttribute("fragment.index"));
            // No fragment.count set for flow files sent when Output Batch Size is set
            assertNull(mff.getAttribute("fragment.count"));
        }

        // Last file should have 2 records
        mff = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(14);
        mff.assertAttributeEquals("record.count", "2");
        mff.assertAttributeExists("fragment.identifier");
        assertEquals(Integer.toString(14), mff.getAttribute("fragment.index"));
        // No fragment.count set for flow files sent when Output Batch Size is set
        assertNull(mff.getAttribute("fragment.count"));
    }

    @Test
    public void testMaxRowsPerFlowFile() throws IOException, SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();
        MockFlowFile mff;

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 100; batch++) {
            stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
            rowCount++;
        }

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "${" + MAX_ROWS_KEY + "}");
        runner.setEnvironmentVariableValue(MAX_ROWS_KEY, "9");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 12);

        //ensure all but the last file have 9 records each
        for (int ff = 0; ff < 11; ff++) {
            mff = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(ff);
            mff.assertAttributeEquals("record.count", "9");

            mff.assertAttributeExists("fragment.identifier");
            assertEquals(Integer.toString(ff), mff.getAttribute("fragment.index"));
            assertEquals("12", mff.getAttribute("fragment.count"));
        }

        //last file should have 1 record
        mff = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(11);
        mff.assertAttributeEquals("record.count", "1");
        mff.assertAttributeExists("fragment.identifier");
        assertEquals(Integer.toString(11), mff.getAttribute("fragment.index"));
        assertEquals("12", mff.getAttribute("fragment.count"));
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Run again, this time should be a single partial flow file
        for (int batch = 0; batch < 5; batch++) {
            stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
            rowCount++;
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        mff = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        mff.assertAttributeExists("fragment.identifier");
        assertEquals(Integer.toString(0), mff.getAttribute("fragment.index"));
        assertEquals("1", mff.getAttribute("fragment.count"));
        mff.assertAttributeEquals("record.count", "5");
        runner.clearTransferState();

        // Run again, this time should be a full batch and a partial
        for (int batch = 0; batch < 14; batch++) {
            stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
            rowCount++;
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 2);
        mff = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("record.count", "9");
        mff = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(1);
        mff.assertAttributeEquals("record.count", "5");
        runner.clearTransferState();

        // Run again with a cleaned state. Should get all rows split into batches
        int ffCount = (int) Math.ceil(rowCount / 9D);
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, ffCount);

        //ensure all but the last file have 9 records each
        for (int ff = 0; ff < ffCount - 1; ff++) {
            mff = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(ff);
            mff.assertAttributeEquals("record.count", "9");
        }

        mff = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(ffCount - 1);
        mff.assertAttributeEquals("record.count", Integer.toString(rowCount % 9));
        runner.clearTransferState();
    }

    @Test
    public void testMaxRowsPerFlowFileWithMaxFragments() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();
        MockFlowFile mff;

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 100; batch++) {
            stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
            rowCount++;
        }

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "9");
        Integer maxFragments = 3;
        runner.setProperty(QueryDatabaseTableRecord.MAX_FRAGMENTS, maxFragments.toString());

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, maxFragments);

        for (int i = 0; i < maxFragments; i++) {
            mff = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(i);
            mff.assertAttributeEquals("record.count", "9");

            mff.assertAttributeExists("fragment.identifier");
            assertEquals(Integer.toString(i), mff.getAttribute("fragment.index"));
            assertEquals(maxFragments.toString(), mff.getAttribute("fragment.count"));
        }

        runner.clearTransferState();
    }

    @Test
    public void testInitialMaxValue() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");

        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);

        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 10; batch++) {
            stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '" + DATE_TIME_FORMATTER.format(dateTime) + "')");

            rowCount++;
            dateTime = dateTime.plusMinutes(1);
        }

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "${" + TABLE_NAME_KEY + "}");
        runner.setEnvironmentVariableValue(TABLE_NAME_KEY, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "created_on");

        dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC).plusMinutes(5);
        runner.setProperty("initial.maxvalue.CREATED_ON", DATE_TIME_FORMATTER.format(dateTime));
        // Initial run with no previous state. Should get only last 4 records
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "4");
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        // Validate Max Value doesn't change also
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();
    }

    @Test
    public void testInitialMaxValueWithEL() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");

        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 10; batch++) {
            stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '" + DATE_TIME_FORMATTER.format(dateTime) + "')");

            rowCount++;
            dateTime = dateTime.plusMinutes(1);
        }

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "${" + TABLE_NAME_KEY + "}");
        runner.setEnvironmentVariableValue(TABLE_NAME_KEY, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "created_on");

        dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC).plusMinutes(5);
        runner.setProperty("initial.maxvalue.CREATED_ON", "${created.on}");
        runner.setEnvironmentVariableValue("created.on", DATE_TIME_FORMATTER.format(dateTime));
        // Initial run with no previous state. Should get only last 4 records
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "4");
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        // Validate Max Value doesn't change also
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();

        // Append a new row, expect 1 flowfile one row
        dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC).plusMinutes(rowCount);
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '" + DATE_TIME_FORMATTER.format(dateTime) + "')");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:10:00.0", Scope.CLUSTER);
        runner.clearTransferState();
    }

    @Test
    public void testInitialLoadStrategyStartAtBeginning() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");

        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 10; batch++) {
            stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '" + DATE_TIME_FORMATTER.format(dateTime) + "')");

            rowCount++;
            dateTime = dateTime.plusMinutes(1);
        }

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "${" + TABLE_NAME_KEY + "}");
        runner.setEnvironmentVariableValue(TABLE_NAME_KEY, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "created_on");
        runner.setProperty(QueryDatabaseTableRecord.INITIAL_LOAD_STRATEGY, QueryDatabaseTableRecord.INITIAL_LOAD_STRATEGY_ALL_ROWS.getValue());

        // Initial run with no previous state. Should get all 10 records
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "10");
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        // Validate Max Value doesn't change also
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();
    }

    @Test
    public void testInitialLoadStrategyStartAtCurrentMaximumValues() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");

        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
        int rowCount = 0;
        //create larger row set
        for (int batch = 0; batch < 10; batch++) {
            stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (" + rowCount + ", 'Joe Smith', 1.0, '" + DATE_TIME_FORMATTER.format(dateTime) + "')");

            rowCount++;
            dateTime = dateTime.plusMinutes(1);
        }

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "${" + TABLE_NAME_KEY + "}");
        runner.setEnvironmentVariableValue(TABLE_NAME_KEY, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "created_on");
        runner.setProperty(QueryDatabaseTableRecord.INITIAL_LOAD_STRATEGY, QueryDatabaseTableRecord.INITIAL_LOAD_STRATEGY_NEW_ROWS.getValue());

        // Initial run with no previous state. Should not get any records but store Max Value in the state
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        // Validate Max Value doesn't change also
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.getStateManager().assertStateEquals("test_query_db_table" + AbstractDatabaseFetchProcessor.NAMESPACE_DELIMITER + "created_on", "1970-01-01 00:09:00.0", Scope.CLUSTER);
        runner.clearTransferState();
    }

    @Test
    public void testAddedRowsCustomWhereClause() throws SQLException, IOException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, type varchar(20), name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (0, 'male', 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (1, 'female', 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (2, NULL, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setProperty(QueryDatabaseTableRecord.WHERE_CLAUSE, "type = 'male'");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTableRecord.RESULT_TABLENAME));
        assertEquals(flowFile.getAttribute("maxvalue.id"), "0");
        flowFile.assertAttributeEquals("record.count", "1");

        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        //Remove Max Rows Per Flow File
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "0");

        // Add a new row with a higher ID and run, one flowfile with one new row should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (3, 'female', 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Sanity check - run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add timestamp as a max value column name
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "id, created_on");

        // Add a new row with a higher ID and run, one flow file will be transferred because no max value for the timestamp has been stored
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (4, 'male', 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals(flowFile.getAttribute("maxvalue.id"), "4");
        assertEquals(flowFile.getAttribute("maxvalue.created_on"), "2011-01-01 03:23:34.234");
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Add a new row with a higher ID but lower timestamp and run, no flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (5, 'male', 'NO NAME', 15.0, '2001-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (6, 'male', 'Mr. NiFi', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Set name as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "name");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "4");
        runner.clearTransferState();

        // Add a new row with a "higher" name than the max but lower than "NULL" (to test that null values are skipped), one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (7, 'male', 'NULK', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "scale");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "5");
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (8, 'male', 'NULK', 100.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "bignum");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "6");
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on, bignum) VALUES (9, 'female', 'Alice Bob', 100.0, '2012-01-01 03:23:34.234', 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testCustomSQL() throws SQLException, IOException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        try {
            stmt.execute("drop table TYPE_LIST");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, type varchar(20), name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (0, 'male', 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (1, 'female', 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (2, NULL, NULL, 2.0, '2010-01-01 00:00:00')");

        stmt.execute("create table TYPE_LIST (type_id integer not null, type varchar(20), descr varchar(255))");
        stmt.execute("insert into TYPE_LIST (type_id, type,descr) VALUES (0, 'male', 'Man')");
        stmt.execute("insert into TYPE_LIST (type_id, type,descr) VALUES (1, 'female', 'Woman')");
        stmt.execute("insert into TYPE_LIST (type_id, type,descr) VALUES (2, '', 'Unspecified')");

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setProperty(QueryDatabaseTableRecord.SQL_QUERY,
                "SELECT id, b.type as gender, b.descr, name, scale, created_on, bignum FROM TEST_QUERY_DB_TABLE a INNER JOIN TYPE_LIST b ON (a.type=b.type)");
        runner.setProperty(QueryDatabaseTableRecord.WHERE_CLAUSE, "gender = 'male'");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute(QueryDatabaseTableRecord.RESULT_TABLENAME));
        assertEquals(flowFile.getAttribute("maxvalue.id"), "0");
        flowFile.assertAttributeEquals("record.count", "1");

        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        //Remove Max Rows Per Flow File
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "0");

        // Add a new row with a higher ID and run, one flowfile with one new row should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (3, 'female', 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Sanity check - run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add timestamp as a max value column name
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "id, created_on");

        // Add a new row with a higher ID and run, one flow file will be transferred because no max value for the timestamp has been stored
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (4, 'male', 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        assertEquals(flowFile.getAttribute("maxvalue.id"), "4");
        assertEquals(flowFile.getAttribute("maxvalue.created_on"), "2011-01-01 03:23:34.234");
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Add a new row with a higher ID but lower timestamp and run, no flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (5, 'male', 'NO NAME', 15.0, '2001-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (6, 'male', 'Mr. NiFi', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Set name as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "name");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "4");
        runner.clearTransferState();

        // Add a new row with a "higher" name than the max but lower than "NULL" (to test that null values are skipped), one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (7, 'male', 'NULK', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "scale");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "5");
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (8, 'male', 'NULK', 100.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "1");
        runner.clearTransferState();

        // Set scale as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "bignum");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(QueryDatabaseTableRecord.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("record.count", "6");
        runner.clearTransferState();

        // Add a new row with a higher value for scale than the max, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on, bignum) VALUES (9, 'female', 'Alice Bob', 100.0, '2012-01-01 03:23:34.234', 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(QueryDatabaseTableRecord.REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testMissingColumn() throws ProcessException, SQLException {
        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        try {
            stmt.execute("drop table TYPE_LIST");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, type varchar(20), name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (0, 'male', 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (1, 'female', 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (2, NULL, NULL, 2.0, '2010-01-01 00:00:00')");

        stmt.execute("create table TYPE_LIST (type_id integer not null, type varchar(20), descr varchar(255))");
        stmt.execute("insert into TYPE_LIST (type_id, type,descr) VALUES (0, 'male', 'Man')");
        stmt.execute("insert into TYPE_LIST (type_id, type,descr) VALUES (1, 'female', 'Woman')");
        stmt.execute("insert into TYPE_LIST (type_id, type,descr) VALUES (2, '', 'Unspecified')");

        runner.setProperty(QueryDatabaseTableRecord.TABLE_NAME, "TYPE_LIST");
        runner.setProperty(QueryDatabaseTableRecord.SQL_QUERY, "SELECT b.type, b.descr, name, scale, created_on, bignum FROM TEST_QUERY_DB_TABLE a INNER JOIN TYPE_LIST b ON (a.type=b.type)");
        runner.setProperty(QueryDatabaseTableRecord.WHERE_CLAUSE, "type = 'male'");
        runner.setIncomingConnection(false);
        runner.setProperty(QueryDatabaseTableRecord.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(QueryDatabaseTableRecord.MAX_ROWS_PER_FLOW_FILE, "2");

        assertThrows(AssertionError.class, () -> {
            runner.run();
        });
    }

    /**
     * Simple implementation only for QueryDatabaseTableRecord processor testing.
     */
    private class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                return DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    @Stateful(scopes = Scope.CLUSTER, description = "Mock for QueryDatabaseTableRecord processor")
    protected static class MockQueryDatabaseTableRecord extends QueryDatabaseTableRecord {
        void putColumnType(String colName, Integer colType) {
            columnTypeMap.put(colName, colType);
        }
    }
}