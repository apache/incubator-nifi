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


import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.MockSessionFactory;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.standard.AbstractDatabaseFetchProcessor.DB_TYPE;
import static org.apache.nifi.processors.standard.AbstractDatabaseFetchProcessor.FRAGMENT_COUNT;
import static org.apache.nifi.processors.standard.AbstractDatabaseFetchProcessor.FRAGMENT_ID;
import static org.apache.nifi.processors.standard.AbstractDatabaseFetchProcessor.FRAGMENT_INDEX;
import static org.apache.nifi.processors.standard.AbstractDatabaseFetchProcessor.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


/**
 * Unit tests for the GenerateTableFetch processor.
 */
public class TestGenerateTableFetch {

    TestRunner runner;
    GenerateTableFetch processor;
    DBCPServiceSimpleImpl dbcp;

    private final static String DB_LOCATION = "target/db_gtf";

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
    public static void cleanUpAfterClass() throws Exception {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true");
        } catch (SQLNonTransientConnectionException e) {
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

    @BeforeEach
    public void setUp() throws Exception {
        processor = new GenerateTableFetch();
        //Mock the DBCP Controller Service so we can control the Results
        dbcp = spy(new DBCPServiceSimpleImpl());

        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(GenerateTableFetch.DBCP_SERVICE, "dbcp");
        runner.setProperty(DB_TYPE, "Derby");
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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");

        runner.run();

        // Assert all the sessions were committed
        MockSessionFactory runnerSessionFactory = (MockSessionFactory) runner.getProcessSessionFactory();
        Set<MockProcessSession> sessions = runnerSessionFactory.getCreatedSessions();
        for (MockProcessSession session : sessions) {
            session.assertCommitted();
        }

        // Verify the expected FlowFile
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID <= 2 ORDER BY ID FETCH NEXT 10000 ROWS ONLY", query);
        flowFile.assertAttributeEquals(FRAGMENT_INDEX, "0");
        flowFile.assertAttributeEquals(FRAGMENT_COUNT, "1");
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be three records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add 3 new rows with a higher ID and run with a partition size of 2. Two flow files should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (4, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (5, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "2");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);
        // Check fragment attributes
        List<MockFlowFile> resultFFs = runner.getFlowFilesForRelationship(REL_SUCCESS);
        MockFlowFile ff1 = resultFFs.get(0);
        MockFlowFile ff2 = resultFFs.get(1);
        assertEquals(ff1.getAttribute(FRAGMENT_ID), ff2.getAttribute(FRAGMENT_ID));
        assertEquals(ff1.getAttribute(FRAGMENT_INDEX), "0");
        assertEquals(ff1.getAttribute(FRAGMENT_COUNT), "2");
        assertEquals(ff2.getAttribute(FRAGMENT_INDEX), "1");
        assertEquals(ff2.getAttribute(FRAGMENT_COUNT), "2");

        // Verify first flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 2 AND ID <= 5 ORDER BY ID FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be two records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        // Verify second flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 2 AND ID <= 5 ORDER BY ID OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (6, 'Mr. NiFi', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 5 AND ID <= 6 ORDER BY ID FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Set name as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "name");
        runner.setProperty(GenerateTableFetch.COLUMN_NAMES, "id, name, scale, created_on");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 4); // 7 records with partition size 2 means 4 generated FlowFiles
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertEquals("SELECT id, name, scale, created_on FROM TEST_QUERY_DB_TABLE WHERE name <= 'Mr. NiFi' ORDER BY name FETCH NEXT 2 ROWS ONLY", new String(flowFile.toByteArray()));
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        assertEquals("SELECT id, name, scale, created_on FROM TEST_QUERY_DB_TABLE WHERE name <= 'Mr. NiFi' ORDER BY name OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY", new String(flowFile.toByteArray()));
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(2);
        assertEquals("SELECT id, name, scale, created_on FROM TEST_QUERY_DB_TABLE WHERE name <= 'Mr. NiFi' ORDER BY name OFFSET 4 ROWS FETCH NEXT 2 ROWS ONLY", new String(flowFile.toByteArray()));
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(3);
        assertEquals("SELECT id, name, scale, created_on FROM TEST_QUERY_DB_TABLE WHERE name <= 'Mr. NiFi' ORDER BY name OFFSET 6 ROWS FETCH NEXT 2 ROWS ONLY", new String(flowFile.toByteArray()));
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute("generatetablefetch.tableName"));
        assertEquals("id, name, scale, created_on", flowFile.getAttribute("generatetablefetch.columnNames"));
        assertEquals("name <= 'Mr. NiFi'", flowFile.getAttribute("generatetablefetch.whereClause"));
        assertEquals("name", flowFile.getAttribute("generatetablefetch.maxColumnNames"));
        assertEquals("2", flowFile.getAttribute("generatetablefetch.limit"));
        assertEquals("6", flowFile.getAttribute("generatetablefetch.offset"));

        runner.clearTransferState();
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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID <= 2 ORDER BY ID FETCH NEXT 10000 ROWS ONLY", query);
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be three records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.clearTransferState();

        // Create and populate a new table and re-run
        stmt.execute("create table TEST_QUERY_DB_TABLE2 (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE2");

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE2 WHERE ID <= 2 ORDER BY ID FETCH NEXT 10000 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be three records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Add 3 new rows with a higher ID and run with a partition size of 2. Two flow files should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (4, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (5, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "2");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        // Verify first flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE2 WHERE ID > 2 AND ID <= 5 ORDER BY ID FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be two records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        // Verify second flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE2 WHERE ID > 2 AND ID <= 5 ORDER BY ID OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();
    }

    @Test
    public void testAddedRowsRightBounded() throws SQLException, IOException {

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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID <= 2 ORDER BY ID FETCH NEXT 10000 ROWS ONLY", query);
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be three records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add 3 new rows with a higher ID and run with a partition size of 2. Two flow files should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (4, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (5, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "2");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        // Verify first flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 2 AND ID <= 5 ORDER BY ID FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be two records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        // Verify second flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 2 AND ID <= 5 ORDER BY ID OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (6, 'Mr. NiFi', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 5 AND ID <= 6 ORDER BY ID FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Set name as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "name");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 4); // 7 records with partition size 2 means 4 generated FlowFiles
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE name <= 'Mr. NiFi' ORDER BY name FETCH NEXT 2 ROWS ONLY", new String(flowFile.toByteArray()));
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE name <= 'Mr. NiFi' ORDER BY name OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY", new String(flowFile.toByteArray()));
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(2);
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE name <= 'Mr. NiFi' ORDER BY name OFFSET 4 ROWS FETCH NEXT 2 ROWS ONLY", new String(flowFile.toByteArray()));
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(3);
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE name <= 'Mr. NiFi' ORDER BY name OFFSET 6 ROWS FETCH NEXT 2 ROWS ONLY", new String(flowFile.toByteArray()));

        runner.clearTransferState();
    }

    @Test
    public void testAddedRowsTimestampRightBounded() throws SQLException {

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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "created_on");

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE created_on <= '2010-01-01 00:00:00.0' ORDER BY created_on FETCH NEXT 10000 ROWS ONLY", query);
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be three records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add 5 new rows, 3 with higher timestamps, 2 with a lower timestamp.
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (4, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (5, 'Marty Johnson', 15.0, '2011-01-01 02:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (6, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (7, 'James Johnson', 16.0, '2011-01-01 04:23:34.236')");
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "2");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        // Verify first flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE created_on > '2010-01-01 00:00:00.0' AND "
                + "created_on <= '2011-01-01 04:23:34.236' ORDER BY created_on FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be two records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        // Verify second flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE created_on > '2010-01-01 00:00:00.0' AND "
                + "created_on <= '2011-01-01 04:23:34.236' ORDER BY created_on OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Add a new row with a higher created_on and run, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (8, 'Mr. NiFi', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE created_on > '2011-01-01 04:23:34.236' AND created_on <= '2012-01-01 03:23:34.234' ORDER BY created_on FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();
    }

    @Test
    public void testOnePartition() throws SQLException {

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
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (2, 0)");

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");
        // Set partition size to 0 so we can see that the flow file gets all rows
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "0");

        runner.run();
        runner.assertAllFlowFilesTransferred(GenerateTableFetch.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GenerateTableFetch.REL_SUCCESS).getFirst();
        flowFile.assertContentEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID <= 2");
        flowFile.assertAttributeExists("generatetablefetch.limit");
        flowFile.assertAttributeEquals("generatetablefetch.limit", null);
        runner.clearTransferState();
    }

    @Test
    public void testFlowFileGeneratedOnZeroResults() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, bucket integer not null)");

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.COLUMN_NAMES, "ID,BUCKET");
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");
        // Set partition size to 0 so we can see that the flow file gets all rows
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "1");
        runner.setProperty(GenerateTableFetch.OUTPUT_EMPTY_FLOWFILE_ON_ZERO_RESULTS, "false");

        runner.run();
        runner.assertAllFlowFilesTransferred(GenerateTableFetch.REL_SUCCESS, 0);
        runner.clearTransferState();

        runner.setProperty(GenerateTableFetch.OUTPUT_EMPTY_FLOWFILE_ON_ZERO_RESULTS, "true");
        runner.run();
        runner.assertAllFlowFilesTransferred(GenerateTableFetch.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GenerateTableFetch.REL_SUCCESS).getFirst();
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute("generatetablefetch.tableName"));
        assertEquals("ID,BUCKET", flowFile.getAttribute("generatetablefetch.columnNames"));
        assertEquals("1=1", flowFile.getAttribute("generatetablefetch.whereClause"));
        assertEquals("ID", flowFile.getAttribute("generatetablefetch.maxColumnNames"));
        assertNull(flowFile.getAttribute("generatetablefetch.limit"));
        assertNull(flowFile.getAttribute("generatetablefetch.offset"));
        assertEquals("0", flowFile.getAttribute("fragment.index"));
        assertEquals("0", flowFile.getAttribute("fragment.count"));
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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID, BUCKET");
        // Set partition size to 1 so we can compare flow files to records
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "1");

        runner.run();
        runner.assertAllFlowFilesTransferred(GenerateTableFetch.REL_SUCCESS, 2);
        runner.clearTransferState();

        // Add a new row in the same bucket
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (2, 0)");
        runner.run();
        runner.assertAllFlowFilesTransferred(GenerateTableFetch.REL_SUCCESS, 1);
        runner.clearTransferState();

        // Add a new row in a new bucket
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (3, 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(GenerateTableFetch.REL_SUCCESS, 1);
        runner.clearTransferState();

        // Add a new row in an old bucket, it should not be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (4, 0)");
        runner.run();
        runner.assertTransferCount(GenerateTableFetch.REL_SUCCESS, 0);

        // Add a new row in the second bucket, only the new row should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (5, 1)");
        runner.run();
        runner.assertAllFlowFilesTransferred(GenerateTableFetch.REL_SUCCESS, 1);
        runner.clearTransferState();
    }

    @Test
    public void testMultiplePartitionsIncomingFlowFiles() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE1");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE1 (id integer not null, bucket integer not null)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE1 (id, bucket) VALUES (0, 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE1 (id, bucket) VALUES (1, 0)");

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE2");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE2 (id integer not null, bucket integer not null)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, bucket) VALUES (0, 0)");


        runner.setProperty(GenerateTableFetch.TABLE_NAME, "${tableName}");
        runner.setIncomingConnection(true);
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "${partSize}");

        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE1", "partSize", "1"));

        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE2", "partSize", "2"));

        // The table does not exist, expect the original flow file to be routed to failure
        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE3", "partSize", "1"));

        runner.run(3);
        runner.assertTransferCount(AbstractDatabaseFetchProcessor.REL_SUCCESS, 3);

        // Two records from table 1
        assertEquals(runner.getFlowFilesForRelationship(AbstractDatabaseFetchProcessor.REL_SUCCESS).stream().filter(
                (ff) -> "TEST_QUERY_DB_TABLE1".equals(ff.getAttribute("tableName"))).count(),
                2);

        // One record from table 2
        assertEquals(runner.getFlowFilesForRelationship(AbstractDatabaseFetchProcessor.REL_SUCCESS).stream().filter(
                (ff) -> "TEST_QUERY_DB_TABLE2".equals(ff.getAttribute("tableName"))).count(),
                1);

        // Table 3 doesn't exist, should be routed to failure
        runner.assertTransferCount(GenerateTableFetch.REL_FAILURE, 1);

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE1");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }
        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE2");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }
    }

    @Test
    public void testBackwardsCompatibilityStateKeyStaticTableDynamicMaxValues() throws Exception {
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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(true);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "${maxValueCol}");
        runner.enqueue("".getBytes(), Map.of("maxValueCol", "id"));

        // Pre-populate the state with a key for column name (not fully-qualified)
        StateManager stateManager = runner.getStateManager();
        stateManager.setState(Map.of("id", "0"), Scope.CLUSTER);

        // Pre-populate the column type map with an entry for id (not fully-qualified)
        processor.columnTypeMap.put("id", 4);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE id > 0 AND id <= 1 ORDER BY id FETCH NEXT 10000 ROWS ONLY", new String(flowFile.toByteArray()));
    }

    @Test
    public void testBackwardsCompatibilityStateKeyDynamicTableDynamicMaxValues() throws Exception {
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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "${tableName}");
        runner.setIncomingConnection(true);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "${maxValueCol}");
        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE", "maxValueCol", "id"));

        // Pre-populate the state with a key for column name (not fully-qualified)
        StateManager stateManager = runner.getStateManager();
        stateManager.setState(Map.of("id", "0"), Scope.CLUSTER);

        // Pre-populate the column type map with an entry for id (not fully-qualified)
        processor.columnTypeMap.put("id", 4);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        // Note there is no WHERE clause here. Because we are using dynamic tables, the old state key/value is not retrieved
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE id <= 1 ORDER BY id FETCH NEXT 10000 ROWS ONLY", new String(flowFile.toByteArray()));
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute("generatetablefetch.tableName"));
        assertNull(flowFile.getAttribute("generatetablefetch.columnNames"));
        assertEquals("id <= 1", flowFile.getAttribute("generatetablefetch.whereClause"));
        assertEquals("id", flowFile.getAttribute("generatetablefetch.maxColumnNames"));
        assertEquals("10000", flowFile.getAttribute("generatetablefetch.limit"));
        assertEquals("0", flowFile.getAttribute("generatetablefetch.offset"));

        runner.clearTransferState();
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (2, 0)");

        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE", "maxValueCol", "id"));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE id > 1 AND id <= 2 ORDER BY id FETCH NEXT 10000 ROWS ONLY", new String(flowFile.toByteArray()));
        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute("generatetablefetch.tableName"));
        assertNull(flowFile.getAttribute("generatetablefetch.columnNames"));
        assertEquals("id > 1 AND id <= 2", flowFile.getAttribute("generatetablefetch.whereClause"));
        assertEquals("id", flowFile.getAttribute("generatetablefetch.maxColumnNames"));
        assertEquals("10000", flowFile.getAttribute("generatetablefetch.limit"));
        assertEquals("0", flowFile.getAttribute("generatetablefetch.offset"));
    }

    @Test
    public void testBackwardsCompatibilityStateKeyDynamicTableStaticMaxValues() throws Exception {
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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "${tableName}");
        runner.setIncomingConnection(true);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "id");
        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE"));

        // Pre-populate the state with a key for column name (not fully-qualified)
        StateManager stateManager = runner.getStateManager();
        stateManager.setState(Map.of("id", "0"), Scope.CLUSTER);

        // Pre-populate the column type map with an entry for id (not fully-qualified)
        processor.columnTypeMap.put("id", 4);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        // Note there is no WHERE clause here. Because we are using dynamic tables, the old state key/value is not retrieved
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE id <= 1 ORDER BY id FETCH NEXT 10000 ROWS ONLY", new String(flowFile.toByteArray()));

        runner.clearTransferState();
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (2, 0)");

        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE", "maxValueCol", "id"));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE id > 1 AND id <= 2 ORDER BY id FETCH NEXT 10000 ROWS ONLY", new String(flowFile.toByteArray()));
    }

    @Test
    public void testBackwardsCompatibilityStateKeyVariableRegistry() throws Exception {
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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "${tableName}");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "${maxValueCol}");

        runner.setEnvironmentVariableValue("tableName", "TEST_QUERY_DB_TABLE");
        runner.setEnvironmentVariableValue("maxValueCol", "id");

        // Pre-populate the state with a key for column name (not fully-qualified)
        StateManager stateManager = runner.getStateManager();
        stateManager.setState(Map.of("id", "0"), Scope.CLUSTER);
        // Pre-populate the column type map with an entry for id (not fully-qualified)
        processor.columnTypeMap.put("id", 4);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        // Note there is no WHERE clause here. Because we are using dynamic tables (i.e. Expression Language,
        // even when not referring to flow file attributes), the old state key/value is not retrieved
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE id <= 1 ORDER BY id FETCH NEXT 10000 ROWS ONLY", new String(flowFile.toByteArray()));
    }

    @Test
    public void testRidiculousRowCount() throws SQLException {
        long rowCount = Long.parseLong(Integer.toString(Integer.MAX_VALUE)) + 100;
        int partitionSize = 1000000;
        int expectedFileCount = (int) (rowCount / partitionSize) + 1;

        Connection conn = mock(Connection.class);
        when(dbcp.getConnection()).thenReturn(conn);
        Statement st = mock(Statement.class);
        when(conn.createStatement()).thenReturn(st);
        doNothing().when(st).close();
        ResultSet rs = mock(ResultSet.class);
        when(st.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn((int) rowCount);
        when(rs.getLong(1)).thenReturn(rowCount);

        final ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(2);
        when(resultSetMetaData.getTableName(1)).thenReturn("");
        when(resultSetMetaData.getColumnType(1)).thenReturn(Types.INTEGER);
        when(resultSetMetaData.getColumnName(1)).thenReturn("COUNT");
        when(resultSetMetaData.getColumnType(2)).thenReturn(Types.INTEGER);
        when(resultSetMetaData.getColumnName(2)).thenReturn("ID");
        when(rs.getInt(2)).thenReturn(1000);


        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, Integer.toString(partitionSize));

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, expectedFileCount);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE 1=1 ORDER BY ID FETCH NEXT 1000000 ROWS ONLY", query);
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
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty("initial.maxvalue.ID", "1");

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 1 AND ID <= 2 ORDER BY ID FETCH NEXT 10000 ROWS ONLY", query);
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be one record (the initial max value skips the first two)
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add 3 new rows with a higher ID and run with a partition size of 2. Two flow files should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (4, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (5, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "2");
        runner.setProperty("initial.maxvalue.ID", "5"); // This should have no effect as there is a max value in the processor state
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        // Verify first flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 2 AND ID <= 5 ORDER BY ID FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be two records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        // Verify second flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 2 AND ID <= 5 ORDER BY ID OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
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
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty("initial.maxvalue.ID", "${maxval.id}");
        runner.setEnvironmentVariableValue("maxval.id", "1");

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 1 AND ID <= 2 ORDER BY ID FETCH NEXT 10000 ROWS ONLY", query);
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be one record (the initial max value skips the first two)
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testInitialMaxValueWithELAndIncoming() throws SQLException {

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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty("initial.maxvalue.ID", "${maxval.id}");
        Map<String, String> attrs = Map.of("maxval.id", "1");
        runner.setIncomingConnection(true);
        runner.enqueue(new byte[0], attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 1 AND ID <= 2 ORDER BY ID FETCH NEXT 10000 ROWS ONLY", query);
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be one record (the initial max value skips the first two)
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.enqueue(new byte[0], attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testInitialMaxValueWithELAndMultipleTables() throws SQLException {

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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "${table.name}");
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty("initial.maxvalue.ID", "${maxval.id}");
        Map<String, String> attrs = new HashMap<>();
        attrs.put("maxval.id", "1");
        attrs.put("table.name", "TEST_QUERY_DB_TABLE");        runner.setIncomingConnection(true);
        runner.enqueue(new byte[0], attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 1 AND ID <= 2 ORDER BY ID FETCH NEXT 10000 ROWS ONLY", query);
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be one record (the initial max value skips the first two)
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.enqueue(new byte[0], attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.clearTransferState();

        // Initial Max Value for second table
        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE2");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE2 (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (1, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE2 (id, name, scale, created_on) VALUES (2, NULL, 2.0, '2010-01-01 00:00:00')");

        attrs.put("table.name", "TEST_QUERY_DB_TABLE2");
        runner.enqueue(new byte[0], attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE2 WHERE ID > 1 AND ID <= 2 ORDER BY ID FETCH NEXT 10000 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record (the initial max value skips the first two)
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.enqueue(new byte[0], attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.clearTransferState();
    }

    @Test
    public void testNoDuplicateWithRightBounded() throws SQLException {

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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());

        // we now insert a row before the query issued by GFT is actually executed by, let's say, ExecuteSQL processor
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (3, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (4, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (5, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");

        ResultSet resultSet = stmt.executeQuery(query);
        int numberRecordsFirstExecution = 0; // Should be three records
        while (resultSet.next()) {
            numberRecordsFirstExecution++;
        }
        runner.clearTransferState();

        // Run again
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());

        resultSet = stmt.executeQuery(query);
        int numberRecordsSecondExecution = 0; // Should be three records
        while (resultSet.next()) {
            numberRecordsSecondExecution++;
        }

        // will fail and will be equal to 9 if right-bounded parameter is set to false.
        assertEquals(numberRecordsFirstExecution + numberRecordsSecondExecution, 6);

        runner.clearTransferState();
    }

    @Test
    public void testAddedRowsWithCustomWhereClause() throws SQLException, IOException {

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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setProperty(GenerateTableFetch.WHERE_CLAUSE, "type = 'male' OR type IS NULL");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE (type = 'male' OR type IS NULL)"
                + " AND ID <= 2 ORDER BY ID FETCH NEXT 10000 ROWS ONLY", query);
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be two records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add 3 new rows with a higher ID and run with a partition size of 2. Two flow files should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (3, 'female', 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (4, 'male', 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (5, 'male', 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "1");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        // Verify first flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 2 AND (type = 'male' OR type IS NULL)"
                + " AND ID <= 5 ORDER BY ID FETCH NEXT 1 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        // Verify second flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 2 AND (type = 'male' OR type IS NULL)"
                + " AND ID <= 5 ORDER BY ID OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Add a new row with a higher ID and run, one flow file will be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, type, name, scale, created_on) VALUES (6, 'male', 'Mr. NiFi', 1.0, '2012-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 5 AND (type = 'male' OR type IS NULL)"
                + " AND ID <= 6 ORDER BY ID FETCH NEXT 1 ROWS ONLY", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Set name as the max value column name (and clear the state), all rows should be returned since the max value for name has not been set
        runner.getStateManager().clear(Scope.CLUSTER);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "name");
        runner.setProperty(GenerateTableFetch.COLUMN_NAMES, "id, type, name, scale, created_on");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 5); // 5 records with partition size 1 means 5 generated FlowFiles
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        assertEquals("SELECT id, type, name, scale, created_on FROM TEST_QUERY_DB_TABLE WHERE (type = 'male' OR type IS NULL)"
                + " AND name <= 'Mr. NiFi' ORDER BY name FETCH NEXT 1 ROWS ONLY", new String(flowFile.toByteArray()));
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        assertEquals("SELECT id, type, name, scale, created_on FROM TEST_QUERY_DB_TABLE WHERE (type = 'male' OR type IS NULL)"
                + " AND name <= 'Mr. NiFi' ORDER BY name OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY", new String(flowFile.toByteArray()));
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(2);
        assertEquals("SELECT id, type, name, scale, created_on FROM TEST_QUERY_DB_TABLE WHERE (type = 'male' OR type IS NULL)"
                + " AND name <= 'Mr. NiFi' ORDER BY name OFFSET 2 ROWS FETCH NEXT 1 ROWS ONLY", new String(flowFile.toByteArray()));
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(3);
        assertEquals("SELECT id, type, name, scale, created_on FROM TEST_QUERY_DB_TABLE WHERE (type = 'male' OR type IS NULL)"
                + " AND name <= 'Mr. NiFi' ORDER BY name OFFSET 3 ROWS FETCH NEXT 1 ROWS ONLY", new String(flowFile.toByteArray()));
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(4);
        assertEquals("SELECT id, type, name, scale, created_on FROM TEST_QUERY_DB_TABLE WHERE (type = 'male' OR type IS NULL)"
                + " AND name <= 'Mr. NiFi' ORDER BY name OFFSET 4 ROWS FETCH NEXT 1 ROWS ONLY", new String(flowFile.toByteArray()));

        assertEquals("TEST_QUERY_DB_TABLE", flowFile.getAttribute("generatetablefetch.tableName"));
        assertEquals("id, type, name, scale, created_on", flowFile.getAttribute("generatetablefetch.columnNames"));
        assertEquals("(type = 'male' OR type IS NULL) AND name <= 'Mr. NiFi'", flowFile.getAttribute("generatetablefetch.whereClause"));
        assertEquals("name", flowFile.getAttribute("generatetablefetch.maxColumnNames"));
        assertEquals("1", flowFile.getAttribute("generatetablefetch.limit"));
        assertEquals("4", flowFile.getAttribute("generatetablefetch.offset"));

        runner.clearTransferState();
    }

    @Test
    public void testColumnTypeMissing() throws SQLException {
        // Load test data to database
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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(true);

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "${tableName}");
        runner.setIncomingConnection(true);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "${maxValueCol}");
        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE", "maxValueCol", "id"));
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE id <= 1 ORDER BY id FETCH NEXT 10000 ROWS ONLY", query);
        runner.clearTransferState();


        // Clear columnTypeMap to simulate it's clean after instance reboot
        processor.columnTypeMap.clear();

        // Insert new records
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (2, 0)");

        // Re-launch FlowFile to se if re-cache column type works
        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE", "maxValueCol", "id"));

        // It should re-cache column type
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE id > 1 AND id <= 2 ORDER BY id FETCH NEXT 10000 ROWS ONLY", query);
        runner.clearTransferState();
    }

    @Test
    public void testMultipleColumnTypeMissing() throws SQLException {

        // Load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
            stmt.execute("drop table TEST_QUERY_DB_TABLE_2");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        // Create multiple table to invoke processor state stored
        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, bucket integer not null)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (1, 0)");
        stmt.execute("create table TEST_QUERY_DB_TABLE_2 (id integer not null, bucket integer not null)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE_2 (id, bucket) VALUES (1, 0)");

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "${tableName}");
        runner.setIncomingConnection(true);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "${maxValueCol}");

        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE", "maxValueCol", "id"));

        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE_2", "maxValueCol", "id"));
        runner.run(2);
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        assertEquals(2, processor.columnTypeMap.size());
        runner.clearTransferState();


        // Remove one element from columnTypeMap to simulate it's re-cache partial state
        processor.columnTypeMap.remove("TEST_QUERY_DB_TABLE");

        // Insert new records
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, bucket) VALUES (2, 0)");

        // Re-launch FlowFile to se if re-cache column type works
        runner.enqueue("".getBytes(), Map.of("tableName", "TEST_QUERY_DB_TABLE", "maxValueCol", "id"));

        // It should re-cache column type
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        assertEquals(2, processor.columnTypeMap.size());
        runner.clearTransferState();
    }

    @Test
    public void testUseColumnValuesForPartitioning() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (10, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (11, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (12, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.MAX_VALUE_COLUMN_NAMES, "ID");
        runner.setProperty(GenerateTableFetch.COLUMN_FOR_VALUE_PARTITIONING, "ID");
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);
        // First flow file
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID <= 12 AND ID >= 10 AND ID < 12", query);
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be two records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        // Second flow file
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID <= 12 AND ID >= 12 AND ID < 14", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, this time no flowfiles/rows should be transferred
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.clearTransferState();

        // Add 3 new rows with a higher ID and run with a partition size of 2. Three flow files should be transferred
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (20, 'Mary West', 15.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (21, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (24, 'Marty Johnson', 15.0, '2011-01-01 03:23:34.234')");
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 3);

        // Verify first flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 12 AND ID <= 24 AND ID >= 20 AND ID < 22", query);
        resultSet = stmt.executeQuery(query);
        // Should be two records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        // Verify second flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 12 AND ID <= 24 AND ID >= 22 AND ID < 24", query);
        resultSet = stmt.executeQuery(query);
        // Should be no records
        assertFalse(resultSet.next());

        // Verify third flow file's contents
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(2);
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID > 12 AND ID <= 24 AND ID >= 24 AND ID < 26", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();
    }

    @Test
    public void testUseColumnValuesForPartitioningNoMaxValueColumn() throws SQLException {

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_QUERY_DB_TABLE");
        } catch (final SQLException sqle) {
            // Ignore this error, probably a "table does not exist" since Derby doesn't yet support DROP IF EXISTS [DERBY-4842]
        }

        stmt.execute("create table TEST_QUERY_DB_TABLE (id integer not null, name varchar(100), scale float, created_on timestamp, bignum bigint default 0)");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (10, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (11, 'Carrie Jones', 5.0, '2000-01-01 03:23:34.234')");
        stmt.execute("insert into TEST_QUERY_DB_TABLE (id, name, scale, created_on) VALUES (12, NULL, 2.0, '2010-01-01 00:00:00')");

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.COLUMN_FOR_VALUE_PARTITIONING, "ID");
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);
        // First flow file
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE 1=1 AND ID >= 10 AND ID < 12", query);
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be two records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        // Second flow file
        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE ID <= 12 AND ID >= 12", query);
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        runner.clearTransferState();

        // Run again, the same flowfiles should be transferred as we have no maximum-value column
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);
        runner.clearTransferState();
    }

    @Test
    public void testCustomOrderByColumn() throws SQLException {

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

        runner.setProperty(GenerateTableFetch.TABLE_NAME, "TEST_QUERY_DB_TABLE");
        runner.setIncomingConnection(false);
        runner.setProperty(GenerateTableFetch.CUSTOM_ORDERBY_COLUMN, "SCALE");
        runner.setProperty(GenerateTableFetch.PARTITION_SIZE, "2");

        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).getFirst();
        String query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE 1=1 ORDER BY SCALE FETCH NEXT 2 ROWS ONLY", query);
        flowFile.assertAttributeEquals(FRAGMENT_INDEX, "0");
        flowFile.assertAttributeEquals(FRAGMENT_COUNT, "2");
        ResultSet resultSet = stmt.executeQuery(query);
        // Should be two records
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        flowFile = runner.getFlowFilesForRelationship(REL_SUCCESS).get(1);
        query = new String(flowFile.toByteArray());
        assertEquals("SELECT * FROM TEST_QUERY_DB_TABLE WHERE 1=1 ORDER BY SCALE OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY", query);
        flowFile.assertAttributeEquals(FRAGMENT_INDEX, "1");
        flowFile.assertAttributeEquals(FRAGMENT_COUNT, "2");
        resultSet = stmt.executeQuery(query);
        // Should be one record
        assertTrue(resultSet.next());
        assertFalse(resultSet.next());

        runner.clearTransferState();
    }

    /**
     * Simple implementation only for GenerateTableFetch processor testing.
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
}