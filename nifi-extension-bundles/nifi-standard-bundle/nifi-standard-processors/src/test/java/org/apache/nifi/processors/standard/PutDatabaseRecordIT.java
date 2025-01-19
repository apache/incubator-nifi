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

import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("resource")
public class PutDatabaseRecordIT {

    private final long MILLIS_TIMESTAMP_LONG = 1707238288351L;
    private final long MICROS_TIMESTAMP_LONG = 1707238288351567L;
    private final String MICROS_TIMESTAMP_FORMATTED = "2024-02-06 11:51:28.351567";
    private final double MICROS_TIMESTAMP_DOUBLE = ((double) MICROS_TIMESTAMP_LONG) / 1000000D;
    private final long NANOS_AFTER_SECOND = 351567000L;
    private final Instant INSTANT_MICROS_PRECISION = Instant.ofEpochMilli(MILLIS_TIMESTAMP_LONG).plusNanos(NANOS_AFTER_SECOND).minusMillis(MILLIS_TIMESTAMP_LONG % 1000);

    private static final String SIMPLE_INPUT_RECORD = """
            {
              "name": "John Doe",
              "age": 50,
              "favorite_color": "blue"
            }
            """;

    private static final String FAVORITE_COLOR_FIELD = "favorite_color";
    private static final String FAVORITE_COLOR = "blue";

    private static PostgreSQLContainer<?> postgres;
    private TestRunner runner;


    @BeforeAll
    public static void startPostgres() {
        postgres = new PostgreSQLContainer<>("postgres:9.6.12")
            .withInitScript("PutDatabaseRecordIT/create-person-table.sql");
        postgres.start();
    }

    @AfterAll
    public static void cleanup() {
        if (postgres != null) {
            postgres.close();
            postgres = null;
        }
    }

    @BeforeEach
    public void setup() throws InitializationException, SQLException {
        truncateTable();

        runner = TestRunners.newTestRunner(PutDatabaseRecord.class);
        final DBCPConnectionPool connectionPool = new DBCPConnectionPool();
        runner.addControllerService("connectionPool", connectionPool);
        runner.setProperty(connectionPool, DBCPProperties.DATABASE_URL, postgres.getJdbcUrl());
        runner.setProperty(connectionPool, DBCPProperties.DB_USER, postgres.getUsername());
        runner.setProperty(connectionPool, DBCPProperties.DB_PASSWORD, postgres.getPassword());
        runner.setProperty(connectionPool, DBCPProperties.DB_DRIVERNAME, postgres.getDriverClassName());
        runner.enableControllerService(connectionPool);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("json-reader", jsonReader);
        runner.setProperty(jsonReader, DateTimeUtils.DATE_FORMAT, "yyyy-MM-dd");
        runner.setProperty(jsonReader, DateTimeUtils.TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss.SSSSSS");
        runner.enableControllerService(jsonReader);

        runner.setProperty(PutDatabaseRecord.RECORD_READER_FACTORY, "json-reader");
        runner.setProperty(PutDatabaseRecord.DBCP_SERVICE, "connectionPool");

        runner.setProperty(PutDatabaseRecord.TABLE_NAME, "person");
        runner.setProperty(PutDatabaseRecord.DB_TYPE, "PostgreSQL");
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, "INSERT");
    }


    @Test
    public void testSimplePut() throws SQLException {
        runner.enqueue(SIMPLE_INPUT_RECORD);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals(FAVORITE_COLOR, results.get(FAVORITE_COLOR_FIELD));
    }

    @Test
    public void testUpsert() throws SQLException {
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, "UPSERT");

        runner.enqueue(SIMPLE_INPUT_RECORD);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals(FAVORITE_COLOR, results.get(FAVORITE_COLOR_FIELD));
    }

    @Test
    public void testInsertIgnore() throws SQLException {
        runner.setProperty(PutDatabaseRecord.STATEMENT_TYPE, "INSERT_IGNORE");

        runner.enqueue(SIMPLE_INPUT_RECORD);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals(FAVORITE_COLOR, results.get(FAVORITE_COLOR_FIELD));
    }

    @Test
    public void testWithDate() throws SQLException {
        runner.enqueue("""
            {
              "name": "John Doe",
              "age": 50,
              "dob": "1975-01-01"
            }
            """);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        final Date dob = (Date) results.get("dob");
        assertEquals(1975, dob.toLocalDate().getYear());
        assertEquals(Month.JANUARY, dob.toLocalDate().getMonth());
        assertEquals(1, dob.toLocalDate().getDayOfMonth());
    }

    @Test
    public void testWithTimestampUsingMillis() throws SQLException {
        runner.enqueue(createJson(MILLIS_TIMESTAMP_LONG));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals(new Timestamp(MILLIS_TIMESTAMP_LONG), results.get("lasttransactiontime"));
    }

    @Test
    public void testWithTimestampUsingMillisAsString() throws SQLException {
        runner.enqueue(createJson(MILLIS_TIMESTAMP_LONG));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        assertEquals(new Timestamp(MILLIS_TIMESTAMP_LONG), results.get("lasttransactiontime"));
    }

    @Test
    public void testWithStringTimestampUsingMicros() throws SQLException {
        runner.enqueue(createJson(MICROS_TIMESTAMP_FORMATTED));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        final LocalDateTime transactionLocalTime = lastTransactionTime.toLocalDateTime();
        assertEquals(2024, transactionLocalTime.getYear());
        assertEquals(Month.FEBRUARY, transactionLocalTime.getMonth());
        assertEquals(6, transactionLocalTime.getDayOfMonth());
        assertEquals(11, transactionLocalTime.getHour());
        assertEquals(51, transactionLocalTime.getMinute());
        assertEquals(28, transactionLocalTime.getSecond());
        assertEquals(351567000, transactionLocalTime.getNano());
    }

    @Test
    public void testWithNumericTimestampUsingMicros() throws SQLException {
        runner.enqueue(createJson(MICROS_TIMESTAMP_LONG));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        assertEquals(INSTANT_MICROS_PRECISION, lastTransactionTime.toInstant());
    }


    @Test
    public void testWithDecimalTimestampUsingMicros() throws SQLException {
        runner.enqueue(createJson(Double.toString(MICROS_TIMESTAMP_DOUBLE)));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        assertEquals(INSTANT_MICROS_PRECISION, lastTransactionTime.toInstant());
    }

    @Test
    public void testWithDecimalTimestampUsingMicrosAsString() throws SQLException {
        runner.enqueue(createJson(Double.toString(MICROS_TIMESTAMP_DOUBLE)));
        runner.run();
        runner.assertAllFlowFilesTransferred(PutDatabaseRecord.REL_SUCCESS, 1);

        final Map<String, Object> results = getResults();
        final Timestamp lastTransactionTime = (Timestamp) results.get("lasttransactiontime");
        assertEquals(INSTANT_MICROS_PRECISION, lastTransactionTime.toInstant());
    }


    private static void truncateTable() throws SQLException {
        try (final Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            final String sqlQuery = "TRUNCATE TABLE person";
            try (final PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                preparedStatement.execute();
            }
        }
    }

    private Map<String, Object> getResults() throws SQLException {
        try (final Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            final String sqlQuery = "SELECT * FROM person";
            final Map<String, Object> resultsMap = new HashMap<>();

            try (final PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
                 final ResultSet resultSet = preparedStatement.executeQuery()) {

                final ResultSetMetaData metaData = resultSet.getMetaData();
                final int columnCount = metaData.getColumnCount();

                while (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        final String columnName = metaData.getColumnName(i);
                        final Object columnValue = resultSet.getObject(i);
                        resultsMap.put(columnName, columnValue);
                    }
                }
            }

            assertEquals("John Doe", resultsMap.get("name"));
            assertEquals(50, resultsMap.get("age"));

            return resultsMap;
        }
    }

    private String createJson(final long lastTransactionTime) {
        return createJson(Long.toString(lastTransactionTime));
    }

    private String createJson(final String lastTransactionTime) {
        return """
            {
              "name": "John Doe",
              "age": 50,
              "lastTransactionTime": "%s"
            }""".formatted(lastTransactionTime);
    }
}
