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
package org.apache.nifi.database.dialect.service;

import org.apache.nifi.database.dialect.service.api.ColumnDefinition;
import org.apache.nifi.database.dialect.service.api.PageRequest;
import org.apache.nifi.database.dialect.service.api.QueryClause;
import org.apache.nifi.database.dialect.service.api.QueryClauseType;
import org.apache.nifi.database.dialect.service.api.StandardColumnDefinition;
import org.apache.nifi.database.dialect.service.api.StandardPageRequest;
import org.apache.nifi.database.dialect.service.api.StandardQueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StandardStatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementResponse;
import org.apache.nifi.database.dialect.service.api.StatementType;
import org.apache.nifi.database.dialect.service.api.TableDefinition;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StandardDatabaseDialectServiceTest {

    private static final String SERVICE_ID = StandardDatabaseDialectService.class.getSimpleName();

    private static final String CATALOG = "APACHE";

    private static final String SCHEMA_NAME = "NIFI";

    private static final String TABLE_NAME = "RECORD";

    private static final String ID_COLUMN_NAME = "ID";

    private static final String LABEL_COLUMN_NAME = "LABEL";

    private static final String CREATED_COLUMN_NAME = "CREATED";

    private static final String ZERO_EQUALS_ONE = "0 = 1";

    private static final String LABEL_IS_NOT_NULL = "LABEL IS NOT NULL";

    private static final long LIMIT = 100;

    private static final long OFFSET = 25;

    private TestRunner runner;

    private StandardDatabaseDialectService service;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardDatabaseDialectService();
        runner.addControllerService(SERVICE_ID, service);
    }

    @Test
    void testEnableDisable() {
        runner.assertValid(service);
        runner.enableControllerService(service);
        runner.disableControllerService(service);
    }

    @Test
    void testGetSupportedStatementTypes() {
        final Set<StatementType> supportedStatementTypes = service.getSupportedStatementTypes();

        final Set<StatementType> expectedStatementTypes = Set.of(
                StatementType.ALTER,
                StatementType.CREATE,
                StatementType.SELECT
        );

        assertEquals(expectedStatementTypes, supportedStatementTypes);
    }

    @Test
    void testUpsertStatementTypeUnsupported() {
        final TableDefinition tableDefinition = getAlterTableDefinition(ColumnDefinition.Nullable.UNKNOWN);
        final StatementRequest statementRequest = new StandardStatementRequest(StatementType.UPSERT, tableDefinition);

        assertThrows(UnsupportedOperationException.class, () -> service.getStatement(statementRequest));
    }

    @Test
    void testAlterStatementType() {
        final TableDefinition tableDefinition = getAlterTableDefinition(ColumnDefinition.Nullable.UNKNOWN);
        final StatementRequest statementRequest = new StandardStatementRequest(StatementType.ALTER, tableDefinition);

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "ALTER TABLE %s ADD COLUMNS (%s TIMESTAMP NOT NULL, %s VARCHAR)".formatted(TABLE_NAME, CREATED_COLUMN_NAME, LABEL_COLUMN_NAME);
        assertEquals(expected, sql);
    }

    @Test
    void testAlterStatementTypeNotNull() {
        final TableDefinition tableDefinition = getAlterTableDefinition(ColumnDefinition.Nullable.NO);
        final StatementRequest statementRequest = new StandardStatementRequest(StatementType.ALTER, tableDefinition);

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "ALTER TABLE %s ADD COLUMNS (%s TIMESTAMP NOT NULL, %s VARCHAR NOT NULL)".formatted(TABLE_NAME, CREATED_COLUMN_NAME, LABEL_COLUMN_NAME);
        assertEquals(expected, sql);
    }

    @Test
    void testCreateStatementType() {
        final TableDefinition tableDefinition = getCreateTableDefinition(ColumnDefinition.Nullable.UNKNOWN);
        final StatementRequest statementRequest = new StandardStatementRequest(StatementType.CREATE, tableDefinition);

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "CREATE TABLE %s (%s INTEGER NOT NULL PRIMARY KEY, %s VARCHAR)".formatted(TABLE_NAME, ID_COLUMN_NAME, LABEL_COLUMN_NAME);
        assertEquals(expected, sql);
    }

    @Test
    void testCreateStatementTypePrimaryKeyNotNull() {
        final TableDefinition tableDefinition = getCreateTableDefinition(ColumnDefinition.Nullable.NO);
        final StatementRequest statementRequest = new StandardStatementRequest(StatementType.CREATE, tableDefinition);

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "CREATE TABLE %s (%s INTEGER NOT NULL PRIMARY KEY, %s VARCHAR NOT NULL)".formatted(TABLE_NAME, ID_COLUMN_NAME, LABEL_COLUMN_NAME);
        assertEquals(expected, sql);
    }

    @Test
    void testSelectStatementType() {
        final TableDefinition tableDefinition = getSelectTableDefinition();
        final StatementRequest statementRequest = new StandardQueryStatementRequest(StatementType.SELECT, tableDefinition);

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "SELECT %s, %s FROM %s".formatted(ID_COLUMN_NAME, LABEL_COLUMN_NAME, TABLE_NAME);
        assertEquals(expected, sql);
    }

    @Test
    void testSelectStatementTypeColumnsUnspecified() {
        final TableDefinition tableDefinition = new TableDefinition(Optional.empty(), Optional.empty(), TABLE_NAME, List.of());
        final StatementRequest statementRequest = new StandardQueryStatementRequest(StatementType.SELECT, tableDefinition);

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "SELECT * FROM %s".formatted(TABLE_NAME);
        assertEquals(expected, sql);
    }

    @Test
    void testSelectStatementTypeCatalogSchemaName() {
        final TableDefinition selectTableDefinition = getSelectTableDefinition();
        final TableDefinition tableDefinition = new TableDefinition(Optional.of(CATALOG), Optional.of(SCHEMA_NAME), TABLE_NAME, selectTableDefinition.columns());
        final StatementRequest statementRequest = new StandardQueryStatementRequest(StatementType.SELECT, tableDefinition);

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "SELECT %s, %s FROM %s.%s.%s".formatted(ID_COLUMN_NAME, LABEL_COLUMN_NAME, CATALOG, SCHEMA_NAME, TABLE_NAME);
        assertEquals(expected, sql);
    }

    @Test
    void testSelectStatementTypeDerivedTable() {
        final TableDefinition tableDefinition = getSelectTableDefinition();
        final String derivedTable = "SELECT 1 AS ID";
        final StatementRequest statementRequest = new StandardQueryStatementRequest(StatementType.SELECT, tableDefinition, Optional.of(derivedTable), List.of(), Optional.empty());

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "SELECT * FROM (%s) AS %s".formatted(derivedTable, TABLE_NAME);
        assertEquals(expected, sql);
    }

    @Test
    void testSelectStatementTypePageRequestLimitOffset() {
        final TableDefinition tableDefinition = getSelectTableDefinition();

        final PageRequest pageRequest = new StandardPageRequest(OFFSET, OptionalLong.of(LIMIT), Optional.empty());

        final StatementRequest statementRequest = new StandardQueryStatementRequest(StatementType.SELECT, tableDefinition, Optional.empty(), List.of(), Optional.of(pageRequest));

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "SELECT %s, %s FROM %s LIMIT %d OFFSET %d".formatted(ID_COLUMN_NAME, LABEL_COLUMN_NAME, TABLE_NAME, LIMIT, OFFSET);
        assertEquals(expected, sql);
    }

    @Test
    void testSelectStatementTypePageRequestIndexColumnName() {
        final TableDefinition tableDefinition = getSelectTableDefinition();

        final PageRequest pageRequest = new StandardPageRequest(OFFSET, OptionalLong.of(LIMIT), Optional.of(ID_COLUMN_NAME));
        final List<QueryClause> queryClauses = List.of(new QueryClause(QueryClauseType.WHERE, LABEL_IS_NOT_NULL));

        final StatementRequest statementRequest = new StandardQueryStatementRequest(StatementType.SELECT, tableDefinition, Optional.empty(), queryClauses, Optional.of(pageRequest));

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "SELECT %s, %s FROM %s WHERE %s AND %s >= %d AND %s < %d".formatted(
                ID_COLUMN_NAME, LABEL_COLUMN_NAME, TABLE_NAME, LABEL_IS_NOT_NULL, ID_COLUMN_NAME, OFFSET, ID_COLUMN_NAME, LIMIT
        );
        assertEquals(expected, sql);
    }

    @Test
    void testSelectStatementTypeWhereClause() {
        final TableDefinition tableDefinition = getSelectTableDefinition();

        final List<QueryClause> queryClauses = List.of(new QueryClause(QueryClauseType.WHERE, ZERO_EQUALS_ONE));
        final StatementRequest statementRequest = new StandardQueryStatementRequest(StatementType.SELECT, tableDefinition, Optional.empty(), queryClauses, Optional.empty());

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "SELECT %s, %s FROM %s WHERE %s".formatted(ID_COLUMN_NAME, LABEL_COLUMN_NAME, TABLE_NAME, ZERO_EQUALS_ONE);
        assertEquals(expected, sql);
    }

    @Test
    void testSelectStatementTypeWhereClauseOrderByClause() {
        final TableDefinition tableDefinition = getSelectTableDefinition();

        final List<QueryClause> queryClauses = List.of(
                new QueryClause(QueryClauseType.WHERE, ZERO_EQUALS_ONE),
                new QueryClause(QueryClauseType.ORDER_BY, LABEL_COLUMN_NAME)
        );
        final StatementRequest statementRequest = new StandardQueryStatementRequest(StatementType.SELECT, tableDefinition, Optional.empty(), queryClauses, Optional.empty());

        final StatementResponse statementResponse = service.getStatement(statementRequest);

        assertNotNull(statementResponse);

        final String sql = statementResponse.sql();
        final String expected = "SELECT %s, %s FROM %s WHERE %s ORDER BY %s".formatted(ID_COLUMN_NAME, LABEL_COLUMN_NAME, TABLE_NAME, ZERO_EQUALS_ONE, LABEL_COLUMN_NAME);
        assertEquals(expected, sql);
    }

    private TableDefinition getSelectTableDefinition() {
        final List<ColumnDefinition> columnDefinitions = List.of(
                new StandardColumnDefinition(ID_COLUMN_NAME),
                new StandardColumnDefinition(LABEL_COLUMN_NAME)
        );

        return new TableDefinition(Optional.empty(), Optional.empty(), TABLE_NAME, columnDefinitions);
    }

    private TableDefinition getAlterTableDefinition(final ColumnDefinition.Nullable nullable) {
        final List<ColumnDefinition> columnDefinitions = List.of(
                new StandardColumnDefinition(
                        CREATED_COLUMN_NAME,
                        Types.TIMESTAMP,
                        ColumnDefinition.Nullable.NO,
                        Optional.empty(),
                        false
                ),
                new StandardColumnDefinition(
                        LABEL_COLUMN_NAME,
                        Types.VARCHAR,
                        nullable,
                        Optional.empty(),
                        false
                )
        );

        return new TableDefinition(Optional.empty(), Optional.empty(), TABLE_NAME, columnDefinitions);
    }

    private TableDefinition getCreateTableDefinition(final ColumnDefinition.Nullable nullable) {
        final List<ColumnDefinition> columnDefinitions = List.of(
                new StandardColumnDefinition(
                        ID_COLUMN_NAME,
                        Types.INTEGER,
                        ColumnDefinition.Nullable.NO,
                        Optional.empty(),
                        true
                ),
                new StandardColumnDefinition(
                        LABEL_COLUMN_NAME,
                        Types.VARCHAR,
                        nullable,
                        Optional.empty(),
                        false
                )
        );

        return new TableDefinition(Optional.empty(), Optional.empty(), TABLE_NAME, columnDefinitions);
    }
}
