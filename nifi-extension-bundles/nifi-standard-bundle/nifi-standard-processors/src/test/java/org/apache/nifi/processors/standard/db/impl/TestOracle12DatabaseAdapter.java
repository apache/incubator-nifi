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
package org.apache.nifi.processors.standard.db.impl;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.processors.standard.db.ColumnDescription;
import org.apache.nifi.processors.standard.db.NameNormalizer;
import org.apache.nifi.processors.standard.db.NameNormalizerFactory;
import org.apache.nifi.processors.standard.db.TableSchema;
import org.apache.nifi.processors.standard.db.TranslationStrategy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOracle12DatabaseAdapter {

    private final Oracle12DatabaseAdapter db = new Oracle12DatabaseAdapter();

    @Test
    public void testGeneration() {
        String sql1 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "", "", null, null);
        String expected1 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename";
        assertEquals(sql1, expected1);

        String sql2 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "that=\'some\"\' value\'", "", null, null);
        String expected2 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE that=\'some\"\' value\'";
        assertEquals(sql2, expected2);

        String sql3 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "that=\'some\"\' value\'", "might DESC", null, null);
        String expected3 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE that=\'some\"\' value\' ORDER BY might DESC";
        assertEquals(sql3, expected3);

        String sql4 = db.getSelectStatement("database.tablename", "", "that=\'some\"\' value\'", "might DESC", null, null);
        String expected4 = "SELECT * FROM database.tablename WHERE that=\'some\"\' value\' ORDER BY might DESC";
        assertEquals(sql4, expected4);
    }

    @Test
    public void testNoTableName() {
        assertThrows(IllegalArgumentException.class, () -> {
            db.getSelectStatement("", "some(set),of(columns),that,might,contain,methods,a.*", "", "", null, null);
        });
    }

    @Test
    public void testPagingQuery() {
        String sql1 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "", "contain", 100L, 0L);
        String expected1 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename ORDER BY contain FETCH NEXT 100 ROWS ONLY";
        assertEquals(sql1, expected1);

        String sql2 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "", "contain", 10000L, 123456L);
        String expected2 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename ORDER BY contain OFFSET 123456 ROWS FETCH NEXT 10000 ROWS ONLY";
        assertEquals(sql2, expected2);

        String sql3 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "methods='strange'", "contain", 10000L, 123456L);
        String expected3 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE methods='strange' ORDER BY contain OFFSET 123456 ROWS FETCH NEXT 10000 ROWS ONLY";
        assertEquals(sql3, expected3);

        String sql4 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "", "", 100L, null);
        String expected4 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename FETCH NEXT 100 ROWS ONLY";
        assertEquals(sql4, expected4);
    }

    @Test
    public void testPagingQueryUsingColumnValuesForPartitioning() {
        String sql1 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "1=1", "contain", 100L, 0L, "contain");
        String expected1 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE 1=1 AND contain >= 0 AND contain < 100";
        assertEquals(expected1, sql1);

        String sql2 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "1=1", "contain", 10000L, 123456L, "contain");
        String expected2 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE 1=1 AND contain >= 123456 AND contain < 133456";
        assertEquals(expected2, sql2);

        String sql3 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "methods='strange'", "contain", 10000L, 123456L, "contain");
        String expected3 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE methods='strange' AND contain >= 123456 AND contain < 133456";
        assertEquals(expected3, sql3);

        // Paging (limit/offset) is only supported when an orderByClause is supplied, note that it is not honored here
        String sql4 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "", "", 100L, null, "contain");
        String expected4 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename";
        assertEquals(expected4, sql4);
    }

    @Test
    public void testSupportsUpsert() {
        assertTrue(db.supportsUpsert(), db.getClass().getSimpleName() + " should support upsert");
    }

    @Test
    public void testGetUpsertStatementWithNullTableName() {
        testGetUpsertStatement(null, Arrays.asList("notEmpty"), Arrays.asList("notEmpty"), new IllegalArgumentException("Table name cannot be null or blank"));
    }

    @Test
    public void testGetUpsertStatementWithBlankTableName() {
        testGetUpsertStatement("", Arrays.asList("notEmpty"), Arrays.asList("notEmpty"), new IllegalArgumentException("Table name cannot be null or blank"));
    }

    @Test
    public void testGetUpsertStatementWithNullColumnNames() {
        testGetUpsertStatement("notEmpty", null, Arrays.asList("notEmpty"), new IllegalArgumentException("Column names cannot be null or empty"));
    }

    @Test
    public void testGetUpsertStatementWithEmptyColumnNames() {
        testGetUpsertStatement("notEmpty", Collections.emptyList(), Arrays.asList("notEmpty"), new IllegalArgumentException("Column names cannot be null or empty"));
    }

    @Test
    public void testGetUpsertStatementWithNullKeyColumnNames() {
        testGetUpsertStatement("notEmpty", Arrays.asList("notEmpty"), null, new IllegalArgumentException("Key column names cannot be null or empty"));
    }

    @Test
    public void testGetUpsertStatementWithEmptyKeyColumnNames() {
        testGetUpsertStatement("notEmpty", Arrays.asList("notEmpty"), Collections.emptyList(), new IllegalArgumentException("Key column names cannot be null or empty"));
    }

    @Test
    public void testGetUpsertStatement() {
        // GIVEN
        String tableName = "table";
        List<String> columnNames = Arrays.asList("column1", "column2", "column3", "column_4");
        // uniqueKeyColumnNames can be normalized, so "column_4" become "column4" here.
        Collection<String> uniqueKeyColumnNames = Arrays.asList("column1", "column4");

        String expected = "MERGE INTO table USING (SELECT ? column1, ? column2, ? column3, ? column_4 FROM DUAL) n" +
                " ON (table.column1 = n.column1 AND table.column_4 = n.column_4) WHEN NOT MATCHED THEN" +
                " INSERT (column1, column2, column3, column_4) VALUES (n.column1, n.column2, n.column3, n.column_4)" +
                " WHEN MATCHED THEN UPDATE SET table.column2 = n.column2, table.column3 = n.column3";

        // WHEN
        // THEN
        testGetUpsertStatement(tableName, columnNames, uniqueKeyColumnNames, expected);
    }

    @Test
    public void testGetCreateTableStatement() {
        assertTrue(db.supportsCreateTableIfNotExists());
        final List<ColumnDescription> columns = Arrays.asList(
                new ColumnDescription("col1", Types.INTEGER, true, 4, false),
                new ColumnDescription("col2", Types.VARCHAR, false, 2000, true)
        );
        NameNormalizer normalizer = NameNormalizerFactory.getNormalizer(TranslationStrategy.REMOVE_UNDERSCORE, null);
        TableSchema tableSchema = new TableSchema("USERS", null, "TEST_TABLE", columns,
                true, normalizer, Collections.singleton("COL1"), db.getColumnQuoteString());

        String expectedStatement = "DECLARE\n\tsql_stmt long;\nBEGIN\n\tsql_stmt:='CREATE TABLE "
                // Strings are returned as VARCHAR2(2000) regardless of reported size and that VARCHAR2 is not in java.sql.Types
                + "\"USERS\".\"TEST_TABLE\" (\"col1\" INTEGER NOT NULL, \"col2\" VARCHAR2(2000))';"
                + "\nEXECUTE IMMEDIATE sql_stmt;\nEXCEPTION\n\tWHEN OTHERS THEN\n\t\tIF SQLCODE = -955 THEN\n\t\t\t"
                + "NULL;\n\t\tELSE\n\t\t\tRAISE;\n\t\tEND IF;\nEND;";
        String actualStatement = db.getCreateTableStatement(tableSchema, true, true);
        assertEquals(expectedStatement, actualStatement);
    }


    private void testGetUpsertStatement(String tableName, List<String> columnNames, Collection<String> uniqueKeyColumnNames, IllegalArgumentException expected) {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
            testGetUpsertStatement(tableName, columnNames, uniqueKeyColumnNames, (String) null);
        });
        assertEquals(expected.getMessage(), e.getMessage());
    }

    private void testGetUpsertStatement(String tableName, List<String> columnNames, Collection<String> uniqueKeyColumnNames, String expected) {
        // WHEN
        String actual = db.getUpsertStatement(tableName, columnNames, uniqueKeyColumnNames);

        // THEN
        assertEquals(expected, actual);
    }

}
