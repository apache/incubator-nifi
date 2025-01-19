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
package org.apache.nifi.processors.standard.db;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Interface for RDBMS/JDBC-specific code.
 */
public interface DatabaseAdapter {

    String getName();

    String getDescription();

    /**
     * Returns a SQL SELECT statement with the given clauses applied.
     *
     * @param tableName     The name of the table to fetch rows from
     * @param columnNames   The names of the columns to fetch from the table
     * @param whereClause   The filter to apply to the statement. This should not include the WHERE keyword
     * @param orderByClause The columns/clause used for ordering the result rows. This should not include the ORDER BY keywords
     * @param limit         The value for the LIMIT clause (i.e. the number of rows to return)
     * @param offset        The value for the OFFSET clause (i.e. the number of rows to skip)
     * @return A String containing a SQL SELECT statement with the given clauses applied
     */
    String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset);

    /**
     * Returns a SQL SELECT statement with the given clauses applied. Note that if this method is overridden, the other overloaded methods
     * need to be overridden as well, to call this method with columnForPartitioning = false
     *
     * @param tableName             The name of the table to fetch rows from
     * @param columnNames           The names of the columns to fetch from the table
     * @param whereClause           The filter to apply to the statement. This should not include the WHERE keyword
     * @param orderByClause         The columns/clause used for ordering the result rows. This should not include the ORDER BY keywords
     * @param limit                 The value for the LIMIT clause (i.e. the number of rows to return)
     * @param offset                The value for the OFFSET clause (i.e. the number of rows to skip)
     * @param columnForPartitioning The (optional) column name that, if provided, the limit and offset values are based on values from the column itself (rather than the row number)
     * @return A String containing a SQL SELECT statement with the given clauses applied
     */
    default String getSelectStatement(String tableName, String columnNames, String whereClause, String orderByClause, Long limit, Long offset, String columnForPartitioning) {
        return getSelectStatement(tableName, columnNames, whereClause, orderByClause, limit, offset);
    }

    /**
     * Tells whether this adapter supports UPSERT.
     *
     * @return true if UPSERT is supported, false otherwise
     */
    default boolean supportsUpsert() {
        return false;
    }

    /**
     * Tells whether this adapter supports INSERT_IGNORE.
     *
     * @return true if INSERT_IGNORE is supported, false otherwise
     */
    default boolean supportsInsertIgnore() {
        return false;
    }

    /**
     * Returns an SQL UPSERT statement - i.e. UPDATE record or INSERT if id doesn't exist.
     * <br /><br />
     * There is no standard way of doing this so not all adapters support it - use together with {@link #supportsUpsert()}!
     *
     * @param table                The name of the table in which to update/insert a record into.
     * @param columnNames          The name of the columns in the table to add values to.
     * @param uniqueKeyColumnNames The name of the columns that form a unique key.
     * @return A String containing the parameterized jdbc SQL statement.
     * The order and number of parameters are the same as that of the provided column list.
     */
    default String getUpsertStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        throw new UnsupportedOperationException("UPSERT is not supported for " + getName());
    }

    /**
     * Returns an SQL INSERT_IGNORE statement - i.e. Ignore record or INSERT if id doesn't exist.
     * <br /><br />
     * There is no standard way of doing this so not all adapters support it - use together with {@link #supportsInsertIgnore()}!
     *
     * @param table                The name of the table in which to ignore/insert a record into.
     * @param columnNames          The name of the columns in the table to add values to.
     * @param uniqueKeyColumnNames The name of the columns that form a unique key.
     * @return A String containing the parameterized jdbc SQL statement.
     * The order and number of parameters are the same as that of the provided column list.
     */
    default String getInsertIgnoreStatement(String table, List<String> columnNames, Collection<String> uniqueKeyColumnNames) {
        throw new UnsupportedOperationException("UPSERT is not supported for " + getName());
    }

    default String getTableAliasClause(String tableName) {
        return "AS " + tableName;
    }

    /**
     * Table Quote String usage limited to statement generation methods within DatabaseAdapter
     *
     * @return Table Quote String
     */
    default String getTableQuoteString() {
        // ANSI standard is a double quote
        return "\"";
    }

    default String getColumnQuoteString() {
        // ANSI standard is a double quote
        return "\"";
    }

    default boolean supportsCreateTableIfNotExists() {
        return false;
    }

    /**
     * Generates a CREATE TABLE statement using the specified table schema
     * @param tableSchema The table schema including column information
     * @param quoteTableName Whether to quote the table name in the generated DDL
     * @param quoteColumnNames Whether to quote column names in the generated DDL
     * @return A String containing DDL to create the specified table
     */
    default String getCreateTableStatement(TableSchema tableSchema, boolean quoteTableName, boolean quoteColumnNames) {
        StringBuilder createTableStatement = new StringBuilder();

        List<ColumnDescription> columns = tableSchema.getColumnsAsList();
        List<String> columnsAndDatatypes = new ArrayList<>(columns.size());
        Set<String> primaryKeyColumnNames = tableSchema.getPrimaryKeyColumnNames();
        for (ColumnDescription column : columns) {
            StringBuilder sb = new StringBuilder()
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(column.getColumnName())
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(" ")
                    .append(getSQLForDataType(column.getDataType()))
                    .append(column.isNullable() ? "" : " NOT NULL")
                    .append(primaryKeyColumnNames != null && primaryKeyColumnNames.contains(column.getColumnName()) ? " PRIMARY KEY" : "");
            columnsAndDatatypes.add(sb.toString());
        }

        createTableStatement.append("CREATE TABLE IF NOT EXISTS ")
                .append(generateTableName(quoteTableName, tableSchema.getCatalogName(), tableSchema.getSchemaName(), tableSchema.getTableName(), tableSchema))
                .append(" (")
                .append(String.join(", ", columnsAndDatatypes))
                .append(") ");

        return createTableStatement.toString();
    }

    default List<String> getAlterTableStatements(String tableName, List<ColumnDescription> columnsToAdd, final boolean quoteTableName, final boolean quoteColumnNames) {
        StringBuilder createTableStatement = new StringBuilder();

        List<String> columnsAndDatatypes = new ArrayList<>(columnsToAdd.size());
        for (ColumnDescription column : columnsToAdd) {
            StringBuilder sb = new StringBuilder()
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(column.getColumnName())
                    .append(quoteColumnNames ? getColumnQuoteString() : "")
                    .append(" ")
                    .append(getSQLForDataType(column.getDataType()));
            columnsAndDatatypes.add(sb.toString());
        }

        createTableStatement.append("ALTER TABLE ")
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(tableName)
                .append(quoteTableName ? getTableQuoteString() : "")
                .append(" ADD COLUMNS (")
                .append(String.join(", ", columnsAndDatatypes))
                .append(") ");

        return List.of(createTableStatement.toString());
    }

    /**
     * Get the auto commit mode to use for reading from this database type.
     * Most databases do not care which auto commit mode is used to read.
     * For PostgreSQL it can make a difference.
     * @param fetchSize The number of rows to retrieve at a time. Value of 0 means retrieve all rows at once.
     * @return Optional.empty() if auto commit mode does not matter and can be left as is.
     *         Return true or false to indicate whether auto commit needs to be true or false for this database.
     */
    default Optional<Boolean> getAutoCommitForReads(Integer fetchSize) {
        return Optional.empty();
    }

    default String getSQLForDataType(int sqlType) {
        return JDBCType.valueOf(sqlType).getName();
    }

    default String generateTableName(final boolean quoteTableName, final String catalog, final String schemaName, final String tableName, final TableSchema tableSchema) {
        final StringBuilder tableNameBuilder = new StringBuilder();
        if (catalog != null) {
            if (quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(catalog)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(catalog);
            }

            tableNameBuilder.append(".");
        }

        if (schemaName != null) {
            if (quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(schemaName)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(schemaName);
            }

            tableNameBuilder.append(".");
        }

        if (quoteTableName) {
            tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(tableSchema.getQuotedIdentifierString());
        } else {
            tableNameBuilder.append(tableName);
        }

        return tableNameBuilder.toString();
    }
}
