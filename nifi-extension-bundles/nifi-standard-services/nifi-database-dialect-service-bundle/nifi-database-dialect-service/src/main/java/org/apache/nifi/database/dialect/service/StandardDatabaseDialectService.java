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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.database.dialect.service.api.ColumnDefinition;
import org.apache.nifi.database.dialect.service.api.DatabaseDialectService;
import org.apache.nifi.database.dialect.service.api.PageRequest;
import org.apache.nifi.database.dialect.service.api.QueryClause;
import org.apache.nifi.database.dialect.service.api.QueryClauseType;
import org.apache.nifi.database.dialect.service.api.QueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StandardStatementResponse;
import org.apache.nifi.database.dialect.service.api.StatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementResponse;
import org.apache.nifi.database.dialect.service.api.StatementType;
import org.apache.nifi.database.dialect.service.api.TableDefinition;

import java.sql.JDBCType;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

@CapabilityDescription("""
    Database Dialect Service supporting ANSI SQL.
    Supported Statement Types: ALTER, CREATE, SELECT
"""
)
@Tags({ "Relational", "Database", "JDBC", "SQL" })
public class StandardDatabaseDialectService extends AbstractControllerService implements DatabaseDialectService {
    private static final char PERIOD_SEPARATOR = '.';

    private static final char SPACE_SEPARATOR = ' ';

    private static final char COMMA_SEPARATOR = ',';

    private static final char ASTERISK_CHARACTER = '*';

    private static final String NOT_NULL_QUALIFIER = "NOT NULL";

    private static final String PRIMARY_KEY_QUALIFIER = "PRIMARY KEY";

    private static final String WHERE_KEYWORD = "WHERE";

    private static final String ORDER_BY_KEYWORD = "ORDER BY";

    private static final String AND_KEYWORD = "AND";

    private static final String LIMIT_KEYWORD = "LIMIT";

    private static final String OFFSET_KEYWORD = "OFFSET";

    private static final String GREATER_THAN_OR_EQUAL = ">=";

    private static final String LESS_THAN = "<";

    private static final Set<StatementType> supportedStatementTypes = Set.of(
            StatementType.ALTER,
            StatementType.CREATE,
            StatementType.SELECT
    );

    @Override
    public StatementResponse getStatement(final StatementRequest statementRequest) {
        Objects.requireNonNull(statementRequest, "Statement Request required");
        return getSupportedStatement(statementRequest);
    }

    @Override
    public Set<StatementType> getSupportedStatementTypes() {
        return supportedStatementTypes;
    }

    private StatementResponse getSupportedStatement(final StatementRequest statementRequest) {
        final StatementType statementType = statementRequest.statementType();
        return switch (statementType) {
            case ALTER -> getAlterStatement(statementRequest);
            case CREATE -> getCreateStatement(statementRequest);
            case SELECT -> getSelectStatement(statementRequest);
            default -> throw new UnsupportedOperationException("Statement Type [%s] not handled".formatted(statementType));
        };
    }

    private StatementResponse getAlterStatement(final StatementRequest statementRequest) {
        final TableDefinition tableDefinition = statementRequest.tableDefinition();
        final String qualifiedTableName = getQualifiedTableName(tableDefinition);
        final String tableColumns = getAlterTableColumns(tableDefinition.columns());

        final String sql = "ALTER TABLE %s ADD COLUMNS (%s)".formatted(qualifiedTableName, tableColumns);
        return new StandardStatementResponse(sql);
    }

    private String getAlterTableColumns(final List<ColumnDefinition> columnDefinitions) {
        final StringBuilder tableColumns = new StringBuilder();

        final Iterator<ColumnDefinition> columns = columnDefinitions.iterator();
        while (columns.hasNext()) {
            final ColumnDefinition columnDefinition = columns.next();
            final String columnName = columnDefinition.columnName();
            final String jdbcTypeName = getJdbcTypeName(columnDefinition);
            tableColumns.append(columnName);
            tableColumns.append(SPACE_SEPARATOR);
            tableColumns.append(jdbcTypeName);

            if (ColumnDefinition.Nullable.NO == columnDefinition.nullable()) {
                tableColumns.append(SPACE_SEPARATOR);
                tableColumns.append(NOT_NULL_QUALIFIER);
            }

            if (columns.hasNext()) {
                tableColumns.append(COMMA_SEPARATOR);
                tableColumns.append(SPACE_SEPARATOR);
            }
        }

        return tableColumns.toString();
    }

    private StatementResponse getCreateStatement(final StatementRequest statementRequest) {
        final TableDefinition tableDefinition = statementRequest.tableDefinition();
        final String qualifiedTableName = getQualifiedTableName(tableDefinition);
        final String tableColumns = getCreateTableColumns(tableDefinition.columns());

        final String sql = "CREATE TABLE %s (%s)".formatted(qualifiedTableName, tableColumns);
        return new StandardStatementResponse(sql);
    }

    private String getCreateTableColumns(final List<ColumnDefinition> columnDefinitions) {
        final StringBuilder tableColumns = new StringBuilder();

        final Iterator<ColumnDefinition> columns = columnDefinitions.iterator();
        while (columns.hasNext()) {
            final ColumnDefinition columnDefinition = columns.next();
            final String columnName = columnDefinition.columnName();
            final String jdbcTypeName = getJdbcTypeName(columnDefinition);
            tableColumns.append(columnName);
            tableColumns.append(SPACE_SEPARATOR);
            tableColumns.append(jdbcTypeName);

            if (ColumnDefinition.Nullable.NO == columnDefinition.nullable()) {
                tableColumns.append(SPACE_SEPARATOR);
                tableColumns.append(NOT_NULL_QUALIFIER);
            }

            if (columnDefinition.primaryKey()) {
                tableColumns.append(SPACE_SEPARATOR);
                tableColumns.append(PRIMARY_KEY_QUALIFIER);
            }

            if (columns.hasNext()) {
                tableColumns.append(COMMA_SEPARATOR);
                tableColumns.append(SPACE_SEPARATOR);
            }
        }

        return tableColumns.toString();
    }

    private String getSelectTableColumns(final List<ColumnDefinition> columnDefinitions) {
        final StringBuilder tableColumns = new StringBuilder();

        final Iterator<ColumnDefinition> columns = columnDefinitions.iterator();
        if (columns.hasNext()) {
            while (columns.hasNext()) {
                final ColumnDefinition columnDefinition = columns.next();
                final String columnName = columnDefinition.columnName();
                tableColumns.append(columnName);

                if (columns.hasNext()) {
                    tableColumns.append(COMMA_SEPARATOR);
                    tableColumns.append(SPACE_SEPARATOR);
                }
            }
        } else {
            tableColumns.append(ASTERISK_CHARACTER);
        }

        return tableColumns.toString();
    }

    private StatementResponse getSelectStatement(final StatementRequest statementRequest) {
        if (statementRequest instanceof QueryStatementRequest queryStatementRequest) {
            final TableDefinition tableDefinition = queryStatementRequest.tableDefinition();
            final String qualifiedTableName = getQualifiedTableName(tableDefinition);
            final Optional<String> derivedTableFound = queryStatementRequest.derivedTable();

            final String selectSql;
            if (derivedTableFound.isPresent()) {
                final String derivedTable = derivedTableFound.get();
                selectSql = "SELECT * FROM (%s) AS %s".formatted(derivedTable, qualifiedTableName);
            } else {
                final String tableColumns = getSelectTableColumns(tableDefinition.columns());
                selectSql = "SELECT %s FROM %s".formatted(tableColumns, qualifiedTableName);
            }

            final StringBuilder sqlBuilder = new StringBuilder(selectSql);

            final Optional<PageRequest> pageRequestFound = queryStatementRequest.pageRequest();

            final Optional<QueryClause> whereQueryClause = queryStatementRequest.queryClauses().stream()
                    .filter(queryClause -> QueryClauseType.WHERE == queryClause.queryClauseType())
                    .findFirst();
            if (whereQueryClause.isPresent()) {
                sqlBuilder.append(SPACE_SEPARATOR);
                sqlBuilder.append(WHERE_KEYWORD);
                sqlBuilder.append(SPACE_SEPARATOR);
                sqlBuilder.append(whereQueryClause.get().criteria());

                // Add paging with index column specified
                if (pageRequestFound.isPresent()) {
                    final PageRequest pageRequest = pageRequestFound.get();
                    appendIndexedPageRequest(pageRequest, sqlBuilder);
                }
            }

            final Optional<QueryClause> orderByQueryClause = queryStatementRequest.queryClauses().stream()
                    .filter(queryClause -> QueryClauseType.ORDER_BY == queryClause.queryClauseType())
                    .findFirst();
            if (orderByQueryClause.isPresent()) {
                sqlBuilder.append(SPACE_SEPARATOR);
                sqlBuilder.append(ORDER_BY_KEYWORD);
                sqlBuilder.append(SPACE_SEPARATOR);
                sqlBuilder.append(orderByQueryClause.get().criteria());
            }

            // Add paging without index column specified
            if (pageRequestFound.isPresent()) {
                final PageRequest pageRequest = pageRequestFound.get();
                appendPageRequest(pageRequest, sqlBuilder);
            }

            return new StandardStatementResponse(sqlBuilder.toString());
        } else {
            throw new IllegalArgumentException("Query Statement Request not found [%s]".formatted(statementRequest.getClass()));
        }
    }

    private void appendPageRequest(final PageRequest pageRequest, final StringBuilder sqlBuilder) {
        final Optional<String> indexColumnNameFound = pageRequest.indexColumnName();
        if (indexColumnNameFound.isEmpty()) {
            final OptionalLong limitFound = pageRequest.limit();
            if (limitFound.isPresent()) {
                sqlBuilder.append(SPACE_SEPARATOR);
                sqlBuilder.append(LIMIT_KEYWORD);
                sqlBuilder.append(SPACE_SEPARATOR);
                sqlBuilder.append(limitFound.getAsLong());
            }
            sqlBuilder.append(SPACE_SEPARATOR);
            sqlBuilder.append(OFFSET_KEYWORD);
            sqlBuilder.append(SPACE_SEPARATOR);
            sqlBuilder.append(pageRequest.offset());
        }
    }

    private void appendIndexedPageRequest(final PageRequest pageRequest, final StringBuilder sqlBuilder) {
        final Optional<String> indexColumnNameFound = pageRequest.indexColumnName();
        if (indexColumnNameFound.isPresent()) {
            sqlBuilder.append(SPACE_SEPARATOR);
            sqlBuilder.append(AND_KEYWORD);
            sqlBuilder.append(SPACE_SEPARATOR);

            final String indexColumnName = indexColumnNameFound.get();
            sqlBuilder.append(indexColumnName);
            sqlBuilder.append(SPACE_SEPARATOR);
            sqlBuilder.append(GREATER_THAN_OR_EQUAL);
            sqlBuilder.append(SPACE_SEPARATOR);
            sqlBuilder.append(pageRequest.offset());

            final OptionalLong limitFound = pageRequest.limit();
            if (limitFound.isPresent()) {
                sqlBuilder.append(SPACE_SEPARATOR);
                sqlBuilder.append(AND_KEYWORD);
                sqlBuilder.append(SPACE_SEPARATOR);

                sqlBuilder.append(indexColumnName);
                sqlBuilder.append(SPACE_SEPARATOR);
                sqlBuilder.append(LESS_THAN);
                sqlBuilder.append(SPACE_SEPARATOR);
                sqlBuilder.append(limitFound.getAsLong());
            }
        }
    }

    private String getQualifiedTableName(final TableDefinition tableDefinition) {
        final StringBuilder builder = new StringBuilder();

        final Optional<String> catalog = tableDefinition.catalog();
        if (catalog.isPresent()) {
            builder.append(catalog.get());
            builder.append(PERIOD_SEPARATOR);
        }

        final Optional<String> schemaName = tableDefinition.schemaName();
        if (schemaName.isPresent()) {
            builder.append(schemaName.get());
            builder.append(PERIOD_SEPARATOR);
        }

        builder.append(tableDefinition.tableName());
        return builder.toString();
    }

    private String getJdbcTypeName(final ColumnDefinition columnDefinition) {
        final int dataType = columnDefinition.dataType();
        final JDBCType jdbcType = JDBCType.valueOf(dataType);
        return jdbcType.getName();
    }
}
