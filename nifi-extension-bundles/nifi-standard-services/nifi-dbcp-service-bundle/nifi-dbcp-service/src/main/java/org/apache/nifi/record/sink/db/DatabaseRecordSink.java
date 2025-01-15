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
package org.apache.nifi.record.sink.db;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Tags({ "db", "jdbc", "database", "connection", "record"  })
@CapabilityDescription("Provides a service to write records using a configured database connection.")
public class DatabaseRecordSink extends AbstractControllerService implements RecordSinkService {

    static final AllowableValue IGNORE_UNMATCHED_FIELD = new AllowableValue("Ignore Unmatched Fields", "Ignore Unmatched Fields",
            "Any field in the document that cannot be mapped to a column in the database is ignored");
    static final AllowableValue FAIL_UNMATCHED_FIELD = new AllowableValue("Fail on Unmatched Fields", "Fail on Unmatched Fields",
            "If the document has any field that cannot be mapped to a column in the database, the FlowFile will be routed to the failure relationship");
    static final AllowableValue IGNORE_UNMATCHED_COLUMN = new AllowableValue("Ignore Unmatched Columns",
            "Ignore Unmatched Columns",
            "Any column in the database that does not have a field in the document will be assumed to not be required.  No notification will be logged");
    static final AllowableValue WARNING_UNMATCHED_COLUMN = new AllowableValue("Warn on Unmatched Columns",
            "Warn on Unmatched Columns",
            "Any column in the database that does not have a field in the document will be assumed to not be required.  A warning will be logged");
    static final AllowableValue FAIL_UNMATCHED_COLUMN = new AllowableValue("Fail on Unmatched Columns",
            "Fail on Unmatched Columns",
            "A flow will fail if any column in the database that does not have a field in the document.  An error will be logged");

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("db-record-sink-dcbp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain a connection to the database for sending records.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor CATALOG_NAME = new PropertyDescriptor.Builder()
            .name("db-record-sink-catalog-name")
            .displayName("Catalog Name")
            .description("The name of the catalog that the statement should update. This may not apply for the database that you are updating. In this case, leave the field empty")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("db-record-sink-schema-name")
            .displayName("Schema Name")
            .description("The name of the schema that the table belongs to. This may not apply for the database that you are updating. In this case, leave the field empty")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("db-record-sink-table-name")
            .displayName("Table Name")
            .description("The name of the table that the statement should affect.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("db-record-sink-translate-field-names")
            .displayName("Translate Field Names")
            .description("If true, the Processor will attempt to translate field names into the appropriate column names for the table specified. "
                    + "If false, the field names must match the column names exactly, or the column will not be updated")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor UNMATCHED_FIELD_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("db-record-sink-unmatched-field-behavior")
            .displayName("Unmatched Field Behavior")
            .description("If an incoming record has a field that does not map to any of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_FIELD, FAIL_UNMATCHED_FIELD)
            .defaultValue(IGNORE_UNMATCHED_FIELD.getValue())
            .build();

    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("db-record-sink-unmatched-column-behavior")
            .displayName("Unmatched Column Behavior")
            .description("If an incoming record does not have a field mapping for all of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_COLUMN, WARNING_UNMATCHED_COLUMN, FAIL_UNMATCHED_COLUMN)
            .defaultValue(FAIL_UNMATCHED_COLUMN.getValue())
            .build();

    static final PropertyDescriptor QUOTED_IDENTIFIERS = new PropertyDescriptor.Builder()
            .name("db-record-sink-quoted-identifiers")
            .displayName("Quote Column Identifiers")
            .description("Enabling this option will cause all column names to be quoted, allowing you to use reserved words as column names in your tables.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUOTED_TABLE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("db-record-sink-quoted-table-identifiers")
            .displayName("Quote Table Identifiers")
            .description("Enabling this option will cause the table name to be quoted to support the use of special characters in the table name.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("db-record-sink-query-timeout")
            .displayName("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL statement "
                    + ", zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        DBCP_SERVICE,
        CATALOG_NAME,
        SCHEMA_NAME,
        TABLE_NAME,
        TRANSLATE_FIELD_NAMES,
        UNMATCHED_FIELD_BEHAVIOR,
        UNMATCHED_COLUMN_BEHAVIOR,
        QUOTED_IDENTIFIERS,
        QUOTED_TABLE_IDENTIFIER,
        QUERY_TIMEOUT
    );

    private volatile ConfigurationContext context;
    private volatile DBCPService dbcpService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.context = context;
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    @Override
    public WriteResult sendData(RecordSet recordSet, Map<String, String> attributes, boolean sendZeroResults) throws IOException {
        Boolean originalAutoCommit = null;
        Connection connection = null;
        WriteResult writeResult = null;
        try {
            connection = dbcpService.getConnection(attributes);
            originalAutoCommit = connection.getAutoCommit();
            if (originalAutoCommit) {
                try {
                    connection.setAutoCommit(false);
                } catch (SQLFeatureNotSupportedException sfnse) {
                    getLogger().debug("setAutoCommit(false) not supported by this driver");
                }
            }
            final DMLSettings settings = new DMLSettings(context);
            final String catalog = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions().getValue();
            final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions().getValue();
            final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
            final int queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS).intValue();

            // Ensure the table name has been set, the generated SQL statements (and TableSchema cache) will need it
            if (StringUtils.isEmpty(tableName)) {
                throw new IOException("Cannot process because Table Name is null or empty");
            }

            TableSchema tableSchema = TableSchema.from(connection, catalog, schemaName, tableName, settings.translateFieldNames);
            // build the fully qualified table name
            final StringBuilder tableNameBuilder = new StringBuilder();
            if (catalog != null) {
                tableNameBuilder.append(catalog).append(".");
            }
            if (schemaName != null) {
                tableNameBuilder.append(schemaName).append(".");
            }
            tableNameBuilder.append(tableName);
            final String fqTableName = tableNameBuilder.toString();

            RecordSchema recordSchema = recordSet.getSchema();
            if (recordSchema == null) {
                throw new IllegalArgumentException("No record schema specified!");
            }

            final SqlAndIncludedColumns sqlHolder;
            sqlHolder = generateInsert(recordSchema, fqTableName, tableSchema, settings);

            try (PreparedStatement ps = connection.prepareStatement(sqlHolder.getSql())) {

                try {
                    ps.setQueryTimeout(queryTimeout); // timeout in seconds
                } catch (SQLException se) {
                    // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
                    if (queryTimeout > 0) {
                        throw se;
                    }
                }

                Record currentRecord;
                List<Integer> fieldIndexes = sqlHolder.getFieldIndexes();
                int recordCount = 0;

                while ((currentRecord = recordSet.next()) != null) {
                    Object[] values = currentRecord.getValues();
                    List<DataType> dataTypes = currentRecord.getSchema().getDataTypes();
                    if (values != null) {
                        if (fieldIndexes != null) {
                            for (int i = 0; i < fieldIndexes.size(); i++) {
                                final int currentFieldIndex = fieldIndexes.get(i);
                                final Object currentValue = values[currentFieldIndex];
                                final DataType dataType = dataTypes.get(currentFieldIndex);
                                final int sqlType = DataTypeUtils.getSQLTypeValue(dataType);

                                ps.setObject(i + 1, currentValue, sqlType);
                            }
                        } else {
                            // If there's no index map, assume all values are included and set them in order
                            for (int i = 0; i < values.length; i++) {
                                final Object currentValue = values[i];
                                final DataType dataType = dataTypes.get(i);
                                final int sqlType = DataTypeUtils.getSQLTypeValue(dataType);
                                ps.setObject(i + 1, currentValue, sqlType);
                            }
                        }
                        ps.addBatch();
                    }
                    recordCount++;
                }
                ps.executeBatch();
                writeResult = WriteResult.of(recordCount, attributes);
            }

        } catch (IOException ioe) {
            throw ioe;
        } catch (Exception e) {
            throw new IOException("Failed to write metrics using record writer: " + e.getMessage(), e);
        } finally {
            if (connection != null) {
                if (originalAutoCommit != null) {
                    try {
                        connection.setAutoCommit(originalAutoCommit);
                    } catch (Exception e) {
                        getLogger().debug("Error restoring auto-commit", e);
                    }
                }
                try {
                    connection.close();
                } catch (Exception e) {
                    getLogger().debug("Error closing connection", e);
                }
            }
        }
        return writeResult;
    }

    private static String normalizeColumnName(final String colName, final boolean translateColumnNames) {
        return colName == null ? null : (translateColumnNames ? colName.toUpperCase().replace("_", "") : colName);
    }

    private Set<String> getNormalizedColumnNames(final RecordSchema schema, final boolean translateFieldNames) {
        final Set<String> normalizedFieldNames = new HashSet<>();
        if (schema != null) {
            schema.getFieldNames().forEach((fieldName) -> normalizedFieldNames.add(normalizeColumnName(fieldName, translateFieldNames)));
        }
        return normalizedFieldNames;
    }

    private SqlAndIncludedColumns generateInsert(final RecordSchema recordSchema, final String tableName, final TableSchema tableSchema, final DMLSettings settings)
            throws IllegalArgumentException, SQLException {

        final Set<String> normalizedFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames);

        for (final String requiredColName : tableSchema.getRequiredColumnNames()) {
            final String normalizedColName = normalizeColumnName(requiredColName, settings.translateFieldNames);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "Record does not have a value for the Required column '" + requiredColName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new IllegalArgumentException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        if (settings.quoteTableName) {
            sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(tableSchema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }
        sqlBuilder.append(" (");

        // iterate over all of the fields in the record, building the SQL statement by adding the column names
        List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(normalizeColumnName(fieldName, settings.translateFieldNames));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database");
                }

                if (desc != null) {
                    if (fieldsFound.getAndIncrement() > 0) {
                        sqlBuilder.append(", ");
                    }

                    if (settings.escapeColumnNames) {
                        sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                                .append(desc.getColumnName())
                                .append(tableSchema.getQuotedIdentifierString());
                    } else {
                        sqlBuilder.append(desc.getColumnName());
                    }

                    includedColumns.add(i);
                }
            }

            // complete the SQL statements by adding ?'s for all of the values to be escaped.
            sqlBuilder.append(") VALUES (");
            sqlBuilder.append(StringUtils.repeat("?", ",", includedColumns.size()));
            sqlBuilder.append(")");

            if (fieldsFound.get() == 0) {
                throw new SQLDataException("None of the fields in the record map to the columns defined by the " + tableName + " table");
            }
        }
        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    private static class DMLSettings {
        private final boolean translateFieldNames;
        private final boolean ignoreUnmappedFields;

        // Is the unmatched column behaviour fail or warning?
        private final boolean failUnmappedColumns;
        private final boolean warningUnmappedColumns;

        // Escape column names?
        private final boolean escapeColumnNames;

        // Quote table name?
        private final boolean quoteTableName;

        private DMLSettings(PropertyContext context) {
            translateFieldNames = context.getProperty(TRANSLATE_FIELD_NAMES).asBoolean();
            ignoreUnmappedFields = IGNORE_UNMATCHED_FIELD.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_FIELD_BEHAVIOR).getValue());

            failUnmappedColumns = FAIL_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());
            warningUnmappedColumns = WARNING_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());

            escapeColumnNames = context.getProperty(QUOTED_IDENTIFIERS).asBoolean();
            quoteTableName = context.getProperty(QUOTED_TABLE_IDENTIFIER).asBoolean();
        }
    }

    /**
     * A holder class for a SQL prepared statement and a BitSet indicating which columns are being inserted (to determine which values from the record to set on the statement)
     * A value of null for getIncludedColumns indicates that all columns/fields should be included.
     */
    static class SqlAndIncludedColumns {
        String sql;
        List<Integer> fieldIndexes;

        /**
         * Constructor
         *
         * @param sql          The prepared SQL statement (including parameters notated by ? )
         * @param fieldIndexes A List of record indexes. The index of the list is the location of the record field in the SQL prepared statement
         */
        SqlAndIncludedColumns(String sql, List<Integer> fieldIndexes) {
            this.sql = sql;
            this.fieldIndexes = fieldIndexes;
        }

        String getSql() {
            return sql;
        }

        List<Integer> getFieldIndexes() {
            return fieldIndexes;
        }
    }

    static class TableSchema {
        private List<String> requiredColumnNames;
        private Map<String, ColumnDescription> columns;
        private String quotedIdentifierString;

        private TableSchema(final List<ColumnDescription> columnDescriptions, final boolean translateColumnNames, final String quotedIdentifierString) {
            this.columns = new HashMap<>();
            this.requiredColumnNames = new ArrayList<>();
            this.quotedIdentifierString = quotedIdentifierString;
            for (final ColumnDescription desc : columnDescriptions) {
                columns.put(normalizeColumnName(desc.columnName, translateColumnNames), desc);
                if (desc.isRequired()) {
                    requiredColumnNames.add(desc.columnName);
                }
            }
        }

        Map<String, ColumnDescription> getColumns() {
            return columns;
        }

        List<String> getRequiredColumnNames() {
            return requiredColumnNames;
        }

        String getQuotedIdentifierString() {
            return quotedIdentifierString;
        }

        static TableSchema from(final Connection conn, final String catalog, final String schema, final String tableName,
                                final boolean translateColumnNames) throws SQLException {
            final DatabaseMetaData dmd = conn.getMetaData();
            if (!dmd.getTables(catalog, schema, tableName, null).next()) {
                throw new SQLException("Table " + tableName + " does not exist in the database");
            }

            try (final ResultSet colrs = dmd.getColumns(catalog, schema, tableName, "%")) {
                final List<ColumnDescription> cols = new ArrayList<>();
                while (colrs.next()) {
                    final ColumnDescription col = ColumnDescription.from(colrs);
                    cols.add(col);
                }

                return new TableSchema(cols, translateColumnNames, dmd.getIdentifierQuoteString());
            }
        }
    }

    protected static class ColumnDescription {
        private final String columnName;
        private final int dataType;
        private final boolean required;
        private final Integer columnSize;

        ColumnDescription(final String columnName, final int dataType, final boolean required, final Integer columnSize) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.required = required;
            this.columnSize = columnSize;
        }

        public int getDataType() {
            return dataType;
        }

        public Integer getColumnSize() {
            return columnSize;
        }

        public String getColumnName() {
            return columnName;
        }

        public boolean isRequired() {
            return required;
        }

        public static ColumnDescription from(final ResultSet resultSet) throws SQLException {
            final ResultSetMetaData md = resultSet.getMetaData();
            List<String> columns = new ArrayList<>();

            for (int i = 1; i < md.getColumnCount() + 1; i++) {
                columns.add(md.getColumnName(i));
            }
            // COLUMN_DEF must be read first to work around Oracle bug, see NIFI-4279 for details
            final String defaultValue = resultSet.getString("COLUMN_DEF");
            final String columnName = resultSet.getString("COLUMN_NAME");
            final int dataType = resultSet.getInt("DATA_TYPE");
            final int colSize = resultSet.getInt("COLUMN_SIZE");

            final String nullableValue = resultSet.getString("IS_NULLABLE");
            final boolean isNullable = "YES".equalsIgnoreCase(nullableValue) || nullableValue.isEmpty();
            String autoIncrementValue = "NO";

            if (columns.contains("IS_AUTOINCREMENT")) {
                autoIncrementValue = resultSet.getString("IS_AUTOINCREMENT");
            }

            final boolean isAutoIncrement = "YES".equalsIgnoreCase(autoIncrementValue);
            final boolean required = !isNullable && !isAutoIncrement && defaultValue == null;

            return new ColumnDescription(columnName, dataType, required, colSize == 0 ? null : colSize);
        }
    }
}

