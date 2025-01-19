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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.database.dialect.service.api.ColumnDefinition;
import org.apache.nifi.database.dialect.service.api.StandardColumnDefinition;
import org.apache.nifi.database.dialect.service.api.DatabaseDialectService;
import org.apache.nifi.database.dialect.service.api.QueryClause;
import org.apache.nifi.database.dialect.service.api.QueryClauseType;
import org.apache.nifi.database.dialect.service.api.QueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StandardQueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementResponse;
import org.apache.nifi.database.dialect.service.api.StatementType;
import org.apache.nifi.database.dialect.service.api.TableDefinition;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.db.DatabaseAdapterDescriptor;
import org.apache.nifi.processors.standard.db.impl.DatabaseAdapterDatabaseDialectService;
import org.apache.nifi.processors.standard.db.impl.DatabaseDialectServiceDatabaseAdapter;
import org.apache.nifi.processors.standard.db.impl.PhoenixDatabaseAdapter;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

/**
 * A base class for common code shared by processors that fetch RDBMS data.
 */
public abstract class AbstractDatabaseFetchProcessor extends AbstractSessionFactoryProcessor {

    public static final String INITIAL_MAX_VALUE_PROP_START = "initial.maxvalue.";
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();

    protected Set<Relationship> relationships;

    // Properties
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain a connection to the database.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the database table to be queried.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("Columns to Return")
            .description("A comma-separated list of column names to be used in the query. If your database requires "
                    + "special treatment of the names (quoting, e.g.), each name should include such treatment. If no "
                    + "column names are supplied, all columns in the specified table will be returned. NOTE: It is important "
                    + "to use consistent column names for a given table for incremental fetch to work properly.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_VALUE_COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("Maximum-value Columns")
            .description("A comma-separated list of column names. The processor will keep track of the maximum value "
                    + "for each column that has been returned since the processor started running. Using multiple columns implies an order "
                    + "to the column list, and each column's values are expected to increase more slowly than the previous columns' values. Thus, "
                    + "using multiple columns implies a hierarchical structure of columns, which is usually used for partitioning tables. This processor "
                    + "can be used to retrieve only those rows that have been added/updated since the last retrieval. Note that some "
                    + "JDBC types such as bit/boolean are not conducive to maintaining maximum value, so columns of these "
                    + "types should not be listed in this property, and will result in error(s) during processing. If no columns "
                    + "are provided, all rows from the table will be considered, which could have a performance impact. NOTE: It is important "
                    + "to use consistent max-value column names for a given table for incremental fetch to work properly.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL select query "
                    + ", zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor WHERE_CLAUSE = new PropertyDescriptor.Builder()
            .name("db-fetch-where-clause")
            .displayName("Additional WHERE clause")
            .description("A custom clause to be added in the WHERE condition when building SQL queries.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SQL_QUERY = new PropertyDescriptor.Builder()
            .name("db-fetch-sql-query")
            .displayName("Custom Query")
            .description("A custom SQL query used to retrieve data. Instead of building a SQL query from "
                    + "other properties, this query will be wrapped as a sub-query. Query must have no ORDER BY statement.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected List<PropertyDescriptor> propDescriptors;

    // The delimiter to use when referencing qualified names (such as table@!@column in the state map)
    protected static final String NAMESPACE_DELIMITER = "@!@";

    public static final PropertyDescriptor DB_TYPE = DatabaseAdapterDescriptor.getDatabaseTypeDescriptor("db-fetch-db-type");
    static final PropertyDescriptor DATABASE_DIALECT_SERVICE = DatabaseAdapterDescriptor.getDatabaseDialectServiceDescriptor(DB_TYPE);

    protected final Map<String, Integer> columnTypeMap = new HashMap<>();

    // This value is set when the processor is scheduled and indicates whether the Table Name property contains Expression Language.
    // It is used for backwards-compatibility purposes; if the value is false and the fully-qualified state key (table + column) is not found,
    // the processor will look for a state key with just the column name.
    protected volatile boolean isDynamicTableName = false;

    // This value is set when the processor is scheduled and indicates whether the Maximum Value Columns property contains Expression Language.
    // It is used for backwards-compatibility purposes; if the table name and max-value columns are static, then the column types can be
    // pre-fetched when the processor is scheduled, rather than having to populate them on-the-fly.
    protected volatile boolean isDynamicMaxValues = false;

    // This value is cleared when the processor is scheduled, and set to true after setup() is called and completes successfully. This enables
    // the setup logic to be performed in onTrigger() versus OnScheduled to avoid any issues with DB connection when first scheduled to run.
    protected final AtomicBoolean setupComplete = new AtomicBoolean(false);

    private static final DateTimeFormatter TIME_TYPE_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private static final QueryClause ZERO_RESULT_WHERE_CLAUSE = new QueryClause(QueryClauseType.WHERE, "1 = 0");

    // A Map (name to value) of initial maximum-value properties, filled at schedule-time and used at trigger-time
    protected Map<String, String> maxValueProperties;

    // A common validation procedure for DB fetch processors, it stores whether the Table Name and/or Max Value Column properties have expression language
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        // For backwards-compatibility, keep track of whether the table name and max-value column properties are dynamic (i.e. has expression language)
        isDynamicTableName = validationContext.isExpressionLanguagePresent(validationContext.getProperty(TABLE_NAME).getValue());
        isDynamicMaxValues = validationContext.isExpressionLanguagePresent(validationContext.getProperty(MAX_VALUE_COLUMN_NAMES).getValue());

        return super.customValidate(validationContext);
    }

    public void setup(final ProcessContext context) {
        setup(context, true, null);
    }

    public void setup(final ProcessContext context, boolean shouldCleanCache, FlowFile flowFile) {
        final DatabaseDialectService databaseDialectService = getDatabaseDialectService(context);

        synchronized (setupComplete) {
            setupComplete.set(false);
            final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions(flowFile).getValue();

            // If there are no max-value column names specified, we don't need to perform this processing
            if (StringUtils.isEmpty(maxValueColumnNames)) {
                setupComplete.set(true);
                return;
            }

            // Try to fill the columnTypeMap with the types of the desired max-value columns
            final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
            final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
            final String sqlQuery = context.getProperty(SQL_QUERY).evaluateAttributeExpressions().getValue();

            try (final Connection con = dbcpService.getConnection(flowFile == null ? Collections.emptyMap() : flowFile.getAttributes());
                 final Statement st = con.createStatement()) {

                // Try a query that returns no rows, for the purposes of getting metadata about the columns. It is possible
                // to use DatabaseMetaData.getColumns(), but not all drivers support this, notably the schema-on-read
                // approach as in Apache Drill
                final QueryStatementRequest statementRequest = getMaxValueStatementRequest(tableName, maxValueColumnNames, sqlQuery);
                final StatementResponse statementResponse = databaseDialectService.getStatement(statementRequest);
                final String query = statementResponse.sql();

                ResultSet resultSet = st.executeQuery(query);
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int numCols = resultSetMetaData.getColumnCount();
                if (numCols > 0) {
                    if (shouldCleanCache) {
                        columnTypeMap.clear();
                    }

                    final String[] maxValueColumnNameList = maxValueColumnNames.toLowerCase().split(",");
                    final List<String> maxValueQualifiedColumnNameList = new ArrayList<>();

                    for (String maxValueColumn : maxValueColumnNameList) {
                        String colKey = getStateKey(tableName, maxValueColumn.trim());
                        maxValueQualifiedColumnNameList.add(colKey);
                    }

                    for (int i = 1; i <= numCols; i++) {
                        String colName = resultSetMetaData.getColumnName(i).toLowerCase();
                        String colKey = getStateKey(tableName, colName);

                        //only include columns that are part of the maximum value tracking column list
                        if (!maxValueQualifiedColumnNameList.contains(colKey)) {
                            continue;
                        }

                        int colType = resultSetMetaData.getColumnType(i);
                        columnTypeMap.putIfAbsent(colKey, colType);
                    }

                    for (String maxValueColumn : maxValueColumnNameList) {
                        String colKey = getStateKey(tableName, maxValueColumn.trim().toLowerCase());
                        if (!columnTypeMap.containsKey(colKey)) {
                            throw new ProcessException("Column not found in the table/query specified: " + maxValueColumn);
                        }
                    }
                } else {
                    throw new ProcessException("No columns found in table from those specified: " + maxValueColumnNames);
                }

            } catch (SQLException e) {
                throw new ProcessException("Unable to communicate with database in order to determine column types", e);
            }
            setupComplete.set(true);
        }
    }

    protected DatabaseDialectService getDatabaseDialectService(final PropertyContext context) {
        final DatabaseDialectService databaseDialectService;
        final String databaseType = context.getProperty(DB_TYPE).getValue();
        if (DatabaseDialectServiceDatabaseAdapter.NAME.equals(databaseType)) {
            databaseDialectService = context.getProperty(DATABASE_DIALECT_SERVICE).asControllerService(DatabaseDialectService.class);
        } else {
            databaseDialectService = new DatabaseAdapterDatabaseDialectService(databaseType);
        }
        return databaseDialectService;
    }

    private QueryStatementRequest getMaxValueStatementRequest(final String tableName, final String maxValueColumnNames, final String derivedTableQuery) {
        final List<ColumnDefinition> maxValueColumns = Arrays.stream(maxValueColumnNames.split(","))
                .map(StandardColumnDefinition::new)
                .map(ColumnDefinition.class::cast)
                .toList();

        final TableDefinition tableDefinition = new TableDefinition(Optional.empty(), Optional.empty(), tableName, maxValueColumns);
        return new StandardQueryStatementRequest(
                StatementType.SELECT,
                tableDefinition,
                Optional.ofNullable(derivedTableQuery),
                List.of(ZERO_RESULT_WHERE_CLAUSE),
                Optional.empty()
        );
    }

    protected static String getMaxValueFromRow(ResultSet resultSet,
                                               int columnIndex,
                                               Integer type,
                                               String maxValueString)
            throws ParseException, IOException, SQLException {

        // Skip any columns we're not keeping track of or whose value is null
        if (type == null || resultSet.getObject(columnIndex) == null) {
            return null;
        }

        switch (type) {
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case ROWID:
                String colStringValue = resultSet.getString(columnIndex);
                if (maxValueString == null || colStringValue.compareTo(maxValueString) > 0) {
                    return colStringValue;
                }
                break;

            case INTEGER:
            case SMALLINT:
            case TINYINT:
                Integer colIntValue = resultSet.getInt(columnIndex);
                Integer maxIntValue = null;
                if (maxValueString != null) {
                    maxIntValue = Integer.valueOf(maxValueString);
                }
                if (maxIntValue == null || colIntValue > maxIntValue) {
                    return colIntValue.toString();
                }
                break;

            case BIGINT:
                Long colLongValue = resultSet.getLong(columnIndex);
                Long maxLongValue = null;
                if (maxValueString != null) {
                    maxLongValue = Long.valueOf(maxValueString);
                }
                if (maxLongValue == null || colLongValue > maxLongValue) {
                    return colLongValue.toString();
                }
                break;

            case FLOAT:
            case REAL:
            case DOUBLE:
                Double colDoubleValue = resultSet.getDouble(columnIndex);
                Double maxDoubleValue = null;
                if (maxValueString != null) {
                    maxDoubleValue = Double.valueOf(maxValueString);
                }
                if (maxDoubleValue == null || colDoubleValue > maxDoubleValue) {
                    return colDoubleValue.toString();
                }
                break;

            case DECIMAL:
            case NUMERIC:
                BigDecimal colBigDecimalValue = resultSet.getBigDecimal(columnIndex);
                BigDecimal maxBigDecimalValue = null;
                if (maxValueString != null) {
                    DecimalFormat df = new DecimalFormat();
                    df.setParseBigDecimal(true);
                    maxBigDecimalValue = (BigDecimal) df.parse(maxValueString);
                }
                if (maxBigDecimalValue == null || colBigDecimalValue.compareTo(maxBigDecimalValue) > 0) {
                    return colBigDecimalValue.toString();
                }
                break;

            case DATE:
                Date rawColDateValue = resultSet.getDate(columnIndex);
                java.sql.Date colDateValue = new java.sql.Date(rawColDateValue.getTime());
                java.sql.Date maxDateValue = null;
                if (maxValueString != null) {
                    maxDateValue = java.sql.Date.valueOf(maxValueString);
                }
                if (maxDateValue == null || colDateValue.after(maxDateValue)) {
                    return colDateValue.toString();
                }
                break;

            case TIME:
                // Compare milliseconds-since-epoch. Need getTimestamp() instead of getTime() since some databases
                // don't return milliseconds in the Time returned by getTime().
                Instant colTimeValue = Instant.ofEpochMilli(resultSet.getTimestamp(columnIndex).getTime());
                LocalTime maxTimeValue = null;
                if (maxValueString != null) {
                    try {
                        maxTimeValue = LocalTime.parse(maxValueString, TIME_TYPE_FORMAT);
                    } catch (DateTimeParseException pe) {
                        // Shouldn't happen, but just in case, leave the value as null so the new value will be stored
                    }
                }
                if (maxTimeValue == null || colTimeValue.isAfter(maxTimeValue.atDate(LocalDate.now()).atZone(ZoneId.systemDefault()).toInstant())) {
                    return TIME_TYPE_FORMAT.format(LocalTime.ofInstant(colTimeValue, ZoneId.systemDefault()));
                }
                break;

            case TIMESTAMP:
                Timestamp colTimestampValue = resultSet.getTimestamp(columnIndex);
                java.sql.Timestamp maxTimestampValue = null;
                if (maxValueString != null) {
                    // For backwards compatibility, the type might be TIMESTAMP but the state value is in DATE format. This should be a one-time occurrence as the next maximum value
                    // should be stored as a full timestamp. Even so, check to see if the value is missing time-of-day information, and use the "date" coercion rather than the
                    // "timestamp" coercion in that case
                    try {
                        maxTimestampValue = java.sql.Timestamp.valueOf(maxValueString);
                    } catch (IllegalArgumentException iae) {
                        maxTimestampValue = new java.sql.Timestamp(java.sql.Date.valueOf(maxValueString).getTime());
                    }
                }
                if (maxTimestampValue == null || colTimestampValue.after(maxTimestampValue)) {
                    return colTimestampValue.toString();
                }
                break;

            case BIT:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case ARRAY:
            case BLOB:
            case CLOB:
            default:
                throw new IOException("Type for column " + columnIndex + " is not valid for maintaining maximum value");
        }
        return null;
    }

    /**
     * Returns a SQL literal for the given value based on its type. For example, values of character type need to be enclosed
     * in single quotes, whereas values of numeric type should not be.
     *
     * @param type  The JDBC type for the desired literal
     * @param value The value to be converted to a SQL literal
     * @return A String representing the given value as a literal of the given type
     */
    protected static String getLiteralByType(int type, String value, String databaseType) {
        // Format value based on column type. For example, strings and timestamps need to be quoted
        switch (type) {
            // For string-represented values, put in single quotes
            case CHAR:
            case LONGNVARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
            case ROWID:
                return "'" + value + "'";
            case TIME:
                if (PhoenixDatabaseAdapter.NAME.equals(databaseType)) {
                    return "time '" + value + "'";
                }
            case DATE:
            case TIMESTAMP:
                // TODO delegate to database adapter the conversion instead of using if in this
                // class.
                // TODO (cont) if a new else is added, please refactor the code.
                // Ideally we should probably have a method on the adapter to get a clause that
                // coerces a
                // column to a Timestamp if need be (the generic one can be a no-op)
                if (!StringUtils.isEmpty(databaseType)
                        && (databaseType.contains("Oracle") || PhoenixDatabaseAdapter.NAME.equals(databaseType))) {
                    // For backwards compatibility, the type might be TIMESTAMP but the state value
                    // is in DATE format. This should be a one-time occurrence as the next maximum
                    // value
                    // should be stored as a full timestamp. Even so, check to see if the value is
                    // missing time-of-day information, and use the "date" coercion rather than the
                    // "timestamp" coercion in that case
                    if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
                        return "date '" + value + "'";
                    } else {
                        return "timestamp '" + value + "'";
                    }
                } else {
                    return "'" + value + "'";
                }
                // Else leave as is (numeric types, e.g.)
            default:
                return value;
        }
    }

    /**
     * Construct a key string for a corresponding state value.
     * @param prefix A prefix may contain database and table name, or just table name, this can be null
     * @param columnName A column name
     * @return a state key string
     */
    protected static String getStateKey(String prefix, String columnName) {
        StringBuilder sb = new StringBuilder();
        if (prefix != null) {
            final String prefixUnwrapped = prefix.toLowerCase().replaceAll("[\"`\\[\\]]", "");
            sb.append(prefixUnwrapped);
            sb.append(NAMESPACE_DELIMITER);
        }
        if (columnName != null) {
            final String columnNameUnwrapped = columnName.toLowerCase().replaceAll("[\"`\\[\\]]", "");
            sb.append(columnNameUnwrapped);
        }
        return sb.toString();
    }

    protected Map<String, String> getDefaultMaxValueProperties(final ProcessContext context, final FlowFile flowFile) {
        final Map<String, String> defaultMaxValues = new HashMap<>();

        context.getProperties().forEach((k, v) -> {
            final String key = k.getName();

            if (key.startsWith(INITIAL_MAX_VALUE_PROP_START)) {
                defaultMaxValues.put(key.substring(INITIAL_MAX_VALUE_PROP_START.length()), context.getProperty(k).evaluateAttributeExpressions(flowFile).getValue());
            }
        });
        return defaultMaxValues;
    }
}
