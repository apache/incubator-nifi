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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.database.dialect.service.api.ColumnDefinition;
import org.apache.nifi.database.dialect.service.api.StandardColumnDefinition;
import org.apache.nifi.database.dialect.service.api.DatabaseDialectService;
import org.apache.nifi.database.dialect.service.api.PageRequest;
import org.apache.nifi.database.dialect.service.api.QueryClause;
import org.apache.nifi.database.dialect.service.api.QueryClauseType;
import org.apache.nifi.database.dialect.service.api.QueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StandardPageRequest;
import org.apache.nifi.database.dialect.service.api.StandardQueryStatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementResponse;
import org.apache.nifi.database.dialect.service.api.StatementType;
import org.apache.nifi.database.dialect.service.api.TableDefinition;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@TriggerSerially
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"sql", "select", "jdbc", "query", "database", "fetch", "generate"})
@SeeAlso({QueryDatabaseTable.class, ExecuteSQL.class, ListDatabaseTables.class})
@CapabilityDescription("Generates SQL select queries that fetch \"pages\" of rows from a table. The partition size property, along with the table's row count, "
        + "determine the size and number of pages and generated FlowFiles. In addition, incremental fetching can be achieved by setting Maximum-Value Columns, "
        + "which causes the processor to track the columns' maximum values, thus only fetching rows whose columns' values exceed the observed maximums. This "
        + "processor is intended to be run on the Primary Node only.\n\n"
        + "This processor can accept incoming connections; the behavior of the processor is different whether incoming connections are provided:\n"
        + "  - If no incoming connection(s) are specified, the processor will generate SQL queries on the specified processor schedule. Expression Language is supported for many "
        + "fields, but no FlowFile attributes are available. However the properties will be evaluated using the Environment/System properties.\n"
        + "  - If incoming connection(s) are specified and no FlowFile is available to a processor task, no work will be performed.\n"
        + "  - If incoming connection(s) are specified and a FlowFile is available to a processor task, the FlowFile's attributes may be used in Expression Language for such fields "
        + "as Table Name and others. However, the Max-Value Columns and Columns to Return fields must be empty or refer to columns that are available in each specified table.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a query on the specified table, the maximum values for "
        + "the specified column(s) will be retained for use in future executions of the query. This allows the Processor "
        + "to fetch only those records that have max values greater than the retained values. This can be used for "
        + "incremental fetching, fetching of newly added rows, etc. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation")
@WritesAttributes({
        @WritesAttribute(attribute = "generatetablefetch.sql.error", description = "If the processor has incoming connections, and processing an incoming FlowFile causes "
                + "a SQL Exception, the FlowFile is routed to failure and this attribute is set to the exception message."),
        @WritesAttribute(attribute = "generatetablefetch.tableName", description = "The name of the database table to be queried."),
        @WritesAttribute(attribute = "generatetablefetch.columnNames", description = "The comma-separated list of column names used in the query."),
        @WritesAttribute(attribute = "generatetablefetch.whereClause", description = "Where clause used in the query to get the expected rows."),
        @WritesAttribute(attribute = "generatetablefetch.maxColumnNames", description = "The comma-separated list of column names used to keep track of data "
                + "that has been returned since the processor started running."),
        @WritesAttribute(attribute = "generatetablefetch.limit", description = "The number of result rows to be fetched by the SQL statement."),
        @WritesAttribute(attribute = "generatetablefetch.offset", description = "Offset to be used to retrieve the corresponding partition."),
        @WritesAttribute(attribute = "fragment.identifier", description = "All FlowFiles generated from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "This is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet."),
        @WritesAttribute(attribute = "fragment.index", description = "This is the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all generated from the same execution. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same execution and in what order  "
                + "FlowFiles were produced"),
})
@DynamicProperty(name = "initial.maxvalue.<max_value_column>", value = "Initial maximum value for the specified column",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES, description = "Specifies an initial "
        + "max value for max value columns. Properties should be added in the format `initial.maxvalue.<max_value_column>`. This value is only used the first time "
        + "the table is accessed (when a Maximum Value Column is specified). In the case of incoming connections, the value is only used the first time for each table "
        + "specified in the FlowFiles.")
public class GenerateTableFetch extends AbstractDatabaseFetchProcessor {

    public static final PropertyDescriptor PARTITION_SIZE = new PropertyDescriptor.Builder()
            .name("gen-table-fetch-partition-size")
            .displayName("Partition Size")
            .description("The number of result rows to be fetched by each generated SQL statement. The total number of rows in "
                    + "the table divided by the partition size gives the number of SQL statements (i.e. FlowFiles) generated. A "
                    + "value of zero indicates that a single FlowFile is to be generated whose SQL statement will fetch all rows "
                    + "in the table.")
            .defaultValue("10000")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor COLUMN_FOR_VALUE_PARTITIONING = new PropertyDescriptor.Builder()
            .name("gen-table-column-for-val-partitioning")
            .displayName("Column for Value Partitioning")
            .description("The name of a column whose values will be used for partitioning. The default behavior is to use row numbers on the result set for partitioning into "
                    + "'pages' to be fetched from the database, using an offset/limit strategy. However for certain databases, it can be more efficient under the right circumstances to use "
                    + "the column values themselves to define the 'pages'. This property should only be used when the default queries are not performing well, when there is no maximum-value "
                    + "column or a single maximum-value column whose type can be coerced to a long integer (i.e. not date or timestamp), and the column values are evenly distributed and not "
                    + "sparse, for best performance.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor OUTPUT_EMPTY_FLOWFILE_ON_ZERO_RESULTS = new PropertyDescriptor.Builder()
            .name("gen-table-output-flowfile-on-zero-results")
            .displayName("Output Empty FlowFile on Zero Results")
            .description("Depending on the specified properties, an execution of this processor may not result in any SQL statements generated. When this property "
                    + "is true, an empty FlowFile will be generated (having the parent of the incoming FlowFile if present) and transferred to the 'success' relationship. "
                    + "When this property is false, no output FlowFiles will be generated.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final PropertyDescriptor CUSTOM_ORDERBY_COLUMN = new PropertyDescriptor.Builder()
            .name("gen-table-custom-orderby-column")
            .displayName("Custom ORDER BY Column")
            .description("The name of a column to be used for ordering the results if Max-Value Columns are not provided and partitioning is enabled. This property is ignored if either "
                    + "Max-Value Columns is set or Partition Size = 0. NOTE: If neither Max-Value Columns nor Custom ORDER BY Column is set, then depending on the "
                    + "the database/driver, the processor may report an error and/or the generated SQL may result in missing and/or duplicate rows. This is because without an explicit "
                    + "ordering, fetching each partition is done using an arbitrary ordering.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            DBCP_SERVICE,
            DB_TYPE,
            DATABASE_DIALECT_SERVICE,
            TABLE_NAME,
            COLUMN_NAMES,
            MAX_VALUE_COLUMN_NAMES,
            QUERY_TIMEOUT,
            PARTITION_SIZE,
            COLUMN_FOR_VALUE_PARTITIONING,
            WHERE_CLAUSE,
            CUSTOM_ORDERBY_COLUMN,
            OUTPUT_EMPTY_FLOWFILE_ON_ZERO_RESULTS
    );

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("This relationship is only used when SQL query execution (using an incoming FlowFile) failed. The incoming FlowFile will be penalized and routed to this relationship. "
                    + "If no incoming connection(s) are specified, this relationship is unused.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    public GenerateTableFetch() {
        propDescriptors = PROPERTIES;
        relationships = RELATIONSHIPS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        final PropertyValue columnForPartitioning = validationContext.getProperty(COLUMN_FOR_VALUE_PARTITIONING);
        // If no EL is present, ensure it's a single column (i.e. no commas in the property value)
        if (columnForPartitioning.isSet() && !columnForPartitioning.isExpressionLanguagePresent() && columnForPartitioning.getValue().contains(",")) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    COLUMN_FOR_VALUE_PARTITIONING.getDisplayName() + " requires a single column name, but a comma was detected").build());
        }

        return results;
    }

    @Override
    @OnScheduled
    public void setup(final ProcessContext context) {
        if (context.hasIncomingConnection() && !context.hasNonLoopConnection()) {
            getLogger().error("The failure relationship can be used only if there is another incoming connection to this processor.");
        }
    }

    @OnStopped
    public void stop() {
        // Reset the column type map in case properties change
        setupComplete.set(false);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        // Fetch the column/table info once (if the table name and max value columns are not dynamic). Otherwise do the setup later
        if (!isDynamicTableName && !isDynamicMaxValues && !setupComplete.get()) {
            super.setup(context);
        }
        ProcessSession session = sessionFactory.createSession();

        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            if (fileToProcess == null) {
                // Incoming connection with no FlowFile available, do no work (see capability description)
                return;
            }
        }
        maxValueProperties = getDefaultMaxValueProperties(context, fileToProcess);


        final ComponentLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final DatabaseDialectService databaseDialectService = getDatabaseDialectService(context);
        final String databaseType = context.getProperty(DB_TYPE).getValue();

        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(fileToProcess).getValue();
        final String columnNames = context.getProperty(COLUMN_NAMES).evaluateAttributeExpressions(fileToProcess).getValue();
        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions(fileToProcess).getValue();
        final int partitionSize = context.getProperty(PARTITION_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final String columnForPartitioning = context.getProperty(COLUMN_FOR_VALUE_PARTITIONING).evaluateAttributeExpressions(fileToProcess).getValue();
        final boolean useColumnValsForPaging = !StringUtils.isEmpty(columnForPartitioning);
        final String customWhereClause = context.getProperty(WHERE_CLAUSE).evaluateAttributeExpressions(fileToProcess).getValue();
        final String customOrderByColumn = context.getProperty(CUSTOM_ORDERBY_COLUMN).evaluateAttributeExpressions(fileToProcess).getValue();
        final boolean outputEmptyFlowFileOnZeroResults = context.getProperty(OUTPUT_EMPTY_FLOWFILE_ON_ZERO_RESULTS).asBoolean();

        final StateMap stateMap;
        FlowFile finalFileToProcess = fileToProcess;

        try {
            stateMap = session.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            logger.error("Failed to retrieve observed maximum values from the State Manager. Will not perform "
                    + "query until this is accomplished.", ioe);
            context.yield();
            return;
        }

        try {
            // Make a mutable copy of the current state property map. This will be updated by the result row callback, and eventually
            // set as the current state map (after the session has been committed)
            final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

            // If an initial max value for column(s) has been specified using properties, and this column is not in the state manager, sync them to the state property map
            for (final Map.Entry<String, String> maxProp : maxValueProperties.entrySet()) {
                String maxPropKey = maxProp.getKey().toLowerCase();
                String fullyQualifiedMaxPropKey = getStateKey(tableName, maxPropKey);
                if (!statePropertyMap.containsKey(fullyQualifiedMaxPropKey)) {
                    String newMaxPropValue;
                    // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                    // the value has been stored under a key that is only the column name. Fall back to check the column name,
                    // but store the new initial max value under the fully-qualified key.
                    if (statePropertyMap.containsKey(maxPropKey)) {
                        newMaxPropValue = statePropertyMap.get(maxPropKey);
                    } else {
                        newMaxPropValue = maxProp.getValue();
                    }
                    statePropertyMap.put(fullyQualifiedMaxPropKey, newMaxPropValue);
                }
            }

            // Build a WHERE clause with maximum-value columns (if they exist), and a list of column names that will contain MAX(<column>) aliases. The
            // executed SQL query will retrieve the count of all records after the filter(s) have been applied, as well as the new maximum values for the
            // specified columns. This allows the processor to generate the correctly partitioned SQL statements as well as to update the state with the
            // latest observed maximum values.
            List<String> maxValueColumnNameList = StringUtils.isEmpty(maxValueColumnNames)
                    ? new ArrayList<>(0)
                    : Arrays.asList(maxValueColumnNames.split("\\s*,\\s*"));
            final int numMaxValueColumns = maxValueColumnNameList.size();

            List<String> maxValueClauses = new ArrayList<>(numMaxValueColumns);
            Long maxValueForPartitioning = null;
            Long minValueForPartitioning = null;

            List<String> maxValueSelectColumns = new ArrayList<>(numMaxValueColumns + 1);

            // replace unnecessary row count with -1 stub value when column values for paging is used, or when partition size is zero.
            if (useColumnValsForPaging || partitionSize == 0) {
                maxValueSelectColumns.add("-1");
            } else {
                maxValueSelectColumns.add("COUNT(*)");
            }

            // For each maximum-value column, get a WHERE filter and a MAX(column) alias
            IntStream.range(0, numMaxValueColumns).forEach((index) -> {
                String colName = maxValueColumnNameList.get(index);

                maxValueSelectColumns.add("MAX(" + colName + ") " + colName);
                String maxValue = getColumnStateMaxValue(tableName, statePropertyMap, colName);
                if (!StringUtils.isEmpty(maxValue)) {
                    if (columnTypeMap.isEmpty() || getColumnType(tableName, colName) == null) {
                        // This means column type cache is clean after instance reboot. We should re-cache column type
                        super.setup(context, false, finalFileToProcess);
                    }
                    Integer type = getColumnType(tableName, colName);

                    // Add a condition for the WHERE clause
                    maxValueClauses.add(colName + (index == 0 ? " > " : " >= ") + getLiteralByType(type, maxValue, databaseType));
                }

            });

            // If we are using a columns' values, get the maximum and minimum values in the context of the aforementioned WHERE clause
            if (useColumnValsForPaging) {
                if (columnForPartitioning.contains(",")) {
                    throw new ProcessException(COLUMN_FOR_VALUE_PARTITIONING.getDisplayName() + " requires a single column name, but a comma was detected");
                }
                maxValueSelectColumns.add("MAX(" + columnForPartitioning + ") " + columnForPartitioning);
                maxValueSelectColumns.add("MIN(" + columnForPartitioning + ") MIN_" + columnForPartitioning);
            }

            if (customWhereClause != null) {
                // adding the custom WHERE clause (if defined) to the list of existing clauses.
                maxValueClauses.add("(" + customWhereClause + ")");
            }

            final String maxWhereClause = StringUtils.join(maxValueClauses, " AND ");
            final QueryStatementRequest queryStatementRequest = getMaxColumnStatementRequest(tableName, maxValueSelectColumns, maxWhereClause);

            // Build a SELECT query with maximum-value columns (if present)
            final StatementResponse statementResponse = databaseDialectService.getStatement(queryStatementRequest);
            final String selectQuery = statementResponse.sql();
            long rowCount;

            try (final Connection con = dbcpService.getConnection(finalFileToProcess == null ? Collections.emptyMap() : finalFileToProcess.getAttributes());
                 final Statement st = con.createStatement()) {

                final int queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asTimePeriod(TimeUnit.SECONDS).intValue();
                st.setQueryTimeout(queryTimeout); // timeout in seconds

                logger.debug("Executing {}", selectQuery);
                ResultSet resultSet;

                resultSet = st.executeQuery(selectQuery);

                if (resultSet.next()) {
                    // Total row count is in the first column
                    rowCount = resultSet.getLong(1);

                    // Update the state map with the newly-observed maximum values
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    int i = 2;
                    for (; i <= numMaxValueColumns + 1; i++) {
                        //Some JDBC drivers consider the columns name and label to be very different things.
                        // Since this column has been aliased lets check the label first,
                        // if there is no label we'll use the column name.
                        String resultColumnName = (StringUtils.isNotEmpty(rsmd.getColumnLabel(i)) ? rsmd.getColumnLabel(i) : rsmd.getColumnName(i)).toLowerCase();
                        String fullyQualifiedStateKey = getStateKey(tableName, resultColumnName);
                        String resultColumnCurrentMax = statePropertyMap.get(fullyQualifiedStateKey);
                        if (StringUtils.isEmpty(resultColumnCurrentMax) && !isDynamicTableName) {
                            // If we can't find the value at the fully-qualified key name and the table name is static, it is possible (under a previous scheme)
                            // the value has been stored under a key that is only the column name. Fall back to check the column name; either way, when a new
                            // maximum value is observed, it will be stored under the fully-qualified key from then on.
                            resultColumnCurrentMax = statePropertyMap.get(resultColumnName);
                        }

                        int type = rsmd.getColumnType(i);
                        if (isDynamicTableName) {
                            // We haven't pre-populated the column type map if the table name is dynamic, so do it here
                            columnTypeMap.put(fullyQualifiedStateKey, type);
                        }
                        try {
                            String newMaxValue = getMaxValueFromRow(resultSet, i, type, resultColumnCurrentMax);
                            if (newMaxValue != null) {
                                statePropertyMap.put(fullyQualifiedStateKey, newMaxValue);
                            }
                        } catch (ParseException | IOException | ClassCastException pice) {
                            // Fail the whole thing here before we start creating FlowFiles and such
                            throw new ProcessException(pice);
                        }
                    }
                    // Process the maximum and minimum values for the partitioning column if necessary
                    // These are currently required to be Long values, will throw a ClassCastException if they are not
                    if (useColumnValsForPaging) {
                        Object o = resultSet.getObject(i);
                        maxValueForPartitioning = o == null ? null : Long.valueOf(o.toString());
                        o = resultSet.getObject(i + 1);
                        minValueForPartitioning = o == null ? null : Long.valueOf(o.toString());
                    }
                } else {
                    // Something is very wrong here, one row (even if count is zero) should be returned
                    throw new SQLException("No rows returned from metadata query: " + selectQuery);
                }

                // for each maximum-value column get a right bounding WHERE condition
                IntStream.range(0, numMaxValueColumns).forEach((index) -> {
                    String colName = maxValueColumnNameList.get(index);

                    String maxValue = getColumnStateMaxValue(tableName, statePropertyMap, colName);
                    if (!StringUtils.isEmpty(maxValue)) {
                        if (columnTypeMap.isEmpty() || getColumnType(tableName, colName) == null) {
                            // This means column type cache is clean after instance reboot. We should re-cache column type
                            super.setup(context, false, finalFileToProcess);
                        }
                        Integer type = getColumnType(tableName, colName);

                        // Add a condition for the WHERE clause
                        maxValueClauses.add(colName + " <= " + getLiteralByType(type, maxValue, databaseType));
                    }
                });

                final long numberOfFetches;
                if (useColumnValsForPaging) {
                    final long valueRangeSize = maxValueForPartitioning == null ? 0 : (maxValueForPartitioning - minValueForPartitioning + 1);
                    numberOfFetches = (partitionSize == 0) ? 1 : (valueRangeSize / partitionSize) + (valueRangeSize % partitionSize == 0 ? 0 : 1);
                } else {
                    numberOfFetches = (partitionSize == 0) ? 1 : (rowCount / partitionSize) + (rowCount % partitionSize == 0 ? 0 : 1);
                }

                // Generate SQL statements to read "pages" of data
                final String fragmentIdentifier = UUID.randomUUID().toString();
                List<FlowFile> flowFilesToTransfer = new ArrayList<>();

                Map<String, String> baseAttributes = new HashMap<>();
                baseAttributes.put("generatetablefetch.tableName", tableName);
                if (columnNames != null) {
                    baseAttributes.put("generatetablefetch.columnNames", columnNames);
                }

                final String maxColumnNames = StringUtils.join(maxValueColumnNameList, ", ");
                if (StringUtils.isNotBlank(maxColumnNames)) {
                    baseAttributes.put("generatetablefetch.maxColumnNames", maxColumnNames);
                }

                baseAttributes.put(FRAGMENT_ID, fragmentIdentifier);
                baseAttributes.put(FRAGMENT_COUNT, String.valueOf(numberOfFetches));

                // If there are no SQL statements to be generated, still output an empty FlowFile if specified by the user
                if (numberOfFetches == 0 && outputEmptyFlowFileOnZeroResults) {
                    FlowFile emptyFlowFile = (fileToProcess == null) ? session.create() : session.create(fileToProcess);
                    Map<String, String> attributesToAdd = new HashMap<>();

                    final String fetchWhereClause = maxValueClauses.isEmpty() ? "1=1" : StringUtils.join(maxValueClauses, " AND ");
                    attributesToAdd.put("generatetablefetch.whereClause", fetchWhereClause);

                    attributesToAdd.put("generatetablefetch.limit", null);
                    if (partitionSize != 0) {
                        attributesToAdd.put("generatetablefetch.offset", null);
                    }
                    // Add fragment attributes
                    attributesToAdd.put(FRAGMENT_INDEX, String.valueOf(0));

                    attributesToAdd.putAll(baseAttributes);
                    emptyFlowFile = session.putAllAttributes(emptyFlowFile, attributesToAdd);
                    flowFilesToTransfer.add(emptyFlowFile);
                } else {
                    Long limit = partitionSize == 0 ? null : (long) partitionSize;
                    for (long i = 0; i < numberOfFetches; i++) {
                        // Add a right bounding for the partitioning column if necessary (only on last partition, meaning we don't need the limit)
                        if ((i == numberOfFetches - 1) && useColumnValsForPaging && (maxValueClauses.isEmpty() || customWhereClause != null)) {
                            maxValueClauses.add(columnForPartitioning + " <= " + maxValueForPartitioning);
                            limit = null;
                        }

                        //Update WHERE list to include new right hand boundaries
                        final String whereClause = maxValueClauses.isEmpty() ? "1=1" : StringUtils.join(maxValueClauses, " AND ");
                        Long offset = partitionSize == 0 ? null : i * partitionSize + (useColumnValsForPaging ? minValueForPartitioning : 0);
                        // Don't use an ORDER BY clause if there's only one partition
                        final String orderByClause = partitionSize == 0 ? null : (maxColumnNames.isEmpty() ? customOrderByColumn : maxColumnNames);

                        final List<String> namedColumns;
                        if (columnNames == null) {
                            namedColumns = List.of();
                        } else {
                            namedColumns = Arrays.asList(columnNames.split(", "));
                        }

                        final QueryStatementRequest selectStatementRequest = getSelectStatementRequest(tableName, namedColumns, whereClause, orderByClause, offset, limit, columnForPartitioning);
                        final StatementResponse selectStatementResponse = databaseDialectService.getStatement(selectStatementRequest);
                        final String query = selectStatementResponse.sql();

                        FlowFile sqlFlowFile = (fileToProcess == null) ? session.create() : session.create(fileToProcess);
                        sqlFlowFile = session.write(sqlFlowFile, out -> out.write(query.getBytes()));
                        Map<String, String> attributesToAdd = new HashMap<>();

                        attributesToAdd.put("generatetablefetch.whereClause", whereClause);
                        attributesToAdd.put("generatetablefetch.limit", (limit == null) ? null : limit.toString());
                        if (partitionSize != 0) {
                            attributesToAdd.put("generatetablefetch.offset", String.valueOf(offset));
                        }
                        // Add fragment attributes
                        attributesToAdd.put(FRAGMENT_INDEX, String.valueOf(i));

                        attributesToAdd.putAll(baseAttributes);
                        sqlFlowFile = session.putAllAttributes(sqlFlowFile, attributesToAdd);
                        flowFilesToTransfer.add(sqlFlowFile);
                    }
                }

                session.transfer(flowFilesToTransfer, REL_SUCCESS);

                if (fileToProcess != null) {
                    session.remove(fileToProcess);
                }
            } catch (SQLException e) {
                if (fileToProcess != null) {
                    logger.error("Routing {} to failure since unable to execute SQL select query {}", fileToProcess, selectQuery, e);
                    fileToProcess = session.putAttribute(fileToProcess, "generatetablefetch.sql.error", e.getMessage());
                    session.transfer(fileToProcess, REL_FAILURE);

                } else {
                    logger.error("Unable to execute SQL select query {}", selectQuery, e);
                    throw new ProcessException(e);
                }
            }

            try {
                // Update the state
                session.setState(statePropertyMap, Scope.CLUSTER);
            } catch (IOException ioe) {
                logger.error("{} failed to update State Manager, observed maximum values will not be recorded. "
                                + "Also, any generated SQL statements may be duplicated.", this, ioe);
            }

            session.commitAsync();

        } catch (final ProcessException pe) {
            // Log the cause of the ProcessException if it is available
            Throwable t = (pe.getCause() == null ? pe : pe.getCause());
            logger.error("Error during processing: {}", t.getMessage(), t);
            session.rollback();
            context.yield();
        }
    }

    private QueryStatementRequest getMaxColumnStatementRequest(final String tableName, final List<String> maxValueSelectColumns, final String whereClause) {
        final List<ColumnDefinition> maxValueColumns = maxValueSelectColumns.stream()
                .map(StandardColumnDefinition::new)
                .map(ColumnDefinition.class::cast)
                .toList();
        final TableDefinition tableDefinition = new TableDefinition(Optional.empty(), Optional.empty(), tableName, maxValueColumns);
        return new StandardQueryStatementRequest(
                StatementType.SELECT,
                tableDefinition,
                Optional.empty(),
                List.of(new QueryClause(QueryClauseType.WHERE, whereClause)),
                Optional.empty()
        );
    }

    private QueryStatementRequest getSelectStatementRequest(
            final String tableName,
            final List<String> namedColumns,
            final String whereClause,
            final String orderByClause,
            final Long offset,
            final Long limit,
            final String indexColumnName
    ) {
        final List<ColumnDefinition> maxValueColumns = (namedColumns).stream()
                .map(StandardColumnDefinition::new)
                .map(ColumnDefinition.class::cast)
                .toList();
        final TableDefinition tableDefinition = new TableDefinition(Optional.empty(), Optional.empty(), tableName, maxValueColumns);

        final List<QueryClause> queryClauses = new ArrayList<>();
        if (orderByClause != null) {
            final QueryClause queryClause = new QueryClause(QueryClauseType.ORDER_BY, orderByClause);
            queryClauses.add(queryClause);
        }
        final QueryClause whereQueryClause = new QueryClause(QueryClauseType.WHERE, whereClause);
        queryClauses.add(whereQueryClause);

        final PageRequest pageRequest;
        if (offset == null) {
            pageRequest = null;
        } else {
            final OptionalLong pageLimit = limit == null ? OptionalLong.empty() : OptionalLong.of(limit);
            pageRequest = new StandardPageRequest(offset, pageLimit, Optional.ofNullable(indexColumnName));
        }

        return new StandardQueryStatementRequest(
                StatementType.SELECT,
                tableDefinition,
                Optional.empty(),
                queryClauses,
                Optional.ofNullable(pageRequest)
        );
    }

    private String getColumnStateMaxValue(String tableName, Map<String, String> statePropertyMap, String colName) {
        final String fullyQualifiedStateKey = getStateKey(tableName, colName);
        String maxValue = statePropertyMap.get(fullyQualifiedStateKey);
        if (StringUtils.isEmpty(maxValue) && !isDynamicTableName) {
            // If the table name is static and the fully-qualified key was not found, try just the column name
            maxValue = statePropertyMap.get(getStateKey(null, colName));
        }

        return maxValue;
    }

    private Integer getColumnType(String tableName, String colName) {
        final String fullyQualifiedStateKey = getStateKey(tableName, colName);
        Integer type = columnTypeMap.get(fullyQualifiedStateKey);
        if (type == null && !isDynamicTableName) {
            // If the table name is static and the fully-qualified key was not found, try just the column name
            type = columnTypeMap.get(getStateKey(null, colName));
        }

        return type;
    }
}
