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
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.database.dialect.service.api.ColumnDefinition;
import org.apache.nifi.database.dialect.service.api.StandardColumnDefinition;
import org.apache.nifi.database.dialect.service.api.DatabaseDialectService;
import org.apache.nifi.database.dialect.service.api.QueryStatementRequest;
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
import org.apache.nifi.processors.standard.sql.SqlWriter;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.db.JdbcCommon;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public abstract class AbstractQueryDatabaseTable extends AbstractDatabaseFetchProcessor {

    public static final String RESULT_TABLENAME = "tablename";
    public static final String RESULT_ROW_COUNT = "querydbtable.row.count";

    private static final AllowableValue TRANSACTION_READ_COMMITTED = new AllowableValue(
            String.valueOf(Connection.TRANSACTION_READ_COMMITTED),
            "TRANSACTION_READ_COMMITTED"
    );
    private static final AllowableValue TRANSACTION_READ_UNCOMMITTED = new AllowableValue(
            String.valueOf(Connection.TRANSACTION_READ_UNCOMMITTED),
            "TRANSACTION_READ_UNCOMMITTED"
    );
    private static final AllowableValue TRANSACTION_REPEATABLE_READ = new AllowableValue(
            String.valueOf(Connection.TRANSACTION_REPEATABLE_READ),
            "TRANSACTION_REPEATABLE_READ"
    );
    private static final AllowableValue TRANSACTION_NONE =  new AllowableValue(
            String.valueOf(Connection.TRANSACTION_NONE),
            "TRANSACTION_NONE"
    );
    private static final AllowableValue TRANSACTION_SERIALIZABLE = new AllowableValue(
            String.valueOf(Connection.TRANSACTION_SERIALIZABLE),
            "TRANSACTION_SERIALIZABLE"
    );

    private static final String FETCH_SIZE_NAME = "Fetch Size";
    private static final String AUTO_COMMIT_NAME = "Set Auto Commit";

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name(FETCH_SIZE_NAME)
            .description("The number of result rows to be fetched from the result set at a time. This is a hint to the database driver and may not be "
                    + "honored and/or exact. If the value specified is zero, then the hint is ignored. "
                    + "If using PostgreSQL, then '" + AUTO_COMMIT_NAME + "' must be equal to 'false' to cause '" + FETCH_SIZE_NAME + "' to take effect.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
            .name(AUTO_COMMIT_NAME)
            .description("Allows enabling or disabling the auto commit functionality of the DB connection. Default value is 'No value set'. " +
                    "'No value set' will leave the db connection's auto commit mode unchanged. " +
                    "For some JDBC drivers such as PostgreSQL driver, it is required to disable the auto commit functionality " +
                    "to get the '" + FETCH_SIZE_NAME + "' setting to take effect. " +
                    "When auto commit is enabled, PostgreSQL driver ignores '" + FETCH_SIZE_NAME + "' setting and loads all rows of the result set to memory at once. " +
                    "This could lead for a large amount of memory usage when executing queries which fetch large data sets. " +
                    "More Details of this behaviour in PostgreSQL driver can be found in https://jdbc.postgresql.org//documentation/head/query.html.")
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("qdbt-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("qdbt-output-batch-size")
            .displayName("Output Batch Size")
            .description("The number of output FlowFiles to queue before committing the process session. When set to zero, the session will be committed when all result set rows "
                    + "have been processed and the output FlowFiles are ready for transfer to the downstream relationship. For large result sets, this can cause a large burst of FlowFiles "
                    + "to be transferred at the end of processor execution. If this property is set, then when the specified number of FlowFiles are ready for transfer, then the session will "
                    + "be committed, thus releasing the FlowFiles to the downstream relationship. NOTE: The maxvalue.* and fragment.count attributes will not be set on FlowFiles when this "
                    + "property is set.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAX_FRAGMENTS = new PropertyDescriptor.Builder()
            .name("qdbt-max-frags")
            .displayName("Maximum Number of Fragments")
            .description("The maximum number of fragments. If the value specified is zero, then all fragments are returned. " +
                    "This prevents OutOfMemoryError when this processor ingests huge table. NOTE: Setting this property can result in data loss, as the incoming results are "
                    + "not ordered, and fragments may end at arbitrary boundaries where rows are not included in the result set.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor TRANS_ISOLATION_LEVEL = new PropertyDescriptor.Builder()
            .name("transaction-isolation-level")
            .displayName("Transaction Isolation Level")
            .description("This setting will set the transaction isolation level for the database connection for drivers that support this setting")
            .required(false)
            .allowableValues(TRANSACTION_NONE, TRANSACTION_READ_COMMITTED, TRANSACTION_READ_UNCOMMITTED, TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE)
            .build();

    public static final AllowableValue INITIAL_LOAD_STRATEGY_ALL_ROWS = new AllowableValue("Start at Beginning", "Start at Beginning", "Loads all existing rows from the database table.");
    public static final AllowableValue INITIAL_LOAD_STRATEGY_NEW_ROWS = new AllowableValue("Start at Current Maximum Values", "Start at Current Maximum Values", "Loads only the newly " +
            "inserted or updated rows based on the maximum value(s) of the column(s) configured in the '" + MAX_VALUE_COLUMN_NAMES.getDisplayName() + "' property.");

    public static final PropertyDescriptor INITIAL_LOAD_STRATEGY = new PropertyDescriptor.Builder()
            .name("initial-load-strategy")
            .displayName("Initial Load Strategy")
            .description("How to handle existing rows in the database table when the processor is started for the first time (or its state has been cleared). The property will be ignored, " +
                    "if any '" + INITIAL_MAX_VALUE_PROP_START + "*' dynamic property has also been configured.")
            .required(true)
            .allowableValues(INITIAL_LOAD_STRATEGY_ALL_ROWS, INITIAL_LOAD_STRATEGY_NEW_ROWS)
            .defaultValue(INITIAL_LOAD_STRATEGY_ALL_ROWS.getValue())
            .build();

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
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final boolean maxValueColumnNames = validationContext.getProperty(MAX_VALUE_COLUMN_NAMES).isSet();
        final String initialLoadStrategy = validationContext.getProperty(INITIAL_LOAD_STRATEGY).getValue();
        if (!maxValueColumnNames && initialLoadStrategy.equals(INITIAL_LOAD_STRATEGY_NEW_ROWS.getValue())) {
            results.add(new ValidationResult.Builder().valid(false)
                    .subject(INITIAL_LOAD_STRATEGY.getDisplayName())
                    .input(INITIAL_LOAD_STRATEGY_NEW_ROWS.getDisplayName())
                    .explanation(String.format("'%s' strategy can only be used when '%s' property is also configured",
                            INITIAL_LOAD_STRATEGY_NEW_ROWS.getDisplayName(), MAX_VALUE_COLUMN_NAMES.getDisplayName()))
                    .build());
        }

        return results;
    }

    @Override
    @OnScheduled
    public void setup(final ProcessContext context) {
        maxValueProperties = getDefaultMaxValueProperties(context, null);
    }

    @OnStopped
    public void stop() {
        // Reset the column type map in case properties change
        setupComplete.set(false);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        // Fetch the column/table info once
        if (!setupComplete.get()) {
            super.setup(context);
        }
        ProcessSession session = sessionFactory.createSession();
        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();

        final ComponentLog logger = getLogger();

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final DatabaseDialectService databaseDialectService = getDatabaseDialectService(context);
        final String databaseType = context.getProperty(DB_TYPE).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        final String columnNames = context.getProperty(COLUMN_NAMES).evaluateAttributeExpressions().getValue();
        final String sqlQuery = context.getProperty(SQL_QUERY).evaluateAttributeExpressions().getValue();
        final String maxValueColumnNames = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions().getValue();
        final String initialLoadStrategy = context.getProperty(INITIAL_LOAD_STRATEGY).getValue();
        final String customWhereClause = context.getProperty(WHERE_CLAUSE).evaluateAttributeExpressions().getValue();
        final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS).intValue();
        final Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Integer outputBatchSizeField = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final int outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
        final Integer maxFragments = context.getProperty(MAX_FRAGMENTS).isSet()
                ? context.getProperty(MAX_FRAGMENTS).evaluateAttributeExpressions().asInteger()
                : 0;
        final Integer transIsolationLevel = context.getProperty(TRANS_ISOLATION_LEVEL).isSet()
                ? context.getProperty(TRANS_ISOLATION_LEVEL).asInteger()
                : null;

        SqlWriter sqlWriter = configureSqlWriter(session, context);

        final StateMap stateMap;
        try {
            stateMap = session.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve observed maximum values from the State Manager. Will not perform "
                    + "query until this is accomplished.", ioe);
            context.yield();
            return;
        }

        // Make a mutable copy of the current state property map. This will be updated by the result row callback, and eventually
        // set as the current state map (after the session has been committed)
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

        //If an initial max value for column(s) has been specified using properties, and this column is not in the state manager, sync them to the state property map
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

        List<String> maxValueColumnNameList = StringUtils.isEmpty(maxValueColumnNames)
                ? null
                : Arrays.asList(maxValueColumnNames.split("\\s*,\\s*"));

        if (maxValueColumnNameList != null && statePropertyMap.isEmpty() && initialLoadStrategy.equals(INITIAL_LOAD_STRATEGY_NEW_ROWS.getValue())) {
            final List<ColumnDefinition> maxValueColumnDefinitions = maxValueColumnNameList.stream()
                    .map(columnName -> String.format("MAX(%s) %s", columnName, columnName))
                    .map(StandardColumnDefinition::new)
                    .map(ColumnDefinition.class::cast)
                    .toList();
            final TableDefinition tableDefinition = new TableDefinition(Optional.empty(), Optional.empty(), tableName, maxValueColumnDefinitions);
            final QueryStatementRequest statementRequest = new StandardQueryStatementRequest(StatementType.SELECT, tableDefinition);
            final StatementResponse maxValueStatementResponse = databaseDialectService.getStatement(statementRequest);
            final String selectMaxQuery = maxValueStatementResponse.sql();

            try (final Connection con = dbcpService.getConnection(Collections.emptyMap());
                 final Statement st = con.createStatement()) {

                if (transIsolationLevel != null) {
                    con.setTransactionIsolation(transIsolationLevel);
                }

                st.setQueryTimeout(queryTimeout); // timeout in seconds

                try (final ResultSet resultSet = st.executeQuery(selectMaxQuery)) {
                    if (resultSet.next()) {
                        final MaxValueResultSetRowCollector maxValCollector = new MaxValueResultSetRowCollector(tableName, statePropertyMap);
                        maxValCollector.processRow(resultSet);
                        maxValCollector.applyStateChanges();
                    }
                }
            } catch (final Exception e) {
                logger.error("Unable to execute SQL select query {} due to {}", selectMaxQuery, e);
                context.yield();
            }
        }

        final List<String> parsedColumnNames;
        if (columnNames == null) {
            parsedColumnNames = List.of();
        } else {
            parsedColumnNames = Arrays.asList(columnNames.split(", "));
        }

        final String selectQuery = getQuery(databaseDialectService, databaseType, tableName, sqlQuery, parsedColumnNames, maxValueColumnNameList, customWhereClause, statePropertyMap);
        final StopWatch stopWatch = new StopWatch(true);
        final String fragmentIdentifier = UUID.randomUUID().toString();

        try (final Connection con = dbcpService.getConnection(Collections.emptyMap());
             final Statement st = con.createStatement()) {

            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (SQLException se) {
                    // Not all drivers support this, just log the error (at debug level) and move on
                    logger.debug("Cannot set fetch size to {} due to {}", fetchSize, se.getLocalizedMessage(), se);
                }
            }

            if (transIsolationLevel != null) {
                con.setTransactionIsolation(transIsolationLevel);
            }

            String jdbcURL = "DBCPService";
            try {
                DatabaseMetaData databaseMetaData = con.getMetaData();
                if (databaseMetaData != null) {
                    jdbcURL = databaseMetaData.getURL();
                }
            } catch (SQLException se) {
                // Ignore and use default JDBC URL. This shouldn't happen unless the driver doesn't implement getMetaData() properly
            }

            st.setQueryTimeout(queryTimeout); // timeout in seconds
            if (logger.isDebugEnabled()) {
                logger.debug("Executing query {}", selectQuery);
            }

            final boolean originalAutoCommit = con.getAutoCommit();
            final Boolean setAutoCommitValue = context.getProperty(AUTO_COMMIT).evaluateAttributeExpressions().asBoolean();
            // If user sets AUTO_COMMIT property to non-null (i.e. true or false), then the property value overrides the dbAdapter's value
            if (setAutoCommitValue != null && originalAutoCommit != setAutoCommitValue) {
                try {
                    con.setAutoCommit(setAutoCommitValue);
                    logger.debug("Driver connection changed to setAutoCommit({})", setAutoCommitValue);
                } catch (Exception ex) {
                    logger.debug("Failed to setAutoCommit({}) due to {}: {}",
                            setAutoCommitValue, ex.getClass().getName(), ex.getMessage());
                }
            }

            try (final ResultSet resultSet = st.executeQuery(selectQuery)) {
                int fragmentIndex = 0;
                // Max values will be updated in the state property map by the callback
                final MaxValueResultSetRowCollector maxValCollector = new MaxValueResultSetRowCollector(tableName, statePropertyMap);

                while (true) {
                    final AtomicLong nrOfRows = new AtomicLong(0L);

                    FlowFile fileToProcess = session.create();
                    try {
                        fileToProcess = session.write(fileToProcess, out -> {
                            try {
                                nrOfRows.set(sqlWriter.writeResultSet(resultSet, out, getLogger(), maxValCollector));
                            } catch (Exception e) {
                                throw new ProcessException("Error during database query or conversion of records.", e);
                            }
                        });
                    } catch (ProcessException e) {
                        // Add flowfile to results before rethrowing so it will be removed from session in outer catch
                        resultSetFlowFiles.add(fileToProcess);
                        throw e;
                    }

                    if (nrOfRows.get() > 0) {
                        // set attributes
                        final Map<String, String> attributesToAdd = new HashMap<>();
                        attributesToAdd.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                        attributesToAdd.put(RESULT_TABLENAME, tableName);

                        if (maxRowsPerFlowFile > 0) {
                            attributesToAdd.put(FRAGMENT_ID, fragmentIdentifier);
                            attributesToAdd.put(FRAGMENT_INDEX, String.valueOf(fragmentIndex));
                        }

                        attributesToAdd.putAll(sqlWriter.getAttributesToAdd());
                        fileToProcess = session.putAllAttributes(fileToProcess, attributesToAdd);
                        sqlWriter.updateCounters(session);

                        logger.debug("{} contains {} records; transferring to 'success'",
                                fileToProcess, nrOfRows.get());

                        session.getProvenanceReporter().receive(fileToProcess, jdbcURL, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        resultSetFlowFiles.add(fileToProcess);
                        // If we've reached the batch size, send out the flow files
                        if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
                            session.transfer(resultSetFlowFiles, REL_SUCCESS);
                            session.commitAsync();
                            resultSetFlowFiles.clear();
                        }
                    } else {
                        // If there were no rows returned, don't send the flowfile
                        session.remove(fileToProcess);
                        // If no rows and this was first FlowFile, yield
                        if (fragmentIndex == 0) {
                            context.yield();
                        }
                        break;
                    }

                    fragmentIndex++;
                    if (maxFragments > 0 && fragmentIndex >= maxFragments) {
                        break;
                    }

                    // If we aren't splitting up the data into flow files or fragments, then the result set has been entirely fetched so don't loop back around
                    if (maxFragments == 0 && maxRowsPerFlowFile == 0) {
                        break;
                    }

                    // If we are splitting up the data into flow files, don't loop back around if we've gotten all results
                    if (maxRowsPerFlowFile > 0 && nrOfRows.get() < maxRowsPerFlowFile) {
                        break;
                    }
                }

                // Apply state changes from the Max Value tracker
                maxValCollector.applyStateChanges();

                // Even though the maximum value and total count are known at this point, to maintain consistent behavior if Output Batch Size is set, do not store the attributes
                if (outputBatchSize == 0) {
                    for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                        final Map<String, String> newAttributesMap = new HashMap<>();

                        // Add maximum values as attributes
                        for (Map.Entry<String, String> entry : statePropertyMap.entrySet()) {
                            // Get just the column name from the key
                            String key = entry.getKey();
                            String colName = key.substring(key.lastIndexOf(NAMESPACE_DELIMITER) + NAMESPACE_DELIMITER.length());
                            newAttributesMap.put("maxvalue." + colName, entry.getValue());
                        }

                        // Set count for all FlowFiles
                        if (maxRowsPerFlowFile > 0) {
                            newAttributesMap.put(FRAGMENT_COUNT, Integer.toString(fragmentIndex));
                        }

                        resultSetFlowFiles.set(i, session.putAllAttributes(resultSetFlowFiles.get(i), newAttributesMap));
                    }
                }
            } catch (final SQLException e) {
                throw e;
            } finally {
                if (con.getAutoCommit() != originalAutoCommit) {
                    try {
                        con.setAutoCommit(originalAutoCommit);
                        logger.debug("Driver connection reset to original setAutoCommit({})", originalAutoCommit);
                    } catch (Exception ex) {
                        logger.debug("Failed to setAutoCommit({}) due to {}: {}",
                                originalAutoCommit, ex.getClass().getName(), ex.getMessage());
                    }
                }
            }

            session.transfer(resultSetFlowFiles, REL_SUCCESS);

        } catch (final ProcessException | SQLException e) {
            logger.error("Unable to execute SQL select query {} due to {}", selectQuery, e);
            if (!resultSetFlowFiles.isEmpty()) {
                session.remove(resultSetFlowFiles);
            }
            context.yield();
        } finally {
            try {
                // Update the state
                session.setState(statePropertyMap, Scope.CLUSTER);
            } catch (IOException ioe) {
                getLogger().error("{} failed to update State Manager, maximum observed values will not be recorded", this, ioe);
            }

            session.commitAsync();
        }
    }

    private String getQuery(
            final DatabaseDialectService databaseDialectService,
            final String databaseType,
            final String tableName,
            final String sqlQuery,
            final List<String> columnNames,
            final List<String> maxValColumnNames,
            final String customWhereClause,
            final Map<String, String> stateMap
    ) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("Table name must be specified");
        }

        final Optional<String> derivedTableQuery = Optional.ofNullable(sqlQuery);

        final List<ColumnDefinition> columnDefinitions = columnNames.stream()
                .map(StandardColumnDefinition::new)
                .map(ColumnDefinition.class::cast)
                .toList();
        final TableDefinition tableDefinition = new TableDefinition(Optional.empty(), Optional.empty(), tableName, columnDefinitions);
        final QueryStatementRequest statementRequest = new StandardQueryStatementRequest(StatementType.SELECT, tableDefinition, derivedTableQuery, List.of(), Optional.empty());
        final StatementResponse statementResponse = databaseDialectService.getStatement(statementRequest);

        final StringBuilder query = new StringBuilder();
        query.append(statementResponse.sql());

        List<String> whereClauses = new ArrayList<>();
        // Check state map for last max values
        if (stateMap != null && !stateMap.isEmpty() && maxValColumnNames != null) {
            IntStream.range(0, maxValColumnNames.size()).forEach((index) -> {
                String colName = maxValColumnNames.get(index);
                String maxValueKey = getStateKey(tableName, colName);
                String maxValue = stateMap.get(maxValueKey);
                if (StringUtils.isEmpty(maxValue)) {
                    // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                    // the value has been stored under a key that is only the column name. Fall back to check the column name; either way, when a new
                    // maximum value is observed, it will be stored under the fully-qualified key from then on.
                    maxValue = stateMap.get(colName.toLowerCase());
                }
                if (!StringUtils.isEmpty(maxValue)) {
                    Integer type = columnTypeMap.get(maxValueKey);
                    if (type == null) {
                        // This shouldn't happen as we are populating columnTypeMap when the processor is scheduled.
                        throw new IllegalArgumentException("No column type found for: " + colName);
                    }
                    // Add a condition for the WHERE clause
                    whereClauses.add(colName + (index == 0 ? " > " : " >= ") + getLiteralByType(type, maxValue, databaseType));
                }
            });
        }

        if (customWhereClause != null) {
            whereClauses.add("(" + customWhereClause + ")");
        }

        if (!whereClauses.isEmpty()) {
            query.append(" WHERE ");
            query.append(StringUtils.join(whereClauses, " AND "));
        }

        return query.toString();
    }

    public class MaxValueResultSetRowCollector implements JdbcCommon.ResultSetRowCallback {
        final Map<String, String> newColMap;
        final Map<String, String> originalState;
        String tableName;

        public MaxValueResultSetRowCollector(String tableName, Map<String, String> stateMap) {
            this.originalState = stateMap;

            this.newColMap = new HashMap<>();
            this.newColMap.putAll(stateMap);

            this.tableName = tableName;
        }

        @Override
        public void processRow(ResultSet resultSet) throws IOException {
            if (resultSet == null) {
                return;
            }
            try {
                // Iterate over the row, check-and-set max values
                final ResultSetMetaData meta = resultSet.getMetaData();
                final int nrOfColumns = meta.getColumnCount();
                if (nrOfColumns > 0) {
                    for (int i = 1; i <= nrOfColumns; i++) {
                        String colName = meta.getColumnName(i).toLowerCase();
                        String fullyQualifiedMaxValueKey = getStateKey(tableName, colName);
                        Integer type = columnTypeMap.get(fullyQualifiedMaxValueKey);
                        // Skip any columns we're not keeping track of or whose value is null
                        if (type == null || resultSet.getObject(i) == null) {
                            continue;
                        }
                        String maxValueString = newColMap.get(fullyQualifiedMaxValueKey);
                        // If we can't find the value at the fully-qualified key name, it is possible (under a previous scheme)
                        // the value has been stored under a key that is only the column name. Fall back to check the column name; either way, when a new
                        // maximum value is observed, it will be stored under the fully-qualified key from then on.
                        if (StringUtils.isEmpty(maxValueString)) {
                            maxValueString = newColMap.get(colName);
                        }
                        String newMaxValueString = getMaxValueFromRow(resultSet, i, type, maxValueString);
                        if (newMaxValueString != null) {
                            newColMap.put(fullyQualifiedMaxValueKey, newMaxValueString);
                        }
                    }
                }
            } catch (ParseException | SQLException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void applyStateChanges() {
            this.originalState.putAll(this.newColMap);
        }
    }

    protected abstract SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context);
}
