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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.standard.sql.DefaultAvroSqlWriter;
import org.apache.nifi.processors.standard.sql.SqlWriter;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.db.JdbcCommon;

import java.util.List;
import java.util.Set;

import static org.apache.nifi.util.db.JdbcProperties.NORMALIZE_NAMES_FOR_AVRO;
import static org.apache.nifi.util.db.JdbcProperties.USE_AVRO_LOGICAL_TYPES;
import static org.apache.nifi.util.db.JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION;
import static org.apache.nifi.util.db.JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE;


@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "select", "jdbc", "query", "database"})
@SeeAlso({GenerateTableFetch.class, ExecuteSQL.class})
@CapabilityDescription("Generates a SQL select query, or uses a provided statement, and executes it to fetch all rows whose values in the specified "
        + "Maximum Value column(s) are larger than the "
        + "previously-seen maxima. Query result will be converted to Avro format. Expression Language is supported for several properties, but no incoming "
        + "connections are permitted. The Environment/System properties may be used to provide values for any property containing Expression Language. If it is desired to "
        + "leverage flow file attributes to perform these queries, the GenerateTableFetch and/or ExecuteSQL processors can be used for this purpose. "
        + "Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer or cron expression, using the standard scheduling methods. This processor is intended to be run on the Primary Node only. FlowFile attribute "
        + "'querydbtable.row.count' indicates how many rows were selected.")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a query on the specified table, the maximum values for "
        + "the specified column(s) will be retained for use in future executions of the query. This allows the Processor "
        + "to fetch only those records that have max values greater than the retained values. This can be used for "
        + "incremental fetching, fetching of newly added rows, etc. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation")
@WritesAttributes({
        @WritesAttribute(attribute = "tablename", description = "Name of the table being queried"),
        @WritesAttribute(attribute = "querydbtable.row.count", description = "The number of rows selected by the query"),
        @WritesAttribute(attribute = "fragment.identifier", description = "If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet. If Output Batch Size is set, then this "
                + "attribute will not be populated."),
        @WritesAttribute(attribute = "fragment.index", description = "If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute = "maxvalue.*", description = "Each attribute contains the observed maximum value of a specified 'Maximum-value Column'. The "
                + "suffix of the attribute is the name of the column. If Output Batch Size is set, then this attribute will not be populated.")})
@DynamicProperty(name = "initial.maxvalue.<max_value_column>", value = "Initial maximum value for the specified column",
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT, description = "Specifies an initial max value for max value column(s). Properties should "
        + "be added in the format `initial.maxvalue.<max_value_column>`. This value is only used the first time the table is accessed (when a Maximum Value Column is specified).")
@PrimaryNodeOnly
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class QueryDatabaseTable extends AbstractQueryDatabaseTable {

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractDatabaseFetchProcessor.TABLE_NAME)
            .description("The name of the database table to be queried. When a custom query is used, this property is used to alias the query and appears as an attribute on the FlowFile.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            DBCP_SERVICE,
            DB_TYPE,
            DATABASE_DIALECT_SERVICE,
            TABLE_NAME,
            COLUMN_NAMES,
            WHERE_CLAUSE,
            SQL_QUERY,
            MAX_VALUE_COLUMN_NAMES,
            INITIAL_LOAD_STRATEGY,
            QUERY_TIMEOUT,
            FETCH_SIZE,
            AUTO_COMMIT,
            MAX_ROWS_PER_FLOW_FILE,
            OUTPUT_BATCH_SIZE,
            MAX_FRAGMENTS,
            NORMALIZE_NAMES_FOR_AVRO,
            TRANS_ISOLATION_LEVEL,
            USE_AVRO_LOGICAL_TYPES,
            VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION,
            VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    public QueryDatabaseTable() {
        relationships = RELATIONSHIPS;
        propDescriptors = PROPERTIES;
    }

    @Override
    protected SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context) {
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        final boolean convertNamesForAvro = context.getProperty(NORMALIZE_NAMES_FOR_AVRO).asBoolean();
        final Boolean useAvroLogicalTypes = context.getProperty(USE_AVRO_LOGICAL_TYPES).asBoolean();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Integer defaultPrecision = context.getProperty(VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION).evaluateAttributeExpressions().asInteger();
        final Integer defaultScale = context.getProperty(VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE).evaluateAttributeExpressions().asInteger();

        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions.builder()
                .recordName(tableName)
                .convertNames(convertNamesForAvro)
                .useLogicalTypes(useAvroLogicalTypes)
                .defaultPrecision(defaultPrecision)
                .defaultScale(defaultScale)
                .maxRows(maxRowsPerFlowFile)
                .build();
        return new DefaultAvroSqlWriter(options);
    }
}
