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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.calcite.RecordPathFunctions;
import org.apache.nifi.processors.standard.calcite.RecordResultSetOutputStreamCallback;
import org.apache.nifi.queryrecord.RecordDataSource;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.sql.CalciteDatabase;
import org.apache.nifi.sql.NiFiTable;
import org.apache.nifi.sql.NiFiTableSchema;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.Tuple;

import java.io.Closeable;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.nifi.util.db.JdbcProperties.DEFAULT_PRECISION;
import static org.apache.nifi.util.db.JdbcProperties.DEFAULT_SCALE;

@SideEffectFree
@SupportsBatching
@Tags({"sql", "query", "calcite", "route", "record", "transform", "select", "update", "modify", "etl", "filter", "record", "csv", "json", "logs", "text", "avro", "aggregate"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Evaluates one or more SQL queries against the contents of a FlowFile. The result of the "
    + "SQL query then becomes the content of the output FlowFile. This can be used, for example, "
    + "for field-specific filtering, transformation, and row-level filtering. "
    + "Columns can be renamed, simple calculations and aggregations performed, etc. "
    + "The Processor is configured with a Record Reader Controller Service and a Record Writer service so as to allow flexibility in incoming and outgoing data formats. "
    + "The Processor must be configured with at least one user-defined property. The name of the Property "
    + "is the Relationship to route data to, and the value of the Property is a SQL SELECT statement that is used to specify how input data should be transformed/filtered. "
    + "The SQL statement must be valid ANSI SQL and is powered by Apache Calcite. "
    + "If the transformation fails, the original FlowFile is routed to the 'failure' relationship. Otherwise, the data selected will be routed to the associated "
    + "relationship. If the Record Writer chooses to inherit the schema from the Record, it is important to note that the schema that is inherited will be from the "
    + "ResultSet, rather than the input Record. This allows a single instance of the QueryRecord processor to have multiple queries, each of which returns a different "
    + "set of columns and aggregations. As a result, though, the schema that is derived will have no schema name, so it is important that the configured Record Writer not attempt "
    + "to write the Schema Name as an attribute if inheriting the Schema from the Record. See the Processor Usage documentation for more information.")
@DynamicRelationship(name = "<Property Name>", description = "Each user-defined property defines a new Relationship for this Processor.")
@DynamicProperty(name = "The name of the relationship to route data to",
                 value = "A SQL SELECT statement that is used to determine what data should be routed to this relationship.",
                 expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                 description = "Each user-defined property specifies a SQL SELECT statement to run over the data, with the data "
                         + "that is selected being routed to the relationship whose name is the property name")
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
    @WritesAttribute(attribute = "record.count", description = "The number of records selected by the query"),
    @WritesAttribute(attribute = QueryRecord.ROUTE_ATTRIBUTE_KEY, description = "The relation to which the FlowFile was routed")
})
@UseCase(
    description = "Filter out records based on the values of the records' fields",
    keywords = {"filter out", "remove", "drop", "strip out", "record field", "sql"},
    configuration = """
        "Record Reader" should be set to a Record Reader that is appropriate for your data.
        "Record Writer" should be set to a Record Writer that writes out data in the desired format.

        One additional property should be added.
        The name of the property should be a short description of the data to keep.
        Its value is a SQL statement that selects all columns from a table named `FLOW_FILE` for relevant rows.
        The WHERE clause selects the data to keep. I.e., it is the exact opposite of what we want to remove.
        It is recommended to always quote column names using double-quotes in order to avoid conflicts with SQL keywords.
        For example, to remove records where either the name is George OR the age is less than 18, we would add a \
          property named "adults not george" with a value that selects records where the name is not George AND the age is greater than or equal to 18. \
          So the value would be `SELECT * FROM FLOWFILE WHERE "name" <> 'George' AND "age" >= 18`

        Adding this property now gives us a new Relationship whose name is the same as the property name. So, the "adults not george" Relationship \
        should be connected to the next Processor in our flow.
        """
)
@UseCase(
    description = "Keep only specific records",
    keywords = {"keep", "filter", "retain", "select", "include", "record", "sql"},
    configuration = """
        "Record Reader" should be set to a Record Reader that is appropriate for your data.
        "Record Writer" should be set to a Record Writer that writes out data in the desired format.

        One additional property should be added.
        The name of the property should be a short description of the data to keep.
        Its value is a SQL statement that selects all columns from a table named `FLOW_FILE` for relevant rows.
        The WHERE clause selects the data to keep.
        It is recommended to always quote column names using double-quotes in order to avoid conflicts with SQL keywords.
        For example, to keep only records where the person is an adult (aged 18 or older), add a property named "adults" \
          with a value that is a SQL statement that selects records where the age is at least 18. \
          So the value would be `SELECT * FROM FLOWFILE WHERE "age" >= 18`

        Adding this property now gives us a new Relationship whose name is the same as the property name. So, the "adults" Relationship \
        should be connected to the next Processor in our flow.
        """
)
@UseCase(
    description = "Keep only specific fields in a a Record, where the names of the fields to keep are known",
    keywords = {"keep", "filter", "retain", "select", "include", "record", "fields", "sql"},
    configuration = """
        "Record Reader" should be set to a Record Reader that is appropriate for your data.
        "Record Writer" should be set to a Record Writer that writes out data in the desired format.

        One additional property should be added.
        The name of the property should be a short description of the data to keep, such as `relevant fields`.
        Its value is a SQL statement that selects the desired columns from a table named `FLOW_FILE` for relevant rows.
        There is no WHERE clause.
        It is recommended to always quote column names using double-quotes in order to avoid conflicts with SQL keywords.
        For example, to keep only the `name`, `age`, and `address` fields, add a property named `relevant fields` \
          with a value of `SELECT "name", "age", "address" FROM FLOWFILE`

        Adding this property now gives us a new Relationship whose name is the same as the property name. So, the `relevant fields` Relationship \
        should be connected to the next Processor in our flow.
        """
)
@UseCase(
    description = "Route record-oriented data for processing based on its contents",
    keywords = {"record", "route", "conditional processing", "field"},
    configuration = """
        "Record Reader" should be set to a Record Reader that is appropriate for your data.
        "Record Writer" should be set to a Record Writer that writes out data in the desired format.

        For each route that you want to create, add a new property.
        The name of the property should be a short description of the data that should be selected for the route.
        Its value is a SQL statement that selects all columns from a table named `FLOW_FILE`. The WHERE clause selects the data that should be included in the route.
        It is recommended to always quote column names using double-quotes in order to avoid conflicts with SQL keywords.

        A new outbound relationship is created for each property that is added. The name of the relationship is the same as the property name.

        For example, to route data based on whether or not it is a large transaction, we would add two properties:
        `small transaction` would have a value such as `SELECT * FROM FLOWFILE WHERE transactionTotal < 100`
        `large transaction` would have a value of `SELECT * FROM FLOWFILE WHERE transactionTotal >= 100`
        """
)
public class QueryRecord extends AbstractProcessor {

    public static final String ROUTE_ATTRIBUTE_KEY = "QueryRecord.Route";

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing results to a FlowFile")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
        .name("include-zero-record-flowfiles")
        .displayName("Include Zero Record FlowFiles")
        .description("When running the SQL statement against an incoming FlowFile, if the result has no data, "
            + "this property specifies whether or not a FlowFile will be sent to the corresponding relationship")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_READER_FACTORY,
            RECORD_WRITER_FACTORY,
            INCLUDE_ZERO_RECORD_FLOWFILES,
            DEFAULT_PRECISION,
            DEFAULT_SCALE
    );

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The original FlowFile is routed to this relationship")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a FlowFile fails processing for any reason (for example, the SQL "
            + "statement contains columns not present in input data), the original FlowFile it will "
            + "be routed to this relationship")
        .build();

    private final Set<Relationship> relationships = Collections.synchronizedSet(new HashSet<>());

    private final Cache<Tuple<String, RecordSchema>, BlockingQueue<CachedStatement>> statementQueues = Caffeine.newBuilder()
        .maximumSize(25)
        .removalListener(this::onCacheEviction)
        .build();

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.removeProperty("cache-schema");
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (!descriptor.isDynamic()) {
            return;
        }

        final Relationship relationship = new Relationship.Builder()
            .name(descriptor.getName())
            .description("User-defined relationship that specifies where data that matches the specified SQL query should be routed")
            .build();

        if (newValue == null) {
            relationships.remove(relationship);
        } else {
            relationships.add(relationship);
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .description("SQL select statement specifies how data should be filtered/transformed. "
                + "SQL SELECT should select from the FLOWFILE table")
            .required(false)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new SqlValidator())
            .build();
    }

    @OnStopped
    public synchronized void cleanup() {
        for (final BlockingQueue<CachedStatement> statementQueue : statementQueues.asMap().values()) {
            clearQueue(statementQueue);
        }

        statementQueues.invalidateAll();
    }

    private void onCacheEviction(final Tuple<String, RecordSchema> key, final BlockingQueue<CachedStatement> queue, final RemovalCause cause) {
        clearQueue(queue);
    }

    private void clearQueue(final BlockingQueue<CachedStatement> statementQueue) {
        CachedStatement stmt;
        while ((stmt = statementQueue.poll()) != null) {
            stmt.close();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);

        final RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
        final Integer defaultPrecision = context.getProperty(DEFAULT_PRECISION).evaluateAttributeExpressions(original).asInteger();
        final Integer defaultScale = context.getProperty(DEFAULT_SCALE).evaluateAttributeExpressions(original).asInteger();

        final Map<FlowFile, Relationship> transformedFlowFiles = new HashMap<>();
        final Set<FlowFile> createdFlowFiles = new HashSet<>();

        // Determine the Record Reader's schema
        final RecordSchema writerSchema;
        final RecordSchema readerSchema;
        try (final InputStream rawIn = session.read(original)) {
            final Map<String, String> originalAttributes = original.getAttributes();
            final RecordReader reader = recordReaderFactory.createRecordReader(originalAttributes, rawIn, original.getSize(), getLogger());
            readerSchema = reader.getSchema();

            writerSchema = recordSetWriterFactory.getSchema(originalAttributes, readerSchema);
        } catch (final Exception e) {
            getLogger().error("Failed to determine Record Schema from {}; routing to failure", original, e);
            original = session.putAttribute(original, ROUTE_ATTRIBUTE_KEY, REL_FAILURE.getName());
            session.transfer(original, REL_FAILURE);
            return;
        }

        // Determine the schema for writing the data
        final Map<String, String> originalAttributes = original.getAttributes();
        int recordsRead = 0;

        try {
            for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
                if (!descriptor.isDynamic()) {
                    continue;
                }

                final Relationship relationship = new Relationship.Builder().name(descriptor.getName()).build();

                // We have to fork a child because we may need to read the input FlowFile more than once,
                // and we cannot call session.read() on the original FlowFile while we are within a write
                // callback for the original FlowFile.
                FlowFile transformed = session.create(original);
                boolean flowFileRemoved = false;

                try {
                    final String sql = context.getProperty(descriptor).evaluateAttributeExpressions(original).getValue();
                    final QueryResult queryResult = query(session, original, readerSchema, sql, recordReaderFactory);

                    final ResultSet rs = queryResult.getResultSet();
                    final RecordResultSetOutputStreamCallback writer = new RecordResultSetOutputStreamCallback(getLogger(),
                            rs, writerSchema, defaultPrecision, defaultScale, recordSetWriterFactory, originalAttributes);
                    try {
                        transformed = session.write(transformed, writer);
                    } finally {
                        closeQuietly(getLogger(), rs, queryResult);
                    }

                    recordsRead = Math.max(recordsRead, queryResult.getRecordsRead());
                    final WriteResult result = writer.getWriteResult();
                    final String mimeType = writer.getMimeType();
                    if (result.getRecordCount() == 0 && !context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean()) {
                        session.remove(transformed);
                        flowFileRemoved = true;
                        transformedFlowFiles.remove(transformed);
                        getLogger().info("Transformed {} but the result contained no data so will not pass on a FlowFile", original);
                    } else {
                        final Map<String, String> attributesToAdd = new HashMap<>();
                        if (result.getAttributes() != null) {
                            attributesToAdd.putAll(result.getAttributes());
                        }
                        if (StringUtils.isNotEmpty(mimeType)) {
                            attributesToAdd.put(CoreAttributes.MIME_TYPE.key(), mimeType);
                        }
                        attributesToAdd.put("record.count", String.valueOf(result.getRecordCount()));
                        attributesToAdd.put(ROUTE_ATTRIBUTE_KEY, relationship.getName());
                        transformed = session.putAllAttributes(transformed, attributesToAdd);
                        transformedFlowFiles.put(transformed, relationship);

                        session.adjustCounter("Records Written", result.getRecordCount(), false);
                    }
                } finally {
                    // Ensure that we have the FlowFile in the set in case we throw any Exception
                    if (!flowFileRemoved) {
                        createdFlowFiles.add(transformed);
                    }
                }
            }

            final long elapsedMillis = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            if (transformedFlowFiles.size() > 0) {
                session.getProvenanceReporter().fork(original, transformedFlowFiles.keySet(), elapsedMillis);

                for (final Map.Entry<FlowFile, Relationship> entry : transformedFlowFiles.entrySet()) {
                    final FlowFile transformed = entry.getKey();
                    final Relationship relationship = entry.getValue();

                    session.getProvenanceReporter().route(transformed, relationship);
                    session.transfer(transformed, relationship);
                }
            }

            getLogger().info("Successfully queried {} in {} millis", original, elapsedMillis);
            original = session.putAttribute(original, ROUTE_ATTRIBUTE_KEY, REL_ORIGINAL.getName());
            session.transfer(original, REL_ORIGINAL);
        } catch (final SQLException e) {
            getLogger().error("Unable to query {} due to {}", original, e.getCause() == null ? e : e.getCause());
            original = session.putAttribute(original, ROUTE_ATTRIBUTE_KEY, REL_FAILURE.getName());
            session.remove(createdFlowFiles);
            session.transfer(original, REL_FAILURE);
        } catch (final Exception e) {
            getLogger().error("Unable to query {} due to {}", original, e);
            original = session.putAttribute(original, ROUTE_ATTRIBUTE_KEY, REL_FAILURE.getName());
            session.remove(createdFlowFiles);
            session.transfer(original, REL_FAILURE);
        }

        session.adjustCounter("Records Read", recordsRead, false);
    }


    private synchronized CachedStatement getStatement(final String sql, final RecordSchema schema, final Supplier<CachedStatement> statementBuilder) {
        final Tuple<String, RecordSchema> tuple = new Tuple<>(sql, schema);
        final BlockingQueue<CachedStatement> statementQueue = statementQueues.get(tuple, key -> new LinkedBlockingQueue<>());

        final CachedStatement cachedStmt = statementQueue.poll();
        if (cachedStmt != null) {
            return cachedStmt;
        }

        return statementBuilder.get();
    }

    private CachedStatement buildCachedStatement(final String sql, final RecordSchema recordSchema) {
        try {
            final CalciteDatabase database = new CalciteDatabase();
            final NiFiTableSchema tableSchema = RecordDataSource.createTableSchema(recordSchema);
            final NiFiTable flowFileTable = new NiFiTable("FLOWFILE", tableSchema, getLogger());
            database.addTable(flowFileTable);
            RecordPathFunctions.addToDatabase(database);

            final PreparedStatement stmt = database.getConnection().prepareStatement(sql);
            return new CachedStatement(stmt, flowFileTable, database);
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }


    private QueryResult query(final ProcessSession session, final FlowFile flowFile, final RecordSchema schema, final String sql, final RecordReaderFactory recordReaderFactory)
                throws SQLException {

        final Supplier<CachedStatement> statementBuilder = () -> buildCachedStatement(sql, schema);

        final CachedStatement cachedStatement = getStatement(sql, schema, statementBuilder);
        final PreparedStatement stmt = cachedStatement.statement();
        final NiFiTable table = cachedStatement.table();
        table.setDataSource(new RecordDataSource(schema, session, flowFile, recordReaderFactory, getLogger()));

        final ResultSet rs;
        try {
            rs = stmt.executeQuery();
        } catch (final Throwable t) {
            cachedStatement.close();
            throw t;
        }

        return new QueryResult() {
            @Override
            public void close() {
                table.close();

                final BlockingQueue<CachedStatement> statementQueue = statementQueues.getIfPresent(new Tuple<>(sql, schema));
                if (statementQueue == null || !statementQueue.offer(cachedStatement)) {
                    cachedStatement.close();
                }
            }

            @Override
            public ResultSet getResultSet() {
                return rs;
            }

            @Override
            public int getRecordsRead() {
                return table.getRecordsRead();
            }

        };
    }

    private static void closeQuietly(final AutoCloseable... closeables) {
        closeQuietly(null, closeables);
    }

    private static void closeQuietly(final ComponentLog logger, final AutoCloseable... closeables) {
        if (closeables == null) {
            return;
        }

        for (final AutoCloseable closeable : closeables) {
            if (closeable == null) {
                continue;
            }

            try {
                closeable.close();
            } catch (final Exception e) {
                if (logger != null) {
                    logger.warn("Failed to close {}", closeable, e);
                }
            }
        }
    }

    private static class SqlValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder()
                    .input(input)
                    .subject(subject)
                    .valid(true)
                    .explanation("Expression Language Present")
                    .build();
            }

            final String substituted = context.newPropertyValue(input).evaluateAttributeExpressions().getValue();

            final Config config = SqlParser.config().withLex(Lex.MYSQL_ANSI);

            final SqlParser parser = SqlParser.create(substituted, config);
            try {
                parser.parseStmt();
                return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(true)
                    .build();
            } catch (final Exception e) {
                return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation("Not a valid SQL Statement: " + e.getMessage())
                    .build();
            }
        }
    }

    private interface QueryResult extends Closeable {
        ResultSet getResultSet();

        int getRecordsRead();
    }

    private record CachedStatement(PreparedStatement statement, NiFiTable table, CalciteDatabase database) implements Closeable {
        @Override
        public void close() {
            closeQuietly(statement, table, database);
        }
    }

}