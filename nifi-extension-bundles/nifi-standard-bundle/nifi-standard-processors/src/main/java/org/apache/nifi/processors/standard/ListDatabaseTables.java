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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A processor to retrieve a list of tables (and their metadata) from a database connection
 */
@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "list", "jdbc", "table", "database"})
@CapabilityDescription("Generates a set of flow files, each containing attributes corresponding to metadata about a table from a database connection. Once "
        + "metadata about a table has been fetched, it will not be fetched again until the Refresh Interval (if set) has elapsed, or until state has been "
        + "manually cleared.")
@WritesAttributes({
        @WritesAttribute(attribute = "db.table.name", description = "Contains the name of a database table from the connection"),
        @WritesAttribute(attribute = "db.table.catalog", description = "Contains the name of the catalog to which the table belongs (may be null)"),
        @WritesAttribute(attribute = "db.table.schema", description = "Contains the name of the schema to which the table belongs (may be null)"),
        @WritesAttribute(attribute = "db.table.fullname", description = "Contains the fully-qualifed table name (possibly including catalog, schema, etc.)"),
        @WritesAttribute(attribute = "db.table.type",
                description = "Contains the type of the database table from the connection. Typical types are \"TABLE\", \"VIEW\", \"SYSTEM TABLE\", "
                        + "\"GLOBAL TEMPORARY\", \"LOCAL TEMPORARY\", \"ALIAS\", \"SYNONYM\""),
        @WritesAttribute(attribute = "db.table.remarks", description = "Contains the name of a database table from the connection"),
        @WritesAttribute(attribute = "db.table.count", description = "Contains the number of rows in the table")
})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of tables, the timestamp of the query is stored. "
        + "This allows the Processor to not re-list tables the next time that the Processor is run. Specifying the refresh interval in the processor properties will "
        + "indicate that when the processor detects the interval has elapsed, the state will be reset and tables will be re-listed as a result. "
        + "This processor is meant to be run on the primary node only.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
@MultiProcessorUseCase(
    description = "Perform a full load of a database, retrieving all rows from all tables, or a specific set of tables.",
    keywords = {"full load", "rdbms", "jdbc", "database"},
    configurations = {
        @ProcessorConfiguration(
            processorClass = ListDatabaseTables.class,
            configuration = """
                Configure the "Database Connection Pooling Service" property to specify a Connection Pool that is applicable for interacting with your database.
                Leave the RecordWriter property unset.

                Set the "Catalog" property to the name of the database Catalog; leave it empty to include all catalogs.
                Set the "Schema Pattern" property to a Java Regular Expression that matches all database Schemas that should be included; leave it empty to include all Schemas.
                Set the "Table Name Pattern" property to a Java Regular Expression that matches the names of all tables that should be included; leave it empty to include all Tables.

                Connect the "success" relationship to GenerateTableFetch.
                """
        ),
        @ProcessorConfiguration(
            processorClass = GenerateTableFetch.class,
            configuration = """
                Configure the "Database Connection Pooling Service" property to specify the same Connection Pool that was used in ListDatabaseTables.
                Set the "Database Type" property to match the appropriate value for your RDBMS vendor.
                Set "Table Name" to `${db.table.fullname}`
                Leave the RecordWriter property unset.

                Connect the "success" relationship to ExecuteSQLRecord.
                """
        ),
        @ProcessorConfiguration(
            processorClass = ExecuteSQLRecord.class,
            configuration = """
                Configure the "Database Connection Pooling Service" property to specify the same Connection Pool that was used in ListDatabaseTables.
                Configure the "Record Writer" property to specify a Record Writer that is appropriate for the desired output data type.
                Leave the "SQL select query" unset.

                Connect the "success" relationship to the next Processor in the flow.
                """
        )
    }
)
public class ListDatabaseTables extends AbstractProcessor {

    // Attribute names
    public static final String DB_TABLE_NAME = "db.table.name";
    public static final String DB_TABLE_CATALOG = "db.table.catalog";
    public static final String DB_TABLE_SCHEMA = "db.table.schema";
    public static final String DB_TABLE_FULLNAME = "db.table.fullname";
    public static final String DB_TABLE_TYPE = "db.table.type";
    public static final String DB_TABLE_REMARKS = "db.table.remarks";
    public static final String DB_TABLE_COUNT = "db.table.count";

    // Property descriptors
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("list-db-tables-db-connection")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor CATALOG = new PropertyDescriptor.Builder()
            .name("list-db-tables-catalog")
            .displayName("Catalog")
            .description("The name of a catalog from which to list database tables. The name must match the catalog name as it is stored in the database. "
                    + "If the property is not set, the catalog name will not be used to narrow the search for tables. If the property is set to an empty string, "
                    + "tables without a catalog will be listed.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor SCHEMA_PATTERN = new PropertyDescriptor.Builder()
            .name("list-db-tables-schema-pattern")
            .displayName("Schema Pattern")
            .description("A pattern for matching schemas in the database. Within a pattern, \"%\" means match any substring of 0 or more characters, "
                    + "and \"_\" means match any one character. The pattern must match the schema name as it is stored in the database. "
                    + "If the property is not set, the schema name will not be used to narrow the search for tables. If the property is set to an empty string, "
                    + "tables without a schema will be listed.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor TABLE_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("list-db-tables-name-pattern")
            .displayName("Table Name Pattern")
            .description("A pattern for matching tables in the database. Within a pattern, \"%\" means match any substring of 0 or more characters, "
                    + "and \"_\" means match any one character. The pattern must match the table name as it is stored in the database. "
                    + "If the property is not set, all tables will be retrieved.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor TABLE_TYPES = new PropertyDescriptor.Builder()
            .name("list-db-tables-types")
            .displayName("Table Types")
            .description("A comma-separated list of table types to include. For example, some databases support TABLE and VIEW types. If the property is not set, "
                    + "tables of all types will be returned.")
            .required(false)
            .defaultValue("TABLE")
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor INCLUDE_COUNT = new PropertyDescriptor.Builder()
            .name("list-db-include-count")
            .displayName("Include Count")
            .description("Whether to include the table's row count as a flow file attribute. This affects performance as a database query will be generated "
                    + "for each table in the retrieved list.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor REFRESH_INTERVAL = new PropertyDescriptor.Builder()
            .name("list-db-refresh-interval")
            .displayName("Refresh Interval")
            .description("The amount of time to elapse before resetting the processor state, thereby causing all current tables to be listed. "
                    + "During this interval, the processor may continue to run, but tables that have already been listed will not be re-listed. However new/added "
                    + "tables will be listed as the processor runs. A value of zero means the state will never be automatically reset, the user must "
                    + "Clear State manually.")
            .required(true)
            .defaultValue("0 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Record Writer to use for creating the listing. If not specified, one FlowFile will be created for each entity that is listed. If the Record Writer is specified, " +
            "all entities will be written to a single FlowFile instead of adding attributes to individual FlowFiles.")
        .required(false)
        .identifiesControllerService(RecordSetWriterFactory.class)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            DBCP_SERVICE,
            CATALOG,
            SCHEMA_PATTERN,
            TABLE_NAME_PATTERN,
            TABLE_TYPES,
            INCLUDE_COUNT,
            RECORD_WRITER,
            REFRESH_INTERVAL
    );

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received are routed to success")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final String catalog = context.getProperty(CATALOG).getValue();
        final String schemaPattern = context.getProperty(SCHEMA_PATTERN).getValue();
        final String tableNamePattern = context.getProperty(TABLE_NAME_PATTERN).getValue();
        final String[] tableTypes = context.getProperty(TABLE_TYPES).isSet()
                ? context.getProperty(TABLE_TYPES).getValue().split("\\s*,\\s*")
                : null;
        final boolean includeCount = context.getProperty(INCLUDE_COUNT).asBoolean();
        final long refreshInterval = context.getProperty(REFRESH_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);

        final StateMap stateMap;
        final Map<String, String> stateMapProperties;
        try {
            stateMap = session.getState(Scope.CLUSTER);
            stateMapProperties = new HashMap<>(stateMap.toMap());
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }

        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final TableListingWriter writer;
        if (writerFactory == null) {
            writer = new AttributeTableListingWriter(session);
        } else {
            writer = new RecordTableListingWriter(session, writerFactory, getLogger());
        }

        try (final Connection con = dbcpService.getConnection(Collections.emptyMap())) {
            writer.beginListing();

            DatabaseMetaData dbMetaData = con.getMetaData();
            try (ResultSet rs = dbMetaData.getTables(catalog, schemaPattern, tableNamePattern, tableTypes)) {
                while (rs.next()) {
                    final String tableCatalog = rs.getString(1);
                    final String tableSchema = rs.getString(2);
                    final String tableName = rs.getString(3);
                    final String tableType = rs.getString(4);
                    final String tableRemarks = rs.getString(5);

                    // Build fully-qualified name
                    final String fqn = Stream.of(tableCatalog, tableSchema, tableName)
                        .filter(segment -> !StringUtils.isEmpty(segment))
                        .collect(Collectors.joining("."));

                    final String lastTimestampForTable = stateMapProperties.get(fqn);
                    boolean refreshTable = true;
                    try {
                        // Refresh state if the interval has elapsed
                        long lastRefreshed = -1;
                        final long currentTime = System.currentTimeMillis();
                        if (!StringUtils.isEmpty(lastTimestampForTable)) {
                            lastRefreshed = Long.parseLong(lastTimestampForTable);
                        }

                        if (lastRefreshed == -1 || (refreshInterval > 0 && currentTime >= (lastRefreshed + refreshInterval))) {
                            stateMapProperties.remove(lastTimestampForTable);
                        } else {
                            refreshTable = false;
                        }
                    } catch (final NumberFormatException nfe) {
                        getLogger().error(
                          "Failed to retrieve observed last table fetches from the State Manager. Will not perform "
                          + "query until this is accomplished.", nfe);
                        context.yield();
                        return;
                    }

                    if (refreshTable) {
                        logger.info("Found {}: {}", tableType, fqn);
                        final Map<String, String> tableInformation = new HashMap<>();

                        if (includeCount) {
                            try (Statement st = con.createStatement()) {
                                final String countQuery = "SELECT COUNT(1) FROM " + fqn;

                                logger.debug("Executing query: {}", countQuery);
                                try (ResultSet countResult = st.executeQuery(countQuery)) {
                                    if (countResult.next()) {
                                        tableInformation.put(DB_TABLE_COUNT, Long.toString(countResult.getLong(1)));
                                    }
                                }
                            } catch (final SQLException se) {
                                logger.error("Couldn't get row count for {}", fqn);
                                continue;
                            }
                        }

                        if (tableCatalog != null) {
                            tableInformation.put(DB_TABLE_CATALOG, tableCatalog);
                        }
                        if (tableSchema != null) {
                            tableInformation.put(DB_TABLE_SCHEMA, tableSchema);
                        }
                        tableInformation.put(DB_TABLE_NAME, tableName);
                        tableInformation.put(DB_TABLE_FULLNAME, fqn);
                        tableInformation.put(DB_TABLE_TYPE, tableType);
                        if (tableRemarks != null) {
                            tableInformation.put(DB_TABLE_REMARKS, tableRemarks);
                        }

                        String transitUri;
                        try {
                            transitUri = dbMetaData.getURL();
                        } catch (final SQLException sqle) {
                            transitUri = "<unknown>";
                        }

                        writer.addToListing(tableInformation, transitUri);

                        stateMapProperties.put(fqn, Long.toString(System.currentTimeMillis()));
                    }
                }

                writer.finishListing();
            }

            session.replaceState(stateMap, stateMapProperties, Scope.CLUSTER);
        } catch (final SQLException | IOException | SchemaNotFoundException e) {
            writer.finishListingExceptionally(e);
            session.rollback();
            throw new ProcessException(e);
        }
    }

    interface TableListingWriter {
        void beginListing() throws IOException, SchemaNotFoundException;

        void addToListing(Map<String, String> tableInformation, String transitUri) throws IOException;

        void finishListing() throws IOException;

        void finishListingExceptionally(Exception cause);
    }


    private static class AttributeTableListingWriter implements TableListingWriter {
        private final ProcessSession session;

        public AttributeTableListingWriter(final ProcessSession session) {
            this.session = session;
        }

        @Override
        public void beginListing() {
        }

        @Override
        public void addToListing(final Map<String, String> tableInformation, final String transitUri) {
            FlowFile flowFile = session.create();
            flowFile = session.putAllAttributes(flowFile, tableInformation);
            session.getProvenanceReporter().receive(flowFile, transitUri);
            session.transfer(flowFile, REL_SUCCESS);
        }

        @Override
        public void finishListing() {
        }

        @Override
        public void finishListingExceptionally(final Exception cause) {
        }
    }

    static class RecordTableListingWriter implements TableListingWriter {
        public static final String TABLE_NAME = "tableName";
        public static final String TABLE_CATALOG = "catalog";
        public static final String TABLE_SCHEMA = "schemaName";
        public static final String TABLE_FULLNAME = "fullName";
        public static final String TABLE_TYPE = "tableType";
        public static final String TABLE_REMARKS = "remarks";
        public static final String TABLE_ROW_COUNT = "rowCount";

        private static final RecordSchema RECORD_SCHEMA = new SimpleRecordSchema(List.of(
                new RecordField(TABLE_NAME, RecordFieldType.STRING.getDataType(), false),
                new RecordField(TABLE_CATALOG, RecordFieldType.STRING.getDataType()),
                new RecordField(TABLE_SCHEMA, RecordFieldType.STRING.getDataType()),
                new RecordField(TABLE_FULLNAME, RecordFieldType.STRING.getDataType(), false),
                new RecordField(TABLE_TYPE, RecordFieldType.STRING.getDataType(), false),
                new RecordField(TABLE_REMARKS, RecordFieldType.STRING.getDataType(), false),
                new RecordField(TABLE_ROW_COUNT, RecordFieldType.LONG.getDataType(), false)
        ));

        private final ProcessSession session;
        private final RecordSetWriterFactory writerFactory;
        private final ComponentLog logger;
        private RecordSetWriter recordWriter;
        private FlowFile flowFile;
        private String transitUri;

        public RecordTableListingWriter(final ProcessSession session, final RecordSetWriterFactory writerFactory, final ComponentLog logger) {
            this.session = session;
            this.writerFactory = writerFactory;
            this.logger = logger;
        }

        @Override
        public void beginListing() throws IOException, SchemaNotFoundException {
            flowFile = session.create();

            final OutputStream out = session.write(flowFile);
            recordWriter = writerFactory.createWriter(logger, RECORD_SCHEMA, out, flowFile);
            recordWriter.beginRecordSet();
        }

        @Override
        public void addToListing(final Map<String, String> tableInfo, final String transitUri) throws IOException {
            this.transitUri = transitUri;
            recordWriter.write(createRecordForListing(tableInfo));
        }

        @Override
        public void finishListing() throws IOException {
            final WriteResult writeResult = recordWriter.finishRecordSet();
            recordWriter.close();

            if (writeResult.getRecordCount() == 0) {
                session.remove(flowFile);
            } else {
                // todo 13590
                final Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
                attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                flowFile = session.putAllAttributes(flowFile, attributes);

                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().receive(flowFile, transitUri);
            }
        }

        @Override
        public void finishListingExceptionally(final Exception cause) {
            try {
                recordWriter.close();
            } catch (IOException e) {
                logger.error("Failed to write listing as Records due to {}", e, e);
            }

            session.remove(flowFile);
        }

        private Record createRecordForListing(final Map<String, String> tableInfo) {
            final Map<String, Object> values = new HashMap<>();
            values.put(TABLE_NAME, tableInfo.get(DB_TABLE_NAME));
            values.put(TABLE_FULLNAME, tableInfo.get(DB_TABLE_FULLNAME));
            values.put(TABLE_CATALOG, tableInfo.get(DB_TABLE_CATALOG));
            values.put(TABLE_REMARKS, tableInfo.get(DB_TABLE_REMARKS));
            values.put(TABLE_SCHEMA, tableInfo.get(DB_TABLE_SCHEMA));
            values.put(TABLE_TYPE, tableInfo.get(DB_TABLE_TYPE));

            final String rowCountString = tableInfo.get(DB_TABLE_COUNT);
            if (rowCountString != null) {
                values.put(TABLE_ROW_COUNT, Long.parseLong(rowCountString));
            }

            return new MapRecord(RECORD_SCHEMA, values);
        }
    }

}
