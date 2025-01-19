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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.database.dialect.service.api.ColumnDefinition;
import org.apache.nifi.database.dialect.service.api.StandardColumnDefinition;
import org.apache.nifi.database.dialect.service.api.DatabaseDialectService;
import org.apache.nifi.database.dialect.service.api.StandardStatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementRequest;
import org.apache.nifi.database.dialect.service.api.StatementResponse;
import org.apache.nifi.database.dialect.service.api.StatementType;
import org.apache.nifi.database.dialect.service.api.TableDefinition;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.DiscontinuedException;
import org.apache.nifi.processors.standard.db.ColumnDescription;
import org.apache.nifi.processors.standard.db.DatabaseAdapterDescriptor;
import org.apache.nifi.processors.standard.db.NameNormalizer;
import org.apache.nifi.processors.standard.db.NameNormalizerFactory;
import org.apache.nifi.processors.standard.db.TableNotFoundException;
import org.apache.nifi.processors.standard.db.TableSchema;
import org.apache.nifi.processors.standard.db.TranslationStrategy;
import org.apache.nifi.processors.standard.db.impl.DatabaseAdapterDatabaseDialectService;
import org.apache.nifi.processors.standard.db.impl.DatabaseDialectServiceDatabaseAdapter;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;

@Tags({"metadata", "jdbc", "database", "table", "update", "alter"})
@CapabilityDescription("This processor uses a JDBC connection and incoming records to generate any database table changes needed to support the incoming records. It expects a 'flat' record layout, "
        + "meaning none of the top-level record fields has nested fields that are intended to become columns themselves.")
@WritesAttributes({
        @WritesAttribute(attribute = "output.table", description = "This attribute is written on the flow files routed to the 'success' "
                + "and 'failure' relationships, and contains the target table name."),
        @WritesAttribute(attribute = "output.path", description = "This attribute is written on the flow files routed to the 'success' "
                + "and 'failure' relationships, and contains the path on the file system to the table (or partition location if the table is partitioned)."),
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer, only if a Record Writer is specified "
                + "and Update Field Names is 'true'."),
        @WritesAttribute(attribute = "record.count", description = "Sets the number of records in the FlowFile, only if a Record Writer is specified and Update Field Names is 'true'.")
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class UpdateDatabaseTable extends AbstractProcessor {

    static final AllowableValue CREATE_IF_NOT_EXISTS = new AllowableValue("Create If Not Exists", "Create If Not Exists",
            "Create a table with the given schema if it does not already exist");
    static final AllowableValue FAIL_IF_NOT_EXISTS = new AllowableValue("Fail If Not Exists", "Fail If Not Exists",
            "If the target does not already exist, log an error and route the flowfile to failure");

    static final String ATTR_OUTPUT_TABLE = "output.table";

    // Properties
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The service for reading incoming flow files. The reader is only used to determine the schema of the records, the actual records will not be processed.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-dbcp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection(s) to the database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor CATALOG_NAME = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-catalog-name")
            .displayName("Catalog Name")
            .description("The name of the catalog that the statement should update. This may not apply for the database that you are updating. In this case, leave the field empty. Note that if the "
                    + "property is set and the database is case-sensitive, the catalog name must match the database's catalog name exactly.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-schema-name")
            .displayName("Schema Name")
            .description("The name of the database schema that the table belongs to. This may not apply for the database that you are updating. In this case, leave the field empty. Note that if the "
                    + "property is set and the database is case-sensitive, the schema name must match the database's schema name exactly.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-table-name")
            .displayName("Table Name")
            .description("The name of the database table to update. If the table does not exist, then it will either be created or an error thrown, depending "
                    + "on the value of the Create Table property.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CREATE_TABLE = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-create-table")
            .displayName("Create Table Strategy")
            .description("Specifies how to process the target table when it does not exist (create it, fail, e.g.).")
            .required(true)
            .addValidator(Validator.VALID)
            .allowableValues(CREATE_IF_NOT_EXISTS, FAIL_IF_NOT_EXISTS)
            .defaultValue(FAIL_IF_NOT_EXISTS.getValue())
            .build();

    static final PropertyDescriptor PRIMARY_KEY_FIELDS = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-primary-keys")
            .displayName("Primary Key Fields")
            .description("A comma-separated list of record field names that uniquely identifies a row in the database. This property is only used if the specified table needs to be created, "
                    + "in which case the Primary Key Fields will be used to specify the primary keys of the newly-created table. IMPORTANT: Primary Key Fields must match the record field "
                    + "names exactly unless 'Quote Column Identifiers' is false and the database allows for case-insensitive column names. In practice it is best to specify Primary Key Fields "
                    + "that exactly match the record field names, and those will become the column names in the created table.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(CREATE_TABLE, CREATE_IF_NOT_EXISTS)
            .build();

    static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-translate-field-names")
            .displayName("Translate Field Names")
            .description("If true, the Processor will attempt to translate field names into the corresponding column names for the table specified, for the purposes of determining whether "
                    + "the field name exists as a column in the target table. NOTE: If the target table does not exist and is to be created, this property is ignored and the field names will be "
                    + "used as-is. If false, the field names must match the column names exactly, or the column may not be found and instead an error my be reported that the column already exists.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor TRANSLATION_STRATEGY = new PropertyDescriptor.Builder()
            .required(true)
            .name("Column Name Translation Strategy")
            .description("The strategy used to normalize table column name. Column Name will be uppercased to " +
                    "do case-insensitive matching irrespective of strategy")
            .allowableValues(TranslationStrategy.class)
            .defaultValue(TranslationStrategy.REMOVE_UNDERSCORE.getValue())
            .dependsOn(TRANSLATE_FIELD_NAMES, TRANSLATE_FIELD_NAMES.getDefaultValue())
            .build();

    public static final PropertyDescriptor TRANSLATION_PATTERN = new PropertyDescriptor.Builder()
            .name("Column Name Translation Pattern")
            .displayName("Column Name Translation Pattern")
            .description("Column name will be normalized with this regular expression")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .dependsOn(TRANSLATE_FIELD_NAMES, TRANSLATE_FIELD_NAMES.getDefaultValue())
            .dependsOn(TRANSLATION_STRATEGY, TranslationStrategy.PATTERN.getValue())
            .build();

    static final PropertyDescriptor UPDATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-update-field-names")
            .displayName("Update Field Names")
            .description("This property indicates whether to update the output schema such that the field names are set to the exact column names from the specified "
                    + "table. This should be used if the incoming record field names may not match the table's column names in terms of upper- and lower-case. For example, this property should be "
                    + "set to true if the output FlowFile is destined for Oracle e.g., which expects the field names to match the column names exactly. NOTE: The value of the "
                    + "'Translate Field Names' property is ignored when updating field names; instead they are updated to match the column name as returned by the database.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing results to a FlowFile. The Record Writer should use Inherit Schema to emulate the inferred schema behavior, i.e. "
                    + "an explicit schema need not be defined in the writer, and will be supplied by the same logic used to infer the schema from the column types. If Create Table Strategy is set "
                    + "'Create If Not Exists', the Record Writer's output format must match the Record Reader's format in order for the data to be placed in the created table location. Note that "
                    + "this property is only used if 'Update Field Names' is set to true and the field names do not all match the column names exactly. If no "
                    + "update is needed for any field names (or 'Update Field Names' is false), the Record Writer is not used and instead the input FlowFile is routed to success or failure "
                    + "without modification.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .dependsOn(UPDATE_FIELD_NAMES, "true")
            .required(true)
            .build();

    static final PropertyDescriptor QUOTE_COLUMN_IDENTIFIERS = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-quoted-column-identifiers")
            .displayName("Quote Column Identifiers")
            .description("Enabling this option will cause all column names to be quoted, allowing you to use reserved words as column names in your tables and/or forcing the "
                    + "record field names to match the column names exactly.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUOTE_TABLE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-quoted-table-identifiers")
            .displayName("Quote Table Identifiers")
            .description("Enabling this option will cause the table name to be quoted to support the use of special characters in the table name and/or forcing the "
                    + "value of the Table Name property to match the target table name exactly.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("updatedatabasetable-query-timeout")
            .displayName("Query Timeout")
            .description("Sets the number of seconds the driver will wait for a query to execute. "
                    + "A value of 0 means no timeout. NOTE: Non-zero values may not be supported by the driver.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor DB_TYPE = DatabaseAdapterDescriptor.getDatabaseTypeDescriptor("db-type");
    static final PropertyDescriptor DATABASE_DIALECT_SERVICE = DatabaseAdapterDescriptor.getDatabaseDialectServiceDescriptor(DB_TYPE);
    private static final List<PropertyDescriptor> properties;

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile containing records routed to this relationship after the record has been successfully transmitted to the database.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile containing records routed to this relationship if the record could not be transmitted to the database.")
            .build();

    protected static Set<Relationship> relationships = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    static {
        properties = List.of(
                RECORD_READER,
                DBCP_SERVICE,
                DB_TYPE,
                DATABASE_DIALECT_SERVICE,
                CATALOG_NAME,
                SCHEMA_NAME,
                TABLE_NAME,
                CREATE_TABLE,
                PRIMARY_KEY_FIELDS,
                TRANSLATE_FIELD_NAMES,
                TRANSLATION_STRATEGY,
                TRANSLATION_PATTERN,
                UPDATE_FIELD_NAMES,
                RECORD_WRITER_FACTORY,
                QUOTE_TABLE_IDENTIFIER,
                QUOTE_COLUMN_IDENTIFIERS,
                QUERY_TIMEOUT
        );
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));
        final boolean recordWriterFactorySet = validationContext.getProperty(RECORD_WRITER_FACTORY).isSet();
        final boolean updateFieldNames = validationContext.getProperty(UPDATE_FIELD_NAMES).asBoolean();

        if (!recordWriterFactorySet && updateFieldNames) {
            validationResults.add(new ValidationResult.Builder().subject(RECORD_WRITER_FACTORY.getDisplayName())
                    .explanation("Record Writer must be set if 'Update Field Names' is true").valid(false).build());
        }

        return validationResults;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory recordWriterFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        final String catalogName = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String primaryKeyFields = context.getProperty(PRIMARY_KEY_FIELDS).evaluateAttributeExpressions(flowFile).getValue();
        final ComponentLog log = getLogger();

        try {
            final RecordReader reader;

            try (final InputStream in = session.read(flowFile)) {
                // if we fail to create the RecordReader then we want to route to failure, so we need to
                // handle this separately from the other IOExceptions which normally route to retry
                try {
                    reader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());
                } catch (Exception e) {
                    throw new ProcessException("Unable to create RecordReader", e);
                }
            } catch (ProcessException rrfe) {
                log.error(
                        "Failed to create {} for {} - routing to failure",
                        RecordReader.class.getSimpleName(), flowFile,
                        rrfe
                );
                // Since we are wrapping the exceptions above there should always be a cause
                // but it's possible it might not have a message. This handles that by logging
                // the name of the class thrown.
                Throwable c = rrfe.getCause();
                if (c != null) {
                    session.putAttribute(flowFile, "record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
                } else {
                    session.putAttribute(flowFile, "record.error.message", rrfe.getClass().getCanonicalName() + " Thrown");
                }
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            final RecordSchema recordSchema = reader.getSchema();

            final boolean createIfNotExists = context.getProperty(CREATE_TABLE).getValue().equals(CREATE_IF_NOT_EXISTS.getValue());
            final boolean updateFieldNames = context.getProperty(UPDATE_FIELD_NAMES).asBoolean();
            final boolean translateFieldNames = context.getProperty(TRANSLATE_FIELD_NAMES).asBoolean();
            final TranslationStrategy translationStrategy = TranslationStrategy.valueOf(context.getProperty(TRANSLATION_STRATEGY).getValue());
            final String translationRegex = context.getProperty(TRANSLATION_PATTERN).getValue();
            final Pattern translationPattern = translationRegex == null ? null : Pattern.compile(translationRegex);
            NameNormalizer normalizer = null;
            if (translateFieldNames) {
                normalizer = NameNormalizerFactory.getNormalizer(translationStrategy, translationPattern);
            }
            if (recordWriterFactory == null && updateFieldNames) {
                throw new ProcessException("Record Writer must be set if 'Update Field Names' is true");
            }
            final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
            final DatabaseDialectService databaseDialectService = getDatabaseDialectService(context);
            try (final Connection connection = dbcpService.getConnection(flowFile.getAttributes())) {
                final boolean quoteTableName = context.getProperty(QUOTE_TABLE_IDENTIFIER).asBoolean();
                final boolean quoteColumnNames = context.getProperty(QUOTE_COLUMN_IDENTIFIERS).asBoolean();
                final Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());

                // If table may need to be created, parse the primary key field names and pass along
                final Set<String> primaryKeyColumnNames;
                if (createIfNotExists && primaryKeyFields != null) {
                    primaryKeyColumnNames = new HashSet<>();
                    Arrays.stream(primaryKeyFields.split(","))
                            .filter(path -> path != null && !path.trim().isEmpty())
                            .map(String::trim)
                            .forEach(primaryKeyColumnNames::add);
                } else {
                    primaryKeyColumnNames = null;
                }
                final OutputMetadataHolder outputMetadataHolder = checkAndUpdateTableSchema(connection, databaseDialectService, recordSchema,
                        catalogName, schemaName, tableName, createIfNotExists, translateFieldNames, normalizer,
                        updateFieldNames, primaryKeyColumnNames, quoteTableName, quoteColumnNames);
                if (outputMetadataHolder != null) {
                    // The output schema changed (i.e. field names were updated), so write out the corresponding FlowFile
                    try {
                        final FlowFile inputFlowFile = flowFile;
                        flowFile = session.write(flowFile, (in, out) -> {

                            // if we fail to create the RecordReader then we want to route to failure, so we need to
                            // handle this separately from the other IOExceptions which normally route to retry
                            final RecordReader recordReader;
                            final RecordSetWriter recordSetWriter;
                            try {
                                recordReader = recordReaderFactory.createRecordReader(inputFlowFile, in, getLogger());
                                recordSetWriter = recordWriterFactory.createWriter(getLogger(), outputMetadataHolder.getOutputSchema(), out, attributes);
                            } catch (Exception e) {
                                if (e instanceof IOException) {
                                    throw (IOException) e;
                                }
                                throw new IOException("Unable to create RecordReader", e);
                            }

                            final WriteResult writeResult = updateRecords(recordSchema, outputMetadataHolder, recordReader, recordSetWriter);
                            recordSetWriter.flush();
                            recordSetWriter.close();
                            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                            attributes.put(CoreAttributes.MIME_TYPE.key(), recordSetWriter.getMimeType());
                            attributes.putAll(writeResult.getAttributes());
                        });
                    } catch (final Exception e) {
                        getLogger().error("Failed to process {}; will route to failure", flowFile, e);
                        // Since we are wrapping the exceptions above there should always be a cause
                        // but it's possible it might not have a message. This handles that by logging
                        // the name of the class thrown.
                        final Throwable c = e.getCause();
                        if (c != null) {
                            session.putAttribute(flowFile, "record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
                        } else {
                            session.putAttribute(flowFile, "record.error.message", e.getClass().getCanonicalName() + " Thrown");
                        }
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    }

                }
                attributes.put(ATTR_OUTPUT_TABLE, tableName);
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.getProvenanceReporter().invokeRemoteProcess(flowFile, getJdbcUrl(connection));
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (IOException | SQLException e) {
            flowFile = session.putAttribute(flowFile, ATTR_OUTPUT_TABLE, tableName);
            log.error("Exception while processing {} - routing to failure", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        } catch (DiscontinuedException e) {
            // The input FlowFile processing is discontinued. Keep it in the input queue.
            getLogger().warn("Discontinued processing for {} due to {}", flowFile, e, e);
            session.transfer(flowFile, Relationship.SELF);
        } catch (Throwable t) {
            throw (t instanceof ProcessException) ? (ProcessException) t : new ProcessException(t);
        }
    }

    private synchronized OutputMetadataHolder checkAndUpdateTableSchema(
            final Connection conn,
            final DatabaseDialectService databaseDialectService,
            final RecordSchema schema,
            final String catalogName,
            final String schemaName,
            final String tableName,
            final boolean createIfNotExists,
            final boolean translateFieldNames,
            final NameNormalizer normalizer,
            final boolean updateFieldNames,
            final Set<String> primaryKeyColumnNames,
            final boolean quoteTableName,
            final boolean quoteColumnNames
    ) throws IOException {
        // Read in the current table metadata, compare it to the reader's schema, and
        // add any columns from the schema that are missing in the table
        try (final Statement s = conn.createStatement()) {
            // Determine whether the table exists
            TableSchema tableSchema = null;
            try {
                tableSchema = TableSchema.from(conn, catalogName, schemaName, tableName, translateFieldNames, normalizer, null, getLogger());
            } catch (TableNotFoundException tnfe) {
                // Do nothing, the value will be populated if necessary
            }

            final List<ColumnDescription> columns = new ArrayList<>();
            boolean tableCreated = false;
            if (tableSchema == null) {
                if (createIfNotExists) {
                    final DatabaseMetaData databaseMetaData = conn.getMetaData();
                    final String quoteString = databaseMetaData.getIdentifierQuoteString();

                    // Create a TableSchema from the record, adding all columns
                    for (RecordField recordField : schema.getFields()) {
                        String recordFieldName = recordField.getFieldName();
                        // Assume a column to be created is required if there is a default value in the schema
                        final boolean required = (recordField.getDefaultValue() != null);

                        final String columnName;
                        if (translateFieldNames) {
                            columnName = normalizer.getNormalizedName(recordFieldName);
                        } else {
                            columnName = recordFieldName;
                        }

                        final String qualifiedColumnName;
                        if (quoteColumnNames) {
                            qualifiedColumnName = s.enquoteIdentifier(columnName, true);
                        } else {
                            qualifiedColumnName = columnName;
                        }

                        final int dataType = DataTypeUtils.getSQLTypeValue(recordField.getDataType());
                        columns.add(new ColumnDescription(qualifiedColumnName, dataType, required, null, recordField.isNullable()));
                        getLogger().debug("Adding column {} to table {}", columnName, tableName);
                    }

                    final String qualifiedCatalogName = catalogName == null ? null : s.enquoteIdentifier(catalogName, quoteTableName);
                    final String qualifiedSchemaName = schemaName == null ? null : s.enquoteIdentifier(schemaName, quoteTableName);
                    final String qualifiedTableName = s.enquoteIdentifier(tableName, quoteTableName);
                    tableSchema = new TableSchema(qualifiedCatalogName, qualifiedSchemaName, qualifiedTableName, columns, translateFieldNames, normalizer, primaryKeyColumnNames, quoteString);

                    final TableDefinition tableDefinition = getTableDefinition(tableSchema);
                    final StatementRequest statementRequest = new StandardStatementRequest(StatementType.CREATE, tableDefinition);
                    final StatementResponse statementResponse = databaseDialectService.getStatement(statementRequest);
                    final String createTableSql = statementResponse.sql();

                    if (StringUtils.isNotEmpty(createTableSql)) {
                        // Perform the table create
                        getLogger().info("Executing DDL: {}", createTableSql);
                        s.execute(createTableSql);
                    }

                    tableCreated = true;
                } else {
                    // The table wasn't found and is not to be created, so throw an error
                    throw new IOException("The table " + tableName + " could not be found in the database and the processor is configured not to create it.");
                }
            }

            final List<String> dbColumns = new ArrayList<>();
            for (final ColumnDescription columnDescription : tableSchema.getColumnsAsList()) {
                dbColumns.add(TableSchema.normalizedName(columnDescription.getColumnName(), translateFieldNames, normalizer));
            }

            final List<ColumnDescription> columnsToAdd = new ArrayList<>();
            // If the table wasn't newly created, alter it accordingly
            if (!tableCreated) {
                // Handle new columns
                for (RecordField recordField : schema.getFields()) {
                    final String recordFieldName = recordField.getFieldName();
                    final String normalizedFieldName = TableSchema.normalizedName(recordFieldName, translateFieldNames, normalizer);
                    if (!dbColumns.contains(normalizedFieldName)) {
                        // The field does not exist in the table, add it
                        ColumnDescription columnToAdd = new ColumnDescription(normalizedFieldName, DataTypeUtils.getSQLTypeValue(recordField.getDataType()),
                                recordField.getDefaultValue() != null, null, recordField.isNullable());
                        columnsToAdd.add(columnToAdd);
                        dbColumns.add(recordFieldName);
                        getLogger().debug("Adding column {} to table {}", recordFieldName, tableName);
                    }
                }

                if (!columnsToAdd.isEmpty()) {
                    final List<ColumnDefinition> columnDefinitions = columnsToAdd.stream().map(columnDescription ->
                            new StandardColumnDefinition(
                                    columnDescription.getColumnName(),
                                    columnDescription.getDataType(),
                                    columnDescription.isNullable() ? ColumnDefinition.Nullable.YES : ColumnDefinition.Nullable.UNKNOWN,
                                    Optional.empty(),
                                    columnDescription.isRequired()
                            )
                            )
                            .map(ColumnDefinition.class::cast)
                            .toList();
                    final TableDefinition tableDefinition = new TableDefinition(Optional.empty(), Optional.empty(), tableName, columnDefinitions);
                    final StatementRequest statementRequest = new StandardStatementRequest(StatementType.ALTER, tableDefinition);
                    final StatementResponse statementResponse = databaseDialectService.getStatement(statementRequest);

                    // Perform the table update
                    getLogger().info("Executing DDL: {}", statementResponse.sql());
                    s.execute(statementResponse.sql());
                }
            }

            // If updating field names, return a new RecordSchema, otherwise return null
            final OutputMetadataHolder outputMetadataHolder;
            if (updateFieldNames) {
                final List<RecordField> inputRecordFields = schema.getFields();
                final List<RecordField> outputRecordFields = new ArrayList<>();
                final Map<String, String> fieldMap = new HashMap<>();
                boolean needsUpdating = false;

                for (RecordField inputRecordField : inputRecordFields) {
                    final String inputRecordFieldName = inputRecordField.getFieldName();
                    boolean found = false;
                    for (final String columnName : dbColumns) {
                        if (inputRecordFieldName.equalsIgnoreCase(columnName)) {
                            // Set a flag if the field name doesn't match the column name exactly. This overall flag will determine whether
                            // the records need updating (if true) or not (if false)
                            if (!inputRecordFieldName.equals(columnName)) {
                                needsUpdating = true;
                            }
                            fieldMap.put(inputRecordFieldName, columnName);
                            outputRecordFields.add(new RecordField(columnName, inputRecordField.getDataType(), inputRecordField.getDefaultValue(), inputRecordField.isNullable()));
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        // If the input field wasn't a table column, add it back to the schema as-is
                        fieldMap.put(inputRecordFieldName, inputRecordFieldName);
                    }
                }
                outputMetadataHolder = needsUpdating ? new OutputMetadataHolder(new SimpleRecordSchema(outputRecordFields), fieldMap)
                        : null;
            } else {
                outputMetadataHolder = null;
            }
            return outputMetadataHolder;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private synchronized WriteResult updateRecords(final RecordSchema inputRecordSchema, final OutputMetadataHolder outputMetadataHolder,
                                                   final RecordReader reader, final RecordSetWriter writer) throws IOException {
        try {
            writer.beginRecordSet();
            Record inputRecord;
            while ((inputRecord = reader.nextRecord()) != null) {
                List<RecordField> inputRecordFields = inputRecordSchema.getFields();
                Map<String, Object> outputRecordFields = new HashMap<>(inputRecordFields.size());
                // Copy values from input field name to output field name
                for (Map.Entry<String, String> mapping : outputMetadataHolder.getFieldMap().entrySet()) {
                    outputRecordFields.put(mapping.getValue(), inputRecord.getValue(mapping.getKey()));
                }
                final Record outputRecord = new MapRecord(outputMetadataHolder.getOutputSchema(), outputRecordFields);
                writer.write(outputRecord);
            }
            return writer.finishRecordSet();

        } catch (MalformedRecordException mre) {
            throw new IOException("Error reading records: " + mre.getMessage(), mre);
        }
    }

    private String getJdbcUrl(final Connection connection) {
        try {
            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData != null) {
                return databaseMetaData.getURL();
            }
        } catch (final Exception e) {
            getLogger().warn("Could not determine JDBC URL based on the Driver Connection.", e);
        }

        return "DBCPService";
    }

    private DatabaseDialectService getDatabaseDialectService(final PropertyContext context) {
        final DatabaseDialectService databaseDialectService;
        final String databaseType = context.getProperty(DB_TYPE).getValue();
        if (DatabaseDialectServiceDatabaseAdapter.NAME.equals(databaseType)) {
            databaseDialectService = context.getProperty(DATABASE_DIALECT_SERVICE).asControllerService(DatabaseDialectService.class);
        } else {
            databaseDialectService = new DatabaseAdapterDatabaseDialectService(databaseType);
        }
        return databaseDialectService;
    }

    private TableDefinition getTableDefinition(final TableSchema tableSchema) {
        final Set<String> primaryKeyColumnNames = tableSchema.getPrimaryKeyColumnNames();
        final Set<String> primaryKeys = primaryKeyColumnNames == null ? Set.of() : primaryKeyColumnNames;

        final List<ColumnDefinition> columnDefinitions = tableSchema.getColumnsAsList().stream()
                .map(columnDescription ->
                    new StandardColumnDefinition(
                            columnDescription.getColumnName(),
                            columnDescription.getDataType(),
                            columnDescription.isNullable() ? ColumnDefinition.Nullable.YES : ColumnDefinition.Nullable.NO,
                            Optional.empty(),
                            primaryKeys.contains(columnDescription.getColumnName())
                    )
                )
                .map(ColumnDefinition.class::cast)
                .toList();

        return new TableDefinition(
                Optional.ofNullable(tableSchema.getCatalogName()),
                Optional.ofNullable(tableSchema.getSchemaName()),
                tableSchema.getTableName(),
                columnDefinitions
        );
    }

    private static class OutputMetadataHolder {
        private final RecordSchema outputSchema;
        private final Map<String, String> fieldMap;

        public OutputMetadataHolder(final RecordSchema outputSchema, final Map<String, String> fieldMap) {
            this.outputSchema = outputSchema;
            this.fieldMap = fieldMap;
        }

        public RecordSchema getOutputSchema() {
            return outputSchema;
        }

        public Map<String, String> getFieldMap() {
            return fieldMap;
        }
    }
}
