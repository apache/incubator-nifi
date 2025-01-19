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

import static java.lang.String.format;
import static org.apache.nifi.expression.ExpressionLanguageScope.ENVIRONMENT;
import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.apache.nifi.processors.standard.db.ColumnDescription;
import org.apache.nifi.processors.standard.db.DatabaseAdapterDescriptor;
import org.apache.nifi.processors.standard.db.NameNormalizer;
import org.apache.nifi.processors.standard.db.NameNormalizerFactory;
import org.apache.nifi.processors.standard.db.TableSchema;
import org.apache.nifi.processors.standard.db.TranslationStrategy;
import org.apache.nifi.processors.standard.db.impl.DatabaseAdapterDatabaseDialectService;
import org.apache.nifi.processors.standard.db.impl.DatabaseDialectServiceDatabaseAdapter;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "record", "jdbc", "put", "database", "update", "insert", "delete"})
@CapabilityDescription("The PutDatabaseRecord processor uses a specified RecordReader to input (possibly multiple) records from an incoming flow file. These records are translated to SQL "
        + "statements and executed as a single transaction. If any errors occur, the flow file is routed to failure or retry, and if the records are transmitted successfully, "
        + "the incoming flow file is "
        + "routed to success.  The type of statement executed by the processor is specified via the Statement Type property, which accepts some hard-coded values such as INSERT, UPDATE, and DELETE, "
        + "as well as 'Use statement.type Attribute', which causes the processor to get the statement type from a flow file attribute.  IMPORTANT: If the Statement Type is UPDATE, then the incoming "
        + "records must not alter the value(s) of the primary keys (or user-specified Update Keys). If such records are encountered, the UPDATE statement issued to the database may do nothing "
        + "(if no existing records with the new primary key values are found), or could inadvertently corrupt the existing data (by changing records for which the new values of the primary keys "
        + "exist).")
@ReadsAttribute(attribute = PutDatabaseRecord.STATEMENT_TYPE_ATTRIBUTE, description = "If 'Use statement.type Attribute' is selected for the Statement Type property, the value of this attribute "
        + "will be used to determine the type of statement (INSERT, UPDATE, DELETE, SQL, etc.) to generate and execute.")
@WritesAttribute(attribute = PutDatabaseRecord.PUT_DATABASE_RECORD_ERROR, description = "If an error occurs during processing, the flow file will be routed to failure or retry, and this attribute "
        + "will be populated with the cause of the error.")
@UseCase(description = "Insert records into a database")
public class PutDatabaseRecord extends AbstractProcessor {

    public static final String UPDATE_TYPE = "UPDATE";
    public static final String INSERT_TYPE = "INSERT";
    public static final String DELETE_TYPE = "DELETE";
    public static final String UPSERT_TYPE = "UPSERT";
    public static final String INSERT_IGNORE_TYPE = "INSERT_IGNORE";
    public static final String SQL_TYPE = "SQL";   // Not an allowable value in the Statement Type property, must be set by attribute
    public static final String USE_ATTR_TYPE = "Use statement.type Attribute";
    public static final String USE_RECORD_PATH = "Use Record Path";

    static final String STATEMENT_TYPE_ATTRIBUTE = "statement.type";

    static final String PUT_DATABASE_RECORD_ERROR = "putdatabaserecord.error";

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

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_RETRY
    );

    // Properties
    static final PropertyDescriptor RECORD_READER_FACTORY = new Builder()
            .name("put-db-record-record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor STATEMENT_TYPE = new Builder()
            .name("put-db-record-statement-type")
            .displayName("Statement Type")
            .description("Specifies the type of SQL Statement to generate. "
                    + "Please refer to the database documentation for a description of the behavior of each operation. "
                    + "Please note that some Database Types may not support certain Statement Types. "
                    + "If 'Use statement.type Attribute' is chosen, then the value is taken from the statement.type attribute in the "
                    + "FlowFile. The 'Use statement.type Attribute' option is the only one that allows the 'SQL' statement type. If 'SQL' is specified, the value of the field specified by the "
                    + "'Field Containing SQL' property is expected to be a valid SQL statement on the target database, and will be executed as-is.")
            .required(true)
            .allowableValues(UPDATE_TYPE, INSERT_TYPE, UPSERT_TYPE, INSERT_IGNORE_TYPE, DELETE_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor STATEMENT_TYPE_RECORD_PATH = new Builder()
            .name("Statement Type Record Path")
            .displayName("Statement Type Record Path")
            .description("Specifies a RecordPath to evaluate against each Record in order to determine the Statement Type. The RecordPath should equate to either INSERT, UPDATE, UPSERT, or DELETE. "
                    + "(Debezium style operation types are also supported: \"r\" and \"c\" for INSERT, \"u\" for UPDATE, and \"d\" for DELETE)")
            .required(true)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(NONE)
            .dependsOn(STATEMENT_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor DATA_RECORD_PATH = new Builder()
            .name("Data Record Path")
            .displayName("Data Record Path")
            .description("If specified, this property denotes a RecordPath that will be evaluated against each incoming" +
                    " Record and the Record that results from evaluating the RecordPath will be sent to" +
                    " the database instead of sending the entire incoming Record. If not specified, the entire incoming Record will be published to the database.")
            .required(false)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(NONE)
            .build();

    static final PropertyDescriptor DBCP_SERVICE = new Builder()
            .name("put-db-record-dcbp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain a connection to the database for sending records.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor CATALOG_NAME = new Builder()
            .name("put-db-record-catalog-name")
            .displayName("Catalog Name")
            .description("The name of the catalog that the statement should update. This may not apply for the database that you are updating. In this case, leave the field empty. Note that if the "
                    + "property is set and the database is case-sensitive, the catalog name must match the database's catalog name exactly.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new Builder()
            .name("put-db-record-schema-name")
            .displayName("Schema Name")
            .description("The name of the schema that the table belongs to. This may not apply for the database that you are updating. In this case, leave the field empty. Note that if the "
                    + "property is set and the database is case-sensitive, the schema name must match the database's schema name exactly.")
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new Builder()
            .name("put-db-record-table-name")
            .displayName("Table Name")
            .description("The name of the table that the statement should affect. Note that if the database is case-sensitive, the table name must match the database's table name exactly.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final AllowableValue BINARY_STRING_FORMAT_UTF8 = new AllowableValue(
            "UTF-8",
            "UTF-8",
            "String values for binary columns contain the original value as text via UTF-8 character encoding"
    );

    static final AllowableValue BINARY_STRING_FORMAT_HEXADECIMAL = new AllowableValue(
            "Hexadecimal",
            "Hexadecimal",
            "String values for binary columns contain the original value in hexadecimal format"
    );

    static final AllowableValue BINARY_STRING_FORMAT_BASE64 = new AllowableValue(
            "Base64",
            "Base64",
            "String values for binary columns contain the original value in Base64 encoded format"
    );

    static final PropertyDescriptor BINARY_STRING_FORMAT = new Builder()
            .name("put-db-record-binary-format")
            .displayName("Binary String Format")
            .description("The format to be applied when decoding string values to binary.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .allowableValues(BINARY_STRING_FORMAT_UTF8, BINARY_STRING_FORMAT_HEXADECIMAL, BINARY_STRING_FORMAT_BASE64)
            .defaultValue(BINARY_STRING_FORMAT_UTF8.getValue())
            .build();

    static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new Builder()
            .name("put-db-record-translate-field-names")
            .displayName("Translate Field Names")
            .description("If true, the Processor will attempt to translate field names into the appropriate column names for the table specified. "
                    + "If false, the field names must match the column names exactly, or the column will not be updated")
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
            .required(true)
            .name("Column Name Translation Pattern")
            .description("Column name will be normalized with this regular expression")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .dependsOn(TRANSLATE_FIELD_NAMES, TRANSLATE_FIELD_NAMES.getDefaultValue())
            .dependsOn(TRANSLATION_STRATEGY, TranslationStrategy.PATTERN.getValue())
            .build();

    static final PropertyDescriptor UNMATCHED_FIELD_BEHAVIOR = new Builder()
            .name("put-db-record-unmatched-field-behavior")
            .displayName("Unmatched Field Behavior")
            .description("If an incoming record has a field that does not map to any of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_FIELD, FAIL_UNMATCHED_FIELD)
            .defaultValue(IGNORE_UNMATCHED_FIELD.getValue())
            .build();

    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new Builder()
            .name("put-db-record-unmatched-column-behavior")
            .displayName("Unmatched Column Behavior")
            .description("If an incoming record does not have a field mapping for all of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_COLUMN, WARNING_UNMATCHED_COLUMN, FAIL_UNMATCHED_COLUMN)
            .defaultValue(FAIL_UNMATCHED_COLUMN.getValue())
            .build();

    static final PropertyDescriptor UPDATE_KEYS = new Builder()
            .name("put-db-record-update-keys")
            .displayName("Update Keys")
            .description("A comma-separated list of column names that uniquely identifies a row in the database for UPDATE statements. "
                    + "If the Statement Type is UPDATE and this property is not set, the table's Primary Keys are used. "
                    + "In this case, if no Primary Key exists, the conversion to SQL will fail if Unmatched Column Behaviour is set to FAIL. "
                    + "This property is ignored if the Statement Type is INSERT")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(STATEMENT_TYPE, UPDATE_TYPE, UPSERT_TYPE, SQL_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor DELETE_KEYS = new Builder()
            .name("Delete Keys")
            .description("A comma-separated list of column names that uniquely identifies a row in the database for DELETE statements. "
                    + "If the Statement Type is DELETE and this property is not set, the table's columns are used. "
                    + "This property is ignored if the Statement Type is not DELETE")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(STATEMENT_TYPE, DELETE_TYPE, SQL_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor FIELD_CONTAINING_SQL = new Builder()
            .name("put-db-record-field-containing-sql")
            .displayName("Field Containing SQL")
            .description("If the Statement Type is 'SQL' (as set in the statement.type attribute), this field indicates which field in the record(s) contains the SQL statement to execute. The value "
                    + "of the field must be a single SQL statement. If the Statement Type is not 'SQL', this field is ignored.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .dependsOn(STATEMENT_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor ALLOW_MULTIPLE_STATEMENTS = new Builder()
            .name("put-db-record-allow-multiple-statements")
            .displayName("Allow Multiple SQL Statements")
            .description("If the Statement Type is 'SQL' (as set in the statement.type attribute), this field indicates whether to split the field value by a semicolon and execute each statement "
                    + "separately. If any statement causes an error, the entire set of statements will be rolled back. If the Statement Type is not 'SQL', this field is ignored.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(STATEMENT_TYPE, USE_ATTR_TYPE, USE_RECORD_PATH)
            .build();

    static final PropertyDescriptor QUOTE_IDENTIFIERS = new Builder()
            .name("put-db-record-quoted-identifiers")
            .displayName("Quote Column Identifiers")
            .description("Enabling this option will cause all column names to be quoted, allowing you to use reserved words as column names in your tables.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUOTE_TABLE_IDENTIFIER = new Builder()
            .name("put-db-record-quoted-table-identifiers")
            .displayName("Quote Table Identifiers")
            .description("Enabling this option will cause the table name to be quoted to support the use of special characters in the table name.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new Builder()
            .name("put-db-record-query-timeout")
            .displayName("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL statement "
                    + ", zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ENVIRONMENT)
            .build();

    static final PropertyDescriptor TABLE_SCHEMA_CACHE_SIZE = new Builder()
            .name("table-schema-cache-size")
            .displayName("Table Schema Cache Size")
            .description("Specifies how many Table Schemas should be cached")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .required(true)
            .build();

    static final PropertyDescriptor MAX_BATCH_SIZE = new Builder()
            .name("put-db-record-max-batch-size")
            .displayName("Maximum Batch Size")
            .description("Specifies maximum number of sql statements to be included in each batch sent to the database. Zero means the batch size is not limited, "
                    + "and all statements are put into a single batch which can cause high memory usage issues for a very large number of statements.")
            .defaultValue("1000")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
            .name("database-session-autocommit")
            .displayName("Database Session AutoCommit")
            .description("The autocommit mode to set on the database connection being used. If set to false, the operation(s) will be explicitly committed or rolled back "
                    + "(based on success or failure respectively). If set to true, the driver/database automatically handles the commit/rollback.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(false)
            .build();

    static final PropertyDescriptor DB_TYPE = DatabaseAdapterDescriptor.getDatabaseTypeDescriptor("db-type");
    static final PropertyDescriptor DATABASE_DIALECT_SERVICE = DatabaseAdapterDescriptor.getDatabaseDialectServiceDescriptor(DB_TYPE);

    protected static final List<PropertyDescriptor> properties = List.of(
            RECORD_READER_FACTORY,
            DB_TYPE,
            DATABASE_DIALECT_SERVICE,
            STATEMENT_TYPE,
            STATEMENT_TYPE_RECORD_PATH,
            DATA_RECORD_PATH,
            DBCP_SERVICE,
            CATALOG_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            BINARY_STRING_FORMAT,
            TRANSLATE_FIELD_NAMES,
            TRANSLATION_STRATEGY,
            TRANSLATION_PATTERN,
            UNMATCHED_FIELD_BEHAVIOR,
            UNMATCHED_COLUMN_BEHAVIOR,
            UPDATE_KEYS,
            DELETE_KEYS,
            FIELD_CONTAINING_SQL,
            ALLOW_MULTIPLE_STATEMENTS,
            QUOTE_IDENTIFIERS,
            QUOTE_TABLE_IDENTIFIER,
            QUERY_TIMEOUT,
            RollbackOnFailure.ROLLBACK_ON_FAILURE,
            TABLE_SCHEMA_CACHE_SIZE,
            MAX_BATCH_SIZE,
            AUTO_COMMIT
    );

    private Cache<SchemaKey, TableSchema> schemaCache;

    private volatile DatabaseDialectService databaseDialectService;
    private volatile Function<Record, String> recordPathOperationType;
    private volatile RecordPath dataRecordPath;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        final DatabaseDialectService dialectService = getDatabaseDialectService(validationContext);
        final Set<StatementType> supportedStatementTypes = dialectService.getSupportedStatementTypes();
        final String configuredStatementType = validationContext.getProperty(STATEMENT_TYPE).getValue();
        if (INSERT_IGNORE_TYPE.equals(configuredStatementType)) {
            if (!supportedStatementTypes.contains(StatementType.INSERT_IGNORE)) {
                validationResults.add(new ValidationResult.Builder()
                        .subject(STATEMENT_TYPE.getDisplayName())
                        .valid(false)
                        .explanation("INSERT IGNORE not supported with Database Dialect")
                        .build()
                );
            }
        }
        if (UPSERT_TYPE.equals(configuredStatementType)) {
            if (!supportedStatementTypes.contains(StatementType.UPSERT)) {
                validationResults.add(new ValidationResult.Builder()
                        .subject(STATEMENT_TYPE.getDisplayName())
                        .valid(false)
                        .explanation("UPSERT not supported with Database Dialect")
                        .build()
                );
            }
        }

        final Boolean autoCommit = validationContext.getProperty(AUTO_COMMIT).asBoolean();
        final boolean rollbackOnFailure = validationContext.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        if (autoCommit != null && autoCommit && rollbackOnFailure) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(RollbackOnFailure.ROLLBACK_ON_FAILURE.getDisplayName())
                    .explanation(format("'%s' cannot be set to 'true' when '%s' is also set to 'true'. "
                                    + "Transaction rollbacks for batch updates cannot rollback all the flow file's statements together "
                                    + "when auto commit is set to 'true' because the database autocommits each batch separately.",
                            RollbackOnFailure.ROLLBACK_ON_FAILURE.getDisplayName(), AUTO_COMMIT.getDisplayName()))
                    .build());
        }

        if (autoCommit != null && autoCommit && !isMaxBatchSizeHardcodedToZero(validationContext)) {
            final String explanation = format("'%s' must be hard-coded to zero when '%s' is set to 'true'."
                            + " Batch size equal to zero executes all statements in a single transaction"
                            + " which allows rollback to revert all the flow file's statements together if an error occurs.",
                    MAX_BATCH_SIZE.getDisplayName(), AUTO_COMMIT.getDisplayName());

            validationResults.add(new ValidationResult.Builder()
                    .subject(MAX_BATCH_SIZE.getDisplayName())
                    .explanation(explanation)
                    .build());
        }

        return validationResults;
    }

    private boolean isMaxBatchSizeHardcodedToZero(ValidationContext validationContext) {
        try {
            return !validationContext.getProperty(MAX_BATCH_SIZE).isExpressionLanguagePresent()
                    && 0 == validationContext.getProperty(MAX_BATCH_SIZE).asInteger();
        } catch (Exception ex) {
            return false;
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        databaseDialectService = getDatabaseDialectService(context);

        final int tableSchemaCacheSize = context.getProperty(TABLE_SCHEMA_CACHE_SIZE).asInteger();
        schemaCache = Caffeine.newBuilder()
                .maximumSize(tableSchemaCacheSize)
                .build();

        final String statementTypeRecordPathValue = context.getProperty(STATEMENT_TYPE_RECORD_PATH).getValue();
        if (statementTypeRecordPathValue == null) {
            recordPathOperationType = null;
        } else {
            final RecordPath recordPath = RecordPath.compile(statementTypeRecordPathValue);
            recordPathOperationType = new RecordPathStatementType(recordPath);
        }

        final String dataRecordPathValue = context.getProperty(DATA_RECORD_PATH).getValue();
        dataRecordPath = dataRecordPathValue == null ? null : RecordPath.compile(dataRecordPathValue);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

        Connection connection = null;
        boolean originalAutoCommit = false;
        try {
            connection = dbcpService.getConnection(flowFile.getAttributes());

            originalAutoCommit = connection.getAutoCommit();
            final Boolean propertyAutoCommitValue = context.getProperty(AUTO_COMMIT).asBoolean();
            if (propertyAutoCommitValue != null && originalAutoCommit != propertyAutoCommitValue) {
                try {
                    connection.setAutoCommit(propertyAutoCommitValue);
                } catch (Exception ex) {
                    getLogger().debug("Failed to setAutoCommit({}) due to {}", propertyAutoCommitValue, ex.getClass().getName(), ex);
                }
            }

            putToDatabase(context, session, flowFile, connection);

            // If the connection's auto-commit setting is false, then manually commit the transaction
            if (!connection.getAutoCommit()) {
                connection.commit();
            }

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, getJdbcUrl(connection));
        } catch (final Exception e) {
            routeOnException(context, session, connection, e, flowFile);
        } finally {
            closeConnection(connection, originalAutoCommit);
        }
    }

    private DatabaseDialectService getDatabaseDialectService(final PropertyContext context) {
        final String databaseType = context.getProperty(DB_TYPE).getValue();

        final DatabaseDialectService service;
        if (DatabaseDialectServiceDatabaseAdapter.NAME.equals(databaseType)) {
            service = context.getProperty(DATABASE_DIALECT_SERVICE).asControllerService(DatabaseDialectService.class);
        } else {
            service = new DatabaseAdapterDatabaseDialectService(databaseType);
        }

        return service;
    }

    private void routeOnException(final ProcessContext context, final ProcessSession session,
                                  Connection connection, Exception e, FlowFile flowFile) {
        // When an Exception is thrown, we want to route to 'retry' if we expect that attempting the same request again
        // might work. Otherwise, route to failure. SQLTransientException is a specific type that indicates that a retry may work.
        final Relationship relationship;
        final Throwable toAnalyze = (e instanceof BatchUpdateException || e instanceof ProcessException) ? e.getCause() : e;
        if (toAnalyze instanceof SQLTransientException) {
            relationship = REL_RETRY;
            flowFile = session.penalize(flowFile);
        } else {
            relationship = REL_FAILURE;
        }

        final boolean rollbackOnFailure = context.getProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE).asBoolean();
        if (rollbackOnFailure) {
            getLogger().error("Failed to put Records to database for {}. Rolling back NiFi session and returning the flow file to its incoming queue.", flowFile, e);
            session.rollback();
            context.yield();
        } else {
            getLogger().error("Failed to put Records to database for {}. Routing to {}.", flowFile, relationship, e);
            flowFile = session.putAttribute(flowFile, PUT_DATABASE_RECORD_ERROR, (e.getMessage() == null ? "Unknown" : e.getMessage()));
            session.transfer(flowFile, relationship);
        }

        rollbackConnection(connection);
    }

    private void rollbackConnection(Connection connection) {
        if (connection != null) {
            try {
                if (!connection.getAutoCommit()) {
                    connection.rollback();
                    getLogger().debug("Manually rolled back JDBC transaction.");
                }
            } catch (final Exception rollbackException) {
                getLogger().error("Failed to rollback JDBC transaction", rollbackException);
            }
        }
    }

    private void closeConnection(Connection connection, boolean originalAutoCommit) {
        if (connection != null) {
            try {
                if (originalAutoCommit != connection.getAutoCommit()) {
                    connection.setAutoCommit(originalAutoCommit);
                }
            } catch (final Exception autoCommitException) {
                getLogger().warn("Failed to set auto-commit back to {} on connection", originalAutoCommit, autoCommitException);
            }

            try {
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (final Exception closeException) {
                getLogger().warn("Failed to close database connection", closeException);
            }
        }
    }

    private void executeSQL(final ProcessContext context, final ProcessSession session, final FlowFile flowFile,
                            final Connection connection, final RecordReader recordReader)
            throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final RecordSchema recordSchema = recordReader.getSchema();

        // Find which field has the SQL statement in it
        final String sqlField = context.getProperty(FIELD_CONTAINING_SQL).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(sqlField)) {
            throw new IllegalArgumentException(format("SQL specified as Statement Type but no Field Containing SQL was found, FlowFile %s", flowFile));
        }

        boolean schemaHasSqlField = recordSchema.getFields().stream().anyMatch((field) -> sqlField.equals(field.getFieldName()));
        if (!schemaHasSqlField) {
            throw new IllegalArgumentException(format("Record schema does not contain Field Containing SQL: %s, FlowFile %s", sqlField, flowFile));
        }

        try (final Statement statement = connection.createStatement()) {
            final int timeoutMillis = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            try {
                statement.setQueryTimeout(timeoutMillis); // timeout in seconds
            } catch (SQLException se) {
                // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
                if (timeoutMillis > 0) {
                    throw se;
                }
            }

            final ComponentLog log = getLogger();
            final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).evaluateAttributeExpressions(flowFile).asInteger();
            // Batch Size 0 means put all sql statements into one batch update no matter how many statements there are.
            // Do not use batch statements if batch size is equal to 1 because that is the same as not using batching.
            // Also do not use batches if the connection does not support batching.
            boolean useBatch = maxBatchSize != 1 && isSupportsBatchUpdates(connection);
            int currentBatchSize = 0;
            int batchIndex = 0;

            boolean isFirstRecord = true;
            Record nextRecord = recordReader.nextRecord();
            while (nextRecord != null) {
                Record currentRecord = nextRecord;
                final String sql = currentRecord.getAsString(sqlField);
                nextRecord = recordReader.nextRecord();

                if (sql == null || StringUtils.isEmpty(sql)) {
                    throw new MalformedRecordException(format("Record had no (or null) value for Field Containing SQL: %s, FlowFile %s", sqlField, flowFile));
                }

                final String[] sqlStatements;
                if (context.getProperty(ALLOW_MULTIPLE_STATEMENTS).asBoolean()) {
                    final String regex = "(?<!\\\\);";
                    sqlStatements = (sql).split(regex);
                } else {
                    sqlStatements = new String[]{sql};
                }

                if (isFirstRecord) {
                    // If there is only one sql statement to process, then do not use batching.
                    if (nextRecord == null && sqlStatements.length == 1) {
                        useBatch = false;
                    }
                    isFirstRecord = false;
                }

                for (String sqlStatement : sqlStatements) {
                    if (useBatch) {
                        currentBatchSize++;
                        statement.addBatch(sqlStatement);
                    } else {
                        statement.execute(sqlStatement);
                    }
                }

                if (useBatch && maxBatchSize > 0 && currentBatchSize >= maxBatchSize) {
                    batchIndex++;
                    log.debug("Executing batch with last query {} because batch reached max size %s for {}; batch index: {}; batch size: {}",
                            sql, maxBatchSize, flowFile, batchIndex, currentBatchSize);
                    statement.executeBatch();
                    session.adjustCounter("Batches Executed", 1, false);
                    currentBatchSize = 0;
                }
            }

            if (useBatch && currentBatchSize > 0) {
                batchIndex++;
                log.debug("Executing last batch because last statement reached for {}; batch index: {}; batch size: {}",
                        flowFile, batchIndex, currentBatchSize);
                statement.executeBatch();
                session.adjustCounter("Batches Executed", 1, false);
            }
        }
    }


    private void executeDML(final ProcessContext context, final ProcessSession session, final FlowFile flowFile,
                            final Connection con, final RecordReader recordReader, final String explicitStatementType, final DMLSettings settings)
            throws IllegalArgumentException, MalformedRecordException, IOException, SQLException {

        final ComponentLog log = getLogger();

        final String catalog = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String updateKeys = context.getProperty(UPDATE_KEYS).evaluateAttributeExpressions(flowFile).getValue();
        final String deleteKeys = context.getProperty(DELETE_KEYS).evaluateAttributeExpressions(flowFile).getValue();
        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).evaluateAttributeExpressions(flowFile).asInteger();
        final int timeoutMillis = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        final String binaryStringFormat = context.getProperty(BINARY_STRING_FORMAT).evaluateAttributeExpressions(flowFile).getValue();

        // Ensure the table name has been set, the generated SQL statements (and TableSchema cache) will need it
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException(format("Cannot process %s because Table Name is null or empty", flowFile));
        }
        final NameNormalizer normalizer = Optional.of(settings)
                .filter(s -> s.translateFieldNames)
                .map(s -> NameNormalizerFactory.getNormalizer(s.translationStrategy, s.translationPattern))
                .orElse(null);
        final SchemaKey schemaKey = new PutDatabaseRecord.SchemaKey(catalog, schemaName, tableName);
        final TableSchema tableSchema;
        try {
            tableSchema = schemaCache.get(schemaKey, key -> {
                try {
                    final TableSchema schema = TableSchema.from(con, catalog, schemaName, tableName, settings.translateFieldNames, normalizer, updateKeys, log);
                    getLogger().debug("Fetched Table Schema {} for table name {}", schema, tableName);
                    return schema;
                } catch (SQLException e) {
                    // Wrap this in a runtime exception, it is unwrapped in the outer try
                    throw new ProcessException(e);
                }
            });
            if (tableSchema == null) {
                throw new IllegalArgumentException("No table schema specified!");
            }
        } catch (ProcessException pe) {
            // Unwrap the SQLException if one occurred
            if (pe.getCause() instanceof SQLException) {
                throw (SQLException) pe.getCause();
            } else {
                throw pe;
            }
        }

        // build the fully qualified table name
        final String fqTableName = generateTableName(settings, catalog, schemaName, tableName, tableSchema);

        final Map<String, PreparedSqlAndColumns> preparedSql = new HashMap<>();
        int currentBatchSize = 0;
        int batchIndex = 0;
        Record outerRecord;
        PreparedStatement lastPreparedStatement = null;

        try {
            while ((outerRecord = recordReader.nextRecord()) != null) {
                final String statementType;
                if (USE_RECORD_PATH.equalsIgnoreCase(explicitStatementType)) {
                    statementType = recordPathOperationType.apply(outerRecord);
                } else {
                    statementType = explicitStatementType;
                }

                final List<Record> dataRecords = getDataRecords(outerRecord);
                for (final Record currentRecord : dataRecords) {
                    PreparedSqlAndColumns preparedSqlAndColumns = preparedSql.get(statementType);
                    if (preparedSqlAndColumns == null) {
                        final RecordSchema recordSchema = currentRecord.getSchema();

                        final SqlAndIncludedColumns sqlHolder;
                        if (INSERT_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateInsert(recordSchema, fqTableName, tableSchema, settings, normalizer);
                        } else if (UPDATE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateUpdate(recordSchema, fqTableName, updateKeys, tableSchema, settings, normalizer);
                        } else if (DELETE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = generateDelete(recordSchema, fqTableName, deleteKeys, tableSchema, settings, normalizer);
                        } else if (UPSERT_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = getSqlStatement(StatementType.UPSERT, recordSchema, fqTableName, updateKeys, tableSchema, settings, normalizer);
                        } else if (INSERT_IGNORE_TYPE.equalsIgnoreCase(statementType)) {
                            sqlHolder = getSqlStatement(StatementType.INSERT_IGNORE, recordSchema, fqTableName, updateKeys, tableSchema, settings, normalizer);
                        } else {
                            throw new IllegalArgumentException(format("Statement Type %s is not valid, FlowFile %s", statementType, flowFile));
                        }

                        // Log debug sqlHolder
                        log.debug("Generated SQL: {}", sqlHolder.getSql());
                        // Create the Prepared Statement
                        final PreparedStatement preparedStatement = con.prepareStatement(sqlHolder.getSql());

                        try {
                            preparedStatement.setQueryTimeout(timeoutMillis); // timeout in seconds
                        } catch (final SQLException se) {
                            // If the driver doesn't support query timeout, then assume it is "infinite". Allow a timeout of zero only
                            if (timeoutMillis > 0) {
                                throw se;
                            }
                        }

                        final int parameterCount = getParameterCount(sqlHolder.sql);
                        preparedSqlAndColumns = new PreparedSqlAndColumns(sqlHolder, preparedStatement, parameterCount);
                        preparedSql.put(statementType, preparedSqlAndColumns);
                    }

                    final PreparedStatement ps = preparedSqlAndColumns.getPreparedStatement();
                    final List<Integer> fieldIndexes = preparedSqlAndColumns.getSqlAndIncludedColumns().getFieldIndexes();
                    final String sql = preparedSqlAndColumns.getSqlAndIncludedColumns().getSql();

                    if (ps != lastPreparedStatement && lastPreparedStatement != null) {
                        batchIndex++;
                        log.debug("Executing query {} because Statement Type changed between Records for {}; fieldIndexes: {}; batch index: {}; batch size: {}",
                                sql, flowFile, fieldIndexes, batchIndex, currentBatchSize);
                        lastPreparedStatement.executeBatch();

                        session.adjustCounter("Batches Executed", 1, false);
                        currentBatchSize = 0;
                    }
                    lastPreparedStatement = ps;

                    final Object[] values = currentRecord.getValues();
                    final List<DataType> dataTypes = currentRecord.getSchema().getDataTypes();
                    final RecordSchema recordSchema = currentRecord.getSchema();
                    final Map<String, ColumnDescription> columns = tableSchema.getColumns();

                    int deleteIndex = 0;
                    for (int i = 0; i < fieldIndexes.size(); i++) {
                        final int currentFieldIndex = fieldIndexes.get(i);
                        Object currentValue = values[currentFieldIndex];
                        final DataType dataType = dataTypes.get(currentFieldIndex);
                        final int fieldSqlType = DataTypeUtils.getSQLTypeValue(dataType);
                        final String fieldName = recordSchema.getField(currentFieldIndex).getFieldName();
                        String columnName = TableSchema.normalizedName(fieldName, settings.translateFieldNames, normalizer);
                        int sqlType;

                        final ColumnDescription column = columns.get(columnName);
                        // 'column' should not be null here as the fieldIndexes should correspond to fields that match table columns, but better to handle just in case
                        if (column == null) {
                            if (!settings.ignoreUnmappedFields) {
                                throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", columns.keySet()));
                            } else {
                                sqlType = fieldSqlType;
                            }
                        } else {
                            sqlType = column.getDataType();
                            // SQLServer returns -150 for sql_variant from DatabaseMetaData though the server expects -156 when setting a sql_variant parameter
                            if (sqlType == -150) {
                                sqlType = -156;
                            }
                        }

                        // Convert (if necessary) from field data type to column data type
                        if (fieldSqlType != sqlType) {
                            try {
                                DataType targetDataType = DataTypeUtils.getDataTypeFromSQLTypeValue(sqlType);
                                // If sqlType is unsupported, fall back to the fieldSqlType instead
                                if (targetDataType == null) {
                                    targetDataType = DataTypeUtils.getDataTypeFromSQLTypeValue(fieldSqlType);
                                }
                                if (targetDataType != null) {
                                    if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.VARBINARY || sqlType == Types.LONGVARBINARY) {
                                        if (currentValue instanceof Object[]) {
                                            // Convert Object[Byte] arrays to byte[]
                                            Object[] src = (Object[]) currentValue;
                                            if (src.length > 0) {
                                                if (!(src[0] instanceof Byte)) {
                                                    throw new IllegalTypeConversionException("Cannot convert value " + currentValue + " to BLOB/BINARY/VARBINARY/LONGVARBINARY");
                                                }
                                            }
                                            byte[] dest = new byte[src.length];
                                            for (int j = 0; j < src.length; j++) {
                                                dest[j] = (Byte) src[j];
                                            }
                                            currentValue = dest;
                                        } else if (currentValue instanceof String stringValue) {
                                            if (BINARY_STRING_FORMAT_BASE64.getValue().equals(binaryStringFormat)) {
                                                currentValue = Base64.getDecoder().decode(stringValue);
                                            } else if (BINARY_STRING_FORMAT_HEXADECIMAL.getValue().equals(binaryStringFormat)) {
                                                currentValue = HexFormat.of().parseHex(stringValue);
                                            } else {
                                                currentValue = stringValue.getBytes(StandardCharsets.UTF_8);
                                            }
                                        } else if (currentValue != null && !(currentValue instanceof byte[])) {
                                            throw new IllegalTypeConversionException("Cannot convert value " + currentValue + " to BLOB/BINARY/VARBINARY/LONGVARBINARY");
                                        }
                                    } else {
                                        currentValue = DataTypeUtils.convertType(
                                                currentValue,
                                                targetDataType,
                                                fieldName);
                                    }
                                }
                            } catch (IllegalTypeConversionException itce) {
                                // If the field and column types don't match or the value can't otherwise be converted to the column datatype,
                                // try with the original object and field datatype
                                sqlType = DataTypeUtils.getSQLTypeValue(dataType);
                            }
                        }

                        // If DELETE type, insert the object twice if the column is nullable because of the null check (see generateDelete for details)
                        if (DELETE_TYPE.equalsIgnoreCase(statementType)) {
                            setParameter(ps, ++deleteIndex, currentValue, fieldSqlType, sqlType);
                            if (column != null && column.isNullable()) {
                                setParameter(ps, ++deleteIndex, currentValue, fieldSqlType, sqlType);
                            }
                        } else if (UPSERT_TYPE.equalsIgnoreCase(statementType)) {
                            // Calculate the number of times to set the parameter based on fields divided by parameters
                            final int timesToAddObjects = fieldIndexes.size() / preparedSqlAndColumns.parameterCount;
                            for (int j = 0; j < timesToAddObjects; j++) {
                                setParameter(ps, i + (fieldIndexes.size() * j) + 1, currentValue, fieldSqlType, sqlType);
                            }
                        } else {
                            setParameter(ps, i + 1, currentValue, fieldSqlType, sqlType);
                        }
                    }

                    ps.addBatch();
                    session.adjustCounter(statementType + " updates performed", 1, false);
                    if (++currentBatchSize == maxBatchSize) {
                        batchIndex++;
                        log.debug("Executing query {} because batch reached max size for {}; fieldIndexes: {}; batch index: {}; batch size: {}",
                                sql, flowFile, fieldIndexes, batchIndex, currentBatchSize);
                        session.adjustCounter("Batches Executed", 1, false);
                        ps.executeBatch();
                        currentBatchSize = 0;
                    }
                }
            }

            if (currentBatchSize > 0) {
                lastPreparedStatement.executeBatch();
                session.adjustCounter("Batches Executed", 1, false);
            }
        } finally {
            for (final PreparedSqlAndColumns preparedSqlAndColumns : preparedSql.values()) {
                preparedSqlAndColumns.getPreparedStatement().close();
            }
        }
    }

    private void setParameter(PreparedStatement ps, int index, Object value, int fieldSqlType, int sqlType) throws IOException {
        if (sqlType == Types.BLOB) {
            // Convert Byte[] or String (anything that has been converted to byte[]) into BLOB
            if (fieldSqlType == Types.ARRAY || fieldSqlType == Types.VARCHAR) {
                if (!(value instanceof byte[])) {
                    if (value == null) {
                        try {
                            ps.setNull(index, Types.BLOB);
                            return;
                        } catch (SQLException e) {
                            throw new IOException("Unable to setNull() on prepared statement", e);
                        }
                    } else {
                        throw new IOException("Expected BLOB to be of type byte[] but is instead " + value.getClass().getName());
                    }
                }
                byte[] byteArray = (byte[]) value;
                try (InputStream inputStream = new ByteArrayInputStream(byteArray)) {
                    ps.setBlob(index, inputStream);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data " + value, e);
                }
            } else {
                try (InputStream inputStream = new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.UTF_8))) {
                    ps.setBlob(index, inputStream);
                } catch (IOException | SQLException e) {
                    throw new IOException("Unable to parse binary data " + value, e);
                }
            }
        } else if (sqlType == Types.CLOB) {
            if (value == null) {
                try {
                    ps.setNull(index, Types.CLOB);
                } catch (SQLException e) {
                    throw new IOException("Unable to setNull() on prepared statement", e);
                }
            } else {
                try {
                    Clob clob = ps.getConnection().createClob();
                    clob.setString(1, value.toString());
                    ps.setClob(index, clob);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse data as CLOB/String " + value, e);
                }
            }
        } else if (sqlType == Types.VARBINARY || sqlType == Types.LONGVARBINARY) {
            if (fieldSqlType == Types.ARRAY || fieldSqlType == Types.VARCHAR) {
                if (!(value instanceof byte[])) {
                    if (value == null) {
                        try {
                            ps.setNull(index, Types.BLOB);
                            return;
                        } catch (SQLException e) {
                            throw new IOException("Unable to setNull() on prepared statement", e);
                        }
                    } else {
                        throw new IOException("Expected VARBINARY/LONGVARBINARY to be of type byte[] but is instead " + value.getClass().getName());
                    }
                }
                byte[] byteArray = (byte[]) value;
                try {
                    ps.setBytes(index, byteArray);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data with size" + byteArray.length, e);
                }
            } else {
                byte[] byteArray = new byte[0];
                try {
                    byteArray = value.toString().getBytes(StandardCharsets.UTF_8);
                    ps.setBytes(index, byteArray);
                } catch (SQLException e) {
                    throw new IOException("Unable to parse binary data with size" + byteArray.length, e);
                }
            }
        } else {
            try {
                // If the specified field type is OTHER and the SQL type is VARCHAR, the conversion went ok as a string literal but try the OTHER type when setting the parameter. If an error occurs,
                // try the normal way of using the sqlType
                // This helps with PostgreSQL enums and possibly other scenarios
                if (fieldSqlType == Types.OTHER && sqlType == Types.VARCHAR) {
                    try {
                        ps.setObject(index, value, fieldSqlType);
                    } catch (SQLException e) {
                        // Fall back to default setObject params
                        ps.setObject(index, value, sqlType);
                    }
                } else {
                    ps.setObject(index, value, sqlType);
                }
            } catch (SQLException e) {
                throw new IOException("Unable to setObject() with value " + value + " at index " + index + " of type " + sqlType, e);
            }
        }
    }

    private List<Record> getDataRecords(final Record outerRecord) {
        if (dataRecordPath == null) {
            return List.of(outerRecord);
        }

        final RecordPathResult result = dataRecordPath.evaluate(outerRecord);
        final List<FieldValue> fieldValues = result.getSelectedFields().toList();
        if (fieldValues.isEmpty()) {
            throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record yielded no results.");
        }

        for (final FieldValue fieldValue : fieldValues) {
            final RecordFieldType fieldType = fieldValue.getField().getDataType().getFieldType();
            if (fieldType != RecordFieldType.RECORD) {
                throw new ProcessException("RecordPath " + dataRecordPath.getPath() + " evaluated against Record expected to return one or more Records but encountered field of type" +
                        " " + fieldType);
            }
        }

        final List<Record> dataRecords = new ArrayList<>(fieldValues.size());
        for (final FieldValue fieldValue : fieldValues) {
            dataRecords.add((Record) fieldValue.getValue());
        }

        return dataRecords;
    }

    private String getJdbcUrl(final Connection connection) {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            if (databaseMetaData != null) {
                return databaseMetaData.getURL();
            }
        } catch (final Exception e) {
            getLogger().warn("Could not determine JDBC URL based on the Driver Connection.", e);
        }

        return "DBCPService";
    }

    private String getStatementType(final ProcessContext context, final FlowFile flowFile) {
        // Get the statement type from the attribute if necessary
        final String statementTypeProperty = context.getProperty(STATEMENT_TYPE).getValue();
        String statementType = statementTypeProperty;
        if (USE_ATTR_TYPE.equals(statementTypeProperty)) {
            statementType = flowFile.getAttribute(STATEMENT_TYPE_ATTRIBUTE);
        }

        return validateStatementType(statementType, flowFile);
    }

    private String validateStatementType(final String statementType, final FlowFile flowFile) {
        if (statementType == null || statementType.trim().isEmpty()) {
            throw new ProcessException("No Statement Type specified for " + flowFile);
        }

        if (INSERT_TYPE.equalsIgnoreCase(statementType) || UPDATE_TYPE.equalsIgnoreCase(statementType) || DELETE_TYPE.equalsIgnoreCase(statementType)
                || UPSERT_TYPE.equalsIgnoreCase(statementType) || SQL_TYPE.equalsIgnoreCase(statementType) || USE_RECORD_PATH.equalsIgnoreCase(statementType)
                || INSERT_IGNORE_TYPE.equalsIgnoreCase(statementType)) {

            return statementType;
        }

        throw new ProcessException("Invalid Statement Type <" + statementType + "> for " + flowFile);
    }

    private void putToDatabase(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Connection connection) throws Exception {
        final String statementType = getStatementType(context, flowFile);

        try (final InputStream in = session.read(flowFile)) {
            final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
            final RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, in, getLogger());

            if (SQL_TYPE.equalsIgnoreCase(statementType)) {
                executeSQL(context, session, flowFile, connection, recordReader);
            } else {
                final DMLSettings settings = new DMLSettings(context);
                executeDML(context, session, flowFile, connection, recordReader, statementType, settings);
            }
        }
    }

    String generateTableName(final DMLSettings settings, final String catalog, final String schemaName, final String tableName, final TableSchema tableSchema) {
        final StringBuilder tableNameBuilder = new StringBuilder();
        if (catalog != null) {
            if (settings.quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(catalog)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(catalog);
            }

            tableNameBuilder.append(".");
        }

        if (schemaName != null) {
            if (settings.quoteTableName) {
                tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                        .append(schemaName)
                        .append(tableSchema.getQuotedIdentifierString());
            } else {
                tableNameBuilder.append(schemaName);
            }

            tableNameBuilder.append(".");
        }

        if (settings.quoteTableName) {
            tableNameBuilder.append(tableSchema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(tableSchema.getQuotedIdentifierString());
        } else {
            tableNameBuilder.append(tableName);
        }

        return tableNameBuilder.toString();
    }

    private Set<String> getNormalizedColumnNames(final RecordSchema schema, final boolean translateFieldNames, NameNormalizer normalizer) {
        final Set<String> normalizedFieldNames = new HashSet<>();
        if (schema != null) {
            schema.getFieldNames().forEach((fieldName) -> normalizedFieldNames.add(TableSchema.normalizedName(fieldName, translateFieldNames, normalizer)));
        }
        return normalizedFieldNames;
    }

    SqlAndIncludedColumns generateInsert(final RecordSchema recordSchema, final String tableName, final TableSchema tableSchema, final DMLSettings settings, NameNormalizer normalizer)
            throws IllegalArgumentException, SQLException {

        checkValuesForRequiredColumns(recordSchema, tableSchema, settings, normalizer);

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ");
        sqlBuilder.append(tableName);
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

                final ColumnDescription desc = tableSchema.getColumns().get(TableSchema.normalizedName(fieldName, settings.translateFieldNames, normalizer));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
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
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '{}' to any column in the database\n{}Columns: {}",
                            fieldName, (settings.translateFieldNames ? "Normalized " : ""), String.join(",", tableSchema.getColumns().keySet()));
                }
            }

            // complete the SQL statements by adding ?'s for all of the values to be escaped.
            sqlBuilder.append(") VALUES (");
            sqlBuilder.append(StringUtils.repeat("?", ",", includedColumns.size()));
            sqlBuilder.append(")");

            if (fieldsFound.get() == 0) {
                throw new SQLDataException("None of the fields in the record map to the columns defined by the " + tableName + " table\n"
                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
            }
        }
        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    private SqlAndIncludedColumns getSqlStatement(
            final StatementType statementType,
            final RecordSchema recordSchema,
            final String qualifiedTableName,
            final String updateKeys,
            final TableSchema tableSchema,
            final DMLSettings settings,
            final NameNormalizer normalizer
    ) throws MalformedRecordException, SQLDataException, SQLIntegrityConstraintViolationException {
        checkValuesForRequiredColumns(recordSchema, tableSchema, settings, normalizer);

        final Set<String> keyColumnNames = getUpdateKeyColumnNames(qualifiedTableName, updateKeys, tableSchema);
        normalizeKeyColumnNamesAndCheckForValues(recordSchema, updateKeys, settings, keyColumnNames, normalizer);

        final List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        final List<Integer> usedColumnIndices = new ArrayList<>();
        final List<String> fieldNames = recordSchema.getFieldNames();
        final int fieldCount = fieldNames.size();
        for (int i = 0; i < fieldCount; i++) {
            final RecordField field = recordSchema.getField(i);
            final String fieldName = field.getFieldName();

            final String columnNameNormalized = TableSchema.normalizedName(fieldName, settings.translateFieldNames, normalizer);
            final ColumnDescription columnDescription = tableSchema.getColumns().get(columnNameNormalized);
            if (columnDescription == null) {
                if (settings.ignoreUnmappedFields) {
                    continue;
                } else {
                    final String tableColumnNames = String.join(",", tableSchema.getColumns().keySet());
                    final String message = "Record Field [%s] not mapped to Table Columns [%s]".formatted(fieldName, tableColumnNames);
                    throw new SQLDataException(message);
                }
            }

            final String columnName;
            if (settings.escapeColumnNames) {
                final String quotedIdentifier = tableSchema.getQuotedIdentifierString();
                columnName = quotedIdentifier + columnDescription.getColumnName() + quotedIdentifier;
            } else {
                columnName = columnDescription.getColumnName();
            }

            final ColumnDefinition columnDefinition = getColumnDefinition(columnDescription, keyColumnNames, columnName);
            columnDefinitions.add(columnDefinition);

            usedColumnIndices.add(i);
        }

        final TableDefinition tableDefinition = new TableDefinition(
                Optional.empty(),
                Optional.empty(),
                qualifiedTableName,
                columnDefinitions
        );
        final StatementRequest statementRequest = new StandardStatementRequest(statementType, tableDefinition);
        final StatementResponse statementResponse = databaseDialectService.getStatement(statementRequest);
        final String sql = statementResponse.sql();

        return new SqlAndIncludedColumns(sql, usedColumnIndices);
    }

    private ColumnDefinition getColumnDefinition(final ColumnDescription columnDescription, final Set<String> keyColumnNames, final String columnName) {
        final int dataType = columnDescription.getDataType();
        final boolean primaryKey = keyColumnNames.contains(columnDescription.getColumnName());
        final StandardColumnDefinition.Nullable nullable = columnDescription.isNullable() ? StandardColumnDefinition.Nullable.YES : StandardColumnDefinition.Nullable.NO;
        return new StandardColumnDefinition(
                columnName,
                dataType,
                nullable,
                Optional.empty(),
                primaryKey
        );
    }

    SqlAndIncludedColumns generateUpdate(final RecordSchema recordSchema, final String tableName, final String updateKeys,
                                         final TableSchema tableSchema, final DMLSettings settings, NameNormalizer normalizer)
            throws IllegalArgumentException, MalformedRecordException, SQLException {

        final Set<String> keyColumnNames = getUpdateKeyColumnNames(tableName, updateKeys, tableSchema);
        final Set<String> normalizedKeyColumnNames = normalizeKeyColumnNamesAndCheckForValues(recordSchema, updateKeys, settings, keyColumnNames, normalizer);

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE ");
        sqlBuilder.append(tableName);

        // iterate over all the fields in the record, building the SQL statement by adding the column names
        List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            sqlBuilder.append(" SET ");

            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final String normalizedColName = TableSchema.normalizedName(fieldName, settings.translateFieldNames, normalizer);
                final ColumnDescription desc = tableSchema.getColumns().get(TableSchema.normalizedName(fieldName, settings.translateFieldNames, normalizer));
                if (desc == null) {
                    if (!settings.ignoreUnmappedFields) {
                        throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                                + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                    } else {
                        // User is ignoring unmapped fields, but log at debug level just in case
                        getLogger().debug("Did not map field '{}' to any column in the database\nColumns: {}",
                                fieldName, (settings.translateFieldNames ? "Normalized " : ""), String.join(",", tableSchema.getColumns().keySet()));
                        continue;
                    }
                }

                // Check if this column is an Update Key. If so, skip it for now. We will come
                // back to it after we finish the SET clause
                if (!normalizedKeyColumnNames.contains(normalizedColName)) {
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

                    sqlBuilder.append(" = ?");
                    includedColumns.add(i);
                }
            }

            AtomicInteger whereFieldCount = new AtomicInteger(0);
            for (int i = 0; i < fieldCount; i++) {
                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();
                boolean firstUpdateKey = true;

                final String normalizedColName = TableSchema.normalizedName(fieldName, settings.translateFieldNames, normalizer);
                final ColumnDescription desc = tableSchema.getColumns().get(TableSchema.normalizedName(fieldName, settings.translateFieldNames, normalizer));
                if (desc != null) {

                    // Check if this column is a Update Key. If so, add it to the WHERE clause
                    if (normalizedKeyColumnNames.contains(normalizedColName)) {

                        if (whereFieldCount.getAndIncrement() > 0) {
                            sqlBuilder.append(" AND ");
                        } else if (firstUpdateKey) {
                            // Set the WHERE clause based on the Update Key values
                            sqlBuilder.append(" WHERE ");
                            firstUpdateKey = false;
                        }

                        if (settings.escapeColumnNames) {
                            sqlBuilder.append(tableSchema.getQuotedIdentifierString())
                                    .append(desc.getColumnName())
                                    .append(tableSchema.getQuotedIdentifierString());
                        } else {
                            sqlBuilder.append(desc.getColumnName());
                        }
                        sqlBuilder.append(" = ?");
                        includedColumns.add(i);
                    }
                }
            }
        }
        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    SqlAndIncludedColumns generateDelete(final RecordSchema recordSchema, final String tableName, String deleteKeys, final TableSchema tableSchema,
                                         final DMLSettings settings, NameNormalizer normalizer)
            throws IllegalArgumentException, MalformedRecordException, SQLDataException {

        final Set<String> normalizedFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames, normalizer);
        for (final String requiredColName : tableSchema.getRequiredColumnNames()) {
            final String normalizedColName = TableSchema.normalizedName(requiredColName, settings.translateFieldNames, normalizer);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "Record does not have a value for the Required column '" + requiredColName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new MalformedRecordException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DELETE FROM ");
        sqlBuilder.append(tableName);

        // iterate over all of the fields in the record, building the SQL statement by adding the column names
        final List<String> fieldNames = recordSchema.getFieldNames();
        final List<Integer> includedColumns = new ArrayList<>();
        if (fieldNames != null) {
            sqlBuilder.append(" WHERE ");
            int fieldCount = fieldNames.size();
            AtomicInteger fieldsFound = new AtomicInteger(0);

            // If 'deleteKeys' is not specified by the user, then all columns of the table
            // should be used in the 'WHERE' clause, in order to keep the original behavior.
            final Set<String> deleteKeysSet;
            if (deleteKeys == null) {
                deleteKeysSet = new HashSet<>(fieldNames);
            } else {
                deleteKeysSet = Arrays.stream(deleteKeys.split(","))
                        .map(String::trim)
                        .collect(Collectors.toSet());
            }

            for (int i = 0; i < fieldCount; i++) {
                if (!deleteKeysSet.contains(fieldNames.get(i))) {
                    continue; // skip this field if it should not be included in 'WHERE'
                }

                RecordField field = recordSchema.getField(i);
                String fieldName = field.getFieldName();

                final ColumnDescription desc = tableSchema.getColumns().get(TableSchema.normalizedName(fieldName, settings.translateFieldNames, normalizer));
                if (desc == null && !settings.ignoreUnmappedFields) {
                    throw new SQLDataException("Cannot map field '" + fieldName + "' to any column in the database\n"
                            + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
                }

                if (desc != null) {
                    if (fieldsFound.getAndIncrement() > 0) {
                        sqlBuilder.append(" AND ");
                    }

                    String columnName;
                    if (settings.escapeColumnNames) {
                        columnName = tableSchema.getQuotedIdentifierString() + desc.getColumnName() + tableSchema.getQuotedIdentifierString();
                    } else {
                        columnName = desc.getColumnName();
                    }
                    // Need to build a null-safe construct for the WHERE clause, since we are using PreparedStatement and won't know if the values are null. If they are null,
                    // then the filter should be "column IS null" vs "column = null". Since we don't know whether the value is null, we can use the following construct (from NIFI-3742):
                    //   (column = ? OR (column is null AND ? is null))
                    sqlBuilder.append("(");
                    sqlBuilder.append(columnName);
                    sqlBuilder.append(" = ?");

                    // Only need null check if the column is nullable, otherwise the row wouldn't exist
                    if (desc.isNullable()) {
                        sqlBuilder.append(" OR (");
                        sqlBuilder.append(columnName);
                        sqlBuilder.append(" is null AND ? is null))");
                    } else {
                        sqlBuilder.append(")");
                    }
                    includedColumns.add(i);
                } else {
                    // User is ignoring unmapped fields, but log at debug level just in case
                    getLogger().debug("Did not map field '{}' to any column in the database\n{}Columns: {}",
                            fieldName, (settings.translateFieldNames ? "Normalized " : ""), String.join(",", tableSchema.getColumns().keySet()));
                }
            }

            if (fieldsFound.get() == 0) {
                throw new SQLDataException("None of the fields in the record map to the columns defined by the " + tableName + " table\n"
                        + (settings.translateFieldNames ? "Normalized " : "") + "Columns: " + String.join(",", tableSchema.getColumns().keySet()));
            }
        }

        return new SqlAndIncludedColumns(sqlBuilder.toString(), includedColumns);
    }

    private void checkValuesForRequiredColumns(RecordSchema recordSchema, TableSchema tableSchema, DMLSettings settings, NameNormalizer normalizer) {
        final Set<String> normalizedFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames, normalizer);

        for (final String requiredColName : tableSchema.getRequiredColumnNames()) {
            final String normalizedColName = TableSchema.normalizedName(requiredColName, settings.translateFieldNames, normalizer);
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
    }

    private Set<String> getUpdateKeyColumnNames(String tableName, String updateKeys, TableSchema tableSchema) throws SQLIntegrityConstraintViolationException {
        final Set<String> updateKeyColumnNames;

        if (updateKeys == null) {
            updateKeyColumnNames = tableSchema.getPrimaryKeyColumnNames();
        } else {
            updateKeyColumnNames = new HashSet<>();
            for (final String updateKey : updateKeys.split(",")) {
                updateKeyColumnNames.add(updateKey.trim());
            }
        }

        if (updateKeyColumnNames.isEmpty()) {
            throw new SQLIntegrityConstraintViolationException("Table '" + tableName + "' not found or does not have a Primary Key and no Update Keys were specified");
        }

        return updateKeyColumnNames;
    }

    private Set<String> normalizeKeyColumnNamesAndCheckForValues(RecordSchema recordSchema, String updateKeys, DMLSettings settings, Set<String> updateKeyColumnNames, NameNormalizer normalizer)
            throws MalformedRecordException {
        // Create a Set of all normalized Update Key names, and ensure that there is a field in the record
        // for each of the Update Key fields.
        final Set<String> normalizedRecordFieldNames = getNormalizedColumnNames(recordSchema, settings.translateFieldNames, normalizer);

        final Set<String> normalizedKeyColumnNames = new HashSet<>();
        for (final String updateKeyColumnName : updateKeyColumnNames) {
            String normalizedKeyColumnName = TableSchema.normalizedName(updateKeyColumnName, settings.translateFieldNames, normalizer);

            if (!normalizedRecordFieldNames.contains(normalizedKeyColumnName)) {
                String missingColMessage = "Record does not have a value for the " + (updateKeys == null ? "Primary" : "Update") + "Key column '" + updateKeyColumnName + "'";
                if (settings.failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new MalformedRecordException(missingColMessage);
                } else if (settings.warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
            normalizedKeyColumnNames.add(normalizedKeyColumnName);
        }

        return normalizedKeyColumnNames;
    }

    private boolean isSupportsBatchUpdates(Connection connection) {
        try {
            return connection.getMetaData().supportsBatchUpdates();
        } catch (Exception ex) {
            getLogger().debug("Exception while testing if connection supportsBatchUpdates", ex);
            return false;
        }
    }

    private int getParameterCount(final String sql) {
        int parameterCount = 0;
        for (char character : sql.toCharArray()) {
            if ('?' == character) {
                parameterCount++;
            }
        }
        return parameterCount;
    }

    static class SchemaKey {
        private final String catalog;
        private final String schemaName;
        private final String tableName;

        public SchemaKey(final String catalog, final String schemaName, final String tableName) {
            this.catalog = catalog;
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        @Override
        public int hashCode() {
            int result = catalog != null ? catalog.hashCode() : 0;
            result = 31 * result + (schemaName != null ? schemaName.hashCode() : 0);
            result = 31 * result + tableName.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SchemaKey schemaKey = (SchemaKey) o;

            if (catalog != null ? !catalog.equals(schemaKey.catalog) : schemaKey.catalog != null) return false;
            if (schemaName != null ? !schemaName.equals(schemaKey.schemaName) : schemaKey.schemaName != null)
                return false;
            return tableName.equals(schemaKey.tableName);
        }
    }

    /**
     * A holder class for a SQL prepared statement and a BitSet indicating which columns are being updated (to determine which values from the record to set on the statement)
     * A value of null for getIncludedColumns indicates that all columns/fields should be included.
     */
    static class SqlAndIncludedColumns {
        private final String sql;
        private final List<Integer> fieldIndexes;

        /**
         * Constructor
         *
         * @param sql          The prepared SQL statement (including parameters notated by ? )
         * @param fieldIndexes A List of record indexes. The index of the list is the location of the record field in the SQL prepared statement
         */
        public SqlAndIncludedColumns(final String sql, final List<Integer> fieldIndexes) {
            this.sql = sql;
            this.fieldIndexes = fieldIndexes;
        }

        public String getSql() {
            return sql;
        }

        public List<Integer> getFieldIndexes() {
            return fieldIndexes;
        }
    }

    static class PreparedSqlAndColumns {
        private final SqlAndIncludedColumns sqlAndIncludedColumns;
        private final PreparedStatement preparedStatement;
        private final int parameterCount;

        public PreparedSqlAndColumns(final SqlAndIncludedColumns sqlAndIncludedColumns, final PreparedStatement preparedStatement, final int parameterCount) {
            this.sqlAndIncludedColumns = sqlAndIncludedColumns;
            this.preparedStatement = preparedStatement;
            this.parameterCount = parameterCount;
        }

        public SqlAndIncludedColumns getSqlAndIncludedColumns() {
            return sqlAndIncludedColumns;
        }

        public PreparedStatement getPreparedStatement() {
            return preparedStatement;
        }
    }

    private static class RecordPathStatementType implements Function<Record, String> {
        private final RecordPath recordPath;

        public RecordPathStatementType(final RecordPath recordPath) {
            this.recordPath = recordPath;
        }

        @Override
        public String apply(final Record record) {
            final RecordPathResult recordPathResult = recordPath.evaluate(record);
            final List<FieldValue> resultList = recordPathResult.getSelectedFields().distinct().toList();
            if (resultList.isEmpty()) {
                throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record but got no results");
            }

            if (resultList.size() > 1) {
                throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record and received multiple distinct results (" + resultList + ")");
            }

            final String resultValue = String.valueOf(resultList.getFirst().getValue()).toUpperCase();

            return switch (resultValue) {
                case INSERT_TYPE, UPDATE_TYPE, DELETE_TYPE, UPSERT_TYPE -> resultValue;
                case "C", "R" -> INSERT_TYPE;
                case "U" -> UPDATE_TYPE;
                case "D" -> DELETE_TYPE;
                default ->
                        throw new ProcessException("Evaluated RecordPath " + recordPath.getPath() + " against Record to determine Statement Type but found invalid value: " + resultValue);
            };
        }
    }

    static class DMLSettings {
        private final boolean translateFieldNames;
        private final TranslationStrategy translationStrategy;
        private final Pattern translationPattern;
        private final boolean ignoreUnmappedFields;

        // Is the unmatched column behaviour fail or warning?
        private final boolean failUnmappedColumns;
        private final boolean warningUnmappedColumns;

        // Escape column names?
        private final boolean escapeColumnNames;

        // Quote table name?
        private final boolean quoteTableName;

        DMLSettings(ProcessContext context) {
            translateFieldNames = context.getProperty(TRANSLATE_FIELD_NAMES).asBoolean();
            translationStrategy = TranslationStrategy.valueOf(context.getProperty(TRANSLATION_STRATEGY).getValue());
            final String translationRegex = context.getProperty(TRANSLATION_PATTERN).getValue();
            translationPattern = translationRegex == null ? null : Pattern.compile(translationRegex);
            ignoreUnmappedFields = IGNORE_UNMATCHED_FIELD.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_FIELD_BEHAVIOR).getValue());

            failUnmappedColumns = FAIL_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());
            warningUnmappedColumns = WARNING_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());

            escapeColumnNames = context.getProperty(QUOTE_IDENTIFIERS).asBoolean();
            quoteTableName = context.getProperty(QUOTE_TABLE_IDENTIFIER).asBoolean();
        }
    }

}
