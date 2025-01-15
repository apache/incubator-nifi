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
package org.apache.nifi.db.schemaregistry;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Tags({"schema", "registry", "database", "table"})
@CapabilityDescription("Provides a service for generating a record schema from a database table definition. The service is configured "
        + "to use a table name and a database connection fetches the table metadata (i.e. table definition) such as column names, data types, "
        + "nullability, etc.")
public class DatabaseTableSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME);

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain a connection to the database for retrieving table information.")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor CATALOG_NAME = new PropertyDescriptor.Builder()
            .name("Catalog Name")
            .displayName("Catalog Name")
            .description("The name of the catalog used to locate the desired table. This may not apply for the database that you are querying. In this case, leave the field empty. Note that if the "
                    + "property is set and the database is case-sensitive, the catalog name must match the database's catalog name exactly.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("Schema Name")
            .displayName("Schema Name")
            .description("The name of the schema that the table belongs to. This may not apply for the database that you are updating. In this case, leave the field empty. Note that if the "
                    + "property is set and the database is case-sensitive, the schema name must match the database's schema name exactly. Also notice that if the same table name exists in multiple "
                    + "schemas and Schema Name is not specified, the service will find those tables and give an error if the different tables have the same column name(s).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        DBCP_SERVICE,
        CATALOG_NAME,
        SCHEMA_NAME
    );

    private volatile DBCPService dbcpService;
    private volatile String dbCatalogName;
    private volatile String dbSchemaName;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        dbCatalogName = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions().getValue();
        dbSchemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions().getValue();
    }

    @Override
    public RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        if (schemaIdentifier.getName().isPresent()) {
            return retrieveSchemaByName(schemaIdentifier);
        } else {
            throw new SchemaNotFoundException("This Schema Registry only supports retrieving a schema by name.");
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }

    RecordSchema retrieveSchemaByName(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final Optional<String> schemaName = schemaIdentifier.getName();
        if (schemaName.isEmpty()) {
            throw new SchemaNotFoundException("Cannot retrieve schema because Schema Name is not present");
        }

        final String tableName = schemaName.get();
        try {
            try (final Connection conn = dbcpService.getConnection()) {
                final DatabaseMetaData databaseMetaData = conn.getMetaData();
                    return getRecordSchemaFromMetadata(databaseMetaData, tableName);
                }
        } catch (SQLException sqle) {
            throw new IOException("Error retrieving schema for table " + schemaName.get(), sqle);
        }
    }

    private RecordSchema getRecordSchemaFromMetadata(final DatabaseMetaData databaseMetaData, final String tableName) throws SQLException, SchemaNotFoundException {
        try (final ResultSet columnResultSet = databaseMetaData.getColumns(dbCatalogName, dbSchemaName, tableName, "%")) {

            final List<RecordField> recordFields = new ArrayList<>();
            while (columnResultSet.next()) {
                recordFields.add(createRecordFieldFromColumn(columnResultSet));
            }

            // If no columns are found, check that the table exists
            if (recordFields.isEmpty()) {
                checkTableExists(databaseMetaData, tableName);
            }
            return new SimpleRecordSchema(recordFields);
        }
    }

    private RecordField createRecordFieldFromColumn(final ResultSet columnResultSet) throws SQLException {
        // COLUMN_DEF must be read first to work around Oracle bug, see NIFI-4279 for details
        final String defaultValue = columnResultSet.getString("COLUMN_DEF");
        final String columnName = columnResultSet.getString("COLUMN_NAME");
        String typeName = columnResultSet.getString("TYPE_NAME");
        final int dataType;
        if (typeName.equalsIgnoreCase("bool")) {
            dataType = 16;
        } else {
            dataType = columnResultSet.getInt("DATA_TYPE");
        }
        final String nullableValue = columnResultSet.getString("IS_NULLABLE");
        final boolean isNullable = "YES".equalsIgnoreCase(nullableValue) || nullableValue.isEmpty();
        return new RecordField(
                columnName,
                DataTypeUtils.getDataTypeFromSQLTypeValue(dataType),
                defaultValue,
                isNullable);
    }

    private void checkTableExists(final DatabaseMetaData databaseMetaData, final String tableName) throws SchemaNotFoundException, SQLException {
        try (final ResultSet tablesResultSet = databaseMetaData.getTables(dbCatalogName, dbSchemaName, tableName, null)) {
            final List<String> qualifiedNameSegments = new ArrayList<>();
            if (dbCatalogName != null) {
                qualifiedNameSegments.add(dbCatalogName);
            }
            if (dbSchemaName != null) {
                qualifiedNameSegments.add(dbSchemaName);
            }
            qualifiedNameSegments.add(tableName);

            final String qualifiedTableName = String.join(".", qualifiedNameSegments);
            if (tablesResultSet.next()) {
                getLogger().warn("No columns found for Table [{}] check permissions for retrieving schema definitions", qualifiedTableName);
            } else {
                throw new SchemaNotFoundException(String.format("Table [%s] not found", qualifiedTableName));
            }
        }
    }
}