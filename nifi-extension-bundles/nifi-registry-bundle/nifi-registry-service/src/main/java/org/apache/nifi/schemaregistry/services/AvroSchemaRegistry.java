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
package org.apache.nifi.schemaregistry.services;

import org.apache.avro.Schema;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Tags({"schema", "registry", "avro", "json", "csv"})
@CapabilityDescription("Provides a service for registering and accessing schemas. You can register a schema "
    + "as a dynamic property where 'name' represents the schema name and 'value' represents the textual "
    + "representation of the actual schema following the syntax and semantics of Avro's Schema format.")
@DynamicProperty(name = "Schema name", value = "Schema Content",
        description = "Adds a named schema using the JSON string representation of an Avro schema",
        expressionLanguageScope = ExpressionLanguageScope.NONE)
public class AvroSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT, SchemaField.SCHEMA_TEXT_FORMAT);
    private final ConcurrentMap<String, RecordSchema> recordSchemas = new ConcurrentHashMap<>();

    static final PropertyDescriptor VALIDATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("avro-reg-validated-field-names")
            .displayName("Validate Field Names")
            .description("Whether or not to validate the field names in the Avro schema based on Avro naming rules. If set to true, all field names must be valid Avro names, "
                    + "which must begin with [A-Za-z_], and subsequently contain only [A-Za-z0-9_]. If set to false, no validation will be performed on the field names.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            VALIDATE_FIELD_NAMES
    );

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isDynamic()) {
            // Dynamic property = schema, validate it
            if (newValue == null) {
                recordSchemas.remove(descriptor.getName());
            } else {
                try {
                    // Use a non-strict parser here, a strict parse can be done (if specified) in customValidate().
                    final Schema avroSchema = new Schema.Parser().setValidate(false).parse(newValue);
                    final SchemaIdentifier schemaId = SchemaIdentifier.builder().name(descriptor.getName()).build();
                    final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema, newValue, schemaId);
                    recordSchemas.put(descriptor.getName(), recordSchema);
                } catch (final Exception e) {
                    // not a problem - the service won't be valid and the validation message will indicate what is wrong.
                }
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();
        boolean strict = validationContext.getProperty(VALIDATE_FIELD_NAMES).asBoolean();

        // Iterate over dynamic properties, validating the schemas, and adding results
        validationContext.getProperties().entrySet().stream().filter(entry -> entry.getKey().isDynamic()).forEach(entry -> {
            String subject = entry.getKey().getDisplayName();
            String input = entry.getValue();

            try {
                final Schema avroSchema = new Schema.Parser().setValidate(strict).parse(input);
                AvroTypeUtil.createSchema(avroSchema, input, SchemaIdentifier.EMPTY);
            } catch (final Exception e) {
                results.add(new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(false)
                        .explanation("Not a valid Avro Schema: " + e.getMessage())
                        .build());
            }
        });
        return results;
    }

    private RecordSchema retrieveSchemaByName(final String schemaName) throws SchemaNotFoundException {
        final RecordSchema recordSchema = recordSchemas.get(schemaName);
        if (recordSchema == null) {
            throw new SchemaNotFoundException("Unable to find schema with name '" + schemaName + "'");
        }
        return recordSchema;
    }

    @Override
    public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final Optional<String> schemaName = schemaIdentifier.getName();
        if (schemaName.isPresent()) {
            return retrieveSchemaByName(schemaName.get());
        } else {
            throw new SchemaNotFoundException("This Schema Registry only supports retrieving a schema by name.");
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    }


    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
