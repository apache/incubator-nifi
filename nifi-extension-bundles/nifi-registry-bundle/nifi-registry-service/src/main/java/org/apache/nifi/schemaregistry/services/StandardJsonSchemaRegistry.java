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

import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.json.schema.JsonSchema;
import org.apache.nifi.schema.access.JsonSchemaRegistryComponent;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.json.schema.SchemaVersion;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Tags({"schema", "registry", "json"})
@CapabilityDescription("Provides a service for registering and accessing JSON schemas. One can register a schema "
        + "as a dynamic property where 'name' represents the schema name and 'value' represents the textual "
        + "representation of the actual schema following the syntax and semantics of the JSON Schema format. "
        + "Empty schemas and schemas only consisting of whitespace are not acceptable schemas."
        + "The registry is heterogeneous registry as it can store schemas of different schema draft versions. "
        + "By default the registry is configured to store schemas of Draft 2020-12. When a schema is added, the version "
        + "which is currently is set, is what the schema is saved as.")
@DynamicProperty(name = "Schema Name", value = "Schema Content",
        description = "Adds a named schema using the JSON string representation of a JSON schema",
        expressionLanguageScope = ExpressionLanguageScope.NONE)
public class StandardJsonSchemaRegistry extends AbstractControllerService implements JsonSchemaRegistry, JsonSchemaRegistryComponent {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SCHEMA_VERSION
    );

    private final ConcurrentMap<String, JsonSchema> jsonSchemas;
    private final ConcurrentMap<SchemaVersion, JsonSchemaFactory> schemaFactories;
    private volatile SchemaVersion schemaVersion;

    public StandardJsonSchemaRegistry() {
        jsonSchemas = new ConcurrentHashMap<>();
        schemaFactories = Arrays.stream(SchemaVersion.values())
                .collect(Collectors.toConcurrentMap(Function.identity(),
                        schemaDraftVersion -> JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.fromId(schemaDraftVersion.getUri()).get())));
        schemaVersion = SchemaVersion.valueOf(SCHEMA_VERSION.getDefaultValue());
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (SCHEMA_VERSION.getName().equals(descriptor.getName()) && !newValue.equals(oldValue)) {
            schemaVersion = SchemaVersion.valueOf(newValue);
        } else if (descriptor.isDynamic() && isBlank(newValue)) {
            jsonSchemas.remove(descriptor.getName());
        } else if (descriptor.isDynamic() && isNotBlank(newValue)) {
            try {
                final String schemaName = descriptor.getName();
                final JsonSchemaFactory jsonSchemaFactory = schemaFactories.get(schemaVersion);
                jsonSchemaFactory.getSchema(newValue);
                jsonSchemas.put(schemaName, new JsonSchema(schemaVersion, newValue));
            } catch (final Exception e) {
                getLogger().debug("Exception thrown when changing value of schema name '{}' from '{}' to '{}'",
                        descriptor.getName(), oldValue, newValue, e);
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Set<ValidationResult> results = new HashSet<>();

        final boolean noSchemasConfigured = validationContext.getProperties().keySet().stream()
                .noneMatch(PropertyDescriptor::isDynamic);
        if (noSchemasConfigured) {
            results.add(new ValidationResult.Builder()
                    .subject("Supported Dynamic Property Descriptor")
                    .valid(false)
                    .explanation("There must be at least one JSON schema specified")
                    .build());
        } else {
            // Iterate over dynamic properties, validating only newly added schemas, and adding results
            schemaVersion = SchemaVersion.valueOf(validationContext.getProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION).getValue());
            validationContext.getProperties().entrySet().stream()
                    .filter(entry -> entry.getKey().isDynamic() && !jsonSchemas.containsKey(entry.getKey().getName()))
                    .forEach(entry -> {
                        String subject = entry.getKey().getName();
                        String input = entry.getValue();
                        if (isNotBlank(input)) {
                            try {
                                final JsonSchemaFactory jsonSchemaFactory = schemaFactories.get(schemaVersion);
                                jsonSchemaFactory.getSchema(input);
                            } catch (Exception e) {
                                results.add(new ValidationResult.Builder()
                                        .input(input)
                                        .subject(subject)
                                        .valid(false)
                                        .explanation("Not a valid JSON Schema: " + e.getMessage())
                                        .build());
                            }
                        }
                    });
        }

        return results;
    }

    @Override
    public JsonSchema retrieveSchema(final String schemaName) throws SchemaNotFoundException {
        JsonSchema jsonSchema = jsonSchemas.get(schemaName);
        if (jsonSchema == null) {
            throw new SchemaNotFoundException("Unable to find schema with name '" + schemaName + "'");
        }
        return jsonSchema;
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
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .build();
    }
}
