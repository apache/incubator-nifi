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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.json.schema.JsonSchema;
import org.apache.nifi.schema.access.JsonSchemaRegistryComponent;
import org.apache.nifi.json.schema.SchemaVersion;
import org.apache.nifi.schemaregistry.services.JsonSchemaRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"JSON", "schema", "validation"})
@WritesAttributes({
    @WritesAttribute(attribute = ValidateJson.ERROR_ATTRIBUTE_KEY, description = "If the flow file is routed to the invalid relationship "
            + ", this attribute will contain the error message resulting from the validation failure.")
})
@CapabilityDescription("Validates the contents of FlowFiles against a configurable JSON Schema. See json-schema.org for specification standards. " +
        "This Processor does not support input containing multiple JSON objects, such as newline-delimited JSON. If the input FlowFile contains " +
        "newline-delimited JSON, only the first line will be validated."
)
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Validating JSON requires reading FlowFile content into memory")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Schema configuration can reference resources over HTTP"
                )
        }
)
public class ValidateJson extends AbstractProcessor {
    public enum JsonSchemaStrategy implements DescribedValue {
        SCHEMA_NAME_PROPERTY(SCHEMA_NAME_PROPERTY_NAME + " Property",
                "The name of the Schema to use is specified by the '" + SCHEMA_NAME_PROPERTY_NAME +
                        "' Property. The value of this property is used to lookup the Schema in the configured JSON Schema Registry Service."),
        SCHEMA_CONTENT_PROPERTY(SCHEMA_CONTENT_PROPERTY_NAME + " Property",
                "A URL or file path to the JSON schema or the actual JSON schema is specified by the '" + SCHEMA_CONTENT_PROPERTY_NAME + "' Property. " +
                        "No matter how the JSON schema is specified, it must be a valid JSON schema");

        JsonSchemaStrategy(String displayName, String description) {
            this.displayName = displayName;
            this.description = description;
        }

        private final String displayName;
        private final String description;

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    protected static final String ERROR_ATTRIBUTE_KEY = "json.validation.errors";
    private static final String SCHEMA_NAME_PROPERTY_NAME = "Schema Name";
    private static final String SCHEMA_CONTENT_PROPERTY_NAME = "JSON Schema";

    public static final PropertyDescriptor SCHEMA_ACCESS_STRATEGY = new PropertyDescriptor.Builder()
            .name("Schema Access Strategy")
            .displayName("Schema Access Strategy")
            .description("Specifies how to obtain the schema that is to be used for interpreting the data.")
            .allowableValues(JsonSchemaStrategy.class)
            .defaultValue(JsonSchemaStrategy.SCHEMA_CONTENT_PROPERTY)
            .required(true)
            .build();

    public static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name(SCHEMA_NAME_PROPERTY_NAME)
            .displayName(SCHEMA_NAME_PROPERTY_NAME)
            .description("Specifies the name of the schema to lookup in the Schema Registry property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${schema.name}")
            .dependsOn(SCHEMA_ACCESS_STRATEGY, JsonSchemaStrategy.SCHEMA_NAME_PROPERTY)
            .build();

    public static final PropertyDescriptor SCHEMA_REGISTRY = new PropertyDescriptor.Builder()
            .name("JSON Schema Registry")
            .displayName("JSON Schema Registry")
            .description("Specifies the Controller Service to use for the JSON Schema Registry")
            .identifiesControllerService(JsonSchemaRegistry.class)
            .required(true)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, JsonSchemaStrategy.SCHEMA_NAME_PROPERTY)
            .build();

    public static final PropertyDescriptor SCHEMA_CONTENT = new PropertyDescriptor.Builder()
            .name(SCHEMA_CONTENT_PROPERTY_NAME)
            .displayName(SCHEMA_CONTENT_PROPERTY_NAME)
            .description("A URL or file path to the JSON schema or the actual JSON schema content")
            .required(true)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.URL, ResourceType.TEXT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, JsonSchemaStrategy.SCHEMA_CONTENT_PROPERTY)
            .build();

    public static final PropertyDescriptor SCHEMA_VERSION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(JsonSchemaRegistryComponent.SCHEMA_VERSION)
            .dependsOn(SCHEMA_ACCESS_STRATEGY, JsonSchemaStrategy.SCHEMA_CONTENT_PROPERTY)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SCHEMA_ACCESS_STRATEGY,
            SCHEMA_NAME,
            SCHEMA_REGISTRY,
            SCHEMA_CONTENT,
            SCHEMA_VERSION
    );

    public static final Relationship REL_VALID = new Relationship.Builder()
        .name("valid")
        .description("FlowFiles that are successfully validated against the schema are routed to this relationship")
        .build();

    public static final Relationship REL_INVALID = new Relationship.Builder()
        .name("invalid")
        .description("FlowFiles that are not valid according to the specified schema are routed to this relationship")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles that cannot be read as JSON are routed to this relationship")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_VALID,
        REL_INVALID,
        REL_FAILURE
    );

    private static final ObjectMapper MAPPER = new ObjectMapper().configure(JsonParser.Feature.ALLOW_COMMENTS, true);

    private final ConcurrentMap<SchemaVersion, JsonSchemaFactory> schemaFactories =  Arrays.stream(SchemaVersion.values())
            .collect(
                    Collectors.toConcurrentMap(
                            Function.identity(),
                            schemaDraftVersion -> JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.fromId(schemaDraftVersion.getUri()).get())
                    )
            );
    private volatile com.networknt.schema.JsonSchema schema;
    private volatile JsonSchemaRegistry jsonSchemaRegistry;

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.renameProperty("Schema Version", SCHEMA_VERSION.getName());
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> validationResults = new ArrayList<>();

        final JsonSchemaStrategy schemaAccessStrategy = getSchemaAccessStrategy(validationContext);
        if (schemaAccessStrategy.equals(JsonSchemaStrategy.SCHEMA_NAME_PROPERTY) && !validationContext.getProperty(SCHEMA_REGISTRY).isSet()) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(SCHEMA_REGISTRY.getDisplayName())
                    .explanation(getPropertyValidateMessage(schemaAccessStrategy, SCHEMA_REGISTRY))
                    .valid(false)
                    .build());
        } else if (schemaAccessStrategy.equals(JsonSchemaStrategy.SCHEMA_CONTENT_PROPERTY) && !validationContext.getProperty(SCHEMA_CONTENT).isSet()) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(SCHEMA_CONTENT.getDisplayName())
                    .explanation(getPropertyValidateMessage(schemaAccessStrategy, SCHEMA_CONTENT))
                    .valid(false)
                    .build());
        }

        return validationResults;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        switch (getSchemaAccessStrategy(context)) {
            case SCHEMA_NAME_PROPERTY ->
                    jsonSchemaRegistry = context.getProperty(SCHEMA_REGISTRY).asControllerService(JsonSchemaRegistry.class);
            case SCHEMA_CONTENT_PROPERTY -> {
                try (final InputStream inputStream = context.getProperty(SCHEMA_CONTENT).asResource().read()) {
                    final SchemaVersion schemaVersion = SchemaVersion.valueOf(context.getProperty(SCHEMA_VERSION).getValue());
                    final JsonSchemaFactory factory = schemaFactories.get(schemaVersion);
                    schema = factory.getSchema(inputStream);
                }
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final JsonSchemaStrategy schemaAccessStrategy = getSchemaAccessStrategy(context);
        if (schemaAccessStrategy.equals(JsonSchemaStrategy.SCHEMA_NAME_PROPERTY)) {
            try {
                final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
                final JsonSchema jsonSchema = jsonSchemaRegistry.retrieveSchema(schemaName);
                final JsonSchemaFactory factory = schemaFactories.get(jsonSchema.getSchemaVersion());
                schema = factory.getSchema(jsonSchema.getSchemaText());
            } catch (Exception e) {
                getLogger().error("Could not retrieve JSON schema for {}", flowFile, e);
                session.getProvenanceReporter().route(flowFile, REL_FAILURE);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        try (final InputStream in = session.read(flowFile)) {
            final JsonNode node = MAPPER.readTree(in);
            final Set<ValidationMessage> errors = schema.validate(node);

            if (errors.isEmpty()) {
                getLogger().debug("JSON {} valid", flowFile);
                session.getProvenanceReporter().route(flowFile, REL_VALID);
                session.transfer(flowFile, REL_VALID);
            } else {
                final String validationMessages = errors.toString();
                flowFile = session.putAttribute(flowFile, ERROR_ATTRIBUTE_KEY, validationMessages);
                getLogger().warn("JSON {} invalid: Validation Errors {}", flowFile, validationMessages);
                session.getProvenanceReporter().route(flowFile, REL_INVALID);
                session.transfer(flowFile, REL_INVALID);
            }
        } catch (final Exception e) {
            getLogger().error("JSON processing failed {}", flowFile, e);
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private String getPropertyValidateMessage(JsonSchemaStrategy schemaAccessStrategy, PropertyDescriptor property) {
        return "The '" + schemaAccessStrategy.getValue() + "' Schema Access Strategy requires that the " + property.getDisplayName() + " property be set.";
    }

    private JsonSchemaStrategy getSchemaAccessStrategy(PropertyContext context) {
        return context.getProperty(SCHEMA_ACCESS_STRATEGY).asAllowableValue(JsonSchemaStrategy.class);
    }
}
