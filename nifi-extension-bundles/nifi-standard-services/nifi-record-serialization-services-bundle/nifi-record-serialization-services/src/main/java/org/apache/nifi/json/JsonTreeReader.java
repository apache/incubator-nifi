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

package org.apache.nifi.json;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.schema.inference.SchemaInferenceUtil.INFER_SCHEMA;
import static org.apache.nifi.schema.inference.SchemaInferenceUtil.SCHEMA_CACHE;

@Tags({"json", "tree", "record", "reader", "parser"})
@CapabilityDescription("Parses JSON into individual Record objects. While the reader expects each record "
        + "to be well-formed JSON, the content of a FlowFile may consist of many records, each as a well-formed "
        + "JSON array or JSON object with optional whitespace between them, such as the common 'JSON-per-line' format. "
        + "If an array is encountered, each element in that array will be treated as a separate record. "
        + "If the schema that is configured contains a field that is not present in the JSON, a null value will be used. If the JSON contains "
        + "a field that is not present in the schema, that field will be skipped. "
        + "See the Usage of the Controller Service for more information and examples.")
@SeeAlso(JsonPathReader.class)
public class JsonTreeReader extends SchemaRegistryService implements RecordReaderFactory {
    protected volatile String dateFormat;
    protected volatile String timeFormat;
    protected volatile String timestampFormat;
    protected volatile String startingFieldName;
    protected volatile StartingFieldStrategy startingFieldStrategy;
    protected volatile SchemaApplicationStrategy schemaApplicationStrategy;
    protected volatile TokenParserFactory tokenParserFactory;

    public static final PropertyDescriptor STARTING_FIELD_STRATEGY = new PropertyDescriptor.Builder()
            .name("starting-field-strategy")
            .displayName("Starting Field Strategy")
            .description("Start processing from the root node or from a specified nested node.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(StartingFieldStrategy.ROOT_NODE.getValue())
            .allowableValues(StartingFieldStrategy.class)
            .build();


    public static final PropertyDescriptor STARTING_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("starting-field-name")
            .displayName("Starting Field Name")
            .description("Skips forward to the given nested JSON field (array or object) to begin processing.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(STARTING_FIELD_STRATEGY, StartingFieldStrategy.NESTED_FIELD.name())
            .build();

    public static final PropertyDescriptor SCHEMA_APPLICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("schema-application-strategy")
            .displayName("Schema Application Strategy")
            .description("Specifies whether the schema is defined for the whole JSON or for the selected part starting from \"Starting Field Name\".")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue(SchemaApplicationStrategy.SELECTED_PART.getValue())
            .dependsOn(STARTING_FIELD_STRATEGY, StartingFieldStrategy.NESTED_FIELD.name())
            .dependsOn(SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY, SCHEMA_REFERENCE_READER_PROPERTY)
            .allowableValues(SchemaApplicationStrategy.class)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(SCHEMA_CACHE)
                .dependsOn(SCHEMA_ACCESS_STRATEGY, INFER_SCHEMA)
                .build());
        properties.add(STARTING_FIELD_STRATEGY);
        properties.add(STARTING_FIELD_NAME);
        properties.add(SCHEMA_APPLICATION_STRATEGY);
        properties.add(AbstractJsonRowRecordReader.MAX_STRING_LENGTH);
        properties.add(AbstractJsonRowRecordReader.ALLOW_COMMENTS);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }

    @OnEnabled
    public void storePropertyValues(final ConfigurationContext context) {
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
        this.startingFieldStrategy = StartingFieldStrategy.valueOf(context.getProperty(STARTING_FIELD_STRATEGY).getValue());
        this.startingFieldName = context.getProperty(STARTING_FIELD_NAME).getValue();
        this.schemaApplicationStrategy = SchemaApplicationStrategy.valueOf(context.getProperty(SCHEMA_APPLICATION_STRATEGY).getValue());
        this.tokenParserFactory = createTokenParserFactory(context);
    }

    protected TokenParserFactory createTokenParserFactory(final ConfigurationContext context) {
        return new JsonParserFactory(buildStreamReadConstraints(context), isAllowCommentsEnabled(context));
    }

    /**
     * Build Stream Read Constraints based on available properties
     *
     * @param context Configuration Context with property values
     * @return Stream Read Constraints
     */
    protected StreamReadConstraints buildStreamReadConstraints(final ConfigurationContext context) {
        final int maxStringLength = context.getProperty(AbstractJsonRowRecordReader.MAX_STRING_LENGTH).asDataSize(DataUnit.B).intValue();
        return StreamReadConstraints.builder().maxStringLength(maxStringLength).build();
    }

    /**
     * Determine whether to allow comments when parsing based on available properties
     *
     * @param context Configuration Context with property values
     * @return Allow comments status
     */
    protected boolean isAllowCommentsEnabled(final ConfigurationContext context) {
        return context.getProperty(AbstractJsonRowRecordReader.ALLOW_COMMENTS).asBoolean();
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();
        allowableValues.add(INFER_SCHEMA);
        allowableValues.addAll(super.getSchemaAccessStrategyValues());
        return allowableValues;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String schemaAccessStrategy, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        final RecordSourceFactory<JsonNode> jsonSourceFactory = createJsonRecordSourceFactory();
        final Supplier<SchemaInferenceEngine<JsonNode>> inferenceSupplier =
                () -> new JsonSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));

        return SchemaInferenceUtil.getSchemaAccessStrategy(schemaAccessStrategy, context, getLogger(), jsonSourceFactory, inferenceSupplier,
                () -> super.getSchemaAccessStrategy(schemaAccessStrategy, schemaRegistry, context));
    }

    protected RecordSourceFactory<JsonNode> createJsonRecordSourceFactory() {
        return (variables, in) -> new JsonRecordSource(in, startingFieldStrategy, startingFieldName, tokenParserFactory);
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return INFER_SCHEMA;
    }

    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger)
            throws IOException, MalformedRecordException, SchemaNotFoundException {
        final RecordSchema schema = getSchema(variables, in, null);
        return createJsonTreeRowRecordReader(in, logger, schema);
    }

    protected JsonTreeRowRecordReader createJsonTreeRowRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema) throws IOException, MalformedRecordException {
        return new JsonTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat, startingFieldStrategy, startingFieldName,
                schemaApplicationStrategy, null, tokenParserFactory);
    }
}
