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
package org.apache.nifi.processors.jslt;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.Parser;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.jslt.JSLTTransformJSON.TransformationStrategy.EACH_OBJECT;
import static org.apache.nifi.processors.jslt.JSLTTransformJSON.TransformationStrategy.ENTIRE_FLOWFILE;

@SideEffectFree
@SupportsBatching
@Tags({"json", "jslt", "transform"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@WritesAttribute(attribute = "mime.type", description = "Always set to application/json")
@CapabilityDescription("Applies a JSLT transformation to the FlowFile JSON payload. A new FlowFile is created "
        + "with transformed content and is routed to the 'success' relationship. If the JSLT transform "
        + "fails, the original FlowFile is routed to the 'failure' relationship.")
public class JSLTTransformJSON extends AbstractProcessor {

    public static String JSLT_FILTER_DEFAULT = ". != null and . != {} and . != []";

    public static final PropertyDescriptor JSLT_TRANSFORM = new PropertyDescriptor.Builder()
            .name("jslt-transform-transformation")
            .displayName("JSLT Transformation")
            .description("JSLT Transformation for transform of JSON data. Any NiFi Expression Language present will be evaluated first to get the final transform to be applied. " +
                    "The JSLT Tutorial provides an overview of supported expressions: https://github.com/schibsted/jslt/blob/master/tutorial.md")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.TEXT, ResourceType.FILE)
            .required(true)
            .build();

    public static final PropertyDescriptor TRANSFORMATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("jslt-transform-transformation-strategy")
            .displayName("Transformation Strategy")
            .description("Whether to apply the JSLT transformation to the entire FlowFile contents or each JSON object in the root-level array")
            .required(true)
            .allowableValues(TransformationStrategy.class)
            .defaultValue(EACH_OBJECT.getValue())
            .build();

    public static final PropertyDescriptor PRETTY_PRINT = new PropertyDescriptor.Builder()
            .name("jslt-transform-pretty_print")
            .displayName("Pretty Print")
            .description("Apply pretty-print formatting to the output of the JSLT transform")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor TRANSFORM_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("jslt-transform-cache-size")
            .displayName("Transform Cache Size")
            .description("Compiling a JSLT Transform can be fairly expensive. Ideally, this will be done only once. However, if the Expression Language is used in the transform, we may need "
                    + "a new Transform for each FlowFile. This value controls how many of those Transforms we cache in memory in order to avoid having to compile the Transform each time.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .required(true)
            .build();

    public static final PropertyDescriptor RESULT_FILTER = new PropertyDescriptor.Builder()
            .name("jslt-transform-result-filter")
            .displayName("Transform Result Filter")
            .description("A filter for output JSON results using a JSLT expression. This property supports changing the default filter,"
                    + " which removes JSON objects with null values, empty objects and empty arrays from the output JSON."
                    + " This JSLT must return true for each JSON object to be included and false for each object to be removed."
                    + " Using a filter value of \"true\" to disables filtering.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .defaultValue(JSLT_FILTER_DEFAULT)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The FlowFile with transformed content will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile fails processing for any reason (for example, the FlowFile is not valid JSON), it will be routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            JSLT_TRANSFORM,
            TRANSFORMATION_STRATEGY,
            PRETTY_PRINT,
            TRANSFORM_CACHE_SIZE,
            RESULT_FILTER
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    private Cache<String, Expression> transformCache;

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
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final PropertyValue transformProperty = validationContext.getProperty(JSLT_TRANSFORM);
        if (transformProperty.isExpressionLanguagePresent()) {
            final ValidationResult.Builder transformBuilder = new ValidationResult.Builder().subject(JSLT_TRANSFORM.getDisplayName());
            results.add(transformBuilder.valid(true).build());
        } else {
            results.add(validateJSLT(JSLT_TRANSFORM, transformProperty));
        }

        final PropertyValue filterProperty = validationContext.getProperty(RESULT_FILTER);
        results.add(validateJSLT(RESULT_FILTER, filterProperty));

        return results;
    }

    private ValidationResult validateJSLT(PropertyDescriptor property, PropertyValue value) {
        final ValidationResult.Builder builder = new ValidationResult.Builder().subject(property.getDisplayName());
        try {
            final String transform = readTransform(value);
            getJstlExpression(transform, null);
            builder.valid(true);
        } catch (final RuntimeException e) {
            final String explanation = String.format("%s not valid: %s", property.getDisplayName(), e.getMessage());
            builder.valid(false).explanation(explanation);
        }
        return builder.build();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        int maxTransformsToCache = context.getProperty(TRANSFORM_CACHE_SIZE).asInteger();
        transformCache = Caffeine.newBuilder()
                .maximumSize(maxTransformsToCache)
                .build();
        // Precompile the transform if it hasn't been done already (and if there is no Expression Language present)
        final PropertyValue transformProperty = context.getProperty(JSLT_TRANSFORM);
        final PropertyValue filterProperty = context.getProperty(RESULT_FILTER);
        if (!transformProperty.isExpressionLanguagePresent()) {
            try {
                final String transform = readTransform(transformProperty);
                transformCache.put(transform, getJstlExpression(transform, filterProperty.getValue()));
            } catch (final RuntimeException e) {
                throw new ProcessException("JSLT Transform compilation failed", e);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final TransformationStrategy transformationStrategy = TransformationStrategy.valueOf(context.getProperty(TRANSFORMATION_STRATEGY).getValue());
        final StopWatch stopWatch = new StopWatch(true);

        final PropertyValue transformProperty = context.getProperty(JSLT_TRANSFORM);
        final PropertyValue filterProperty = context.getProperty(RESULT_FILTER);
        FlowFile transformed;
        final JsonFactory jsonFactory = new JsonFactory();

        try {
            final String transform = readTransform(transformProperty, original);
            final Expression jsltExpression = transformCache.get(transform, currString -> getJstlExpression(transform, filterProperty.getValue()));
            final boolean prettyPrint = context.getProperty(PRETTY_PRINT).asBoolean();

            transformed = session.write(original, (inputStream, outputStream) -> {
                boolean topLevelArray = false;
                JsonParser jsonParser;
                JsonNode firstJsonNode;
                if (EACH_OBJECT.equals(transformationStrategy)) {
                    jsonParser = jsonFactory.createParser(inputStream);
                    jsonParser.setCodec(JSON_OBJECT_MAPPER);

                    JsonToken token = jsonParser.nextToken();
                    if (token == JsonToken.START_ARRAY) {
                        token = jsonParser.nextToken(); // advance to START_OBJECT token
                        topLevelArray = true;
                    }
                    if (token == JsonToken.START_OBJECT) { // could be END_ARRAY also
                        firstJsonNode = jsonParser.readValueAsTree();
                    } else {
                        firstJsonNode = null;
                    }
                } else {
                    firstJsonNode = readJson(inputStream);
                    jsonParser = null; // This will not be used when applying the transform to the entire FlowFile
                }

                final ObjectWriter writer = prettyPrint ? JSON_OBJECT_MAPPER.writerWithDefaultPrettyPrinter() : JSON_OBJECT_MAPPER.writer();
                final JsonGenerator jsonGenerator = writer.createGenerator(outputStream);

                Object outputObject;
                JsonNode nextNode;

                if (topLevelArray) {
                    jsonGenerator.writeStartArray();
                }
                nextNode = firstJsonNode;
                do {
                    final JsonNode transformedJson = jsltExpression.apply(nextNode);
                    if (transformedJson == null || transformedJson.isNull()) {
                        getLogger().warn("JSLT Transform resulted in no data {}", original);
                        outputObject = null;
                    } else {
                        outputObject = transformedJson;
                    }
                    if (outputObject != null) {
                        jsonGenerator.writeObject(outputObject);
                    }
                } while ((nextNode = getNextJsonNode(transformationStrategy, jsonParser)) != null);
                if (topLevelArray) {
                    jsonGenerator.writeEndArray();
                }
                jsonGenerator.flush();
            });

            transformed = session.putAttribute(transformed, CoreAttributes.MIME_TYPE.key(), "application/json");
            session.transfer(transformed, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(transformed, "Modified With " + transform, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            stopWatch.stop();
            getLogger().debug("JSLT Transform completed {}", original);
        } catch (final Exception e) {
            getLogger().error("JSLT Transform failed {}", original, e);
            session.transfer(original, REL_FAILURE);
        }
    }

    @OnStopped
    @OnShutdown
    public void onStopped() {
        if (transformCache != null) {
            transformCache.cleanUp();
        }
    }

    private Expression getJstlExpression(String transform, String jsltFilter) {
        Parser parser = new Parser(new StringReader(transform))
                .withSource("<inline>");
        if (jsltFilter != null && !jsltFilter.isEmpty() && !jsltFilter.equals(JSLT_FILTER_DEFAULT)) {
            parser = parser.withObjectFilter(jsltFilter);
        }
        return parser.compile();
    }

    private JsonNode readJson(final InputStream in) throws IOException {
        try {
            return JSON_OBJECT_MAPPER.readTree(in);
        } catch (final JsonParseException e) {
            throw new IOException("Could not parse data as JSON", e);
        }
    }

    private String readTransform(final PropertyValue propertyValue, final FlowFile flowFile) {
        final String transform;

        if (propertyValue.isExpressionLanguagePresent()) {
            transform = propertyValue.evaluateAttributeExpressions(flowFile).getValue();
        } else {
            transform = readTransform(propertyValue);
        }

        return transform;
    }

    private String readTransform(final PropertyValue propertyValue) {
        final ResourceReference resourceReference = propertyValue.asResource();
        if (resourceReference == null) {
            return propertyValue.getValue();
        }
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(resourceReference.read()))) {
            return reader.lines().collect(Collectors.joining());
        } catch (final IOException e) {
            throw new UncheckedIOException("Read JSLT Transform failed", e);
        }
    }


    protected JsonNode getNextJsonNode(final TransformationStrategy transformationStrategy, final JsonParser jsonParser) throws IOException {

        if (ENTIRE_FLOWFILE.equals(transformationStrategy)) {
            return null;
        }
        return getJsonNode(jsonParser);
    }

    private JsonNode getJsonNode(JsonParser jsonParser) throws IOException {
        while (true) {
            final JsonToken token = jsonParser.nextToken();
            if (token == null) {
                return null;
            }

            switch (token) {
                case START_ARRAY:
                case END_ARRAY:
                case END_OBJECT:
                    break;
                case START_OBJECT:
                    return jsonParser.readValueAsTree();
                default:
                    throw new IOException("Expected to get a JSON Object but got a token of type " + token.name());
            }
        }
    }

    enum TransformationStrategy implements DescribedValue {
        ENTIRE_FLOWFILE("Entire FlowFile", "Apply transformation to entire FlowFile content JSON"),
        EACH_OBJECT("Each JSON Object", "Apply transformation each JSON Object in an array");

        private final String displayName;

        private final String description;

        TransformationStrategy(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

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
}