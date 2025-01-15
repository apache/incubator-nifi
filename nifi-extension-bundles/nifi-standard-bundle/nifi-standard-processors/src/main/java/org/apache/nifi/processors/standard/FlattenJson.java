/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.flattener.FlattenMode;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.flattener.PrintMode;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"json", "flatten", "unflatten"})
@CapabilityDescription(
        "Provides the user with the ability to take a nested JSON document and flatten it into a simple key/value pair " +
                "document. The keys are combined at each level with a user-defined separator that defaults to '.'. " +
                "This Processor also allows to unflatten back the flattened json. It supports four kinds of flatten mode " +
                "such as normal, keep-arrays, dot notation for MongoDB query and keep-primitive-arrays. Default flatten mode " +
                "is 'keep-arrays'."
)
public class FlattenJson extends AbstractProcessor {

    public static final String RETURN_TYPE_FLATTEN = "flatten";
    public static final String RETURN_TYPE_UNFLATTEN = "unflatten";

    public static final AllowableValue FLATTEN_MODE_NORMAL = new AllowableValue("normal", "normal",
            "Flattens every objects into a single level json");

    public static final AllowableValue FLATTEN_MODE_KEEP_ARRAYS = new AllowableValue("keep arrays", "keep arrays",
            "Flattens every objects and keep arrays format");

    public static final AllowableValue FLATTEN_MODE_DOT_NOTATION = new AllowableValue("dot notation", "dot notation",
            "Conforms to MongoDB dot notation to update also nested documents");

    public static final AllowableValue FLATTEN_MODE_KEEP_PRIMITIVE_ARRAYS = new AllowableValue("keep primitive arrays", "keep primitive arrays",
            "Flattens every objects except arrays which contain only primitive types (strings, numbers, booleans and null)");

    public static final PropertyDescriptor SEPARATOR = new PropertyDescriptor.Builder()
            .name("flatten-json-separator")
            .displayName("Separator")
            .defaultValue(".")
            .description("The separator character used for joining keys. Must be a JSON-legal character.")
            .addValidator((subject, input, context) -> {
                if (context.isExpressionLanguagePresent(input)) {
                    ExpressionLanguageCompiler elc = context.newExpressionLanguageCompiler();
                    final boolean validExpression = elc.isValidExpression(input);
                    return new ValidationResult.Builder().subject(subject).input(input)
                            .valid(validExpression).explanation(validExpression ? "" : "Not a valid Expression").build();
                }

                boolean valid = input != null && input.length() == 1;
                String message = !valid ? "The separator must be a single character in length." : "";

                ObjectMapper mapper = new ObjectMapper();
                String test = String.format("{ \"prop%sprop\": \"test\" }", input);
                try {
                    mapper.readValue(test, Map.class);
                } catch (IOException e) {
                    message = e.getLocalizedMessage();
                    valid = false;
                }

                return new ValidationResult.Builder().subject(subject).input(input).valid(valid).explanation(message).build();
            })
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FLATTEN_MODE = new PropertyDescriptor.Builder()
            .name("flatten-mode")
            .displayName("Flatten Mode")
            .description("Specifies how json should be flattened/unflattened")
            .defaultValue(FLATTEN_MODE_KEEP_ARRAYS.getValue())
            .required(true)
            .allowableValues(FLATTEN_MODE_NORMAL, FLATTEN_MODE_KEEP_ARRAYS, FLATTEN_MODE_DOT_NOTATION, FLATTEN_MODE_KEEP_PRIMITIVE_ARRAYS)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor IGNORE_RESERVED_CHARACTERS = new PropertyDescriptor.Builder()
            .name("ignore-reserved-characters")
            .displayName("Ignore Reserved Characters")
            .description("If true, reserved characters in keys will be ignored")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder()
            .name("flatten-json-return-type")
            .displayName("Return Type")
            .description("Specifies the desired return type of json such as flatten/unflatten")
            .defaultValue(RETURN_TYPE_FLATTEN)
            .required(true)
            .allowableValues(RETURN_TYPE_FLATTEN, RETURN_TYPE_UNFLATTEN)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("flatten-json-character-set")
            .displayName("Character Set")
            .description("The Character Set in which file is encoded")
            .defaultValue("UTF-8")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor PRETTY_PRINT = new PropertyDescriptor.Builder()
            .name("flatten-json-pretty-print-json")
            .displayName("Pretty Print JSON")
            .description("Specifies whether or not resulted json should be pretty printed")
            .defaultValue("false")
            .required(true)
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SEPARATOR,
            FLATTEN_MODE,
            IGNORE_RESERVED_CHARACTERS,
            RETURN_TYPE,
            CHARACTER_SET,
            PRETTY_PRINT
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("Successfully flattened/unflattened files go to this relationship.")
            .name("success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .description("Files that cannot be flattened/unflattened go to this relationship.")
            .name("failure")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

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
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String mode = context.getProperty(FLATTEN_MODE).getValue();
        final FlattenMode flattenMode = getFlattenMode(mode);

        final Character separator = context.getProperty(SEPARATOR).evaluateAttributeExpressions(flowFile).getValue().charAt(0);
        final String returnType = context.getProperty(RETURN_TYPE).getValue();
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final PrintMode printMode = context.getProperty(PRETTY_PRINT).asBoolean() ? PrintMode.PRETTY : PrintMode.MINIMAL;
        final boolean ignoreReservedCharacters = context.getProperty(IGNORE_RESERVED_CHARACTERS).asBoolean();

        try {
            final StringBuilder contents = new StringBuilder();
            session.read(flowFile, in -> contents.append(IOUtils.toString(in, charset)));

            final String resultedJson;
            if (returnType.equals(RETURN_TYPE_FLATTEN)) {
                final JsonFlattener jsonFlattener = new JsonFlattener(contents.toString())
                    .withFlattenMode(flattenMode)
                    .withSeparator(separator)
                    .withStringEscapePolicy(() -> StringEscapeUtils.ESCAPE_JSON)
                    .withPrintMode(printMode);
                setIgnoreReservedCharacters(ignoreReservedCharacters, jsonFlattener);
                resultedJson = jsonFlattener.flatten();
            } else {
                resultedJson = new JsonUnflattener(contents.toString())
                        .withFlattenMode(flattenMode)
                        .withSeparator(separator)
                        .withPrintMode(printMode)
                        .unflatten();
            }

            flowFile = session.write(flowFile, out -> out.write(resultedJson.getBytes(charset)));

            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to {} JSON", returnType, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private FlattenMode getFlattenMode(String mode) {
        if (FLATTEN_MODE_NORMAL.getValue().equals(mode)) {
            return FlattenMode.NORMAL;
        } else if (FLATTEN_MODE_DOT_NOTATION.getValue().equals(mode)) {
            return FlattenMode.MONGODB;
        } else if (FLATTEN_MODE_KEEP_PRIMITIVE_ARRAYS.getValue().equals(mode)) {
            return FlattenMode.KEEP_PRIMITIVE_ARRAYS;
        } else {
            return FlattenMode.KEEP_ARRAYS;
        }
    }

    private void setIgnoreReservedCharacters(final boolean ignoreReservedCharacters, final JsonFlattener jsonFlattener) {
        if (ignoreReservedCharacters) {
            jsonFlattener.ignoreReservedCharacters();
        }
    }
}
