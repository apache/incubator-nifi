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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SideEffectFree
@SupportsBatching
@Tags({"json", "attributes", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Generates a JSON representation of the input FlowFile Attributes. The resulting JSON " +
        "can be written to either a new Attribute 'JSONAttributes' or written to the FlowFile as content. Attributes " +
        " which contain nested JSON objects can either be handled as JSON or as escaped JSON depending on the strategy chosen.")
@WritesAttribute(attribute = "JSONAttributes", description = "JSON representation of Attributes")
public class AttributesToJSON extends AbstractProcessor {
    public enum JsonHandlingStrategy implements DescribedValue {
        ESCAPED("Escaped", "Escapes JSON attribute values to strings"),
        NESTED("Nested", "Handles JSON attribute values as nested structured objects or arrays");

        JsonHandlingStrategy(String displayName, String description) {
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

    public static final String JSON_ATTRIBUTE_NAME = "JSONAttributes";
    private static final String AT_LIST_SEPARATOR = ",";

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";
    public static final String APPLICATION_JSON = "application/json";


    public static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("Attributes List")
            .description("Comma separated list of attributes to be included in the resulting JSON. If this value " +
                    "is left empty then all existing Attributes will be included. This list of attributes is " +
                    "case sensitive. If an attribute specified in the list is not found it will be be emitted " +
                    "to the resulting JSON with an empty string or NULL value.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor ATTRIBUTES_REGEX = new PropertyDescriptor.Builder()
            .name("attributes-to-json-regex")
            .displayName("Attributes Regular Expression")
            .description("Regular expression that will be evaluated against the flow file attributes to select "
                    + "the matching attributes. This property can be used in combination with the attributes "
                    + "list property.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Control if JSON value is written as a new flowfile attribute '" + JSON_ATTRIBUTE_NAME + "' " +
                    "or written in the flowfile content. Writing to flowfile content will overwrite any " +
                    "existing flowfile content.")
            .required(true)
            .allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT)
            .defaultValue(DESTINATION_ATTRIBUTE)
            .build();

    public static final PropertyDescriptor INCLUDE_CORE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include Core Attributes")
            .description("Determines if the FlowFile org.apache.nifi.flowfile.attributes.CoreAttributes which are " +
                    "contained in every FlowFile should be included in the final JSON value generated.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor NULL_VALUE_FOR_EMPTY_STRING = new PropertyDescriptor.Builder()
            .name(("Null Value"))
            .description("If true a non existing selected attribute will be NULL in the resulting JSON. If false an empty " +
                    "string will be placed in the JSON")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor JSON_HANDLING_STRATEGY = new PropertyDescriptor.Builder()
            .name("JSON Handling Strategy")
            .displayName("JSON Handling Strategy")
            .description("Strategy to use for handling attributes which contain nested JSON.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(JsonHandlingStrategy.class)
            .defaultValue(AttributesToJSON.JsonHandlingStrategy.ESCAPED)
            .build();

    public static final PropertyDescriptor PRETTY_PRINT = new PropertyDescriptor.Builder()
            .name("Pretty Print")
            .displayName("Pretty Print")
            .description("Apply pretty print formatting to the output.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(DESTINATION, DESTINATION_CONTENT)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ATTRIBUTES_LIST,
            ATTRIBUTES_REGEX,
            DESTINATION,
            INCLUDE_CORE_ATTRIBUTES,
            NULL_VALUE_FOR_EMPTY_STRING,
            JSON_HANDLING_STRATEGY,
            PRETTY_PRINT
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted attributes to JSON").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to convert attributes to JSON").build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private volatile Set<String> attributesToRemove;
    private volatile Set<String> attributes;
    private volatile Boolean nullValueForEmptyString;
    private volatile boolean destinationContent;
    private volatile ObjectWriter objectWriter;
    private volatile Pattern pattern;
    private volatile JsonHandlingStrategy jsonHandlingStrategy;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * Builds the Map of attributes that should be included in the JSON that is emitted from this process.
     *
     * @return
     *  Map of values that are feed to a Jackson ObjectMapper
     */
    protected Map<String, Object> buildAttributesMapForFlowFile(FlowFile ff, Set<String> attributes, Set<String> attributesToRemove,
            boolean nullValForEmptyString, Pattern attPattern) {
        Map<String, Object> result;
        //If list of attributes specified get only those attributes. Otherwise write them all
        if (attributes != null || attPattern != null) {
            result = new LinkedHashMap<>();
            if (attributes != null) {
                for (String attribute : attributes) {
                    String val = ff.getAttribute(attribute);
                    if (val != null || nullValForEmptyString) {
                        result.put(attribute, val);
                    } else {
                        result.put(attribute, "");
                    }
                }
            }
            if (attPattern != null) {
                for (Map.Entry<String, String> e : ff.getAttributes().entrySet()) {
                    if (attPattern.matcher(e.getKey()).matches()) {
                        result.put(e.getKey(), e.getValue());
                    }
                }
            }
        } else {
            Map<String, String> ffAttributes = ff.getAttributes();
            result = new LinkedHashMap<>(ffAttributes.size());
            for (Map.Entry<String, String> e : ffAttributes.entrySet()) {
                if (!attributesToRemove.contains(e.getKey())) {
                    result.put(e.getKey(), e.getValue());
                }
            }
        }
        return result;
    }

    private Set<String> buildAtrs(String atrList) {
        //If list of attributes specified get only those attributes. Otherwise write them all
        if (StringUtils.isNotBlank(atrList)) {
            String[] ats = StringUtils.split(atrList, AT_LIST_SEPARATOR);
            if (ats != null) {
                Set<String> result = new HashSet<>(ats.length);
                for (String str : ats) {
                    result.add(str.trim());
                }
                return result;
            }
        }
        return null;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        attributesToRemove = context.getProperty(INCLUDE_CORE_ATTRIBUTES).asBoolean() ? Set.of() : Arrays.stream(CoreAttributes.values())
                .map(CoreAttributes::key)
                .collect(Collectors.toSet());
        attributes = buildAtrs(context.getProperty(ATTRIBUTES_LIST).getValue());
        nullValueForEmptyString = context.getProperty(NULL_VALUE_FOR_EMPTY_STRING).asBoolean();
        destinationContent = DESTINATION_CONTENT.equals(context.getProperty(DESTINATION).getValue());
        final boolean prettyPrint = context.getProperty(PRETTY_PRINT).asBoolean();
        objectWriter = destinationContent && prettyPrint ? OBJECT_MAPPER.writerWithDefaultPrettyPrinter() : OBJECT_MAPPER.writer();
        jsonHandlingStrategy = context.getProperty(JSON_HANDLING_STRATEGY).asAllowableValue(JsonHandlingStrategy.class);

        if (context.getProperty(ATTRIBUTES_REGEX).isSet()) {
            pattern = Pattern.compile(context.getProperty(ATTRIBUTES_REGEX).evaluateAttributeExpressions().getValue());
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final Map<String, Object> atrList = buildAttributesMapForFlowFile(original, attributes, attributesToRemove, nullValueForEmptyString, pattern);

        try {
            final Map<String, Object> formattedAttributes = getFormattedAttributes(atrList);
            if (destinationContent) {
                FlowFile conFlowfile = session.write(original, (in, out) -> {
                    try (OutputStream outputStream = new BufferedOutputStream(out)) {
                        objectWriter.writeValue(outputStream, formattedAttributes);
                    }
                });
                conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
                session.transfer(conFlowfile, REL_SUCCESS);
            } else {
                FlowFile atFlowfile = session.putAttribute(original, JSON_ATTRIBUTE_NAME, OBJECT_MAPPER.writeValueAsString(formattedAttributes));
                session.transfer(atFlowfile, REL_SUCCESS);
            }
        } catch (JsonProcessingException e) {
            getLogger().error(e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }

    private Map<String, Object> getFormattedAttributes(Map<String, Object> flowFileAttributes) throws JsonProcessingException {
        if (JsonHandlingStrategy.ESCAPED == jsonHandlingStrategy) {
            return flowFileAttributes;
        }

        Map<String, Object> formattedAttributes = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : flowFileAttributes.entrySet()) {
            String value = (String) entry.getValue();
            if (value != null) {
                final String trimmedValue = value.trim();
                if (isPossibleJsonArray(trimmedValue) || isPossibleJsonObject(trimmedValue)) {
                    formattedAttributes.put(entry.getKey(), OBJECT_MAPPER.readTree(trimmedValue));
                    continue;
                }
            }
            formattedAttributes.put(entry.getKey(), value);
        }

        return formattedAttributes;
    }

    private boolean isPossibleJsonArray(String value) {
        return value.startsWith("[") && value.endsWith("]");
    }

    private boolean isPossibleJsonObject(String value) {
        return value.startsWith("{") && value.endsWith("}");
    }
}
