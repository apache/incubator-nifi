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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SideEffectFree
@SupportsBatching
@Tags({"csv", "attributes", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Generates a CSV representation of the input FlowFile Attributes. The resulting CSV " +
        "can be written to either a newly generated attribute named 'CSVAttributes' or written to the FlowFile as content.  " +
        "If the attribute value contains a comma, newline or double quote, then the attribute value will be " +
        "escaped with double quotes.  Any double quote characters in the attribute value are escaped with " +
        "another double quote.")
@WritesAttributes({
        @WritesAttribute(attribute = "CSVSchema", description = "CSV representation of the Schema"),
        @WritesAttribute(attribute = "CSVData", description = "CSV representation of Attributes")
})

public class AttributesToCSV extends AbstractProcessor {
    private static final String DATA_ATTRIBUTE_NAME = "CSVData";
    private static final String SCHEMA_ATTRIBUTE_NAME = "CSVSchema";
    private static final String OUTPUT_SEPARATOR = ",";
    private static final String OUTPUT_MIME_TYPE = "text/csv";
    private static final String SPLIT_REGEX = OUTPUT_SEPARATOR + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    static final AllowableValue OUTPUT_OVERWRITE_CONTENT = new AllowableValue("flowfile-content", "flowfile-content", "The resulting CSV string will be placed into the content of the flowfile." +
            "Existing flowfile context will be overwritten. 'CSVData' will not be written to at all (neither null nor empty string).");
    static final AllowableValue OUTPUT_NEW_ATTRIBUTE = new AllowableValue("flowfile-attribute", "flowfile-attribute", "The resulting CSV string will be placed into a new flowfile" +
            " attribute named 'CSVData'.  The content of the flowfile will not be changed.");

    public static final PropertyDescriptor ATTRIBUTES_LIST = new PropertyDescriptor.Builder()
            .name("attribute-list")
            .displayName("Attribute List")
            .description("Comma separated list of attributes to be included in the resulting CSV. If this value " +
                    "is left empty then all existing Attributes will be included. This list of attributes is " +
                    "case sensitive and supports attribute names that contain commas. If an attribute specified in the list is not found it will be emitted " +
                    "to the resulting CSV with an empty string or null depending on the 'Null Value' property. " +
                    "If a core attribute is specified in this list " +
                    "and the 'Include Core Attributes' property is false, the core attribute will be included. The attribute list " +
                    "ALWAYS wins.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ATTRIBUTES_REGEX = new PropertyDescriptor.Builder()
            .name("attributes-regex")
            .displayName("Attributes Regular Expression")
            .description("Regular expression that will be evaluated against the flow file attributes to select "
                    + "the matching attributes. This property can be used in combination with the attributes "
                    + "list property.  The final output will contain a combination of matches found in the ATTRIBUTE_LIST and ATTRIBUTE_REGEX.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("destination")
            .displayName("Destination")
            .description("Control if CSV value is written as a new flowfile attribute 'CSVData' " +
                    "or written in the flowfile content.")
            .required(true)
            .allowableValues(OUTPUT_NEW_ATTRIBUTE, OUTPUT_OVERWRITE_CONTENT)
            .defaultValue(OUTPUT_NEW_ATTRIBUTE.getDisplayName())
            .build();

    public static final PropertyDescriptor INCLUDE_CORE_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("include-core-attributes")
            .displayName("Include Core Attributes")
            .description("Determines if the FlowFile org.apache.nifi.flowfile.attributes.CoreAttributes, which are " +
                    "contained in every FlowFile, should be included in the final CSV value generated.  Core attributes " +
                    "will be added to the end of the CSVData and CSVSchema strings.  The Attribute List property " +
                    "overrides this setting.")
            .required(true)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor NULL_VALUE_FOR_EMPTY_STRING = new PropertyDescriptor.Builder()
            .name("null-value")
            .displayName("Null Value")
            .description("If true a non existing or empty attribute will be 'null' in the resulting CSV. If false an empty " +
                    "string will be placed in the CSV")
            .required(true)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor INCLUDE_SCHEMA = new PropertyDescriptor.Builder()
            .name("include-schema")
            .displayName("Include Schema")
            .description("If true the schema (attribute names) will also be converted to a CSV string which will either be " +
                    "applied to a new attribute named 'CSVSchema' or applied at the first row in the " +
                    "content depending on the DESTINATION property setting.")
            .required(true)
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ATTRIBUTES_LIST,
            ATTRIBUTES_REGEX,
            DESTINATION,
            INCLUDE_CORE_ATTRIBUTES,
            NULL_VALUE_FOR_EMPTY_STRING,
            INCLUDE_SCHEMA
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted attributes to CSV").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to convert attributes to CSV").build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private volatile Boolean includeCoreAttributes;
    private volatile Set<String> coreAttributes;
    private volatile boolean destinationContent;
    private volatile boolean nullValForEmptyString;
    private volatile Pattern pattern;
    private volatile Boolean includeSchema;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }


    private Map<String, String> buildAttributesMapForFlowFile(FlowFile ff, Set<String> attributes, Pattern attPattern) {
        Map<String, String> result;
        Map<String, String> ffAttributes = ff.getAttributes();
        result = new LinkedHashMap<>(ffAttributes.size());

        if (!attributes.isEmpty() || attPattern != null) {
            if (!attributes.isEmpty()) {
                //the user gave a list of attributes
                for (String attribute : attributes) {
                    String val = ff.getAttribute(attribute);
                    if (val != null && !val.isEmpty()) {
                        result.put(attribute, val);
                    } else {
                        if (nullValForEmptyString) {
                            result.put(attribute, "null");
                        } else {
                            result.put(attribute, "");
                        }
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
            //the user did not give a list of attributes, take all the attributes from the flowfile
            result.putAll(ffAttributes);
        }

        //now glue on the core attributes if the user wants them.
        if (includeCoreAttributes) {
            for (String coreAttribute : coreAttributes) {
                //make sure this coreAttribute is applicable to this flowfile.
                String val = ff.getAttribute(coreAttribute);
                if (ffAttributes.containsKey(coreAttribute)) {
                    if (!StringUtils.isEmpty(val)) {
                        result.put(coreAttribute, val);
                    } else {
                        if (nullValForEmptyString) {
                            result.put(coreAttribute, "null");
                        } else {
                            result.put(coreAttribute, "");
                        }
                    }
                }
            }
        } else {
            //remove core attributes since the user does not want them, unless they are in the attribute list. Attribute List always wins
            for (String coreAttribute : coreAttributes) {
                //never override user specified attributes, even if the user has selected to exclude core attributes
                if (!attributes.contains(coreAttribute)) {
                    result.remove(coreAttribute);
                }
            }
        }
        return result;
    }

    private LinkedHashSet<String> attributeListStringToSet(String attributeList) {
        //take the user specified attribute list string and convert to list of strings.
        LinkedHashSet<String> result = new LinkedHashSet<>();
        if (StringUtils.isNotBlank(attributeList)) {
            String[] ats = attributeList.split(SPLIT_REGEX);
            for (String str : ats) {
                result.add(StringEscapeUtils.unescapeCsv(str.trim()));
            }
        }
        return result;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        includeCoreAttributes = context.getProperty(INCLUDE_CORE_ATTRIBUTES).asBoolean();
        coreAttributes = Arrays.stream(CoreAttributes.values()).map(CoreAttributes::key).collect(Collectors.toCollection(LinkedHashSet::new));
        destinationContent = OUTPUT_OVERWRITE_CONTENT.getValue().equals(context.getProperty(DESTINATION).getValue());
        nullValForEmptyString = context.getProperty(NULL_VALUE_FOR_EMPTY_STRING).asBoolean();
        includeSchema = context.getProperty(INCLUDE_SCHEMA).asBoolean();
     }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }
        if (context.getProperty(ATTRIBUTES_REGEX).isSet()) {
            pattern = Pattern.compile(context.getProperty(ATTRIBUTES_REGEX).evaluateAttributeExpressions(original).getValue());
        }

        final Set<String> attributeList = attributeListStringToSet(context.getProperty(ATTRIBUTES_LIST).evaluateAttributeExpressions(original).getValue());
        final Map<String, String> atrList = buildAttributesMapForFlowFile(original, attributeList, pattern);

        //escape attribute values
        int index = 0;
        final int atrListSize = atrList.values().size() - 1;
        final StringBuilder sbValues = new StringBuilder();
        for (final Map.Entry<String, String> attr : atrList.entrySet()) {
            sbValues.append(StringEscapeUtils.escapeCsv(attr.getValue()));
            sbValues.append(index++ < atrListSize ? OUTPUT_SEPARATOR : "");
        }

        //build the csv header if needed
        final StringBuilder sbNames = new StringBuilder();
        if (includeSchema) {
            index = 0;
            for (final Map.Entry<String, String> attr : atrList.entrySet()) {
                sbNames.append(StringEscapeUtils.escapeCsv(attr.getKey()));
                sbNames.append(index++ < atrListSize ? OUTPUT_SEPARATOR : "");
            }
        }

        try {
            if (destinationContent) {
                FlowFile conFlowfile = session.write(original, (in, out) -> {
                        if (includeSchema) {
                            sbNames.append(System.lineSeparator());
                            out.write(sbNames.toString().getBytes());
                        }
                        out.write(sbValues.toString().getBytes());
                });
                conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), OUTPUT_MIME_TYPE);
                session.transfer(conFlowfile, REL_SUCCESS);
            } else {
                FlowFile atFlowfile = session.putAttribute(original, DATA_ATTRIBUTE_NAME, sbValues.toString());
                if (includeSchema) {
                    session.putAttribute(original, SCHEMA_ATTRIBUTE_NAME, sbNames.toString());
                }
                session.transfer(atFlowfile, REL_SUCCESS);
            }
        } catch (Exception e) {
            getLogger().error(e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }
}