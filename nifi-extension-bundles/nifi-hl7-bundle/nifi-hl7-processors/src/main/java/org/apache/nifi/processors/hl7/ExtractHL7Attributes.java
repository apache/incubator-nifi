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
package org.apache.nifi.processors.hl7;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Composite;
import ca.uhn.hl7v2.model.Group;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.Segment;
import ca.uhn.hl7v2.model.Structure;
import ca.uhn.hl7v2.model.Type;
import ca.uhn.hl7v2.model.Visitable;
import ca.uhn.hl7v2.parser.CanonicalModelClassFactory;
import ca.uhn.hl7v2.parser.DefaultEscaping;
import ca.uhn.hl7v2.parser.EncodingCharacters;
import ca.uhn.hl7v2.parser.Escaping;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.validation.ValidationContext;
import ca.uhn.hl7v2.validation.impl.ValidationContextFactory;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"HL7", "health level 7", "healthcare", "extract", "attributes"})
@CapabilityDescription("Extracts information from an HL7 (Health Level 7) formatted FlowFile and adds the information as FlowFile Attributes. "
        + "The attributes are named as <Segment Name> <dot> <Field Index>. If the segment is repeating, the naming will be "
        + "<Segment Name> <underscore> <Segment Index> <dot> <Field Index>. For example, we may have an attribute named \"MHS.12\" with "
        + "a value of \"2.1\" and an attribute named \"OBX_11.3\" with a value of \"93000^CPT4\".")
public class ExtractHL7Attributes extends AbstractProcessor {

    private static final EncodingCharacters HL7_ENCODING = EncodingCharacters.defaultInstance();

    private static final Escaping HL7_ESCAPING = new DefaultEscaping();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Encoding")
            .displayName("Character Encoding")
            .description("The Character Encoding that is used to encode the HL7 data")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    public static final PropertyDescriptor USE_SEGMENT_NAMES = new PropertyDescriptor.Builder()
            .name("use-segment-names")
            .displayName("Use Segment Names")
            .description("Whether or not to use HL7 segment names in attributes")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor PARSE_SEGMENT_FIELDS = new PropertyDescriptor.Builder()
            .name("parse-segment-fields")
            .displayName("Parse Segment Fields")
            .description("Whether or not to parse HL7 segment fields into attributes")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor SKIP_VALIDATION = new PropertyDescriptor.Builder()
            .name("skip-validation")
            .displayName("Skip Validation")
            .description("Whether or not to validate HL7 message values")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor HL7_INPUT_VERSION = new PropertyDescriptor.Builder()
            .name("hl7-input-version")
            .displayName("HL7 Input Version")
            .description("The HL7 version to use for parsing and validation")
            .required(true)
            .allowableValues("autodetect", "2.2", "2.3", "2.3.1", "2.4", "2.5", "2.5.1", "2.6")
            .defaultValue("autodetect")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship if it is properly parsed as HL7 and its attributes extracted")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be mapped to FlowFile Attributes. This would happen if the FlowFile does not contain valid HL7 data")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CHARACTER_SET,
            USE_SEGMENT_NAMES,
            PARSE_SEGMENT_FIELDS,
            SKIP_VALIDATION,
            HL7_INPUT_VERSION
    );

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

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).evaluateAttributeExpressions(flowFile).getValue());
        final Boolean useSegmentNames = context.getProperty(USE_SEGMENT_NAMES).asBoolean();
        final Boolean parseSegmentFields = context.getProperty(PARSE_SEGMENT_FIELDS).asBoolean();
        final Boolean skipValidation = context.getProperty(SKIP_VALIDATION).asBoolean();
        final String inputVersion = context.getProperty(HL7_INPUT_VERSION).getValue();

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));

        @SuppressWarnings("resource")
        final HapiContext hapiContext = new DefaultHapiContext();
        if (!inputVersion.equals("autodetect")) {
            hapiContext.setModelClassFactory(new CanonicalModelClassFactory(inputVersion));
        }
        if (skipValidation) {
            hapiContext.setValidationContext((ValidationContext) ValidationContextFactory.noValidation());
        }

        final PipeParser parser = hapiContext.getPipeParser();
        final String hl7Text = new String(buffer, charset);
        try {
            final Message message = parser.parse(hl7Text);
            final Map<String, String> attributes = getAttributes(message, useSegmentNames, parseSegmentFields);
            flowFile = session.putAllAttributes(flowFile, attributes);
            getLogger().debug("Added the following attributes for {}: {}", flowFile, attributes);
        } catch (final HL7Exception e) {
            getLogger().error("Failed to extract attributes from {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    public static Map<String, String> getAttributes(final Group group, final boolean useNames, final boolean parseFields) throws HL7Exception {
        final Map<String, String> attributes = new TreeMap<>();
        if (!isEmpty(group)) {
            for (final Map.Entry<String, Segment> segmentEntry : getAllSegments(group).entrySet()) {
                final String segmentKey = segmentEntry.getKey();
                final Segment segment = segmentEntry.getValue();
                final Map<String, Type> fields = getAllFields(segmentKey, segment, useNames);
                for (final Map.Entry<String, Type> fieldEntry : fields.entrySet()) {
                    final String fieldKey = fieldEntry.getKey();
                    final Type field = fieldEntry.getValue();
                    // These maybe should used the escaped values, but that would
                    // change the existing non-broken behavior of the processor
                    if (parseFields && (field instanceof Composite) && !isTimestamp(field)) {
                        for (final Map.Entry<String, Type> componentEntry : getAllComponents(fieldKey, field, useNames).entrySet()) {
                            final String componentKey = componentEntry.getKey();
                            final Type component = componentEntry.getValue();
                            final String componentValue = HL7_ESCAPING.unescape(component.encode(), HL7_ENCODING);
                            if (!StringUtils.isEmpty(componentValue)) {
                                attributes.put(componentKey, componentValue);
                            }
                        }
                    } else {
                        final String fieldValue = HL7_ESCAPING.unescape(field.encode(), HL7_ENCODING);
                        if (!StringUtils.isEmpty(fieldValue)) {
                            attributes.put(fieldKey, fieldValue);
                        }
                    }

                }
            }
        }
        return attributes;
    }

    private static Map<String, Segment> getAllSegments(final Group group) throws HL7Exception {
        final Map<String, Segment> segments = new TreeMap<>();
        addSegments(group, segments, new HashMap<>());
        return Collections.unmodifiableMap(segments);
    }

    private static void addSegments(final Group group, final Map<String, Segment> segments, final Map<String, Integer> segmentIndexes) throws HL7Exception {
        if (!isEmpty(group)) {
            for (final String name : group.getNames()) {
                for (final Structure structure : group.getAll(name)) {
                    if (group.isGroup(name) && structure instanceof Group) {
                        addSegments((Group) structure, segments, segmentIndexes);
                    } else if (structure instanceof Segment) {
                        addSegments((Segment) structure, segments, segmentIndexes);
                    }
                }
                segmentIndexes.put(name, segmentIndexes.getOrDefault(name, 1) + 1);
            }
        }
    }

    private static void addSegments(final Segment segment, final Map<String, Segment> segments, final Map<String, Integer> segmentIndexes) throws HL7Exception {
        if (!isEmpty(segment)) {
            final String segmentName = segment.getName();
            final StringBuilder sb = new StringBuilder().append(segmentName);
            if (isRepeating(segment)) {
                final int segmentIndex = segmentIndexes.getOrDefault(segmentName, 1);
                sb.append("_").append(segmentIndex);
            }
            final String segmentKey = sb.toString();
            segments.put(segmentKey, segment);
        }
    }

    private static Map<String, Type> getAllFields(final String segmentKey, final Segment segment, final boolean useNames) throws HL7Exception {
        final Map<String, Type> fields = new TreeMap<>();
        final String[] segmentNames = segment.getNames();
        for (int i = 1; i <= segment.numFields(); i++) {
            final Type[] fieldValues = segment.getField(i);
            if (fieldValues != null && fieldValues.length != 0) {
                final String fieldName;
                //Some user defined segments (e.g. Z segments) will not have corresponding names returned
                //from segment.getNames() above. If we encounter one of these, do the next best thing
                //and return what we otherwise would if we were not in useNames mode.
                String segmentName = segmentNames[i - 1];
                if (useNames && StringUtils.isNotBlank(segmentName)) {
                    fieldName = WordUtils.capitalize(segmentName).replaceAll("\\W+", "");
                } else {
                    fieldName = String.valueOf(i);
                }

                final String fieldKey = "%s.%s".formatted(segmentKey, fieldName);

                //Checks if the field is repeatable, if the max cardinality value is 0 or more than 1 then the field is repeatable
                if (segment.getMaxCardinality(i) == 1) {
                    fields.put(fieldKey, fieldValues[0]);
                } else {
                    for (int j = 0; j < fieldValues.length; j++) {
                        final String repeatableFieldKey = "%s_%s".formatted(fieldKey, j + 1);
                        fields.put(repeatableFieldKey, fieldValues[j]);
                    }
                }
            }
        }
        return fields;
    }

    private static Map<String, Type> getAllComponents(final String fieldKey, final Type field, final boolean useNames) throws HL7Exception {
        final Map<String, Type> components = new TreeMap<>();
        if (!isEmpty(field) && (field instanceof Composite)) {
            if (useNames) {
                final Pattern p = Pattern.compile("^(cm_msg|[a-z][a-z][a-z]?)([0-9]+)_(\\w+)$");
                try {
                    final java.beans.PropertyDescriptor[] properties = PropertyUtils.getPropertyDescriptors(field);
                    for (final java.beans.PropertyDescriptor property : properties) {
                        final String propertyName = property.getName();
                        final Matcher matcher = p.matcher(propertyName);
                        if (matcher.find()) {
                            final Type type = (Type) PropertyUtils.getProperty(field, propertyName);
                            if (!isEmpty(type)) {
                                final String componentName = matcher.group(3);
                                final String typeKey = "%s.%s".formatted(fieldKey, componentName);
                                components.put(typeKey, type);
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            } else {
                final Type[] types = ((Composite) field).getComponents();
                for (int i = 0; i < types.length; i++) {
                    final Type type = types[i];
                    if (!isEmpty(type)) {
                        String fieldName = field.getName();
                        if (fieldName.equals("CM_MSG")) {
                            fieldName = "CM";
                        }
                        final String typeKey = "%s.%s.%s".formatted(fieldKey, fieldName, (i + 1));
                        components.put(typeKey, type);
                    }
                }
            }
        }
        return components;
    }

    private static boolean isTimestamp(final Type field) throws HL7Exception {
        if (isEmpty(field)) {
            return false;
        }
        final String fieldName = field.getName();
        return (fieldName.equals("TS") || fieldName.equals("DT") || fieldName.equals("TM"));
    }

    private static boolean isRepeating(final Segment segment) throws HL7Exception {
        if (isEmpty(segment)) {
            return false;
        }

        final Group parent = segment.getParent();
        final Group grandparent = parent.getParent();
        if (parent == grandparent) {
            return false;
        }

        return grandparent.isRepeating(parent.getName());
    }

    private static boolean isEmpty(final Visitable visitable) throws HL7Exception {
        return (visitable == null || visitable.isEmpty());
    }
}
