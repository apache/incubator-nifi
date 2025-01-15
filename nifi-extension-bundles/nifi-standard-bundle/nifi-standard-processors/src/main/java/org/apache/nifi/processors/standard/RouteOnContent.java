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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeValueDecorator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"route", "content", "regex", "regular expression", "regexp", "find", "text", "string", "search", "filter", "detect"})
@CapabilityDescription("Applies Regular Expressions to the content of a FlowFile and routes a copy of the FlowFile to each "
        + "destination whose Regular Expression matches. Regular Expressions are added as User-Defined Properties where the name "
        + "of the property is the name of the relationship and the value is a Regular Expression to match against the FlowFile "
        + "content. User-Defined properties do support the Attribute Expression Language, but the results are interpreted as "
        + "literal values, not Regular Expressions")
@DynamicProperty(name = "Relationship Name", value = "A Regular Expression", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                 description = "Routes FlowFiles whose content matches the regular expression defined by Dynamic Property's value to the "
                         + "Relationship defined by the Dynamic Property's key")
@DynamicRelationship(name = "Name from Dynamic Property", description = "FlowFiles that match the Dynamic Property's Regular Expression")
public class RouteOnContent extends AbstractProcessor {

    public static final String ROUTE_ATTRIBUTE_KEY = "RouteOnContent.Route";

    public static final String MATCH_ALL = "content must match exactly";
    public static final String MATCH_SUBSEQUENCE = "content must contain match";

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Content Buffer Size")
            .description("Specifies the maximum amount of data to buffer in order to apply the regular expressions. If the size of the FlowFile "
                    + "exceeds this value, any amount of this value will be ignored")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();
    public static final PropertyDescriptor MATCH_REQUIREMENT = new PropertyDescriptor.Builder()
            .name("Match Requirement")
            .description("Specifies whether the entire content of the file must match the regular expression exactly, or if any part of the file "
                    + "(up to Content Buffer Size) can contain the regular expression in order to be considered a match")
            .required(true)
            .allowableValues(MATCH_ALL, MATCH_SUBSEQUENCE)
            .defaultValue(MATCH_ALL)
            .build();
    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the file is encoded")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            MATCH_REQUIREMENT,
            CHARACTER_SET,
            BUFFER_SIZE
    );

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles that do not match any of the user-supplied regular expressions will be routed to this relationship")
            .build();

    private final AtomicReference<Set<Relationship>> relationships = new AtomicReference<>(Set.of(REL_NO_MATCH));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (propertyDescriptorName.equals(REL_NO_MATCH.getName())) {
            return null;
        }

        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isDynamic()) {
            final Set<Relationship> relationships = new HashSet<>(this.relationships.get());
            final Relationship relationship = new Relationship.Builder().name(descriptor.getName()).build();

            if (newValue == null) {
                relationships.remove(relationship);
            } else {
                relationships.add(relationship);
            }

            this.relationships.set(relationships);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final List<FlowFile> flowFiles = session.get(1);
        if (flowFiles.isEmpty()) {
            return;
        }

        final AttributeValueDecorator quoteDecorator = attributeValue -> (attributeValue == null) ? null : Pattern.quote(attributeValue);

        final Map<FlowFile, Set<Relationship>> flowFileDestinationMap = new HashMap<>();
        final ComponentLog logger = getLogger();

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final byte[] buffer = new byte[context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue()];
        for (final FlowFile flowFile : flowFiles) {
            final Set<Relationship> destinations = new HashSet<>();
            flowFileDestinationMap.put(flowFile, destinations);

            final AtomicInteger bufferedByteCount = new AtomicInteger(0);
            session.read(flowFile, in -> bufferedByteCount.set(StreamUtils.fillBuffer(in, buffer, false)));

            final String contentString = new String(buffer, 0, bufferedByteCount.get(), charset);

            for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
                if (!descriptor.isDynamic()) {
                    continue;
                }

                final String regex = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile, quoteDecorator).getValue();
                final Pattern pattern = Pattern.compile(regex);
                final boolean matches;
                if (context.getProperty(MATCH_REQUIREMENT).getValue().equalsIgnoreCase(MATCH_ALL)) {
                    matches = pattern.matcher(contentString).matches();
                } else {
                    matches = pattern.matcher(contentString).find();
                }

                if (matches) {
                    final Relationship relationship = new Relationship.Builder().name(descriptor.getName()).build();
                    destinations.add(relationship);
                }
            }
        }

        for (final Map.Entry<FlowFile, Set<Relationship>> entry : flowFileDestinationMap.entrySet()) {
            FlowFile flowFile = entry.getKey();
            final Set<Relationship> destinations = entry.getValue();

            if (destinations.isEmpty()) {
                flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_KEY, REL_NO_MATCH.getName());
                session.transfer(flowFile, REL_NO_MATCH);
                session.getProvenanceReporter().route(flowFile, REL_NO_MATCH);
                logger.info("Routing {} to 'unmatched'", flowFile);
            } else {
                final Relationship firstRelationship = destinations.iterator().next();
                destinations.remove(firstRelationship);

                for (final Relationship relationship : destinations) {
                    FlowFile clone = session.clone(flowFile);
                    clone = session.putAttribute(clone, ROUTE_ATTRIBUTE_KEY, relationship.getName());
                    session.getProvenanceReporter().route(clone, relationship);
                    session.transfer(clone, relationship);
                    logger.info("Cloning {} to {} and routing clone to {}", flowFile, clone, relationship);
                }

                flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_KEY, firstRelationship.getName());
                session.getProvenanceReporter().route(flowFile, firstRelationship);
                session.transfer(flowFile, firstRelationship);
                logger.info("Routing {} to {}", flowFile, firstRelationship);
            }
        }
    }
}
