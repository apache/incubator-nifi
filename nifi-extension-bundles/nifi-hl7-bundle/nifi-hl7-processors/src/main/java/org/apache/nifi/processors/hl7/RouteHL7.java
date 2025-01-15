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

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hl7.hapi.HapiMessage;
import org.apache.nifi.hl7.model.HL7Message;
import org.apache.nifi.hl7.query.HL7Query;
import org.apache.nifi.hl7.query.QueryResult;
import org.apache.nifi.hl7.query.exception.HL7QueryParsingException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.validation.impl.ValidationContextFactory;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"HL7", "healthcare", "route", "Health Level 7"})
@DynamicProperties({
    @DynamicProperty(name = "Name of a Relationship", value = "An HL7 Query Language query",
            description = "If a FlowFile matches the query, it will be routed to a relationship with the name of the property")})
@WritesAttributes({
    @WritesAttribute(attribute = "RouteHL7.Route", description = "The name of the relationship to which the FlowFile was routed")})
@CapabilityDescription("Routes incoming HL7 data according to user-defined queries. To add a query, add a new property to the processor."
        + " The name of the property will become a new relationship for the processor, and the value is an HL7 Query Language query. If"
        + " a FlowFile matches the query, a copy of the FlowFile will be routed to the associated relationship.")
public class RouteHL7 extends AbstractProcessor {

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Encoding")
            .description("The Character Encoding that is used to encode the HL7 data")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be parsed as HL7 will be routed to this relationship")
            .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that comes into this processor will be routed to this relationship, unless it is routed to 'failure'")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CHARACTER_SET
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_FAILURE,
            REL_ORIGINAL
    );

    private volatile Map<Relationship, HL7Query> queries = new HashMap<>();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Specifies a query that will cause any HL7 message matching the query to be routed to the '" + propertyDescriptorName + "' relationship")
                .required(false)
                .dynamic(true)
                .addValidator(new HL7QueryValidator())
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (!descriptor.isDynamic()) {
            return;
        }

        final Map<Relationship, HL7Query> updatedQueryMap = new HashMap<>(queries);
        final Relationship relationship = new Relationship.Builder().name(descriptor.getName()).build();

        if (newValue == null) {
            updatedQueryMap.remove(relationship);
        } else {
            final HL7Query query = HL7Query.compile(newValue);
            updatedQueryMap.put(relationship, query);
        }

        this.queries = updatedQueryMap;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(queries.keySet());
        relationships.addAll(RELATIONSHIPS);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).evaluateAttributeExpressions(flowFile).getValue());

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));

        @SuppressWarnings("resource")
        final HapiContext hapiContext = new DefaultHapiContext();
        hapiContext.setValidationContext((ca.uhn.hl7v2.validation.ValidationContext) ValidationContextFactory.noValidation());

        final PipeParser parser = hapiContext.getPipeParser();
        final String hl7Text = new String(buffer, charset);
        final HL7Message message;
        try {
            final Message hapiMessage = parser.parse(hl7Text);
            message = new HapiMessage(hapiMessage);
        } catch (final Exception e) {
            getLogger().error("Routing to failure  since failed to parse {} as HL7", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Set<String> matchingRels = new HashSet<>();
        final Map<Relationship, HL7Query> queryMap = queries;
        for (final Map.Entry<Relationship, HL7Query> entry : queryMap.entrySet()) {
            final Relationship relationship = entry.getKey();
            final HL7Query query = entry.getValue();

            final QueryResult result = query.evaluate(message);
            if (result.isMatch()) {
                FlowFile clone = session.clone(flowFile);
                clone = session.putAttribute(clone, "RouteHL7.Route", relationship.getName());
                session.transfer(clone, relationship);
                session.getProvenanceReporter().route(clone, relationship);
                matchingRels.add(relationship.getName());
            }
        }

        session.transfer(flowFile, REL_ORIGINAL);
        getLogger().info("Routed a copy of {} to {} relationships: {}", flowFile, matchingRels.size(), matchingRels);
    }

    private static class HL7QueryValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            String error = null;

            try {
                final HL7Query hl7Query = HL7Query.compile(input);
                final List<Class<?>> returnTypes = hl7Query.getReturnTypes();
                if (returnTypes.size() != 1) {
                    error = "RouteHL7 requires that the HL7 Query return exactly 1 element of type MESSAGE";
                } else if (!HL7Message.class.isAssignableFrom(returnTypes.getFirst())) {
                    error = "RouteHL7 requires that the HL7 Query return exactly 1 element of type MESSAGE";
                }
            } catch (final HL7QueryParsingException e) {
                error = e.toString();
            }

            if (error == null) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            } else {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(error).build();
            }
        }

    }
}
