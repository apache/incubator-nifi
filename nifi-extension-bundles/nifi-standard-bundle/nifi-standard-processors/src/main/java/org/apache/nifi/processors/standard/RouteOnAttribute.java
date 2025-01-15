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

import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 * This processor routes a FlowFile based on its flow file attributes by using the Attribute Expression Language. The Expression Language is used by adding Optional Properties to the processor. The
 * name of the Property indicates the name of the relationship to which a FlowFile will be routed if matched. The value of the Property indicates an Attribute Expression Language Expression that will
 * be used to determine whether or not a given FlowFile will be routed to the associated relationship. If multiple expressions match a FlowFile's attributes, that FlowFile will be cloned and routed to
 * each corresponding relationship. If none of the supplied expressions matches for a given FlowFile, that FlowFile will be routed to the 'unmatched' relationship.
 * </p>
 *
 */
@SideEffectFree
@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"attributes", "routing", "Attribute Expression Language", "regexp", "regex", "Regular Expression", "Expression Language", "find", "text", "string", "search", "filter", "detect"})
@CapabilityDescription("Routes FlowFiles based on their Attributes using the Attribute Expression Language")
@DynamicProperty(name = "Relationship Name", value = "Expression Language expression that returns a boolean value indicating whether or not the FlowFile should be routed to this Relationship",
    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    description = "Routes FlowFiles whose attributes match the Expression Language specified in the Dynamic Property Value to the Relationship " +
                  "specified in the Dynamic Property Key")
@DynamicRelationship(name = "Name from Dynamic Property", description = "FlowFiles that match the Dynamic Property's Attribute Expression Language")
@WritesAttributes({
    @WritesAttribute(attribute = RouteOnAttribute.ROUTE_ATTRIBUTE_KEY, description = "The relation to which the FlowFile was routed")
})
@UseCase(
    description = "Route data to one or more relationships based on its attributes using the NiFi Expression Language.",
    keywords = {"attributes", "routing", "expression language"},
    configuration = """
        Set the "Routing Strategy" property to "Route to Property name".
        For each route that a FlowFile might be routed to, add a new property. The name of the property should describe the route.
        The value of the property is an Attribute Expression Language expression that returns a boolean value indicating whether or not a given FlowFile will be routed to the \
        associated relationship.

        For example, we might route data based on its file extension using the following properties:
            - "Routing Strategy" = "Route to Property Name"
            - "jpg" = "${filename:endsWith('.jpg')}"
            - "png" = "${filename:endsWith('.png')}"
            - "pdf" = "${filename:endsWith('.pdf')}"

        The Processor will now have 3 relationships: `jpg`, `png`, and `pdf`. Each of these should be connected to the appropriate downstream processor.
        """
)
@UseCase(
    description = "Keep data only if its attributes meet some criteria, such as its filename ends with .txt.",
    keywords = {"keep", "filter", "remove", "delete", "expression language"},
    configuration = """
        Add a new property for each condition that must be satisfied in order to keep the data.
        If the data should be kept in the case that any of the provided conditions is met, set the "Routing Strategy" property to "Route to 'matched' if any matches".
        If all conditions must be met in order to keep the data, set the "Routing Strategy" property  to "Route to 'matched' if all match".

        For example, to keep files whose filename ends with .txt and have a file size of at least 1000 bytes, we will use the following properties:
            - "ends_with_txt" = "${filename:endsWith('.txt')}"
            - "large_enough" = "${fileSize:ge(1000)}
            - "Routing Strategy" = "Route to 'matched' if all match"

        Auto-terminate the 'unmatched' relationship.
        Connect the 'matched' relationship to the next processor in the flow.
        """
)
@UseCase(
    description = "Discard or drop a file based on attributes, such as filename.",
    keywords = {"discard", "drop", "filter", "remove", "delete", "expression language"},
    configuration = """
        Add a new property for each condition that must be satisfied in order to drop the data.
        If the data should be dropped in the case that any of the provided conditions is met, set the "Routing Strategy" property to "Route to 'matched' if any matches".
        If all conditions must be met in order to drop the data, set the "Routing Strategy" property  to "Route to 'matched' if all match".

        Here are a couple of examples for configuring the properties:
            Example 1 Use Case: Data should be dropped if its "uuid" attribute has an 'a' in it or ends with '0'.
              Here, we will use the following properties:
                - "has_a" = "${uuid:contains('a')}"
                - "ends_with_0" = "${uuid:endsWith('0')}
                - "Routing Strategy" = "Route to 'matched' if any matches"
            Example 2 Use Case: Data should be dropped if its 'uuid' attribute has an 'a' AND it ends with a '1'.
              Here, we will use the following properties:
                - "has_a" = "${uuid:contains('a')}"
                - "ends_with_1" = "${uuid:endsWith('1')}
                - "Routing Strategy" = "Route to 'matched' if all match"

        Auto-terminate the 'matched' relationship.
        Connect the 'unmatched' relationship to the next processor in the flow.
        """
)
@MultiProcessorUseCase(
    description = "Route record-oriented data based on whether or not the record's values meet some criteria",
    keywords = {"record", "route", "content", "data"},
    configurations = {
        @ProcessorConfiguration(
            processorClass = PartitionRecord.class,
            configuration = """
                Choose a RecordReader that is appropriate based on the format of the incoming data.
                Choose a RecordWriter that writes the data in the desired output format.

                Add a single additional property. The name of the property should describe the criteria to route on. \
                The property's value should be a RecordPath that returns `true` if the Record meets the criteria or `false` otherwise. \
                This adds a new attribute to the FlowFile whose name is equal to the property name.

                Connect the 'success' Relationship to RouteOnAttribute.
                """
        ),
        @ProcessorConfiguration(
            processorClass = RouteOnAttribute.class,
            configuration = """
                Set "Routing Strategy" to "Route to Property name"

                Add two additional properties. For the first one, the name of the property should describe data that matches the criteria. \
                The value is an Expression Language expression that checks if the attribute added by the PartitionRecord processor has a value of `true`. \
                For example, `${criteria:equals('true')}`.
                The second property should have a name that describes data that does not match the criteria. The value is an Expression Language that evaluates to the \
                opposite of the first property value. For example, `${criteria:equals('true'):not()}`.

                Connect each of the newly created Relationships to the appropriate downstream processors.
                """
        )
    }
)
public class RouteOnAttribute extends AbstractProcessor {

    public static final String ROUTE_ATTRIBUTE_KEY = "RouteOnAttribute.Route";

    // keep the word 'match' instead of 'matched' to maintain backward compatibility (there was a typo originally)
    private static final String routeAllMatchValue = "Route to 'match' if all match";
    private static final String routeAnyMatches = "Route to 'match' if any matches";
    private static final String routePropertyNameValue = "Route to Property name";

    public static final AllowableValue ROUTE_PROPERTY_NAME = new AllowableValue(routePropertyNameValue, "Route to Property name",
            "A copy of the FlowFile will be routed to each relationship whose corresponding expression evaluates to 'true'");
    public static final AllowableValue ROUTE_ALL_MATCH = new AllowableValue(routeAllMatchValue, "Route to 'matched' if all match",
            "Requires that all user-defined expressions evaluate to 'true' for the FlowFile to be considered a match");
    // keep the word 'match' instead of 'matched' to maintain backward compatibility (there was a typo originally)
    public static final AllowableValue ROUTE_ANY_MATCHES = new AllowableValue(routeAnyMatches,
            "Route to 'matched' if any matches",
            "Requires that at least one user-defined expression evaluate to 'true' for the FlowFile to be considered a match");

    public static final PropertyDescriptor ROUTE_STRATEGY = new PropertyDescriptor.Builder()
            .name("Routing Strategy")
            .description("Specifies how to determine which relationship to use when evaluating the Expression Language")
            .required(true)
            .allowableValues(ROUTE_PROPERTY_NAME, ROUTE_ALL_MATCH, ROUTE_ANY_MATCHES)
            .defaultValue(ROUTE_PROPERTY_NAME.getValue())
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ROUTE_STRATEGY
    );

    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles that do not match any user-define expression will be routed here")
            .build();
    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles will be routed to 'match' if one or all Expressions match, depending on the configuration of the Routing Strategy property")
            .build();

    private final AtomicReference<Set<Relationship>> relationships = new AtomicReference<>(Set.of(REL_NO_MATCH));
    private volatile String configuredRouteStrategy = ROUTE_STRATEGY.getDefaultValue();
    private volatile Set<String> dynamicPropertyNames = new HashSet<>();

    /**
     * Cache of dynamic properties set during {@link #onScheduled(ProcessContext)} for quick access in
     * {@link #onTrigger(ProcessContext, ProcessSession)}
     */
    private volatile Map<Relationship, PropertyValue> propertyMap = new HashMap<>();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships.get();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.BOOLEAN, false))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(ROUTE_STRATEGY)) {
            configuredRouteStrategy = newValue;
        } else {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {    // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }

            this.dynamicPropertyNames = Set.copyOf(newDynamicPropertyNames);
        }

        // formulate the new set of Relationships
        final Set<String> allDynamicProps = this.dynamicPropertyNames;
        final Set<Relationship> newRelationships = new HashSet<>();
        final String routeStrategy = configuredRouteStrategy;
        if (ROUTE_PROPERTY_NAME.equals(routeStrategy)) {
            for (final String propName : allDynamicProps) {
                newRelationships.add(new Relationship.Builder().name(propName).build());
            }
        } else {
            newRelationships.add(REL_MATCH);
        }

        newRelationships.add(REL_NO_MATCH);
        this.relationships.set(newRelationships);
    }

    /**
     * When this processor is scheduled, update the dynamic properties into the map
     * for quick access during each onTrigger call
     * @param context ProcessContext used to retrieve dynamic properties
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final Map<Relationship, PropertyValue> newPropertyMap = new HashMap<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (!descriptor.isDynamic()) {
                continue;
            }
            getLogger().debug("Adding new dynamic property: {}", descriptor);
            newPropertyMap.put(new Relationship.Builder().name(descriptor.getName()).build(), context.getProperty(descriptor));
        }

        this.propertyMap = newPropertyMap;
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final Map<Relationship, PropertyValue> propMap = this.propertyMap;
        final Set<Relationship> matchingRelationships = new HashSet<>();
        for (final Map.Entry<Relationship, PropertyValue> entry : propMap.entrySet()) {
            final PropertyValue value = entry.getValue();

            final boolean matches = value.evaluateAttributeExpressions(flowFile).asBoolean();
            if (matches) {
                matchingRelationships.add(entry.getKey());
            }
        }

        final Set<Relationship> destinationRelationships = new HashSet<>();
        switch (context.getProperty(ROUTE_STRATEGY).getValue()) {
            case routeAllMatchValue:
                if (matchingRelationships.size() == propMap.size()) {
                    destinationRelationships.add(REL_MATCH);
                } else {
                    destinationRelationships.add(REL_NO_MATCH);
                }
                break;
            case routeAnyMatches:
                if (matchingRelationships.isEmpty()) {
                    destinationRelationships.add(REL_NO_MATCH);
                } else {
                    destinationRelationships.add(REL_MATCH);
                }
                break;
            case routePropertyNameValue:
            default:
                destinationRelationships.addAll(matchingRelationships);
                break;
        }

        if (destinationRelationships.isEmpty()) {
            logger.info("Routing {} to unmatched", flowFile);
            flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_KEY, REL_NO_MATCH.getName());
            session.getProvenanceReporter().route(flowFile, REL_NO_MATCH);
            session.transfer(flowFile, REL_NO_MATCH);
        } else {
            final Iterator<Relationship> relationshipNameIterator = destinationRelationships.iterator();
            final Relationship firstRelationship = relationshipNameIterator.next();
            final Map<Relationship, FlowFile> transferMap = new HashMap<>();

            // make all the clones for any remaining relationships
            while (relationshipNameIterator.hasNext()) {
                final Relationship relationship = relationshipNameIterator.next();
                final FlowFile cloneFlowFile = session.clone(flowFile);
                transferMap.put(relationship, cloneFlowFile);
            }

            // now transfer any clones generated
            for (final Map.Entry<Relationship, FlowFile> entry : transferMap.entrySet()) {
                logger.info("Cloned {} into {} and routing clone to relationship {}", flowFile, entry.getValue(), entry.getKey());
                FlowFile updatedFlowFile = session.putAttribute(entry.getValue(), ROUTE_ATTRIBUTE_KEY, entry.getKey().getName());
                session.getProvenanceReporter().route(updatedFlowFile, entry.getKey());
                session.transfer(updatedFlowFile, entry.getKey());
            }

            //now transfer the original flow file
            logger.info("Routing {} to {}", flowFile, firstRelationship);
            session.getProvenanceReporter().route(flowFile, firstRelationship);
            flowFile = session.putAttribute(flowFile, ROUTE_ATTRIBUTE_KEY, firstRelationship.getName());
            session.transfer(flowFile, firstRelationship);
        }
    }
}