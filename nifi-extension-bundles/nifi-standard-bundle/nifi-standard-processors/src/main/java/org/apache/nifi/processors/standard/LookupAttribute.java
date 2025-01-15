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
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"lookup", "cache", "enrich", "join", "attributes", "Attribute Expression Language"})
@CapabilityDescription("Lookup attributes from a lookup service")
@DynamicProperty(name = "The name of the attribute to add to the FlowFile",
    value = "The name of the key or property to retrieve from the lookup service",
    expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    description = "Adds a FlowFile attribute specified by the dynamic property's key with the value found in the lookup service using the the dynamic property's value")
public class LookupAttribute extends AbstractProcessor {

    public static final PropertyDescriptor LOOKUP_SERVICE =
        new PropertyDescriptor.Builder()
            .name("lookup-service")
            .displayName("Lookup Service")
            .description("The lookup service to use for attribute lookups")
            .identifiesControllerService(StringLookupService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor INCLUDE_EMPTY_VALUES =
        new PropertyDescriptor.Builder()
            .name("include-empty-values")
            .displayName("Include Empty Values")
            .description("Include null or blank values for keys that are null or blank")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            LOOKUP_SERVICE,
            INCLUDE_EMPTY_VALUES
    );

    public static final Relationship REL_MATCHED = new Relationship.Builder()
            .description("FlowFiles with matching lookups are routed to this relationship")
            .name("matched")
            .build();

    public static final Relationship REL_UNMATCHED = new Relationship.Builder()
            .description("FlowFiles with missing lookups are routed to this relationship")
            .name("unmatched")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .description("FlowFiles with failing lookups are routed to this relationship")
            .name("failure")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_MATCHED,
            REL_UNMATCHED,
            REL_FAILURE
    );

    private Map<PropertyDescriptor, PropertyValue> dynamicProperties;

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> errors = new ArrayList<>(super.customValidate(validationContext));

        final Set<PropertyDescriptor> dynamicProperties = validationContext.getProperties().keySet().stream()
            .filter(PropertyDescriptor::isDynamic)
            .collect(Collectors.toSet());

        if (dynamicProperties.isEmpty()) {
            errors.add(new ValidationResult.Builder()
                .subject("User-Defined Properties")
                .valid(false)
                .explanation("At least one user-defined property must be specified.")
                .build());
        }

        final Set<String> requiredKeys = validationContext.getProperty(LOOKUP_SERVICE).asControllerService(LookupService.class).getRequiredKeys();
        if (requiredKeys == null || requiredKeys.size() != 1) {
            errors.add(new ValidationResult.Builder()
                .subject(LOOKUP_SERVICE.getDisplayName())
                .valid(false)
                .explanation("LookupAttribute requires a key-value lookup service supporting exactly one required key.")
                .build());
        }

        return errors;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .required(false)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dynamic(true)
            .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Load up all the dynamic properties once for use later in onTrigger
        final Map<PropertyDescriptor, PropertyValue> dynamicProperties = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> e : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = e.getKey();
            if (descriptor.isDynamic()) {
                final PropertyValue value = context.getProperty(descriptor);
                dynamicProperties.put(descriptor, value);
            }
        }
        this.dynamicProperties = Map.copyOf(dynamicProperties);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        final LookupService lookupService = context.getProperty(LOOKUP_SERVICE).asControllerService(LookupService.class);
        final boolean includeEmptyValues = context.getProperty(INCLUDE_EMPTY_VALUES).asBoolean();
        for (FlowFile flowFile : session.get(50)) {
            try {
                onTrigger(logger, lookupService, includeEmptyValues, flowFile, session);
            } catch (final IOException e) {
                throw new ProcessException(e.getMessage(), e);
            }
        }
    }

    private void onTrigger(ComponentLog logger, LookupService lookupService,
        boolean includeEmptyValues, FlowFile flowFile, ProcessSession session)
        throws ProcessException, IOException {

        final Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());

        boolean matched = false;
        try {
            final Set<String> requiredKeys = lookupService.getRequiredKeys();
            if (requiredKeys == null || requiredKeys.size() != 1) {
                throw new ProcessException("LookupAttribute requires a key-value lookup service supporting exactly one required key, was: " +
                    (requiredKeys == null ? "null" : String.valueOf(requiredKeys.size())));
            }

            final String coordinateKey = requiredKeys.iterator().next();
            for (final Map.Entry<PropertyDescriptor, PropertyValue> e : dynamicProperties.entrySet()) {
                final PropertyValue lookupKeyExpression = e.getValue();
                final String lookupKey = lookupKeyExpression.evaluateAttributeExpressions(flowFile).getValue();
                final String attributeName = e.getKey().getName();
                final Optional<String> attributeValue = lookupService.lookup(Collections.singletonMap(coordinateKey, lookupKey),
                        flowFile.getAttributes());
                matched = putAttribute(attributeName, attributeValue, attributes, includeEmptyValues, logger) || matched;

                if (!matched && logger.isDebugEnabled()) {
                    logger.debug("No such value for key: {}", lookupKey);
                }
            }

            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, matched ? REL_MATCHED : REL_UNMATCHED);

        } catch (final LookupFailureException e) {
            logger.error(e.getMessage(), e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private boolean putAttribute(final String attributeName, final Optional<String> attributeValue, final Map<String, String> attributes, final boolean includeEmptyValues, final ComponentLog logger) {
        boolean matched = false;
        if (attributeValue.isPresent() && StringUtils.isNotBlank(attributeValue.get())) {
            attributes.put(attributeName, attributeValue.get());
            matched = true;
        } else if (includeEmptyValues) {
            attributes.put(attributeName, attributeValue.isPresent() ? "" : "null");
            matched = true;
        }
        return matched;
    }
}
