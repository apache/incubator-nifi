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
package org.apache.nifi.lookup;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;

@Tags({"lookup", "parse", "record", "row", "reader"})
@SeeAlso({RecordSetWriterLookup.class})
@CapabilityDescription("Provides a RecordReaderFactory that can be used to dynamically select another RecordReaderFactory. " +
    "This will allow multiple RecordReaderFactories to be defined and registered, and then selected " +
    "dynamically at runtime by referencing a FlowFile attribute in the Service to Use property.")
@DynamicProperty(name = "Name of the RecordReader", value = "A RecordReaderFactory controller service", description = "", expressionLanguageScope = NONE)
public class ReaderLookup extends AbstractControllerService implements RecordReaderFactory {

    static final PropertyDescriptor SERVICE_TO_USE = new Builder()
        .name("Service to Use")
        .displayName("Service to Use")
        .description("Specifies the name of the user-defined property whose associated Controller Service should be used.")
        .required(true)
        .defaultValue("${recordreader.name}")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        SERVICE_TO_USE
    );

    private volatile Map<String, RecordReaderFactory> recordReaderFactoryMap;
    private volatile PropertyValue serviceToUseValue;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new Builder()
            .name(propertyDescriptorName)
            .description("The RecordReaderFactory to return when '" + propertyDescriptorName + "' is the chosen Record Reader")
            .identifiesControllerService(RecordReaderFactory.class)
            .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        final Set<String> serviceNames = new HashSet<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                serviceNames.add(descriptor.getName());
            }

            final String referencedId = context.getProperty(descriptor).getValue();
            if (this.getIdentifier().equals(referencedId)) {
                results.add(new ValidationResult.Builder()
                        .subject(descriptor.getDisplayName())
                        .explanation("The current service cannot be registered as a RecordReaderFactory to lookup")
                        .valid(false)
                        .build());
            }
        }

        if (serviceNames.isEmpty()) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .explanation("At least one RecordReaderFactory must be defined via dynamic properties")
                    .valid(false)
                    .build());
        }

        final PropertyValue serviceToUseValue = context.getProperty(SERVICE_TO_USE);
        if (!serviceToUseValue.isExpressionLanguagePresent()) {
            final String selectedValue = serviceToUseValue.getValue();
            if (!serviceNames.contains(selectedValue)) {
                results.add(new ValidationResult.Builder()
                    .subject(SERVICE_TO_USE.getDisplayName())
                    .explanation("No service is defined with the name <" + selectedValue + ">")
                    .valid(false)
                    .build());
            }
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final Map<String, RecordReaderFactory> serviceMap = new HashMap<>();

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                final RecordReaderFactory recordReaderFactory = context.getProperty(descriptor).asControllerService(RecordReaderFactory.class);
                serviceMap.put(descriptor.getName(), recordReaderFactory);
            }
        }

        recordReaderFactoryMap = Collections.unmodifiableMap(serviceMap);
        serviceToUseValue = context.getProperty(SERVICE_TO_USE);
    }


    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger)
                throws MalformedRecordException, IOException, SchemaNotFoundException {

        final String serviceName = serviceToUseValue.evaluateAttributeExpressions(variables).getValue();
        if (serviceName.trim().isEmpty()) {
            throw new ProcessException("Unable to determine which Record Reader to use: after evaluating the property value against supplied variables, got an empty value");
        }

        final RecordReaderFactory recordReaderFactory = recordReaderFactoryMap.get(serviceName);
        if (recordReaderFactory == null) {
            throw new ProcessException("No RecordReaderFactory was configured with the name <" + serviceName + ">");
        }

        return recordReaderFactory.createRecordReader(variables, in, inputLength, logger);
    }
}