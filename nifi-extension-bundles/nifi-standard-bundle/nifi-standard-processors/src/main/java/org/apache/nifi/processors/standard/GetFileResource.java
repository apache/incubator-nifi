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
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({ "test", "file", "generate", "load" })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("""
        This processor creates FlowFiles with the content of the configured File Resource. GetFileResource
        is useful for load testing, configuration, and simulation.
        """)
@DynamicProperty(
        name = "Generated FlowFile attribute name", value = "Generated FlowFile attribute value", expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
        description = "Specifies an attribute on generated FlowFiles defined by the Dynamic Property's key and value."
)
@WritesAttributes(
    {
            @WritesAttribute(attribute = "mime.type", description = "Sets the MIME type of the output if the 'MIME Type' property is set"),
            @WritesAttribute(attribute = "Dynamic property key", description = "Value for the corresponding dynamic property, if any is set")
    }
)
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
@Restricted(
        restrictions = {
                @Restriction(requiredPermission = RequiredPermission.READ_FILESYSTEM, explanation = "Provides operator the ability to read from any file that NiFi has access to."),
                @Restriction(requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES, explanation = "File Resource can reference resources over HTTP/HTTPS")
        }
)
public class GetFileResource extends AbstractProcessor {

    public static final PropertyDescriptor FILE_RESOURCE = new PropertyDescriptor.Builder()
            .name("File Resource")
            .description("Location of the File Resource (Local File or URL). This file will be used as content of the generated FlowFiles.")
            .required(true)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.URL)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor MIME_TYPE = new PropertyDescriptor.Builder()
            .name("MIME Type")
            .description("Specifies the value to set for the [mime.type] attribute.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        FILE_RESOURCE,
        MIME_TYPE
    );

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        SUCCESS
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .description("Specifies an attribute on generated FlowFiles defined by the Dynamic Property's key and value.")
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .dynamic(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final Map<PropertyDescriptor, String> processorProperties = context.getProperties();
        final Map<String, String> generatedAttributes = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : processorProperties.entrySet()) {
            final PropertyDescriptor property = entry.getKey();
            if (property.isDynamic()) {
                final String dynamicValue = context.getProperty(property).evaluateAttributeExpressions().getValue();
                generatedAttributes.put(property.getName(), dynamicValue);
            }
        }

        if (context.getProperty(MIME_TYPE).isSet()) {
            generatedAttributes.put(CoreAttributes.MIME_TYPE.key(), context.getProperty(MIME_TYPE).getValue());
        }

        FlowFile flowFile = session.create();

        try (final InputStream inputStream = context.getProperty(FILE_RESOURCE).asResource().read()) {
            flowFile = session.write(flowFile, out -> StreamUtils.copy(inputStream, out));
        } catch (IOException e) {
            getLogger().error("Could not create FlowFile from Resource [{}]", context.getProperty(FILE_RESOURCE).getValue(), e);
        }

        flowFile = session.putAllAttributes(flowFile, generatedAttributes);
        session.getProvenanceReporter().create(flowFile);
        session.transfer(flowFile, SUCCESS);
    }
}
