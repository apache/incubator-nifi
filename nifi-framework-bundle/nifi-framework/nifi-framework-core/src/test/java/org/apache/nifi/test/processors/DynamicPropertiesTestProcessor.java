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
package org.apache.nifi.test.processors;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DynamicPropertiesTestProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();

    private static final Logger LOG = LoggerFactory.getLogger(DynamicPropertiesTestProcessor.class);

    private final Set<Relationship> relationships;

    private volatile Set<String> dynamicPropertyNames = new HashSet<>();

    public Set<String> getDynamicPropertyNames() {
        return dynamicPropertyNames;
    }

    public static final PropertyDescriptor STATIC_PROPERTY = new PropertyDescriptor.Builder()
            .name("static-property")
            .displayName("Static Property")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        STATIC_PROPERTY
    );

    private ProcessorNode processorNode;

    public void setProcessorNode(ProcessorNode processorNode) {
        this.processorNode = processorNode;
    }

    public DynamicPropertiesTestProcessor() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(rels);
    }
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {

        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isDynamic()) {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {    // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }
            this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    @OnScheduled
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        processorNode.getProcessGroup().terminateProcessor(processorNode);
        FlowFile flowFile = session.get();
        if (flowFile != null) {
            LOG.warn("Removing flow file");
            session.remove(flowFile);
        }
    }

}
