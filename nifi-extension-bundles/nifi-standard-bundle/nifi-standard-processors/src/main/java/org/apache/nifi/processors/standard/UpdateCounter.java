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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
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

import java.util.List;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"counter", "debug", "instrumentation"})
@CapabilityDescription("This processor allows users to set specific counters and key points in their flow. It is useful for debugging and basic counting functions.")
@ReadsAttribute(attribute = "counterName", description = "The name of the counter to update/get.")
public class UpdateCounter extends AbstractProcessor {

    static final PropertyDescriptor COUNTER_NAME = new PropertyDescriptor.Builder()
            .name("counter-name")
            .displayName("Counter Name")
            .description("The name of the counter you want to set the value of - supports expression language like ${counterName}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor DELTA = new PropertyDescriptor.Builder()
            .name("delta")
            .displayName("Delta")
            .description("Adjusts the counter by the specified delta for each flow file received. May be a positive or negative integer.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            COUNTER_NAME,
            DELTA
    );

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Counter was updated/retrieved")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        session.adjustCounter(context.getProperty(COUNTER_NAME).evaluateAttributeExpressions(flowFile).getValue(),
                Long.parseLong(context.getProperty(DELTA).evaluateAttributeExpressions(flowFile).getValue()),
                false
        );
        session.transfer(flowFile, SUCCESS);
    }
}
