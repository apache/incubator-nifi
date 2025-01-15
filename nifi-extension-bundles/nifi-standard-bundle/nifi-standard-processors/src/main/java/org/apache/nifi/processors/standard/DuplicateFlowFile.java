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
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
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

@SupportsBatching
@Tags({"test", "load", "duplicate"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Intended for load testing, this processor will create the configured number of copies of each incoming FlowFile. The original FlowFile as well as all "
+ "generated copies are sent to the 'success' relationship. In addition, each FlowFile gets an attribute 'copy.index' set to the copy number, where the original FlowFile gets "
+ "a value of zero, and all copies receive incremented integer values.")
@WritesAttributes({
        @WritesAttribute(attribute = "copy.index", description = "A zero-based incrementing integer value based on which copy the FlowFile is.")
})
public class DuplicateFlowFile extends AbstractProcessor {

    public static final String COPY_INDEX_ATTRIBUTE = "copy.index";

    static final PropertyDescriptor NUM_COPIES = new PropertyDescriptor.Builder()
    .name("Number of Copies")
    .displayName("Number of Copies")
    .description("Specifies how many copies of each incoming FlowFile will be made")
    .required(true)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .defaultValue("100")
    .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
    .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        NUM_COPIES
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
    .name("success")
    .description("The original FlowFile and all copies will be sent to this relationship")
    .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        for (int i = 1; i <= context.getProperty(NUM_COPIES).evaluateAttributeExpressions(flowFile).asInteger(); i++) {
            FlowFile copy = session.clone(flowFile);
            copy = session.putAttribute(copy, COPY_INDEX_ATTRIBUTE, Integer.toString(i));
            session.transfer(copy, REL_SUCCESS);
        }

        flowFile = session.putAttribute(flowFile, COPY_INDEX_ATTRIBUTE, "0");
        session.transfer(flowFile, REL_SUCCESS);
    }

}
