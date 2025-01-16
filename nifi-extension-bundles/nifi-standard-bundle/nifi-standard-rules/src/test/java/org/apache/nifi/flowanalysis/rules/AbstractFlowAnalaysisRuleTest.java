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
package org.apache.nifi.flowanalysis.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.attribute.expression.language.StandardPropertyValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flowanalysis.AbstractFlowAnalysisRule;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleContext;
import org.apache.nifi.flowanalysis.GroupAnalysisResult;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
public abstract class AbstractFlowAnalaysisRuleTest<T extends AbstractFlowAnalysisRule> {

    private static final ObjectMapper FLOW_MAPPER = new ObjectMapper();
    protected Map<PropertyDescriptor, PropertyValue> properties = new HashMap<>();
    protected T rule;

    @Mock
    protected FlowAnalysisRuleContext flowAnalysisRuleContext;

    @Mock
    protected ConfigurationContext configurationContext;

    @Mock
    protected ValidationContext validationContext;

    protected abstract T initializeRule();

    @BeforeEach
    public void setup() {
        rule = initializeRule();
        Mockito.lenient().when(flowAnalysisRuleContext.getProperty(any())).thenAnswer(invocation -> {
            return properties.get(invocation.getArgument(0));
        });
        Mockito.lenient().when(configurationContext.getProperty(any())).thenAnswer(invocation -> {
            return properties.get(invocation.getArgument(0));
        });
        Mockito.lenient().when(validationContext.getProperty(any())).thenAnswer(invocation -> {
            return properties.get(invocation.getArgument(0));
        });
    }

    protected void setProperty(PropertyDescriptor propertyDescriptor, String value) {
        properties.put(propertyDescriptor, new StandardPropertyValue(value, null, null));
    }

    private VersionedProcessGroup getProcessGroup(String flowDefinition) throws Exception {
        final RegisteredFlowSnapshot flowSnapshot = FLOW_MAPPER.readValue(new FileInputStream(flowDefinition), RegisteredFlowSnapshot.class);
        return flowSnapshot.getFlowContents();
    }

    protected void testAnalyzeProcessGroup(String flowDefinition, List<String> expected) throws Exception {
        final Collection<GroupAnalysisResult> actual = rule.analyzeProcessGroup(getProcessGroup(flowDefinition), flowAnalysisRuleContext);
        assertIterableEquals(expected, actual.stream().map(r -> r.getComponent().get().getInstanceIdentifier()).sorted().toList());
    }
}
