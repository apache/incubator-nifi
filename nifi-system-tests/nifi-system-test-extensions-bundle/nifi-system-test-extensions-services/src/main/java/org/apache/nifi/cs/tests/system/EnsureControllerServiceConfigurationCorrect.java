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

package org.apache.nifi.cs.tests.system;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.POSITIVE_INTEGER_VALIDATOR;

public class EnsureControllerServiceConfigurationCorrect extends AbstractControllerService implements VerifiableControllerService {
    static final PropertyDescriptor SUCCESSFUL_VERIFICATION = new PropertyDescriptor.Builder()
        .name("Successful Verification")
        .displayName("Successful Verification")
        .description("Whether or not Verification should succeed")
        .required(true)
        .allowableValues("true", "false")
        .build();

    static final PropertyDescriptor VERIFICATION_STEPS = new PropertyDescriptor.Builder()
        .name("Verification Steps")
        .displayName("Verification Steps")
        .description("The number of steps to use in the Verification")
        .required(true)
        .addValidator(POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .defaultValue("1")
        .build();

    static final PropertyDescriptor EXCEPTION_ON_VERIFICATION = new PropertyDescriptor.Builder()
        .name("Exception on Verification")
        .displayName("Exception on Verification")
        .description("If true, attempting to perform verification will throw a RuntimeException")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    static final PropertyDescriptor FAILURE_NODE_NUMBER = new PropertyDescriptor.Builder()
        .name("Failure Node Number")
        .displayName("Failure Node Number")
        .description("The Node Number to Fail On")
        .required(false)
        .addValidator(POSITIVE_INTEGER_VALIDATOR)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(
            SUCCESSFUL_VERIFICATION,
            VERIFICATION_STEPS,
            EXCEPTION_ON_VERIFICATION,
            FAILURE_NODE_NUMBER
        );
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final boolean exception = context.getProperty(EXCEPTION_ON_VERIFICATION).asBoolean();
        if (exception) {
            throw new RuntimeException("Intentional Exception - Processor was configured to throw an Exception when performing config verification");
        }

        final List<ConfigVerificationResult> results = new ArrayList<>();

        final int iterations;
        try {
            iterations = context.getProperty(VERIFICATION_STEPS).evaluateAttributeExpressions(attributes).asInteger();
        } catch (final NumberFormatException nfe) {
            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Determine Number of Verification Steps")
                .outcome(Outcome.FAILED)
                .explanation("Invalid value for the number of Verification Steps")
                .build());

            return results;
        }

        final boolean success = context.getProperty(SUCCESSFUL_VERIFICATION).asBoolean();
        final Outcome outcome = success ? Outcome.SUCCESSFUL : Outcome.FAILED;

        for (int i = 0; i < iterations; i++) {
            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Verification Step #" + i)
                .outcome(outcome)
                .explanation("Verification Step #" + i)
                .build());
        }

        // Consider the 'Failure Node Number' Property. This makes it easy to get different results from different nodes for testing purposes
        final Integer failureNodeNum = context.getProperty(FAILURE_NODE_NUMBER).asInteger();
        if (failureNodeNum == null) {
            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Fail Based on Node Number")
                .outcome(Outcome.SKIPPED)
                .explanation("Not configured to Fail based on node number")
                .build());
        } else {
            final String currentNodeNumberString = System.getProperty("nodeNumber");
            final Integer currentNodeNumber = currentNodeNumberString == null ? null : Integer.parseInt(currentNodeNumberString);
            final boolean shouldFail = Objects.equals(failureNodeNum, currentNodeNumber);

            if (shouldFail) {
                results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Fail Based on Node Number")
                    .outcome(Outcome.FAILED)
                    .explanation("This node is Node Number " + currentNodeNumberString + " and configured to fail on this Node Number")
                    .build());
            } else {
                results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Fail Based on Node Number")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("This node is Node Number " + currentNodeNumberString + " and configured not to fail on this Node Number")
                    .build());
            }
        }

        return results;
    }
}
