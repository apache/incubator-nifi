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

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LifecycleFailureService extends AbstractControllerService {
    static final PropertyDescriptor ENABLE_FAILURE_COUNT = new PropertyDescriptor.Builder()
        .name("Enable Failure Count")
        .description("How many times the CS should fail to enable before succeeding")
        .required(true)
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .defaultValue("0")
        .build();

    static final PropertyDescriptor FAIL_ON_DISABLE = new PropertyDescriptor.Builder()
        .name("Fail on Disable")
        .displayName("Fail on Disable")
        .description("Whether or not hte Controller Service should fail when disabled")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    private final AtomicInteger invocationCount = new AtomicInteger(0);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(
                ENABLE_FAILURE_COUNT,
                FAIL_ON_DISABLE
        );
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final int maxFailureCount = context.getProperty(ENABLE_FAILURE_COUNT).asInteger();
        final int currentInvocationCount = invocationCount.getAndIncrement();
        if (currentInvocationCount >= maxFailureCount) {
            getLogger().info("Enabling successfully because invocation count is {}", currentInvocationCount);
            return;
        }

        getLogger().info("Will fail to enable because invocation count is {}", currentInvocationCount);
        throw new RuntimeException("Failing to enable because configured to fail " + maxFailureCount + " times and current failure count is only " + currentInvocationCount);
    }

    @OnDisabled
    public void onDisabled(final ConfigurationContext context) {
        if (context.getProperty(FAIL_ON_DISABLE).asBoolean()) {
            getLogger().info("Throwing Exception in onDisabled as configured");
            throw new RuntimeException("Failing to disable because configured to fail on disable");
        }

        getLogger().info("Completing onDisabled successfully as configured");
    }
}
