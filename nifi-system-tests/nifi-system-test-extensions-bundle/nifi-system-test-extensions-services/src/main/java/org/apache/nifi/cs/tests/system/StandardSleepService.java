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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StandardSleepService extends AbstractControllerService implements SleepService {
    public static final PropertyDescriptor VALIDATE_SLEEP_TIME = new PropertyDescriptor.Builder()
        .name("Validate Sleep Time")
        .description("The amount of time to sleep during validation")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("0 sec")
        .build();
    public static final PropertyDescriptor TRIGGER_SLEEP_TIME = new PropertyDescriptor.Builder()
        .name("Trigger Sleep Time")
        .description("The amount of time to sleep during each trigger")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("0 sec")
        .build();
    public static final PropertyDescriptor ON_ENABLED_SLEEP_TIME = new PropertyDescriptor.Builder()
        .name("@OnEnabled Sleep Time")
        .description("The amount of time to sleep when enabled")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("0 sec")
        .build();
    public static final PropertyDescriptor ON_DISABLED_SLEEP_TIME = new PropertyDescriptor.Builder()
        .name("@OnDisabled Sleep Time")
        .description("The amount of time to sleep when disabled")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("0 sec")
        .build();
    public static final PropertyDescriptor DEPENDENT_SERVICE = new PropertyDescriptor.Builder()
        .name("Dependent Service")
        .description("Another Controller Service that this one depends on. This is helpful for testing when Service A depends on Service B how enabling/disabling/etc. work")
        .required(false)
        .identifiesControllerService(SleepService.class)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(
            VALIDATE_SLEEP_TIME,
            ON_ENABLED_SLEEP_TIME,
            TRIGGER_SLEEP_TIME,
            ON_DISABLED_SLEEP_TIME,
            DEPENDENT_SERVICE
        );
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final long sleepMillis = validationContext.getProperty(VALIDATE_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        sleep(sleepMillis);

        return Collections.emptyList();
    }

    private void sleep(final long millis) {
        if (millis > 0L) {
            try {
                Thread.sleep(millis);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        sleep(context.getProperty(ON_ENABLED_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS));
    }

    @OnDisabled
    public void onDisabled(final ConfigurationContext context) {
        sleep(context.getProperty(ON_DISABLED_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS));
    }

    @Override
    public void sleep() {
        sleep(getConfigurationContext().getProperty(TRIGGER_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS));

        final SleepService dependentService = getConfigurationContext().getProperty(DEPENDENT_SERVICE).asControllerService(SleepService.class);
        if (dependentService != null) {
            dependentService.sleep();
        }
    }
}
