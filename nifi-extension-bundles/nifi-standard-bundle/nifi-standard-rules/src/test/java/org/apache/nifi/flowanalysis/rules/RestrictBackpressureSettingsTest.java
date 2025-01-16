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

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RestrictBackpressureSettingsTest extends AbstractFlowAnalaysisRuleTest<RestrictBackpressureSettings> {

    @Override
    protected RestrictBackpressureSettings initializeRule() {
        return new RestrictBackpressureSettings();
    }

    @BeforeEach
    @Override
    public void setup() {
        super.setup();
        setProperty(RestrictBackpressureSettings.COUNT_MIN, RestrictBackpressureSettings.COUNT_MIN.getDefaultValue());
        setProperty(RestrictBackpressureSettings.COUNT_MAX, RestrictBackpressureSettings.COUNT_MAX.getDefaultValue());
        setProperty(RestrictBackpressureSettings.SIZE_MIN, RestrictBackpressureSettings.SIZE_MIN.getDefaultValue());
        setProperty(RestrictBackpressureSettings.SIZE_MAX, RestrictBackpressureSettings.SIZE_MAX.getDefaultValue());
    }

    @Test
    public void testWrongCountConfiguration() {
        setProperty(RestrictBackpressureSettings.COUNT_MIN, "100");
        setProperty(RestrictBackpressureSettings.COUNT_MAX, "10");
        assertFalse(rule.customValidate(validationContext).isEmpty());
    }

    @Test
    public void testWrongSizeConfiguration() {
        setProperty(RestrictBackpressureSettings.SIZE_MIN, "1GB");
        setProperty(RestrictBackpressureSettings.SIZE_MAX, "1MB");
        assertFalse(rule.customValidate(validationContext).isEmpty());
    }

    @Test
    public void testNoViolations() throws Exception {
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictBackpressureSettings/RestrictBackpressureSettings_noViolation.json",
                List.of()
            );
    }

    @Test
    public void testViolations() throws Exception {
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictBackpressureSettings/RestrictBackpressureSettings.json",
                List.of(
                        "e26f20c1-0192-1000-ff8b-bcf395c02076",  // processor GenerateFlowFile connecting to UpdateAttribute
                        "e26f3857-0192-1000-776a-3d62e15f75dc", // processor UpdateAttribute connecting to funnel
                        "e26f3857-0192-1000-776a-3d62e15f75dc", // processor UpdateAttribute connecting from funnel
                        "e26fd0d5-0192-1000-ee3d-f90141590475", // connection from funnel to funnel
                        "e27073f8-0192-1000-cf43-9c41e69eadd2", // connection from output port to funnel
                        "e270eaa4-0192-1000-0622-8f9af5319328" // connection from funnel to input port
                    )
            );
    }

}
