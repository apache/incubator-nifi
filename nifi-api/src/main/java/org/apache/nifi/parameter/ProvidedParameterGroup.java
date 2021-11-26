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
package org.apache.nifi.parameter;

import java.util.Collections;
import java.util.List;

/**
 * Encapsulates a named group of externally fetched parameters that can be provided to referencing Parameter Contexts.
 */
public class ProvidedParameterGroup extends AbstractParameterGroup<Parameter> {

    /**
     * Creates a named parameter group with a specific sensitivity.
     * @param groupName The parameter group name
     * @param sensitivity The parameter sensitivity
     * @param parameters A list of parameters
     */
    public ProvidedParameterGroup(final String groupName, final ParameterSensitivity sensitivity, final List<Parameter> parameters) {
        super(groupName, sensitivity, Collections.unmodifiableList(parameters));
    }

    /**
     * Creates an unnamed parameter group with a specific sensitivity.
     * @param sensitivity The parameter sensitivity
     * @param parameters A list of parameters
     */
    public ProvidedParameterGroup(final ParameterSensitivity sensitivity, final List<Parameter> parameters) {
        super(sensitivity, Collections.unmodifiableList(parameters));
    }
}
