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
package org.apache.nifi.controller;

import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterGroupKey;
import org.apache.nifi.parameter.ParameterSensitivity;

public class ParameterProviderUsageReference {

    private final ParameterContext parameterContext;

    private final ParameterSensitivity sensitivity;

    public ParameterProviderUsageReference(final ParameterContext parameterContext, final ParameterSensitivity sensitivity) {
        this.parameterContext = parameterContext;
        this.sensitivity = sensitivity;
    }

    public ParameterContext getParameterContext() {
        return parameterContext;
    }

    public ParameterSensitivity getSensitivity() {
        return sensitivity;
    }

    /**
     * @param groupKey A ParameterGroupKey
     * @return True if this reference parameter context's name matches (ignoring ase) the group key name, as well as sensitivity
     */
    public boolean matchesParameterGroup(final ParameterGroupKey groupKey) {
        if (groupKey == null) {
            return false;
        }

        return groupKey.getSensitivity() == sensitivity
                && (groupKey.getGroupName() == null || groupKey.getGroupName().equalsIgnoreCase(parameterContext.getName()));
    }
}
