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

import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.util.List;

/**
 * Defines a provider that is responsible for fetching from an external source Parameters with
 * which a ParameterContext can be populated.
 *
 * <p>
 * <code>ParameterProvider</code>s are discovered using Java's
 * <code>ServiceLoader</code> mechanism. As a result, all implementations must
 * follow these rules:
 *
 * <ul>
 * <li>The implementation must implement this interface.</li>
 * <li>The implementation must have a file named
 * org.apache.nifi.parameter.ParameterProvider located within the jar's
 * <code>META-INF/services</code> directory. This file contains a list of
 * fully-qualified class names of all <code>ParameterProvider</code>s in the jar,
 * one-per-line.
 * <li>The implementation must support a default constructor.</li>
 * </ul>
 * </p>
 *
 * <p>
 * All implementations of this interface must be thread-safe.
 * </p>
 *
 * <p>
 * Parameter Providers may choose to annotate a method with the
 * {@link OnConfigurationRestored @OnConfigurationRestored} annotation. If this is done, that method
 * will be invoked after all properties have been set for the ParameterProvider and
 * before its parameters are fetched.
 * </p>
 */
public interface ParameterProvider extends ConfigurableComponent {

    /**
     * Provides the Parameter Provider with access to objects that may be of use
     * throughout the life of the service
     *
     * @param config of initialization context
     * @throws org.apache.nifi.reporting.InitializationException if unable to init
     */
    void initialize(ParameterProviderInitializationContext config) throws InitializationException;

    /**
     * Fetches parameters from an external source.
     * @param context The <code>ConfigurationContext</code>for the provider
     * @return A list of fetched Parameters
     * @throws IOException if there is an I/O problem while fetching the Parameters
     */
    List<Parameter> fetchParameters(ConfigurationContext context) throws IOException;
}
