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
package org.apache.nifi.lookup.configuration2;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.builder.ConfigurationBuilderEvent;
import org.apache.commons.configuration2.builder.ReloadingFileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.FileBasedBuilderParameters;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.reporting.InitializationException;

import java.io.File;
import java.lang.reflect.ParameterizedType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This abstract class defines a generic {@link org.apache.nifi.lookup.LookupService} backed by an
 * Apache Commons Configuration {@link FileBasedConfiguration}.
 *
 */
public abstract class CommonsConfigurationLookupService<T extends FileBasedConfiguration> extends AbstractControllerService implements StringLookupService {

    private static final String KEY = "key";

    private static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(Stream.of(KEY).collect(Collectors.toSet()));

    public static final PropertyDescriptor CONFIGURATION_FILE =
        new PropertyDescriptor.Builder()
            .name("configuration-file")
            .displayName("Configuration File")
            .description("A configuration file")
            .required(true)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .addValidator(new XXEValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONFIGURATION_FILE
    );

    private final Class<T> resultClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];


    private volatile ReloadingFileBasedConfigurationBuilder<T> builder;

    private Configuration getConfiguration() throws LookupFailureException {
        try {
            if (builder != null) {
                return builder.getConfiguration();
            }
        } catch (final ConfigurationException e) {
            throw new LookupFailureException("Failed to get configuration due to " + e.getMessage(), e);
        }
        return null;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String config = context.getProperty(CONFIGURATION_FILE).evaluateAttributeExpressions().getValue();
        final FileBasedBuilderParameters params = new Parameters().fileBased().setFile(new File(config));
        this.builder = new ReloadingFileBasedConfigurationBuilder<>(resultClass).configure(params);
        builder.addEventListener(ConfigurationBuilderEvent.CONFIGURATION_REQUEST,
                event -> {
                    if (builder.getReloadingController().checkForReloading(null)) {
                        getLogger().debug("Reloading {}", config);
                    }
                });

        try {
            // Try getting configuration to see if there is any issue, for example wrong file format.
            // Then throw InitializationException to keep this service in 'Enabling' state.
            builder.getConfiguration();
        } catch (ConfigurationException e) {
            throw new InitializationException(e);
        }
    }

    @Override
    public Optional<String> lookup(final Map<String, Object> coordinates) throws LookupFailureException {
        if (coordinates == null) {
            return Optional.empty();
        }

        final String key = coordinates.get(KEY).toString();
        if (StringUtils.isBlank(key)) {
            return Optional.empty();
        }

        final Configuration config = getConfiguration();
        if (config != null) {
            final Object value = config.getProperty(key);
            if (value != null) {
                return Optional.of(String.valueOf(value));
            }
        }

        return Optional.empty();
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

}
