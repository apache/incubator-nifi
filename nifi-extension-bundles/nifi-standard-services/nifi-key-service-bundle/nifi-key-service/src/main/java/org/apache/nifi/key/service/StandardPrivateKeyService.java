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
package org.apache.nifi.key.service;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.key.service.reader.BouncyCastlePrivateKeyReader;
import org.apache.nifi.key.service.reader.PrivateKeyReader;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Standard implementation of Private Key Service supporting encrypted or unencrypted sources
 */
@Tags({"PEM", "PKCS8"})
@CapabilityDescription("Private Key Service provides access to a Private Key loaded from configured sources")
public class StandardPrivateKeyService extends AbstractControllerService implements PrivateKeyService {
    public static final PropertyDescriptor KEY_FILE = new PropertyDescriptor.Builder()
            .name("key-file")
            .displayName("Key File")
            .description("File path to Private Key structured using PKCS8 and encoded as PEM")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .build();

    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("key")
            .displayName("Key")
            .description("Private Key structured using PKCS8 and encoded as PEM")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_PASSWORD = new PropertyDescriptor.Builder()
            .name("key-password")
            .displayName("Key Password")
            .description("Password used for decrypting Private Keys")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            KEY_FILE,
            KEY,
            KEY_PASSWORD
    );

    private static final Charset KEY_CHARACTER_SET = StandardCharsets.US_ASCII;

    private static final PrivateKeyReader PRIVATE_KEY_READER = new BouncyCastlePrivateKeyReader();

    private final AtomicReference<PrivateKey> keyReference = new AtomicReference<>();

    @Override
    public PrivateKey getPrivateKey() {
        return keyReference.get();
    }

    /**
     * On Property Modified clears the current Private Key reference to require a new read for validation
     *
     * @param propertyDescriptor the descriptor for the property being modified
     * @param oldValue the value that was previously set, or null if no value
     *            was previously set for this property
     * @param newValue the new property value or if null indicates the property
     *            was removed
     */
    @Override
    public void onPropertyModified(final PropertyDescriptor propertyDescriptor, final String oldValue, final String newValue) {
        keyReference.set(null);
    }

    /**
     * On Enabled reads Private Keys using configured properties
     *
     * @param context Configuration Context with properties
     * @throws InitializationException Thrown when unable to load
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            final PrivateKey readKey = readKey(context);
            keyReference.set(readKey);
        } catch (final RuntimeException e) {
            throw new InitializationException("Reading Private Key Failed", e);
        }
    }

    /**
     * On Disabled clears Private Keys
     */
    @OnDisabled
    public void onDisabled() {
        keyReference.set(null);
    }

    /**
     * Get Supported Property Descriptors
     *
     * @return Supported Property Descriptors
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * Custom Validate reads key using configured password for encrypted keys
     *
     * @param context Validation Context
     * @return Validation Results
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final PropertyValue keyFileProperty = context.getProperty(KEY_FILE);
        final PropertyValue keyProperty = context.getProperty(KEY);
        if (keyFileProperty.isSet() && keyProperty.isSet()) {
            final String explanation = String.format("Both [%s] and [%s] properties configured", KEY_FILE.getDisplayName(), KEY.getDisplayName());
            final ValidationResult result = new ValidationResult.Builder()
                    .valid(false)
                    .subject(KEY.getDisplayName())
                    .explanation(explanation)
                    .build();
            results.add(result);
        } else if (keyReference.get() == null) {
            try {
                final PrivateKey readKey = readKey(context);
                keyReference.set(readKey);
            } catch (final RuntimeException e) {
                final ValidationResult result = new ValidationResult.Builder()
                        .valid(false)
                        .subject(KEY.getDisplayName())
                        .explanation(e.getMessage())
                        .build();
                results.add(result);
            }
        }

        return results;
    }

    private PrivateKey readKey(final PropertyContext context) {
        final PrivateKey readKey;

        final char[] keyPassword = getKeyPassword(context);

        final PropertyValue keyFileProperty = context.getProperty(KEY_FILE);
        final PropertyValue keyProperty = context.getProperty(KEY);

        if (keyFileProperty.isSet()) {
            try (final InputStream inputStream = keyFileProperty.asResource().read()) {
                readKey = PRIVATE_KEY_READER.readPrivateKey(inputStream, keyPassword);
            } catch (final IOException e) {
                throw new UncheckedIOException("Read Private Key File failed", e);
            }
        } else if (keyProperty.isSet()) {
            final byte[] key = keyProperty.getValue().getBytes(KEY_CHARACTER_SET);
            try (final InputStream inputStream = new ByteArrayInputStream(key)) {
                readKey = PRIVATE_KEY_READER.readPrivateKey(inputStream, keyPassword);
            } catch (final IOException e) {
                throw new UncheckedIOException("Read Private Key failed", e);
            }
        } else {
            throw new IllegalStateException("Private Key not configured");
        }

        return readKey;
    }

    private char[] getKeyPassword(final PropertyContext context) {
        final PropertyValue keyPasswordProperty = context.getProperty(KEY_PASSWORD);
        return keyPasswordProperty.isSet() ? keyPasswordProperty.getValue().toCharArray() : new char[]{};
    }
}
