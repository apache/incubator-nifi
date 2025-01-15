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
package org.apache.nifi.pgp.service.standard;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.pgp.service.api.KeyIdentifierConverter;
import org.apache.nifi.pgp.service.api.PGPPrivateKeyService;
import org.apache.nifi.pgp.service.standard.exception.PGPConfigurationException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.StringUtils;

import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.KeyFingerPrintCalculator;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Standard Pretty Good Privacy Private Key Service reads Private Keys from configured Keyring files or properties
 */
@Tags({"PGP", "GPG", "OpenPGP", "Encryption", "Private", "Key", "RFC 4880"})
@CapabilityDescription("PGP Private Key Service provides Private Keys loaded from files or properties")
public class StandardPGPPrivateKeyService extends AbstractControllerService implements PGPPrivateKeyService {
    public static final PropertyDescriptor KEYRING_FILE = new PropertyDescriptor.Builder()
            .name("keyring-file")
            .displayName("Keyring File")
            .description("File path to PGP Keyring or Secret Key encoded in binary or ASCII Armor")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEYRING = new PropertyDescriptor.Builder()
            .name("keyring")
            .displayName("Keyring")
            .description("PGP Keyring or Secret Key encoded in ASCII Armor")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_PASSWORD = new PropertyDescriptor.Builder()
            .name("key-password")
            .displayName("Key Password")
            .description("Password used for decrypting Private Keys")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final Charset KEY_CHARSET = StandardCharsets.US_ASCII;

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            KEYRING_FILE,
            KEYRING,
            KEY_PASSWORD
    );

    private volatile Map<Long, PGPPrivateKey> privateKeys = Collections.emptyMap();

    /**
     * On Enabled reads Private Keys using configured properties
     *
     * @param context Configuration Context with properties
     * @throws InitializationException Thrown when unable to load keys
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            final PBESecretKeyDecryptor keyDecryptor = getKeyDecryptor(context);
            final List<PGPPrivateKey> extractedPrivateKeys = new ArrayList<>(readKeyringFile(keyDecryptor, context));
            extractedPrivateKeys.addAll(readKeyring(keyDecryptor, context));

            privateKeys = extractedPrivateKeys.stream().collect(
                    Collectors.toMap(
                            privateKey -> privateKey.getKeyID(),
                            privateKey -> privateKey
                    )
            );
        } catch (final RuntimeException e) {
            throw new InitializationException("Reading Private Keys Failed", e);
        }
    }

    /**
     * On Disabled clears Private Keys
     */
    @OnDisabled
    public void onDisabled() {
        privateKeys = Collections.emptyMap();
    }

    /**
     * Find Private Key matching Key Identifier
     *
     * @param keyIdentifier Private Key Identifier
     * @return Optional container for PGP Private Key empty when no matching Key found
     */
    @Override
    public Optional<PGPPrivateKey> findPrivateKey(final long keyIdentifier) {
        getLogger().debug("Find Private Key [{}]", KeyIdentifierConverter.format(keyIdentifier));
        return Optional.ofNullable(privateKeys.get(keyIdentifier));
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
     * Custom Validate reads keyring using provided password
     *
     * @param context Validation Context
     * @return Validation Results
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final PBESecretKeyDecryptor keyDecryptor = getKeyDecryptor(context);
        final List<PGPPrivateKey> extractedPrivateKeys = new ArrayList<>();

        try {
            extractedPrivateKeys.addAll(readKeyringFile(keyDecryptor, context));
        } catch (final RuntimeException e) {
            final ValidationResult result = new ValidationResult.Builder()
                    .valid(false)
                    .subject(KEYRING_FILE.getDisplayName())
                    .explanation(String.format("Reading Secret Keyring File Failed: %s", e.getMessage()))
                    .build();
            results.add(result);
        }

        try {
            extractedPrivateKeys.addAll(readKeyring(keyDecryptor, context));
        } catch (final RuntimeException e) {
            final ValidationResult result = new ValidationResult.Builder()
                    .valid(false)
                    .subject(KEYRING.getDisplayName())
                    .explanation(String.format("Reading Secret Keyring Failed: %s", e.getMessage()))
                    .build();
            results.add(result);
        }

        if (extractedPrivateKeys.isEmpty()) {
            final String explanation = String.format("No Private Keys Read from [%s] or [%s]", KEYRING_FILE.getDisplayName(), KEYRING.getDisplayName());
            final ValidationResult result = new ValidationResult.Builder()
                    .valid(false)
                    .subject(getClass().getSimpleName())
                    .explanation(explanation)
                    .build();
            results.add(result);
        }

        return results;
    }

    private List<PGPPrivateKey> readKeyringFile(final PBESecretKeyDecryptor keyDecryptor, final PropertyContext context) {
        final List<PGPPrivateKey> extractedPrivateKeys = new ArrayList<>();
        final String keyringFile = context.getProperty(KEYRING_FILE).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(keyringFile)) {
            try (final InputStream inputStream = new FileInputStream(keyringFile)) {
                extractedPrivateKeys.addAll(extractPrivateKeys(inputStream, keyDecryptor));
            } catch (final IOException | RuntimeException e) {
                final String message = String.format("Reading Secret Keyring File [%s] Failed", keyringFile);
                throw new PGPConfigurationException(message, e);
            }
        }
        return extractedPrivateKeys;
    }

    private List<PGPPrivateKey> readKeyring(final PBESecretKeyDecryptor keyDecryptor, final PropertyContext context) {
        final List<PGPPrivateKey> extractedPrivateKeys = new ArrayList<>();
        final String keyring = context.getProperty(KEYRING).getValue();
        if (StringUtils.isNotBlank(keyring)) {
            final byte[] keyringBytes = keyring.getBytes(KEY_CHARSET);
            try (final InputStream inputStream = new ByteArrayInputStream(keyringBytes)) {
                extractedPrivateKeys.addAll(extractPrivateKeys(inputStream, keyDecryptor));
            } catch (final IOException | RuntimeException e) {
                throw new PGPConfigurationException("Reading Secret Keyring Failed", e);
            }
        }
        return extractedPrivateKeys;
    }

    private List<PGPPrivateKey> extractPrivateKeys(final InputStream inputStream, final PBESecretKeyDecryptor keyDecryptor) {
        try (final InputStream decoderStream = PGPUtil.getDecoderStream(inputStream)) {
            final PGPSecretKeyRingCollection keyRings = readKeyRings(decoderStream);
            return extractPrivateKeys(keyRings, keyDecryptor);
        } catch (final IOException e) {
            throw new PGPConfigurationException("Reading Secret Keyring Stream Failed", e);
        }
    }

    private PGPSecretKeyRingCollection readKeyRings(final InputStream inputStream) throws IOException {
        final KeyFingerPrintCalculator calculator = new JcaKeyFingerprintCalculator();
        try {
            return new PGPSecretKeyRingCollection(inputStream, calculator);
        } catch (final PGPException e) {
            throw new PGPConfigurationException("Reading Secret Keyring Collection Failed", e);
        }
    }

    private List<PGPPrivateKey> extractPrivateKeys(final PGPSecretKeyRingCollection keyRings, final PBESecretKeyDecryptor keyDecryptor) {
        final List<PGPPrivateKey> extractedPrivateKeys = new ArrayList<>();

        for (final PGPSecretKeyRing keyRing : keyRings) {
            for (final PGPSecretKey secretKey : keyRing) {
                final long keyId = secretKey.getKeyID();
                final String keyIdentifier = KeyIdentifierConverter.format(keyId);
                try {
                    final PGPPrivateKey privateKey = secretKey.extractPrivateKey(keyDecryptor);
                    extractedPrivateKeys.add(privateKey);
                    getLogger().debug("Extracted Private Key [{}]", keyIdentifier);
                } catch (final PGPException e) {
                    final String message = String.format("Private Key [%s] Extraction Failed: check password", keyIdentifier);
                    throw new PGPConfigurationException(message, e);
                }
            }
        }

        return Collections.unmodifiableList(extractedPrivateKeys);
    }

    private PBESecretKeyDecryptor getKeyDecryptor(final PropertyContext context) {
        final String keyPassword = context.getProperty(KEY_PASSWORD).getValue();
        try {
            return new JcePBESecretKeyDecryptorBuilder().build(keyPassword.toCharArray());
        } catch (final PGPException e) {
            throw new PGPConfigurationException("Building Secret Key Decryptor using password failed", e);
        }
    }
}
