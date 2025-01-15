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
package org.apache.nifi.processors.cipher;

import com.exceptionfactory.jagged.DecryptingChannelFactory;
import com.exceptionfactory.jagged.RecipientStanzaReader;
import com.exceptionfactory.jagged.framework.armor.ArmoredDecryptingChannelFactory;
import com.exceptionfactory.jagged.framework.stream.StandardDecryptingChannelFactory;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.cipher.age.AgeKeyIndicator;
import org.apache.nifi.processors.cipher.age.AgeKeyReader;
import org.apache.nifi.processors.cipher.age.AgeKeyValidator;
import org.apache.nifi.processors.cipher.age.AgePrivateKeyReader;
import org.apache.nifi.processors.cipher.age.AgeProviderResolver;
import org.apache.nifi.processors.cipher.age.KeySource;
import org.apache.nifi.processors.cipher.io.ChannelStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"age", "age-encryption.org", "encryption", "ChaCha20-Poly1305", "X25519"})
@CapabilityDescription(
        "Decrypt content using the age-encryption.org/v1 specification. " +
        "Detects binary or ASCII armored content encoding using the initial file header bytes. " +
        "The age standard uses ChaCha20-Poly1305 for authenticated encryption of the payload. " +
        "The age-keygen command supports generating X25519 key pairs for encryption and decryption operations."
)
@SeeAlso({ EncryptContentAge.class })
public class DecryptContentAge extends AbstractProcessor implements VerifiableProcessor {

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Decryption Completed")
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Decryption Failed")
            .build();

    static final PropertyDescriptor PRIVATE_KEY_SOURCE = new PropertyDescriptor.Builder()
            .name("Private Key Source")
            .displayName("Private Key Source")
            .description("Source of information determines the loading strategy for X25519 Private Key Identities")
            .required(true)
            .defaultValue(KeySource.PROPERTIES)
            .allowableValues(KeySource.class)
            .build();

    static final PropertyDescriptor PRIVATE_KEY_IDENTITIES = new PropertyDescriptor.Builder()
            .name("Private Key Identities")
            .displayName("Private Key Identities")
            .description("One or more X25519 Private Key Identities, separated with newlines, encoded according to the age specification, starting with AGE-SECRET-KEY-1")
            .required(true)
            .sensitive(true)
            .addValidator(new AgeKeyValidator(AgeKeyIndicator.PRIVATE_KEY))
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.TEXT)
            .dependsOn(PRIVATE_KEY_SOURCE, KeySource.PROPERTIES)
            .build();

    static final PropertyDescriptor PRIVATE_KEY_IDENTITY_RESOURCES = new PropertyDescriptor.Builder()
            .name("Private Key Identity Resources")
            .displayName("Private Key Identity Resources")
            .description("One or more files or URLs containing X25519 Private Key Identities, separated with newlines, encoded according to the age specification, starting with AGE-SECRET-KEY-1")
            .required(true)
            .addValidator(new AgeKeyValidator(AgeKeyIndicator.PRIVATE_KEY))
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.URL)
            .dependsOn(PRIVATE_KEY_SOURCE, KeySource.RESOURCES)
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS,
            FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PRIVATE_KEY_SOURCE,
            PRIVATE_KEY_IDENTITIES,
            PRIVATE_KEY_IDENTITY_RESOURCES
    );

    private static final AgeKeyReader<RecipientStanzaReader> PRIVATE_KEY_READER = new AgePrivateKeyReader();

    private static final Provider CIPHER_PROVIDER = AgeProviderResolver.getCipherProvider();

    /** 64 Kilobyte buffer plus 16 bytes for authentication tag aligns with age-encryption payload chunk sizing */
    private static final int BUFFER_CAPACITY = 65552;

    /** age-encryption.org version indicator at the beginning of binary encoded files */
    private static final String VERSION_INDICATOR = "age-encryption.org";

    private static final byte[] BINARY_VERSION_INDICATOR = VERSION_INDICATOR.getBytes(StandardCharsets.US_ASCII);

    private static final int INPUT_BUFFER_SIZE = BINARY_VERSION_INDICATOR.length;

    private static final String KEY_VERIFICATION_STEP = "Verify Private Key Identities";

    private static final String NOT_FOUND_EXPLANATION = "Private Key Identities not found";

    private volatile List<RecipientStanzaReader> configuredRecipientStanzaReaders = Collections.emptyList();

    /**
     * Get Relationships
     *
     * @return Processor Relationships
     */
    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * Get Supported Property Descriptors
     *
     * @return Processor Supported Property Descriptors
     */
    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * Verify Private Key Identities
     *
     * @param context Process Context with configured properties
     * @param verificationLogger Logger for writing verification results
     * @param attributes Sample FlowFile attributes for property value resolution
     * @return Configuration Verification Results
     */
    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        final ConfigVerificationResult.Builder verificationBuilder = new ConfigVerificationResult.Builder()
                .verificationStepName(KEY_VERIFICATION_STEP);

        try {
            final List<RecipientStanzaReader> recipientStanzaReaders = getRecipientStanzaReaders(context);

            if (recipientStanzaReaders.isEmpty()) {
                verificationLogger.warn(NOT_FOUND_EXPLANATION);
                verificationBuilder.outcome(ConfigVerificationResult.Outcome.FAILED).explanation(NOT_FOUND_EXPLANATION);
            } else {
                final String explanation = String.format("Private Key Identities found: %d", recipientStanzaReaders.size());
                verificationLogger.info(explanation);
                verificationBuilder.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL).explanation(explanation);
            }
        } catch (final Exception e) {
            final String explanation = String.format("%s: %s", NOT_FOUND_EXPLANATION, e);
            verificationLogger.warn(NOT_FOUND_EXPLANATION, e);

            verificationBuilder.outcome(ConfigVerificationResult.Outcome.FAILED).explanation(explanation);
        }

        results.add(verificationBuilder.build());
        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        configuredRecipientStanzaReaders = getRecipientStanzaReaders(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            final StreamCallback streamCallback = new DecryptingStreamCallback(configuredRecipientStanzaReaders);
            flowFile = session.write(flowFile, streamCallback);

            session.transfer(flowFile, SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Decryption Failed {}", flowFile, e);
            session.transfer(flowFile, FAILURE);
        }
    }

    private List<RecipientStanzaReader> getRecipientStanzaReaders(final PropertyContext context) throws IOException {
        final KeySource keySource = context.getProperty(PRIVATE_KEY_SOURCE).asAllowableValue(KeySource.class);
        final List<ResourceReference> resources = switch (keySource) {
            case PROPERTIES -> List.of(context.getProperty(PRIVATE_KEY_IDENTITIES).asResource());
            case RESOURCES -> context.getProperty(PRIVATE_KEY_IDENTITY_RESOURCES).asResources().asList();
        };

        final List<RecipientStanzaReader> recipientStanzaReaders = new ArrayList<>();
        for (final ResourceReference resource : resources) {
            try (final InputStream inputStream = resource.read()) {
                final List<RecipientStanzaReader> readers = PRIVATE_KEY_READER.read(inputStream);
                recipientStanzaReaders.addAll(readers);
            }
        }

        if (recipientStanzaReaders.isEmpty()) {
            throw new IOException(NOT_FOUND_EXPLANATION);
        }

        return recipientStanzaReaders;
    }

    private static class DecryptingStreamCallback extends ChannelStreamCallback {
        private final List<RecipientStanzaReader> recipientStanzaReaders;

        private DecryptingStreamCallback(final List<RecipientStanzaReader> recipientStanzaReaders) {
            super(BUFFER_CAPACITY);
            this.recipientStanzaReaders = recipientStanzaReaders;
        }

        @Override
        protected ReadableByteChannel getReadableChannel(final InputStream inputStream) throws IOException {
            final PushbackInputStream pushbackInputStream = new PushbackInputStream(inputStream, INPUT_BUFFER_SIZE);
            final byte[] versionIndicator = getVersionIndicator(pushbackInputStream);

            final DecryptingChannelFactory decryptingChannelFactory = getDecryptingChannelFactory(versionIndicator);
            try {
                final ReadableByteChannel inputChannel = super.getReadableChannel(pushbackInputStream);
                return decryptingChannelFactory.newDecryptingChannel(inputChannel, recipientStanzaReaders);
            } catch (final GeneralSecurityException e) {
                throw new IOException("Channel initialization failed", e);
            }
        }

        private byte[] getVersionIndicator(final PushbackInputStream pushbackInputStream) throws IOException {
            final byte[] versionIndicator = new byte[INPUT_BUFFER_SIZE];
            StreamUtils.fillBuffer(pushbackInputStream, versionIndicator);
            pushbackInputStream.unread(versionIndicator);
            return versionIndicator;
        }

        private DecryptingChannelFactory getDecryptingChannelFactory(final byte[] versionIndicator) {
            if (Arrays.equals(BINARY_VERSION_INDICATOR, versionIndicator)) {
                return new StandardDecryptingChannelFactory(CIPHER_PROVIDER);
            } else {
                return new ArmoredDecryptingChannelFactory(CIPHER_PROVIDER);
            }
        }
    }
}
