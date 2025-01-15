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

import com.exceptionfactory.jagged.EncryptingChannelFactory;
import com.exceptionfactory.jagged.RecipientStanzaWriter;
import com.exceptionfactory.jagged.framework.armor.ArmoredEncryptingChannelFactory;
import com.exceptionfactory.jagged.framework.stream.StandardEncryptingChannelFactory;
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
import org.apache.nifi.processors.cipher.age.AgeProviderResolver;
import org.apache.nifi.processors.cipher.age.AgePublicKeyReader;
import org.apache.nifi.processors.cipher.age.FileEncoding;
import org.apache.nifi.processors.cipher.age.KeySource;
import org.apache.nifi.processors.cipher.io.ChannelStreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.WritableByteChannel;
import java.security.GeneralSecurityException;
import java.security.Provider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"age", "age-encryption.org", "encryption", "ChaCha20-Poly1305", "X25519"})
@CapabilityDescription(
        "Encrypt content using the age-encryption.org/v1 specification. " +
        "Supports binary or ASCII armored content encoding using configurable properties. " +
        "The age standard uses ChaCha20-Poly1305 for authenticated encryption of the payload. " +
        "The age-keygen command supports generating X25519 key pairs for encryption and decryption operations."
)
@SeeAlso({ DecryptContentAge.class })
public class EncryptContentAge extends AbstractProcessor implements VerifiableProcessor {

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Encryption Completed")
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Encryption Failed")
            .build();

    static final PropertyDescriptor FILE_ENCODING = new PropertyDescriptor.Builder()
            .name("File Encoding")
            .displayName("File Encoding")
            .description("Output encoding for encrypted files. Binary encoding provides optimal processing performance.")
            .required(true)
            .defaultValue(FileEncoding.BINARY)
            .allowableValues(FileEncoding.class)
            .build();

    static final PropertyDescriptor PUBLIC_KEY_SOURCE = new PropertyDescriptor.Builder()
            .name("Public Key Source")
            .displayName("Public Key Source")
            .description("Source of information determines the loading strategy for X25519 Public Key Recipients")
            .required(true)
            .defaultValue(KeySource.PROPERTIES)
            .allowableValues(KeySource.class)
            .build();

    static final PropertyDescriptor PUBLIC_KEY_RECIPIENTS = new PropertyDescriptor.Builder()
            .name("Public Key Recipients")
            .displayName("Public Key Recipients")
            .description("One or more X25519 Public Key Recipients, separated with newlines, encoded according to the age specification, starting with age1")
            .required(true)
            .sensitive(true)
            .addValidator(new AgeKeyValidator(AgeKeyIndicator.PUBLIC_KEY))
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.TEXT)
            .dependsOn(PUBLIC_KEY_SOURCE, KeySource.PROPERTIES)
            .build();

    static final PropertyDescriptor PUBLIC_KEY_RECIPIENT_RESOURCES = new PropertyDescriptor.Builder()
            .name("Public Key Recipient Resources")
            .displayName("Public Key Recipient Resources")
            .description("One or more files or URLs containing X25519 Public Key Recipients, separated with newlines, encoded according to the age specification, starting with age1")
            .required(true)
            .addValidator(new AgeKeyValidator(AgeKeyIndicator.PUBLIC_KEY))
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.URL)
            .dependsOn(PUBLIC_KEY_SOURCE, KeySource.RESOURCES)
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS,
            FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FILE_ENCODING,
            PUBLIC_KEY_SOURCE,
            PUBLIC_KEY_RECIPIENTS,
            PUBLIC_KEY_RECIPIENT_RESOURCES
    );

    private static final Provider CIPHER_PROVIDER = AgeProviderResolver.getCipherProvider();

    private static final AgeKeyReader<RecipientStanzaWriter> PUBLIC_KEY_READER = new AgePublicKeyReader();

    /** 64 Kilobyte buffer aligns with age-encryption payload chunk sizing */
    private static final int BUFFER_CAPACITY = 65535;

    private static final String KEY_VERIFICATION_STEP = "Verify Public Key Recipients";

    private static final String NOT_FOUND_EXPLANATION = "Public Key Recipients not found";

    private volatile List<RecipientStanzaWriter> configuredRecipientStanzaWriters = Collections.emptyList();

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
     * Verify Public Key Identities
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
            final List<RecipientStanzaWriter> recipientStanzaWriters = getRecipientStanzaWriters(context);

            if (recipientStanzaWriters.isEmpty()) {
                verificationLogger.warn(NOT_FOUND_EXPLANATION);
                verificationBuilder.outcome(ConfigVerificationResult.Outcome.FAILED).explanation(NOT_FOUND_EXPLANATION);
            } else {
                final String explanation = String.format("Public Key Recipients found: %d", recipientStanzaWriters.size());
                verificationLogger.info(explanation);
                verificationBuilder.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL).explanation(explanation);
            }
        } catch (final Exception e) {
            final String explanation = String.format("Public Key Recipients not found: %s", e.getMessage());
            verificationLogger.warn("Public Key Recipients not found", e);

            verificationBuilder.outcome(ConfigVerificationResult.Outcome.FAILED).explanation(explanation);
        }

        results.add(verificationBuilder.build());
        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        configuredRecipientStanzaWriters = getRecipientStanzaWriters(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            final FileEncoding fileEncoding = context.getProperty(FILE_ENCODING).asAllowableValue(FileEncoding.class);
            final EncryptingChannelFactory encryptingChannelFactory = getEncryptingChannelFactory(fileEncoding);
            final StreamCallback streamCallback = new EncryptingStreamCallback(configuredRecipientStanzaWriters, encryptingChannelFactory);
            flowFile = session.write(flowFile, streamCallback);

            session.transfer(flowFile, SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Encryption Failed {}", flowFile, e);
            session.transfer(flowFile, FAILURE);
        }
    }

    private EncryptingChannelFactory getEncryptingChannelFactory(final FileEncoding fileEncoding) {
        return switch (fileEncoding) {
            case ASCII -> new ArmoredEncryptingChannelFactory(CIPHER_PROVIDER);
            case BINARY -> new StandardEncryptingChannelFactory(CIPHER_PROVIDER);
        };
    }

    private List<RecipientStanzaWriter> getRecipientStanzaWriters(final PropertyContext context) throws IOException {
        final KeySource keySource = context.getProperty(PUBLIC_KEY_SOURCE).asAllowableValue(KeySource.class);
        final List<ResourceReference> resources = switch (keySource) {
            case PROPERTIES -> List.of(context.getProperty(PUBLIC_KEY_RECIPIENTS).asResource());
            case RESOURCES -> context.getProperty(PUBLIC_KEY_RECIPIENT_RESOURCES).asResources().asList();
        };

        final List<RecipientStanzaWriter> recipientStanzaWriters = new ArrayList<>();
        for (final ResourceReference resource : resources) {
            try (final InputStream inputStream = resource.read()) {
                final List<RecipientStanzaWriter> writers = PUBLIC_KEY_READER.read(inputStream);
                recipientStanzaWriters.addAll(writers);
            }
        }

        if (recipientStanzaWriters.isEmpty()) {
            throw new IOException(NOT_FOUND_EXPLANATION);
        }

        return recipientStanzaWriters;
    }

    private static class EncryptingStreamCallback extends ChannelStreamCallback {
        private final List<RecipientStanzaWriter> recipientStanzaWriters;

        private final EncryptingChannelFactory encryptingChannelFactory;

        private EncryptingStreamCallback(
                final List<RecipientStanzaWriter> recipientStanzaWriters,
                final EncryptingChannelFactory encryptingChannelFactory
        ) {
            super(BUFFER_CAPACITY);
            this.recipientStanzaWriters = recipientStanzaWriters;
            this.encryptingChannelFactory = encryptingChannelFactory;
        }

        @Override
        protected WritableByteChannel getWritableChannel(final OutputStream outputStream) throws IOException {
            try {
                final WritableByteChannel outputChannel = super.getWritableChannel(outputStream);
                return encryptingChannelFactory.newEncryptingChannel(outputChannel, recipientStanzaWriters);
            } catch (final GeneralSecurityException e) {
                throw new IOException("Channel initialization failed", e);
            }
        }
    }
}
