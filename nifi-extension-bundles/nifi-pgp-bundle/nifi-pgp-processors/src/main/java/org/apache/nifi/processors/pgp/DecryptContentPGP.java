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
package org.apache.nifi.processors.pgp;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.pgp.service.api.PGPPrivateKeyService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.pgp.attributes.DecryptionStrategy;
import org.apache.nifi.processors.pgp.exception.PGPDecryptionException;
import org.apache.nifi.processors.pgp.exception.PGPProcessException;
import org.apache.nifi.pgp.service.api.KeyIdentifierConverter;
import org.apache.nifi.stream.io.StreamUtils;

import org.apache.nifi.util.StringUtils;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPPBEEncryptedData;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.PBEDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.PublicKeyDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.bc.BcPBEDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.bc.BcPGPDigestCalculatorProvider;
import org.bouncycastle.openpgp.operator.bc.BcPublicKeyDataDecryptorFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Decrypt Content using Open Pretty Good Privacy decryption methods
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"PGP", "GPG", "OpenPGP", "Encryption", "RFC 4880"})
@CapabilityDescription("Decrypt contents of OpenPGP messages. Using the Packaged Decryption Strategy preserves OpenPGP encoding to support subsequent signature verification.")
@SeeAlso({EncryptContentPGP.class, SignContentPGP.class, VerifyContentPGP.class})
@WritesAttributes({
        @WritesAttribute(attribute = PGPAttributeKey.LITERAL_DATA_FILENAME, description = "Filename from decrypted Literal Data"),
        @WritesAttribute(attribute = PGPAttributeKey.LITERAL_DATA_MODIFIED, description = "Modified Date from decrypted Literal Data"),
        @WritesAttribute(attribute = PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_BLOCK_CIPHER, description = "Symmetric-Key Algorithm Block Cipher"),
        @WritesAttribute(attribute = PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_ID, description = "Symmetric-Key Algorithm Identifier")
})
public class DecryptContentPGP extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Decryption Succeeded")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Decryption Failed")
            .build();

    public static final PropertyDescriptor DECRYPTION_STRATEGY = new PropertyDescriptor.Builder()
            .name("decryption-strategy")
            .displayName("Decryption Strategy")
            .description("Strategy for writing files to success after decryption")
            .required(true)
            .defaultValue(DecryptionStrategy.DECRYPTED.name())
            .allowableValues(
                    Arrays.stream(DecryptionStrategy.values()).map(strategy ->
                            new AllowableValue(strategy.name(), strategy.name(), strategy.getDescription())
                    ).toArray(AllowableValue[]::new)
            )
            .build();

    public static final PropertyDescriptor PASSPHRASE = new PropertyDescriptor.Builder()
            .name("passphrase")
            .displayName("Passphrase")
            .description("Passphrase used for decrypting data encrypted with Password-Based Encryption")
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRIVATE_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("private-key-service")
            .displayName("Private Key Service")
            .description("PGP Private Key Service for decrypting data encrypted with Public Key Encryption")
            .identifiesControllerService(PGPPrivateKeyService.class)
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS,
            FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            DECRYPTION_STRATEGY,
            PASSPHRASE,
            PRIVATE_KEY_SERVICE
    );

    private static final String PASSWORD_BASED_ENCRYPTION = "Password-Based Encryption";

    private static final String PUBLIC_KEY_ENCRYPTION = "Public Key Encryption";

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
     * On Trigger decrypts Flow File contents using configured properties
     *
     * @param context Process Context
     * @param session Process Session
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final char[] passphrase = getPassphrase(context);
        final PGPPrivateKeyService privateKeyService = getPrivateKeyService(context);
        final DecryptionStrategy decryptionStrategy = getDecryptionStrategy(context);
        final DecryptStreamCallback callback = new DecryptStreamCallback(passphrase, privateKeyService, decryptionStrategy);

        try {
            flowFile = session.write(flowFile, callback);
            flowFile = session.putAllAttributes(flowFile, callback.attributes);
            session.transfer(flowFile, SUCCESS);
        } catch (final RuntimeException e) {
            getLogger().error("Decryption Failed {}", flowFile, e);
            session.transfer(flowFile, FAILURE);
        }
    }

    /**
     * Custom Validate requires at least one decryption property to be configured
     *
     * @param context Validation Context
     * @return Collection of Validation Results
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String passphrase = context.getProperty(PASSPHRASE).getValue();
        if (StringUtils.isBlank(passphrase)) {
            final PGPPrivateKeyService privateKeyService = context.getProperty(PRIVATE_KEY_SERVICE).asControllerService(PGPPrivateKeyService.class);
            if (privateKeyService == null) {
                final String explanation = String.format("Neither [%s] nor [%s] configured", PASSPHRASE.getDisplayName(), PRIVATE_KEY_SERVICE.getDisplayName());
                final ValidationResult result = new ValidationResult.Builder()
                        .valid(false)
                        .subject(getClass().getSimpleName())
                        .explanation(explanation)
                        .build();
                results.add(result);
            }
        }

        return results;
    }

    private char[] getPassphrase(final ProcessContext context) {
        char[] passphrase = null;
        final PropertyValue passphraseProperty = context.getProperty(PASSPHRASE);
        if (passphraseProperty.isSet()) {
            passphrase = passphraseProperty.getValue().toCharArray();
        }
        return passphrase;
    }

    private PGPPrivateKeyService getPrivateKeyService(final ProcessContext context) {
        PGPPrivateKeyService privateKeyService = null;
        final PropertyValue privateKeyServiceProperty = context.getProperty(PRIVATE_KEY_SERVICE);
        if (privateKeyServiceProperty.isSet()) {
            privateKeyService = privateKeyServiceProperty.asControllerService(PGPPrivateKeyService.class);
        }
        return privateKeyService;
    }

    private DecryptionStrategy getDecryptionStrategy(final ProcessContext context) {
        final String strategy = context.getProperty(DECRYPTION_STRATEGY).getValue();
        return DecryptionStrategy.valueOf(strategy);
    }

    private class DecryptStreamCallback implements StreamCallback {
        private final char[] passphrase;

        private final PGPPrivateKeyService privateKeyService;

        private final DecryptionStrategy decryptionStrategy;

        private final Map<String, String> attributes = new HashMap<>();

        public DecryptStreamCallback(final char[] passphrase,
                                     final PGPPrivateKeyService privateKeyService,
                                     final DecryptionStrategy decryptionStrategy) {
            this.passphrase = passphrase;
            this.privateKeyService = privateKeyService;
            this.decryptionStrategy = decryptionStrategy;
        }

        /**
         * Process Input Stream containing encrypted data and write decrypted contents to Output Stream
         *
         * @param inputStream  Input Stream containing encrypted data
         * @param outputStream Output Stream for decrypted contents
         * @throws IOException Thrown when unable to read or write streams
         */
        @Override
        public void process(final InputStream inputStream, final OutputStream outputStream) throws IOException {
            final PGPEncryptedDataList encryptedDataList = getEncryptedDataList(inputStream);
            final PGPEncryptedData encryptedData = findSupportedEncryptedData(encryptedDataList);

            if (DecryptionStrategy.PACKAGED == decryptionStrategy) {
                try {
                    final InputStream decryptedDataStream = getDecryptedDataStream(encryptedData);
                    StreamUtils.copy(decryptedDataStream, outputStream);
                } catch (final PGPException e) {
                    final String message = String.format("PGP Decryption Failed [%s]", getEncryptedDataType(encryptedData));
                    throw new PGPDecryptionException(message, e);
                }
            } else {
                final PGPLiteralData literalData = getLiteralData(encryptedData);
                attributes.put(PGPAttributeKey.LITERAL_DATA_FILENAME, literalData.getFileName());
                attributes.put(PGPAttributeKey.LITERAL_DATA_MODIFIED, Long.toString(literalData.getModificationTime().getTime()));

                getLogger().debug("PGP Decrypted File Name [{}] Modified [{}]", literalData.getFileName(), literalData.getModificationTime());
                StreamUtils.copy(literalData.getInputStream(), outputStream);
            }

            if (isVerified(encryptedData)) {
                getLogger().debug("PGP Encrypted Data Verified");
            } else {
                final String message = String.format("PGP Encrypted Data [%s] Not Verified", encryptedData.getClass().getSimpleName());
                throw new PGPDecryptionException(message);
            }
        }

        private PGPEncryptedData findSupportedEncryptedData(final PGPEncryptedDataList encryptedDataList) {
            PGPEncryptedData supportedEncryptedData = null;

            final List<PGPPBEEncryptedData> passwordBasedEncrypted = new ArrayList<>();
            final List<PGPPublicKeyEncryptedData> publicKeyEncrypted = new ArrayList<>();
            for (final PGPEncryptedData encryptedData : encryptedDataList) {
                if (supportedEncryptedData == null) {
                    supportedEncryptedData = encryptedData;
                }

                if (encryptedData instanceof PGPPBEEncryptedData) {
                    passwordBasedEncrypted.add((PGPPBEEncryptedData) encryptedData);
                } else if (encryptedData instanceof PGPPublicKeyEncryptedData) {
                    publicKeyEncrypted.add((PGPPublicKeyEncryptedData) encryptedData);
                }
            }
            getLogger().debug("PGP Encrypted Data Password-Based Tags [{}] Public Key Tags [{}]", passwordBasedEncrypted.size(), publicKeyEncrypted.size());

            final Iterator<PGPPublicKeyEncryptedData> publicKeyData = publicKeyEncrypted.iterator();
            if (privateKeyService == null) {
                final Iterator<PGPPBEEncryptedData> passwordBasedData = passwordBasedEncrypted.iterator();
                if (passwordBasedData.hasNext()) {
                    supportedEncryptedData = passwordBasedData.next();
                } else {
                    final String message = String.format("PGP [%s] Tag not found and [%s] not configured", PASSWORD_BASED_ENCRYPTION, PRIVATE_KEY_SERVICE.getDisplayName());
                    throw new PGPDecryptionException(message);
                }
            } else if (publicKeyData.hasNext()) {
                while (publicKeyData.hasNext()) {
                    final PGPPublicKeyEncryptedData publicKeyEncryptedData = publicKeyData.next();
                    final long keyId = publicKeyEncryptedData.getKeyID();
                    final Optional<PGPPrivateKey> privateKey = privateKeyService.findPrivateKey(keyId);
                    if (privateKey.isPresent()) {
                        supportedEncryptedData = publicKeyEncryptedData;
                        final String keyIdentifier = KeyIdentifierConverter.format(keyId);
                        getLogger().debug("PGP Private Key [{}] Found for Public Key Encrypted Data", keyIdentifier);
                        break;
                    }
                }
            }

            if (supportedEncryptedData == null) {
                final String message = String.format("Supported Encrypted Data not found in Password-Based [%d] Public Key [%d]", passwordBasedEncrypted.size(), publicKeyEncrypted.size());
                throw new PGPDecryptionException(message);
            }

            return supportedEncryptedData;
        }

        private PGPLiteralData getLiteralData(final PGPEncryptedData encryptedData) {
            try {
                final InputStream decryptedDataStream = getDecryptedDataStream(encryptedData);
                final PGPObjectFactory objectFactory = new JcaPGPObjectFactory(decryptedDataStream);
                return getLiteralData(objectFactory);
            } catch (final PGPException e) {
                final String message = String.format("PGP Decryption Failed [%s]", getEncryptedDataType(encryptedData));
                throw new PGPDecryptionException(message, e);
            }
        }

        private PGPLiteralData getLiteralData(final PGPObjectFactory objectFactory) throws PGPException {
            PGPLiteralData literalData = null;

            for (final Object object : objectFactory) {
                if (object instanceof PGPCompressedData) {
                    final PGPCompressedData compressedData = (PGPCompressedData) object;
                    getLogger().debug("PGP Compressed Data Algorithm [{}] Found", compressedData.getAlgorithm());
                    final PGPObjectFactory compressedObjectFactory = new JcaPGPObjectFactory(compressedData.getDataStream());
                    literalData = getLiteralData(compressedObjectFactory);
                    break;
                } else if (object instanceof PGPLiteralData) {
                    literalData = (PGPLiteralData) object;
                    break;
                }
            }

            if (literalData == null) {
                throw new PGPProcessException("PGP Literal Data not found");
            }

            return literalData;
        }

        private InputStream getDecryptedDataStream(final PGPEncryptedData encryptedData) throws PGPException {
            getLogger().debug("PGP Encrypted Data [{}] Found", getEncryptedDataType(encryptedData));

            if (encryptedData instanceof PGPPBEEncryptedData) {
                return getDecryptedDataStream((PGPPBEEncryptedData) encryptedData);
            } else if (encryptedData instanceof PGPPublicKeyEncryptedData) {
                return getDecryptedDataStream((PGPPublicKeyEncryptedData) encryptedData);
            } else {
                final String message = String.format("PGP Encrypted Data [%s] Not Supported", getEncryptedDataType(encryptedData));
                throw new UnsupportedOperationException(message);
            }
        }

        private InputStream getDecryptedDataStream(final PGPPBEEncryptedData passwordBasedEncryptedData) throws PGPException {
            if (passphrase == null) {
                throw new PGPProcessException("PGP Password-Based Encryption Found: Passphrase not configured");
            } else {
                final PBEDataDecryptorFactory decryptorFactory = new BcPBEDataDecryptorFactory(passphrase, new BcPGPDigestCalculatorProvider());
                final int symmetricAlgorithm = passwordBasedEncryptedData.getSymmetricAlgorithm(decryptorFactory);
                setSymmetricKeyAlgorithmAttributes(symmetricAlgorithm);
                return passwordBasedEncryptedData.getDataStream(decryptorFactory);
            }
        }

        private InputStream getDecryptedDataStream(final PGPPublicKeyEncryptedData publicKeyEncryptedData) throws PGPException {
            if (privateKeyService == null) {
                throw new PGPProcessException("PGP Public Key Encryption Found: Private Key Service not configured");
            } else {
                final long keyId = publicKeyEncryptedData.getKeyID();
                final Optional<PGPPrivateKey> foundPrivateKey = privateKeyService.findPrivateKey(keyId);
                if (foundPrivateKey.isPresent()) {
                    final PGPPrivateKey privateKey = foundPrivateKey.get();
                    final PublicKeyDataDecryptorFactory decryptorFactory = new BcPublicKeyDataDecryptorFactory(privateKey);
                    final int symmetricAlgorithm = publicKeyEncryptedData.getSymmetricAlgorithm(decryptorFactory);
                    setSymmetricKeyAlgorithmAttributes(symmetricAlgorithm);
                    return publicKeyEncryptedData.getDataStream(decryptorFactory);
                } else {
                    final String keyIdentifier = KeyIdentifierConverter.format(keyId);
                    final String message = String.format("PGP Private Key [%s] not found for Public Key Encryption", keyIdentifier);
                    throw new PGPDecryptionException(message);
                }
            }
        }

        private void setSymmetricKeyAlgorithmAttributes(final int symmetricAlgorithm) {
            final String blockCipher = PGPUtil.getSymmetricCipherName(symmetricAlgorithm);
            attributes.put(PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_BLOCK_CIPHER, blockCipher);
            attributes.put(PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_ID, Integer.toString(symmetricAlgorithm));
        }

        private boolean isVerified(final PGPEncryptedData encryptedData) {
            boolean verified;

            if (encryptedData.isIntegrityProtected()) {
                try {
                    verified = encryptedData.verify();
                } catch (final PGPException e) {
                    throw new PGPDecryptionException("PGP Encrypted Data Verification Failed", e);
                } catch (final IOException e) {
                    throw new UncheckedIOException("PGP Encrypted Data Reading Signature Failed", e);
                }
            } else {
                verified = true;
            }

            return verified;
        }

        private PGPEncryptedDataList getEncryptedDataList(final InputStream inputStream) throws IOException {
            final InputStream decoderInputStream = PGPUtil.getDecoderStream(inputStream);
            final PGPEncryptedDataList encryptedDataList = findEncryptedDataList(decoderInputStream);
            if (encryptedDataList == null) {
                throw new PGPProcessException("PGP Encrypted Data Packets not found");
            } else {
                getLogger().debug("PGP Encrypted Data Packets found [{}]", encryptedDataList.size());
                return encryptedDataList;
            }
        }

        private PGPEncryptedDataList findEncryptedDataList(final InputStream inputStream) {
            PGPEncryptedDataList encryptedDataList = null;

            final PGPObjectFactory objectFactory = new JcaPGPObjectFactory(inputStream);
            for (final Object object : objectFactory) {
                getLogger().debug("PGP Object Read [{}]", object.getClass().getSimpleName());
                if (object instanceof PGPEncryptedDataList) {
                    encryptedDataList = (PGPEncryptedDataList) object;
                    break;
                }
            }

            return encryptedDataList;
        }

        private String getEncryptedDataType(final PGPEncryptedData encryptedData) {
            String encryptedDataType = encryptedData.getClass().getSimpleName();
            if (encryptedData instanceof PGPPBEEncryptedData) {
                encryptedDataType = PASSWORD_BASED_ENCRYPTION;
            } else if (encryptedData instanceof PGPPublicKeyEncryptedData) {
                encryptedDataType = PUBLIC_KEY_ENCRYPTION;
            }
            return encryptedDataType;
        }
    }
}
