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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.pgp.service.api.PGPPublicKeyService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.pgp.exception.PGPProcessException;
import org.apache.nifi.pgp.service.api.KeyIdentifierConverter;
import org.apache.nifi.stream.io.StreamUtils;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPOnePassSignature;
import org.bouncycastle.openpgp.PGPOnePassSignatureList;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureList;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentVerifierBuilderProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Verify Content using Open Pretty Good Privacy Public Keys
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"PGP", "GPG", "OpenPGP", "Encryption", "Signing", "RFC 4880"})
@CapabilityDescription("Verify signatures using OpenPGP Public Keys")
@SeeAlso({ DecryptContentPGP.class, EncryptContentPGP.class, SignContentPGP.class })
@WritesAttributes({
        @WritesAttribute(attribute = PGPAttributeKey.LITERAL_DATA_FILENAME, description = "Filename from Literal Data"),
        @WritesAttribute(attribute = PGPAttributeKey.LITERAL_DATA_MODIFIED, description = "Modified Date Time from Literal Data in milliseconds"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_CREATED, description = "Signature Creation Time in milliseconds"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_ALGORITHM, description = "Signature Algorithm including key and hash algorithm names"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_HASH_ALGORITHM_ID, description = "Signature Hash Algorithm Identifier"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_KEY_ALGORITHM_ID, description = "Signature Key Algorithm Identifier"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_KEY_ID, description = "Signature Public Key Identifier"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_TYPE_ID, description = "Signature Type Identifier"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_VERSION, description = "Signature Version Number"),
})
public class VerifyContentPGP extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Signature Verification Succeeded")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Signature Verification Failed")
            .build();

    public static final PropertyDescriptor PUBLIC_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("public-key-service")
            .displayName("Public Key Service")
            .description("PGP Public Key Service for verifying signatures with Public Key Encryption")
            .identifiesControllerService(PGPPublicKeyService.class)
            .required(true)
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS,
            FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PUBLIC_KEY_SERVICE
    );

    private static final int BUFFER_SIZE = 8192;

    private static final String KEY_ID_UNKNOWN = "UNKNOWN";

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
     * On Trigger verifies signatures found in Flow File contents using configured properties
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

        final PGPPublicKeyService publicKeyService = context.getProperty(PUBLIC_KEY_SERVICE).asControllerService(PGPPublicKeyService.class);
        final VerifyStreamCallback callback = new VerifyStreamCallback(publicKeyService);
        try {
            flowFile = session.write(flowFile, callback);
            flowFile = session.putAllAttributes(flowFile, callback.attributes);
            final String keyId = flowFile.getAttribute(PGPAttributeKey.SIGNATURE_KEY_ID);
            getLogger().info("Signature Key ID [{}] Verification Completed {}", keyId, flowFile);
            session.transfer(flowFile, SUCCESS);
        } catch (final RuntimeException e) {
            flowFile = session.putAllAttributes(flowFile, callback.attributes);
            getLogger().error("Processing Failed {}", flowFile, e);
            session.transfer(flowFile, FAILURE);
        }
    }

    private class VerifyStreamCallback implements StreamCallback {
        private final PGPPublicKeyService publicKeyService;

        private final Map<String, String> attributes = new HashMap<>();

        private boolean verified;

        private VerifyStreamCallback(final PGPPublicKeyService publicKeyService) {
            this.publicKeyService = publicKeyService;
        }

        /**
         * Process Input Stream containing binary or ASCII Armored OpenPGP messages and write literal data after verification
         *
         * @param inputStream Input Stream to be read
         * @param outputStream Output Stream for literal data contents
         * @throws IOException Thrown when unable to read or write streams
         */
        @Override
        public void process(final InputStream inputStream, final OutputStream outputStream) throws IOException {
            final InputStream decoderInputStream = PGPUtil.getDecoderStream(inputStream);
            final PGPObjectFactory pgpObjectFactory = new JcaPGPObjectFactory(decoderInputStream);
            final Iterator<?> objects = pgpObjectFactory.iterator();
            if (objects.hasNext()) {
                processObjectFactory(objects, outputStream);
            }

            if (verified) {
                getLogger().debug("One-Pass Signature Algorithm [{}] Verified", attributes.get(PGPAttributeKey.SIGNATURE_ALGORITHM));
            } else {
                final String keyId = attributes.getOrDefault(PGPAttributeKey.SIGNATURE_KEY_ID, KEY_ID_UNKNOWN);
                throw new PGPProcessException(String.format("Signature Key ID [%s] Verification Failed", keyId));
            }
        }

        private void processObjectFactory(final Iterator<?> objects, final OutputStream outputStream) throws IOException {
            PGPOnePassSignature onePassSignature = null;

            while (objects.hasNext()) {
                final Object object = objects.next();
                getLogger().debug("PGP Object Read [{}]", object.getClass().getSimpleName());

                if (object instanceof PGPCompressedData) {
                    final PGPCompressedData compressedData = (PGPCompressedData) object;
                    try {
                        final PGPObjectFactory compressedObjectFactory = new JcaPGPObjectFactory(compressedData.getDataStream());
                        processObjectFactory(compressedObjectFactory.iterator(), outputStream);
                    } catch (final PGPException e) {
                        throw new PGPProcessException("Read Compressed Data Failed", e);
                    }
                } else if (object instanceof PGPOnePassSignatureList) {
                    final PGPOnePassSignatureList onePassSignatureList = (PGPOnePassSignatureList) object;
                    onePassSignature = processOnePassSignatures(onePassSignatureList);
                } else if (object instanceof PGPLiteralData) {
                    final PGPLiteralData literalData = (PGPLiteralData) object;
                    processLiteralData(literalData, outputStream, onePassSignature);
                } else if (object instanceof PGPSignatureList) {
                    final PGPSignatureList signatureList = (PGPSignatureList) object;
                    processSignatures(signatureList, onePassSignature);
                }
            }
        }

        private PGPOnePassSignature processOnePassSignatures(final PGPOnePassSignatureList onePassSignatureList) {
            getLogger().debug("One-Pass Signatures Found [{}]", onePassSignatureList.size());

            PGPOnePassSignature initializedOnePassSignature = null;
            final Iterator<PGPOnePassSignature> onePassSignatures = onePassSignatureList.iterator();
            if (onePassSignatures.hasNext()) {
                final PGPOnePassSignature onePassSignature = onePassSignatures.next();
                setOnePassSignatureAttributes(onePassSignature);

                final String keyId = KeyIdentifierConverter.format(onePassSignature.getKeyID());
                final Optional<PGPPublicKey> optionalPublicKey = publicKeyService.findPublicKey(keyId);
                if (optionalPublicKey.isPresent()) {
                    getLogger().debug("One-Pass Signature Key ID [{}] found", keyId);
                    final PGPPublicKey publicKey = optionalPublicKey.get();
                    try {
                        onePassSignature.init(new JcaPGPContentVerifierBuilderProvider(), publicKey);
                        initializedOnePassSignature = onePassSignature;
                    } catch (final PGPException e) {
                        throw new PGPProcessException(String.format("One-Pass Signature Key ID [%s] Initialization Failed", keyId), e);
                    }
                } else {
                    getLogger().warn("One-Pass Signature Key ID [{}] not found in Public Key Service", keyId);
                }
            }
            return initializedOnePassSignature;
        }

        private void processLiteralData(final PGPLiteralData literalData,
                                        final OutputStream outputStream,
                                        final PGPOnePassSignature onePassSignature) throws IOException {
            setLiteralDataAttributes(literalData);
            final InputStream literalInputStream = literalData.getInputStream();
            if (onePassSignature == null) {
                StreamUtils.copy(literalInputStream, outputStream);
            } else {
                processSignedStream(literalInputStream, outputStream, onePassSignature);
            }
        }

        private void processSignatures(final PGPSignatureList signatureList, final PGPOnePassSignature onePassSignature) {
            getLogger().debug("Signatures Found [{}]", signatureList.size());
            final Iterator<PGPSignature> signatures = signatureList.iterator();
            if (signatures.hasNext()) {
                final PGPSignature signature = signatures.next();
                setSignatureAttributes(signature);

                if (onePassSignature == null) {
                    getLogger().debug("One-Pass Signature not found: Verification Failed");
                } else {
                    try {
                        verified = onePassSignature.verify(signature);
                    } catch (final PGPException e) {
                        final String keyId = KeyIdentifierConverter.format(onePassSignature.getKeyID());
                        throw new PGPProcessException(String.format("One-Pass Signature Key ID [%s] Verification Failed", keyId), e);
                    }
                }
            }
        }

        private void processSignedStream(final InputStream inputStream, final OutputStream outputStream, final PGPOnePassSignature onePassSignature) throws IOException {
            final String keyId = KeyIdentifierConverter.format(onePassSignature.getKeyID());
            getLogger().debug("Processing Data for One-Pass Signature with Key ID [{}]", keyId);
            final byte[] buffer = new byte[BUFFER_SIZE];
            int read;
            while ((read = inputStream.read(buffer)) >= 0) {
                onePassSignature.update(buffer, 0, read);
                outputStream.write(buffer, 0, read);
            }
        }

        private void setOnePassSignatureAttributes(final PGPOnePassSignature onePassSignature) {
            setSignatureAlgorithm(onePassSignature.getKeyAlgorithm(), onePassSignature.getHashAlgorithm());
            attributes.put(PGPAttributeKey.SIGNATURE_KEY_ID, KeyIdentifierConverter.format(onePassSignature.getKeyID()));
            attributes.put(PGPAttributeKey.SIGNATURE_TYPE_ID, Integer.toString(onePassSignature.getSignatureType()));
        }

        private void setSignatureAttributes(final PGPSignature signature) {
            setSignatureAlgorithm(signature.getKeyAlgorithm(), signature.getHashAlgorithm());
            attributes.put(PGPAttributeKey.SIGNATURE_CREATED, Long.toString(signature.getCreationTime().getTime()));
            attributes.put(PGPAttributeKey.SIGNATURE_KEY_ID, KeyIdentifierConverter.format(signature.getKeyID()));
            attributes.put(PGPAttributeKey.SIGNATURE_TYPE_ID, Integer.toString(signature.getSignatureType()));
            attributes.put(PGPAttributeKey.SIGNATURE_VERSION, Integer.toString(signature.getVersion()));
        }

        private void setLiteralDataAttributes(final PGPLiteralData literalData) {
            attributes.put(PGPAttributeKey.LITERAL_DATA_FILENAME, literalData.getFileName());
            attributes.put(PGPAttributeKey.LITERAL_DATA_MODIFIED, Long.toString(literalData.getModificationTime().getTime()));
        }

        private void setSignatureAlgorithm(final int keyAlgorithm, final int hashAlgorithm) {
            attributes.put(PGPAttributeKey.SIGNATURE_HASH_ALGORITHM_ID, Integer.toString(hashAlgorithm));
            attributes.put(PGPAttributeKey.SIGNATURE_KEY_ALGORITHM_ID, Integer.toString(keyAlgorithm));
            try {
                final String algorithm = PGPUtil.getSignatureName(keyAlgorithm, hashAlgorithm);
                attributes.put(PGPAttributeKey.SIGNATURE_ALGORITHM, algorithm);
            } catch (final PGPException e) {
                getLogger().debug("Signature Algorithm Key Identifier [{}] Hash Identifier [{}] not found", keyAlgorithm, hashAlgorithm);
            }
        }
    }
}
