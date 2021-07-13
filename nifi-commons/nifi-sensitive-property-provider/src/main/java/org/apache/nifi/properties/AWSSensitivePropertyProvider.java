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
package org.apache.nifi.properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.BootstrapProperties.BootstrapPropertyKey;

import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.DecoderException;
import org.bouncycastle.util.encoders.EncoderException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KmsException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public class AWSSensitivePropertyProvider extends AbstractSensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(AWSSensitivePropertyProvider.class);

    private static final String AWS_PREFIX = "aws";
    private static final String ACCESS_KEY_PROPS_NAME = "aws.access.key.id";
    private static final String SECRET_KEY_PROPS_NAME = "aws.secret.key.id";
    private static final String REGION_KEY_PROPS_NAME = "aws.region";
    private static final String KMS_KEY_PROPS_NAME = "aws.kms.key.id";

    private static final Charset PROPERTY_CHARSET = StandardCharsets.UTF_8;

    private final BootstrapProperties awsBootstrapProperties;
    private KmsClient client;
    private String keyId;


    AWSSensitivePropertyProvider(final BootstrapProperties bootstrapProperties) throws SensitivePropertyProtectionException {
        super(bootstrapProperties);
        // if either awsBootstrapProperties or keyId is loaded as null values, then isSupported will return false
        awsBootstrapProperties = getAWSBootstrapProperties(bootstrapProperties);
        if (awsBootstrapProperties != null) {
            loadRequiredAWSProperties(awsBootstrapProperties);
        }
    }

    /**
     * Initializes the KMS Client to be used for encrypt, decrypt and other interactions with AWS KMS
     * First attempts to use credentials/configuration in bootstrap-aws.conf
     * If credentials/configuration in bootstrap-aws.conf is not fully configured, attempt to initialize credentials using default AWS credentials/configuration chain
     * Note: This does not verify if credentials are valid
     */
    private final void initializeClient() {
        if (awsBootstrapProperties == null) {
            logger.warn("Cannot initialize client if awsBootstrapProperties is null");
            return;
        }
        final String accessKeyId = awsBootstrapProperties.getProperty(ACCESS_KEY_PROPS_NAME);
        final String secretKeyId = awsBootstrapProperties.getProperty(SECRET_KEY_PROPS_NAME);
        final String region = awsBootstrapProperties.getProperty(REGION_KEY_PROPS_NAME);

        if (StringUtils.isNotBlank(accessKeyId) && StringUtils.isNotBlank(secretKeyId) && StringUtils.isNotBlank(region)) {
            logger.debug("Credentials/Configuration provided in bootstrap-aws.conf");
            try {
                final AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, secretKeyId);
                client = KmsClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(StaticCredentialsProvider.create(credentials))
                        .build();
            } catch (final KmsException | NullPointerException | IllegalArgumentException e) {
                final String msg = "Valid configuration/credentials are required to initialize KMS client";
                logger.error(msg);
                throw new SensitivePropertyProtectionException(msg, e);
            }
        } else {
            // attempts to initialize client with credentials provider chain
            logger.debug("Credentials/Configuration not provided in bootstrap-aws.conf, attempting to use default configuration");
            try {
                final DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder()
                        .build();
                // the following is needed to check the default credential builder, if it fails, throws SdkClientException
                credentialsProvider.resolveCredentials();
                client = KmsClient.builder()
                        .credentialsProvider(credentialsProvider)
                        .build();
            } catch (final SdkClientException e) {
                // this exception occurs if default credentials are not provided
                final String msg = "Valid configuration/credentials are required to initialize KMS client";
                logger.error(msg);
                throw new SensitivePropertyProtectionException(msg, e);
            }
        }
    }

    /**
     * Checks if the client used to communicate with AWS KMS service is open
     * @return true if the client has been initialized and open, false otherwise
     */
    private final boolean isClientOpen() {
        return client != null;
    }

    /**
     * Validates the key ARN, credentials and configuration provided by the user.
     * Note: This function performs checks on the key and indirectly also validates the credentials and
     * configurations provided during the initialization of the client
     */
    private final void validate() throws KmsException, SensitivePropertyProtectionException {
        if (!isClientOpen()) {
            final String msg = "The AWS KMS Client failed to open, cannot validate key";
            logger.error(msg);
            throw new SensitivePropertyProtectionException(msg);
        }
        if (StringUtils.isBlank(keyId)) {
            final String msg = "The AWS KMS key provided is blank";
            logger.error(msg);
            throw new SensitivePropertyProtectionException(msg);
        }

        // asking for a Key Description is the best way to check whether a key is valid
        // because AWS KMS accepts various formats for its keys.

        final DescribeKeyRequest request = DescribeKeyRequest.builder()
                .keyId(keyId)
                .build();

        // using the KmsClient in a DescribeKey request indirectly also verifies if the credentials provided
        // during the initialization of the key are valid
        final DescribeKeyResponse response = client.describeKey(request);
        final KeyMetadata metadata = response.keyMetadata();

        if (!metadata.enabled()) {
            final String msg = String.format("AWS KMS key [%s] is not enabled", keyId);
            throw new SensitivePropertyProtectionException(msg);
        }
    }

    /**
     * Checks if we have a key ID from AWS KMS and loads it into {@link #keyId}. Will load null if key is not present
     * Note: This function does not verify if the key is correctly formatted/valid
     * @param props the properties representing bootstrap-aws.conf
     */
    private void loadRequiredAWSProperties(final BootstrapProperties props) {
        if (props != null) {
            keyId = props.getProperty(KMS_KEY_PROPS_NAME);
        }
    }


    /**
     * Checks bootstrap.conf to check if BootstrapPropertyKey.AWS_KMS_SENSITIVE_PROPERTY_PROVIDER_CONF property is
     * configured to the bootstrap-aws.conf file. Also will try to load bootstrap-aws.conf to {@link #awsBootstrapProperties}
     * @param bootstrapProperties BootstrapProperties object corresponding to bootstrap.conf
     * @return BootstrapProperties object corresponding to bootstrap-aws.conf, null otherwise
     */
    private BootstrapProperties getAWSBootstrapProperties(final BootstrapProperties bootstrapProperties) {
        if (bootstrapProperties == null) {
            logger.warn("The file bootstrap.conf provided to AWS SPP is null");
            return null;
        }

        final BootstrapProperties cloudBootstrapProperties;

        // Load the bootstrap-aws.conf file based on path specified in
        // "nifi.bootstrap.protection.aws.kms.conf" property of bootstrap.conf
        final String filePath = bootstrapProperties.getProperty(BootstrapPropertyKey.AWS_KMS_SENSITIVE_PROPERTY_PROVIDER_CONF).orElse(null);
        if (StringUtils.isBlank(filePath)) {
            logger.warn("File Path to bootstrap-aws.conf in bootstrap.conf is blank");
            return null;
        }

        try {
            cloudBootstrapProperties = AbstractBootstrapPropertiesLoader.loadBootstrapProperties(
                    Paths.get(filePath), AWS_PREFIX);
        } catch (IOException e) {
            throw new SensitivePropertyProtectionException("Could not load " + filePath, e);
        }

        return cloudBootstrapProperties;
    }

    /**
     * Checks bootstrap-aws.conf for the required configurations for AWS KMS encrypt/decrypt operations
     * Note: This does not check for credentials/region configurations.
     *       Credentials/configuration will be checked during the first protect/unprotect call during runtime.
     * @return True if bootstrap-aws.conf contains the required properties for AWS SPP, False otherwise
     */
    private boolean hasRequiredAWSProperties() {
        return awsBootstrapProperties != null && StringUtils.isNotBlank(keyId);
    }

    @Override
    public boolean isSupported() {
        return hasRequiredAWSProperties();
    }

    @Override
    protected PropertyProtectionScheme getProtectionScheme() {
        return PropertyProtectionScheme.AWS_KMS;
    }

    /**
     * Returns the name of the underlying implementation.
     *
     * @return the name of this sensitive property provider
     */
    @Override
    public String getName() {
        return PropertyProtectionScheme.AWS_KMS.getName();
    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property
     */
    @Override
    public String getIdentifierKey() {
        return PropertyProtectionScheme.AWS_KMS.getIdentifier();
    }


    /**
     * Returns the ciphertext blob of this value encrypted using an AWS KMS CMK.
     *
     * @return the ciphertext blob to persist in the {@code nifi.properties} file
     */
    private byte[] encrypt(final byte[] input) {
        final SdkBytes plainBytes = SdkBytes.fromByteArray(input);

        // builds an encryption request to be sent to the kmsClient
        final EncryptRequest encryptRequest = EncryptRequest.builder()
                .keyId(keyId)
                .plaintext(plainBytes)
                .build();

        // sends request, records response
        final EncryptResponse response = client.encrypt(encryptRequest);

        // get encrypted data
        final SdkBytes encryptedData = response.ciphertextBlob();

        return encryptedData.asByteArray();
    }

    /**
     * Returns the value corresponding to a ciphertext blob decrypted using an AWS KMS CMK
     *
     * @return the "unprotected" byte[] of this value, which could be used by the application
     */
    private byte[] decrypt(final byte[] input) {
        final SdkBytes cipherBytes = SdkBytes.fromByteArray(input);

        // builds a decryption request to be sent to the kmsClient
        final DecryptRequest decryptRequest = DecryptRequest.builder()
                .ciphertextBlob(cipherBytes)
                .keyId(keyId)
                .build();

        // sends request, records response
        final DecryptResponse response = client.decrypt(decryptRequest);

        // get decrypted data
        final SdkBytes decryptedData = response.plaintext();

        return decryptedData.asByteArray();
    }

    /**
     * Returns the "protected" form of this value. This is a form which can safely be persisted in the {@code nifi.properties} file without compromising the value.
     * An encryption-based provider would return a cipher text, while a remote-lookup provider could return a unique ID to retrieve the secured value.
     *
     * @param unprotectedValue the sensitive value
     * @return the value to persist in the {@code nifi.properties} file
     */
    @Override
    public String protect(final String unprotectedValue) throws SensitivePropertyProtectionException {
        if (StringUtils.isBlank(unprotectedValue)) {
            throw new IllegalArgumentException("Cannot encrypt a null/empty value");
        }

        if (!isClientOpen()) {
            try {
                initializeClient();
                validate();
            } catch (final SdkClientException | KmsException | SensitivePropertyProtectionException e) {
                logger.error("Encountered an error initializing the client for {}: {}", getName(), e.getMessage());
                throw new SensitivePropertyProtectionException("Error initializing the AWS KMS Client", e);
            }
        }

        try {
            final byte[] plainBytes = unprotectedValue.getBytes(PROPERTY_CHARSET);
            final byte[] cipherBytes = encrypt(plainBytes);
            logger.debug(getName() + " encrypted a sensitive value successfully");
            return Base64.toBase64String(cipherBytes);
        } catch (final SdkClientException | KmsException | EncoderException e) {
            final String msg = "Error encrypting a protected value";
            logger.error(msg, e);
            throw new SensitivePropertyProtectionException(msg, e);
        }
    }

    /**
     * Returns the "unprotected" form of this value. This is the raw sensitive value which is used by the application logic.
     * An encryption-based provider would decrypt a cipher text and return the plaintext, while a remote-lookup provider could retrieve the secured value.
     *
     * @param protectedValue the protected value read from the {@code nifi.properties} file
     * @return the raw value to be used by the application
     */
    @Override
    public String unprotect(final String protectedValue) throws SensitivePropertyProtectionException {
        if (StringUtils.isBlank(protectedValue)) {
            throw new IllegalArgumentException("Cannot decrypt a null/empty cipher");
        }

        if (!isClientOpen()) {
            try {
                initializeClient();
                validate();
            } catch (final SdkClientException | KmsException | SensitivePropertyProtectionException e) {
                logger.error("Encountered an error initializing the client for {}: {}", getName(), e.getMessage());
                throw new SensitivePropertyProtectionException("Error initializing the AWS KMS Client", e);
            }
        }

        try {
            final byte[] cipherBytes = Base64.decode(protectedValue);
            final byte[] plainBytes = decrypt(cipherBytes);
            logger.debug(getName() + " decrypted a sensitive value successfully");
            return new String(plainBytes, PROPERTY_CHARSET);
        } catch (final SdkClientException | KmsException | DecoderException e) {
            final String msg = "Error decrypting a protected value";
            logger.error(msg, e);
            throw new SensitivePropertyProtectionException(msg, e);
        }
    }

    /**
     * Closes AWS KMS client that may have been opened
     */
    @Override
    public void cleanUp() {
        if (isClientOpen()) {
            client.close();
            client = null;
        }
    }
}
