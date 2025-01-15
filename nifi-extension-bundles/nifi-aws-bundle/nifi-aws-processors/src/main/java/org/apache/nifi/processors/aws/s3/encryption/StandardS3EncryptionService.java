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
package org.apache.nifi.processors.aws.s3.encryption;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Builder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService;
import org.apache.nifi.processors.aws.util.RegionUtilV1;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;


@Tags({"service", "aws", "s3", "encryption", "encrypt", "decryption", "decrypt", "key"})
@CapabilityDescription("Adds configurable encryption to S3 Put and S3 Fetch operations.")
public class StandardS3EncryptionService extends AbstractControllerService implements AmazonS3EncryptionService {
    private static final Logger logger = LoggerFactory.getLogger(StandardS3EncryptionService.class);

    private static final Map<String, S3EncryptionStrategy> NAMED_STRATEGIES = Map.of(
            STRATEGY_NAME_NONE, new NoOpEncryptionStrategy(),
            STRATEGY_NAME_SSE_S3, new ServerSideS3EncryptionStrategy(),
            STRATEGY_NAME_SSE_KMS, new ServerSideKMSEncryptionStrategy(),
            STRATEGY_NAME_SSE_C, new ServerSideCEncryptionStrategy(),
            STRATEGY_NAME_CSE_KMS, new ClientSideKMSEncryptionStrategy(),
            STRATEGY_NAME_CSE_C, new ClientSideCEncryptionStrategy()
    );

    private static final AllowableValue NONE = new AllowableValue(STRATEGY_NAME_NONE, "None", "No encryption.");
    private static final AllowableValue SSE_S3 = new AllowableValue(STRATEGY_NAME_SSE_S3, "Server-side S3", "Use server-side, S3-managed encryption.");
    private static final AllowableValue SSE_KMS = new AllowableValue(STRATEGY_NAME_SSE_KMS, "Server-side KMS", "Use server-side, KMS key to perform encryption.");
    private static final AllowableValue SSE_C = new AllowableValue(STRATEGY_NAME_SSE_C, "Server-side Customer Key", "Use server-side, customer-supplied key to perform encryption.");
    private static final AllowableValue CSE_KMS = new AllowableValue(STRATEGY_NAME_CSE_KMS, "Client-side KMS", "Use client-side, KMS key to perform encryption.");
    private static final AllowableValue CSE_C = new AllowableValue(STRATEGY_NAME_CSE_C, "Client-side Customer Key", "Use client-side, customer-supplied key to perform encryption.");

    public static final Map<String, AllowableValue> ENCRYPTION_STRATEGY_ALLOWABLE_VALUES =
            Map.of(STRATEGY_NAME_NONE, NONE,
                    STRATEGY_NAME_SSE_S3, SSE_S3,
                    STRATEGY_NAME_SSE_KMS, SSE_KMS,
                    STRATEGY_NAME_SSE_C, SSE_C,
                    STRATEGY_NAME_CSE_KMS, CSE_KMS,
                    STRATEGY_NAME_CSE_C, CSE_C);

    public static final PropertyDescriptor ENCRYPTION_STRATEGY = new PropertyDescriptor.Builder()
            .name("encryption-strategy")
            .displayName("Encryption Strategy")
            .description("Strategy to use for S3 data encryption and decryption.")
            .allowableValues(NONE, SSE_S3, SSE_KMS, SSE_C, CSE_KMS, CSE_C)
            .required(true)
            .defaultValue(NONE.getValue())
            .build();

    public static final PropertyDescriptor ENCRYPTION_VALUE = new PropertyDescriptor.Builder()
            .name("key-id-or-key-material")
            .displayName("Key ID or Key Material")
            .description("For None and Server-side S3: not used. For Server-side KMS and Client-side KMS: the KMS Key ID must be configured. " +
                    "For Server-side Customer Key and Client-side Customer Key: the Key Material must be specified in Base64 encoded form. " +
                    "In case of Server-side Customer Key, the key must be an AES-256 key. In case of Client-side Customer Key, it can be an AES-256, AES-192 or AES-128 key.")
            .required(false)
            .sensitive(true)
            .addValidator(Validator.VALID) // will be validated in customValidate()
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor KMS_REGION = new PropertyDescriptor.Builder()
            .name("kms-region")
            .displayName("KMS Region")
            .description("The Region of the AWS Key Management Service. Only used in case of Client-side KMS.")
            .required(false)
            .allowableValues(RegionUtilV1.getAvailableRegions())
            .defaultValue(RegionUtilV1.createAllowableValue(Regions.DEFAULT_REGION).getValue())
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
       ENCRYPTION_STRATEGY,
       ENCRYPTION_VALUE,
       KMS_REGION
    );

    private String keyValue = "";
    private String kmsRegion = "";
    private S3EncryptionStrategy encryptionStrategy = new NoOpEncryptionStrategy();
    private String strategyName = STRATEGY_NAME_NONE;

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        final String newStrategyName = context.getProperty(ENCRYPTION_STRATEGY).getValue();
        final String newKeyValue = context.getProperty(ENCRYPTION_VALUE).evaluateAttributeExpressions().getValue();
        final S3EncryptionStrategy newEncryptionStrategy = NAMED_STRATEGIES.get(newStrategyName);

        kmsRegion = context.getProperty(KMS_REGION).getValue();

        if (newEncryptionStrategy == null) {
            final String msg = "No encryption strategy found for name: " + strategyName;
            logger.warn(msg);
            throw new InitializationException(msg);
        }

        strategyName = newStrategyName;
        encryptionStrategy = newEncryptionStrategy;
        keyValue = newKeyValue;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        Collection<ValidationResult> validationResults = new ArrayList<>();

        String encryptionStrategyName = validationContext.getProperty(ENCRYPTION_STRATEGY).getValue();
        String encryptionStrategyDisplayName = ENCRYPTION_STRATEGY_ALLOWABLE_VALUES.get(encryptionStrategyName).getDisplayName();
        PropertyValue encryptionValueProperty = validationContext.getProperty(ENCRYPTION_VALUE);
        String encryptionValue = encryptionValueProperty.evaluateAttributeExpressions().getValue();

        switch (encryptionStrategyName) {
            case STRATEGY_NAME_NONE:
            case STRATEGY_NAME_SSE_S3:
                if (encryptionValueProperty.isSet()) {
                    validationResults.add(new ValidationResult.Builder()
                            .subject(ENCRYPTION_VALUE.getDisplayName())
                            .valid(false)
                            .explanation("the property cannot be specified for encryption strategy " + encryptionStrategyDisplayName)
                            .build()
                    );
                }
                break;
            case STRATEGY_NAME_SSE_KMS:
            case STRATEGY_NAME_CSE_KMS:
                if (StringUtils.isEmpty(encryptionValue)) {
                    validationResults.add(new ValidationResult.Builder()
                            .subject(ENCRYPTION_VALUE.getDisplayName())
                            .valid(false)
                            .explanation("a non-empty Key ID must be specified for encryption strategy " + encryptionStrategyDisplayName)
                            .build()
                    );
                }
                break;
            case STRATEGY_NAME_SSE_C:
            case STRATEGY_NAME_CSE_C:
                if (StringUtils.isEmpty(encryptionValue)) {
                    validationResults.add(new ValidationResult.Builder()
                            .subject(ENCRYPTION_VALUE.getDisplayName())
                            .valid(false)
                            .explanation("a non-empty Key Material must be specified for encryption strategy " + encryptionStrategyDisplayName)
                            .build()
                    );
                } else {
                    S3EncryptionStrategy encryptionStrategy = NAMED_STRATEGIES.get(encryptionStrategyName);
                    String keyIdOrMaterial = validationContext.getProperty(ENCRYPTION_VALUE).evaluateAttributeExpressions().getValue();

                    validationResults.add(encryptionStrategy.validateKey(keyIdOrMaterial));
                }
                break;
        }

        return validationResults;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void configurePutObjectRequest(PutObjectRequest request, ObjectMetadata objectMetadata) {
        encryptionStrategy.configurePutObjectRequest(request, objectMetadata, keyValue);
    }

    @Override
    public void configureInitiateMultipartUploadRequest(InitiateMultipartUploadRequest request, ObjectMetadata objectMetadata) {
        encryptionStrategy.configureInitiateMultipartUploadRequest(request, objectMetadata, keyValue);
    }

    @Override
    public void configureGetObjectRequest(GetObjectRequest request, ObjectMetadata objectMetadata) {
        encryptionStrategy.configureGetObjectRequest(request, objectMetadata, keyValue);
    }

    @Override
    public void configureUploadPartRequest(UploadPartRequest request, ObjectMetadata objectMetadata) {
        encryptionStrategy.configureUploadPartRequest(request, objectMetadata, keyValue);
    }

    @Override
    public AmazonS3 createEncryptionClient(final Consumer<AmazonS3Builder<?, ?>> clientBuilder) {
        return encryptionStrategy.createEncryptionClient(clientBuilder, kmsRegion, keyValue);
    }

    @Override
    public String getKmsRegion() {
        return kmsRegion;
    }

    @Override
    public String getStrategyName() {
        return strategyName;
    }

    @Override
    public String getStrategyDisplayName() {
        return ENCRYPTION_STRATEGY_ALLOWABLE_VALUES.get(strategyName).getDisplayName();
    }
}


