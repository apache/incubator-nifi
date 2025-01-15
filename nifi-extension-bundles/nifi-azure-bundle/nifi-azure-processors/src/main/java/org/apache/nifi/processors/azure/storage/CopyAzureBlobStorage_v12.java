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
package org.apache.nifi.processors.azure.storage;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.HttpAuthorization;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobUploadFromUrlOptions;
import com.azure.storage.blob.options.BlockBlobCommitBlockListOptions;
import com.azure.storage.blob.options.BlockBlobStageBlockFromUrlOptions;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor_v12;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.storage.AzureStorageConflictResolutionStrategy;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsType;
import reactor.core.publisher.Mono;

import java.text.DecimalFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.azure.storage.blob.specialized.BlockBlobClient.MAX_STAGE_BLOCK_BYTES_LONG;
import static com.azure.storage.blob.specialized.BlockBlobClient.MAX_UPLOAD_BLOB_BYTES_LONG;
import static com.azure.storage.common.implementation.Constants.STORAGE_SCOPE;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.BLOB_STORAGE_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_BLOBTYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_CONTAINER;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_ERROR_CODE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_IGNORED;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_LANG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_MIME_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_TIMESTAMP;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBTYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_CONTAINER;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_ERROR_CODE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_IGNORED;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LANG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_MIME_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_TIMESTAMP;

@Tags({"azure", "microsoft", "cloud", "storage", "blob"})
@SeeAlso({ListAzureBlobStorage_v12.class, FetchAzureBlobStorage_v12.class, DeleteAzureBlobStorage_v12.class, PutAzureBlobStorage_v12.class})
@CapabilityDescription("Copies a blob in Azure Blob Storage from one account/container to another. The processor uses Azure Blob Storage client library v12.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({@WritesAttribute(attribute = ATTR_NAME_CONTAINER, description = ATTR_DESCRIPTION_CONTAINER),
        @WritesAttribute(attribute = ATTR_NAME_BLOBNAME, description = ATTR_DESCRIPTION_BLOBNAME),
        @WritesAttribute(attribute = ATTR_NAME_PRIMARY_URI, description = ATTR_DESCRIPTION_PRIMARY_URI),
        @WritesAttribute(attribute = ATTR_NAME_ETAG, description = ATTR_DESCRIPTION_ETAG),
        @WritesAttribute(attribute = ATTR_NAME_BLOBTYPE, description = ATTR_DESCRIPTION_BLOBTYPE),
        @WritesAttribute(attribute = ATTR_NAME_MIME_TYPE, description = ATTR_DESCRIPTION_MIME_TYPE),
        @WritesAttribute(attribute = ATTR_NAME_LANG, description = ATTR_DESCRIPTION_LANG),
        @WritesAttribute(attribute = ATTR_NAME_TIMESTAMP, description = ATTR_DESCRIPTION_TIMESTAMP),
        @WritesAttribute(attribute = ATTR_NAME_LENGTH, description = ATTR_DESCRIPTION_LENGTH),
        @WritesAttribute(attribute = ATTR_NAME_ERROR_CODE, description = ATTR_DESCRIPTION_ERROR_CODE),
        @WritesAttribute(attribute = ATTR_NAME_IGNORED, description = ATTR_DESCRIPTION_IGNORED)})
public class CopyAzureBlobStorage_v12 extends AbstractAzureBlobProcessor_v12 {
    private final static int GENERATE_SAS_EXPIRY_HOURS = 24;

    public static final PropertyDescriptor SOURCE_STORAGE_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("Source Storage Credentials")
            .displayName("Source Storage Credentials")
            .description("Credentials Service used to obtain Azure Blob Storage Credentials to read Source Blob information")
            .identifiesControllerService(AzureStorageCredentialsService_v12.class)
            .required(true)
            .build();

    public static final PropertyDescriptor SOURCE_CONTAINER_NAME = new PropertyDescriptor.Builder()
            .name("Source Container Name")
            .displayName("Source Container Name")
            .description("Name of the Azure storage container that will be copied")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor SOURCE_BLOB_NAME = new PropertyDescriptor.Builder()
            .name("Source Blob Name")
            .displayName("Source Blob Name")
            .description("Name of the Azure blob that will be copied")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .description("The full name of the source blob")
            .build();

    public static final PropertyDescriptor DESTINATION_STORAGE_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BLOB_STORAGE_CREDENTIALS_SERVICE)
            .displayName("Destination Storage Credentials")
            .build();

    public static final PropertyDescriptor DESTINATION_CONTAINER_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.CONTAINER)
            .displayName("Destination Container Name")
            .description("Name of the Azure storage container destination defaults to the Source Container Name when not specified")
            .required(false)
            .build();

    public static final PropertyDescriptor DESTINATION_BLOB_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BLOB_NAME)
            .displayName("Destination Blob Name")
            .description("The full name of the destination blob defaults to the Source Blob Name when not specified")
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SOURCE_STORAGE_CREDENTIALS_SERVICE,
            SOURCE_CONTAINER_NAME,
            SOURCE_BLOB_NAME,
            DESTINATION_STORAGE_CREDENTIALS_SERVICE,
            DESTINATION_CONTAINER_NAME,
            DESTINATION_BLOB_NAME,
            AzureStorageUtils.CONFLICT_RESOLUTION,
            AzureStorageUtils.CREATE_CONTAINER,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String sourceContainerName = context.getProperty(SOURCE_CONTAINER_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String sourceBlobName = context.getProperty(SOURCE_BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationContainerName = Optional.ofNullable(
                context.getProperty(DESTINATION_CONTAINER_NAME).evaluateAttributeExpressions(flowFile).getValue()
        ).orElse(sourceContainerName);
        final String destinationBlobName = Optional.ofNullable(
                context.getProperty(DESTINATION_BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue()
        ).orElse(sourceBlobName);

        final boolean createContainer = context.getProperty(AzureStorageUtils.CREATE_CONTAINER).asBoolean();
        final AzureStorageConflictResolutionStrategy conflictResolution = context.getProperty(AzureStorageUtils.CONFLICT_RESOLUTION).asAllowableValue(AzureStorageConflictResolutionStrategy.class);

        final long startNanos = System.nanoTime();
        try {
            final BlobServiceClient destinationServiceClient = getStorageClient(context, DESTINATION_STORAGE_CREDENTIALS_SERVICE, flowFile);
            final BlobContainerClient destinationContainerClient = destinationServiceClient.getBlobContainerClient(destinationContainerName);
            if (createContainer && !destinationContainerClient.exists()) {
                destinationContainerClient.create();
            }

            final BlobClient destinationBlobClient = destinationContainerClient.getBlobClient(destinationBlobName);
            final Map<String, String> attributes = new LinkedHashMap<>();
            applyStandardBlobAttributes(attributes, destinationBlobClient);
            final boolean ignoreStrategyEnabled = conflictResolution == AzureStorageConflictResolutionStrategy.IGNORE_RESOLUTION;

            final BlobRequestConditions destinationRequestConditions = new BlobRequestConditions();

            try {
                if (conflictResolution != AzureStorageConflictResolutionStrategy.REPLACE_RESOLUTION) {
                    destinationRequestConditions.setIfNoneMatch("*");
                }

                final AzureStorageCredentialsService_v12 sourceCredentialsService = getCopyFromCredentialsService(context);
                final BlobServiceClient sourceServiceClient = getStorageClient(context, SOURCE_STORAGE_CREDENTIALS_SERVICE, flowFile);
                final BlobContainerClient sourceContainerClient = sourceServiceClient.getBlobContainerClient(sourceContainerName);
                final BlobClient sourceBlobClient = sourceContainerClient.getBlobClient(sourceBlobName);

                AzureStorageCredentialsDetails_v12 sourceCredentialsDetails = sourceCredentialsService.getCredentialsDetails(flowFile.getAttributes());
                String sourceUrl = sourceBlobClient.getBlobUrl();
                final BlobProperties sourceBlobProperties = sourceBlobClient.getProperties();
                final long blobSize = sourceBlobProperties.getBlobSize();

                final BlobRequestConditions sourceRequestConditions = new BlobRequestConditions();
                sourceRequestConditions.setIfMatch(sourceBlobProperties.getETag());

                final HttpAuthorization httpAuthorization;
                final String sasToken = (sourceCredentialsDetails.getCredentialsType() == AzureStorageCredentialsType.ACCOUNT_KEY)
                        ? generateSas(sourceContainerClient)
                        : sourceCredentialsDetails.getSasToken();
                if (sasToken == null) {
                    httpAuthorization = getHttpAuthorization(sourceCredentialsDetails);
                } else {
                    sourceUrl += "?" + sasToken;
                    httpAuthorization = null;
                }

                copy(destinationBlobClient, httpAuthorization, sourceUrl, blobSize, sourceRequestConditions, destinationRequestConditions);
                applyBlobMetadata(attributes, destinationBlobClient);

                if (ignoreStrategyEnabled) {
                    attributes.put(ATTR_NAME_IGNORED, Boolean.FALSE.toString());
                }
            } catch (BlobStorageException e) {
                final BlobErrorCode errorCode = e.getErrorCode();
                flowFile = session.putAttribute(flowFile, ATTR_NAME_ERROR_CODE, e.getErrorCode().toString());

                if (errorCode == BlobErrorCode.BLOB_ALREADY_EXISTS && ignoreStrategyEnabled) {
                    getLogger().info("Blob already exists: remote blob not modified. Transferring {} to success", flowFile);
                    attributes.put(ATTR_NAME_IGNORED, Boolean.TRUE.toString());
                } else {
                    throw e;
                }
            }

            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);

            long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            String transitUri = attributes.get(ATTR_NAME_PRIMARY_URI);
            session.getProvenanceReporter().send(flowFile, transitUri, transferMillis);
        } catch (Exception e) {
            getLogger().error("Failed to create blob on Azure Blob Storage", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void copy(final BlobClient destinationBlobClient,
                      final HttpAuthorization httpAuthorization,
                      final String sourceUrl,
                      final long blobSize,
                      final BlobRequestConditions sourceRequestConditions,
                      final BlobRequestConditions destinationRequestConditions) {
        final BlockBlobClient blockBlobClient = destinationBlobClient.getBlockBlobClient();

        // If the blob size is below the limit, use the one-shot upload endpoint
        if (blobSize < MAX_UPLOAD_BLOB_BYTES_LONG) {
            final BlobUploadFromUrlOptions options = new BlobUploadFromUrlOptions(sourceUrl);
            if (httpAuthorization != null) {
                options.setSourceAuthorization(httpAuthorization);
            }
            options.setSourceRequestConditions(sourceRequestConditions);
            options.setDestinationRequestConditions(destinationRequestConditions);
            blockBlobClient.uploadFromUrlWithResponse(options, null, Context.NONE);
            return;
        }

        final DecimalFormat df = new DecimalFormat("0000000");
        long offset = 0;
        int blockId = 1;
        final List<String> blockIds = new ArrayList<>();

        // Upload each block in sequential chunks
        while (true) {
            long count = Math.min(blobSize - offset, MAX_STAGE_BLOCK_BYTES_LONG);
            if (count == 0) {
                break;
            }

            // The zero-padded block ID must be base64-encoded as per the protocol
            final String zeroPadded = df.format(blockId);
            final String base64BlockId = Base64.getEncoder().encodeToString(zeroPadded.getBytes());

            BlockBlobStageBlockFromUrlOptions blockBlobStageBlockFromUrlOptions = new BlockBlobStageBlockFromUrlOptions(base64BlockId, sourceUrl);
            blockBlobStageBlockFromUrlOptions.setSourceRange(new BlobRange(offset, count));

            if (httpAuthorization != null) {
                blockBlobStageBlockFromUrlOptions.setSourceAuthorization(httpAuthorization);
            }
            blockBlobStageBlockFromUrlOptions.setSourceRequestConditions(sourceRequestConditions);
            final int statusCode = blockBlobClient.stageBlockFromUrlWithResponse(blockBlobStageBlockFromUrlOptions, null, Context.NONE).getStatusCode();
            if (statusCode != HTTP_ACCEPTED) {
                throw new ProcessException(String.format("Failed staging one or more blocks: HTTP %d", statusCode));
            }
            blockIds.add(base64BlockId);
            offset += count;
            blockId++;
        }

        final BlockBlobCommitBlockListOptions options = new BlockBlobCommitBlockListOptions(blockIds);
        options.setRequestConditions(destinationRequestConditions);
        final Response<BlockBlobItem> response = blockBlobClient.commitBlockListWithResponse(options, null, Context.NONE);
        final int statusCode = response.getStatusCode();
        if (statusCode != HTTP_ACCEPTED) {
            throw new ProcessException(String.format("Failed committing block list: HTTP %d", statusCode));
        }
    }

    private static String generateSas(final BlobContainerClient sourceContainerClient) {
        final BlobContainerSasPermission permissions = new BlobContainerSasPermission().setCreatePermission(true).setWritePermission(true).setAddPermission(true).setReadPermission(true);
        final OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        final OffsetDateTime expiryTime = now.plusHours(GENERATE_SAS_EXPIRY_HOURS);
        final BlobServiceSasSignatureValues signatureValues = new BlobServiceSasSignatureValues(expiryTime, permissions);
        return sourceContainerClient.generateSas(signatureValues);
    }

    private static AzureStorageCredentialsService_v12 getCopyFromCredentialsService(ProcessContext context) {
        return context.getProperty(SOURCE_STORAGE_CREDENTIALS_SERVICE).asControllerService(AzureStorageCredentialsService_v12.class);
    }

    private static HttpAuthorization getHttpAuthorization(final AzureStorageCredentialsDetails_v12 credentialsDetails) {
        switch (credentialsDetails.getCredentialsType()) {
            case ACCESS_TOKEN -> {
                TokenCredential credential = tokenRequestContext -> Mono.just(credentialsDetails.getAccessToken());
                return getHttpAuthorizationFromTokenCredential(credential);
            }
            case MANAGED_IDENTITY -> {
                final ManagedIdentityCredential credential = new ManagedIdentityCredentialBuilder()
                        .clientId(credentialsDetails.getManagedIdentityClientId())
                        .build();
                return getHttpAuthorizationFromTokenCredential(credential);
            }
            case SERVICE_PRINCIPAL -> {
                final ClientSecretCredential credential = new ClientSecretCredentialBuilder().clientId(
                                credentialsDetails.getServicePrincipalClientId()
                        ).clientSecret(credentialsDetails.getServicePrincipalClientSecret())
                        .tenantId(credentialsDetails.getServicePrincipalTenantId()).build();
                return getHttpAuthorizationFromTokenCredential(credential);
            }
        }
        return null;
    }

    private static HttpAuthorization getHttpAuthorizationFromTokenCredential(final TokenCredential credential) {
        final TokenRequestContext tokenRequestContext = new TokenRequestContext();
        tokenRequestContext.setScopes(Collections.singletonList(STORAGE_SCOPE));
        final AccessToken accessToken = credential.getToken(tokenRequestContext).block();
        if (accessToken == null) {
            throw new IllegalStateException("Storage Access Token not retrieved");
        }
        final String token = accessToken.getToken();
        return new HttpAuthorization("Bearer", token);
    }
}
