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

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlobType;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor_v12;
import org.apache.nifi.processors.azure.ClientSideEncryptionSupport;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.transfer.ResourceTransferSource;
import org.apache.nifi.services.azure.storage.AzureStorageConflictResolutionStrategy;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.azure.core.http.ContentType.APPLICATION_OCTET_STREAM;
import static com.azure.core.util.FluxUtil.toFluxByteBuffer;
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
import static org.apache.nifi.processors.transfer.ResourceTransferProperties.FILE_RESOURCE_SERVICE;
import static org.apache.nifi.processors.transfer.ResourceTransferProperties.RESOURCE_TRANSFER_SOURCE;
import static org.apache.nifi.processors.transfer.ResourceTransferUtils.getFileResource;

@Tags({"azure", "microsoft", "cloud", "storage", "blob"})
@SeeAlso({ListAzureBlobStorage_v12.class, FetchAzureBlobStorage_v12.class, DeleteAzureBlobStorage_v12.class,
        CopyAzureBlobStorage_v12.class})
@CapabilityDescription("Puts content into a blob on Azure Blob Storage. The processor uses Azure Blob Storage client library v12.")
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
public class PutAzureBlobStorage_v12 extends AbstractAzureBlobProcessor_v12 implements ClientSideEncryptionSupport {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BLOB_STORAGE_CREDENTIALS_SERVICE,
            AzureStorageUtils.CONTAINER,
            AzureStorageUtils.CREATE_CONTAINER,
            AzureStorageUtils.CONFLICT_RESOLUTION,
            BLOB_NAME,
            RESOURCE_TRANSFER_SOURCE,
            FILE_RESOURCE_SERVICE,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE,
            CSE_KEY_TYPE,
            CSE_KEY_ID,
            CSE_LOCAL_KEY
    );

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        results.addAll(validateClientSideEncryptionProperties(validationContext));
        return results;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String containerName = context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        final boolean createContainer = context.getProperty(AzureStorageUtils.CREATE_CONTAINER).asBoolean();
        final String blobName = context.getProperty(BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final AzureStorageConflictResolutionStrategy conflictResolution = context.getProperty(AzureStorageUtils.CONFLICT_RESOLUTION).asAllowableValue(AzureStorageConflictResolutionStrategy.class);
        final ResourceTransferSource resourceTransferSource = ResourceTransferSource.valueOf(context.getProperty(RESOURCE_TRANSFER_SOURCE).getValue());

        long startNanos = System.nanoTime();
        try {
            final Optional<FileResource> fileResourceFound = getFileResource(resourceTransferSource, context, flowFile.getAttributes());
            BlobServiceClient storageClient = getStorageClient(context, flowFile);
            BlobContainerClient containerClient = storageClient.getBlobContainerClient(containerName);
            if (createContainer && !containerClient.exists()) {
                containerClient.create();
            }

            final BlobClient blobClient;
            if (isClientSideEncryptionEnabled(context)) {
                blobClient = getEncryptedBlobClient(context, containerClient, blobName);
            } else {
                blobClient = containerClient.getBlobClient(blobName);
            }

            final BlobRequestConditions blobRequestConditions = new BlobRequestConditions();
            Map<String, String> attributes = new HashMap<>();
            applyStandardBlobAttributes(attributes, blobClient);
            final boolean ignore = conflictResolution == AzureStorageConflictResolutionStrategy.IGNORE_RESOLUTION;

            try {
                if (conflictResolution != AzureStorageConflictResolutionStrategy.REPLACE_RESOLUTION) {
                    blobRequestConditions.setIfNoneMatch("*");
                }

                final long transferSize = fileResourceFound.map(FileResource::getSize).orElse(flowFile.getSize());
                final FlowFile sourceFlowFile = flowFile;
                try (InputStream sourceInputStream = fileResourceFound
                        .map(FileResource::getInputStream)
                        .orElseGet(() -> session.read(sourceFlowFile))
                ) {
                    final BlobParallelUploadOptions blobParallelUploadOptions = new BlobParallelUploadOptions(toFluxByteBuffer(sourceInputStream, BlobClient.BLOB_DEFAULT_UPLOAD_BLOCK_SIZE));
                    blobParallelUploadOptions.setRequestConditions(blobRequestConditions);
                    Response<BlockBlobItem> response = blobClient.uploadWithResponse(blobParallelUploadOptions, null, Context.NONE);
                    BlockBlobItem blob = response.getValue();
                    applyUploadResultAttributes(attributes, blob, BlobType.BLOCK_BLOB, transferSize);
                    applyBlobMetadata(attributes, blobClient);
                    if (ignore) {
                        attributes.put(ATTR_NAME_IGNORED, "false");
                    }
                }
            } catch (BlobStorageException e) {
                final BlobErrorCode errorCode = e.getErrorCode();
                flowFile = session.putAttribute(flowFile, ATTR_NAME_ERROR_CODE, e.getErrorCode().toString());

                if (errorCode == BlobErrorCode.BLOB_ALREADY_EXISTS && ignore) {
                    getLogger().info("Blob already exists: remote blob not modified. Transferring {} to success", flowFile);
                    attributes.put(ATTR_NAME_IGNORED, "true");
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

    private static void applyUploadResultAttributes(final Map<String, String> attributes, final BlockBlobItem blob, final BlobType blobType, final long length) {
        attributes.put(ATTR_NAME_BLOBTYPE, blobType.toString());
        attributes.put(ATTR_NAME_ETAG, blob.getETag());
        attributes.put(ATTR_NAME_LENGTH, String.valueOf(length));
        attributes.put(ATTR_NAME_TIMESTAMP, String.valueOf(blob.getLastModified()));
        attributes.put(ATTR_NAME_LANG, null);
        attributes.put(ATTR_NAME_MIME_TYPE, APPLICATION_OCTET_STREAM);
    }
}
