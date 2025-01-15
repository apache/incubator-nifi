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
package org.apache.nifi.services.azure.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.fileresource.service.api.FileResourceService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor_v12;
import org.apache.nifi.processors.azure.storage.FetchAzureBlobStorage_v12;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.BlobServiceClientFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.BLOB_STORAGE_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.getProxyOptions;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_CONTAINER;
import static org.apache.nifi.util.StringUtils.isBlank;

@Tags({"azure", "microsoft", "cloud", "storage", "file", "resource", "blob"})
@SeeAlso({FetchAzureBlobStorage_v12.class})
@CapabilityDescription("Provides an Azure Blob Storage file resource for other components.")
@UseCase(
        description = "Fetch a specific file from Azure Blob Storage." +
                " The service provides higher performance compared to fetch processors when the data should be moved between different storages without any transformation.",
        configuration = """
                "Container Name" = "${azure.container}"
                "Blob Name" = "${azure.blobname}"

                The "Storage Credentials" property should specify an instance of the AzureStorageCredentialsService_v12 in order to provide credentials for accessing the storage container.
                """
)
public class AzureBlobStorageFileResourceService extends AbstractControllerService implements FileResourceService {

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.CONTAINER)
            .defaultValue(String.format("${%s}", ATTR_NAME_CONTAINER))
            .build();

    public static final PropertyDescriptor BLOB_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractAzureBlobProcessor_v12.BLOB_NAME)
            .defaultValue(String.format("${%s}", ATTR_NAME_BLOBNAME))
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BLOB_STORAGE_CREDENTIALS_SERVICE,
            CONTAINER,
            BLOB_NAME
    );

    private volatile BlobServiceClientFactory clientFactory;
    private volatile ConfigurationContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.clientFactory = new BlobServiceClientFactory(getLogger(), getProxyOptions(context));
        this.context = context;
    }

    @OnDisabled
    public void onDisabled() {
        this.clientFactory = null;
        this.context = null;
    }

    @Override
    public FileResource getFileResource(Map<String, String> attributes) {
        final BlobServiceClient client = getStorageClient(attributes);
        try {
            return fetchBlob(client, attributes);
        } catch (final BlobStorageException | IOException e) {
            throw new ProcessException("Failed to fetch blob from Azure Blob Storage", e);
        }
    }

    protected BlobServiceClient getStorageClient(Map<String, String> attributes) {
        final AzureStorageCredentialsService_v12 credentialsService = context.getProperty(BLOB_STORAGE_CREDENTIALS_SERVICE)
                .asControllerService(AzureStorageCredentialsService_v12.class);
        return clientFactory.getStorageClient(credentialsService.getCredentialsDetails(attributes));
    }

    /**
     * Fetching blob from the provided container.
     *
     * @param storageClient azure blob storage client
     * @param attributes configuration attributes
     * @return fetched blob as FileResource
     * @throws IOException exception caused by missing parameters or blob not found
     */
    private FileResource fetchBlob(final BlobServiceClient storageClient, final Map<String, String> attributes) throws IOException {
        final String containerName = context.getProperty(CONTAINER).evaluateAttributeExpressions(attributes).getValue();
        final String blobName = context.getProperty(BLOB_NAME).evaluateAttributeExpressions(attributes).getValue();

        if (isBlank(containerName) || isBlank(blobName)) {
            throw new ProcessException("Container name and blob name cannot be empty");
        }

        final BlobContainerClient containerClient = storageClient.getBlobContainerClient(containerName);
        final BlobClient blobClient = containerClient.getBlobClient(blobName);
        if (!blobClient.exists()) {
            throw new ProcessException(String.format("Blob %s/%s not found", containerName, blobName));
        }
        return new FileResource(blobClient.openInputStream(), blobClient.getProperties().getBlobSize());
    }
}
