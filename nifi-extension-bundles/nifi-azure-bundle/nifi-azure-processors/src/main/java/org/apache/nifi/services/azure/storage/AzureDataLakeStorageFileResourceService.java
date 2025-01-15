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

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
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
import org.apache.nifi.processors.azure.storage.FetchAzureDataLakeStorage;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.DataLakeServiceClientFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.ADLS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.FILE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateDirectoryProperty;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateFileProperty;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateFileSystemProperty;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.getProxyOptions;

@Tags({"azure", "microsoft", "cloud", "storage", "adlsgen2", "file", "resource", "datalake"})
@SeeAlso({FetchAzureDataLakeStorage.class})
@CapabilityDescription("Provides an Azure Data Lake Storage (ADLS) file resource for other components.")
@UseCase(
        description = "Fetch the specified file from Azure Data Lake Storage." +
                " The service provides higher performance compared to fetch processors when the data should be moved between different storages without any transformation.",
        configuration = """
                "Filesystem Name" = "${azure.filesystem}"
                "Directory Name" = "${azure.directory}"
                "File Name" = "${azure.filename}"

                The "ADLS Credentials" property should specify an instance of the ADLSCredentialsService in order to provide credentials for accessing the filesystem.
                """
)
public class AzureDataLakeStorageFileResourceService extends AbstractControllerService implements FileResourceService {

    public static final PropertyDescriptor FILESYSTEM = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.FILESYSTEM)
            .defaultValue(String.format("${%s}", ATTR_NAME_FILESYSTEM))
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.DIRECTORY)
            .defaultValue(String.format("${%s}", ATTR_NAME_DIRECTORY))
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ADLS_CREDENTIALS_SERVICE,
            FILESYSTEM,
            DIRECTORY,
            FILE
    );

    private volatile DataLakeServiceClientFactory clientFactory;
    private volatile ConfigurationContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.clientFactory = new DataLakeServiceClientFactory(getLogger(), getProxyOptions(context));
        this.context = context;
    }

    @OnDisabled
    public void onDisabled() {
        this.clientFactory = null;
        this.context = null;
    }

    @Override
    public FileResource getFileResource(Map<String, String> attributes) {
        final DataLakeServiceClient client = getStorageClient(attributes);
        try {
            return fetchFile(client, attributes);
        } catch (final DataLakeStorageException | IOException e) {
            throw new ProcessException("Failed to fetch file from ADLS Storage", e);
        }
    }

    protected DataLakeServiceClient getStorageClient(Map<String, String> attributes) {
        final ADLSCredentialsService credentialsService = context.getProperty(ADLS_CREDENTIALS_SERVICE)
                .asControllerService(ADLSCredentialsService.class);
        return clientFactory.getStorageClient(credentialsService.getCredentialsDetails(attributes));
    }

    /**
     * Fetching file from the provided filesystem and directory in ADLS.
     *
     * @param storageClient azure data lake service client
     * @param attributes configuration attributes
     * @return fetched file as FileResource
     * @throws IOException exception caused by missing parameters or blob not found
     */
    private FileResource fetchFile(final DataLakeServiceClient storageClient, final Map<String, String> attributes) throws IOException {
        final String fileSystem = evaluateFileSystemProperty(FILESYSTEM, context, attributes);
        final String directory = evaluateDirectoryProperty(DIRECTORY, context, attributes);
        final String file = evaluateFileProperty(context, attributes);

        final DataLakeFileSystemClient fileSystemClient = storageClient.getFileSystemClient(fileSystem);
        final DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(directory);
        final DataLakeFileClient fileClient = directoryClient.getFileClient(file);

        if (fileClient.getProperties().isDirectory()) {
            throw new ProcessException(FILE.getDisplayName() + " (" + file + ") points to a directory. Full path: " + fileClient.getFilePath());
        }

        if (!fileClient.exists()) {
            throw new ProcessException(String.format("File %s/%s not found in file system: %s", directory, file, fileSystem));
        }

        return new FileResource(fileClient.openInputStream().getInputStream(),
                fileClient.getProperties().getFileSize());
    }
}
