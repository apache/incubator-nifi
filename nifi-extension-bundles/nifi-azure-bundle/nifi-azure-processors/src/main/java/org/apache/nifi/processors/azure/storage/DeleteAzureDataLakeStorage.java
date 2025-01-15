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

import com.azure.core.util.Context;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.time.Duration;
import java.util.List;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.ADLS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateDirectoryProperty;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateFileProperty;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateFileSystemProperty;

@Tags({"azure", "microsoft", "cloud", "storage", "adlsgen2", "datalake"})
@SeeAlso({PutAzureDataLakeStorage.class, FetchAzureDataLakeStorage.class, ListAzureDataLakeStorage.class})
@CapabilityDescription("Deletes the provided file from Azure Data Lake Storage")
@InputRequirement(Requirement.INPUT_REQUIRED)
public class DeleteAzureDataLakeStorage extends AbstractAzureDataLakeStorageProcessor {

    public static final AllowableValue FS_TYPE_FILE = new AllowableValue("file", "File", "The object to be deleted is a file.");
    public static final AllowableValue FS_TYPE_DIRECTORY = new AllowableValue("directory", "Directory", "The object to be deleted is a directory.");

    public static final PropertyDescriptor FILESYSTEM_OBJECT_TYPE = new PropertyDescriptor.Builder()
            .name("filesystem-object-type")
            .displayName("Filesystem Object Type")
            .description("They type of the file system object to be deleted. It can be either folder or file.")
            .allowableValues(FS_TYPE_FILE, FS_TYPE_DIRECTORY)
            .defaultValue(FS_TYPE_FILE)
            .required(true)
            .build();

    public static final PropertyDescriptor FILE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.FILE)
            .dependsOn(FILESYSTEM_OBJECT_TYPE, FS_TYPE_FILE)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ADLS_CREDENTIALS_SERVICE,
            FILESYSTEM,
            FILESYSTEM_OBJECT_TYPE,
            DIRECTORY,
            FILE,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE
    );

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            final boolean isFile = context.getProperty(FILESYSTEM_OBJECT_TYPE).getValue().equals(FS_TYPE_FILE.getValue());
            final DataLakeServiceClient storageClient = getStorageClient(context, flowFile);

            final String fileSystem = evaluateFileSystemProperty(FILESYSTEM, context, flowFile);
            final DataLakeFileSystemClient fileSystemClient = storageClient.getFileSystemClient(fileSystem);

            final String directory = evaluateDirectoryProperty(DIRECTORY, context, flowFile);
            final DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(directory);

            if (isFile) {
                final String fileName = evaluateFileProperty(context, flowFile);
                final DataLakeFileClient fileClient = directoryClient.getFileClient(fileName);
                fileClient.delete();
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().invokeRemoteProcess(flowFile, fileClient.getFileUrl(), "File deleted");
            } else {
                directoryClient.deleteWithResponse(true, new DataLakeRequestConditions(), Duration.ofSeconds(10), Context.NONE);
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().invokeRemoteProcess(flowFile, directoryClient.getDirectoryUrl(), "Directory deleted");
            }
        } catch (Exception e) {
            getLogger().error("Failed to delete the specified file from Azure Data Lake Storage", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }
}