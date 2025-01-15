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

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import org.apache.nifi.annotation.behavior.InputRequirement;
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
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.DirectoryValidator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_SOURCE_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_SOURCE_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_SOURCE_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_SOURCE_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.ADLS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.FILE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateDirectoryProperty;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateFileProperty;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateFileSystemProperty;

@Tags({"azure", "microsoft", "cloud", "storage", "adlsgen2", "datalake"})
@SeeAlso({DeleteAzureDataLakeStorage.class, FetchAzureDataLakeStorage.class, ListAzureDataLakeStorage.class})
@CapabilityDescription("Moves content within an Azure Data Lake Storage Gen 2." +
        " After the move, files will be no longer available on source location.")
@WritesAttributes({@WritesAttribute(attribute = ATTR_NAME_SOURCE_FILESYSTEM, description = ATTR_DESCRIPTION_SOURCE_FILESYSTEM),
        @WritesAttribute(attribute = ATTR_NAME_SOURCE_DIRECTORY, description = ATTR_DESCRIPTION_SOURCE_DIRECTORY),
        @WritesAttribute(attribute = ATTR_NAME_FILESYSTEM, description = ATTR_DESCRIPTION_FILESYSTEM),
        @WritesAttribute(attribute = ATTR_NAME_DIRECTORY, description = ATTR_DESCRIPTION_DIRECTORY),
        @WritesAttribute(attribute = ATTR_NAME_FILENAME, description = ATTR_DESCRIPTION_FILENAME),
        @WritesAttribute(attribute = ATTR_NAME_PRIMARY_URI, description = ATTR_DESCRIPTION_PRIMARY_URI),
        @WritesAttribute(attribute = ATTR_NAME_LENGTH, description = ATTR_DESCRIPTION_LENGTH)})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class MoveAzureDataLakeStorage extends AbstractAzureDataLakeStorageProcessor {

    public static final String FAIL_RESOLUTION = "fail";
    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("conflict-resolution-strategy")
            .displayName("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(FAIL_RESOLUTION, REPLACE_RESOLUTION, IGNORE_RESOLUTION)
            .build();

    public static final PropertyDescriptor SOURCE_FILESYSTEM = new PropertyDescriptor.Builder()
            .name("source-filesystem-name")
            .displayName("Source Filesystem")
            .description("Name of the Azure Storage File System from where the move should happen.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue(String.format("${%s}", ATTR_NAME_FILESYSTEM))
            .build();

    public static final PropertyDescriptor SOURCE_DIRECTORY = new PropertyDescriptor.Builder()
            .name("source-directory-name")
            .displayName("Source Directory")
            .description("Name of the Azure Storage Directory from where the move should happen. The Directory Name cannot contain a leading '/'." +
                    " The root directory can be designated by the empty string value.")
            .addValidator(new DirectoryValidator("Source Directory"))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .defaultValue(String.format("${%s}", ATTR_NAME_DIRECTORY))
            .build();

    public static final PropertyDescriptor DESTINATION_FILESYSTEM = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(FILESYSTEM)
            .displayName("Destination Filesystem")
            .description("Name of the Azure Storage File System where the files will be moved.")
            .build();

    public static final PropertyDescriptor DESTINATION_DIRECTORY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(DIRECTORY)
            .displayName("Destination Directory")
            .description("Name of the Azure Storage Directory where the files will be moved. The Directory Name cannot contain a leading '/'." +
                    " The root directory can be designated by the empty string value. Non-existing directories will be created." +
                    " If the original directory structure should be kept, the full directory path needs to be provided after the destination directory." +
                    " e.g.: destdir/${azure.directory}")
            .addValidator(new DirectoryValidator("Destination Directory"))
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ADLS_CREDENTIALS_SERVICE,
            SOURCE_FILESYSTEM,
            SOURCE_DIRECTORY,
            DESTINATION_FILESYSTEM,
            DESTINATION_DIRECTORY,
            FILE,
            CONFLICT_RESOLUTION,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        try {
            final String sourceFileSystem = evaluateFileSystemProperty(SOURCE_FILESYSTEM, context, flowFile);
            final String sourceDirectory = evaluateDirectoryProperty(SOURCE_DIRECTORY, context, flowFile);
            final String destinationFileSystem = evaluateFileSystemProperty(DESTINATION_FILESYSTEM, context, flowFile);
            final String destinationDirectory = evaluateDirectoryProperty(DESTINATION_DIRECTORY, context, flowFile);
            final String fileName = evaluateFileProperty(context, flowFile);

            final String destinationPath;
            if (!destinationDirectory.isEmpty() && !sourceDirectory.isEmpty()) {
                destinationPath = destinationDirectory + "/";
            } else {
                destinationPath = destinationDirectory;
            }

            final DataLakeServiceClient storageClient = getStorageClient(context, flowFile);
            final DataLakeFileSystemClient sourceFileSystemClient = storageClient.getFileSystemClient(sourceFileSystem);
            final DataLakeDirectoryClient sourceDirectoryClient = sourceFileSystemClient.getDirectoryClient(sourceDirectory);
            final DataLakeFileSystemClient destinationFileSystemClient = storageClient.getFileSystemClient(destinationFileSystem);
            final DataLakeDirectoryClient destinationDirectoryClient = destinationFileSystemClient.getDirectoryClient(destinationDirectory);
            DataLakeFileClient sourceFileClient = sourceDirectoryClient.getFileClient(fileName);
            final DataLakeRequestConditions sourceConditions = new DataLakeRequestConditions();
            final DataLakeRequestConditions destinationConditions = new DataLakeRequestConditions();
            final String conflictResolution = context.getProperty(CONFLICT_RESOLUTION).getValue();

            try {
                if (!destinationDirectory.isEmpty() && !destinationDirectoryClient.exists()) {
                    destinationDirectoryClient.create();
                }

                if (!conflictResolution.equals(REPLACE_RESOLUTION)) {
                    destinationConditions.setIfNoneMatch("*");
                }

                final DataLakeFileClient destinationFileClient = sourceFileClient.renameWithResponse(destinationFileSystem,
                                destinationPath + fileName,
                                sourceConditions,
                                destinationConditions,
                                null,
                                null)
                        .getValue();

                final Map<String, String> attributes = new HashMap<>();
                attributes.put(ATTR_NAME_SOURCE_FILESYSTEM, sourceFileSystem);
                attributes.put(ATTR_NAME_SOURCE_DIRECTORY, sourceDirectory);
                attributes.put(ATTR_NAME_FILESYSTEM, destinationFileSystem);
                attributes.put(ATTR_NAME_DIRECTORY, destinationDirectory);
                attributes.put(ATTR_NAME_FILENAME, fileName);
                attributes.put(ATTR_NAME_PRIMARY_URI, destinationFileClient.getFileUrl());
                attributes.put(ATTR_NAME_LENGTH, String.valueOf(destinationFileClient.getProperties().getFileSize()));
                flowFile = session.putAllAttributes(flowFile, attributes);

                session.transfer(flowFile, REL_SUCCESS);
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().send(flowFile, sourceFileClient.getFileUrl(), transferMillis);
            } catch (DataLakeStorageException dlsException) {
                if (dlsException.getStatusCode() == 409 && conflictResolution.equals(IGNORE_RESOLUTION)) {
                    session.transfer(flowFile, REL_SUCCESS);
                    String warningMessage = String.format("File with the same name already exists. " +
                            "Remote file not modified. " +
                            "Transferring {} to success due to %s being set to '%s'.", CONFLICT_RESOLUTION.getDisplayName(), conflictResolution);
                    getLogger().warn(warningMessage, flowFile);
                } else {
                    throw dlsException;
                }
            }
        } catch (Exception e) {
            getLogger().error("Failed to move file on Azure Data Lake Storage", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
