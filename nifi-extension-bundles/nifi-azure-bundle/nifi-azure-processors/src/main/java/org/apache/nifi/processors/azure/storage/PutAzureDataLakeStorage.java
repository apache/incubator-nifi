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
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.options.DataLakeFileFlushOptions;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.DirectoryValidator;
import org.apache.nifi.processors.azure.storage.utils.WritingStrategy;
import org.apache.nifi.processors.transfer.ResourceTransferSource;
import org.apache.nifi.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.ADLS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.FILE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateDirectoryProperty;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateFileProperty;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateFileSystemProperty;
import static org.apache.nifi.processors.transfer.ResourceTransferProperties.FILE_RESOURCE_SERVICE;
import static org.apache.nifi.processors.transfer.ResourceTransferProperties.RESOURCE_TRANSFER_SOURCE;
import static org.apache.nifi.processors.transfer.ResourceTransferUtils.getFileResource;

@Tags({"azure", "microsoft", "cloud", "storage", "adlsgen2", "datalake"})
@SeeAlso({DeleteAzureDataLakeStorage.class, FetchAzureDataLakeStorage.class, ListAzureDataLakeStorage.class})
@CapabilityDescription("Writes the contents of a FlowFile as a file on Azure Data Lake Storage Gen 2")
@WritesAttributes({@WritesAttribute(attribute = ATTR_NAME_FILESYSTEM, description = ATTR_DESCRIPTION_FILESYSTEM),
        @WritesAttribute(attribute = ATTR_NAME_DIRECTORY, description = ATTR_DESCRIPTION_DIRECTORY),
        @WritesAttribute(attribute = ATTR_NAME_FILENAME, description = ATTR_DESCRIPTION_FILENAME),
        @WritesAttribute(attribute = ATTR_NAME_PRIMARY_URI, description = ATTR_DESCRIPTION_PRIMARY_URI),
        @WritesAttribute(attribute = ATTR_NAME_LENGTH, description = ATTR_DESCRIPTION_LENGTH)})
@InputRequirement(Requirement.INPUT_REQUIRED)
public class PutAzureDataLakeStorage extends AbstractAzureDataLakeStorageProcessor {

    public static final String FAIL_RESOLUTION = "fail";
    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";

    public static long MAX_CHUNK_SIZE = 100 * 1024 * 1024; // current chunk limit is 100 MiB on Azure

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("conflict-resolution-strategy")
            .displayName("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(FAIL_RESOLUTION, REPLACE_RESOLUTION, IGNORE_RESOLUTION)
            .build();

    protected static final PropertyDescriptor WRITING_STRATEGY = new PropertyDescriptor.Builder()
            .name("writing-strategy")
            .displayName("Writing Strategy")
            .description("Defines the approach for writing the Azure file.")
            .required(true)
            .allowableValues(WritingStrategy.class)
            .defaultValue(WritingStrategy.WRITE_AND_RENAME)
            .build();

    public static final PropertyDescriptor BASE_TEMPORARY_PATH = new PropertyDescriptor.Builder()
            .name("base-temporary-path")
            .displayName("Base Temporary Path")
            .description("The Path where the temporary directory will be created. The Path name cannot contain a leading '/'." +
                    " The root directory can be designated by the empty string value. Non-existing directories will be created." +
                    "The Temporary File Directory name is " + TEMP_FILE_DIRECTORY)
            .defaultValue("")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new DirectoryValidator("Base Temporary Path"))
            .dependsOn(WRITING_STRATEGY, WritingStrategy.WRITE_AND_RENAME)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ADLS_CREDENTIALS_SERVICE,
            FILESYSTEM,
            DIRECTORY,
            FILE,
            WRITING_STRATEGY,
            BASE_TEMPORARY_PATH,
            CONFLICT_RESOLUTION,
            RESOURCE_TRANSFER_SOURCE,
            FILE_RESOURCE_SERVICE,
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

        final long startNanos = System.nanoTime();
        try {
            final String fileSystem = evaluateFileSystemProperty(FILESYSTEM, context, flowFile);
            final String directory = evaluateDirectoryProperty(DIRECTORY, context, flowFile);
            final String fileName = evaluateFileProperty(context, flowFile);

            final DataLakeFileSystemClient fileSystemClient = getFileSystemClient(context, flowFile, fileSystem);
            final DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(directory);

            final WritingStrategy writingStrategy = context.getProperty(WRITING_STRATEGY).asAllowableValue(WritingStrategy.class);
            final String conflictResolution = context.getProperty(CONFLICT_RESOLUTION).getValue();
            final ResourceTransferSource resourceTransferSource = ResourceTransferSource.valueOf(context.getProperty(RESOURCE_TRANSFER_SOURCE).getValue());
            final Optional<FileResource> fileResourceFound = getFileResource(resourceTransferSource, context, flowFile.getAttributes());
            final long transferSize = fileResourceFound.map(FileResource::getSize).orElse(flowFile.getSize());

            final DataLakeFileClient fileClient;

            if (writingStrategy == WritingStrategy.WRITE_AND_RENAME) {
                final String tempPath = evaluateDirectoryProperty(BASE_TEMPORARY_PATH, context, flowFile);
                final String tempDirectory = createPath(tempPath, TEMP_FILE_DIRECTORY);
                final String tempFilePrefix = UUID.randomUUID().toString();

                final DataLakeDirectoryClient tempDirectoryClient = fileSystemClient.getDirectoryClient(tempDirectory);
                final DataLakeFileClient tempFileClient = tempDirectoryClient.createFile(tempFilePrefix + fileName, true);

                uploadFile(session, flowFile, fileResourceFound, transferSize, tempFileClient);

                createDirectoryIfNotExists(directoryClient);

                fileClient = renameFile(tempFileClient, directoryClient.getDirectoryPath(), fileName, conflictResolution);
            } else {
                fileClient = createFile(directoryClient, fileName, conflictResolution);

                if (fileClient != null) {
                    uploadFile(session, flowFile, fileResourceFound, transferSize, fileClient);
                }
            }

            if (fileClient != null) {
                final String fileUrl = fileClient.getFileUrl();
                final Map<String, String> attributes = createAttributeMap(fileSystem, directory, fileName, fileUrl, transferSize);
                flowFile = session.putAllAttributes(flowFile, attributes);

                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().send(flowFile, fileUrl, transferMillis);
            }

            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to create file on Azure Data Lake Storage", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private DataLakeFileSystemClient getFileSystemClient(final ProcessContext context, final FlowFile flowFile, final String fileSystem) {
        final DataLakeServiceClient storageClient = getStorageClient(context, flowFile);
        return storageClient.getFileSystemClient(fileSystem);
    }

    private Map<String, String> createAttributeMap(final String fileSystem, final String originalDirectory, final String fileName, final String fileUrl, final long length) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_NAME_FILESYSTEM, fileSystem);
        attributes.put(ATTR_NAME_DIRECTORY, originalDirectory);
        attributes.put(ATTR_NAME_FILENAME, fileName);
        attributes.put(ATTR_NAME_PRIMARY_URI, fileUrl);
        attributes.put(ATTR_NAME_LENGTH, String.valueOf(length));
        return attributes;
    }

    private void createDirectoryIfNotExists(final DataLakeDirectoryClient directoryClient) {
        if (!directoryClient.getDirectoryPath().isEmpty() && !directoryClient.exists()) {
            directoryClient.create();
        }
    }

    private void uploadFile(final ProcessSession session, final FlowFile flowFile, final Optional<FileResource> fileResourceFound,
                            final long transferSize, final DataLakeFileClient fileClient) throws Exception {
        try (final InputStream inputStream = new BufferedInputStream(
                fileResourceFound.map(FileResource::getInputStream)
                        .orElseGet(() -> session.read(flowFile)))
        ) {
            uploadContent(fileClient, inputStream, transferSize);
        } catch (final Exception e) {
            removeFile(fileClient);
            throw e;
        }
    }

    private static void uploadContent(final DataLakeFileClient fileClient, final InputStream in, final long length) throws IOException  {
        long chunkStart = 0;
        long chunkSize;

        while (chunkStart < length) {
            chunkSize = Math.min(length - chunkStart, MAX_CHUNK_SIZE);

            // com.azure.storage.common.Utility.convertStreamToByteBuffer() throws an exception
            // if there are more available bytes in the stream after reading the chunk
            BoundedInputStream boundedIn = BoundedInputStream.builder()
                    .setInputStream(in)
                    .setMaxCount(chunkSize)
                    .get();

            fileClient.append(boundedIn, chunkStart, chunkSize);

            chunkStart += chunkSize;
        }

        fileClient.flushWithResponse(length, new DataLakeFileFlushOptions().setClose(true), null, Context.NONE);
    }

    /**
     * Creates the file on Azure for 'Simple Write' strategy. Upon upload, a 0-byte file is created, then the payload is appended to it.
     * Because of that, a work-in-progress file is available for readers before the upload is complete.
     *
     * @param directoryClient directory client of the uploaded file's parent directory
     * @param fileName name of the uploaded file
     * @param conflictResolution conflict resolution strategy
     * @return the file client of the uploaded file or {@code null} if the file already exists and conflict resolution strategy is 'ignore'
     * @throws ProcessException if the file already exists and the conflict resolution strategy is 'fail'; also in case of other errors
     */
    private DataLakeFileClient createFile(DataLakeDirectoryClient directoryClient, final String fileName, final String conflictResolution) {
        final String destinationPath = createPath(directoryClient.getDirectoryPath(), fileName);

        try {
            final boolean overwrite = conflictResolution.equals(REPLACE_RESOLUTION);
            return directoryClient.createFile(fileName, overwrite);
        } catch (DataLakeStorageException dataLakeStorageException) {
            return handleDataLakeStorageException(dataLakeStorageException, destinationPath, conflictResolution);
        }
    }

    /**
     * This method serves as a "commit" for the upload process in case of 'Write and Rename' strategy. In order to prevent work-in-progress files from being available for readers,
     * a temporary file is written first, and then renamed/moved to its final destination. It is not an efficient approach in case of conflicts because FlowFiles are uploaded unnecessarily,
     * but it is a calculated risk because consistency is more important for 'Write and Rename' strategy.
     * <p>
     * Visible for testing
     *
     * @param sourceFileClient file client of the temporary file
     * @param destinationDirectory final location of the uploaded file
     * @param destinationFileName final name of the uploaded file
     * @param conflictResolution conflict resolution strategy
     * @return the file client of the uploaded file or {@code null} if the file already exists and conflict resolution strategy is 'ignore'
     * @throws ProcessException if the file already exists and the conflict resolution strategy is 'fail'; also in case of other errors
     */
    DataLakeFileClient renameFile(final DataLakeFileClient sourceFileClient, final String destinationDirectory, final String destinationFileName, final String conflictResolution) {
        final String destinationPath = createPath(destinationDirectory, destinationFileName);

        try {
            final DataLakeRequestConditions destinationCondition = new DataLakeRequestConditions();
            if (!conflictResolution.equals(REPLACE_RESOLUTION)) {
                destinationCondition.setIfNoneMatch("*");
            }
            return sourceFileClient.renameWithResponse(null, destinationPath, null, destinationCondition, null, null).getValue();
        } catch (DataLakeStorageException dataLakeStorageException) {
            removeFile(sourceFileClient);

            return handleDataLakeStorageException(dataLakeStorageException, destinationPath, conflictResolution);
        }
    }

    private DataLakeFileClient handleDataLakeStorageException(final DataLakeStorageException dataLakeStorageException, final String destinationPath, final String conflictResolution) {
        final boolean fileAlreadyExists = dataLakeStorageException.getStatusCode() == 409;
        if (fileAlreadyExists && conflictResolution.equals(IGNORE_RESOLUTION)) {
            getLogger().info("File [{}] already exists. Remote file not modified due to {} being set to '{}'.",
                    destinationPath, CONFLICT_RESOLUTION.getDisplayName(), conflictResolution);
            return null;
        } else if (fileAlreadyExists && conflictResolution.equals(FAIL_RESOLUTION)) {
            throw new ProcessException(String.format("File [%s] already exists.", destinationPath), dataLakeStorageException);
        } else {
            throw new ProcessException(String.format("File operation failed [%s]", destinationPath), dataLakeStorageException);
        }
    }

    private String createPath(final String baseDirectory, final String path) {
        return StringUtils.isNotBlank(baseDirectory)
                ? baseDirectory + "/" + path
                : path;
    }

    private void removeFile(final DataLakeFileClient fileClient) {
        try {
            fileClient.delete();
        } catch (Exception e) {
            getLogger().error("Renaming File [{}] failed", fileClient.getFileName(), e);
        }
    }
}
