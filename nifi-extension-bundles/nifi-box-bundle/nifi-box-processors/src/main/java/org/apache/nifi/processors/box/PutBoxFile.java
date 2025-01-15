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

package org.apache.nifi.processors.box;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static org.apache.nifi.processor.util.StandardValidators.createRegexMatchingValidator;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.PATH_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP_DESC;
import static org.apache.nifi.processors.box.BoxFileUtils.BOX_URL;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.IGNORE;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.REPLACE;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIException;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy;


@SeeAlso({ListBoxFile.class, FetchBoxFile.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "put"})
@CapabilityDescription("Puts content to a Box folder.")
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the Box object.")
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = "filename", description = FILENAME_DESC),
        @WritesAttribute(attribute = "path", description = PATH_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)})
public class PutBoxFile extends AbstractProcessor {
    public static final int CHUNKED_UPLOAD_LOWER_LIMIT_IN_BYTES = 20 * 1024 * 1024;
    public static final int CHUNKED_UPLOAD_UPPER_LIMIT_IN_BYTES = 50 * 1024 * 1024;

    public static final int NUMBER_OF_RETRIES = 10;
    public static final int WAIT_TIME_MS = 1000;

    public static final PropertyDescriptor FOLDER_ID = new PropertyDescriptor.Builder()
            .name("box-folder-id")
            .displayName("Folder ID")
            .description("The ID of the folder where the file is uploaded." +
            " Please see Additional Details to obtain Folder ID.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("file-name")
            .displayName("Filename")
            .description("The name of the file to upload to the specified Box folder.")
            .required(true)
            .defaultValue("${filename}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUBFOLDER_NAME = new PropertyDescriptor.Builder()
            .name("subfolder-name")
            .displayName("Subfolder Name")
            .description("The name (path) of the subfolder where files are uploaded. The subfolder name is relative to the folder specified by 'Folder ID'."
                    + " Example: subFolder, subFolder1/subfolder2")
            .addValidator(createRegexMatchingValidator(Pattern.compile("^(?!/).+(?<!/)$"), false,
                    "Subfolder Name should not contain leading or trailing slash ('/') character."))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor CREATE_SUBFOLDER = new PropertyDescriptor.Builder()
            .name("create-folder")
            .displayName("Create Subfolder")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(SUBFOLDER_NAME)
            .description("Specifies whether to check if the subfolder exists and to automatically create it if it does not. " +
                    "Permission to list folders is required. ")
            .build();

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("conflict-resolution-strategy")
            .displayName("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the specified Box folder.")
            .required(true)
            .defaultValue(ConflictResolutionStrategy.FAIL.getValue())
            .allowableValues(ConflictResolutionStrategy.class)
            .build();

    public static final PropertyDescriptor CHUNKED_UPLOAD_THRESHOLD = new PropertyDescriptor.Builder()
            .name("chunked-upload-threshold")
            .displayName("Chunked Upload Threshold")
            .description("The maximum size of the content which is uploaded at once. FlowFiles larger than this threshold are uploaded in chunks."
                    + " Chunked upload is allowed for files larger than 20 MB. It is recommended to use chunked upload for files exceeding 50 MB.")
            .defaultValue("20 MB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(CHUNKED_UPLOAD_LOWER_LIMIT_IN_BYTES, CHUNKED_UPLOAD_UPPER_LIMIT_IN_BYTES))
            .required(false)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BoxClientService.BOX_CLIENT_SERVICE,
            FOLDER_ID,
            SUBFOLDER_NAME,
            CREATE_SUBFOLDER,
            FILE_NAME,
            CONFLICT_RESOLUTION,
            CHUNKED_UPLOAD_THRESHOLD
    );

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("Files that have been successfully written to Box are transferred to this relationship.")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder()
                    .name("failure")
                    .description("Files that could not be written to Box for some reason are transferred to this relationship.")
                    .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final int CONFLICT_RESPONSE_CODE = 409;
    private static final int NOT_FOUND_RESPONSE_CODE = 404;

    private volatile BoxAPIConnection boxAPIConnection;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final BoxClientService boxClientService = context.getProperty(BoxClientService.BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);

        boxAPIConnection = boxClientService.getBoxApiConnection();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String filename = context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final long chunkUploadThreshold = context.getProperty(CHUNKED_UPLOAD_THRESHOLD)
                .asDataSize(DataUnit.B)
                .longValue();
        final ConflictResolutionStrategy conflictResolution = ConflictResolutionStrategy.forValue(context.getProperty(CONFLICT_RESOLUTION).getValue());

        final long startNanos = System.nanoTime();
        String fullPath = null;

        try {
            final long size = flowFile.getSize();
            final BoxFolder parentFolder = getOrCreateDirectParentFolder(context, flowFile);
            fullPath = BoxFileUtils.getFolderPath(parentFolder.getInfo());
            BoxFile.Info uploadedFileInfo = null;

            try (InputStream rawIn = session.read(flowFile)) {

                if (REPLACE.equals(conflictResolution)) {
                    uploadedFileInfo = replaceBoxFileIfExists(parentFolder, filename, rawIn, size, chunkUploadThreshold);
                }

                if (uploadedFileInfo == null) {
                   uploadedFileInfo = createBoxFile(parentFolder, filename, rawIn, size, chunkUploadThreshold);
                }
            } catch (BoxAPIResponseException e) {
                if (e.getResponseCode() == CONFLICT_RESPONSE_CODE) {
                    handleConflict(conflictResolution, filename, fullPath, e);
                } else {
                    throw e;
                }
            }

            if (uploadedFileInfo != null) {
                final Map<String, String> attributes = BoxFileUtils.createAttributeMap(uploadedFileInfo);
                final String url = BOX_URL + uploadedFileInfo.getID();
                flowFile = session.putAllAttributes(flowFile, attributes);
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().send(flowFile, url, transferMillis);
            }

            session.transfer(flowFile, REL_SUCCESS);
        } catch (BoxAPIResponseException e) {
            getLogger().error("Upload failed: File [{}] Folder [{}] Response Code [{}]", filename, fullPath, e.getResponseCode(), e);
            handleExpectedError(session, flowFile, e);
        } catch (Exception e) {
            getLogger().error("Upload failed: File [{}], Folder [{}]", filename, fullPath, e);
            handleUnexpectedError(session, flowFile, e);
        }
    }

    BoxFolder getFolder(String folderId) {
        return new BoxFolder(boxAPIConnection, folderId);
    }

    private BoxFolder getOrCreateDirectParentFolder(ProcessContext context, FlowFile flowFile ) {
        final String subfolderPath = context.getProperty(SUBFOLDER_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final boolean createFolder = context.getProperty(CREATE_SUBFOLDER).asBoolean();
        final String folderId = context.getProperty(FOLDER_ID).evaluateAttributeExpressions(flowFile).getValue();
        BoxFolder parentFolder = getFolderById(folderId);

        if (subfolderPath != null) {
            final Queue<String> subFolderNames = getSubFolderNames(subfolderPath);
            parentFolder = getOrCreateSubfolders(subFolderNames, parentFolder, createFolder);
        }

        return parentFolder;
    }

    private BoxFile.Info replaceBoxFileIfExists(BoxFolder parentFolder, String filename, final InputStream inputStream, final long size, final long chunkUploadThreshold)
            throws IOException, InterruptedException {
        final Optional<BoxFile> existingBoxFileInfo = getFileByName(filename, parentFolder);
        if (existingBoxFileInfo.isPresent()) {
            final BoxFile existingBoxFile = existingBoxFileInfo.get();

            if (size > chunkUploadThreshold) {
                return existingBoxFile.uploadLargeFile(inputStream, size);
            } else {
                return existingBoxFile.uploadNewVersion(inputStream);
            }
        }
        return null;
    }

    private BoxFile.Info createBoxFile(BoxFolder parentFolder, String filename, InputStream inputStream, long size, final long chunkUploadThreshold)
            throws IOException, InterruptedException {
        if (size > chunkUploadThreshold) {
            return parentFolder.uploadLargeFile(inputStream, filename, size);
        } else {
            return parentFolder.uploadFile(inputStream, filename);
        }
    }

    private Queue<String> getSubFolderNames(String subfolderPath)  {
        final Queue<String> subfolderNames = new LinkedList<>();
        Collections.addAll(subfolderNames, subfolderPath.split("/"));
        return subfolderNames;
    }

    private BoxFolder getOrCreateSubfolders(Queue<String> subFolderNames, BoxFolder parentFolder, boolean createFolder) {
        final BoxFolder newParentFolder = getOrCreateFolder(subFolderNames.poll(), parentFolder, createFolder);

        if (!subFolderNames.isEmpty()) {
           return getOrCreateSubfolders(subFolderNames, newParentFolder, createFolder);
        } else {
            return newParentFolder;
        }
    }

    private BoxFolder getOrCreateFolder(String folderName, BoxFolder parentFolder, boolean createFolder) {
        final Optional<BoxFolder> existingFolder = getFolderByName(folderName, parentFolder);

        if (existingFolder.isPresent()) {
            return existingFolder.get();
        }

        if (!createFolder) {
           throw new ProcessException(format("The specified subfolder [%s] does not exist and [%s] is false.",
                   folderName, CREATE_SUBFOLDER.getDisplayName()));
        }

        return createFolder(folderName, parentFolder);
    }

    private BoxFolder createFolder(final String folderName, final BoxFolder parentFolder) {
        getLogger().info("Creating Folder [{}], Parent [{}]", folderName, parentFolder.getID());

        try {
           return parentFolder.createFolder(folderName).getResource();
        } catch (BoxAPIResponseException e) {
            if (e.getResponseCode() != CONFLICT_RESPONSE_CODE) {
                throw e;
            } else {
                Optional<BoxFolder> createdFolder = waitForOngoingFolderCreationToFinish(folderName, parentFolder);
                return createdFolder.orElseThrow(() -> new ProcessException(format("Created subfolder [%s] can not be found under [%s]",
                        folderName, parentFolder.getID())));
            }
        }
    }

    private Optional<BoxFolder> waitForOngoingFolderCreationToFinish(final String folderName, final BoxFolder parentFolder) {
        try {
            Optional<BoxFolder> createdFolder = getFolderByName(folderName, parentFolder);

            for (int i = 0; i < NUMBER_OF_RETRIES && createdFolder.isEmpty(); i++) {
                getLogger().debug("Subfolder [{}] under [{}] has not been created yet, waiting {} ms",
                        folderName, parentFolder.getID(), WAIT_TIME_MS);
                Thread.sleep(WAIT_TIME_MS);
                createdFolder = getFolderByName(folderName, parentFolder);
            }
            return createdFolder;
        } catch (InterruptedException ie) {
            throw new RuntimeException(format("Waiting for creation of subfolder [%s] under [%s] was interrupted",
                    folderName, parentFolder.getID()), ie);
        }
    }

    private BoxFolder getFolderById(final String folderId) {
        final BoxFolder folder = getFolder(folderId);
        try {
            //Error is returned for nonexistent folder only when a method is called on BoxFolder.
            folder.getInfo();
        } catch (BoxAPIResponseException e) {
            if (e.getResponseCode() == NOT_FOUND_RESPONSE_CODE) {
                throw new ProcessException(format("The Folder [%s] specified by [%s] does not exist", folderId, FOLDER_ID.getDisplayName()));
            }
        }
        return folder;
    }

    private Optional<BoxFolder> getFolderByName(final String folderName, final BoxFolder parentFolder) {
        return getItemByName(folderName, parentFolder, BoxFolder.Info.class)
                .map(BoxFolder.Info::getResource);
    }

    private Optional<BoxFile> getFileByName(final String filename, final BoxFolder parentFolder) {
        return getItemByName(filename, parentFolder, BoxFile.Info.class)
                .map(BoxFile.Info::getResource);
    }

    private <T extends BoxItem.Info> Optional<T> getItemByName(final String itemName, final BoxFolder parentFolder, Class<T> type) {
        return StreamSupport.stream(parentFolder.getChildren("name").spliterator(), false)
                .filter(type::isInstance)
                .map(type::cast)
                .filter(info -> info.getName().equals(itemName))
                .findAny();
    }

    private void handleConflict(final ConflictResolutionStrategy conflictResolution, final String filename, String path, final BoxAPIException e) {
        if (conflictResolution == IGNORE) {
            getLogger().info("File with the same name [{}] already exists in [{}]. Remote file is not modified due to [{}] being set to [{}]",
                    filename, path, CONFLICT_RESOLUTION.getDisplayName(), conflictResolution.getDisplayName());
        } else {
            throw new ProcessException(format("File with the same name [%s] already exists in [%s]", filename, path), e);
        }
    }

    private void handleUnexpectedError(final ProcessSession session, FlowFile flowFile, final Exception e) {
        flowFile = session.putAttribute(flowFile, BoxFileAttributes.ERROR_MESSAGE, e.getMessage());
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    private void handleExpectedError(final ProcessSession session, FlowFile flowFile, final BoxAPIResponseException e) {
        flowFile = session.putAttribute(flowFile, BoxFileAttributes.ERROR_MESSAGE, e.getMessage());
        flowFile = session.putAttribute(flowFile, BoxFileAttributes.ERROR_CODE, valueOf(e.getResponseCode()));
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }
}