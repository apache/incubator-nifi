/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.dropbox;

import static java.lang.String.format;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.FAIL;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.IGNORE;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.REPLACE;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.ERROR_MESSAGE_DESC;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.FILENAME;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.ID;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.ID_DESC;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.PATH;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.PATH_DESC;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.REVISION;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.REVISION_DESC;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.SIZE;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.SIZE_DESC;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.TIMESTAMP;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.TIMESTAMP_DESC;

import com.dropbox.core.DbxApiException;
import com.dropbox.core.DbxException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.CommitInfo;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.UploadErrorException;
import com.dropbox.core.v2.files.UploadSessionAppendV2Uploader;
import com.dropbox.core.v2.files.UploadSessionCursor;
import com.dropbox.core.v2.files.UploadSessionFinishErrorException;
import com.dropbox.core.v2.files.UploadSessionFinishUploader;
import com.dropbox.core.v2.files.UploadSessionStartUploader;
import com.dropbox.core.v2.files.UploadUploader;
import com.dropbox.core.v2.files.WriteMode;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;

/**
 * This processor uploads objects to Dropbox.
 */
@SeeAlso({ListDropbox.class, FetchDropbox.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"dropbox", "storage", "put"})
@CapabilityDescription("Puts content to a Dropbox folder.")
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the Dropbox object.")
@WritesAttributes({
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC),
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = PATH, description = PATH_DESC),
        @WritesAttribute(attribute = FILENAME, description = FILENAME_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = REVISION, description = REVISION_DESC)})
public class PutDropbox extends AbstractProcessor implements DropboxTrait {

    public static final int SINGLE_UPLOAD_LIMIT_IN_BYTES = 150 * 1024 * 1024;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to Dropbox are transferred to this relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to Dropbox for some reason are transferred to this relationship.")
            .build();

    public static final PropertyDescriptor FOLDER = new PropertyDescriptor.Builder()
            .name("folder")
            .displayName("Folder")
            .description("The path of the Dropbox folder to upload files to. "
                    + "The folder will be created if it does not exist yet.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("/.*")))
            .defaultValue("/")
            .build();

    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("file-name")
            .displayName("Filename")
            .description("The full name of the file to upload.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("${filename}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("conflict-resolution-strategy")
            .displayName("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the specified Dropbox folder.")
            .required(true)
            .defaultValue(FAIL.getValue())
            .allowableValues(ConflictResolutionStrategy.class)
            .build();

    public static final PropertyDescriptor CHUNKED_UPLOAD_SIZE = new PropertyDescriptor.Builder()
            .name("chunked-upload-size")
            .displayName("Chunked Upload Size")
            .description("Defines the size of a chunk. Used when a FlowFile's size exceeds 'Chunked Upload Threshold' and content is uploaded in smaller chunks. "
                    + "It is recommended to specify chunked upload size smaller than 'Chunked Upload Threshold' and as multiples of 4 MB. "
                    + "Maximum allowed value is 150 MB.")
            .defaultValue("8 MB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, SINGLE_UPLOAD_LIMIT_IN_BYTES))
            .required(false)
            .build();

    public static final PropertyDescriptor CHUNKED_UPLOAD_THRESHOLD = new PropertyDescriptor.Builder()
            .name("chunked-upload-threshold")
            .displayName("Chunked Upload Threshold")
            .description("The maximum size of the content which is uploaded at once. FlowFiles larger than this threshold are uploaded in chunks. "
                    + "Maximum allowed value is 150 MB.")
            .defaultValue("150 MB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, SINGLE_UPLOAD_LIMIT_IN_BYTES))
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CREDENTIAL_SERVICE,
            FOLDER,
            FILE_NAME,
            CONFLICT_RESOLUTION,
            CHUNKED_UPLOAD_THRESHOLD,
            CHUNKED_UPLOAD_SIZE,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxySpec.HTTP_AUTH)
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS,
        REL_FAILURE
    );

    private volatile DbxClientV2 dropboxApiClient;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        dropboxApiClient = getDropboxApiClient(context, getIdentifier());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String folder = context.getProperty(FOLDER).evaluateAttributeExpressions(flowFile).getValue();
        final String filename = context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowFile).getValue();

        final long chunkUploadThreshold = context.getProperty(CHUNKED_UPLOAD_THRESHOLD)
                .asDataSize(DataUnit.B)
                .longValue();

        final long uploadChunkSize = context.getProperty(CHUNKED_UPLOAD_SIZE)
                .asDataSize(DataUnit.B)
                .longValue();

        final ConflictResolutionStrategy conflictResolution = ConflictResolutionStrategy.forValue(context.getProperty(CONFLICT_RESOLUTION).getValue());
        final long size = flowFile.getSize();
        final String uploadPath = convertFolderName(folder) + "/" + filename;
        final long startNanos = System.nanoTime();
        FileMetadata fileMetadata = null;

        try {
            try (final InputStream rawIn = session.read(flowFile)) {
                if (size <= chunkUploadThreshold) {
                    try (UploadUploader uploader = createUploadUploader(uploadPath, conflictResolution)) {
                        fileMetadata = uploader.uploadAndFinish(rawIn);
                    }
                } else {
                    fileMetadata = uploadLargeFileInChunks(uploadPath, rawIn, size, uploadChunkSize, conflictResolution);
                }
            } catch (UploadErrorException e) {
                handleUploadError(conflictResolution, uploadPath, e);
            } catch (UploadSessionFinishErrorException e) {
                handleUploadSessionError(conflictResolution, uploadPath, e);
            } catch (RateLimitException e) {
                context.yield();
                throw new ProcessException("Dropbox API rate limit exceeded while uploading file", e);
            }

            if (fileMetadata != null) {
                final Map<String, String> attributes = createAttributeMap(fileMetadata);
                String url = DROPBOX_HOME_URL + fileMetadata.getPathDisplay();
                flowFile = session.putAllAttributes(flowFile, attributes);
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().send(flowFile, url, transferMillis);
            }

            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Exception occurred while uploading file '{}' to Dropbox folder '{}'", filename, folder, e);
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void handleUploadError(final ConflictResolutionStrategy conflictResolution, final String uploadPath, final UploadErrorException e)  {
        if (e.errorValue.isPath() && e.errorValue.getPathValue().getReason().isConflict()) {
            handleConflict(conflictResolution, uploadPath, e);
        } else {
            throw new ProcessException(e);
        }
    }

    private void handleUploadSessionError(final ConflictResolutionStrategy conflictResolution, final String uploadPath, final UploadSessionFinishErrorException e) {
        if (e.errorValue.isPath() && e.errorValue.getPathValue().isConflict()) {
            handleConflict(conflictResolution, uploadPath, e);
        } else {
            throw new ProcessException(e);
        }
    }

    private void handleConflict(final ConflictResolutionStrategy conflictResolution, final String uploadPath, final DbxApiException e) {
        if (conflictResolution == IGNORE) {
            getLogger().info("File with the same name [{}] already exists. Remote file is not modified due to {} being set to '{}'.",
                    uploadPath, CONFLICT_RESOLUTION.getDisplayName(), conflictResolution);
        } else if (conflictResolution == FAIL) {
            throw new ProcessException(format("File with the same name [%s] already exists.", uploadPath), e);
        }
    }

    private FileMetadata uploadLargeFileInChunks(String path, InputStream rawIn, long size, long uploadChunkSize,
            ConflictResolutionStrategy conflictResolution) throws DbxException, IOException {
        final String sessionId;
        try (UploadSessionStartUploader uploader = createUploadSessionStartUploader()) {
            sessionId = uploader.uploadAndFinish(rawIn, uploadChunkSize).getSessionId();
        }

        long uploadedBytes = uploadChunkSize;

        UploadSessionCursor cursor = new UploadSessionCursor(sessionId, uploadedBytes);

        while (size - uploadedBytes > uploadChunkSize) {
            try (UploadSessionAppendV2Uploader uploader = createUploadSessionAppendV2Uploader(cursor)) {
                uploader.uploadAndFinish(rawIn, uploadChunkSize);
                uploadedBytes += uploadChunkSize;
                cursor = new UploadSessionCursor(sessionId, uploadedBytes);
            }
        }

        final long remainingBytes = size - uploadedBytes;

        final CommitInfo commitInfo = CommitInfo.newBuilder(path)
                .withMode(getWriteMode(conflictResolution))
                .withStrictConflict(true)
                .withClientModified(new Date(System.currentTimeMillis()))
                .build();

        try (UploadSessionFinishUploader uploader = createUploadSessionFinishUploader(cursor, commitInfo)) {
            return uploader.uploadAndFinish(rawIn, remainingBytes);
        }
    }

    private WriteMode getWriteMode(ConflictResolutionStrategy conflictResolution) {
        if (conflictResolution == REPLACE) {
            return WriteMode.OVERWRITE;
        } else {
            return WriteMode.ADD;
        }
    }

    private UploadUploader createUploadUploader(String path, ConflictResolutionStrategy conflictResolution) throws DbxException {
        return dropboxApiClient
                .files()
                .uploadBuilder(path)
                .withMode(getWriteMode(conflictResolution))
                .withStrictConflict(true)
                .start();
    }

    private UploadSessionStartUploader createUploadSessionStartUploader() throws DbxException {
        return dropboxApiClient
                .files()
                .uploadSessionStart();
    }

    private UploadSessionAppendV2Uploader createUploadSessionAppendV2Uploader(UploadSessionCursor cursor) throws DbxException {
        return dropboxApiClient
                .files()
                .uploadSessionAppendV2(cursor);
    }

    private UploadSessionFinishUploader createUploadSessionFinishUploader(UploadSessionCursor cursor, CommitInfo commitInfo) throws DbxException {
        return dropboxApiClient
                .files()
                .uploadSessionFinish(cursor, commitInfo);
    }
}