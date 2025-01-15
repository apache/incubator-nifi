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
package org.apache.nifi.processors.gcp.drive;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveRequest;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static org.apache.nifi.processor.util.StandardValidators.DATA_SIZE_VALIDATOR;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.FAIL;
import static org.apache.nifi.processors.conflict.resolution.ConflictResolutionStrategy.IGNORE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_CODE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ERROR_MESSAGE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MIME_TYPE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP_DESC;
import static org.apache.nifi.processors.gcp.util.GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE;

@SeeAlso({ListGoogleDrive.class, FetchGoogleDrive.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"google", "drive", "storage", "put"})
@CapabilityDescription("Writes the contents of a FlowFile as a file in Google Drive.")
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the Google Drive object.")
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = "filename", description = FILENAME_DESC),
        @WritesAttribute(attribute = "mime.type", description = MIME_TYPE_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)})
public class PutGoogleDrive extends AbstractProcessor implements GoogleDriveTrait {
    public static final int MIN_ALLOWED_CHUNK_SIZE_IN_BYTES = MediaHttpUploader.MINIMUM_CHUNK_SIZE;
    public static final int MAX_ALLOWED_CHUNK_SIZE_IN_BYTES = 1024 * 1024 * 1024;

    public static final PropertyDescriptor FOLDER_ID = new PropertyDescriptor.Builder()
            .name("folder-id")
            .displayName("Folder ID")
            .description("The ID of the shared folder." +
                    " Please see Additional Details to set up access to Google Drive and obtain Folder ID.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("file-name")
            .displayName("Filename")
            .description("The name of the file to upload to the specified Google Drive folder.")
            .required(true)
            .defaultValue("${filename}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("conflict-resolution-strategy")
            .displayName("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the specified Google Drive folder.")
            .required(true)
            .defaultValue(FAIL.getValue())
            .allowableValues(ConflictResolutionStrategy.class)
            .build();

    public static final PropertyDescriptor CHUNKED_UPLOAD_SIZE = new PropertyDescriptor.Builder()
            .name("chunked-upload-size")
            .displayName("Chunked Upload Size")
            .description("Defines the size of a chunk. Used when a FlowFile's size exceeds 'Chunked Upload Threshold' and content is uploaded in smaller chunks. "
                    + "Minimum allowed chunk size is 256 KB, maximum allowed chunk size is 1 GB.")
            .addValidator(createChunkSizeValidator())
            .defaultValue("10 MB")
            .required(false)
            .build();

    public static final PropertyDescriptor CHUNKED_UPLOAD_THRESHOLD = new PropertyDescriptor.Builder()
            .name("chunked-upload-threshold")
            .displayName("Chunked Upload Threshold")
            .description("The maximum size of the content which is uploaded at once. FlowFiles larger than this threshold are uploaded in chunks.")
            .defaultValue("100 MB")
            .addValidator(DATA_SIZE_VALIDATOR)
            .required(false)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GCP_CREDENTIALS_PROVIDER_SERVICE,
            FOLDER_ID,
            FILE_NAME,
            CONFLICT_RESOLUTION,
            CHUNKED_UPLOAD_THRESHOLD,
            CHUNKED_UPLOAD_SIZE,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxyAwareTransportFactory.PROXY_SPECS)
    );

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("Files that have been successfully written to Google Drive are transferred to this relationship.")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder()
                    .name("failure")
                    .description("Files that could not be written to Google Drive for some reason are transferred to this relationship.")
                    .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    public static final String MULTIPART_UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true";

    private volatile Drive driveService;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public List<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final long chunkUploadThreshold = validationContext.getProperty(CHUNKED_UPLOAD_THRESHOLD)
                .asDataSize(DataUnit.B)
                .longValue();

        final int uploadChunkSize = validationContext.getProperty(CHUNKED_UPLOAD_SIZE)
                .asDataSize(DataUnit.B)
                .intValue();

        if (uploadChunkSize > chunkUploadThreshold) {
            results.add(new ValidationResult.Builder()
                    .subject(CHUNKED_UPLOAD_SIZE.getDisplayName())
                    .explanation(format("%s should not be bigger than %s", CHUNKED_UPLOAD_SIZE.getDisplayName(), CHUNKED_UPLOAD_THRESHOLD.getDisplayName()))
                    .valid(false)
                    .build());
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String folderId = context.getProperty(FOLDER_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String filename = context.getProperty(FILE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());

        try {
            final long startNanos = System.nanoTime();
            final long size = flowFile.getSize();

            final long chunkUploadThreshold = context.getProperty(CHUNKED_UPLOAD_THRESHOLD)
                    .asDataSize(DataUnit.B)
                    .longValue();

            final int uploadChunkSize = context.getProperty(CHUNKED_UPLOAD_SIZE)
                    .asDataSize(DataUnit.B)
                    .intValue();

            final ConflictResolutionStrategy conflictResolution = ConflictResolutionStrategy.forValue(context.getProperty(CONFLICT_RESOLUTION).getValue());
            final Optional<File> alreadyExistingFile = checkFileExistence(filename, folderId);
            final File fileMetadata = alreadyExistingFile.orElseGet(() -> createMetadata(filename, folderId));

            if (alreadyExistingFile.isPresent() && conflictResolution == FAIL) {
                getLogger().error("File [{}] already exists in [{}] Folder, conflict resolution is [{}]", filename, folderId, FAIL.getDisplayName());
                flowFile = addAttributes(alreadyExistingFile.get(), flowFile, session);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            if (alreadyExistingFile.isPresent() && conflictResolution == IGNORE) {
                getLogger().info("File [{}] already exists in [{}] Folder, conflict resolution is [{}]", filename,  folderId, IGNORE.getDisplayName());
                flowFile = addAttributes(alreadyExistingFile.get(), flowFile, session);
                session.transfer(flowFile, REL_SUCCESS);
                return;
            }

            final File uploadedFile;

            try (final InputStream rawIn = session.read(flowFile); final BufferedInputStream bufferedInputStream = new BufferedInputStream(rawIn)) {
                final InputStreamContent mediaContent = new InputStreamContent(mimeType, bufferedInputStream);
                mediaContent.setLength(size);

                final DriveRequest<File> driveRequest = createDriveRequest(fileMetadata, mediaContent);

                if (size > chunkUploadThreshold) {
                    uploadedFile = uploadFileInChunks(driveRequest, fileMetadata, uploadChunkSize, mediaContent);
                } else {
                    uploadedFile = driveRequest.execute();
                }
            }

            if (uploadedFile != null) {
                final Map<String, String> attributes = createAttributeMap(uploadedFile);
                final String url = DRIVE_URL + uploadedFile.getId();
                flowFile = session.putAllAttributes(flowFile, attributes);
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().send(flowFile, url, transferMillis);
            }
            session.transfer(flowFile, REL_SUCCESS);
        } catch (GoogleJsonResponseException e) {
            getLogger().error("Exception occurred while uploading File [{}] to [{}] Google Drive Folder", filename,
                    folderId, e);
            handleExpectedError(session, flowFile, e);
        } catch (Exception e) {
            getLogger().error("Exception occurred while uploading File [{}] to [{}] Google Drive Folder", filename,
                    folderId, e);

            if (e.getCause() != null && e.getCause() instanceof GoogleJsonResponseException) {
                handleExpectedError(session, flowFile, (GoogleJsonResponseException) e.getCause());
            } else {
                handleUnexpectedError(session, flowFile, e);
            }
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context);

        final HttpTransport httpTransport = new ProxyAwareTransportFactory(proxyConfiguration).create();

        driveService = createDriveService(context, httpTransport, DriveScopes.DRIVE, DriveScopes.DRIVE_METADATA);
    }

    private FlowFile addAttributes(File file, FlowFile flowFile, ProcessSession session) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ID, file.getId());
        attributes.put(FILENAME, file.getName());
        return session.putAllAttributes(flowFile, attributes);
    }

    private DriveRequest<File> createDriveRequest(File fileMetadata, final InputStreamContent mediaContent) throws IOException {
        if (fileMetadata.getId() == null) {
            return driveService.files()
                    .create(fileMetadata, mediaContent)
                    .setSupportsAllDrives(true)
                    .setFields("id, name, createdTime, mimeType, size");
        } else {
            return driveService.files()
                    .update(fileMetadata.getId(), new File(), mediaContent)
                    .setSupportsAllDrives(true)
                    .setFields("id, name, createdTime, mimeType, size");
        }
    }

    private File uploadFileInChunks(DriveRequest<File> driveRequest, File fileMetadata, final int chunkSize, final InputStreamContent mediaContent) throws IOException {
        final HttpResponse response = driveRequest
                .getMediaHttpUploader()
                .setChunkSize(chunkSize)
                .setDirectUploadEnabled(false)
                .upload(new GenericUrl(MULTIPART_UPLOAD_URL));

        if (response.getStatusCode() == HttpStatusCodes.STATUS_CODE_OK) {
            fileMetadata.setId(getUploadedFileId(response.getContent()));
            fileMetadata.setMimeType(mediaContent.getType());
            fileMetadata.setCreatedTime(new DateTime(System.currentTimeMillis()));
            fileMetadata.setSize(mediaContent.getLength());
            return fileMetadata;
        } else {
            throw new ProcessException(format("Upload of File [%s] to Folder [%s] failed, HTTP error code: [%d]",
                    fileMetadata.getName(), fileMetadata.getParents().stream().findFirst().orElse(""), response.getStatusCode()));
        }
    }

    private String getUploadedFileId(final InputStream content) {
        final String contentAsString = new BufferedReader(new InputStreamReader(content, UTF_8))
                .lines()
                .collect(joining("\n"));
        return new JSONObject(contentAsString).getString("id");
    }

    private File createMetadata(final String name, final String parentId) {
        final File metadata = new File();
        metadata.setName(name);
        metadata.setParents(singletonList(parentId));
        return metadata;
    }

    private Optional<File> checkFileExistence(String fileName, String parentId) throws IOException {
        final FileList result = driveService.files()
                .list()
                .setSupportsAllDrives(true)
                .setIncludeItemsFromAllDrives(true)
                .setQ(format("name='%s' and ('%s' in parents)", fileName, parentId))
                .setFields("files(name, id)")
                .execute();

        return result.getFiles().stream()
                .findFirst();
    }

    private void handleUnexpectedError(final ProcessSession session, FlowFile flowFile, final Exception e) {
        flowFile = session.putAttribute(flowFile, GoogleDriveAttributes.ERROR_MESSAGE, e.getMessage());
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    private void handleExpectedError(final ProcessSession session, FlowFile flowFile, final GoogleJsonResponseException e) {
        flowFile = session.putAttribute(flowFile, GoogleDriveAttributes.ERROR_MESSAGE, e.getMessage());
        flowFile = session.putAttribute(flowFile, GoogleDriveAttributes.ERROR_CODE, valueOf(e.getStatusCode()));
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    private static Validator createChunkSizeValidator() {
        return (subject, input, context) -> {
            final ValidationResult vr = StandardValidators.createDataSizeBoundsValidator(MIN_ALLOWED_CHUNK_SIZE_IN_BYTES, MAX_ALLOWED_CHUNK_SIZE_IN_BYTES)
                    .validate(subject, input, context);
            if (!vr.isValid()) {
                return vr;
            }

            final long dataSizeBytes = DataUnit.parseDataSize(input, DataUnit.B).longValue();

            if (dataSizeBytes % MIN_ALLOWED_CHUNK_SIZE_IN_BYTES != 0 ) {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("Must be a positive multiple of " + MIN_ALLOWED_CHUNK_SIZE_IN_BYTES + " bytes")
                        .build();
            }

            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(true)
                    .build();
        };
    }
}
