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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.processors.gcp.util.GoogleUtils;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.ID_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.MIME_TYPE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.SIZE_DESC;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP;
import static org.apache.nifi.processors.gcp.drive.GoogleDriveAttributes.TIMESTAMP_DESC;

@PrimaryNodeOnly
@TriggerSerially
@Tags({"google", "drive", "storage"})
@CapabilityDescription("Performs a listing of concrete files (shortcuts are ignored) in a Google Drive folder. " +
        "If the 'Record Writer' property is set, a single Output FlowFile is created, and each file in the listing is written as a single record to the output file. " +
        "Otherwise, for each file in the listing, an individual FlowFile is created, the metadata being written as FlowFile attributes. " +
        "This Processor is designed to run on Primary Node only in a cluster. If the primary node changes, the new Primary Node will pick up where the " +
        "previous node left off without duplicating all of the data. " +
        "Please see Additional Details to set up access to Google Drive.")
@SeeAlso({FetchGoogleDrive.class, PutGoogleDrive.class})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = "filename", description = FILENAME_DESC),
        @WritesAttribute(attribute = "mime.type", description = MIME_TYPE_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC)})
@Stateful(scopes = {Scope.CLUSTER}, description = "The processor stores necessary data to be able to keep track what files have been listed already." +
        " What exactly needs to be stored depends on the 'Listing Strategy'." +
        " State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary Node is selected, the new node can pick up" +
        " where the previous node left off, without duplicating the data.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListGoogleDrive extends AbstractListProcessor<GoogleDriveFileInfo> implements GoogleDriveTrait {
    public static final PropertyDescriptor FOLDER_ID = new PropertyDescriptor.Builder()
            .name("folder-id")
            .displayName("Folder ID")
            .description("The ID of the folder from which to pull list of files." +
                    " Please see Additional Details to set up access to Google Drive and obtain Folder ID." +
                    " WARNING: Unauthorized access to the folder is treated as if the folder was empty." +
                    " This results in the processor not creating outgoing FlowFiles. No additional error message is provided.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    public static final PropertyDescriptor RECURSIVE_SEARCH = new PropertyDescriptor.Builder()
            .name("recursive-search")
            .displayName("Search Recursively")
            .description("When 'true', will include list of files from concrete sub-folders (ignores shortcuts)." +
                    " Otherwise, will return only files that have the defined 'Folder ID' as their parent directly." +
                    " WARNING: The listing may fail if there are too many sub-folders (500+).")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("min-age")
            .displayName("Minimum File Age")
            .description("The minimum age a file must be in order to be considered; any files younger than this will be ignored.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();

    public static final PropertyDescriptor LISTING_STRATEGY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractListProcessor.LISTING_STRATEGY)
            .allowableValues(BY_TIMESTAMPS, BY_ENTITIES, BY_TIME_WINDOW, NO_TRACKING)
            .build();

    public static final PropertyDescriptor TRACKING_STATE_CACHE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ListedEntityTracker.TRACKING_STATE_CACHE)
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor TRACKING_TIME_WINDOW = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ListedEntityTracker.TRACKING_TIME_WINDOW)
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor INITIAL_LISTING_TARGET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ListedEntityTracker.INITIAL_LISTING_TARGET)
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE,
            FOLDER_ID,
            RECURSIVE_SEARCH,
            MIN_AGE,
            LISTING_STRATEGY,
            TRACKING_STATE_CACHE,
            TRACKING_TIME_WINDOW,
            INITIAL_LISTING_TARGET,
            RECORD_WRITER,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxyAwareTransportFactory.PROXY_SPECS)
    );

    private volatile Drive driveService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
    }

    @Override
    protected Map<String, String> createAttributes(
            final GoogleDriveFileInfo entity,
            final ProcessContext context
    ) {
        final Map<String, String> attributes = new HashMap<>();

        for (GoogleDriveFlowFileAttribute attribute : GoogleDriveFlowFileAttribute.values()) {
            Optional.ofNullable(attribute.getValue(entity))
                    .ifPresent(value -> attributes.put(attribute.getName(), value));
        }

        return attributes;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context);

        HttpTransport httpTransport = new ProxyAwareTransportFactory(proxyConfiguration).create();

        driveService = createDriveService(context, httpTransport, DriveScopes.DRIVE_METADATA_READONLY);
    }

    @Override
    protected String getListingContainerName(final ProcessContext context) {
        return String.format("Google Drive Folder [%s]", getPath(context));
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(FOLDER_ID).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return LISTING_STRATEGY.equals(property)
                || FOLDER_ID.equals(property)
                || RECURSIVE_SEARCH.equals(property);
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        return Scope.CLUSTER;
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return GoogleDriveFileInfo.getRecordSchema();
    }

    @Override
    protected String getDefaultTimePrecision() {
        return PRECISION_SECONDS.getValue();
    }

    @Override
    protected List<GoogleDriveFileInfo> performListing(
            final ProcessContext context,
            final Long minTimestamp,
            final ListingMode listingMode
    ) throws IOException {
        final List<GoogleDriveFileInfo> listing = new ArrayList<>();

        final String folderId = context.getProperty(FOLDER_ID).evaluateAttributeExpressions().getValue();
        final Boolean recursive = context.getProperty(RECURSIVE_SEARCH).asBoolean();
        final Long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);

        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(buildQueryForDirs(driveService, folderId, recursive));
        queryBuilder.append(" and (mimeType != 'application/vnd.google-apps.folder')");
        queryBuilder.append(" and (mimeType != 'application/vnd.google-apps.shortcut')");
        queryBuilder.append(" and trashed = false");
        if (minTimestamp != null && minTimestamp > 0) {
            String formattedMinTimestamp = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(OffsetDateTime.ofInstant(Instant.ofEpochMilli(minTimestamp), ZoneOffset.UTC));

            queryBuilder.append(" and (");
            queryBuilder.append("modifiedTime >= '" + formattedMinTimestamp + "'");
            queryBuilder.append(" or createdTime >= '" + formattedMinTimestamp + "'");
            queryBuilder.append(")");
        }
        if (minAge != null && minAge > 0) {
            long maxTimestamp = System.currentTimeMillis() - minAge;
            String formattedMaxTimestamp = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(OffsetDateTime.ofInstant(Instant.ofEpochMilli(maxTimestamp), ZoneOffset.UTC));

            queryBuilder.append(" and modifiedTime < '" + formattedMaxTimestamp + "'");
            queryBuilder.append(" and createdTime < '" + formattedMaxTimestamp + "'");
        }

        String pageToken = null;
        do {
            FileList result = driveService.files()
                    .list()
                    .setSupportsAllDrives(true)
                    .setIncludeItemsFromAllDrives(true)
                    .setQ(queryBuilder.toString())
                    .setPageToken(pageToken)
                    .setFields("nextPageToken, files(id, name, size, createdTime, modifiedTime, mimeType)")
                    .execute();

            for (File file : result.getFiles()) {
                GoogleDriveFileInfo.Builder builder = new GoogleDriveFileInfo.Builder()
                        .id(file.getId())
                        .fileName(file.getName())
                        .size(file.getSize())
                        .createdTime(Optional.ofNullable(file.getCreatedTime()).map(DateTime::getValue).orElse(0L))
                        .modifiedTime(Optional.ofNullable(file.getModifiedTime()).map(DateTime::getValue).orElse(0L))
                        .mimeType(file.getMimeType());

                listing.add(builder.build());
            }

            pageToken = result.getNextPageToken();
        } while (pageToken != null);

        return listing;
    }

    @Override
    protected Integer countUnfilteredListing(final ProcessContext context) throws IOException {
        return performListing(context, null, ListingMode.CONFIGURATION_VERIFICATION).size();
    }

    private static String buildQueryForDirs(
            final Drive service,
            final String folderId,
            boolean recursive
    ) throws IOException {
        StringBuilder queryBuilder = new StringBuilder("('")
                .append(folderId)
                .append("' in parents");

        if (recursive) {
            List<File> subDirectoryList = new LinkedList<>();

            collectSubDirectories(service, folderId, subDirectoryList);

            for (File subDirectory : subDirectoryList) {
                queryBuilder.append(" or '")
                        .append(subDirectory.getId())
                        .append("' in parents");
            }
        }

        queryBuilder.append(")");

        return queryBuilder.toString();
    }

    private static void collectSubDirectories(
            final Drive service,
            final String folderId,
            final List<File> dirList
    ) throws IOException {
        String pageToken = null;
        do {
            FileList directoryList = service.files()
                    .list()
                    .setSupportsAllDrives(true)
                    .setIncludeItemsFromAllDrives(true)
                    .setQ("'" + folderId + "' in parents "
                            + "and mimeType = 'application/vnd.google-apps.folder'"
                    )
                    .setPageToken(pageToken)
                    .execute();

            for (File directory : directoryList.getFiles()) {
                dirList.add(directory);
                collectSubDirectories(service, directory.getId(), dirList);
            }

            pageToken = directoryList.getNextPageToken();
        } while (pageToken != null);
    }
}
