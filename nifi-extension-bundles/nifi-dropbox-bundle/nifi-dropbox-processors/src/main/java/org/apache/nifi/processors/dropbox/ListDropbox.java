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
package org.apache.nifi.processors.dropbox;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
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

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DbxUserListFolderBuilder;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
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
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.serialization.record.RecordSchema;

@PrimaryNodeOnly
@TriggerSerially
@Tags({"dropbox", "storage"})
@CapabilityDescription("Retrieves a listing of files from Dropbox (shortcuts are ignored)." +
        " Each listed file may result in one FlowFile, the metadata being written as FlowFile attributes." +
        " When the 'Record Writer' property is set, the entire result is written as records to a single FlowFile." +
        " This Processor is designed to run on Primary Node only in a cluster. If the primary node changes, the new Primary Node will pick up where the" +
        " previous node left off without duplicating all of the data.")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = PATH, description = PATH_DESC),
        @WritesAttribute(attribute = FILENAME, description = FILENAME_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = REVISION, description = REVISION_DESC)})
@Stateful(scopes = {Scope.CLUSTER}, description = "The processor stores necessary data to be able to keep track what files have been listed already. " +
        "What exactly needs to be stored depends on the 'Listing Strategy'.")
@SeeAlso({FetchDropbox.class, PutDropbox.class})
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListDropbox extends AbstractListProcessor<DropboxFileInfo> implements DropboxTrait {

    public static final PropertyDescriptor FOLDER = new PropertyDescriptor.Builder()
            .name("folder")
            .displayName("Folder")
            .description("The Dropbox identifier or path of the folder from which to pull list of files." +
                    " 'Folder' should match the following regular expression pattern: /.*|id:.* ." +
                    " Example for folder identifier: id:odTlUvbpIEAAAAAAAAAGGQ." +
                    " Example for folder path: /Team1/Task1.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("/.*|id:.*")))
            .defaultValue("/")
            .build();

    public static final PropertyDescriptor RECURSIVE_SEARCH = new PropertyDescriptor.Builder()
            .name("recursive-search")
            .displayName("Search Recursively")
            .description("Indicates whether to list files from subfolders of the Dropbox folder.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor MIN_AGE = new PropertyDescriptor.Builder()
            .name("min-age")
            .displayName("Minimum File Age")
            .description("The minimum age a file must be in order to be considered; any files newer than this will be ignored.")
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
            CREDENTIAL_SERVICE,
            FOLDER,
            RECURSIVE_SEARCH,
            MIN_AGE,
            LISTING_STRATEGY,
            TRACKING_STATE_CACHE,
            TRACKING_TIME_WINDOW,
            INITIAL_LISTING_TARGET,
            RECORD_WRITER,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxySpec.HTTP_AUTH)
    );

    private volatile DbxClientV2 dropboxApiClient;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        dropboxApiClient = getDropboxApiClient(context, getIdentifier());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Map<String, String> createAttributes(
            final DropboxFileInfo entity,
            final ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();

        for (DropboxFlowFileAttribute attribute : DropboxFlowFileAttribute.values()) {
            Optional.ofNullable(attribute.getValue(entity))
                    .ifPresent(value -> attributes.put(attribute.getName(), value));
        }

        return attributes;
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(FOLDER).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected List<DropboxFileInfo> performListing(ProcessContext context, Long minTimestamp,
            ListingMode listingMode) throws IOException {
        final List<DropboxFileInfo> listing = new ArrayList<>();

        final String folderName = getPath(context);
        final Boolean recursive = context.getProperty(RECURSIVE_SEARCH).asBoolean();
        final Long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);

        try {
            Predicate<FileMetadata> metadataFilter = createMetadataFilter(minTimestamp, minAge);

            final DbxUserListFolderBuilder listFolderBuilder = dropboxApiClient.files().listFolderBuilder(convertFolderName(folderName));
            ListFolderResult result = listFolderBuilder
                    .withRecursive(recursive)
                    .start();

            final List<FileMetadata> metadataList = new ArrayList<>(filterMetadata(result, metadataFilter));

            while (result.getHasMore()) {
                result = dropboxApiClient.files().listFolderContinue(result.getCursor());
                metadataList.addAll(filterMetadata(result, metadataFilter));
            }

            for (final FileMetadata metadata : metadataList) {
                final DropboxFileInfo.Builder builder = new DropboxFileInfo.Builder()
                        .id(metadata.getId())
                        .path(getParentPath(metadata.getPathDisplay()))
                        .name(metadata.getName())
                        .size(metadata.getSize())
                        .timestamp(metadata.getServerModified().getTime())
                        .revision(metadata.getRev());

                listing.add(builder.build());
            }
        } catch (DbxException e) {
            throw new IOException("Failed to list Dropbox folder [" + folderName + "]", e);
        }

        return listing;
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return LISTING_STRATEGY.equals(property)
                || FOLDER.equals(property)
                || RECURSIVE_SEARCH.equals(property);
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        return Scope.CLUSTER;
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return DropboxFileInfo.getRecordSchema();
    }

    @Override
    protected Integer countUnfilteredListing(final ProcessContext context) throws IOException {
        return performListing(context, null, ListingMode.CONFIGURATION_VERIFICATION).size();
    }

    @Override
    protected String getListingContainerName(final ProcessContext context) {
        return format("Dropbox Folder [%s]", getPath(context));
    }

    private Predicate<FileMetadata> createMetadataFilter(Long minTimestamp, Long minAge) {
        Predicate<FileMetadata> metadataFilter = FileMetadata::getIsDownloadable;

        if (minTimestamp != null && minTimestamp > 0) {
            metadataFilter = metadataFilter.and(metadata -> metadata.getServerModified().getTime() >= minTimestamp);
        }

        if (minAge != null && minAge > 0) {
            long maxTimestamp = System.currentTimeMillis() - minAge;
            metadataFilter = metadataFilter.and(metadata -> metadata.getServerModified().getTime() < maxTimestamp);
        }
        return metadataFilter;
    }

    private List<FileMetadata> filterMetadata(ListFolderResult result, Predicate<FileMetadata> metadataFilter) {
        return result.getEntries().stream()
                .filter(metadata -> metadata instanceof FileMetadata)
                .map(FileMetadata.class::cast)
                .filter(metadataFilter)
                .collect(toList());
    }


}
