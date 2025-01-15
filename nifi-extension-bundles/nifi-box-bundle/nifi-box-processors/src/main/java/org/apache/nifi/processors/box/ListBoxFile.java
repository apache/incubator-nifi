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

import static org.apache.nifi.processors.box.BoxFileAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.PATH_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP_DESC;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.serialization.record.RecordSchema;

@PrimaryNodeOnly
@TriggerSerially
@Tags({"box", "storage"})
@CapabilityDescription("Lists files in a Box folder. " +
    "Each listed file may result in one FlowFile, the metadata being written as FlowFile attributes. " +
    "Or - in case the 'Record Writer' property is set - the entire result is written as records to a single FlowFile. " +
    "This Processor is designed to run on Primary Node only in a cluster. If the primary node changes, the new Primary Node will pick up where the " +
    "previous node left off without duplicating all of the data.")
@SeeAlso({FetchBoxFile.class, PutBoxFile.class})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
    @WritesAttribute(attribute = ID, description = ID_DESC),
    @WritesAttribute(attribute = "filename", description = FILENAME_DESC),
    @WritesAttribute(attribute = "path", description = PATH_DESC),
    @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
    @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC)})
@Stateful(scopes = {Scope.CLUSTER}, description = "The processor stores necessary data to be able to keep track what files have been listed already." +
    " What exactly needs to be stored depends on the 'Listing Strategy'.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListBoxFile extends AbstractListProcessor<BoxFileInfo> {
    public static final PropertyDescriptor FOLDER_ID = new PropertyDescriptor.Builder()
        .name("box-folder-id")
        .displayName("Folder ID")
        .description("The ID of the folder from which to pull list of files.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(true)
        .build();

    public static final PropertyDescriptor RECURSIVE_SEARCH = new PropertyDescriptor.Builder()
        .name("recursive-search")
        .displayName("Search Recursively")
        .description("When 'true', will include list of files from sub-folders." +
            " Otherwise, will return only files that are within the folder defined by the 'Folder ID' property.")
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
        BoxClientService.BOX_CLIENT_SERVICE,
        FOLDER_ID,
        RECURSIVE_SEARCH,
        MIN_AGE,
        LISTING_STRATEGY,
        TRACKING_STATE_CACHE,
        TRACKING_TIME_WINDOW,
        INITIAL_LISTING_TARGET,
        RECORD_WRITER
    );

    private volatile BoxAPIConnection boxAPIConnection;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Map<String, String> createAttributes(
        final BoxFileInfo entity,
        final ProcessContext context
    ) {
        final Map<String, String> attributes = new HashMap<>();

        for (BoxFlowFileAttribute attribute : BoxFlowFileAttribute.values()) {
            Optional.ofNullable(attribute.getValue(entity))
                .ifPresent(value -> attributes.put(attribute.getName(), value));
        }

        return attributes;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        BoxClientService boxClientService = context.getProperty(BoxClientService.BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);

        boxAPIConnection = boxClientService.getBoxApiConnection();
    }

    @Override
    protected String getListingContainerName(final ProcessContext context) {
        return String.format("Box Folder [%s]", getPath(context));
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
        return BoxFileInfo.getRecordSchema();
    }

    @Override
    protected String getDefaultTimePrecision() {
        return PRECISION_SECONDS.getValue();
    }

    @Override
    protected List<BoxFileInfo> performListing(
        final ProcessContext context,
        final Long minTimestamp,
        final ListingMode listingMode)  {

        final List<BoxFileInfo> listing = new ArrayList<>();

        final String folderId = context.getProperty(FOLDER_ID).evaluateAttributeExpressions().getValue();
        final Boolean recursive = context.getProperty(RECURSIVE_SEARCH).asBoolean();
        final Long minAge = context.getProperty(MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);

        long createdAtMax = Instant.now().toEpochMilli() - minAge;
        listFolder(listing, folderId, recursive, createdAtMax);
        return listing;
    }

    private void listFolder(List<BoxFileInfo> listing, String folderId, Boolean recursive, long createdAtMax) {
        BoxFolder folder = getFolder(folderId);
        for (BoxItem.Info itemInfo : folder.getChildren(
            "id",
            "name",
            "item_status",
            "size",
            "created_at",
            "modified_at",
            "content_created_at",
            "content_modified_at",
            "path_collection"
        )) {
            if (itemInfo instanceof BoxFile.Info info) {

                long createdAt = itemInfo.getCreatedAt().getTime();

                if (createdAt <= createdAtMax) {
                    BoxFileInfo boxFileInfo = new BoxFileInfo.Builder()
                        .id(info.getID())
                        .fileName(info.getName())
                        .path(BoxFileUtils.getParentPath(info))
                        .size(info.getSize())
                        .createdTime(info.getCreatedAt().getTime())
                        .modifiedTime(info.getModifiedAt().getTime())
                        .build();
                    listing.add(boxFileInfo);
                }
            } else if (recursive && itemInfo instanceof BoxFolder.Info info) {
                listFolder(listing, info.getID(), recursive, createdAtMax);
            }
        }
    }

    BoxFolder getFolder(String folderId) {
        return new BoxFolder(boxAPIConnection, folderId);
    }

    @Override
    protected Integer countUnfilteredListing(final ProcessContext context) {
        return performListing(context, null, ListingMode.CONFIGURATION_VERIFICATION).size();
    }
}
