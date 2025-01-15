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
package org.apache.nifi.processors.smb;

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.nifi.components.state.Scope.CLUSTER;
import static org.apache.nifi.processor.util.StandardValidators.DATA_SIZE_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.TIME_PERIOD_VALIDATOR;
import static org.apache.nifi.services.smb.SmbListableEntity.ALLOCATION_SIZE;
import static org.apache.nifi.services.smb.SmbListableEntity.CHANGE_TIME;
import static org.apache.nifi.services.smb.SmbListableEntity.LAST_MODIFIED_TIME;
import static org.apache.nifi.services.smb.SmbListableEntity.CREATION_TIME;
import static org.apache.nifi.services.smb.SmbListableEntity.FILENAME;
import static org.apache.nifi.services.smb.SmbListableEntity.LAST_ACCESS_TIME;
import static org.apache.nifi.services.smb.SmbListableEntity.PATH;
import static org.apache.nifi.services.smb.SmbListableEntity.SERVICE_LOCATION;
import static org.apache.nifi.services.smb.SmbListableEntity.SHORT_NAME;
import static org.apache.nifi.services.smb.SmbListableEntity.SIZE;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbListableEntity;

@PrimaryNodeOnly
@TriggerSerially
@Tags({"samba, smb, cifs, files", "list"})
@SeeAlso({PutSmbFile.class, GetSmbFile.class, FetchSmb.class})
@CapabilityDescription("Lists concrete files shared via SMB protocol. " +
        "Each listed file may result in one flowfile, the metadata being written as flowfile attributes. " +
        "Or - in case the 'Record Writer' property is set - the entire result is written as records to a single flowfile. "
        +
        "This Processor is designed to run on Primary Node only in a cluster. If the primary node changes, the new Primary Node will pick up where the "
        +
        "previous node left off without duplicating all of the data.")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = FILENAME, description = "The name of the file that was read from filesystem."),
        @WritesAttribute(attribute = SHORT_NAME, description = "The short name of the file that was read from filesystem."),
        @WritesAttribute(attribute = PATH, description =
                "The path is set to the relative path of the file's directory on the remote filesystem compared to the "
                        + "Share root directory. For example, for a given remote location"
                        + "smb://HOSTNAME:PORT/SHARE/DIRECTORY, and a file is being listed from "
                        + "smb://HOSTNAME:PORT/SHARE/DIRECTORY/sub/folder/file then the path attribute will be set to "
                        + "\"DIRECTORY/sub/folder\"."),
        @WritesAttribute(attribute = SERVICE_LOCATION, description =
                "The SMB URL of the share."),
        @WritesAttribute(attribute = LAST_MODIFIED_TIME, description =
                "The timestamp of when the file's content changed in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = CREATION_TIME, description =
                "The timestamp of when the file was created in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = LAST_ACCESS_TIME, description =
                "The timestamp of when the file was accessed in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = CHANGE_TIME, description =
                "The timestamp of when the file's attributes was changed in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = SIZE, description = "The size of the file in bytes."),
        @WritesAttribute(attribute = ALLOCATION_SIZE, description = "The number of bytes allocated for the file on the server."),
})
@Stateful(scopes = {Scope.CLUSTER}, description =
        "After performing a listing of files, the state of the previous listing can be stored in order to list files "
                + "continuously without duplication."
)
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListSmb extends AbstractListProcessor<SmbListableEntity> {

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .displayName("Input Directory")
            .name("directory")
            .description("The network folder from which to list files. This is the remaining relative path " +
                    "after the share: smb://HOSTNAME:PORT/SHARE/[DIRECTORY]/sub/directories. It is also possible "
                    + "to add subdirectories. The given path on the remote file share must exist. "
                    + "This can be checked using verification. You may mix Windows and Linux-style "
                    + "directory separators.")
            .required(false)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor MINIMUM_AGE = new PropertyDescriptor.Builder()
            .displayName("Minimum File Age")
            .name("min-file-age")
            .description("The minimum age that a file must be in order to be listed; any file younger than this "
                    + "amount of time will be ignored.")
            .required(true)
            .addValidator(TIME_PERIOD_VALIDATOR)
            .defaultValue("5 secs")
            .build();

    public static final PropertyDescriptor MAXIMUM_AGE = new PropertyDescriptor.Builder()
            .displayName("Maximum File Age")
            .name("max-file-age")
            .description("Any file older than the given value will be omitted.")
            .required(false)
            .addValidator(TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MINIMUM_SIZE = new PropertyDescriptor.Builder()
            .displayName("Minimum File Size")
            .name("min-file-size")
            .description("Any file smaller than the given value will be omitted.")
            .required(false)
            .addValidator(DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAXIMUM_SIZE = new PropertyDescriptor.Builder()
            .displayName("Maximum File Size")
            .name("max-file-size")
            .description("Any file larger than the given value will be omitted.")
            .required(false)
            .addValidator(DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor SMB_LISTING_STRATEGY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(LISTING_STRATEGY)
            .allowableValues(BY_ENTITIES, NO_TRACKING, BY_TIMESTAMPS)
            .build();

    public static final PropertyDescriptor SMB_CLIENT_PROVIDER_SERVICE = new Builder()
            .name("smb-client-provider-service")
            .displayName("SMB Client Provider Service")
            .description("Specifies the SMB client provider to use for creating SMB connections.")
            .required(true)
            .identifiesControllerService(SmbClientProviderService.class)
            .build();

    public static final PropertyDescriptor FILE_NAME_SUFFIX_FILTER = new Builder()
            .name("file-name-suffix-filter")
            .displayName("File Name Suffix Filter")
            .description("Files ending with the given suffix will be omitted. Can be used to make sure that files "
                    + "that are still uploading are not listed multiple times, by having those files have a suffix "
                    + "and remove the suffix once the upload finishes. This is highly recommended when using "
                    + "'Tracking Entities' or 'Tracking Timestamps' listing strategies.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .addValidator(new MustNotContainDirectorySeparatorsValidator())
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SMB_CLIENT_PROVIDER_SERVICE,
            SMB_LISTING_STRATEGY,
            DIRECTORY,
            FILE_NAME_SUFFIX_FILTER,
            AbstractListProcessor.RECORD_WRITER,
            MINIMUM_AGE,
            MAXIMUM_AGE,
            MINIMUM_SIZE,
            MAXIMUM_SIZE,
            AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION,
            ListedEntityTracker.TRACKING_STATE_CACHE,
            ListedEntityTracker.TRACKING_TIME_WINDOW,
            ListedEntityTracker.INITIAL_LISTING_TARGET
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Map<String, String> createAttributes(SmbListableEntity entity, ProcessContext context) {
        final Map<String, String> attributes = new TreeMap<>();
        final SmbClientProviderService clientProviderService =
                context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
        attributes.put(FILENAME, entity.getName());
        attributes.put(SHORT_NAME, entity.getShortName());
        attributes.put(PATH, entity.getPath());
        attributes.put(SERVICE_LOCATION, clientProviderService.getServiceLocation().toString());
        attributes.put(LAST_MODIFIED_TIME, formatTimeStamp(entity.getLastModifiedTime()));
        attributes.put(CREATION_TIME, formatTimeStamp(entity.getCreationTime()));
        attributes.put(LAST_ACCESS_TIME, formatTimeStamp(entity.getLastAccessTime()));
        attributes.put(CHANGE_TIME, formatTimeStamp(entity.getChangeTime()));
        attributes.put(SIZE, String.valueOf(entity.getSize()));
        attributes.put(ALLOCATION_SIZE, String.valueOf(entity.getAllocationSize()));
        return unmodifiableMap(attributes);
    }

    @Override
    protected String getPath(ProcessContext context) {
        final SmbClientProviderService clientProviderService =
                context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
        final URI serviceLocation = clientProviderService.getServiceLocation();
        final String directory = getDirectory(context);
        return String.format("%s/%s", serviceLocation.toString(), directory.isEmpty() ? "" : directory + "/");
    }

    @Override
    protected List<SmbListableEntity> performListing(ProcessContext context, Long minimumTimestampOrNull,
            ListingMode listingMode) throws IOException {

        final Predicate<SmbListableEntity> fileFilter =
                createFileFilter(context, minimumTimestampOrNull);

        try (Stream<SmbListableEntity> listing = performListing(context)) {
            final Iterator<SmbListableEntity> iterator = listing.iterator();
            final List<SmbListableEntity> result = new LinkedList<>();
            while (iterator.hasNext()) {
                if (isExecutionStopped(listingMode)) {
                    return emptyList();
                }
                final SmbListableEntity entity = iterator.next();
                if (fileFilter.test(entity)) {
                    result.add(entity);
                }
            }
            return result;
        } catch (Exception e) {
            throw new IOException("Could not perform listing", e);
        }
    }

    @Override
    protected boolean isListingResetNecessary(PropertyDescriptor property) {
        return asList(SMB_CLIENT_PROVIDER_SERVICE, DIRECTORY, FILE_NAME_SUFFIX_FILTER).contains(property);
    }

    @Override
    protected Scope getStateScope(PropertyContext context) {
        return CLUSTER;
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return SmbListableEntity.getRecordSchema();
    }

    @Override
    protected Integer countUnfilteredListing(ProcessContext context) throws IOException {
        try (Stream<SmbListableEntity> listing = performListing(context)) {
            return Long.valueOf(listing.count()).intValue();
        } catch (Exception e) {
            throw new IOException("Could not count files", e);
        }
    }

    @Override
    protected String getListingContainerName(ProcessContext context) {
        return String.format("Remote Directory [%s]", getPath(context));
    }

    private String formatTimeStamp(long timestamp) {
        return ISO_DATE_TIME.format(
                LocalDateTime.ofEpochSecond(MILLISECONDS.toSeconds(timestamp), 0, UTC));
    }

    private boolean isExecutionStopped(ListingMode listingMode) {
        return ListingMode.EXECUTION.equals(listingMode) && !isScheduled();
    }

    private Predicate<SmbListableEntity> createFileFilter(ProcessContext context, Long minTimestampOrNull) {

        final Long minimumAge = context.getProperty(MINIMUM_AGE).asTimePeriod(MILLISECONDS);
        final Long maximumAgeOrNull = context.getProperty(MAXIMUM_AGE).isSet() ? context.getProperty(MAXIMUM_AGE)
                .asTimePeriod(MILLISECONDS) : null;
        final Double minimumSizeOrNull =
                context.getProperty(MINIMUM_SIZE).isSet() ? context.getProperty(MINIMUM_SIZE).asDataSize(DataUnit.B)
                        : null;
        final Double maximumSizeOrNull =
                context.getProperty(MAXIMUM_SIZE).isSet() ? context.getProperty(MAXIMUM_SIZE).asDataSize(DataUnit.B)
                        : null;
        final String suffixOrNull = context.getProperty(FILE_NAME_SUFFIX_FILTER).getValue();

        final long now = getCurrentTime();
        Predicate<SmbListableEntity> filter = entity -> now - entity.getLastModifiedTime() >= minimumAge;

        if (maximumAgeOrNull != null) {
            filter = filter.and(entity -> now - entity.getLastModifiedTime() <= maximumAgeOrNull);
        }

        if (minTimestampOrNull != null) {
            filter = filter.and(entity -> entity.getLastModifiedTime() >= minTimestampOrNull);
        }

        if (minimumSizeOrNull != null) {
            filter = filter.and(entity -> entity.getSize() >= minimumSizeOrNull);
        }

        if (maximumSizeOrNull != null) {
            filter = filter.and(entity -> entity.getSize() <= maximumSizeOrNull);
        }

        if (suffixOrNull != null) {
            filter = filter.and(entity -> !entity.getName().endsWith(suffixOrNull));
        }

        return filter;
    }

    private Stream<SmbListableEntity> performListing(ProcessContext context) throws IOException {
        final SmbClientProviderService clientProviderService =
                context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
        final String directory = getDirectory(context);
        final SmbClientService clientService = clientProviderService.getClient();
        return clientService.listFiles(directory).onClose(() -> {
            try {
                clientService.close();
            } catch (Exception e) {
                throw new RuntimeException("Could not close SMB client", e);
            }
        });
    }

    private String getDirectory(ProcessContext context) {
        final PropertyValue property = context.getProperty(DIRECTORY);
        final String directory = property.isSet() ? property.getValue().replace('\\', '/') : "";
        return "/".equals(directory) ? "" : directory;
    }

    private static class MustNotContainDirectorySeparatorsValidator implements Validator {

        @Override
        public ValidationResult validate(String subject, String value, ValidationContext context) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(value)
                    .valid(!value.contains("/") && !value.contains("\\"))
                    .explanation(subject + " must not contain any folder separator character.")
                    .build();
        }

    }

}