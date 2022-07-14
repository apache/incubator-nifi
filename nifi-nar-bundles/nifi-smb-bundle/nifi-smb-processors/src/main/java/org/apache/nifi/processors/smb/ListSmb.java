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

import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static org.apache.nifi.components.state.Scope.CLUSTER;
import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.TIME_PERIOD_VALIDATOR;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.smb.SmbSessionProviderService;

@PrimaryNodeOnly
@TriggerSerially
@Tags({"microsoft", "storage", "samba"})
@SeeAlso({PutSmbFile.class, GetSmbFile.class})
@CapabilityDescription("Retrieves a listing of files shared via SMB protocol. For each file that is listed, " +
        "creates a FlowFile that represents the file. This Processor is designed to run on Primary Node only in " +
        "a cluster. If the primary node changes, the new Primary Node will pick up where the previous node left " +
        "off without duplicating all of the data.")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file that was read from filesystem."),
        @WritesAttribute(attribute = "shortname", description = "The short name of the file that was read from filesystem."),
        @WritesAttribute(attribute = "path", description =
                "The path is set to the relative path of the file's directory "
                        + "on filesystem compared to the Share and Input Directory properties and the configured host "
                        + "and port inherited from the configured connection pool controller service. For example, for "
                        + "a given remote location smb://HOSTNAME:PORT/SHARE:\\DIRECTORY, and a file is being listed from "
                        + "smb://HOSTNAME:PORT/SHARE:DIRECTORY\\sub\\folder\\file then the path attribute will be set to \"sub\\folder\\file\"."),
        @WritesAttribute(attribute = "absolute.path", description =
                "The absolute.path is set to the absolute path of the file's directory on the remote location. For example, "
                        + "given a remote location smb://HOSTNAME:PORT/SHARE:\\DIRECTORY, and a file is being listen from "
                        + "SHARE:\\DIRECTORY\\sub\\folder\\file then the absolute.path attribute will be set to "
                        + "\"SHARE:\\DIRECTORY\\sub\\folder\\file\"."),
        @WritesAttribute(attribute = "identifier", description =
                "The identifier of the file. This equals to the path attribute so two files with the same relative path "
                        + "coming from different file shares considered to be identical."),
        @WritesAttribute(attribute = "timestamp", description =
                "The timestamp of when the file's content in the filesystem as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute = "createTime", description =
                "The timestamp of when the file was created in the filesystem as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute = "lastAccessTime", description =
                "The timestamp of when the file was accessed in the filesystem as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute = "changeTime", description =
                "The timestamp of when the file's attributes was changed in the filesystem as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute = "size", description = "The number of bytes in the source file"),
        @WritesAttribute(attribute = "allocationSize", description = "The number of bytes allocated for the file on the server"),
})
@Stateful(scopes = {Scope.CLUSTER}, description =
        "After performing a listing of files, the state of the previous listing can be stored in order to list files "
                + "continuously without duplication."
)
public class ListSmb extends AbstractListProcessor<SmbListableEntity> {

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .displayName("Input Directory")
            .name("directory")
            .description("The network folder to which files should be written. This is the remaining relative " +
                    "after the hostname: smb://HOSTNAME:PORT/SHARE/[DIRECTORY]\\sub\\directories. It is also possible "
                    + " to add subdirectories using this property. The given path on the remote file share must exists. "
                    + "The existence of the remote folder can be checked using verification. You may mix different "
                    + "directory separators in this property. If so NiFi will unify all of them and will use windows's"
                    + "directory separator: '\\' ")
            .required(false)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor MINIMUM_AGE = new PropertyDescriptor.Builder()
            .displayName("Minimum File Age in milliseconds")
            .name("min-file-age")
            .description(
                    "Any file younger then the given value will be omitted. Ideally this value should be greater then"
                            + "the amount of time needed to perform a list.")
            .required(true)
            .addValidator(TIME_PERIOD_VALIDATOR)
            .defaultValue("5ms")
            .build();

    public static final PropertyDescriptor MINIMUM_SIZE = new PropertyDescriptor.Builder()
            .displayName("Minimum File Size in bytes")
            .name("min-file-size")
            .description("Any file smaller then the given value will be omitted.")
            .required(false)
            .addValidator(NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAXIMUM_SIZE = new PropertyDescriptor.Builder()
            .displayName("Maximum File Size in bytes")
            .name("max-file-size")
            .description("Any file bigger then the given value will be omitted.")
            .required(false)
            .addValidator(NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();


    public static final PropertyDescriptor SMB_LISTING_STRATEGY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(LISTING_STRATEGY)
            .allowableValues(BY_ENTITIES, NO_TRACKING, BY_TIMESTAMPS)
            .build();

    public static final PropertyDescriptor SMB_CONNECTION_POOL_SERVICE = new Builder()
            .name("smb-connection-pool-service")
            .displayName("SMB Connection Pool Service")
            .description("Specifies the SMB Connection Pool to use for creating SMB connections.")
            .required(true)
            .identifiesControllerService(SmbSessionProviderService.class)
            .build();

    public static final PropertyDescriptor SKIP_FILES_WITH_SUFFIX = new Builder()
            .name("file-name-suffix-filter")
            .displayName("File Name Suffix Filter")
            .description("Files ends with the given suffix will be omitted. This is handy when writing large data into "
                    + "temporary files and then moved to a final one. Please be advised that writing data into files "
                    + "first is highly recommended when using Entity Tracking or Timestamp based listing strategies.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .addValidator(new MustNotContainDirectorySeparatorsValidator())
            .build();
    public static final PropertyDescriptor SHARE = new PropertyDescriptor.Builder()
            .displayName("Share")
            .name("share")
            .description("The network share to which files should be listed from. This is the \"first folder\"" +
                    "after the hostname: smb://hostname:port\\[share]\\dir1\\dir2")
            .required(false)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = unmodifiableList(asList(
            SMB_LISTING_STRATEGY,
            SMB_CONNECTION_POOL_SERVICE,
            SHARE,
            DIRECTORY,
            AbstractListProcessor.RECORD_WRITER,
            SKIP_FILES_WITH_SUFFIX,
            MINIMUM_AGE,
            MINIMUM_SIZE,
            MAXIMUM_SIZE,
            AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION,
            ListedEntityTracker.TRACKING_STATE_CACHE,
            ListedEntityTracker.TRACKING_TIME_WINDOW,
            ListedEntityTracker.INITIAL_LISTING_TARGET
    ));

    NiFiSmbClientFactory smbClientFactory = new NiFiSmbClientFactory();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Map<String, String> createAttributes(SmbListableEntity entity, ProcessContext context) {
        final Map<String, String> attributes = new TreeMap<>();
        attributes.put("filename", entity.getName());
        attributes.put("shortname", entity.getShortName());
        attributes.put("path", entity.getPath());
        attributes.put("absolute.path", getPath(context) + entity.getPathWithName());
        attributes.put("identifier", entity.getIdentifier());
        attributes.put("timestamp",
                ISO_DATE_TIME.format(LocalDateTime.ofEpochSecond(entity.getTimestamp(), 0, ZoneOffset.UTC)));
        attributes.put("creationTime",
                ISO_DATE_TIME.format(LocalDateTime.ofEpochSecond(entity.getCreationTime(), 0, ZoneOffset.UTC)));
        attributes.put("lastAccessedTime",
                ISO_DATE_TIME.format(LocalDateTime.ofEpochSecond(entity.getLastAccessTime(), 0, ZoneOffset.UTC)));
        attributes.put("changeTime",
                ISO_DATE_TIME.format(LocalDateTime.ofEpochSecond(entity.getChangeTime(), 0, ZoneOffset.UTC)));
        attributes.put("size", String.valueOf(entity.getSize()));
        attributes.put("allocationSize", String.valueOf(entity.getAllocationSize()));
        return unmodifiableMap(attributes);
    }

    @Override
    protected String getPath(ProcessContext context) {
        final SmbSessionProviderService connectionPoolService =
                context.getProperty(SMB_CONNECTION_POOL_SERVICE).asControllerService(SmbSessionProviderService.class);
        final URI serviceLocation = connectionPoolService.getServiceLocation();
        final String directory = getDirectory(context);
        final String shareName = context.getProperty(SHARE).isSet() ? "/" + context.getProperty(SHARE).getValue() : "";
        return String.format("%s%s:\\%s", serviceLocation.toString(), shareName,
                directory.isEmpty() ? "" : directory + "\\");
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
                if (!isExecutionScheduled(listingMode)) {
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
        return asList(SMB_CONNECTION_POOL_SERVICE, DIRECTORY, SKIP_FILES_WITH_SUFFIX).contains(property);
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

    private boolean isExecutionScheduled(ListingMode listingMode) {
        return ListingMode.CONFIGURATION_VERIFICATION.equals(listingMode) || isScheduled();
    }

    private Predicate<SmbListableEntity> createFileFilter(ProcessContext context, Long minTimestampOrNull) {

        final Long minimumAge = context.getProperty(MINIMUM_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Integer minimumSizeOrNull =
                context.getProperty(MINIMUM_SIZE).isSet() ? context.getProperty(MINIMUM_SIZE).asInteger() : null;
        final Integer maximumSizeOrNull =
                context.getProperty(MAXIMUM_SIZE).isSet() ? context.getProperty(MAXIMUM_SIZE).asInteger() : null;
        final String suffixOrNull = context.getProperty(SKIP_FILES_WITH_SUFFIX).getValue();

        final long now = getCurrentTime();
        Predicate<SmbListableEntity> filter = entity -> now - entity.getTimestamp() >= minimumAge;

        if (minTimestampOrNull != null) {
            filter = filter.and(entity -> entity.getTimestamp() >= minTimestampOrNull);
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
        final SmbSessionProviderService connectionPoolService =
                context.getProperty(SMB_CONNECTION_POOL_SERVICE).asControllerService(SmbSessionProviderService.class);
        final String directory = getDirectory(context);
        final String share = context.getProperty(SHARE).isSet() ? context.getProperty(SHARE).getValue() : "";
        final NiFiSmbClient smbClient = smbClientFactory.create(connectionPoolService.getSession(), share);
        return smbClient.listRemoteFiles(directory).onClose(smbClient::close);
    }

    private String getDirectory(ProcessContext context) {
        final PropertyValue property = context.getProperty(DIRECTORY);
        final String directory = property.isSet() ? NiFiSmbClient.unifyDirectorySeparators(property.getValue()) : "";
        return directory.equals("\\") ? "" : directory;
    }

    private static class MustNotContainDirectorySeparatorsValidator implements Validator {

        @Override
        public ValidationResult validate(String subject, String value, ValidationContext context) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(value)
                    .valid(!value.contains("\\"))
                    .explanation(subject + " must not contain any folder separator character.")
                    .build();
        }
    }
}
