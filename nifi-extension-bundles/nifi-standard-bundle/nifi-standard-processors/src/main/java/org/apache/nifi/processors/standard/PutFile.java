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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"put", "local", "copy", "archive", "files", "filesystem"})
@CapabilityDescription("Writes the contents of a FlowFile to the local file system")
@SeeAlso({FetchFile.class, GetFile.class})
@ReadsAttribute(attribute = "filename", description = "The filename to use when writing the FlowFile to disk.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
                        explanation = "Provides operator the ability to write to any file that NiFi has access to.")
        }
)
public class PutFile extends AbstractProcessor {

    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";

    public static final String FILE_MODIFY_DATE_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(FILE_MODIFY_DATE_ATTR_FORMAT);

    public static final Pattern RWX_PATTERN = Pattern.compile("^([r-][w-])([x-])([r-][w-])([x-])([r-][w-])([x-])$");
    public static final Pattern NUM_PATTERN = Pattern.compile("^[0-7]{3}$");

    private static final Validator PERMISSIONS_VALIDATOR = (subject, input, context) -> {
        ValidationResult.Builder vr = new ValidationResult.Builder();
        if (context.isExpressionLanguagePresent(input)) {
            return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
        }

        if (RWX_PATTERN.matcher(input).matches() || NUM_PATTERN.matcher(input).matches()) {
            return vr.valid(true).build();
        }
        return vr.valid(false)
                .subject(subject)
                .input(input)
                .explanation("This must be expressed in rwxr-x--- form or octal triplet form.")
                .build();
    };

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The directory to which files should be written. You may use expression language such as /aa/bb/${path}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MAX_DESTINATION_FILES = new PropertyDescriptor.Builder()
            .name("Maximum File Count")
            .description("Specifies the maximum number of files that can exist in the output directory")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION)
            .allowableValues(REPLACE_RESOLUTION, IGNORE_RESOLUTION, FAIL_RESOLUTION)
            .build();
    public static final PropertyDescriptor CHANGE_LAST_MODIFIED_TIME = new PropertyDescriptor.Builder()
            .name("Last Modified Time")
            .description("Sets the lastModifiedTime on the output file to the value of this attribute.  Format must be yyyy-MM-dd'T'HH:mm:ssZ.  "
                    + "You may also use expression language such as ${file.lastModifiedTime}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_PERMISSIONS = new PropertyDescriptor.Builder()
            .name("Permissions")
            .description("Sets the permissions on the output file to the value of this attribute.  Format must be either UNIX rwxrwxrwx with a - in "
                    + "place of denied permissions (e.g. rw-r--r--) or an octal number (e.g. 644).  You may also use expression language such as "
                    + "${file.permissions}.")
            .required(false)
            .addValidator(PERMISSIONS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_OWNER = new PropertyDescriptor.Builder()
            .name("Owner")
            .description("Sets the owner on the output file to the value of this attribute.  You may also use expression language such as "
                    + "${file.owner}. Note on many operating systems Nifi must be running as a super-user to have the permissions to set the file owner.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CHANGE_GROUP = new PropertyDescriptor.Builder()
            .name("Group")
            .description("Sets the group on the output file to the value of this attribute.  You may also use expression language such "
                    + "as ${file.group}.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor CREATE_DIRS = new PropertyDescriptor.Builder()
            .name("Create Missing Directories")
            .description("If true, then missing destination directories will be created. If false, flowfiles are penalized and sent to failure.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            DIRECTORY,
            CONFLICT_RESOLUTION,
            CREATE_DIRS,
            MAX_DESTINATION_FILES,
            CHANGE_LAST_MODIFIED_TIME,
            CHANGE_PERMISSIONS,
            CHANGE_OWNER,
            CHANGE_GROUP
    );

    public static final int MAX_FILE_LOCK_ATTEMPTS = 10;
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to the output directory are transferred to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output directory for some reason are transferred to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final Path configuredRootDirPath = Paths.get(context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue());
        final String conflictResponse = context.getProperty(CONFLICT_RESOLUTION).getValue();
        final Integer maxDestinationFiles = context.getProperty(MAX_DESTINATION_FILES).asInteger();
        final ComponentLog logger = getLogger();

        Path tempDotCopyFile = null;
        try {
            final Path rootDirPath = configuredRootDirPath.toAbsolutePath();
            String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            final Path tempCopyFile = rootDirPath.resolve("." + filename);
            final Path copyFile = rootDirPath.resolve(filename);

            final String permissions = context.getProperty(CHANGE_PERMISSIONS).evaluateAttributeExpressions(flowFile).getValue();
            final String owner = context.getProperty(CHANGE_OWNER).evaluateAttributeExpressions(flowFile).getValue();
            final String group = context.getProperty(CHANGE_GROUP).evaluateAttributeExpressions(flowFile).getValue();
            if (!Files.exists(rootDirPath)) {
                if (context.getProperty(CREATE_DIRS).asBoolean()) {
                    Path existing = rootDirPath;
                    while (!Files.exists(existing)) {
                        existing = existing.getParent();
                    }
                    if (permissions != null && !permissions.trim().isEmpty()) {
                        try {
                            String perms = stringPermissions(permissions, true);
                            if (!perms.isEmpty()) {
                                Files.createDirectories(rootDirPath, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(perms)));
                            } else {
                                Files.createDirectories(rootDirPath);
                            }
                        } catch (Exception e) {
                            flowFile = session.penalize(flowFile);
                            session.transfer(flowFile, REL_FAILURE);
                            logger.error("Could not set create directory with permissions {}", permissions, e);
                            return;
                        }
                    } else {
                        Files.createDirectories(rootDirPath);
                    }

                    boolean chOwner = owner != null && !owner.trim().isEmpty();
                    boolean chGroup = group != null && !group.trim().isEmpty();
                    if (chOwner || chGroup) {
                        Path currentPath = rootDirPath;
                        while (!currentPath.equals(existing)) {
                            if (chOwner) {
                                try {
                                    UserPrincipalLookupService lookupService = currentPath.getFileSystem().getUserPrincipalLookupService();
                                    Files.setOwner(currentPath, lookupService.lookupPrincipalByName(owner));
                                } catch (Exception e) {
                                    logger.warn("Could not set directory owner to {}", owner, e);
                                }
                            }
                            if (chGroup) {
                                try {
                                    UserPrincipalLookupService lookupService = currentPath.getFileSystem().getUserPrincipalLookupService();
                                    PosixFileAttributeView view = Files.getFileAttributeView(currentPath, PosixFileAttributeView.class);
                                    view.setGroup(lookupService.lookupPrincipalByGroupName(group));
                                } catch (Exception e) {
                                    logger.warn("Could not set file group to {}", group, e);
                                }
                            }
                            currentPath = currentPath.getParent();
                        }
                    }
                } else {
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                    logger.error("Penalizing {} and routing to 'failure' because the output directory {} does not exist and Processor is configured not to create missing directories",
                            flowFile, rootDirPath);
                    return;
                }
            }

            final Path dotCopyFile = tempCopyFile;
            tempDotCopyFile = dotCopyFile;
            Path finalCopyFile = copyFile;

            final Path finalCopyFileDir = finalCopyFile.getParent();
            if (Files.exists(finalCopyFileDir) && maxDestinationFiles != null) { // check if too many files already
                final long numFiles = getFilesNumberInFolder(finalCopyFileDir, filename);

                if (numFiles >= maxDestinationFiles) {
                    flowFile = session.penalize(flowFile);
                    logger.warn("Penalizing {} and routing to 'failure' because the output directory {} has {} files, which exceeds the configured maximum number of files",
                            flowFile, finalCopyFileDir, numFiles);
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            }

            if (Files.exists(finalCopyFile)) {
                switch (conflictResponse) {
                    case REPLACE_RESOLUTION:
                        Files.delete(finalCopyFile);
                        logger.info("Deleted {} as configured in order to replace with the contents of {}", finalCopyFile, flowFile);
                        break;
                    case IGNORE_RESOLUTION:
                        session.transfer(flowFile, REL_SUCCESS);
                        logger.info("Transferring {} to success because file with same name already exists", flowFile);
                        return;
                    case FAIL_RESOLUTION:
                        flowFile = session.penalize(flowFile);
                        logger.warn("Penalizing {} and routing to failure as configured because file with the same name already exists", flowFile);
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    default:
                        break;
                }
            }

            session.exportTo(flowFile, dotCopyFile, false);

            final String lastModifiedTime = context.getProperty(CHANGE_LAST_MODIFIED_TIME).evaluateAttributeExpressions(flowFile).getValue();
            if (lastModifiedTime != null && !lastModifiedTime.trim().isEmpty()) {
                try {
                    final OffsetDateTime fileModifyTime = OffsetDateTime.parse(lastModifiedTime, DATE_TIME_FORMATTER);
                    dotCopyFile.toFile().setLastModified(fileModifyTime.toInstant().toEpochMilli());
                } catch (Exception e) {
                    logger.warn("Could not set file lastModifiedTime to {}", lastModifiedTime, e);
                }
            }

            if (permissions != null && !permissions.trim().isEmpty()) {
                try {
                    String perms = stringPermissions(permissions, false);
                    if (!perms.isEmpty()) {
                        Files.setPosixFilePermissions(dotCopyFile, PosixFilePermissions.fromString(perms));
                    }
                } catch (Exception e) {
                    logger.warn("Could not set file permissions to {}", permissions, e);
                }
            }

            if (owner != null && !owner.trim().isEmpty()) {
                try {
                    UserPrincipalLookupService lookupService = dotCopyFile.getFileSystem().getUserPrincipalLookupService();
                    Files.setOwner(dotCopyFile, lookupService.lookupPrincipalByName(owner));
                } catch (Exception e) {
                    logger.warn("Could not set file owner to {}", owner, e);
                }
            }

            if (group != null && !group.trim().isEmpty()) {
                try {
                    UserPrincipalLookupService lookupService = dotCopyFile.getFileSystem().getUserPrincipalLookupService();
                    PosixFileAttributeView view = Files.getFileAttributeView(dotCopyFile, PosixFileAttributeView.class);
                    view.setGroup(lookupService.lookupPrincipalByGroupName(group));
                } catch (Exception e) {
                    logger.warn("Could not set file group to {}", group, e);
                }
            }

            boolean renamed = false;
            for (int i = 0; i < 10; i++) { // try rename up to 10 times.
                if (dotCopyFile.toFile().renameTo(finalCopyFile.toFile())) {
                    renamed = true;
                    break; // rename was successful
                }
                Thread.sleep(100L); // try waiting a few ms to let whatever might cause rename failure to resolve
            }

            if (!renamed) {
                if (Files.exists(dotCopyFile) && dotCopyFile.toFile().delete()) {
                    logger.debug("Deleted dot copy file {}", dotCopyFile);
                }
                throw new ProcessException("Could not rename: " + dotCopyFile);
            } else {
                logger.info("Produced copy of {} at location {}", flowFile, finalCopyFile);
            }

            session.getProvenanceReporter().send(flowFile, finalCopyFile.toFile().toURI().toString(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Throwable t) {
            if (tempDotCopyFile != null) {
                try {
                    Files.deleteIfExists(tempDotCopyFile);
                } catch (final Exception e) {
                    logger.error("Unable to remove temporary file {}", tempDotCopyFile, e);
                }
            }

            flowFile = session.penalize(flowFile);
            logger.error("Penalizing {} and transferring to failure", flowFile, t);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private long getFilesNumberInFolder(Path folder, String filename) {
        String[] filesInFolder = folder.toFile().list();
        return Arrays.stream(filesInFolder)
                .filter(eachFilename -> !eachFilename.equals(filename))
                .count();
    }

    protected String stringPermissions(String perms, boolean directory) {
        String permissions = "";
        Matcher rwx = RWX_PATTERN.matcher(perms);
        if (rwx.matches()) {
            if (directory) {
                // To read or write, directory access will be required
                StringBuilder permBuilder = new StringBuilder();
                permBuilder.append("$1");
                permBuilder.append(rwx.group(1).equals("--") ? "$2" : "x");
                permBuilder.append("$3");
                permBuilder.append(rwx.group(3).equals("--") ? "$4" : "x");
                permBuilder.append("$5");
                permBuilder.append(rwx.group(5).equals("--") ? "$6" : "x");
                permissions = rwx.replaceAll(permBuilder.toString());
            } else {
                permissions = perms;
            }
        } else if (NUM_PATTERN.matcher(perms).matches()) {
            try {
                int number = Integer.parseInt(perms, 8);
                StringBuilder permBuilder = new StringBuilder();
                if ((number & 0x100) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x80) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if (directory || (number & 0x40) > 0) {
                    permBuilder.append('x');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x20) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x10) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x8) > 0) {
                    permBuilder.append('x');
                } else {
                    if (directory && (number & 0x30) > 0) {
                        // To read or write, directory access will be required
                        permBuilder.append('x');
                    } else {
                        permBuilder.append('-');
                    }
                }
                if ((number & 0x4) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x2) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x1) > 0) {
                    permBuilder.append('x');
                } else {
                    if (directory && (number & 0x6) > 0) {
                        // To read or write, directory access will be required
                        permBuilder.append('x');
                    } else {
                        permBuilder.append('-');
                    }
                }
                permissions = permBuilder.toString();
            } catch (NumberFormatException ignore) {
            }
        }

        return permissions;
    }
}
