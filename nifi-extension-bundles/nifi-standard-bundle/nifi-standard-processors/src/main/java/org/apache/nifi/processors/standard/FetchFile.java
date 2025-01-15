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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"local", "files", "filesystem", "ingest", "ingress", "get", "source", "input", "fetch"})
@CapabilityDescription("Reads the contents of a file from disk and streams it into the contents of an incoming FlowFile. Once this is done, the file is optionally moved elsewhere or deleted "
    + "to help keep the file system organized.")
@SeeAlso({GetFile.class, PutFile.class, ListFile.class})
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.READ_FILESYSTEM,
                        explanation = "Provides operator the ability to read from any file that NiFi has access to."),
                @Restriction(
                        requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
                        explanation = "Provides operator the ability to delete any file that NiFi has access to.")
        }
)
@MultiProcessorUseCase(
    description = "Ingest all files from a directory into NiFi",
    keywords = {"local", "files", "filesystem", "ingest", "ingress", "get", "source", "input", "fetch"},
    configurations = {
        @ProcessorConfiguration(processorClass = ListFile.class,
            configuration = """
                Configure the "Input Directory" property to point to the directory that you want to ingest files from.
                Set the "Input Directory Location" property to "Local"
                Optionally, set "Minimum File Age" to a small value such as "1 min" to avoid ingesting files that are still being written to.

                Connect the 'success' Relationship to the FetchFile processor.
                """
        ),
        @ProcessorConfiguration(processorClass = FetchFile.class,
            configuration = """
                Set the "File to Fetch" property to `${absolute.path}/${filename}`
                Set the "Completion Strategy" property to `None`
                """
        )
    }
)
@MultiProcessorUseCase(
    description = "Ingest specific files from a directory into NiFi, filtering on filename",
    keywords = {"local", "files", "filesystem", "ingest", "ingress", "get", "source", "input", "fetch", "filter"},
    configurations = {
        @ProcessorConfiguration(processorClass = ListFile.class,
            configuration = """
                Configure the "Input Directory" property to point to the directory that you want to ingest files from.
                Set the "Input Directory Location" property to "Local"
                Set the "File Filter" property to a Regular Expression that matches the filename (without path) of the files that you want to ingest. \
                For example, to ingest all .jpg files, set the value to `.*\\.jpg`
                Optionally, set "Minimum File Age" to a small value such as "1 min" to avoid ingesting files that are still being written to.

                Connect the 'success' Relationship to the FetchFile processor.
                """
        ),
        @ProcessorConfiguration(processorClass = FetchFile.class,
            configuration = """
                Set the "File to Fetch" property to `${absolute.path}/${filename}`
                Set the "Completion Strategy" property to `None`
                """
        )
    }
)
public class FetchFile extends AbstractProcessor {
    static final AllowableValue COMPLETION_NONE = new AllowableValue("None", "None", "Leave the file as-is");
    static final AllowableValue COMPLETION_MOVE = new AllowableValue("Move File", "Move File", "Moves the file to the directory specified by the <Move Destination Directory> property");
    static final AllowableValue COMPLETION_DELETE = new AllowableValue("Delete File", "Delete File", "Deletes the original file from the file system");

    static final AllowableValue CONFLICT_REPLACE = new AllowableValue("Replace File", "Replace File", "The newly ingested file should replace the existing file in the Destination Directory");
    static final AllowableValue CONFLICT_KEEP_INTACT = new AllowableValue("Keep Existing", "Keep Existing", "The existing file should in the Destination Directory should stay intact and the newly "
        + "ingested file should be deleted");
    static final AllowableValue CONFLICT_FAIL = new AllowableValue("Fail", "Fail", "The existing destination file should remain intact and the incoming FlowFile should be routed to failure");
    static final AllowableValue CONFLICT_RENAME = new AllowableValue("Rename", "Rename", "The existing destination file should remain intact. The newly ingested file should be moved to the "
        + "destination directory but be renamed to a random filename");

    static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
        .name("File to Fetch")
        .description("The fully-qualified filename of the file to fetch from the file system")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${absolute.path}/${filename}")
        .required(true)
        .build();
    static final PropertyDescriptor COMPLETION_STRATEGY = new PropertyDescriptor.Builder()
        .name("Completion Strategy")
        .description("Specifies what to do with the original file on the file system once it has been pulled into NiFi")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues(COMPLETION_NONE, COMPLETION_MOVE, COMPLETION_DELETE)
        .defaultValue(COMPLETION_NONE.getValue())
        .required(true)
        .build();
    static final PropertyDescriptor MOVE_DESTINATION_DIR = new PropertyDescriptor.Builder()
        .name("Move Destination Directory")
        .description("The directory to the move the original file to once it has been fetched from the file system. This property is ignored unless the Completion Strategy is set to \"Move File\". "
            + "If the directory does not exist, it will be created.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(false)
        .build();
    static final PropertyDescriptor CONFLICT_STRATEGY = new PropertyDescriptor.Builder()
        .name("Move Conflict Strategy")
        .description("If Completion Strategy is set to Move File and a file already exists in the destination directory with the same name, this property specifies "
            + "how that naming conflict should be resolved")
        .allowableValues(CONFLICT_RENAME, CONFLICT_REPLACE, CONFLICT_KEEP_INTACT, CONFLICT_FAIL)
        .defaultValue(CONFLICT_RENAME.getValue())
        .required(true)
        .build();
    static final PropertyDescriptor FILE_NOT_FOUND_LOG_LEVEL = new PropertyDescriptor.Builder()
        .name("Log level when file not found")
        .description("Log level to use in case the file does not exist when the processor is triggered")
        .allowableValues(LogLevel.values())
        .defaultValue(LogLevel.ERROR.toString())
        .required(true)
        .build();
    static final PropertyDescriptor PERM_DENIED_LOG_LEVEL = new PropertyDescriptor.Builder()
        .name("Log level when permission denied")
        .description("Log level to use in case user " + System.getProperty("user.name") + " does not have sufficient permissions to read the file")
        .allowableValues(LogLevel.values())
        .defaultValue(LogLevel.ERROR.toString())
        .required(true)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FILENAME,
            COMPLETION_STRATEGY,
            MOVE_DESTINATION_DIR,
            CONFLICT_STRATEGY,
            FILE_NOT_FOUND_LOG_LEVEL,
            PERM_DENIED_LOG_LEVEL
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that is successfully fetched from the file system will be transferred to this Relationship.")
        .build();
    static final Relationship REL_NOT_FOUND = new Relationship.Builder()
        .name("not.found")
        .description("Any FlowFile that could not be fetched from the file system because the file could not be found will be transferred to this Relationship.")
        .build();
    static final Relationship REL_PERMISSION_DENIED = new Relationship.Builder()
        .name("permission.denied")
        .description("Any FlowFile that could not be fetched from the file system due to the user running NiFi not having sufficient permissions will be transferred to this Relationship.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description(
            "Any FlowFile that could not be fetched from the file system for any reason other than insufficient permissions or the file not existing will be transferred to this Relationship.")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_NOT_FOUND,
            REL_PERMISSION_DENIED,
            REL_FAILURE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        if (COMPLETION_MOVE.getValue().equalsIgnoreCase(validationContext.getProperty(COMPLETION_STRATEGY).getValue())) {
            if (!validationContext.getProperty(MOVE_DESTINATION_DIR).isSet()) {
                results.add(new ValidationResult.Builder().subject(MOVE_DESTINATION_DIR.getName()).input(null).valid(false).explanation(
                    MOVE_DESTINATION_DIR.getName() + " must be specified if " + COMPLETION_STRATEGY.getName() + " is set to " + COMPLETION_MOVE.getDisplayName()).build());
            }
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        final LogLevel levelFileNotFound = LogLevel.valueOf(context.getProperty(FILE_NOT_FOUND_LOG_LEVEL).getValue());
        final LogLevel levelPermDenied = LogLevel.valueOf(context.getProperty(PERM_DENIED_LOG_LEVEL).getValue());
        final File file = new File(filename);

        // Verify that file system is reachable and file exists
        Path filePath = file.toPath();
        if (!Files.exists(filePath) && !Files.notExists(filePath)) { // see https://docs.oracle.com/javase/tutorial/essential/io/check.html for more details
            getLogger().log(levelFileNotFound, "Could not fetch file {} from file system for {} because the existence of the file cannot be verified; routing to failure",
                    file, flowFile);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        } else if (!Files.exists(filePath)) {
            getLogger().log(levelFileNotFound, "Could not fetch file {} from file system for {} because the file does not exist; routing to not.found", file, flowFile);
            session.getProvenanceReporter().route(flowFile, REL_NOT_FOUND);
            session.transfer(session.penalize(flowFile), REL_NOT_FOUND);
            return;
        }

        // Verify read permission on file
        final String user = System.getProperty("user.name");
        if (!isReadable(file)) {
            getLogger().log(levelPermDenied, "Could not fetch file {} from file system for {} due to user {} not having sufficient permissions to read the file; routing to permission.denied",
                    file, flowFile, user);
            session.getProvenanceReporter().route(flowFile, REL_PERMISSION_DENIED);
            session.transfer(session.penalize(flowFile), REL_PERMISSION_DENIED);
            return;
        }

        // If configured to move the file and fail if unable to do so, check that the existing file does not exist and that we have write permissions
        // for the parent file.
        final String completionStrategy = context.getProperty(COMPLETION_STRATEGY).getValue();
        final String targetDirectoryName = context.getProperty(MOVE_DESTINATION_DIR).evaluateAttributeExpressions(flowFile).getValue();
        if (targetDirectoryName != null) {
            final File targetDir = new File(targetDirectoryName);
            if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
                if (targetDir.exists() && (!isWritable(targetDir) || !isDirectory(targetDir))) {
                    getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, "
                        + "but that is not a directory or user {} does not have permissions to write to that directory",
                            file, flowFile, targetDir, user);
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }

                if (!targetDir.exists()) {
                    try {
                        Files.createDirectories(targetDir.toPath());
                    } catch (Exception e) {
                        getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, "
                                        + "but that directory does not exist and could not be created due to: {}",
                                file, flowFile, targetDir, e.getMessage(), e);
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    }
                }

                final String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();

                if (CONFLICT_FAIL.getValue().equalsIgnoreCase(conflictStrategy)) {
                    final File targetFile = new File(targetDir, file.getName());
                    if (targetFile.exists()) {
                        getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, "
                            + "but a file with name {} already exists in that directory and the Move Conflict Strategy is configured for failure",
                                file, flowFile, targetDir, file.getName());
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    }
                }
            }
        }

        // import content from file system
        try (final FileInputStream fis = new FileInputStream(file)) {
            flowFile = session.importFrom(fis, flowFile);
        } catch (final IOException | FlowFileAccessException ioe) {
            getLogger().error("Could not fetch file {} from file system for {}", file, flowFile, ioe);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        session.getProvenanceReporter().fetch(flowFile, file.toURI().toString(), "Replaced content of FlowFile with contents of " + file.toURI(), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        session.transfer(flowFile, REL_SUCCESS);

        // It is critical that we commit the session before we perform the Completion Strategy. Otherwise, we could have a case where we
        // ingest the file, delete/move the file, and then NiFi is restarted before the session is committed. That would result in data loss.
        // As long as we commit the session right here, before we perform the Completion Strategy, we are safe.
        final FlowFile fetched = flowFile;
        session.commitAsync(() -> {
            performCompletionAction(completionStrategy, file, targetDirectoryName, fetched, context);
        });
    }

    private void performCompletionAction(final String completionStrategy, final File file, final String targetDirectoryName, final FlowFile flowFile, final ProcessContext context) {
        // Attempt to perform the Completion Strategy action
        Exception completionFailureException = null;
        if (COMPLETION_DELETE.getValue().equalsIgnoreCase(completionStrategy)) {
            // convert to path and use Files.delete instead of file.delete so that if we fail, we know why
            try {
                delete(file);
            } catch (final IOException ioe) {
                completionFailureException = ioe;
            }
        } else if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
            final File targetDirectory = new File(targetDirectoryName);
            final File targetFile = new File(targetDirectory, file.getName());
            try {
                if (targetFile.exists()) {
                    final String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();
                    if (CONFLICT_KEEP_INTACT.getValue().equalsIgnoreCase(conflictStrategy)) {
                        // don't move, just delete the original
                        Files.delete(file.toPath());
                    } else if (CONFLICT_RENAME.getValue().equalsIgnoreCase(conflictStrategy)) {
                        // rename to add a random UUID but keep the file extension if it has one.
                        final String simpleFilename = targetFile.getName();
                        final String newName;
                        if (simpleFilename.contains(".")) {
                            newName = StringUtils.substringBeforeLast(simpleFilename, ".") + "-" + UUID.randomUUID().toString() + "." + StringUtils.substringAfterLast(simpleFilename, ".");
                        } else {
                            newName = simpleFilename + "-" + UUID.randomUUID().toString();
                        }

                        move(file, new File(targetDirectory, newName), false);
                    } else if (CONFLICT_REPLACE.getValue().equalsIgnoreCase(conflictStrategy)) {
                        move(file, targetFile, true);
                    }
                } else {
                    move(file, targetFile, false);
                }
            } catch (final IOException ioe) {
                completionFailureException = ioe;
            }
        }

        // Handle completion failures
        if (completionFailureException != null) {
            getLogger().warn("Successfully fetched the content from {} for {} but failed to perform Completion Action due to {}; routing to success",
                    file, flowFile, completionFailureException, completionFailureException);
        }
    }

    //
    // The following set of methods exist purely for testing purposes
    //
    protected void move(final File source, final File target, final boolean overwrite) throws IOException {
        final File targetDirectory = target.getParentFile();

        // convert to path and use Files.move instead of file.renameTo so that if we fail, we know why
        final Path targetPath = target.toPath();
        if (!targetDirectory.exists()) {
            Files.createDirectories(targetDirectory.toPath());
        }

        final CopyOption[] copyOptions = overwrite ? new CopyOption[] {StandardCopyOption.REPLACE_EXISTING} : new CopyOption[] {};
        Files.move(source.toPath(), targetPath, copyOptions);
    }

    protected void delete(final File file) throws IOException {
        Files.delete(file.toPath());
    }

    protected boolean isReadable(final File file) {
        return file.canRead();
    }

    protected boolean isWritable(final File file) {
        return file.canWrite();
    }

    protected boolean isDirectory(final File file) {
        return file.isDirectory();
    }
}
