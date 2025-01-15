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

import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"file", "remove", "delete", "local", "files", "filesystem"})
@CapabilityDescription("Deletes a file from the filesystem.")
@UseCase(
        description = "Delete source file only after its processing completed",
        configuration = """
                Retrieve a file from the filesystem, e.g. using 'ListFile' and 'FetchFile'.
                Process the file using any combination of processors.
                Store the resulting file to a destination, e.g. using 'PutSFTP'.
                Using 'DeleteFile', delete the file from the filesystem only after the result has been stored.
                """
)
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
public class DeleteFile extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles, for which an existing file has been deleted, are routed to this relationship")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("All FlowFiles, for which the file to delete did not exist, are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles, for which an existing file could not be deleted, are routed to this relationship")
            .build();

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_NOT_FOUND,
            REL_FAILURE
    );

    public static final PropertyDescriptor DIRECTORY_PATH = new PropertyDescriptor.Builder()
            .name("Directory Path")
            .description("The path to the directory the file to delete is located in.")
            .required(true)
            .defaultValue("${" + CoreAttributes.ABSOLUTE_PATH.key() + "}")
            .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
            .name("Filename")
            .description("The name of the file to delete.")
            .required(true)
            .defaultValue("${" + CoreAttributes.FILENAME.key() + "}")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            DIRECTORY_PATH,
            FILENAME
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
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final long startNanos = System.nanoTime();

        final String directoryPathProperty = context.getProperty(DIRECTORY_PATH).evaluateAttributeExpressions(flowFile).getValue();
        final String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();

        try {
            final Path directoryPath = Paths.get(directoryPathProperty).toRealPath();
            final Path filePath = directoryPath.resolve(filename).toRealPath();

            if (!directoryPath.equals(filePath.getParent())) {
                final String errorMessage = "Attempting to delete file at path '%s' which is not a direct child of the directory '%s'"
                        .formatted(filePath, directoryPath);

                handleFailure(session, flowFile, errorMessage, null);
                return;
            }

            Files.delete(filePath);

            session.transfer(flowFile, REL_SUCCESS);
            final String transitUri = "file://%s".formatted(filePath);
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            getLogger().debug("Successfully deleted file at path {} in {} millis; routing to success", flowFile, transferMillis);
            session.getProvenanceReporter().invokeRemoteProcess(flowFile, transitUri, "Object deleted");
        } catch (final NoSuchFileException noSuchFileException) {
            session.transfer(flowFile, REL_NOT_FOUND);
        } catch (final IOException ioException) {
            final String errorMessage = "Failed to delete file '%s' in directory '%s'"
                    .formatted(filename, directoryPathProperty);

            handleFailure(session, flowFile, errorMessage, ioException);
        }
    }

    private void handleFailure(ProcessSession session, FlowFile flowFile, String errorMessage, Throwable throwable) {
        getLogger().error(errorMessage, throwable);

        session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }
}
