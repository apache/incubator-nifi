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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.smb.util.CompletionStrategy;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"samba", "smb", "cifs", "files", "fetch"})
@CapabilityDescription("Fetches files from a SMB Share. Designed to be used in tandem with ListSmb.")
@SeeAlso({ListSmb.class, PutSmbFile.class, GetSmbFile.class})
@WritesAttributes({
        @WritesAttribute(attribute = FetchSmb.ERROR_CODE_ATTRIBUTE, description = "The error code returned by SMB when the fetch of a file fails."),
        @WritesAttribute(attribute = FetchSmb.ERROR_MESSAGE_ATTRIBUTE, description = "The error message returned by SMB when the fetch of a file fails.")
})
public class FetchSmb extends AbstractProcessor {

    public static final String ERROR_CODE_ATTRIBUTE = "error.code";
    public static final String ERROR_MESSAGE_ATTRIBUTE = "error.message";

    public static final PropertyDescriptor REMOTE_FILE = new PropertyDescriptor.Builder()
            .name("remote-file")
            .displayName("Remote File")
            .description("The full path of the file to be retrieved from the remote server. Expression language is supported.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .defaultValue("${path}/${filename}")
            .addValidator(ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();

    public static final PropertyDescriptor COMPLETION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Completion Strategy")
            .description("Specifies what to do with the original file on the server once it has been processed. If the Completion Strategy fails, a warning will be "
                    + "logged but the data will still be transferred.")
            .allowableValues(CompletionStrategy.class)
            .defaultValue(CompletionStrategy.NONE)
            .required(true)
            .build();

    public static final PropertyDescriptor DESTINATION_DIRECTORY = new PropertyDescriptor.Builder()
            .name("Destination Directory")
            .description("The directory on the remote server to move the original file to once it has been processed.")
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .dependsOn(COMPLETION_STRATEGY, CompletionStrategy.MOVE)
            .build();

    public static final PropertyDescriptor CREATE_DESTINATION_DIRECTORY = new PropertyDescriptor.Builder()
            .name("Create Destination Directory")
            .description("Specifies whether or not the remote directory should be created if it does not exist.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .dependsOn(COMPLETION_STRATEGY, CompletionStrategy.MOVE)
            .build();

    public static final PropertyDescriptor SMB_CLIENT_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("smb-client-provider-service")
            .displayName("SMB Client Provider Service")
            .description("Specifies the SMB client provider to use for creating SMB connections.")
            .required(true)
            .identifiesControllerService(SmbClientProviderService.class)
            .build();

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("A FlowFile will be routed here for each successfully fetched file.")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder()
                    .name("failure")
                    .description("A FlowFile will be routed here when failed to fetch its content.")
                    .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SMB_CLIENT_PROVIDER_SERVICE,
            REMOTE_FILE,
            COMPLETION_STRATEGY,
            DESTINATION_DIRECTORY,
            CREATE_DESTINATION_DIRECTORY
    );

    public static final String UNCATEGORIZED_ERROR = "-2";

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Map<String, String> attributes = flowFile.getAttributes();
        final String filePath = context.getProperty(REMOTE_FILE).evaluateAttributeExpressions(attributes).getValue();

        final SmbClientProviderService clientProviderService = context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);

        try (SmbClientService client = clientProviderService.getClient()) {
            flowFile = session.write(flowFile, outputStream -> client.readFile(filePath, outputStream));

            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Could not fetch file {}.", filePath, e);
            flowFile = session.putAttribute(flowFile, ERROR_CODE_ATTRIBUTE, getErrorCode(e));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.commitAsync(() -> performCompletionStrategy(context, attributes));
    }

    private String getErrorCode(final Exception exception) {
        return Optional.ofNullable(exception instanceof SmbException ? (SmbException) exception : null)
                .map(SmbException::getErrorCode)
                .map(String::valueOf)
                .orElse(UNCATEGORIZED_ERROR);
    }

    private void performCompletionStrategy(final ProcessContext context, final Map<String, String> attributes) {
        final CompletionStrategy completionStrategy = context.getProperty(COMPLETION_STRATEGY).asAllowableValue(CompletionStrategy.class);

        if (completionStrategy == CompletionStrategy.NONE) {
            return;
        }

        final String filePath = context.getProperty(REMOTE_FILE).evaluateAttributeExpressions(attributes).getValue();

        final SmbClientProviderService clientProviderService = context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);

        try (SmbClientService client = clientProviderService.getClient()) {
            if (completionStrategy == CompletionStrategy.MOVE) {
                final String destinationDirectory = context.getProperty(DESTINATION_DIRECTORY).evaluateAttributeExpressions(attributes).getValue();
                final boolean createDestinationDirectory = context.getProperty(CREATE_DESTINATION_DIRECTORY).asBoolean();

                if (createDestinationDirectory) {
                    client.ensureDirectory(destinationDirectory);
                }

                client.moveFile(filePath, destinationDirectory);
            } else if (completionStrategy == CompletionStrategy.DELETE) {
                client.deleteFile(filePath);
            }
        } catch (Exception e) {
            getLogger().warn("Could not perform completion strategy {} for file {}", completionStrategy, filePath, e);
        }
    }

}
