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

import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.v2.DbxClientV2;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialDetails;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"dropbox", "storage", "fetch"})
@CapabilityDescription("Fetches files from Dropbox. Designed to be used in tandem with ListDropbox.")
@WritesAttribute(attribute = "error.message", description = "When a FlowFile is routed to 'failure', this attribute is added indicating why the file could "
        + "not be fetched from Dropbox.")
@SeeAlso(ListDropbox.class)
@WritesAttributes(
        @WritesAttribute(attribute = FetchDropbox.ERROR_MESSAGE_ATTRIBUTE, description = "The error message returned by Dropbox when the fetch of a file fails."))
public class FetchDropbox extends AbstractProcessor {

    public static final String ERROR_MESSAGE_ATTRIBUTE = "error.message";

    public static final PropertyDescriptor FILE = new PropertyDescriptor
            .Builder().name("file")
            .displayName("File")
            .description("The Dropbox identifier or path of the Dropbox file to fetch." +
                    " The 'File' should match the following regular expression pattern: /.*|id:.* ." +
                    " When ListDropbox is used for input, either '${dropbox.id}' (identify file by Dropbox id)" +
                    " or '${path}/${filename}' (identify file by path) can be used as 'File' value.")
            .required(true)
            .defaultValue("${dropbox.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createRegexMatchingValidator(
                    Pattern.compile("/.*|id:.*")))
            .build();
    public static final PropertyDescriptor CREDENTIAL_SERVICE = new PropertyDescriptor.Builder()
            .name("dropbox-credential-service")
            .displayName("Dropbox Credential Service")
            .description("Controller Service used to obtain Dropbox credentials." +
                    " See controller service's usage documentation for more details.")
            .identifiesControllerService(DropboxCredentialService.class)
            .required(true)
            .build();
    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("A FlowFile will be routed here for each successfully fetched File.")
                    .build();
    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description(
                            "A FlowFile will be routed here for each File for which fetch was attempted but failed.")
                    .build();
    public static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            FILE,
            CREDENTIAL_SERVICE
    ));
    private DbxClientV2 dropboxApiClient;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        dropboxApiClient = getDropboxApiClient(context);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String fileIdentifier = context.getProperty(FILE).evaluateAttributeExpressions(flowFile).getValue();
        fileIdentifier = correctFilePath(fileIdentifier);

        FlowFile outFlowFile = flowFile;
        try {
            fetchFile(fileIdentifier, session, outFlowFile);
            session.transfer(outFlowFile, REL_SUCCESS);
        } catch (Exception e) {
            handleError(session, flowFile, fileIdentifier, e);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    protected DbxClientV2 getDropboxApiClient(ProcessContext context) {
        final DropboxCredentialService credentialService = context.getProperty(CREDENTIAL_SERVICE)
                .asControllerService(DropboxCredentialService.class);
        DbxRequestConfig config = new DbxRequestConfig(format("%s-%s", getClass().getSimpleName(), getIdentifier()));
        DropboxCredentialDetails credential = credentialService.getDropboxCredential();
        return new DbxClientV2(config, new DbxCredential(credential.getAccessToken(), -1L,
                credential.getRefreshToken(), credential.getAppKey(), credential.getAppSecret()));
    }

    private void fetchFile(String fileId, ProcessSession session, FlowFile outFlowFile) throws DbxException {
        InputStream dropboxInputStream = dropboxApiClient.files()
                .download(fileId)
                .getInputStream();
        session.importFrom(dropboxInputStream, outFlowFile);
    }

    private void handleError(ProcessSession session, FlowFile flowFile, String fileId, Exception e) {
        getLogger().error("Error while fetching and processing file with id '{}'", fileId, e);
        FlowFile outFlowFile = session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());
        session.transfer(outFlowFile, REL_FAILURE);
    }

    private String correctFilePath(String folderName) {
        return folderName.startsWith("//") ? folderName.replaceFirst("//", "/") : folderName;
    }
}
