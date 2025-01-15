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

import static org.apache.nifi.processors.dropbox.DropboxAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.dropbox.DropboxAttributes.ERROR_MESSAGE_DESC;
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

import com.dropbox.core.DbxDownloader;
import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"dropbox", "storage", "fetch"})
@CapabilityDescription("Fetches files from Dropbox. Designed to be used in tandem with ListDropbox.")
@SeeAlso({PutDropbox.class, ListDropbox.class})
@WritesAttributes({
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC),
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = PATH, description = PATH_DESC),
        @WritesAttribute(attribute = FILENAME, description = FILENAME_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = REVISION, description = REVISION_DESC)}
)
public class FetchDropbox extends AbstractProcessor implements DropboxTrait {

    public static final PropertyDescriptor FILE = new PropertyDescriptor
            .Builder().name("file")
            .displayName("File")
            .description("The Dropbox identifier or path of the Dropbox file to fetch." +
                    " The 'File' should match the following regular expression pattern: /.*|id:.* ." +
                    " When ListDropbox is used for input, either '${dropbox.id}' (identifying files by Dropbox id)" +
                    " or '${path}/${filename}' (identifying files by path) can be used as 'File' value.")
            .required(true)
            .defaultValue("${dropbox.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createRegexMatchingValidator(
                    Pattern.compile("/.*|id:.*")))
            .build();

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("A FlowFile will be routed here for each successfully fetched File.")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description("A FlowFile will be routed here for each File for which fetch was attempted but failed.")
                    .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CREDENTIAL_SERVICE,
            FILE,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxySpec.HTTP_AUTH)
    );

    private volatile DbxClientV2 dropboxApiClient;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        dropboxApiClient = getDropboxApiClient(context, getIdentifier());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String fileIdentifier = context.getProperty(FILE).evaluateAttributeExpressions(flowFile).getValue();
        fileIdentifier = correctFilePath(fileIdentifier);

        FlowFile outFlowFile = flowFile;
        final long startNanos = System.nanoTime();
        try {
            FileMetadata fileMetadata = fetchFile(fileIdentifier, session, outFlowFile);

            final Map<String, String> attributes = createAttributeMap(fileMetadata);
            outFlowFile = session.putAllAttributes(outFlowFile, attributes);
            String url = DROPBOX_HOME_URL + fileMetadata.getPathDisplay();
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().fetch(flowFile, url, transferMillis);

            session.transfer(outFlowFile, REL_SUCCESS);
        } catch (Exception e) {
            handleError(session, flowFile, fileIdentifier, e);
        }
    }

    private FileMetadata fetchFile(String fileId, ProcessSession session, FlowFile outFlowFile) throws DbxException {
        try (DbxDownloader<FileMetadata> downloader = dropboxApiClient.files().download(fileId)) {
            final InputStream dropboxInputStream = downloader.getInputStream();
            session.importFrom(dropboxInputStream, outFlowFile);
            return downloader.getResult();
        }
    }

    private void handleError(ProcessSession session, FlowFile flowFile, String fileId, Exception e) {
        getLogger().error("Error while fetching and processing file with id '{}'", fileId, e);
        final FlowFile outFlowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
        session.penalize(outFlowFile);
        session.transfer(outFlowFile, REL_FAILURE);
    }

    private String correctFilePath(String folderName) {
        return folderName.startsWith("//") ? folderName.replaceFirst("//", "/") : folderName;
    }
}
