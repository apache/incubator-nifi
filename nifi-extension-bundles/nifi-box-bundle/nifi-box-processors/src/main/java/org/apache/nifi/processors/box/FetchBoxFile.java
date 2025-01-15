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

import static java.lang.String.valueOf;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_CODE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE;
import static org.apache.nifi.processors.box.BoxFileAttributes.ERROR_MESSAGE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.FILENAME_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID;
import static org.apache.nifi.processors.box.BoxFileAttributes.ID_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.PATH_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE;
import static org.apache.nifi.processors.box.BoxFileAttributes.SIZE_DESC;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP;
import static org.apache.nifi.processors.box.BoxFileAttributes.TIMESTAMP_DESC;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxFile;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"box", "storage", "fetch"})
@CapabilityDescription("Fetches files from a Box Folder. Designed to be used in tandem with ListBoxFile.")
@SeeAlso({ListBoxFile.class, PutBoxFile.class})
@ReadsAttribute(attribute = ID, description = ID_DESC)
@WritesAttributes({
        @WritesAttribute(attribute = ID, description = ID_DESC),
        @WritesAttribute(attribute = "filename", description = FILENAME_DESC),
        @WritesAttribute(attribute = "path", description = PATH_DESC),
        @WritesAttribute(attribute = SIZE, description = SIZE_DESC),
        @WritesAttribute(attribute = TIMESTAMP, description = TIMESTAMP_DESC),
        @WritesAttribute(attribute = ERROR_CODE, description = ERROR_CODE_DESC),
        @WritesAttribute(attribute = ERROR_MESSAGE, description = ERROR_MESSAGE_DESC)
})
public class FetchBoxFile extends AbstractProcessor {

    public static final PropertyDescriptor FILE_ID = new PropertyDescriptor
            .Builder().name("box-file-id")
            .displayName("File ID")
            .description("The ID of the File to fetch")
            .required(true)
            .defaultValue("${box.id}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder()
                    .name("success")
                    .description("A FlowFile will be routed here for each successfully fetched File.")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder()
                    .name("failure")
                    .description("A FlowFile will be routed here for each File for which fetch was attempted but failed.")
                    .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BoxClientService.BOX_CLIENT_SERVICE,
            FILE_ID
    );

    private volatile BoxAPIConnection boxAPIConnection;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        BoxClientService boxClientService = context.getProperty(BoxClientService.BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);

        boxAPIConnection = boxClientService.getBoxApiConnection();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String fileId = context.getProperty(FILE_ID).evaluateAttributeExpressions(flowFile).getValue();
        try {
            final long startNanos = System.nanoTime();
            flowFile = fetchFile(fileId, session, flowFile);
            final String boxUrlOfFile = BoxFileUtils.BOX_URL + fileId;
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().fetch(flowFile, boxUrlOfFile, transferMillis);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (BoxAPIResponseException e) {
            handleErrorResponse(session, fileId, flowFile, e);
        } catch (Exception e) {
            handleUnexpectedError(session, flowFile, fileId, e);
        }
    }

    BoxFile getBoxFile(String fileId) {
        return new BoxFile(boxAPIConnection, fileId);
    }

    private FlowFile fetchFile(String fileId, ProcessSession session, FlowFile flowFile) {
        final BoxFile boxFile = getBoxFile(fileId);
        flowFile = session.write(flowFile, outputStream -> boxFile.download(outputStream));
        flowFile = session.putAllAttributes(flowFile, BoxFileUtils.createAttributeMap(boxFile.getInfo()));
        return flowFile;
    }

    private void handleErrorResponse(ProcessSession session, String fileId, FlowFile flowFile, BoxAPIResponseException e) {
        getLogger().error("Couldn't fetch file with id [{}]", fileId, e);

        flowFile = session.putAttribute(flowFile, ERROR_CODE, valueOf(e.getResponseCode()));
        flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    private void handleUnexpectedError(ProcessSession session, FlowFile flowFile, String fileId, Exception e) {
        getLogger().error("Failed fetching and processing file with id [{}]", fileId, e);

        flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, e.getMessage());
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }
}
