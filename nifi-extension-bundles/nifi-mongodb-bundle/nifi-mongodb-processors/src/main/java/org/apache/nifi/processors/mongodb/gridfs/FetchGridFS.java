/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.mongodb.gridfs;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.model.GridFSFile;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processors.mongodb.QueryHelper;
import org.apache.nifi.util.StringUtils;
import org.bson.Document;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes(
    @WritesAttribute(attribute = "gridfs.file.metadata", description = "The custom metadata stored with a file is attached to this property if it exists.")
)
@Tags({"fetch", "gridfs", "mongo"})
@CapabilityDescription("Retrieves one or more files from a GridFS bucket by file name or by a user-defined query.")
public class FetchGridFS extends AbstractGridFSProcessor implements QueryHelper {

    static final String METADATA_ATTRIBUTE = "gridfs.file.metadata";

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("gridfs-query")
        .displayName("Query")
        .description("A valid MongoDB query to use to fetch one or more files from GridFS.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(JsonValidator.INSTANCE)
        .required(false)
        .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The original input flowfile goes to this relationship if the query does not cause an error")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                FILE_NAME,
                QUERY,
                QUERY_ATTRIBUTE,
                OPERATION_MODE
            )
    ).toList();

    private static final Set<Relationship> RELATIONSHIPS = Stream.concat(
            getCommonRelationships().stream(),
            Stream.of(
                REL_ORIGINAL
            )
    ).collect(Collectors.toUnmodifiableSet());

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private String getQuery(ProcessSession session, ProcessContext context, FlowFile input) throws IOException {
        String queryString;
        if (context.getProperty(FILE_NAME).isSet()) {
            String fileName = context.getProperty(FILE_NAME).evaluateAttributeExpressions(input).getValue();
            queryString = String.format("{ \"filename\": \"%s\"}", fileName);
        } else if (context.getProperty(QUERY).isSet()) {
            queryString = context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue();
        } else {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            session.exportTo(input, out);
            out.close();
            queryString = new String(out.toByteArray());
        }

        return queryString;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.clientService = context.getProperty(CLIENT_SERVICE).asControllerService(MongoDBClientService.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        final String operatingMode = context.getProperty(OPERATION_MODE).getValue();
        final Map<String, String> originalAttributes = input.getAttributes();

        String queryStr;
        try {
            queryStr = getQuery(session, context, input);
            if (StringUtils.isEmpty(queryStr)) {
                getLogger().error("No query could be found or built from the supplied input.");
                session.transfer(input, REL_FAILURE);
                return;
            }
        } catch (IOException ex) {
            getLogger().error("No query could be found from supplied input", ex);
            session.transfer(input, REL_FAILURE);
            return;
        }

        Document query = Document.parse(queryStr);

        try {
            final GridFSBucket bucket = getBucket(input, context);
            final String queryPtr = queryStr;
            final FlowFile parent = operatingMode.equals(MODE_ONE_COMMIT.getValue()) ? input : null;

            MongoCursor it = bucket.find(query).iterator();
            if (operatingMode.equals(MODE_MANY_COMMITS.getValue())) {
                session.transfer(input, REL_ORIGINAL);
                input = null;
            }

            while (it.hasNext()) {
                GridFSFile gridFSFile = (GridFSFile) it.next();
                handleFile(bucket, session, context, parent, gridFSFile, queryPtr);

                if (operatingMode.equals(MODE_MANY_COMMITS.getValue())) {
                    session.commitAsync();
                }
            }

            if (input != null) {
                session.transfer(input, REL_ORIGINAL);
            }
        } catch (Exception ex) {
            getLogger().error("An error occurred wile trying to run the query.", ex);
            if (input != null && operatingMode.equals(MODE_ONE_COMMIT.getValue())) {
                session.transfer(input, REL_FAILURE);
            } else if (input != null && operatingMode.equals(MODE_MANY_COMMITS.getValue())) {
                final String queryPtr = queryStr;
                FlowFile cloned = session.create();
                cloned = session.putAllAttributes(cloned, originalAttributes);
                cloned = session.write(cloned, out -> out.write(queryPtr.getBytes()));
                session.transfer(cloned, REL_FAILURE);
            }
        }
    }

    private void handleFile(GridFSBucket bucket, ProcessSession session, ProcessContext context, FlowFile parent, GridFSFile input, String query) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(METADATA_ATTRIBUTE, input.getMetadata() != null ? input.getMetadata().toJson() : "{}");
        if (context.getProperty(QUERY_ATTRIBUTE).isSet()) {
            String key = context.getProperty(QUERY_ATTRIBUTE).evaluateAttributeExpressions(parent).getValue();
            attrs.put(key, query);
        }
        attrs.put(CoreAttributes.FILENAME.key(), input.getFilename());
        FlowFile output = parent != null ? session.create(parent) : session.create();
        output = session.write(output, out -> bucket.downloadToStream(input.getObjectId(), out));
        output = session.putAllAttributes(output, attrs);
        session.transfer(output, REL_SUCCESS);
        session.getProvenanceReporter().receive(output, getTransitUri(input.getObjectId(), output, context));
    }
}
