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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@CapabilityDescription("Deletes a file from GridFS using a file name or a query.")
@Tags({"gridfs", "delete", "mongodb"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class DeleteGridFS extends AbstractGridFSProcessor {
    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("delete-gridfs-query")
        .displayName("Query")
        .description("A valid MongoDB query to use to find and delete one or more files from GridFS.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(JsonValidator.INSTANCE)
        .required(false)
        .build();

    static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
        .name("gridfs-file-name")
        .displayName("File Name")
        .description("The name of the file in the bucket that is the target of this processor. GridFS file names do not " +
                "include path information because GridFS does not sort files into folders within a bucket.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                FILE_NAME,
                QUERY,
                QUERY_ATTRIBUTE
            )
    ).toList();

    @Override
    public Set<Relationship> getRelationships() {
        return getCommonRelationships();
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        ArrayList<ValidationResult> problems = new ArrayList<>();

        boolean fileName = validationContext.getProperty(FILE_NAME).isSet();
        boolean query    = validationContext.getProperty(QUERY).isSet();

        if (fileName && query) {
            problems.add(new ValidationResult.Builder()
                .valid(false)
                .explanation("File name and Query cannot be set at the same time.")
                .build()
            );
        } else if (!fileName && !query) {
            problems.add(new ValidationResult.Builder()
                .valid(false)
                .explanation("File name or Query must be set, but not both at the same time.")
                .build()
            );
        }

        return problems;
    }

    private String getQuery(ProcessContext context, FlowFile input) {
        String queryString;
        if (context.getProperty(FILE_NAME).isSet()) {
            String fileName = context.getProperty(FILE_NAME).evaluateAttributeExpressions(input).getValue();
            queryString = String.format("{ \"filename\": \"%s\"}", fileName);
        } else {
            queryString = context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue();
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

        final String deleteQuery = getQuery(context, input);
        final String queryAttribute = context.getProperty(QUERY_ATTRIBUTE).isSet()
                ? context.getProperty(QUERY_ATTRIBUTE).evaluateAttributeExpressions(input).getValue()
                : null;
        GridFSBucket bucket = getBucket(input, context);

        try {
            Document query = Document.parse(deleteQuery);
            MongoCursor cursor = bucket.find(query).iterator();
            if (cursor.hasNext()) {
                GridFSFile file = (GridFSFile) cursor.next();
                bucket.delete(file.getObjectId());

                if (!StringUtils.isEmpty(queryAttribute)) {
                    input = session.putAttribute(input, queryAttribute, deleteQuery);
                }

                session.transfer(input, REL_SUCCESS);
            } else {
                getLogger().error("Query {} did not delete anything in {}", deleteQuery, bucket.getBucketName());
                session.transfer(input, REL_FAILURE);
            }

            cursor.close();
        } catch (Exception ex) {
            getLogger().error("Error deleting using query: {}", deleteQuery, ex);
            session.transfer(input, REL_FAILURE);
        }
    }
}
