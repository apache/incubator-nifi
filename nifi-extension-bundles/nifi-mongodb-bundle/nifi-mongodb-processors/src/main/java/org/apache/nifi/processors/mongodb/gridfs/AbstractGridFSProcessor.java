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

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Set;

public abstract class AbstractGridFSProcessor extends AbstractProcessor {
    static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("gridfs-client-service")
        .displayName("Client Service")
        .description("The MongoDB client service to use for database connections.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .identifiesControllerService(MongoDBClientService.class)
        .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
        .name("gridfs-database-name")
        .displayName("Mongo Database Name")
        .description("The name of the database to use")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor BUCKET_NAME = new PropertyDescriptor.Builder()
        .name("gridfs-bucket-name")
        .displayName("Bucket Name")
        .description("The GridFS bucket where the files will be stored. If left blank, it will use the default value 'fs' " +
                "that the MongoDB client driver uses.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
        .name("gridfs-file-name")
        .displayName("File Name")
        .description("The name of the file in the bucket that is the target of this processor.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor QUERY_ATTRIBUTE = new PropertyDescriptor.Builder()
        .name("mongo-query-attribute")
        .displayName("Query Output Attribute")
        .description("If set, the query will be written to a specified attribute on the output flowfiles.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
        .required(false)
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("When there is a failure processing the flowfile, it goes to this relationship.")
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("When the operation succeeds, the flowfile is sent to this relationship.")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        CLIENT_SERVICE,
        DATABASE_NAME,
        BUCKET_NAME
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS,
        REL_FAILURE
    );

    protected volatile MongoDBClientService clientService;

    protected static List<PropertyDescriptor> getCommonPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    protected static Set<Relationship> getCommonRelationships() {
        return RELATIONSHIPS;
    }

    protected MongoDatabase getDatabase(FlowFile input, ProcessContext context) {
        return clientService.getDatabase(context.getProperty(DATABASE_NAME)
                .evaluateAttributeExpressions(input)
                .getValue());
    }

    protected GridFSBucket getBucket(FlowFile input, ProcessContext context) {
        final String name = getBucketName(input, context);
        if (StringUtils.isEmpty(name)) {
            return GridFSBuckets.create(getDatabase(input, context));
        } else {
            return GridFSBuckets.create(getDatabase(input, context), name);
        }
    }

    protected String getBucketName(FlowFile input, ProcessContext context) {
        return context.getProperty(BUCKET_NAME).isSet()
            ? context.getProperty(BUCKET_NAME).evaluateAttributeExpressions(input).getValue()
            : null;
    }

    protected String getTransitUri(ObjectId id, FlowFile input, ProcessContext context) {
        String bucket = getBucketName(input, context);
        String uri = clientService.getURI();
        return new StringBuilder()
            .append(uri)
            .append(uri.endsWith("/") ? "" : "/")
            .append(bucket)
            .append("/")
            .append(id.toString())
            .toString();
    }
}
