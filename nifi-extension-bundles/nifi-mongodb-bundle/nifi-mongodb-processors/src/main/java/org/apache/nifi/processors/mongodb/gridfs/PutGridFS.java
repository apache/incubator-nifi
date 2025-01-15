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

import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"mongo", "gridfs", "put", "file", "store"})
@CapabilityDescription("Writes a file to a GridFS bucket.")
public class PutGridFS extends AbstractGridFSProcessor {

    static final PropertyDescriptor PROPERTIES_PREFIX = new PropertyDescriptor.Builder()
        .name("putgridfs-properties-prefix")
        .displayName("File Properties Prefix")
        .description("Attributes that have this prefix will be added to the file stored in GridFS as metadata.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(Validator.VALID)
        .build();

    static final AllowableValue NO_UNIQUE   = new AllowableValue("none", "None", "No uniqueness will be enforced.");
    static final AllowableValue UNIQUE_NAME = new AllowableValue("name", "Name", "Only the filename must " +
            "be unique.");
    static final AllowableValue UNIQUE_HASH = new AllowableValue("hash", "Hash", "Only the file hash must be " +
            "unique.");
    static final AllowableValue UNIQUE_BOTH = new AllowableValue("both", "Both", "Both the filename and hash " +
            "must be unique.");

    static final PropertyDescriptor ENFORCE_UNIQUENESS = new PropertyDescriptor.Builder()
        .name("putgridfs-enforce-uniqueness")
        .displayName("Enforce Uniqueness")
        .description("When enabled, this option will ensure that uniqueness is enforced on the bucket. It will do so by creating a MongoDB index " +
                "that matches your selection. It should ideally be configured once when the bucket is created for the first time because " +
                "it could take a long time to build on an existing bucket wit a lot of data.")
        .allowableValues(NO_UNIQUE, UNIQUE_BOTH, UNIQUE_NAME, UNIQUE_HASH)
        .defaultValue(NO_UNIQUE.getValue())
        .required(true)
        .build();
    static final PropertyDescriptor HASH_ATTRIBUTE = new PropertyDescriptor.Builder()
        .name("putgridfs-hash-attribute")
        .displayName("Hash Attribute")
        .description("If uniquness enforcement is enabled and the file hash is part of the constraint, this must be set to an attribute that " +
                "exists on all incoming flowfiles.")
        .defaultValue("hash.value")
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    static final PropertyDescriptor CHUNK_SIZE = new PropertyDescriptor.Builder()
        .name("putgridfs-chunk-size")
        .displayName("Chunk Size")
        .description("Controls the maximum size of each chunk of a file uploaded into GridFS.")
        .defaultValue("256 KB")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();

    static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
        .name("gridfs-file-name")
        .displayName("File Name")
        .description("The name of the file in the bucket that is the target of this processor. GridFS file names do not " +
                "include path information because GridFS does not sort files into folders within a bucket.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final Relationship REL_DUPLICATE = new Relationship.Builder()
        .name("duplicate")
        .description("Flowfiles that fail the duplicate check are sent to this relationship.")
        .build();

    static final String ID_ATTRIBUTE = "gridfs.id";

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                FILE_NAME,
                PROPERTIES_PREFIX,
                ENFORCE_UNIQUENESS,
                HASH_ATTRIBUTE,
                CHUNK_SIZE
            )
    ).toList();

    private static final Set<Relationship> RELATIONSHIPS = Stream.concat(
            getCommonRelationships().stream(),
            Stream.of(
                REL_DUPLICATE
            )
    ).collect(Collectors.toUnmodifiableSet());

    private String uniqueness;
    private String hashAttribute;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        this.uniqueness = context.getProperty(ENFORCE_UNIQUENESS).getValue();
        this.hashAttribute = context.getProperty(HASH_ATTRIBUTE).evaluateAttributeExpressions().getValue();
        this.clientService = context.getProperty(CLIENT_SERVICE).asControllerService(MongoDBClientService.class);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        GridFSBucket bucket = getBucket(input, context);

        if (!canUploadFile(context, input, bucket.getBucketName())) {
            getLogger().error("Cannot upload the file because of the uniqueness policy configured.");
            session.transfer(input, REL_DUPLICATE);
            return;
        }

        final int chunkSize = context.getProperty(CHUNK_SIZE).evaluateAttributeExpressions(input).asDataSize(DataUnit.B).intValue();

        try (InputStream fileInput = session.read(input)) {
            String fileName = context.getProperty(FILE_NAME).evaluateAttributeExpressions(input).getValue();
            GridFSUploadOptions options = new GridFSUploadOptions()
                .chunkSizeBytes(chunkSize)
                .metadata(getMetadata(input, context));
            ObjectId id = bucket.uploadFromStream(fileName, fileInput, options);
            fileInput.close();

            if (id != null) {
                input = session.putAttribute(input, ID_ATTRIBUTE, id.toString());
                session.transfer(input, REL_SUCCESS);
                session.getProvenanceReporter().send(input, getTransitUri(id, input, context));
            } else {
                getLogger().error("ID was null, assuming failure.");
                session.transfer(input, REL_FAILURE);
            }
        } catch (Exception ex) {
            getLogger().error("Failed to upload file", ex);
            session.transfer(input, REL_FAILURE);
        }
    }

    private boolean canUploadFile(ProcessContext context, FlowFile input, String bucketName) {
        boolean retVal;

        if (uniqueness.equals(NO_UNIQUE.getValue())) {
            retVal = true;
        } else {
            final String fileName = input.getAttribute(CoreAttributes.FILENAME.key());
            final String fileColl = String.format("%s.files", bucketName);
            final String hash     = input.getAttribute(hashAttribute);

            if ((uniqueness.equals(UNIQUE_BOTH.getValue()) || uniqueness.equals(UNIQUE_HASH.getValue())) && StringUtils.isEmpty(hash)) {
                throw new RuntimeException(String.format("Uniqueness mode %s was set and the hash attribute %s was not found.", uniqueness, hashAttribute));
            }

            Document query;
            if (uniqueness.equals(UNIQUE_BOTH.getValue())) {
                query = new Document().append("filename", fileName).append("md5", hash);
            } else if (uniqueness.equals(UNIQUE_HASH.getValue())) {
                query = new Document().append("md5", hash);
            } else {
                query = new Document().append("filename", fileName);
            }

            retVal = getDatabase(input, context).getCollection(fileColl).countDocuments(query) == 0;
        }

        return retVal;
    }

    private Document getMetadata(FlowFile input, ProcessContext context) {
        final String prefix = context.getProperty(PROPERTIES_PREFIX).evaluateAttributeExpressions(input).getValue();
        Document doc;

        if (StringUtils.isEmpty(prefix)) {
            doc = Document.parse("{}");
        } else {
            doc = new Document();
            Map<String, String> attributes = input.getAttributes();
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                if (entry.getKey().startsWith(prefix)) {
                    String cleanPrefix = prefix.endsWith(".") ? prefix : String.format("%s.", prefix);
                    String cleanKey = entry.getKey().replace(cleanPrefix, "");
                    doc.append(cleanKey, entry.getValue());
                }
            }
        }

        return doc;
    }
}
