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
package org.apache.nifi.processors.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StringUtils;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@Tags({ "mongodb", "insert", "update", "write", "put" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes the contents of a FlowFile to MongoDB")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@WritesAttributes({
    @WritesAttribute(attribute = PutMongo.ATTRIBUTE_UPDATE_MATCH_COUNT, description = "The match count from result if update/upsert is performed, otherwise not set."),
    @WritesAttribute(attribute = PutMongo.ATTRIBUTE_UPDATE_MODIFY_COUNT, description = "The modify count from result if update/upsert is performed, otherwise not set."),
    @WritesAttribute(attribute = PutMongo.ATTRIBUTE_UPSERT_ID, description = "The '_id' hex value if upsert is performed, otherwise not set.")
})
public class PutMongo extends AbstractMongoProcessor {
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to MongoDB are routed to this relationship").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();

    static final String ATTRIBUTE_UPDATE_MATCH_COUNT = "mongo.put.update.match.count";
    static final String ATTRIBUTE_UPDATE_MODIFY_COUNT = "mongo.put.update.modify.count";
    static final String ATTRIBUTE_UPSERT_ID = "mongo.put.upsert.id";

    static final String MODE_INSERT = "insert";
    static final String MODE_UPDATE = "update";

    static final AllowableValue UPDATE_WITH_DOC = new AllowableValue("doc", "With whole document");
    static final AllowableValue UPDATE_WITH_OPERATORS = new AllowableValue("operators", "With operators enabled");

    static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
        .name("Mode")
        .description("Indicates whether the processor should insert or update content")
        .required(true)
        .allowableValues(MODE_INSERT, MODE_UPDATE)
        .defaultValue(MODE_INSERT)
        .build();
    static final PropertyDescriptor UPSERT = new PropertyDescriptor.Builder()
        .name("Upsert")
        .description("When true, inserts a document if no document matches the update query criteria; this property is valid only when using update mode, "
                + "otherwise it is ignored")
        .required(true)
        .dependsOn(MODE, MODE_UPDATE)
        .allowableValues("true", "false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .defaultValue("false")
        .build();
    static final PropertyDescriptor UPDATE_QUERY_KEY = new PropertyDescriptor.Builder()
        .name("Update Query Key")
        .description("One or more comma-separated document key names used to build the update query criteria, such as _id")
        .required(false)
        .dependsOn(MODE, MODE_UPDATE)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();
    static final PropertyDescriptor UPDATE_QUERY = new PropertyDescriptor.Builder()
        .name("putmongo-update-query")
        .displayName("Update Query")
        .description("Specify a full MongoDB query to be used for the lookup query to do an update/upsert. NOTE: this field is ignored if the '%s' value is not empty."
            .formatted(UPDATE_QUERY_KEY.getDisplayName()))
        .required(false)
        .dependsOn(MODE, MODE_UPDATE)
        .addValidator(JsonValidator.INSTANCE)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor UPDATE_OPERATION_MODE = new PropertyDescriptor.Builder()
        .displayName("Update Mode")
        .name("put-mongo-update-mode")
        .required(true)
        .dependsOn(MODE, MODE_UPDATE)
        .allowableValues(UPDATE_WITH_DOC, UPDATE_WITH_OPERATORS)
        .defaultValue(UPDATE_WITH_DOC)
        .description("Choose an update mode. You can either supply a JSON document to use as a direct replacement " +
                "or specify a document that contains update operators like $set, $unset, and $inc. " +
                "When Operators mode is enabled, the flowfile content is expected to be the operator part " +
                "for example: {$set:{\"key\": \"value\"},$inc:{\"count\":1234}} and the update query will come " +
                "from the configured Update Query property.")
         .build();
    static final PropertyDescriptor UPDATE_METHOD = new PropertyDescriptor.Builder()
        .name("Update Method")
        .dependsOn(UPDATE_OPERATION_MODE, UPDATE_WITH_OPERATORS)
        .description("MongoDB method for running collection update operations, such as updateOne or updateMany")
        .allowableValues(UpdateMethod.class)
        .defaultValue(UpdateMethod.UPDATE_ONE)
        .build();
    static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("The Character Set in which the data is encoded")
        .required(true)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .build();

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                    MODE,
                    UPSERT,
                    UPDATE_QUERY_KEY,
                    UPDATE_QUERY,
                    UPDATE_OPERATION_MODE,
                    UPDATE_METHOD,
                    CHARACTER_SET
            )
    ).toList();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        List<ValidationResult> problems = new ArrayList<>();

        if (validationContext.getProperty(MODE).getValue().equals(MODE_INSERT)) {
            return problems;
        }

        final boolean queryKey = validationContext.getProperty(UPDATE_QUERY_KEY).isSet();
        final boolean query    = validationContext.getProperty(UPDATE_QUERY).isSet();

        if (queryKey && query) {
            problems.add(new ValidationResult.Builder()
                .valid(false)
                .explanation("Both update query key and update query cannot be set at the same time.")
                .build()
            );
        } else if (!queryKey && !query) {
            problems.add(new ValidationResult.Builder()
                .valid(false)
                .explanation("Either the update query key or the update query field must be set.")
                .build()
            );
        }

        return problems;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final String processorMode = context.getProperty(MODE).getValue();
        final String updateOperationMode = context.getProperty(UPDATE_OPERATION_MODE).getValue();
        final WriteConcern writeConcern = clientService.getWriteConcern();

        try {
            final MongoCollection<Document> collection = getCollection(context, flowFile).withWriteConcern(writeConcern);
            // Read the contents of the FlowFile into a byte array
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, in -> StreamUtils.fillBuffer(in, content, true));

            // parse
            final Object doc = (processorMode.equals(MODE_INSERT) || (processorMode.equals(MODE_UPDATE) && updateOperationMode.equals(UPDATE_WITH_DOC.getValue())))
                    ? Document.parse(new String(content, charset)) : BasicDBObject.parse(new String(content, charset));

            if (MODE_INSERT.equals(processorMode)) {
                collection.insertOne((Document) doc);
                logger.info("inserted {} into MongoDB", flowFile);
            } else {
                // update
                final boolean upsert = context.getProperty(UPSERT).asBoolean();
                final String updateKey = context.getProperty(UPDATE_QUERY_KEY).evaluateAttributeExpressions(flowFile).getValue();
                final String filterQuery = context.getProperty(UPDATE_QUERY).evaluateAttributeExpressions(flowFile).getValue();
                final Document updateQuery;

                if (StringUtils.isNotBlank(updateKey)) {
                    updateQuery = parseUpdateKey(updateKey, (Map) doc);
                    removeUpdateKeys(updateKey, (Map) doc);
                } else {
                    updateQuery = Document.parse(filterQuery);
                }
                UpdateResult updateResult;
                if (updateOperationMode.equals(UPDATE_WITH_DOC.getValue())) {
                    updateResult = collection.replaceOne(updateQuery, (Document) doc, new ReplaceOptions().upsert(upsert));
                } else {
                    BasicDBObject update = (BasicDBObject) doc;
                    update.remove(updateKey);
                    UpdateOptions updateOptions = new UpdateOptions().upsert(upsert);
                    UpdateMethod updateQueryMode = context.getProperty(UPDATE_METHOD).asAllowableValue(UpdateMethod.class);

                    if (this.updateModeMatches(UpdateMethod.UPDATE_ONE, updateQueryMode, flowFile)) {
                        updateResult = collection.updateOne(updateQuery, update, updateOptions);
                    } else if (this.updateModeMatches(UpdateMethod.UPDATE_MANY, updateQueryMode, flowFile)) {
                        updateResult = collection.updateMany(updateQuery, update, updateOptions);
                    } else {
                        String flowfileUpdateMode = flowFile.getAttribute(ATTRIBUTE_MONGODB_UPDATE_MODE);
                        throw new ProcessException("Unrecognized '" + ATTRIBUTE_MONGODB_UPDATE_MODE + "' value '" + flowfileUpdateMode + "'");
                    }
                }

                flowFile = session.putAttribute(flowFile, ATTRIBUTE_UPDATE_MATCH_COUNT, String.valueOf(updateResult.getMatchedCount()));
                flowFile = session.putAttribute(flowFile, ATTRIBUTE_UPDATE_MODIFY_COUNT, String.valueOf(updateResult.getModifiedCount()));
                BsonValue upsertedId = updateResult.getUpsertedId();
                if (upsertedId != null) {
                    String id = upsertedId.isString() ? upsertedId.asString().getValue() : upsertedId.asObjectId().getValue().toString();
                    flowFile = session.putAttribute(flowFile, ATTRIBUTE_UPSERT_ID, id);
                }
                logger.info("updated {} into MongoDB", flowFile);
            }

            session.getProvenanceReporter().send(flowFile, getURI(context));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            logger.error("Failed to insert {} into MongoDB due to {}", flowFile, e, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private void removeUpdateKeys(String updateKeyParam, Map doc) {
        String[] parts = updateKeyParam.split(",[\\s]*");
        for (String part : parts) {
            if (part.contains(".")) {
                doc.remove(part);
            }
        }
    }

    private Document parseUpdateKey(String updateKey, Map doc) {
        Document retVal;
        if (updateKey.equals("_id")) {
            if (doc.get("_id") instanceof ObjectId) {
                retVal = new Document("_id", doc.get("_id"));
            } else if (ObjectId.isValid((String) doc.get("_id"))) {
                retVal = new Document("_id", new ObjectId((String) doc.get("_id")));
            } else {
                retVal = new Document("_id", doc.get("_id"));
            }
        } else if (updateKey.contains(",")) {
            String[] parts = updateKey.split(",[\\s]*");
            retVal = new Document();
            for (String part : parts) {
                retVal.append(part, doc.get(part));
            }
        } else {
            retVal = new Document(updateKey, doc.get(updateKey));
        }

        return retVal;
    }
}
