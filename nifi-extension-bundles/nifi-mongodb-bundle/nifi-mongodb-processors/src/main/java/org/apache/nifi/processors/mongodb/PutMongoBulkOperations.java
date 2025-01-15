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

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.DecoderContext;
import org.bson.conversions.Bson;
import org.bson.json.JsonReader;

import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@Tags({ "mongodb", "insert", "update", "write", "put", "bulk" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes the contents of a FlowFile to MongoDB as bulk-update")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutMongoBulkOperations extends AbstractMongoProcessor {
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to MongoDB are routed to this relationship").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();

    static final PropertyDescriptor ORDERED = new PropertyDescriptor.Builder()
            .name("Ordered")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .description("Ordered execution of bulk-writes and break on error - otherwise arbitrary order and continue on error")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
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
            Stream.of(ORDERED, CHARACTER_SET)
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (null == flowFile) {
            return;
        }

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final WriteConcern writeConcern = clientService.getWriteConcern();

        try {
            final MongoCollection<Document> collection = getCollection(context, flowFile).withWriteConcern(writeConcern);

            final BsonArrayCodec arrayCodec = new BsonArrayCodec();
            final DecoderContext decoderContext = DecoderContext.builder().build();
            final BsonArray updateItems;
            try (final Reader reader = new InputStreamReader(session.read(flowFile), charset)) {
                updateItems = arrayCodec.decode(new JsonReader(reader), decoderContext);
            }

            List<WriteModel<Document>> updateModels = new ArrayList<>();
            for (Object item : updateItems) {
                final BsonDocument updateItem = (BsonDocument) item;
                if (updateItem.keySet().size() != 1) {
                    getLogger().error("Invalid bulk-update in {}: more than one type given {}", flowFile, String.join(", ", updateItem.keySet()));
                    session.transfer(flowFile, REL_FAILURE);
                    context.yield();
                    return;
                }
                final WriteModel<Document> writeModel = getWriteModel(updateItem);
                if (null == writeModel) {
                    getLogger().error("Invalid bulk-update in {}: invalid update type {}", flowFile, getUpdateType(updateItem));
                    session.transfer(flowFile, REL_FAILURE);
                    context.yield();
                    return;
                }
                updateModels.add(writeModel);
            }

            collection.bulkWrite(updateModels, (new BulkWriteOptions().ordered(context.getProperty(ORDERED).asBoolean())));
            getLogger().info("bulk-updated {} into MongoDB", flowFile);

            session.getProvenanceReporter().send(flowFile, getURI(context));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to bulk-update {} into MongoDB", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private WriteModel<Document> getWriteModel(final BsonDocument updateItem) {
        final String updateType = getUpdateType(updateItem);
        final BsonDocument updateSpec = (BsonDocument) updateItem.get(updateType);
        final WriteModel<Document> writeModel;
        if ("insertOne".equals(updateType)) {
            writeModel = new InsertOneModel<>(toBsonDocument((BsonDocument) updateSpec.get("document")));
        } else if ("updateOne".equals(updateType)) {
            final UpdateOptions options = parseUpdateOptions(updateSpec);
            writeModel = new UpdateOneModel<>((BsonDocument) updateSpec.get("filter"), (BsonDocument) updateSpec.get("update"), options);
        } else if ("updateMany".equals(updateType)) {
            final UpdateOptions options = parseUpdateOptions(updateSpec);
            writeModel = new UpdateManyModel<>((BsonDocument) updateSpec.get("filter"), (BsonDocument) updateSpec.get("update"), options);
        } else if ("replaceOne".equals(updateType)) {
            final ReplaceOptions options = parseReplaceOptions(updateSpec);
            writeModel = new ReplaceOneModel<>((BsonDocument) updateSpec.get("filter"),
                    toBsonDocument((BsonDocument) updateSpec.get("replacement")), options);
        } else if ("deleteOne".equals(updateType)) {
            final DeleteOptions options = parseDeleteOptions(updateSpec);
            writeModel = new DeleteOneModel<>((BsonDocument) updateSpec.get("filter"), options);
        } else  if ("deleteMany".equals(updateType)) {
            final DeleteOptions options = parseDeleteOptions(updateSpec);
            writeModel = new DeleteManyModel<>((BsonDocument) updateSpec.get("filter"), options);
        } else {
            return null;
        }
        return writeModel;
    }

    private static String getUpdateType(BsonDocument updateItem) {
        return updateItem.keySet().iterator().next();
    }

    private static Document toBsonDocument(BsonDocument doc) {
        if (null == doc) {
            return null;
        }
        return new Document(doc);
    }

    protected UpdateOptions parseUpdateOptions(BsonDocument updateSpec) {
        final UpdateOptions options = new UpdateOptions();
        if (updateSpec.containsKey("upsert")) {
            options.upsert(updateSpec.getBoolean("upsert").getValue());
        }
        if (updateSpec.containsKey("arrayFilters")) {
            options.arrayFilters((List<? extends Bson>) updateSpec.get("arrayFilters"));
        }
        if (updateSpec.containsKey("collation")) {
            options.collation(parseCollation((BsonDocument) updateSpec.get("collation")));
        }
        return options;
    }

    protected ReplaceOptions parseReplaceOptions(BsonDocument updateSpec) {
        final ReplaceOptions options = new ReplaceOptions();
        if (updateSpec.containsKey("upsert")) {
            options.upsert(updateSpec.getBoolean("upsert").getValue());
        }
        if (updateSpec.containsKey("collation")) {
            options.collation(parseCollation((BsonDocument) updateSpec.get("collation")));
        }
        return options;
    }

    protected DeleteOptions parseDeleteOptions(BsonDocument updateSpec) {
        final DeleteOptions options = new DeleteOptions();
        if (updateSpec.containsKey("collation")) {
            options.collation(parseCollation((BsonDocument) updateSpec.get("collation")));
        }
        return options;
    }

    protected Collation parseCollation(BsonDocument collationSpec) {
        final Collation.Builder builder = Collation.builder();
        if (collationSpec.containsKey("locale")) {
            builder.locale(collationSpec.getString("locale").getValue());
        }
        if (collationSpec.containsKey("caseLevel")) {
            builder.caseLevel(collationSpec.getBoolean("caseLevel").getValue());
        }
        if (collationSpec.containsKey("caseFirst")) {
            builder.collationCaseFirst(CollationCaseFirst.fromString(collationSpec.getString("caseFirst").getValue()));
        }
        if (collationSpec.containsKey("strength")) {
            builder.collationStrength(CollationStrength.fromInt(collationSpec.getInt32("strength").getValue()));
        }
        if (collationSpec.containsKey("numericOrdering")) {
            builder.numericOrdering(collationSpec.getBoolean("numericOrdering").getValue());
        }
        if (collationSpec.containsKey("alternate")) {
            builder.collationAlternate(CollationAlternate.fromString(collationSpec.getString("alternate").getValue()));
        }
        if (collationSpec.containsKey("maxVariable")) {
            builder.collationMaxVariable(CollationMaxVariable.fromString(collationSpec.getString("maxVariable").getValue()));
        }
        if (collationSpec.containsKey("normalization")) {
            builder.normalization(collationSpec.getBoolean("normalization").getValue());
        }
        if (collationSpec.containsKey("backwards")) {
            builder.backwards(collationSpec.getBoolean("backwards").getValue());
        }
        return builder.build();
    }

}
