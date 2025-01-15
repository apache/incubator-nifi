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

package org.apache.nifi.processors.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

@CapabilityDescription("A record-based version of GetMongo that uses the Record writers to write the MongoDB result set.")
@Tags({"mongo", "mongodb", "get", "fetch", "record", "json"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@WritesAttributes({
    @WritesAttribute(attribute = GetMongo.DB_NAME, description = "The database where the results came from."),
    @WritesAttribute(attribute = GetMongo.COL_NAME, description = "The collection where the results came from.")
})
public class GetMongoRecord extends AbstractMongoQueryProcessor {
    public static final PropertyDescriptor WRITER_FACTORY = new PropertyDescriptor.Builder()
        .name("get-mongo-record-writer-factory")
        .displayName("Record Writer")
        .description("The record writer to use to write the result sets.")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();
    public static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
        .name("mongodb-schema-name")
        .displayName("Schema Name")
        .description("The name of the schema in the configured schema registry to use for the query results.")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .defaultValue("${schema.name}")
        .required(true)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CLIENT_SERVICE,
            WRITER_FACTORY,
            DATABASE_NAME,
            COLLECTION_NAME,
            SCHEMA_NAME,
            QUERY_ATTRIBUTE,
            QUERY,
            PROJECTION,
            SORT,
            LIMIT,
            BATCH_SIZE
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_ORIGINAL
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private volatile MongoDBClientService clientService;
    private volatile RecordSetWriterFactory writerFactory;

    @OnScheduled
    public void onEnabled(ProcessContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(MongoDBClientService.class);
        writerFactory = context.getProperty(WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = null;

        if (context.hasIncomingConnection()) {
            input = session.get();
            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final String database = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(input).getValue();
        final String collection = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions(input).getValue();
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(input).getValue();
        final Document query = getQuery(context, session, input);

        MongoCollection mongoCollection = clientService.getDatabase(database).getCollection(collection);

        FindIterable<Document> find = mongoCollection.find(query);
        if (context.getProperty(SORT).isSet()) {
            find = find.sort(Document.parse(context.getProperty(SORT).evaluateAttributeExpressions(input).getValue()));
        }
        if (context.getProperty(PROJECTION).isSet()) {
            find = find.projection(Document.parse(context.getProperty(PROJECTION).evaluateAttributeExpressions(input).getValue()));
        }
        if (context.getProperty(LIMIT).isSet()) {
            find = find.limit(context.getProperty(LIMIT).evaluateAttributeExpressions(input).asInteger());
        }

        MongoCursor<Document> cursor = find.iterator();

        FlowFile output = input != null ? session.create(input) : session.create();
        final FlowFile inputPtr = input;
        try {
            final Map<String, String> attributes = getAttributes(context, input, query, mongoCollection);
            try (OutputStream out = session.write(output)) {
                Map<String, String> attrs = inputPtr != null ? inputPtr.getAttributes() : Map.of("schema.name", schemaName);
                RecordSchema schema = writerFactory.getSchema(attrs, null);
                RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out, attrs);
                long count = 0L;
                writer.beginRecordSet();
                while (cursor.hasNext()) {
                    Document next = cursor.next();
                    if (next.get("_id") instanceof ObjectId) {
                        next.put("_id", next.get("_id").toString());
                    }
                    Record record = new MapRecord(schema, next);
                    writer.write(record);
                    count++;
                }
                writer.finishRecordSet();
                writer.close();
                out.close();
                attributes.put("record.count", String.valueOf(count));
            } catch (SchemaNotFoundException e) {
                throw new RuntimeException(e);
            }


            output = session.putAllAttributes(output, attributes);

            session.getProvenanceReporter().fetch(output, getURI(context));
            session.transfer(output, REL_SUCCESS);
            if (input != null) {
                session.transfer(input, REL_ORIGINAL);
            }
        } catch (Exception ex) {
            getLogger().error("Error writing record set from Mongo query.", ex);
            session.remove(output);
            if (input != null) {
                session.transfer(input, REL_FAILURE);
            }
        }
    }
}
