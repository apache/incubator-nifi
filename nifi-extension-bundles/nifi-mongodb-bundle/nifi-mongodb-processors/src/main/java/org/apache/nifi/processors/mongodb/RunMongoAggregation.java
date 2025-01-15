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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@Tags({"mongo", "aggregation", "aggregate"})
@CapabilityDescription("A processor that runs an aggregation query whenever a flowfile is received.")
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
public class RunMongoAggregation extends AbstractMongoProcessor {

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .description("The input flowfile gets sent to this relationship when the query succeeds.")
            .name("original")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .description("The input flowfile gets sent to this relationship when the query fails.")
            .name("failure")
            .build();
    static final Relationship REL_RESULTS = new Relationship.Builder()
            .description("The result set of the aggregation will be sent to this relationship.")
            .name("results")
            .build();

    static final List<Bson> buildAggregationQuery(String query) throws IOException {
        List<Bson> result = new ArrayList<>();

        ObjectMapper mapper = new ObjectMapper();
        List<Map> querySteps = mapper.readValue(query, List.class);
        for (Map<?, ?> queryStep : querySteps) {
            BasicDBObject bson = BasicDBObject.parse(mapper.writeValueAsString(queryStep));
            result.add(bson);
        }

        return result;
    }

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("mongo-agg-query")
            .displayName("Query")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .description("The aggregation query to be executed.")
            .required(true)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    static final PropertyDescriptor ALLOW_DISK_USE = new PropertyDescriptor.Builder()
            .name("allow-disk-use")
            .displayName("Allow Disk Use")
            .description("Set this to true to enable writing data to temporary files to prevent exceeding the " +
                    "maximum memory use limit during aggregation pipeline staged when handling large datasets.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_RESULTS,
            REL_ORIGINAL,
            REL_FAILURE
    );

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                    CHARSET,
                    QUERY,
                    ALLOW_DISK_USE,
                    JSON_TYPE,
                    QUERY_ATTRIBUTE,
                    BATCH_SIZE,
                    RESULTS_PER_FLOWFILE,
                    DATE_FORMAT
            )
    ).toList();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private String buildBatch(List<Document> batch) {
        String retVal;
        try {
            retVal = objectMapper.writeValueAsString(batch.size() > 1 ? batch : batch.getFirst());
        } catch (Exception e) {
            retVal = null;
        }

        return retVal;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = null;
        if (context.hasIncomingConnection()) {
            flowFile = session.get();

            if (flowFile == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();
        final Boolean allowDiskUse = context.getProperty(ALLOW_DISK_USE).asBoolean();
        final String queryAttr = context.getProperty(QUERY_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
        final Integer batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final Integer resultsPerFlowfile = context.getProperty(RESULTS_PER_FLOWFILE).asInteger();
        final String jsonTypeSetting = context.getProperty(JSON_TYPE).getValue();
        final String dateFormat      = context.getProperty(DATE_FORMAT).evaluateAttributeExpressions(flowFile).getValue();

        configureMapper(jsonTypeSetting, dateFormat);

        Map<String, String> attrs = new HashMap<>();
        if (queryAttr != null && !queryAttr.trim().isEmpty()) {
            attrs.put(queryAttr, query);
        }

        MongoCursor<Document> iter = null;

        try {
            MongoCollection<Document> collection = getCollection(context, flowFile);
            List<Bson> aggQuery = buildAggregationQuery(query);
            AggregateIterable<Document> it = collection.aggregate(aggQuery).allowDiskUse(allowDiskUse);
            it.batchSize(batchSize != null ? batchSize : 1);

            iter = it.iterator();
            List<Document> batch = new ArrayList<>();
            Boolean doneSomething = false;

            while (iter.hasNext()) {
                batch.add(iter.next());
                if (batch.size() == resultsPerFlowfile) {
                    writeBatch(buildBatch(batch), flowFile, context, session, attrs, REL_RESULTS);
                    batch = new ArrayList<>();
                    doneSomething |= true;
                }
            }

            if (!batch.isEmpty()) {
                // Something remains in batch list, write it to RESULT
                writeBatch(buildBatch(batch), flowFile, context, session, attrs, REL_RESULTS);
            } else if (!doneSomething) {
                // The batch list is empty and no batch was written (empty result!), so write empty string to RESULT
                writeBatch("", flowFile, context, session, attrs, REL_RESULTS);
            }

            if (flowFile != null) {
                session.transfer(flowFile, REL_ORIGINAL);
            }
        } catch (Exception e) {
            getLogger().error("Error running MongoDB aggregation query.", e);
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
    }
}
