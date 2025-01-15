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
package org.apache.nifi.processors.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.graph.GraphClientService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@SupportsBatching
@Tags({"cypher", "neo4j", "graph", "network", "insert", "update", "delete", "put", "get",
        "node", "relationship", "connection", "executor", "gremlin", "tinkerpop"})
@CapabilityDescription("This processor is designed to execute queries in either the Cypher query language or the Tinkerpop " +
        "Gremlin DSL. It delegates most of the logic to a configured client service that handles the interaction with the " +
        "remote data source. All of the output is written out as JSON data.")
@WritesAttributes({
    @WritesAttribute(attribute = AbstractGraphExecutor.ERROR_MESSAGE, description = "GraphDB error message"),
    @WritesAttribute(attribute = GraphClientService.LABELS_ADDED, description = "Number of labels added"),
    @WritesAttribute(attribute = GraphClientService.NODES_CREATED, description = "Number of nodes created"),
    @WritesAttribute(attribute = GraphClientService.NODES_DELETED, description = "Number of nodes deleted"),
    @WritesAttribute(attribute = GraphClientService.PROPERTIES_SET, description = "Number of properties set"),
    @WritesAttribute(attribute = GraphClientService.RELATIONS_CREATED, description = "Number of relationships created"),
    @WritesAttribute(attribute = GraphClientService.RELATIONS_DELETED, description = "Number of relationships deleted"),
    @WritesAttribute(attribute = GraphClientService.ROWS_RETURNED, description = "Number of rows returned"),
    @WritesAttribute(attribute = ExecuteGraphQuery.EXECUTION_TIME, description = "The amount of time in milliseconds that the query" +
            "took to execute.")
    })
public class ExecuteGraphQuery extends AbstractGraphExecutor {

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS,
        REL_ORIGINAL,
        REL_FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        CLIENT_SERVICE,
        QUERY
    );

    public static final String EXECUTION_TIME = "query.took";

    protected ObjectMapper mapper = new ObjectMapper();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private volatile GraphClientService clientService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(GraphClientService.class);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        FlowFile output = flowFile != null ? session.create(flowFile) : session.create();
        try (OutputStream os = session.write(output)) {
            String query = getQuery(context, session, flowFile);
            long startTimeMillis = System.currentTimeMillis();

            os.write("[".getBytes());
            Map<String, String> resultAttrs = clientService.executeQuery(query, getParameters(context, output), (record, hasMore) -> {
                try {
                    String obj = mapper.writeValueAsString(record);
                    os.write(obj.getBytes());
                    if (hasMore) {
                        os.write(",".getBytes());
                    }
                } catch (Exception ex) {
                    throw new ProcessException(ex);
                }
            });
            os.write("]".getBytes());
            os.close();

            final long endTimeMillis = System.currentTimeMillis();

            String executionTime = String.valueOf((endTimeMillis - startTimeMillis));
            resultAttrs.put(EXECUTION_TIME, executionTime);
            resultAttrs.put(CoreAttributes.MIME_TYPE.key(), "application/json");
            output = session.putAllAttributes(output, resultAttrs);
            session.transfer(output, REL_SUCCESS);
            session.getProvenanceReporter().invokeRemoteProcess(output, clientService.getTransitUrl(),
                String.format("The following query was executed in %s milliseconds: \"%s\"", executionTime, query)
            );
            if (flowFile != null) {
                session.transfer(flowFile, REL_ORIGINAL);
            }

        } catch (Exception exception) {
            getLogger().error("Failed to execute graph statement due to {}", exception.getLocalizedMessage(), exception);
            session.remove(output);
            if (flowFile != null) {
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE, String.valueOf(exception.getMessage()));
                session.transfer(flowFile, REL_FAILURE);
            }
            context.yield();
        }
    }

    protected String getQuery(ProcessContext context, ProcessSession session, FlowFile input) {
        String query = context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue();
        if (StringUtils.isEmpty(query) && input != null) {
            try {
                if (input.getSize() > (64 * 1024)) {
                    throw new Exception("Input bigger than 64kb. Cannot assume this is a valid query for Gremlin Server " +
                            "or Neo4J.");
                    /*
                     * Note: Gremlin Server compiles the query down to Java byte code, and queries 64kb and above often
                     * bounce because they violate Java method size limits. We might want to revist this, but as a starting
                     * point, it is a sane default to assume that if a flowfile is bigger than about 64kb it is either not a
                     * query or it is a query likely to be a very poor fit here.
                     */
                }

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                session.exportTo(input, out);
                out.close();


                query = new String(out.toByteArray());
            } catch (Exception ex) {
                throw new ProcessException(ex);
            }
        }
        return query;
    }
}