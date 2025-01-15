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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.graph.GraphClientService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Tags({"graph", "gremlin", "cypher"})
@CapabilityDescription("This uses FlowFile records as input to perform graph mutations. Each record is associated with an individual query/mutation, and a FlowFile will "
    + "be output for each successful operation. Failed records will be sent as a single FlowFile to the failure relationship.")
@WritesAttributes({
        @WritesAttribute(attribute = ExecuteGraphQueryRecord.GRAPH_OPERATION_TIME, description = "The amount of time it took to execute all of the graph operations."),
        @WritesAttribute(attribute = ExecuteGraphQueryRecord.RECORD_COUNT, description = "The number of records unsuccessfully processed (written on FlowFiles routed to the "
                + "'failure' relationship.")
})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "A dynamic property to be used as a parameter in the graph script",
        value = "The variable name to be set", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Uses a record path to set a variable as a parameter in the graph script")
public class ExecuteGraphQueryRecord extends  AbstractGraphExecutor {

    public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("client-service")
            .displayName("Client Service")
            .description("The graph client service for connecting to a graph database.")
            .identifiesControllerService(GraphClientService.class)
            .addValidator(Validator.VALID)
            .required(true)
            .build();

    public static final PropertyDescriptor READER_SERVICE = new PropertyDescriptor.Builder()
            .name("reader-service")
            .displayName("Record Reader")
            .description("The record reader to use with this processor.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor WRITER_SERVICE = new PropertyDescriptor.Builder()
            .name("writer-service")
            .displayName("Failed Record Writer")
            .description("The record writer to use for writing failed records.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor SUBMISSION_SCRIPT = new PropertyDescriptor.Builder()
            .name("record-script")
            .displayName("Graph Record Script")
            .description("Script to perform the business logic on graph, using flow file attributes and custom properties " +
                    "as variable-value pairs in its logic.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    }

    public static final Relationship SUCCESS = new Relationship.Builder().name("original")
                                                    .description("Original flow files that successfully interacted with " +
                                                            "graph server.")
                                                    .build();
    public static final Relationship FAILURE = new Relationship.Builder().name("failure")
                                                    .description("Flow files that fail to interact with graph server.")
                                                    .build();
    public static final Relationship GRAPH = new Relationship.Builder().name("response")
                                                    .description("The response object from the graph server.")
                                                    .autoTerminateDefault(true)
                                                    .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CLIENT_SERVICE,
            READER_SERVICE,
            WRITER_SERVICE,
            SUBMISSION_SCRIPT
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS,
            FAILURE,
            GRAPH
    );

    public static final String RECORD_COUNT = "record.count";
    public static final String GRAPH_OPERATION_TIME = "graph.operations.took";
    private volatile RecordPathCache recordPathCache;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private GraphClientService clientService;
    private RecordReaderFactory recordReaderFactory;
    private RecordSetWriterFactory recordSetWriterFactory;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(GraphClientService.class);
        recordReaderFactory = context.getProperty(READER_SERVICE).asControllerService(RecordReaderFactory.class);
        recordSetWriterFactory = context.getProperty(WRITER_SERVICE).asControllerService(RecordSetWriterFactory.class);
        recordPathCache = new RecordPathCache(100);
    }

    private Object getRecordValue(Record record, RecordPath recordPath) {
        final RecordPathResult result = recordPath.evaluate(record);
        final List<FieldValue> values = result.getSelectedFields().collect(Collectors.toList());
        if (values != null && !values.isEmpty()) {
            if (values.size() == 1) {
                Object raw = values.get(0).getValue();

                if (raw != null && raw.getClass().isArray()) {
                    Object[] arr = (Object[]) raw;
                    raw = Arrays.asList(arr);
                }

                return raw;
            } else {
                return values.stream().map(fv -> fv.getValue()).collect(Collectors.toList());
            }
        } else {
            return null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if ( input == null ) {
            return;
        }

        String recordScript = context.getProperty(SUBMISSION_SCRIPT)
                .evaluateAttributeExpressions(input)
                .getValue();

        Map<String, RecordPath> dynamic = new HashMap<>();

        FlowFile finalInput = input;
        context.getProperties()
                .keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .forEach(it ->
                    dynamic.put(it.getName(), recordPathCache.getCompiled(
                                    context
                                    .getProperty(it.getName())
                                    .evaluateAttributeExpressions(finalInput)
                                    .getValue()))
                );

        long delta;
        FlowFile failedRecords = session.create(input);
        WriteResult failedWriteResult = null;
        try (InputStream is = session.read(input);
             RecordReader reader = recordReaderFactory.createRecordReader(input, is, getLogger());
             OutputStream os = session.write(failedRecords);
             RecordSetWriter failedWriter = recordSetWriterFactory.createWriter(getLogger(), reader.getSchema(), os, input.getAttributes())
        ) {
            Record record;
            long start = System.currentTimeMillis();
            failedWriter.beginRecordSet();
            int records = 0;
            while ((record = reader.nextRecord()) != null) {
                FlowFile graph = session.create(input);

                try {
                    Map<String, Object> dynamicPropertyMap = new HashMap<>();
                    for (String entry : dynamic.keySet()) {
                        if (!dynamicPropertyMap.containsKey(entry)) {
                            dynamicPropertyMap.put(entry, getRecordValue(record, dynamic.get(entry)));
                        }
                    }

                    dynamicPropertyMap.putAll(input.getAttributes());
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("Dynamic Properties: {}", dynamicPropertyMap);
                    }
                    List<Map<String, Object>> graphResponses = new ArrayList<>(executeQuery(recordScript, dynamicPropertyMap));

                    OutputStream graphOutputStream = session.write(graph);
                    String graphOutput = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(graphResponses);
                    graphOutputStream.write(graphOutput.getBytes(StandardCharsets.UTF_8));
                    graphOutputStream.close();
                    session.transfer(graph, GRAPH);
                } catch (Exception e) {
                    getLogger().error("Error processing record at index {}", records, e);
                    // write failed records to a flowfile destined for the failure relationship
                    failedWriter.write(record);
                    session.remove(graph);
                } finally {
                    records++;
                }
            }
            long end = System.currentTimeMillis();
            delta = (end - start) / 1000;
            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Took %s seconds.\nHandled %d records", delta, records));
            }
            failedWriteResult = failedWriter.finishRecordSet();
            failedWriter.flush();

        } catch (Exception ex) {
            getLogger().error("Error reading records, routing input FlowFile to failure", ex);
            session.remove(failedRecords);
            session.transfer(input, FAILURE);
            return;
        }

        // Generate provenance and send input flowfile to success
        session.getProvenanceReporter().send(input, clientService.getTransitUrl(), delta * 1000);

        if (failedWriteResult.getRecordCount() < 1) {
            // No failed records, remove the failure flowfile and send the input flowfile to success
            session.remove(failedRecords);
            input = session.putAttribute(input, GRAPH_OPERATION_TIME, String.valueOf(delta));
            session.transfer(input, SUCCESS);
        } else {
            failedRecords = session.putAttribute(failedRecords, RECORD_COUNT, String.valueOf(failedWriteResult.getRecordCount()));
            session.transfer(failedRecords, FAILURE);
            // There were failures, don't send the input flowfile to SUCCESS
            session.remove(input);
        }
    }

    private List<Map<String, Object>> executeQuery(String recordScript, Map<String, Object> parameters) {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> graphResponses = new ArrayList<>();
        clientService.executeQuery(recordScript, parameters, (map, b) -> {
            if (getLogger().isDebugEnabled()) {
                try {
                    getLogger().debug(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map));
                } catch (JsonProcessingException ex) {
                    getLogger().error("Error converted map to JSON ", ex);
                }
            }
            graphResponses.add(map);
        });
        return graphResponses;
    }
}
