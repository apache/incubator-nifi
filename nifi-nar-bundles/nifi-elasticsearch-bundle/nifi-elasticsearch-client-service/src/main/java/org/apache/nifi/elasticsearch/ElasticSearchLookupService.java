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

package org.apache.nifi.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.JsonInferenceSchemaRegistryService;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ElasticSearchLookupService extends JsonInferenceSchemaRegistryService implements LookupService<Record> {
    public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("el-rest-client-service")
        .displayName("Client Service")
        .description("An ElasticSearch client service to use for running queries.")
        .identifiesControllerService(ElasticSearchClientService.class)
        .required(true)
        .build();
    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
        .name("el-lookup-index")
        .displayName("Index")
        .description("The name of the index to read from")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
        .name("el-lookup-type")
        .displayName("Type")
        .description("The type of this document (used by Elasticsearch for indexing and searching)")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private ElasticSearchClientService clientService;

    private String index;
    private String type;
    private ObjectMapper mapper;

    private final List<PropertyDescriptor> DESCRIPTORS;

    ElasticSearchLookupService() {
        List<PropertyDescriptor> _desc = new ArrayList<>();
        _desc.addAll(super.getSupportedPropertyDescriptors());
        _desc.add(CLIENT_SERVICE);
        _desc.add(INDEX);
        _desc.add(TYPE);
        DESCRIPTORS = Collections.unmodifiableList(_desc);
    }

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class);
        index = context.getProperty(INDEX).evaluateAttributeExpressions().getValue();
        type  = context.getProperty(TYPE).evaluateAttributeExpressions().getValue();
        mapper = new ObjectMapper();

        super.onEnabled(context);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        Map<String, String> context = coordinates.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().toString()
            ));
        return lookup(coordinates, context);
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        validateCoordinates(coordinates);

        try {
            Record record;
            if (coordinates.containsKey("_id")) {
                record = getById((String)coordinates.get("_id"), context);
            } else {
                record = getByQuery(coordinates, context);
            }

            return record == null ? Optional.empty() : Optional.of(record);
        } catch (Exception ex) {
            getLogger().error("Error during lookup.", ex);
            throw new LookupFailureException(ex);
        }
    }

    private RecordSchema getSchemaFromCoordinates(Map<String, Object> coordinates) {
        Map<String, String> variables = coordinates.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue().toString()
            ));
        try {
            return getSchema(variables, null);
        } catch (SchemaNotFoundException | IOException e) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Could not load schema, will create one from the results.", e);
            }
            return null;
        }
    }

    private void validateCoordinates(Map coordinates) throws LookupFailureException {
        List<String> reasons = new ArrayList<>();

        if (coordinates.containsKey("_id") && !(coordinates.get("_id") instanceof String)) {
            reasons.add("_id was supplied, but it was not a String.");
        }

        if (coordinates.containsKey("_id") && coordinates.size() > 1) {
            reasons.add("When _id is used, it can be the only key used in the lookup.");
        }

        if (reasons.size() > 0) {
            String error = String.join("\n", reasons);
            throw new LookupFailureException(error);
        }
    }

    private Record getById(final String _id, Map<String, String> context) throws IOException, LookupFailureException, SchemaNotFoundException {
        Map<String, Object> query = new HashMap<String, Object>(){{
            put("query", new HashMap<String, Object>() {{
                put("match", new HashMap<String, String>(){{
                    put("_id", _id);
                }});
            }});
        }};

        String json = mapper.writeValueAsString(query);

        SearchResponse response = clientService.search(json, index, type);

        if (response.getNumberOfHits() > 1) {
            throw new LookupFailureException(String.format("Expected 1 response, got %d for query %s",
                response.getNumberOfHits(), json));
        } else if (response.getNumberOfHits() == 0) {
            return null;
        }

        final Map<String, Object> source = (Map)response.getHits().get(0).get("_source");

        RecordSchema toUse = getSchema(context, source, null);

        return new MapRecord(toUse, source);
    }

    private Map<String, Object> buildQuery(Map<String, Object> coordinates) {
        Map<String, Object> query = new HashMap<String, Object>(){{
            put("bool", new HashMap<String, Object>(){{
                put("must", coordinates.entrySet().stream()
                    .map(e -> new HashMap<String, Object>(){{
                        put("match", new HashMap<String, Object>(){{
                            put(e.getKey(), e.getValue());
                        }});
                    }}).collect(Collectors.toList())
                );
            }});
        }};

        Map<String, Object> outter = new HashMap<String, Object>(){{
            put("size", 1);
            put("query", query);
        }};

        return outter;
    }

    private Record getByQuery(final Map<String, Object> query, Map<String, String> context) throws LookupFailureException {
        try {
            final String json = mapper.writeValueAsString(buildQuery(query));

            SearchResponse response = clientService.search(json, index, type);

            if (response.getNumberOfHits() == 0) {
                return null;
            } else {
                final Map<String, Object> source = (Map)response.getHits().get(0).get("_source");
                RecordSchema toUse = getSchema(context, source, null);
                return new MapRecord(toUse, source);
            }

        } catch (Exception e) {
            throw new LookupFailureException(e);
        }
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }
}
