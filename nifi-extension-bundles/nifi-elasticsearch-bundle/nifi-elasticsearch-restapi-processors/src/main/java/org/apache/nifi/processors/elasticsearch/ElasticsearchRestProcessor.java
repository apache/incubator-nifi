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

package org.apache.nifi.processors.elasticsearch;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticsearchException;
import org.apache.nifi.elasticsearch.ElasticsearchRequestOptions;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.elasticsearch.api.QueryDefinitionType;
import org.apache.nifi.util.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface ElasticsearchRestProcessor extends Processor, VerifiableProcessor {
    String ATTR_RECORD_COUNT = "record.count";
    String VERIFICATION_STEP_INDEX_EXISTS = "Elasticsearch Index Exists";
    String VERIFICATION_STEP_QUERY_JSON_VALID = "Elasticsearch Query JSON Valid";
    String VERIFICATION_STEP_QUERY_VALID = "Elasticsearch Query Valid";
    String DEFAULT_MAX_STRING_LENGTH = "20 MB";
    String DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER = "HEADER:";
    Predicate<Map.Entry<PropertyDescriptor, String>> REQUEST_HEADER_FILTER = entry ->
            StringUtils.startsWith(entry.getKey().getName(), DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER);

    PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("el-rest-fetch-index")
            .displayName("Index")
            .description("The name of the index to use.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("el-rest-type")
            .displayName("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor QUERY_DEFINITION_STYLE = new PropertyDescriptor.Builder()
            .name("el-rest-query-definition-style")
            .displayName("Query Definition Style")
            .description("How the JSON Query will be defined for use by the processor.")
            .required(true)
            .allowableValues(QueryDefinitionType.class)
            .defaultValue(QueryDefinitionType.FULL_QUERY)
            .build();

    PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("el-rest-query")
            .displayName("Query")
            .description("A query in JSON syntax, not Lucene syntax. Ex: {\"query\":{\"match\":{\"somefield\":\"somevalue\"}}}. " +
                    "If this parameter is not set, the query will be read from the flowfile content. " +
                    "If the query (property and flowfile content) is empty, a default empty JSON Object will be used, " +
                    "which will result in a \"match_all\" query in Elasticsearch.")
            .dependsOn(QUERY_DEFINITION_STYLE, QueryDefinitionType.FULL_QUERY)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    PropertyDescriptor QUERY_CLAUSE = new PropertyDescriptor.Builder()
            .name("el-rest-query-clause")
            .displayName("Query Clause")
            .description("A \"query\" clause in JSON syntax, not Lucene syntax. Ex: {\"match\":{\"somefield\":\"somevalue\"}}. " +
                    "If the query is empty, a default JSON Object will be used, which will result in a \"match_all\" query in Elasticsearch.")
            .dependsOn(QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    PropertyDescriptor SCRIPT = new PropertyDescriptor.Builder()
            .name("el-rest-script")
            .displayName("Script")
            .description("A \"script\" to execute during the operation, in JSON syntax. " +
                    "Ex: {\"source\": \"ctx._source.count++\", \"lang\": \"painless\"}")
            .dependsOn(QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    PropertyDescriptor SIZE = new PropertyDescriptor.Builder()
            .name("es-rest-size")
            .displayName("Size")
            .description("The maximum number of documents to retrieve in the query. If the query is paginated, " +
                    "this \"size\" applies to each page of the query, not the \"size\" of the entire result set.")
            .dependsOn(QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    PropertyDescriptor AGGREGATIONS = new PropertyDescriptor.Builder()
            .name("es-rest-query-aggs")
            .displayName("Aggregations")
            .description("One or more query aggregations (or \"aggs\"), in JSON syntax. " +
                    "Ex: {\"items\": {\"terms\": {\"field\": \"product\", \"size\": 10}}}")
            .dependsOn(QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    PropertyDescriptor SORT = new PropertyDescriptor.Builder()
            .name("es-rest-query-sort")
            .displayName("Sort")
            .description("Sort results by one or more fields, in JSON syntax. " +
                    "Ex: [{\"price\" : {\"order\" : \"asc\", \"mode\" : \"avg\"}}, {\"post_date\" : {\"format\": \"strict_date_optional_time_nanos\"}}]")
            .dependsOn(QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("es-rest-query-fields")
            .displayName("Fields")
            .description("Fields of indexed documents to be retrieved, in JSON syntax. " +
                    "Ex: [\"user.id\", \"http.response.*\", {\"field\": \"@timestamp\", \"format\": \"epoch_millis\"}]")
            .dependsOn(QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    PropertyDescriptor SCRIPT_FIELDS = new PropertyDescriptor.Builder()
            .name("es-rest-query-script-fields")
            .displayName("Script Fields")
            .description("Fields to created using script evaluation at query runtime, in JSON syntax. " +
                    "Ex: {\"test1\": {\"script\": {\"lang\": \"painless\", \"source\": \"doc['price'].value * 2\"}}, " +
                    "\"test2\": {\"script\": {\"lang\": \"painless\", \"source\": \"doc['price'].value * params.factor\", \"params\": {\"factor\": 2.0}}}}")
            .dependsOn(QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    PropertyDescriptor QUERY_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("el-query-attribute")
            .displayName("Query Attribute")
            .description("If set, the executed query will be set on each result flowfile in the specified attribute.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .required(false)
            .build();

    PropertyDescriptor MAX_JSON_FIELD_STRING_LENGTH = new PropertyDescriptor.Builder()
            .name("Max JSON Field String Length")
            .description("The maximum allowed length of a string value when parsing a JSON document or attribute.")
            .required(true)
            .defaultValue(DEFAULT_MAX_STRING_LENGTH)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("el-rest-client-service")
            .displayName("Client Service")
            .description("An Elasticsearch client service to use for running queries.")
            .identifiesControllerService(ElasticSearchClientService.class)
            .required(true)
            .build();

    PropertyDescriptor LOG_ERROR_RESPONSES = new PropertyDescriptor.Builder()
            .name("put-es-record-log-error-responses")
            .displayName("Log Error Responses")
            .description("If this is enabled, errors will be logged to the NiFi logs at the error log level. Otherwise, they will " +
                    "only be logged if debug logging is enabled on NiFi as a whole. The purpose of this option is to give the user " +
                    "the ability to debug failed operations without having to turn on debug logging.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All flowfiles that fail for reasons unrelated to server availability go to this relationship.")
            .build();
    Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("All flowfiles that fail due to server/cluster availability go to this relationship.")
            .build();

    String DEFAULT_QUERY_JSON = "{}";

    default ObjectMapper buildObjectMapper(final ProcessContext context) {
        final int maxStringLength = context.getProperty(MAX_JSON_FIELD_STRING_LENGTH).asDataSize(DataUnit.B).intValue();

        final StreamReadConstraints streamReadConstraints = StreamReadConstraints.builder().maxStringLength(maxStringLength).build();
        final JsonFactory jsonFactory = JsonFactory.builder()
                .streamReadConstraints(streamReadConstraints)
                .build();

        return new ObjectMapper(jsonFactory);
    }

    default String getQuery(final FlowFile input, final ProcessContext context, final ProcessSession session, final ObjectMapper mapper) throws IOException {
        String retVal = getQuery(input != null ? input.getAttributes() : Collections.emptyMap(), context, mapper);
        if (DEFAULT_QUERY_JSON.equals(retVal) && input != null
            && QueryDefinitionType.FULL_QUERY == context.getProperty(QUERY_DEFINITION_STYLE).asAllowableValue(QueryDefinitionType.class)
            && !context.getProperty(QUERY).isSet()) {
            try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                session.exportTo(input, out);
                retVal = out.toString();
            }
        }

        return StringUtils.isNotBlank(retVal) ? retVal : DEFAULT_QUERY_JSON;
    }

    default String getQuery(final Map<String, String> attributes, final ProcessContext context, final ObjectMapper mapper) throws IOException {
        final String retVal;
        if (QueryDefinitionType.FULL_QUERY == context.getProperty(QUERY_DEFINITION_STYLE).asAllowableValue(QueryDefinitionType.class)) {
            if (context.getProperty(QUERY).isSet()) {
                retVal = context.getProperty(QUERY).evaluateAttributeExpressions(attributes).getValue();
            } else {
                retVal = null;
            }
        } else {
            final Map<String, Object> query = new HashMap<>(7, 1);
            addQueryClause(query, attributes, context, mapper);
            if (context.getProperty(SIZE).isSet()) {
                query.put("size", context.getProperty(SIZE).evaluateAttributeExpressions(attributes).asInteger());
            }
            addSortClause(query, attributes, context, mapper);
            if (context.getProperty(AGGREGATIONS).isSet()) {
                query.put("aggs", mapper.readTree(context.getProperty(AGGREGATIONS).evaluateAttributeExpressions(attributes).getValue()));
            }
            if (context.getProperty(SCRIPT).isSet()) {
                query.put("script", mapper.readTree(context.getProperty(SCRIPT).evaluateAttributeExpressions(attributes).getValue()));
            }
            if (context.getProperty(FIELDS).isSet()) {
                query.put("fields", mapper.readTree(context.getProperty(FIELDS).evaluateAttributeExpressions(attributes).getValue()));
            }
            if (context.getProperty(SCRIPT_FIELDS).isSet()) {
                query.put("script_fields", mapper.readTree(context.getProperty(SCRIPT_FIELDS).evaluateAttributeExpressions(attributes).getValue()));
            }
            retVal = mapper.writeValueAsString(query);
        }

        // allow for no query to be specified, which will run a "match_all" query in Elasticsearch by default
        return StringUtils.isNotBlank(retVal) ? retVal : DEFAULT_QUERY_JSON;
    }

    /**
     * Add "query" clause to the Elasticsearch query object.
     * Overridable method for processors that build a query clause from separate components, e.g. ConsumeElasticsearch
     *
     * @param query      the Query object being constructed
     * @param attributes (optional) input FlowFile attributes
     * @param context    ProcessContext of the running processor
     */
    default void addQueryClause(final Map<String, Object> query, final Map<String, String> attributes, final ProcessContext context, final ObjectMapper mapper) throws IOException {
        if (context.getProperty(QUERY_CLAUSE).isSet()) {
            query.put("query", mapper.readTree(context.getProperty(QUERY_CLAUSE).evaluateAttributeExpressions(attributes).getValue()));
        }
    }

    /**
     * Add "sort" clause to the Elasticsearch query object.
     * Overridable method for processors that build a sort clause from separate components, e.g. ConsumeElasticsearch
     *
     * @param query   the Query object being constructed
     * @param attributes (optional) input FlowFile attributes
     * @param context ProcessContext of the running processor
     */
    default void addSortClause(final Map<String, Object> query, final Map<String, String> attributes, final ProcessContext context, final ObjectMapper mapper) throws IOException {
        if (context.getProperty(SORT).isSet()) {
            // ensure sort is specified as a List for easier manipulation if needed later
            final List<Map<String, Object>> sortList;
            final JsonNode sort = mapper.readTree(context.getProperty(SORT).evaluateAttributeExpressions(attributes).getValue());
            if (sort.isArray()) {
                sortList = mapper.convertValue(sort, new TypeReference<>() {
                });
            } else {
                sortList = Collections.singletonList(mapper.convertValue(sort, new TypeReference<>() {
                }));
            }
            query.put("sort", new ArrayList<>(sortList));
        }
    }

    default Map<String, String> getRequestParametersFromDynamicProperties(final ProcessContext context, final FlowFile flowFile) {
        return getRequestParametersFromDynamicProperties(context, flowFile != null ? flowFile.getAttributes() : null);
    }

    default Map<String, String> getRequestParametersFromDynamicProperties(final ProcessContext context, final Map<String, String> attributes) {
        return getDynamicProperties(context, attributes, Predicate.not(REQUEST_HEADER_FILTER));
    }

    default Map<String, String> getRequestHeadersFromDynamicProperties(final ProcessContext context, final FlowFile flowFile) {
        return getRequestHeadersFromDynamicProperties(context, flowFile != null ? flowFile.getAttributes() : null);
    }

    default Map<String, String> getRequestHeadersFromDynamicProperties(final ProcessContext context, final Map<String, String> attributes) {
        final Map<String, String> dynamicProperties = getDynamicProperties(context, attributes, REQUEST_HEADER_FILTER);
        return dynamicProperties.entrySet().stream().collect(Collectors.toMap(
                e -> e.getKey().replaceFirst("^" + DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER, ""),
                Map.Entry::getValue));
    }

    default Map<String, String> getDynamicProperties(final ProcessContext context, final Map<String, String> attributes, final Predicate<Map.Entry<PropertyDescriptor, String>> predicate) {
        return context.getProperties().entrySet().stream()
                // filter non-blank dynamic properties
                .filter(e -> e.getKey().isDynamic()
                        && StringUtils.isNotBlank(e.getValue())
                        && StringUtils.isNotBlank(context.getProperty(e.getKey()).evaluateAttributeExpressions(attributes).getValue())
                )
                .filter(predicate)
                // convert to Map keys and evaluated property values
                .collect(Collectors.toMap(
                        e -> e.getKey().getName(),
                        e -> context.getProperty(e.getKey()).evaluateAttributeExpressions(attributes).getValue()
                ));
    }

    @Override
    default List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        final ConfigVerificationResult.Builder indexExistsResult = new ConfigVerificationResult.Builder()
                .verificationStepName(VERIFICATION_STEP_INDEX_EXISTS);

        ElasticSearchClientService verifyClientService = null;
        String index = null;
        boolean indexExists = false;
        if (context.getProperty(CLIENT_SERVICE).isSet()) {
            verifyClientService = context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class);
            if (context.getProperty(INDEX).isSet()) {
                index = context.getProperty(INDEX).evaluateAttributeExpressions(attributes).getValue();
                try {
                    if (verifyClientService.exists(index, new ElasticsearchRequestOptions(getRequestParametersFromDynamicProperties(context, attributes),
                            getRequestHeadersFromDynamicProperties(context, attributes)))) {
                        indexExistsResult.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                                .explanation(String.format("Index [%s] exists", index));
                        indexExists = true;
                    } else {
                        if (isIndexNotExistSuccessful()) {
                            indexExistsResult.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL);
                        } else {
                            indexExistsResult.outcome(ConfigVerificationResult.Outcome.FAILED);
                        }
                        indexExistsResult.explanation(String.format("Index [%s] does not exist", index));
                    }
                } catch (final Exception ex) {
                    verificationLogger.error("Error checking whether index [{}] exists", index, ex);
                    indexExistsResult.outcome(ConfigVerificationResult.Outcome.FAILED)
                            .explanation(String.format("Failed to check whether index [%s] exists", index));
                }
            } else {
                indexExistsResult.outcome(ConfigVerificationResult.Outcome.SKIPPED)
                        .explanation(String.format("No [%s] specified for existence check", INDEX.getDisplayName()));
            }
        } else {
            indexExistsResult.outcome(ConfigVerificationResult.Outcome.SKIPPED)
                    .explanation(CLIENT_SERVICE.getDisplayName() + " not configured, cannot check index existence");
        }
        results.add(indexExistsResult.build());

        final ObjectMapper mapper = buildObjectMapper(context);
        results.addAll(verifyAfterIndex(context, verificationLogger, attributes, verifyClientService, index, indexExists, mapper));

        return results;
    }

    boolean isIndexNotExistSuccessful();

    default List<ConfigVerificationResult> verifyAfterIndex(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes,
                                                            final ElasticSearchClientService verifyClientService, final String index, final boolean indexExists, final ObjectMapper mapper) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        final ConfigVerificationResult.Builder queryJsonValidResult = new ConfigVerificationResult.Builder()
                .verificationStepName(VERIFICATION_STEP_QUERY_JSON_VALID);
        final ConfigVerificationResult.Builder queryValidResult = new ConfigVerificationResult.Builder()
                .verificationStepName(VERIFICATION_STEP_QUERY_VALID);

        if (indexExists) {
            try {
                final String query = getQuery(attributes, context, mapper);
                verificationLogger.debug("Query JSON: {}", query);
                final ObjectNode queryJson = mapper.readValue(query, ObjectNode.class);
                queryJsonValidResult.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL).explanation("Query JSON successfully parsed");

                if (queryJson.has("script")) {
                    verificationLogger.debug("Removing \"script\" field from verification Query, not valid for _search");
                    queryJson.remove("script");
                }
                final String type = context.getProperty(TYPE).evaluateAttributeExpressions(attributes).getValue();
                final Map<String, String> requestParameters = new HashMap<>(getRequestParametersFromDynamicProperties(context, attributes));
                requestParameters.putIfAbsent("_source", "false");

                final SearchResponse response = verifyClientService.search(
                        mapper.writeValueAsString(queryJson), index, type, new ElasticsearchRequestOptions(requestParameters, getRequestHeadersFromDynamicProperties(context, attributes)));
                queryValidResult.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                        .explanation(String.format("Query found %d hits and %d aggregations in %d milliseconds, timed out: %s",
                                response.getNumberOfHits(), response.getAggregations() == null ? 0 : response.getAggregations().size(), response.getTook(), response.isTimedOut()));
            } catch (final IOException ioe) {
                verificationLogger.warn("Unable to parse Query as JSON", ioe);
                queryJsonValidResult.outcome(ConfigVerificationResult.Outcome.FAILED)
                        .explanation(String.format("Query cannot be parsed as valid JSON: %s", ioe.getMessage()));
                queryValidResult.outcome(ConfigVerificationResult.Outcome.SKIPPED).explanation("Query JSON could not be parsed");
            } catch (final ElasticsearchException ee) {
                verificationLogger.warn("Query failed in Elasticsearch", ee);
                queryValidResult.outcome(ConfigVerificationResult.Outcome.FAILED)
                        .explanation(String.format("Query failed in Elasticsearch: %s", ee.getMessage()));
            }
        } else {
            final String skippedReason = String.format("Index %s does not exist", index);
            queryJsonValidResult.outcome(ConfigVerificationResult.Outcome.SKIPPED).explanation(skippedReason);
            queryValidResult.outcome(ConfigVerificationResult.Outcome.SKIPPED).explanation(skippedReason);
        }
        results.add(queryJsonValidResult.build());
        results.add(queryValidResult.build());

        return results;
    }
}
