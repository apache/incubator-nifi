/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processors.elasticsearch.api.AggregationResultsFormat;
import org.apache.nifi.processors.elasticsearch.api.JsonQueryParameters;
import org.apache.nifi.processors.elasticsearch.api.QueryDefinitionType;
import org.apache.nifi.processors.elasticsearch.api.ResultOutputStrategy;
import org.apache.nifi.processors.elasticsearch.api.SearchResultsFormat;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractJsonQueryElasticsearchTest<P extends AbstractJsonQueryElasticsearch<? extends JsonQueryParameters>> {
    private static final String TEST_DIR = "src/test/resources/AbstractJsonQueryElasticsearchTest";
    private static final String TEST_COMMON_DIR = "src/test/resources/common";
    private static final String INDEX_NAME = "messages";
    static final String RANGE_FIELD_NAME = "msg";
    static final String RANGE_SORT_ORDER = "asc";
    static final String RANGE_FIELD_VALUE = "123456";

    // bool query with range filter as constructed by ConsumeElasticsearch
    private static final String QUERY_CLAUSE = String.format("{\"bool\": {\"filter\": [{\"range\": {\"%s\": {\"gt\": \"123456\"}}}]}}", RANGE_FIELD_NAME);
    static final String CONSUME_ELASTICSEARCH_SORT_CLAUSE = String.format("{\"%s\":\"%s\"}", RANGE_FIELD_NAME, RANGE_SORT_ORDER);

    protected static String matchAllQuery;
    protected static String matchAllAggregationWithDefaultTermsQuery;

    static final ObjectMapper TEST_MAPPER = new ObjectMapper();

    abstract P getProcessor();

    abstract Scope getStateScope();

    abstract boolean isInput();

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        matchAllQuery = JsonUtils.readString(Paths.get(TEST_COMMON_DIR, "matchAllQuery.json"));
        matchAllAggregationWithDefaultTermsQuery = JsonUtils.readString(Paths.get(TEST_DIR, "matchAllAggregationWithDefaultTermsQuery.json"));
    }

    @Test
    void testMandatoryProperties() {
        final TestRunner runner = createRunner(false);
        runner.removeProperty(ElasticsearchRestProcessor.CLIENT_SERVICE);
        runner.removeProperty(ElasticsearchRestProcessor.INDEX);
        runner.removeProperty(ElasticsearchRestProcessor.TYPE);
        runner.removeProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE);
        runner.removeProperty(ElasticsearchRestProcessor.QUERY);
        runner.removeProperty(ElasticsearchRestProcessor.QUERY_ATTRIBUTE);
        runner.removeProperty(ElasticsearchRestProcessor.MAX_JSON_FIELD_STRING_LENGTH);
        runner.removeProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_SPLIT);
        runner.removeProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT);
        runner.removeProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS);

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        final String expected = String.format("""
                        Processor has 2 validation failures:
                        '%s' is invalid because %s is required
                        '%s' is invalid because %s is required
                        """,
                ElasticsearchRestProcessor.INDEX.getDisplayName(), ElasticsearchRestProcessor.INDEX.getDisplayName(),
                ElasticsearchRestProcessor.CLIENT_SERVICE.getDisplayName(), ElasticsearchRestProcessor.CLIENT_SERVICE.getDisplayName());
        assertEquals(expected, assertionError.getMessage());
    }

    @Test
    void testInvalidProperties() {
        final TestRunner runner = createRunner(false);
        runner.setProperty(ElasticsearchRestProcessor.CLIENT_SERVICE, "not-a-service");
        runner.setProperty(ElasticsearchRestProcessor.INDEX, "");
        runner.setProperty(ElasticsearchRestProcessor.TYPE, "");
        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, "not-valid");
        runner.setProperty(ElasticsearchRestProcessor.MAX_JSON_FIELD_STRING_LENGTH, "not-a-size");
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_SPLIT, "not-enum");
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, "not-enum2");
        runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, "not-boolean");

        final String nonPaginatedResultOutputStrategies =
                ResultOutputStrategy.getNonPaginatedResponseOutputStrategies()
                        .stream().map(ResultOutputStrategy::getValue)
                        .collect(Collectors.joining(", "));
        final String expectedAllowedSplitHits = getProcessor() instanceof AbstractPaginatedJsonQueryElasticsearch ? Arrays.stream(ResultOutputStrategy.values())
                .map(ResultOutputStrategy::getValue)
                .collect(Collectors.joining(", ")) : nonPaginatedResultOutputStrategies;

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        String expected;
        if (runner.getProcessor() instanceof ConsumeElasticsearch) {
            expected = "Processor has 8 validation failures:\n";
        } else {
            expected = String.format("""
                            Processor has 9 validation failures:
                            '%s' validated against 'not-valid' is invalid because Given value not found in allowed set '%s'
                            """,
                    ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE.getName(),
                    Arrays.stream(QueryDefinitionType.values()).map(QueryDefinitionType::getValue).collect(Collectors.joining(", ")));
        }
        expected += String.format(
                """
                        '%s' validated against '' is invalid because %s cannot be empty
                        '%s' validated against '' is invalid because %s cannot be empty
                        '%s' validated against 'not-a-size' is invalid because Must be of format <Data Size> <Data Unit> where <Data Size> is a non-negative integer \
                        and <Data Unit> is a supported Data Unit, such as: B, KB, MB, GB, TB
                        '%s' validated against 'not-a-service' is invalid because Property references a Controller Service that does not exist
                        '%s' validated against 'not-enum2' is invalid because Given value not found in allowed set '%s'
                        '%s' validated against 'not-enum' is invalid because Given value not found in allowed set '%s'
                        '%s' validated against 'not-boolean' is invalid because Given value not found in allowed set 'true, false'
                        '%s' validated against 'not-a-service' is invalid because Invalid Controller Service: not-a-service is not a valid Controller Service Identifier
                        """,
                ElasticsearchRestProcessor.INDEX.getName(), ElasticsearchRestProcessor.INDEX.getName(),
                ElasticsearchRestProcessor.TYPE.getName(), ElasticsearchRestProcessor.TYPE.getName(),
                ElasticsearchRestProcessor.MAX_JSON_FIELD_STRING_LENGTH.getName(),
                ElasticsearchRestProcessor.CLIENT_SERVICE.getDisplayName(),
                AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT.getName(), expectedAllowedSplitHits,
                AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_SPLIT.getName(), nonPaginatedResultOutputStrategies,
                AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS.getName(), ElasticsearchRestProcessor.CLIENT_SERVICE.getDisplayName());
        assertEquals(expected, assertionError.getMessage());
    }

    @Test
    void testInvalidQueryProperty() {
        final TestRunner runner = createRunner(false);
        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.FULL_QUERY.getValue());
        final PropertyDescriptor queryPropertyDescriptor;
        if (runner.getProcessor() instanceof SearchElasticsearch) {
            queryPropertyDescriptor = SearchElasticsearch.QUERY;
        } else {
            queryPropertyDescriptor = ElasticsearchRestProcessor.QUERY;
        }
        runner.setProperty(queryPropertyDescriptor, "not-json");

        if (runner.getProcessor() instanceof ConsumeElasticsearch) {
            // ConsumeElasticsearch does not use the QUERY property
            runner.assertValid();
        } else {
            final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
            assertTrue(assertionError.getMessage().contains("not-json"));
        }
    }

    @Test
    void testInvalidQueryBuilderProperties() {
        final TestRunner runner = createRunner(false);
        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY);
        runner.setProperty(ElasticsearchRestProcessor.QUERY_CLAUSE, "not-json");
        runner.setProperty(ElasticsearchRestProcessor.SIZE, "-1");
        runner.setProperty(ElasticsearchRestProcessor.AGGREGATIONS, "not-json-aggs");
        runner.setProperty(ElasticsearchRestProcessor.SORT, "not-json-sort");
        runner.setProperty(ElasticsearchRestProcessor.FIELDS, "not-json-fields");
        runner.setProperty(ElasticsearchRestProcessor.SCRIPT_FIELDS, "not-json-script_fields");

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertTrue(assertionError.getMessage().contains("not-json"));
    }

    @Test
    void testBasicQuery() {
        // test hits (no splitting) - full hit format
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, SearchResultsFormat.FULL);
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
        final MockFlowFile hits = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst();
        hits.assertAttributeEquals("hit.count", "10");
        assertOutputContent(hits.getContent(), 10, false);
        final List<Map<String, Object>> result = JsonUtils.readListOfMaps(hits.getContent());
        result.forEach(hit -> {
            assertFalse(hit.isEmpty());
            assertTrue(hit.containsKey("_source"));
            assertTrue(hit.containsKey("_index"));
        });
        assertEquals(1L, runner.getProvenanceEvents().stream()
                .filter(pe -> ProvenanceEventType.RECEIVE.equals(pe.getEventType()) && pe.getAttribute("uuid").equals(hits.getAttribute("uuid")))
                .count());
        reset(runner);

        // test splitting hits - _source only format
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, ResultOutputStrategy.PER_HIT);
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, SearchResultsFormat.SOURCE_ONLY);
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 10, 0, 0);

        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(hit -> {
            hit.assertAttributeEquals("hit.count", "1");
            assertOutputContent(hit.getContent(), 1, false);
            final Map<String, Object> h = JsonUtils.readMap(hit.getContent());
            assertFalse(h.isEmpty());
            assertFalse(h.containsKey("_source"));
            assertFalse(h.containsKey("_index"));
            // should be the _source content only
            assertTrue(h.containsKey("msg"));

            assertEquals(1L,
                    runner.getProvenanceEvents().stream()
                            .filter(pe ->
                                    pe.getEventType() == ProvenanceEventType.RECEIVE && pe.getAttribute("uuid").equals(hit.getAttribute("uuid")))
                            .count());
        });
        reset(runner);

        // test splitting hits - metadata only format
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, ResultOutputStrategy.PER_HIT);
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, SearchResultsFormat.METADATA_ONLY);
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 10, 0, 0);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(hit -> {
            hit.assertAttributeEquals("hit.count", "1");
            assertOutputContent(hit.getContent(), 1, false);
            final Map<String, Object> h = JsonUtils.readMap(hit.getContent());
            assertFalse(h.isEmpty());
            assertFalse(h.containsKey("_source"));
            assertTrue(h.containsKey("_index"));
            assertEquals(1L,
                    runner.getProvenanceEvents().stream()
                            .filter(pe ->
                                    ProvenanceEventType.RECEIVE.equals(pe.getEventType()) && pe.getAttribute("uuid").equals(hit.getAttribute("uuid")))
                            .count());
        });
    }

    @Test
    void testNoHits() {
        // test no hits (no output)
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = getService(runner);
        service.setMaxPages(0);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);
        runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, "false");
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 0, 0, 0);
        assertEquals(0L, runner.getProvenanceEvents().stream()
                .filter(pe -> ProvenanceEventType.RECEIVE.equals(pe.getEventType()))
                .count());
        reset(runner);

        // test not hits (with output)
        runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, "true");
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);

        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(
                hit -> {
                    hit.assertAttributeEquals("hit.count", "0");
                    assertOutputContent(hit.getContent(), 0, false);
                    assertEquals(1L, runner.getProvenanceEvents().stream()
                            .filter(pe ->
                                    ProvenanceEventType.RECEIVE.equals(pe.getEventType()) && pe.getAttribute("uuid").equals(hit.getAttribute("uuid")))
                            .count());
                });
    }

    @SuppressWarnings("unchecked")
    @Test
    void testAggregationsFullFormat() {
        final TestRunner runner = createRunner(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllAggregationWithDefaultTermsQuery);
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT, AggregationResultsFormat.FULL);
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 1);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "10");
        final MockFlowFile aggregations = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS).getFirst();
        aggregations.assertAttributeNotExists("aggregation.number");
        aggregations.assertAttributeNotExists("aggregation.name");
        // count == 1 because aggregations is a single Map rather than a List of Maps, even when there are multiple aggs
        assertOutputContent(aggregations.getContent(), 1, false);
        final Map<String, Object> agg = JsonUtils.readMap(aggregations.getContent());
        // agg Map of 2 Maps (buckets and metadata)
        assertEquals(2, agg.size());
        agg.keySet().forEach(aggName -> {
            final Map<String, Object> termAgg = (Map<String, Object>) agg.get(aggName);
            assertInstanceOf(List.class, termAgg.get("buckets"));
            assertTrue(termAgg.containsKey("doc_count_error_upper_bound"));
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    void testWithQueryParameterNoIncomingConnectionAndBucketsAggregationFormat() {
        final TestRunner runner = createRunner(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllAggregationWithDefaultTermsQuery);
        runner.setIncomingConnection(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT, AggregationResultsFormat.BUCKETS_ONLY);
        runner.run(1, true, true);
        testCounts(runner, 0, 1, 0, 1);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "10");
        final MockFlowFile singleAgg = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS).getFirst();
        singleAgg.assertAttributeNotExists("aggregation.number");
        singleAgg.assertAttributeNotExists("aggregation.name");
        final Map<String, Object> agg2 = JsonUtils.readMap(singleAgg.getContent());
        // agg Map of 2 Lists (bucket contents only)
        assertEquals(2, agg2.size());
        agg2.keySet().forEach(aggName -> {
            final List<Map<String, Object>> termAgg = (List<Map<String, Object>>) agg2.get(aggName);
            assertEquals(5, termAgg.size());
            termAgg.forEach(a -> {
                assertTrue(a.containsKey("key"));
                assertTrue(a.containsKey("doc_count"));
            });
        });
    }

    @Test
    void testSplittingAggregationsMetadataOnlyFormat() {
        final TestRunner runner = createRunner(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllAggregationWithDefaultTermsQuery);
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_SPLIT, ResultOutputStrategy.PER_HIT);
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT, AggregationResultsFormat.METADATA_ONLY);
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 2);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "10");
        int a = 0;
        for (final MockFlowFile termAgg : runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS)) {
            termAgg.assertAttributeEquals("aggregation.name", a == 0 ? "term_agg" : "term_agg2");
            termAgg.assertAttributeEquals("aggregation.number", Integer.toString(++a));
            assertOutputContent(termAgg.getContent(), 1, false);

            final Map<String, Object> aggContent = JsonUtils.readMap(termAgg.getContent());
            // agg Map (metadata, no buckets)
            assertTrue(aggContent.containsKey("doc_count_error_upper_bound"));
            assertFalse(aggContent.containsKey("buckets"));
        }
    }
    @Test
    void testAggregationsUsingExpressionLanguage() throws Exception {
        final TestRunner runner = createRunner(true);
        final String query = JsonUtils.readString(Paths.get(TEST_DIR, "matchAllAggregationWithDefaultTermsInExpressionLanguageQuery.json"));
        runner.setEnvironmentVariableValue("fieldValue", "msg");
        runner.setEnvironmentVariableValue("es.index", INDEX_NAME);
        runner.setEnvironmentVariableValue("es.type", "msg");
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractJsonQueryElasticsearch.INDEX, "${es.index}");
        runner.setProperty(AbstractJsonQueryElasticsearch.TYPE, "${es.type}");
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_SPLIT, ResultOutputStrategy.PER_HIT);
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT, AggregationResultsFormat.FULL);
        runOnce(runner);
        testCounts(runner, isInput() ? 1 : 0, 1, 0, 2);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "10");
        int a = 0;
        for (final MockFlowFile termAgg : runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS)) {
            termAgg.assertAttributeEquals("aggregation.name", a == 0 ? "term_agg" : "term_agg2");
            termAgg.assertAttributeEquals("aggregation.number", Integer.toString(++a));
            assertOutputContent(termAgg.getContent(), 1, false);
        }
    }

    @Test
    void testErrorDuringSearch() {
        final TestRunner runner = createRunner(true);
        getService(runner).setThrowErrorInSearch(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllAggregationWithDefaultTermsQuery);
        runOnce(runner);
        testCounts(runner, 0, 0, isInput() ? 1 : 0, 0);
    }

    @ParameterizedTest
    @EnumSource(QueryDefinitionType.class)
    void testQueryAttribute(final QueryDefinitionType queryDefinitionType) throws JsonProcessingException {
        Assumptions.assumeFalse(QueryDefinitionType.FULL_QUERY.equals(queryDefinitionType) && getProcessor() instanceof ConsumeElasticsearch,
                "ConsumeElasticsearch doesn't use the FULL_QUERY definition type");

        final String query = matchAllAggregationWithDefaultTermsQuery;
        final String queryAttr = "es.query";

        final TestRunner runner = createRunner(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr);
        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, queryDefinitionType);
        setQuery(runner, query);

        runOnce(runner);

        testCounts(runner, isInput() ? 1 : 0, 1, 0, 1);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS);
        flowFiles.addAll(runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS));

        for (final MockFlowFile mockFlowFile : flowFiles) {
            final String attr = mockFlowFile.getAttribute(queryAttr);
            assertNotNull(attr, "Missing query attribute");

            final ObjectNode expected = TEST_MAPPER.readValue(query, ObjectNode.class);
            if (runner.getProcessor() instanceof ConsumeElasticsearch) {
                // no "query" will be defined with Range Field but no initial value provided
                expected.remove("query");
                addExpectedSort(expected);
            }
            assertEquals(expected, TEST_MAPPER.readTree(attr), "Query had wrong value.");
        }
    }

    @Test
    void testInputHandling() {
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);

        runner.setIncomingConnection(true);
        runner.run();
        testCounts(runner, 0, 0, 0, 0);
        reset(runner);

        runner.setIncomingConnection(false);
        runner.run();
        testCounts(runner, 0, 1, 0, 0);
    }

    @Test
    void testRequestParameters() {
        final TestRunner runner = createRunner(false);
        if (!(runner.getProcessor() instanceof ConsumeElasticsearch)) {
            runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);
        }
        runner.setProperty("refresh", "true");
        runner.setProperty("slices", "${slices}");
        runner.setProperty(ElasticsearchRestProcessor.DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER + "Accept", "${accept}");
        runner.setEnvironmentVariableValue("slices", "auto");
        runner.setEnvironmentVariableValue("accept", "application/json");

        runOnce(runner);

        final TestElasticsearchClientService service = getService(runner);
        if (runner.getProcessor() instanceof AbstractPaginatedJsonQueryElasticsearch) {
            assertEquals(3, service.getElasticsearchRequestOptions().getRequestParameters().size());
            assertEquals("600s", service.getElasticsearchRequestOptions().getRequestParameters().get("scroll"));
        } else {
            assertEquals(2, service.getElasticsearchRequestOptions().getRequestParameters().size());
        }

        assertEquals("true", service.getElasticsearchRequestOptions().getRequestParameters().get("refresh"));
        assertEquals("auto", service.getElasticsearchRequestOptions().getRequestParameters().get("slices"));

        assertEquals(1, service.getElasticsearchRequestOptions().getRequestHeaders().size());
        assertEquals("application/json", service.getElasticsearchRequestOptions().getRequestHeaders().get("Accept"));
    }

    @ParameterizedTest
    @MethodSource
    void testQueryBuilder(final String queryClause, final Integer size, final String aggs, final String sort,
                          final String fields, final String scriptFields, final String expectedQuery) throws Exception {
        final TestRunner runner = createRunner(false);
        if (runner.getProcessor() instanceof ConsumeElasticsearch) {
            // test Range Field and initial value defined
            runner.setProperty(ConsumeElasticsearch.RANGE_FIELD, RANGE_FIELD_NAME);
            runner.setProperty(ConsumeElasticsearch.RANGE_INITIAL_VALUE, RANGE_FIELD_VALUE);

            // as onScheduled won't run (the processor will not actually br triggered), set these fields directly
            ((ConsumeElasticsearch) runner.getProcessor()).trackingRangeField = RANGE_FIELD_NAME;
            ((ConsumeElasticsearch) runner.getProcessor()).trackingSortOrder = RANGE_SORT_ORDER;
        } else {
            runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY);
        }

        if (queryClause != null) {
            runner.setProperty(ElasticsearchRestProcessor.QUERY_CLAUSE, queryClause);
        }
        if (size != null) {
            runner.setProperty(ElasticsearchRestProcessor.SIZE, String.valueOf(size));
        }
        if (aggs != null) {
            runner.setProperty(ElasticsearchRestProcessor.AGGREGATIONS, aggs);
        }
        if (sort != null) {
            runner.setProperty(ElasticsearchRestProcessor.SORT, sort);
        }
        if (fields != null) {
            runner.setProperty(ElasticsearchRestProcessor.FIELDS, fields);
        }
        if (scriptFields != null) {
            runner.setProperty(ElasticsearchRestProcessor.SCRIPT_FIELDS, scriptFields);
        }

        @SuppressWarnings("unchecked")
        final String query = ((P) runner.getProcessor()).getQuery(null, runner.getProcessContext(), null, TEST_MAPPER);
        assertNotNull(query);

        final ObjectNode expected = TEST_MAPPER.readValue(expectedQuery, ObjectNode.class);
        if (runner.getProcessor() instanceof ConsumeElasticsearch) {
            // ConsumeElasticsearch should build the "query" based upon the Range Field and initial value
            expected.set("query", TEST_MAPPER.readTree(QUERY_CLAUSE));
            addExpectedSort(expected);
        }
        assertEquals(expected, TEST_MAPPER.readTree(query));
    }

    private void addExpectedSort(final ObjectNode expected) throws JsonProcessingException {
        // ConsumeElasticsearch should add the "sort" for the Range field
        final ArrayNode expectedSort;
        if (expected.has("sort")) {
            expectedSort = expected.withArray("sort");
        } else {
            expectedSort = TEST_MAPPER.getNodeFactory().arrayNode(1);
            expected.set("sort", expectedSort);
        }
        expectedSort.insert(0, TEST_MAPPER.readTree(CONSUME_ELASTICSEARCH_SORT_CLAUSE));
    }

    private static Stream<Arguments> testQueryBuilder() {
        final int size = 123;
        final String aggs = "{\"foo_terms\": {\"terms\": {\"field\": \"foo.keyword\"}}}";
        final String sort = "[{\"price\" : {\"order\" : \"asc\", \"mode\" : \"avg\"}}, {\"post_date\" : {\"format\": \"strict_date_optional_time_nanos\"}}]";
        final String fields = "[\"user.id\", \"http.response.*\", {\"field\": \"@timestamp\", \"format\": \"epoch_millis\"}]";
        final String scriptFields = "{\"test1\": {\"script\": {\"lang\": \"painless\", \"source\": \"doc['price'].value * 2\"}}}";

        return Stream.of(
                Arguments.of(null, null, null, null, null, null, "{}"),
                Arguments.of(QUERY_CLAUSE, null, null, null, null, null, String.format("{\"query\": %s}", QUERY_CLAUSE)),
                Arguments.of(null, size, null, null, null, null, String.format("{\"size\": %d}", size)),
                Arguments.of(null, null, aggs, null, null, null, String.format("{\"aggs\": %s}", aggs)),
                Arguments.of(null, null, null, sort, null, null, String.format("{\"sort\": %s}", sort)),
                Arguments.of(null, null, null, null, fields, null, String.format("{\"fields\": %s}", fields)),
                Arguments.of(null, null, null, null, null, scriptFields, String.format("{\"script_fields\": %s}", scriptFields)),
                Arguments.of(QUERY_CLAUSE, size, null, null, null, null, String.format("{\"query\": %s, \"size\": %d}", QUERY_CLAUSE, size)),
                Arguments.of(QUERY_CLAUSE, size, aggs, sort, fields, scriptFields,
                        String.format("{\"query\": %s, \"size\": %d, \"aggs\": %s, \"sort\": %s, \"fields\": %s, \"script_fields\": %s}",
                                QUERY_CLAUSE, size, aggs, sort, fields, scriptFields)
                )
        );
    }

    @Test
    void testDefaultQuery() {
        final TestRunner runner = createRunner(false);

        runner.removeProperty(ElasticsearchRestProcessor.QUERY);
        runner.removeProperty(ElasticsearchRestProcessor.QUERY_CLAUSE);

        final String expected;
        if (runner.getProcessor() instanceof ConsumeElasticsearch) {
            // test Range Field defined but no initial value
            runner.setProperty(ConsumeElasticsearch.QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY);
            runner.setProperty(ConsumeElasticsearch.RANGE_FIELD, RANGE_FIELD_NAME);

            // should be no "query" (with no initial value) but "sort" added
            expected = String.format("{\"sort\":[%s]}", CONSUME_ELASTICSEARCH_SORT_CLAUSE);
        } else {
            expected = "{}";
        }

        runOnce(runner, "");
        assertEquals(expected, getService(runner).getQuery());
    }

    void setQuery(final TestRunner runner, final String query) throws JsonProcessingException {
        if (runner.getProcessor() instanceof ConsumeElasticsearch) {
            runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY);
        }

        if (QueryDefinitionType.BUILD_QUERY.getValue().equals(runner.getProcessContext().getProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE).getValue())) {
            final Map<String, Object> queryMap = TEST_MAPPER.readValue(query, new TypeReference<>() {
            });
            if (queryMap.containsKey("query")) {
                if (runner.getProcessor() instanceof ConsumeElasticsearch) {
                    runner.setProperty(ConsumeElasticsearch.RANGE_FIELD, RANGE_FIELD_NAME);
                } else {
                    runner.setProperty(ElasticsearchRestProcessor.QUERY_CLAUSE, TEST_MAPPER.writeValueAsString(queryMap.get("query")));
                }
            }
            if (queryMap.containsKey("size")) {
                runner.setProperty(ElasticsearchRestProcessor.SIZE, TEST_MAPPER.writeValueAsString(queryMap.get("size")));
            }
            if (queryMap.containsKey("aggs")) {
                runner.setProperty(ElasticsearchRestProcessor.AGGREGATIONS, TEST_MAPPER.writeValueAsString(queryMap.get("aggs")));
            }
            if (queryMap.containsKey("sort")) {
                runner.setProperty(ElasticsearchRestProcessor.SORT, TEST_MAPPER.writeValueAsString(queryMap.get("sort")));
            }
        } else {
            runner.setProperty(ElasticsearchRestProcessor.QUERY, query);
        }
    }

    static void testCounts(final TestRunner runner, final int original, final int hits, final int failure, final int aggregations) {
        runner.assertTransferCount(AbstractJsonQueryElasticsearch.REL_ORIGINAL, original);
        runner.assertTransferCount(AbstractJsonQueryElasticsearch.REL_HITS, hits);
        runner.assertTransferCount(AbstractJsonQueryElasticsearch.REL_FAILURE, failure);
        runner.assertTransferCount(AbstractJsonQueryElasticsearch.REL_AGGREGATIONS, aggregations);
        runner.assertTransferCount(AbstractJsonQueryElasticsearch.REL_RETRY, 0);
    }

    static void assertOutputContent(final String content, final int count, final boolean ndjson) {
        if (ndjson) {
            assertEquals(count, (content.split("\n").length));
        } else {
            if (count == 0) {
                assertEquals("", content);
            } else if (count == 1) {
                assertTrue(content.startsWith("{") && content.endsWith("}"));
            } else {
                assertTrue(content.startsWith("[") && content.endsWith("]"));
            }
        }
    }

    TestRunner createRunner(final boolean returnAggs) {
        final P processor = getProcessor();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final TestElasticsearchClientService service = new TestElasticsearchClientService(returnAggs);
        try {
            runner.addControllerService("esService", service);
        } catch (final InitializationException e) {
            throw new RuntimeException(e);
        }
        runner.enableControllerService(service);
        runner.setProperty(AbstractJsonQueryElasticsearch.CLIENT_SERVICE, "esService");
        runner.setProperty(AbstractJsonQueryElasticsearch.INDEX, INDEX_NAME);
        runner.setProperty(AbstractJsonQueryElasticsearch.TYPE, "message");
        runner.setValidateExpressionUsage(true);

        return runner;
    }

    MockFlowFile runOnce(final TestRunner runner) {
        return runOnce(runner, "test");
    }

    MockFlowFile runOnce(final TestRunner runner, final String data) {
        final MockFlowFile ff;
        if (isInput()) {
            runner.setIncomingConnection(true);
            ff = runner.enqueue(data);
        } else {
            runner.setIncomingConnection(false);
            ff = null;
        }

        runner.run(1, true, true);
        return ff;
    }

    static TestElasticsearchClientService getService(final TestRunner runner) {
        return runner.getControllerService("esService", TestElasticsearchClientService.class);
    }

    void reset(final TestRunner runner) {
        runner.clearProvenanceEvents();
        runner.clearTransferState();
        if (getStateScope() != null) {
            try {
                runner.getStateManager().clear(getStateScope());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        getService(runner).resetPageCount();
    }
}
