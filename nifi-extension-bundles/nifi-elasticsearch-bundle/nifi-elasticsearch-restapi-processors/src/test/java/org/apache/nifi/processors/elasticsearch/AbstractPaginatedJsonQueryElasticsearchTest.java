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
import org.apache.nifi.processors.elasticsearch.api.AggregationResultsFormat;
import org.apache.nifi.processors.elasticsearch.api.PaginationType;
import org.apache.nifi.processors.elasticsearch.api.ResultOutputStrategy;
import org.apache.nifi.processors.elasticsearch.api.SearchResultsFormat;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractPaginatedJsonQueryElasticsearchTest extends AbstractJsonQueryElasticsearchTest<AbstractPaginatedJsonQueryElasticsearch> {
    protected static String matchAllWithSortByMsgWithSizeQuery;
    private static final String TEST_DIR = "src/test/resources/AbstractPaginatedJsonQueryElasticsearchTest";
    private static String matchAllWithSortByMessage;
    private static String matchAllWithSortByMsgWithoutSize;

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        AbstractJsonQueryElasticsearchTest.setUpBeforeClass();
        matchAllWithSortByMessage = JsonUtils.readString(Paths.get(TEST_DIR, "matchAllWithSortByMessageQuery.json"));
        matchAllWithSortByMsgWithoutSize = JsonUtils.readString(Paths.get(TEST_DIR, "matchAllWithSortByMsgQueryWithoutSize.json"));
        matchAllWithSortByMsgWithSizeQuery = JsonUtils.readString(Paths.get(TEST_DIR, "matchAllWithSortByMsgQueryWithSize.json"));
    }

    @Test
    void testInvalidPaginationProperties() {
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_KEEP_ALIVE, "not-a-period");
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, "not-enum");

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        final String expected = String.format("""
                        Processor has 2 validation failures:
                        '%s' validated against 'not-enum' is invalid because Given value not found in allowed set '%s'
                        '%s' validated against 'not-a-period' is invalid because Must be of format <duration> <TimeUnit> where <duration> \
                        is a non-negative integer and TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days
                        """,
                AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE.getName(),
                Stream.of(PaginationType.values()).map(PaginationType::getValue).collect(Collectors.joining(", ")),
                AbstractPaginatedJsonQueryElasticsearch.PAGINATION_KEEP_ALIVE.getName());
        assertEquals(expected, assertionError.getMessage());
    }

    @Test
    void testSinglePage() {
        // paged query hits (no splitting)
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllQuery);
        MockFlowFile input = runOnce(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
        final MockFlowFile pageQueryHitsNoSplitting = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst();
        pageQueryHitsNoSplitting.assertAttributeEquals("hit.count", "10");
        pageQueryHitsNoSplitting.assertAttributeEquals("page.number", "1");
        AbstractJsonQueryElasticsearchTest.assertOutputContent(pageQueryHitsNoSplitting.getContent(), 10, false);
        assertEquals(1L, runner.getProvenanceEvents().stream().filter(event ->
                ProvenanceEventType.RECEIVE.equals(event.getEventType()) && event.getAttribute("uuid").equals(pageQueryHitsNoSplitting.getAttribute("uuid"))).count());

        assertSendEvent(runner, input);
        reset(runner);

        // paged query hits splitting
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, ResultOutputStrategy.PER_HIT);
        input = runOnce(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 10, 0, 0);
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach(hit -> {
            hit.assertAttributeEquals("hit.count", "1");
            hit.assertAttributeEquals("page.number", "1");
            assertOutputContent(hit.getContent(), 1, false);
            assertEquals(1L, runner.getProvenanceEvents().stream().filter(event ->
                    ProvenanceEventType.RECEIVE.equals(event.getEventType()) && event.getAttribute("uuid").equals(hit.getAttribute("uuid"))).count());
        });
        assertSendEvent(runner, input);
        reset(runner);

        // paged query hits combined
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, ResultOutputStrategy.PER_QUERY);
        input = runOnce(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
        final MockFlowFile pageQueryHitsCombined = runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst();
        pageQueryHitsCombined.assertAttributeEquals("hit.count", "10");
        pageQueryHitsCombined.assertAttributeEquals("page.number", "1");
        AbstractJsonQueryElasticsearchTest.assertOutputContent(pageQueryHitsCombined.getContent(), 10, true);
        assertEquals(1L, runner.getProvenanceEvents().stream().filter(event ->
                ProvenanceEventType.RECEIVE.equals(event.getEventType()) && event.getAttribute("uuid").equals(pageQueryHitsCombined.getAttribute("uuid"))).count());
        assertSendEvent(runner, input);
    }

    static void assertFormattedResult(final SearchResultsFormat searchResultsFormat, final Map<String, Object> hit) {
        assertFalse(hit.isEmpty());
        switch (searchResultsFormat) {
            case SOURCE_ONLY:
                assertFalse(hit.containsKey("_source"));
                assertFalse(hit.containsKey("_index"));
                break;
            case METADATA_ONLY:
                assertFalse(hit.containsKey("_source"));
                assertTrue(hit.containsKey("_index"));
                break;
            case FULL:
                assertTrue(hit.containsKey("_source"));
                assertTrue(hit.containsKey("_index"));
                break;
            default:
                throw new IllegalArgumentException("Unknown SearchResultsFormat value: " + searchResultsFormat);
        }
    }

    private void assertResultsFormat(final TestRunner runner, final ResultOutputStrategy resultOutputStrategy, final SearchResultsFormat searchResultsFormat) {
        final int flowFileCount;
        final String hitsCount;
        boolean ndjson = false;

        switch (resultOutputStrategy) {
            case PER_QUERY:
                flowFileCount = 1;
                hitsCount = "10";
                ndjson = true;
                break;
            case PER_HIT:
                flowFileCount = 10;
                hitsCount = "1";
                break;
            case PER_RESPONSE:
                flowFileCount = 1;
                hitsCount = "10";
                break;
            default:
                throw new IllegalArgumentException("Unknown ResultOutputStrategy value: " + resultOutputStrategy);
        }

        // Test Relationship counts
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, flowFileCount, 0, 0);

        // Per response outputs an array of values
        final boolean ndJsonCopy = ndjson;
        runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).forEach( hit -> {
            hit.assertAttributeEquals("hit.count", hitsCount);
            AbstractJsonQueryElasticsearchTest.assertOutputContent(hit.getContent(), Integer.parseInt(hitsCount), ndJsonCopy);
            if (ResultOutputStrategy.PER_RESPONSE.equals(resultOutputStrategy)) {
                JsonUtils.readListOfMaps(hit.getContent()).forEach(h -> assertFormattedResult(searchResultsFormat, h));
            } else {
                final Map<String, Object> h = JsonUtils.readMap(hit.getContent());
                assertFormattedResult(searchResultsFormat, h);
            }

            assertEquals(1L, runner.getProvenanceEvents().stream().filter(event ->
                    ProvenanceEventType.RECEIVE.equals(event.getEventType()) && event.getAttribute("uuid").equals(hit.getAttribute("uuid"))).count());
        });
    }

    @ParameterizedTest
    @EnumSource(ResultOutputStrategy.class)
    void testResultsFormat(final ResultOutputStrategy resultOutputStrategy) throws JsonProcessingException {
        final TestRunner runner = createRunner(false);
        setQuery(runner, matchAllWithSortByMessage);
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, resultOutputStrategy);

        // Test against each result format
        for (final SearchResultsFormat searchResultsFormat : SearchResultsFormat.values()) {
            runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, searchResultsFormat);

            // Test against each pagination type
            for (final PaginationType paginationType : PaginationType.values()) {
                runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType);

                runOnce(runner);
                assertResultsFormat(runner, resultOutputStrategy, searchResultsFormat);
                reset(runner);
            }
        }
    }

    @Test
    void testDeleteScrollError() {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setThrowErrorInDelete(true);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.SCROLL);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllWithSortByMsgWithoutSize);

        // still expect "success" output for exception during final clean-up
        runMultiple(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
        assertTrue(runner.getLogger().getWarnMessages().stream().anyMatch(logMessage ->
                logMessage.getMsg().contains("Error while cleaning up Elasticsearch pagination resources") && logMessage.getMsg().contains("Simulated IOException - deleteScroll")));
        // check error was caught and logged
    }

    @Test
    void testScrollError() {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setMaxPages(2);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.SCROLL);
        runner.setProperty(AbstractJsonQueryElasticsearch.QUERY, matchAllWithSortByMsgWithSizeQuery);

        if (getStateScope() != null) {
            // initialize search for SearchElasticsearch, first page successful
            service.setThrowErrorInSearch(false);
            runOnce(runner);
            AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 1, 0, 0);
            runner.clearTransferState();
        }

        // scroll (error)
        service.setThrowErrorInSearch(true);
        runOnce(runner);
        // fir PaginatedJsonQueryElasticsearch, the input flowfile will be routed to failure, for SearchElasticsearch, there will be no output
        AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 0, getStateScope() == null ? 1 : 0, 0);
        assertTrue(runner.getLogger().getErrorMessages().stream().anyMatch(logMessage ->
                logMessage.getMsg().contains("Could not query documents") && logMessage.getThrowable().getMessage().contains("Simulated IOException")));
    }

    @Test
    void testDeletePitError() throws JsonProcessingException {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setThrowErrorInDelete(true);
        runner.setProperty(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_FORMAT, SearchResultsFormat.FULL);
        runner.setProperty(AbstractJsonQueryElasticsearch.AGGREGATION_RESULTS_FORMAT, AggregationResultsFormat.FULL);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.POINT_IN_TIME);
        setQuery(runner, matchAllWithSortByMsgWithoutSize);

        // still expect "success" output for exception during final clean-up
        runMultiple(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
        // check error was caught and logged
        assertTrue(runner.getLogger().getWarnMessages().stream().anyMatch(logMessage ->
                logMessage.getMsg().contains("Error while cleaning up Elasticsearch pagination resources") && logMessage.getMsg().contains("Simulated IOException - deletePointInTime")));
    }

    @Test
    void testInitialisePitError() throws JsonProcessingException {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setThrowErrorInPit(true);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, PaginationType.POINT_IN_TIME);
        setQuery(runner, matchAllWithSortByMsgWithoutSize);

        // expect "failure" output for exception during query setup
        runOnce(runner);
        AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 0, isInput() ? 1 : 0, 0);
        // check error was caught and logged
        assertTrue(runner.getLogger().getErrorMessages().stream().anyMatch(logMessage ->
                logMessage.getMsg().contains("Could not query documents") && logMessage.getThrowable().getMessage().contains("Simulated IOException - initialisePointInTime")));
    }

    @ParameterizedTest
    @EnumSource(PaginationType.class)
    void testPaginatedQueryWithoutSort(final PaginationType paginationType) throws JsonProcessingException {
        final TestRunner runner = createRunner(false);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType);
        setQuery(runner, matchAllQuery);

        if (PaginationType.SCROLL == paginationType) {
            // test scroll without sort (should succeed)
            runMultiple(runner);
            AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);
        } else {
            // test PiT/search_after without sort
            runOnce(runner);
            if (runner.getProcessor() instanceof ConsumeElasticsearch) {
                // sort is added based upon the Range Field (which cannot be empty)
                AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 1, 0, 0);
            } else {
                // expect "failure" output for exception during query setup
                AbstractJsonQueryElasticsearchTest.testCounts(runner, 0, 0, isInput() ? 1 : 0, 0);

                // check error was caught and logged
                assertTrue(runner.getLogger().getErrorMessages().stream().anyMatch(logMessage ->
                        logMessage.getMsg().contains("Could not query documents") && logMessage.getThrowable().getMessage().equals("Query using pit/search_after must contain a \"sort\" field")));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(PaginationType.class)
    void testPagination(final PaginationType paginationType) throws Exception {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setMaxPages(2);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType);
        setQuery(runner, matchAllWithSortByMsgWithSizeQuery);

        // Tests flowfile per page, hits splitting and hits combined
        for (final ResultOutputStrategy resultOutputStrategy : ResultOutputStrategy.values()) {
            runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, resultOutputStrategy);
            for (int iteration = 1; iteration < 4; iteration++) {
                // Check that changing OUTPUT_NO_HITS doesn't have any adverse effects on pagination
                runner.setProperty(AbstractJsonQueryElasticsearch.OUTPUT_NO_HITS, String.valueOf(iteration % 2 > 0).toLowerCase());
                runOnce(runner);
                validatePagination(runner, resultOutputStrategy, paginationType, iteration);
                if (ResultOutputStrategy.PER_QUERY.equals(resultOutputStrategy)) {
                    break;
                }
                runner.clearTransferState();
                if (getStateScope() == null) {
                    // reset PaginatedJsonQueryElasticsearch to re-run the query with different OUTPUT_NO_HITS setting
                    reset(runner);
                }
            }
            reset(runner);
        }
    }

    abstract void validatePagination(final TestRunner runner, final ResultOutputStrategy resultOutputStrategy, final PaginationType paginationType, int iteration) throws Exception;

    private void runMultiple(final TestRunner runner) {
        if (isInput()) {
            // with an input FlowFile, the processor still only triggers a single time and completes all processing
            runOnce(runner);
        } else {
            // with no input, the processor executes multiple times and tracks progress using STATE.Local
            runner.setIncomingConnection(false);
            runner.run(2, true, true);
        }
    }

    private void assertSendEvent(final TestRunner runner, final MockFlowFile input) {
        if (isInput()) {
            assertEquals(1L, runner.getProvenanceEvents().stream().filter(event ->
                    ProvenanceEventType.SEND.equals(event.getEventType()) && event.getAttribute("uuid").equals(input.getAttribute("uuid"))).count());
        } else {
            assertEquals(0L, runner.getProvenanceEvents().stream().filter(event ->
                    ProvenanceEventType.SEND.equals(event.getEventType())).count());
        }
    }

    @ParameterizedTest
    @EnumSource(PaginationType.class)
    void testEmptyHitsFlowFileIsProducedForEachResultSplitSetup(final PaginationType paginationType) throws JsonProcessingException {
        final TestRunner runner = createRunner(false);
        final TestElasticsearchClientService service = AbstractJsonQueryElasticsearchTest.getService(runner);
        service.setMaxPages(0);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.PAGINATION_TYPE, paginationType);
        setQuery(runner, matchAllWithSortByMessage);
        runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.OUTPUT_NO_HITS, "true");

        for (final ResultOutputStrategy resultOutputStrategy : ResultOutputStrategy.values()) {
            // test that an empty flow file is produced for a per query setup
            runner.setProperty(AbstractPaginatedJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT, resultOutputStrategy);
            runOnce(runner);
            AbstractJsonQueryElasticsearchTest.testCounts(runner, isInput() ? 1 : 0, 1, 0, 0);

            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("hit.count", "0");
            runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().assertAttributeEquals("page.number", "1");
            assertEquals(0, runner.getFlowFilesForRelationship(AbstractJsonQueryElasticsearch.REL_HITS).getFirst().getSize());
            reset(runner);
        }
    }
}
