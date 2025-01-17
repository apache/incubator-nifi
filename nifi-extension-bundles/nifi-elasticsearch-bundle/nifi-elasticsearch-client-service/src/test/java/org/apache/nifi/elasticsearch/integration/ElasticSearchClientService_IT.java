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

package org.apache.nifi.elasticsearch.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.AuthorizationScheme;
import org.apache.nifi.elasticsearch.DeleteOperationResponse;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl;
import org.apache.nifi.elasticsearch.ElasticsearchException;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.elasticsearch.MapBuilder;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.elasticsearch.UpdateOperationResponse;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceLookup;
import org.apache.nifi.util.StringUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ElasticSearchClientService_IT extends AbstractElasticsearch_IT {
    @AfterEach
    void after() throws Exception {
        ((ElasticSearchClientServiceImpl) service).onDisabled();
    }

    private Map<PropertyDescriptor, String> getClientServiceProperties() {
        return ((MockControllerServiceLookup) runner.getProcessContext().getControllerServiceLookup())
                .getControllerServices().get(CLIENT_SERVICE_NAME).getProperties();
    }

    @Test
    void testVerifySuccess() {
        final List<ConfigVerificationResult> results = service.verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), null),
                runner.getLogger(),
                Collections.emptyMap()
        );
        assertEquals(4, results.size());
        assertEquals(3, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(), results.toString());
        assertVerifySnifferSkipped(results);
    }

    @Test
    void testVerifyDisabledSuccess() {
        //Disable as verify can be run in either state
        runner.disableControllerService(service);
        final List<ConfigVerificationResult> results = service.verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), null),
                runner.getLogger(),
                Collections.emptyMap()
        );
        assertEquals(4, results.size());
        assertEquals(3, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(), results.toString());
    }

    @Test
    void testVerifySniffer() {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.SNIFF_CLUSTER_NODES, "true");
        runner.setProperty(service, ElasticSearchClientService.SNIFF_ON_FAILURE, "false");
        runner.enableControllerService(service);
        assertVerifySniffer();

        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.SNIFF_ON_FAILURE, "true");
        runner.enableControllerService(service);
        assertVerifySniffer();
    }

    private void assertVerifySniffer() {
        final List<ConfigVerificationResult> results = service.verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), null),
                runner.getLogger(),
                Collections.emptyMap()
        );
        assertEquals(4, results.size());
        assertEquals(4, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(), results.toString());
    }

    @Test
    void testVerifySuccessWithApiKeyAuth() throws IOException {
        final Pair<String, String> apiKey = createApiKeyForIndex();

        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.AUTHORIZATION_SCHEME, AuthorizationScheme.API_KEY);
        runner.removeProperty(service, ElasticSearchClientService.USERNAME);
        runner.removeProperty(service, ElasticSearchClientService.PASSWORD);
        runner.setProperty(service, ElasticSearchClientService.API_KEY_ID, apiKey.getKey());
        runner.setProperty(service, ElasticSearchClientService.API_KEY, apiKey.getValue());
        runner.enableControllerService(service);

        final List<ConfigVerificationResult> results = service.verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), null),
                runner.getLogger(),
                Collections.emptyMap()
        );
        assertEquals(4, results.size());
        assertEquals(3, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(), results.toString());
        assertVerifySnifferSkipped(results);
    }

    @Test
    void testVerifyFailedURL() {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, "blah://invalid");

        final List<ConfigVerificationResult> results = service.verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), null),
                runner.getLogger(),
                Collections.emptyMap()
        );
        assertEquals(4, results.size());
        assertEquals(3, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SKIPPED).count(), results.toString());
        assertEquals(1, results.stream().filter(
                result -> Objects.equals(result.getVerificationStepName(), ElasticSearchClientServiceImpl.VERIFICATION_STEP_CLIENT_SETUP)
                        && Objects.equals(result.getExplanation(), "Incorrect/invalid " + ElasticSearchClientService.HTTP_HOSTS.getDisplayName())
                        && result.getOutcome() == ConfigVerificationResult.Outcome.FAILED).count(),
                results.toString()
        );
    }

    @Test
    void testVerifyFailedAuth() {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.USERNAME, "invalid");
        runner.setProperty(service, ElasticSearchClientService.PASSWORD, "not-real");

        final List<ConfigVerificationResult> results = service.verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), null),
                runner.getLogger(),
                Collections.emptyMap()
        );
        assertEquals(4, results.size());
        assertEquals(1, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(), results.toString());
        assertEquals(2, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SKIPPED).count(), results.toString());
        assertEquals(1, results.stream().filter(
                result -> Objects.equals(result.getVerificationStepName(), ElasticSearchClientServiceImpl.VERIFICATION_STEP_CONNECTION)
                        && Objects.equals(result.getExplanation(), "Unable to retrieve system summary from Elasticsearch root endpoint")
                        && result.getOutcome() == ConfigVerificationResult.Outcome.FAILED).count(),
                results.toString()
        );
    }

    @Test
    void testVerifyFailedApiKeyAuth() {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.AUTHORIZATION_SCHEME, AuthorizationScheme.API_KEY);
        runner.removeProperty(service, ElasticSearchClientService.USERNAME);
        runner.removeProperty(service, ElasticSearchClientService.PASSWORD);
        runner.setProperty(service, ElasticSearchClientService.API_KEY_ID, "invalid");
        runner.setProperty(service, ElasticSearchClientService.API_KEY, "not-real");
        runner.enableControllerService(service);

        final List<ConfigVerificationResult> results = service.verify(
                new MockConfigurationContext(service, getClientServiceProperties(), runner.getProcessContext().getControllerServiceLookup(), null),
                runner.getLogger(),
                Collections.emptyMap()
        );
        assertEquals(4, results.size());
        assertEquals(1, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL).count(), results.toString());
        assertEquals(2, results.stream().filter(result -> result.getOutcome() == ConfigVerificationResult.Outcome.SKIPPED).count(), results.toString());
        assertEquals(1, results.stream().filter(
                result -> Objects.equals(result.getVerificationStepName(), ElasticSearchClientServiceImpl.VERIFICATION_STEP_CONNECTION)
                        && Objects.equals(result.getExplanation(), "Unable to retrieve system summary from Elasticsearch root endpoint")
                        && result.getOutcome() == ConfigVerificationResult.Outcome.FAILED).count(),
                results.toString()
        );
    }

    @Test
    void testBasicSearch() throws Exception {
        assertBasicSearch(null);
    }

    @Test
    void testBasicSearchRequestParameters() throws Exception {
        assertBasicSearch(createParameters("preference", "_local"));
    }

    private void assertBasicSearch(final Map<String, String> requestParameters) throws JsonProcessingException {
        final Map<String, Object> temp = new MapBuilder()
                .of("size", 10, "query", new MapBuilder().of("match_all", new HashMap<>()).build(),
                        "aggs", new MapBuilder()
                                .of("term_counts", new MapBuilder()
                                        .of("terms", new MapBuilder()
                                                .of("field", "msg", "size", 5)
                                                .build())
                                        .build())
                                .build())
                .build();
        final String query = prettyJson(temp);


        final SearchResponse response = service.search(query, "messages", type, requestParameters);
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(10, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations are missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");

        @SuppressWarnings("unchecked") final Map<String, Object> termCounts = (Map<String, Object>) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        @SuppressWarnings("unchecked") final List<Map<String, Object>> buckets = (List<Map<String, Object>>) termCounts.get("buckets");
        assertNotNull(buckets, "Buckets branch was empty");
        final Map<String, Object> expected = new MapBuilder()
                .of("one", 1, "two", 2, "three", 3,
                        "four", 4, "five", 5)
                .build();

        buckets.forEach(aggRes -> {
            final String key = (String) aggRes.get("key");
            final Integer docCount = (Integer) aggRes.get("doc_count");
            assertEquals(expected.get(key), docCount, String.format("%s did not match.", key));
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    void testSearchEmptySource() throws Exception {
        final Map<String, Object> temp = new MapBuilder()
                .of("size", 2,
                        "query", new MapBuilder().of("match_all", new HashMap<>()).build())
                .build();
        final String query = prettyJson(temp);


        final SearchResponse response = service.search(query, "messages", type, createParameters("_source", "not_exists"));
        assertNotNull(response, "Response was null");

        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(2, response.getHits().size(), "Wrong number of hits");
        response.getHits().forEach(h -> {
            assertInstanceOf(Map.class, h.get("_source"), "Source not a Map");
            assertTrue(((Map<String, Object>) h.get("_source")).isEmpty(), "Source not empty");
        });
    }

    @Test
    void testSearchNoSource() throws Exception {
        final Map<String, Object> temp = new MapBuilder()
                .of("size", 1,
                        "query", new MapBuilder().of("match_all", new HashMap<>()).build())
                .build();
        final String query = prettyJson(temp);


        final SearchResponse response = service.search(query, "no_source", type, null);
        assertNotNull(response, "Response was null");

        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(1, response.getHits().size(), "Wrong number of hits");
        response.getHits().forEach(h -> {
            assertFalse(h.isEmpty(), "Hit was empty");
            assertFalse(h.containsKey("_source"), "Hit contained _source");
        });
    }

    @Test
    void testV6SearchWarnings() throws JsonProcessingException {
        assumeTrue(getElasticMajorVersion() == 6, "Requires Elasticsearch 6");
        final String query = prettyJson(new MapBuilder()
                .of("size", 1,
                        "query", new MapBuilder().of("query_string",
                                new MapBuilder().of("query", 1, "all_fields", true).build()
                        ).build())
                .build());
        final String type = "a-type";
        final SearchResponse response = service.search(query, INDEX, type, null);
        assertFalse(response.getWarnings().isEmpty(), "Missing warnings");
    }

    @Test
    void testV7SearchWarnings() throws JsonProcessingException {
        assumeTrue(getElasticMajorVersion() == 7, "Requires Elasticsearch 7");
        final String query = prettyJson(new MapBuilder()
                .of("size", 1, "query", new MapBuilder().of("match_all", new HashMap<>()).build())
                .build());
        final String type = "a-type";
        final SearchResponse response = service.search(query, INDEX, type, null);
        assertFalse(response.getWarnings().isEmpty(), "Missing warnings");
    }

    @Disabled("Skip until Elasticsearch 8.x has a _search API deprecation")
    @Test
    void testV8SearchWarnings() {
        assumeTrue(getElasticMajorVersion() == 8, "Requires Elasticsearch 8");
        fail("Elasticsearch 8 search API deprecations not currently available for test");
    }

    @Test
    void testScroll() throws JsonProcessingException {
        final String query = prettyJson(new MapBuilder()
                .of("size", 2, "query", new MapBuilder().of("match_all", new HashMap<>()).build(),
                        "aggs", new MapBuilder()
                        .of("term_counts", new MapBuilder()
                                .of("terms", new MapBuilder()
                                        .of("field", "msg", "size", 5)
                                        .build())
                                .build())
                        .build())
                .build());

        // initiate the scroll
        final SearchResponse response = service.search(query, INDEX, type, Collections.singletonMap("scroll", "10s"));
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(2, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations are missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");
        assertNotNull(response.getScrollId(), "ScrollId missing");
        assertNull(response.getSearchAfter(), "Unexpected Search_After");
        assertNull(response.getPitId(), "Unexpected pitId");

        @SuppressWarnings("unchecked") final Map<String, Object> termCounts = (Map<String, Object>) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        @SuppressWarnings("unchecked") final List<Map<String, Object>> buckets = (List<Map<String, Object>>) termCounts.get("buckets");
        assertEquals(5, buckets.size(), "Buckets count is wrong");

        // scroll the next page
        final Map<String, String> parameters = createParameters("scroll_id", response.getScrollId(), "scroll", "10s");
        final SearchResponse scrollResponse = service.scroll(prettyJson(parameters));
        assertNotNull(scrollResponse, "Scroll Response was null");

        assertEquals(15, scrollResponse.getNumberOfHits(), "Wrong count");
        assertFalse(scrollResponse.isTimedOut(), "Timed out");
        assertNotNull(scrollResponse.getHits(), "Hits was null");
        assertEquals(2, scrollResponse.getHits().size(), "Wrong number of hits");
        assertNotNull(scrollResponse.getAggregations(), "Aggregations missing");
        assertEquals(0, scrollResponse.getAggregations().size(), "Aggregation count is wrong");
        assertNotNull(scrollResponse.getScrollId(), "ScrollId missing");
        assertNull(scrollResponse.getSearchAfter(), "Unexpected Search_After");
        assertNull(scrollResponse.getPitId(), "Unexpected pitId");

        assertNotEquals(scrollResponse.getHits(), response.getHits(), "Same results");

        // delete the scroll
        DeleteOperationResponse deleteResponse = service.deleteScroll(scrollResponse.getScrollId());
        assertNotNull(deleteResponse, "Delete Response was null");

        // delete scroll again (should now be unknown but the 404 caught and ignored)
        deleteResponse = service.deleteScroll(scrollResponse.getScrollId());
        assertNotNull(deleteResponse, "Delete Response was null");
    }

    @Test
    void testSearchAfter() throws JsonProcessingException {
        final Map<String, Object> queryMap = new MapBuilder()
                .of("size", 2, "query", new MapBuilder()
                        .of("match_all", new HashMap<>()).build(), "aggs", new MapBuilder()
                        .of("term_counts", new MapBuilder()
                            .of("terms", new MapBuilder()
                                    .of("field", "msg", "size", 5)
                                    .build())
                            .build()).build())
                .of("sort", Collections.singletonList(
                        new MapBuilder().of("msg", "desc").build()
                ))
                .build();
        final String query = prettyJson(queryMap);

        // search first page
        final SearchResponse response = service.search(query, INDEX, type, null);
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(2, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");
        assertNull(response.getScrollId(), "Unexpected ScrollId");
        assertNotNull(response.getSearchAfter(), "Search_After missing");
        assertNull(response.getPitId(), "Unexpected pitId");

        @SuppressWarnings("unchecked") final Map<String, Object> termCounts = (Map<String, Object>) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        @SuppressWarnings("unchecked") final List<Map<String, Object>> buckets = (List<Map<String, Object>>) termCounts.get("buckets");
        assertEquals(5, buckets.size(), "Buckets count is wrong");

        // search the next page
        final Map<String, Object> page2QueryMap = new HashMap<>(queryMap);
        page2QueryMap.put("search_after", MAPPER.readValue(response.getSearchAfter(), List.class));
        page2QueryMap.remove("aggs");
        final String secondPage = prettyJson(page2QueryMap);
        final SearchResponse secondResponse = service.search(secondPage, INDEX, type, null);
        assertNotNull(secondResponse, "Second Response was null");

        assertEquals(15, secondResponse.getNumberOfHits(), "Wrong count");
        assertFalse(secondResponse.isTimedOut(), "Timed out");
        assertNotNull(secondResponse.getHits(), "Hits was null");
        assertEquals(2, secondResponse.getHits().size(), "Wrong number of hits");
        assertNotNull(secondResponse.getAggregations(), "Aggregations missing");
        assertEquals(0, secondResponse.getAggregations().size(), "Aggregation count is wrong");
        assertNull(secondResponse.getScrollId(), "Unexpected ScrollId");
        assertNotNull(secondResponse.getSearchAfter(), "Unexpected Search_After");
        assertNull(secondResponse.getPitId(), "Unexpected pitId");

        assertNotEquals(secondResponse.getHits(), response.getHits(), "Same results");
    }

    @Test
    void testPointInTime() throws JsonProcessingException {
        // Point in Time only available in 7.10+ with XPack enabled
        final double majorVersion = getElasticMajorVersion();
        final double minorVersion = getElasticMinorVersion();
        assumeTrue(majorVersion >= 8 || (majorVersion == 7 && minorVersion >= 10), "Requires version 7.10+");

        // initialise
        final String pitId = service.initialisePointInTime(INDEX, "10s");

        final Map<String, Object> queryMap = new MapBuilder()
                .of("size", 2, "query", new MapBuilder().of("match_all", new HashMap<>()).build())
                .of("aggs", new MapBuilder().of("term_counts", new MapBuilder()
                        .of("terms", new MapBuilder()
                                .of("field", "msg", "size", 5)
                                .build())
                        .build()).build())
                .of("sort", Collections.singletonList(
                        new MapBuilder().of("msg", "desc").build()
                ))
                .of("pit", new MapBuilder()
                        .of("id", pitId, "keep_alive", "10s")
                        .build())
                .build();
        final String query = prettyJson(queryMap);

        // search first page
        final SearchResponse response = service.search(query, null, type, null);
        assertNotNull(response, "Response was null");

        assertEquals(15, response.getNumberOfHits(), "Wrong count");
        assertFalse(response.isTimedOut(), "Timed out");
        assertNotNull(response.getHits(), "Hits was null");
        assertEquals(2, response.getHits().size(), "Wrong number of hits");
        assertNotNull(response.getAggregations(), "Aggregations missing");
        assertEquals(1, response.getAggregations().size(), "Aggregation count is wrong");
        assertNull(response.getScrollId(), "Unexpected ScrollId");
        assertNotNull(response.getSearchAfter(), "Unexpected Search_After");
        assertNotNull(response.getPitId(), "pitId missing");

        @SuppressWarnings("unchecked") final Map<String, Object> termCounts = (Map<String, Object>) response.getAggregations().get("term_counts");
        assertNotNull(termCounts, "Term counts was missing");
        @SuppressWarnings("unchecked") final List<Map<String, Object>> buckets = (List<Map<String, Object>>) termCounts.get("buckets");
        assertEquals(5, buckets.size(), "Buckets count is wrong");

        // search the next page
        final Map<String, Object> page2QueryMap = new HashMap<>(queryMap);
        page2QueryMap.put("search_after", MAPPER.readValue(response.getSearchAfter(), List.class));
        page2QueryMap.remove("aggs");
        final String secondPage = prettyJson(page2QueryMap);
        final SearchResponse secondResponse = service.search(secondPage, null, type, null);
        assertNotNull(secondResponse, "Second Response was null");

        assertEquals(15, secondResponse.getNumberOfHits(), "Wrong count");
        assertFalse(secondResponse.isTimedOut(), "Timed out");
        assertNotNull(secondResponse.getHits(), "Hits was null");
        assertEquals(2, secondResponse.getHits().size(), "Wrong number of hits");
        assertNotNull(secondResponse.getAggregations(), "Aggregations missing");
        assertEquals(0, secondResponse.getAggregations().size(), "Aggregation count is wrong");
        assertNull(secondResponse.getScrollId(), "Unexpected ScrollId");
        assertNotNull(secondResponse.getSearchAfter(), "Unexpected Search_After");
        assertNotNull(secondResponse.getPitId(), "pitId missing");

        assertNotEquals(secondResponse.getHits(), response.getHits(), "Same results");

        // delete pitId
        DeleteOperationResponse deleteResponse = service.deletePointInTime(pitId);
        assertNotNull(deleteResponse, "Delete Response was null");

        // delete pitId again (should now be unknown but the 404 caught and ignored)
        deleteResponse = service.deletePointInTime(pitId);
        assertNotNull(deleteResponse, "Delete Response was null");
    }

    @Test
    void testDeleteByQuery() throws Exception {
        final String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "five").build())
                                .build()).build());
        final DeleteOperationResponse response = service.deleteByQuery(query, INDEX, type, null);
        assertNotNull(response);
    }

    @Test
    void testDeleteByQueryRequestParameters() throws Exception {
        final String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "six").build())
                        .build()).build());
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("refresh", "true");
        final DeleteOperationResponse response = service.deleteByQuery(query, INDEX, type, parameters);
        assertNotNull(response);
    }

    @Test
    void testUpdateByQuery() throws Exception {
        final String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "four").build())
                        .build()).build());
        final UpdateOperationResponse response = service.updateByQuery(query, INDEX, type, null);
        assertNotNull(response);
    }

    @Test
    void testUpdateByQueryRequestParameters() throws Exception {
        final String query = prettyJson(new MapBuilder()
                .of("query", new MapBuilder()
                        .of("match", new MapBuilder().of("msg", "four").build())
                        .build()).build());
        final Map<String, String> parameters = new HashMap<>();
        parameters.put("refresh", "true");
        parameters.put("slices", "1");
        final UpdateOperationResponse response = service.updateByQuery(query, INDEX, type, parameters);
        assertNotNull(response);
    }

    @Test
    void testDeleteById() throws Exception {
        final String ID = "1";
        final Map<String, Object> originalDoc = service.get(INDEX, type, ID, null);
        try {
            final DeleteOperationResponse response = service.deleteById(INDEX, type, ID, null);
            assertNotNull(response);
            final ElasticsearchException ee = assertThrows(ElasticsearchException.class, () ->
                service.get(INDEX, type, ID, null));
            assertTrue(ee.isNotFound());
            final Map<String, Object> doc = service.get(INDEX, type, "2", null);
            assertNotNull(doc);
        } finally {
            // replace the deleted doc
            service.add(new IndexOperationRequest(INDEX, type, "1", originalDoc, IndexOperationRequest.Operation.Index, null, false, null, null), null);
            waitForIndexRefresh(); // (affects later tests using _search or _bulk)
        }
    }

    @Test
    void testGet() {
        for (int index = 1; index <= 15; index++) {
            final String id = String.valueOf(index);
            final Map<String, Object> doc = service.get(INDEX, type, id, null);
            assertNotNull(doc, "Doc was null");
            assertNotNull(doc.get("msg"), "${doc.toString()}\t${doc.keySet().toString()}");
        }
    }

    @Test
    void testGetEmptySource() {
        final Map<String, Object> doc = service.get(INDEX, type, "1", Collections.singletonMap("_source", "not_exist"));
        assertNotNull(doc, "Doc was null");
        assertTrue(doc.isEmpty(), "Doc was not empty");
    }
    @Test
    void testGetNoSource() {
        final Map<String, Object> doc = service.get("no_source", type, "1", null);
        assertNotNull(doc, "Doc was null");
        assertTrue(doc.isEmpty(), "Doc was not empty");
    }

    @Test
    void testGetNotFound() {
        final ElasticsearchException ee = assertThrows(ElasticsearchException.class, () -> service.get(INDEX, type, "not_found", null));
        assertTrue(ee.isNotFound());
    }

    @Test
    void testExists() {
        assertTrue(service.exists(INDEX, null), "index does not exist");
        assertFalse(service.exists("index-does-not-exist", null), "index exists");
    }

    @Test
    void testCompression() {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.COMPRESSION, "true");
        runner.enableControllerService(service);
        runner.assertValid(service);

        assertTrue(service.exists(INDEX, null), "index does not exist");
    }

    @Test
    void testNoMetaHeader() {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.SEND_META_HEADER, "false");
        runner.enableControllerService(service);
        runner.assertValid(service);

        assertTrue(service.exists(INDEX, null), "index does not exist");
    }

    @Test
    void testStrictDeprecation() {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.STRICT_DEPRECATION, "true");
        runner.enableControllerService(service);
        runner.assertValid(service);

        assertTrue(service.exists(INDEX, null), "index does not exist");
    }

    @Test
    void testNodeSelector() {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.NODE_SELECTOR, ElasticSearchClientService.NODE_SELECTOR_SKIP_DEDICATED_MASTERS);
        runner.enableControllerService(service);
        runner.assertValid(service);

        assertTrue(service.exists(INDEX, null), "index does not exist");
    }

    @Test
    void testRestClientRequestHeaders() {
        runner.disableControllerService(service);
        runner.setProperty(service, "User-Agent", "NiFi Integration Tests");
        runner.setProperty(service, "X-Extra_header", "Request should still work");
        runner.enableControllerService(service);
        runner.assertValid(service);

        assertTrue(service.exists(INDEX, null), "index does not exist");
    }

    @Test
    void testSniffer() {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.SNIFF_CLUSTER_NODES, "false");
        runner.setProperty(service, ElasticSearchClientService.SNIFF_ON_FAILURE, "true");
        runner.assertNotValid(service);

        runner.setProperty(service, ElasticSearchClientService.SNIFF_CLUSTER_NODES, "true");
        runner.enableControllerService(service);
        runner.assertValid(service);

        assertTrue(service.exists(INDEX, null), "index does not exist");
    }

    @Test
    void testNullSuppression() throws InterruptedException {
        final Map<String, Object> doc = new HashMap<>();
        doc.put("msg", "test");
        doc.put("is_null", null);
        doc.put("is_empty", "");
        doc.put("is_blank", " ");
        doc.put("empty_nested", Collections.emptyMap());
        doc.put("empty_array", Collections.emptyList());

        // index with nulls
        suppressNulls(false);
        IndexOperationResponse response = service.bulk(
                Collections.singletonList(
                        new IndexOperationRequest("nulls", type, "1", doc, IndexOperationRequest.Operation.Index, null, false, null, null)
                ),
                null
        );
        assertNotNull(response);
        waitForIndexRefresh();

        Map<String, Object> result = service.get("nulls", type, "1", null);
        assertEquals(doc, result);

        // suppress nulls
        suppressNulls(true);
        response = service.bulk(Collections.singletonList(new IndexOperationRequest("nulls", type, "2", doc, IndexOperationRequest.Operation.Index, null, false, null, null)), null);
        assertNotNull(response);
        waitForIndexRefresh();

        result = service.get("nulls", type, "2", null);
        assertTrue(result.keySet().containsAll(Arrays.asList("msg", "is_blank")), "Non-nulls (present): " + result);
        assertFalse(result.containsKey("is_null"), "is_null (should be omitted): " + result);
        assertFalse(result.containsKey("is_empty"), "is_empty (should be omitted): " + result);
        assertFalse(result.containsKey("empty_nested"), "empty_nested (should be omitted): " + result);
        assertFalse(result.containsKey("empty_array"), "empty_array (should be omitted): " + result);
    }

    private void suppressNulls(final boolean suppressNulls) {
        runner.disableControllerService(service);
        runner.setProperty(service, ElasticSearchClientService.SUPPRESS_NULLS, suppressNulls
                ? ElasticSearchClientService.ALWAYS_SUPPRESS.getValue()
                : ElasticSearchClientService.NEVER_SUPPRESS.getValue());
        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @Test
    void testBulkAddTwoIndexes() throws Exception {
        final List<IndexOperationRequest> payload = new ArrayList<>();
        for (int x = 0; x < 20; x++) {
            final String index = x % 2 == 0 ? "bulk_a" : "bulk_b";
            payload.add(new IndexOperationRequest(index, type, String.valueOf(x), new HashMap<>() {{
                put("msg", "test");
            }}, IndexOperationRequest.Operation.Index, null, false, null, null));
        }
        for (int x = 0; x < 5; x++) {
            payload.add(new IndexOperationRequest("bulk_c", type, String.valueOf(x), new HashMap<>() {{
                put("msg", "test");
            }}, IndexOperationRequest.Operation.Index, null, false, null, null));
        }
        final IndexOperationResponse response = service.bulk(payload, createParameters("refresh", "true"));
        assertNotNull(response);
        waitForIndexRefresh();

        /*
         * Now, check to ensure that both indexes got populated appropriately.
         */
        final String query = "{ \"query\": { \"match_all\": {}}}";
        final Long indexA = service.count(query, "bulk_a", type, null);
        final Long indexB = service.count(query, "bulk_b", type, null);
        final Long indexC = service.count(query, "bulk_c", type, null);

        assertNotNull(indexA);
        assertNotNull(indexB);
        assertNotNull(indexC);
        assertEquals(indexA, indexB);
        assertEquals(10, indexA.intValue());
        assertEquals(10, indexB.intValue());
        assertEquals(5, indexC.intValue());

        final Long total = service.count(query, "bulk_a,bulk_b,bulk_c", type, null);
        assertNotNull(total);
        assertEquals(25, total.intValue());
    }

    @Test
    void testBulkRequestParametersAndBulkHeaders() {
        final List<IndexOperationRequest> payload = new ArrayList<>();
        for (int x = 0; x < 20; x++) {
            final String index = x % 2 == 0 ? "bulk_a" : "bulk_b";
            payload.add(new IndexOperationRequest(index, type, String.valueOf(x), new MapBuilder().of("msg", "test").build(),
                    IndexOperationRequest.Operation.Index, null, false, null, Collections.singletonMap("retry_on_conflict", "3")));
        }
        for (int x = 0; x < 5; x++) {
            payload.add(new IndexOperationRequest("bulk_c", type, String.valueOf(x), new MapBuilder().of("msg", "test").build(),
                    IndexOperationRequest.Operation.Index, null, false, null, null));
        }
        final IndexOperationResponse response = service.bulk(payload, createParameters("refresh", "true"));
        assertNotNull(response);

        /*
         * Now, check to ensure that all indices got populated and refreshed appropriately.
         */
        final String query = "{ \"query\": { \"match_all\": {}}}";
        final Long indexA = service.count(query, "bulk_a", type, null);
        final Long indexB = service.count(query, "bulk_b", type, null);
        final Long indexC = service.count(query, "bulk_c", type, null);

        assertNotNull(indexA);
        assertNotNull(indexB);
        assertNotNull(indexC);
        assertEquals(indexA, indexB);
        assertEquals(10, indexA.intValue());
        assertEquals(10, indexB.intValue());
        assertEquals(5, indexC.intValue());

        final Long total = service.count(query, "bulk_*", type, null);
        assertNotNull(total);
        assertEquals(25, total.intValue());
    }

    @Test
    void testUnknownBulkHeader() {
        final IndexOperationRequest failingRequest = new IndexOperationRequest("bulk_c", type, "1", new MapBuilder().of("msg", "test").build(),
                IndexOperationRequest.Operation.Index, null, false, null, Collections.singletonMap("not_exist", "true"));
        final ElasticsearchException ee = assertThrows(ElasticsearchException.class, () -> service.add(failingRequest, null));
        assertInstanceOf(ResponseException.class, ee.getCause());
        assertTrue(ee.getCause().getMessage().contains("Action/metadata line [1] contains an unknown parameter [not_exist]"));
    }

    @Test
    void testDynamicTemplates() {
        assumeFalse(getElasticMajorVersion() == 6, "Requires Elasticsearch > 6");

        final List<IndexOperationRequest> payload = Collections.singletonList(
                new IndexOperationRequest("dynamo", type, "1", new MapBuilder().of("msg", "test", "hello", "world").build(),
                        IndexOperationRequest.Operation.Index, null, false, new MapBuilder().of("hello", "test_text").build(), null)
        );

        final IndexOperationResponse response = service.bulk(payload, createParameters("refresh", "true"));
        assertNotNull(response);

        /*
         * Now, check the dynamic_template was applied
         */
        final Map<String, Object> fieldMapping = getIndexFieldMapping(type);
        assertEquals(1, fieldMapping.size());
        assertEquals("text", fieldMapping.get("type"));
    }

    @Test
    void testUpdateAndUpsert() throws InterruptedException {
        final String TEST_ID = "update-test";
        final String UPSERTED_ID = "upsert-ftw";
        final String UPSERT_SCRIPT_ID = "upsert-script";
        final String SCRIPTED_UPSERT_ID = "scripted-upsert-test";
        try {
            final Map<String, Object> doc = new HashMap<>();
            doc.put("msg", "Buongiorno, mondo");
            doc.put("counter", 1);
            service.add(new IndexOperationRequest(INDEX, type, TEST_ID, doc, IndexOperationRequest.Operation.Index, null, false, null, null), createParameters("refresh", "true"));
            Map<String, Object> result = service.get(INDEX, type, TEST_ID, null);
            assertEquals(doc, result, "Not the same");

            final Map<String, Object> updates = new HashMap<>();
            updates.put("from", "john.smith");
            final Map<String, Object> merged = new HashMap<>();
            merged.putAll(updates);
            merged.putAll(doc);
            IndexOperationRequest request = new IndexOperationRequest(INDEX, type, TEST_ID, updates, IndexOperationRequest.Operation.Update, null, false, null, null);
            service.add(request, createParameters("refresh", "true"));
            result = service.get(INDEX, type, TEST_ID, null);
            assertTrue(result.containsKey("from"));
            assertTrue(result.containsKey("counter"));
            assertTrue(result.containsKey("msg"));
            assertEquals(merged, result, "Not the same after update.");

            final Map<String, Object> upsertItems = new HashMap<>();
            upsertItems.put("upsert_1", "hello");
            upsertItems.put("upsert_2", 1);
            upsertItems.put("upsert_3", true);
            request = new IndexOperationRequest(INDEX, type, UPSERTED_ID, upsertItems, IndexOperationRequest.Operation.Upsert, null, false, null, null);
            service.add(request, createParameters("refresh", "true"));
            result = service.get(INDEX, type, UPSERTED_ID, null);
            assertEquals(upsertItems, result);

            final Map<String, Object> upsertDoc = new HashMap<>();
            upsertDoc.put("msg", "Only if doesn't already exist");
            final Map<String, Object> script = new HashMap<>();
            script.put("source", "ctx._source.counter += params.count");
            script.put("lang", "painless");
            script.put("params", Collections.singletonMap("count", 2));
            // apply script to existing document
            request = new IndexOperationRequest(INDEX, type, TEST_ID, upsertDoc, IndexOperationRequest.Operation.Upsert, script, false, null, null);
            service.add(request, createParameters("refresh", "true"));
            result = service.get(INDEX, type, TEST_ID, null);
            assertEquals(doc.get("msg"), result.get("msg"));
            assertEquals(3, result.get("counter"));
            // index document that doesn't already exist (don't apply script)
            request = new IndexOperationRequest(INDEX, type, UPSERT_SCRIPT_ID, upsertDoc, IndexOperationRequest.Operation.Upsert, script, false, null, null);
            service.add(request, createParameters("refresh", "true"));
            result = service.get(INDEX, type, UPSERT_SCRIPT_ID, null);
            assertNull(result.get("counter"));
            assertEquals(upsertDoc, result);

            final Map<String, Object> emptyUpsertDoc = Collections.emptyMap();
            final Map<String, Object> upsertScript = new HashMap<>();
            upsertScript.put("source", "if ( ctx.op == 'create' ) { ctx._source.counter = params.count } else { ctx._source.counter += params.count }");
            upsertScript.put("lang", "painless");
            upsertScript.put("params", Collections.singletonMap("count", 2));
            // no script execution if doc found (without scripted_upsert)
            request = new IndexOperationRequest(INDEX, type, SCRIPTED_UPSERT_ID, emptyUpsertDoc, IndexOperationRequest.Operation.Upsert, upsertScript, false, null, null);
            service.add(request, createParameters("refresh", "true"));
            assertFalse(service.documentExists(INDEX, type, SCRIPTED_UPSERT_ID, null));
            // script execution with no doc found (with scripted_upsert) - doc not create, no "upsert" doc provided (empty objects suppressed)
            suppressNulls(true);
            request = new IndexOperationRequest(INDEX, type, SCRIPTED_UPSERT_ID, emptyUpsertDoc, IndexOperationRequest.Operation.Upsert, upsertScript, true, null, null);
            service.add(request, createParameters("refresh", "true"));
            assertFalse(service.documentExists(INDEX, type, SCRIPTED_UPSERT_ID, null));
            // script execution with no doc found (with scripted_upsert) - doc created, empty "upsert" doc provided
            suppressNulls(false);
            request = new IndexOperationRequest(INDEX, type, SCRIPTED_UPSERT_ID, emptyUpsertDoc, IndexOperationRequest.Operation.Upsert, upsertScript, true, null, null);
            service.add(request, createParameters("refresh", "true"));
            result = service.get(INDEX, type, SCRIPTED_UPSERT_ID, null);
            assertEquals(2, result.get("counter"));
            // script execution with no doc found (with scripted_upsert) - doc updated
            request = new IndexOperationRequest(INDEX, type, SCRIPTED_UPSERT_ID, emptyUpsertDoc, IndexOperationRequest.Operation.Upsert, upsertScript, true, null, null);
            service.add(request, createParameters("refresh", "true"));
            result = service.get(INDEX, type, SCRIPTED_UPSERT_ID, null);
            assertEquals(4, result.get("counter"));
        } finally {
            final List<IndexOperationRequest> deletes = new ArrayList<>();
            deletes.add(new IndexOperationRequest(INDEX, type, TEST_ID, null, IndexOperationRequest.Operation.Delete, null, false, null, null));
            deletes.add(new IndexOperationRequest(INDEX, type, UPSERTED_ID, null, IndexOperationRequest.Operation.Delete, null, false, null, null));
            deletes.add(new IndexOperationRequest(INDEX, type, UPSERT_SCRIPT_ID, null, IndexOperationRequest.Operation.Delete, null, false, null, null));
            deletes.add(new IndexOperationRequest(INDEX, type, SCRIPTED_UPSERT_ID, null, IndexOperationRequest.Operation.Delete, null, false, null, null));
            assertFalse(service.bulk(deletes, createParameters("refresh", "true")).hasErrors());
            waitForIndexRefresh(); // wait 1s for index refresh (doesn't prevent GET but affects later tests using _search or _bulk)
            assertFalse(service.documentExists(INDEX, type, TEST_ID, null));
            assertFalse(service.documentExists(INDEX, type, UPSERTED_ID, null));
            assertFalse(service.documentExists(INDEX, type, UPSERT_SCRIPT_ID, null));
            assertFalse(service.documentExists(INDEX, type, SCRIPTED_UPSERT_ID, null));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void testGetBulkResponsesWithErrors() {
        final List<IndexOperationRequest> ops = Arrays.asList(
                new IndexOperationRequest(INDEX, type, "1", new MapBuilder().of("msg", "one", "intField", 1).build(),
                        IndexOperationRequest.Operation.Index, null, false, null, null), // OK
                new IndexOperationRequest(INDEX, type, "2", new MapBuilder().of("msg", "two", "intField", 1).build(),
                        IndexOperationRequest.Operation.Create, null, false, null, null), // already exists
                new IndexOperationRequest(INDEX, type, "1", new MapBuilder().of("msg", "one", "intField", "notaninteger").build(),
                        IndexOperationRequest.Operation.Index, null, false, null, null) // can't parse int field
        );
        final IndexOperationResponse response = service.bulk(ops, createParameters("refresh", "true"));
        assertTrue(response.hasErrors());
        assertEquals(2, response.getItems().stream().filter(it -> {
            final Optional<String> first = it.keySet().stream().findFirst();
            if (first.isPresent()) {
                final String key = first.get();
                return ((Map<String, Object>) it.get(key)).containsKey("error");
            } else {
                throw new IllegalStateException("Cannot find index response operation items");
            }
        }).count());
    }

    private Map<String, String> createParameters(final String... extra) {
        if (extra.length % 2 == 1) { //Putting this here to help maintainers catch stupid bugs before they happen
            throw new RuntimeException("createParameters must have an even number of String parameters.");
        }

        final Map<String, String> parameters = new HashMap<>();
        for (int index = 0; index < extra.length; index += 2) {
            parameters.put(extra[index], extra[index + 1]);
        }

        return parameters;
    }

    private static void waitForIndexRefresh() throws InterruptedException {
        Thread.sleep(1000);
    }

    private void assertVerifySnifferSkipped(final List<ConfigVerificationResult> results) {
        assertEquals(1, results.stream().filter(
                        result -> Objects.equals(result.getVerificationStepName(), ElasticSearchClientServiceImpl.VERIFICATION_STEP_SNIFFER)
                                && Objects.equals(result.getExplanation(), "Sniff on Connection not enabled")
                                && result.getOutcome() == ConfigVerificationResult.Outcome.SKIPPED).count(),
                results.toString()
        );
    }

    protected Pair<String, String> createApiKeyForIndex() throws IOException {
        final String body = prettyJson(new MapBuilder()
                .of("name", "test-api-key")
                .of("role_descriptors", new MapBuilder()
                        .of("test-role", new MapBuilder()
                                .of("cluster", Collections.singletonList("all"))
                                .of("index", Collections.singletonList(new MapBuilder()
                                        .of("names", Collections.singletonList("*"))
                                        .of("privileges", Collections.singletonList("all"))
                                        .build()))
                                .build())
                        .build())
                .build());
        final String endpoint = String.format("%s/%s", elasticsearchHost, "_security/api_key");
        final Request request = new Request("POST", endpoint);
        final HttpEntity jsonBody = new NStringEntity(body, ContentType.APPLICATION_JSON);
        request.setEntity(jsonBody);

        final Response response = testDataManagementClient.performRequest(request);
        final InputStream inputStream = response.getEntity().getContent();
        final byte[] result = IOUtils.toByteArray(inputStream);
        inputStream.close();
        final Map<String, String> ret = MAPPER.readValue(new String(result, StandardCharsets.UTF_8), new TypeReference<>() { });
        return Pair.of(ret.get("id"), ret.get("api_key"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getIndexFieldMapping(final String type) {
        final String index = "dynamo";
        final String field = "hello";

        final Request request = new Request("GET",
                String.format("%s/%s/_mapping%s/field/%s", elasticsearchHost, index, StringUtils.isBlank(type) ? "" : "/" + type, field)
        );
        try {
            final Response response = testDataManagementClient.performRequest(request);
            final InputStream inputStream = response.getEntity().getContent();
            final byte[] result = IOUtils.toByteArray(inputStream);
            inputStream.close();
            final Map<String, Object> parsed = MAPPER.readValue(new String(result, StandardCharsets.UTF_8), Map.class);
            final Map<String, Object> parsedIndex = (Map<String, Object>) parsed.get(index);
            final Map<String, Object> parsedMappings = (Map<String, Object>) (StringUtils.isBlank(type)
                    ? parsedIndex.get("mappings")
                    : ((Map<String, Object>) parsedIndex.get("mappings")).get(type));
            return (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) parsedMappings.get(field)).get("mapping")).get(field);
        } catch (final IOException ioe) {
            throw new IllegalStateException(String.format("Error getting field mappings %s [%s]", index, field), ioe);
        }
    }
}