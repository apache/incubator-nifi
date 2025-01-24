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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.processors.elasticsearch.api.QueryDefinitionType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractByQueryElasticsearchTest {
    private static final String TEST_DIR = "src/test/resources/AbstractByQueryElasticsearchTest";
    private static final String INDEX = "test_idx";
    private static final String TYPE = "test_type";
    private static final String CLIENT_NAME = "clientService";

    private static final ObjectMapper TEST_MAPPER = new ObjectMapper();

    private static String matchAllQuery;
    private TestElasticsearchClientService client;
    private TestRunner runner;

    public abstract String queryAttr();

    public abstract String tookAttr();

    public abstract String errorAttr();

    public abstract AbstractByQueryElasticsearch getTestProcessor();

    public abstract void expectError(final TestElasticsearchClientService client);

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        matchAllQuery = JsonUtils.readString(Paths.get("src/test/resources/common/matchAllQuery.json"));
    }

    @BeforeEach
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(getTestProcessor());
        client = new TestElasticsearchClientService(true);
        runner.addControllerService(CLIENT_NAME, client);
        runner.enableControllerService(client);
        runner.setProperty(AbstractByQueryElasticsearch.CLIENT_SERVICE, CLIENT_NAME);
    }

    private void postTest(final TestRunner runner, final String queryParam) {
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_SUCCESS, 1);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(AbstractByQueryElasticsearch.REL_SUCCESS);
        final String attr = flowFiles.getFirst().getAttribute(tookAttr());
        final String query = flowFiles.getFirst().getAttribute(queryAttr());
        assertNotNull(attr);
        assertEquals("100", attr);
        assertNotNull(query);
        assertEquals(queryParam, query);
    }

    @Test
    void testMandatoryProperties() {
        runner.removeProperty(ElasticsearchRestProcessor.CLIENT_SERVICE);
        runner.removeProperty(ElasticsearchRestProcessor.INDEX);
        runner.removeProperty(ElasticsearchRestProcessor.TYPE);
        runner.removeProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE);
        runner.removeProperty(ElasticsearchRestProcessor.QUERY);
        runner.removeProperty(ElasticsearchRestProcessor.QUERY_ATTRIBUTE);
        runner.removeProperty(ElasticsearchRestProcessor.MAX_JSON_FIELD_STRING_LENGTH);

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
        runner.setProperty(ElasticsearchRestProcessor.CLIENT_SERVICE, "not-a-service");
        runner.setProperty(ElasticsearchRestProcessor.INDEX, "");
        runner.setProperty(ElasticsearchRestProcessor.TYPE, "");
        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, "not-valid");
        runner.setProperty(ElasticsearchRestProcessor.MAX_JSON_FIELD_STRING_LENGTH, "not-a-size");

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        final String expected = String.format("""
                        Processor has 6 validation failures:
                        '%s' validated against 'not-valid' is invalid because Given value not found in allowed set '%s'
                        '%s' validated against '' is invalid because %s cannot be empty
                        '%s' validated against '' is invalid because %s cannot be empty
                        '%s' validated against 'not-a-size' is invalid because Must be of format <Data Size> <Data Unit> where <Data Size> is a non-negative integer \
                        and <Data Unit> is a supported Data Unit, such as: B, KB, MB, GB, TB
                        '%s' validated against 'not-a-service' is invalid because Property references a Controller Service that does not exist
                        '%s' validated against 'not-a-service' is invalid because Invalid Controller Service: not-a-service is not a valid Controller Service Identifier
                        """,
        ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE.getName(),
        Arrays.stream(QueryDefinitionType.values()).map(QueryDefinitionType::getValue).collect(Collectors.joining(", ")),
        ElasticsearchRestProcessor.INDEX.getName(), ElasticsearchRestProcessor.INDEX.getName(),
        ElasticsearchRestProcessor.TYPE.getName(), ElasticsearchRestProcessor.TYPE.getName(),
        ElasticsearchRestProcessor.MAX_JSON_FIELD_STRING_LENGTH.getName(),
        ElasticsearchRestProcessor.CLIENT_SERVICE.getDisplayName(), ElasticsearchRestProcessor.CLIENT_SERVICE.getDisplayName());
        assertEquals(expected, assertionError.getMessage());
    }

    @Test
    void testInvalidQueryProperty() {
        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.FULL_QUERY);
        runner.setProperty(ElasticsearchRestProcessor.INDEX, "test-index");
        runner.setProperty(ElasticsearchRestProcessor.QUERY, "not-json");

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertTrue(assertionError.getMessage().contains("not-json"));
    }

    @Test
    void testInvalidQueryBuilderProperties() {
        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY);
        runner.setProperty(ElasticsearchRestProcessor.INDEX, "test-index");
        runner.setProperty(ElasticsearchRestProcessor.QUERY_CLAUSE, "not-json");
        runner.setProperty(ElasticsearchRestProcessor.SCRIPT, "not-json-script");

        final AssertionError assertionError = assertThrows(AssertionError.class, runner::run);
        assertTrue(assertionError.getMessage().contains("not-json"));
    }

    @Test
    public void testWithFlowfileInput() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.assertValid();
        runner.enqueue(query);
        runner.run();

        postTest(runner, query);

        assertTrue(client.getElasticsearchRequestOptions().getRequestParameters().isEmpty());
        assertTrue(client.getElasticsearchRequestOptions().getRequestHeaders().isEmpty());
    }

    @Test
    public void testWithFlowfileInputAndRequestParametersAndRequestHeaders() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.setProperty("refresh", "true");
        runner.setProperty("slices", "${slices}");
        runner.setProperty(ElasticsearchRestProcessor.DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER + "Accept", "${accept}");
        runner.assertValid();
        runner.enqueue(query, Map.of("slices", "auto", "accept", "application/json"));
        runner.run();

        postTest(runner, query);

        assertEquals(2, client.getElasticsearchRequestOptions().getRequestParameters().size());
        assertEquals("true", client.getElasticsearchRequestOptions().getRequestParameters().get("refresh"));
        assertEquals("auto", client.getElasticsearchRequestOptions().getRequestParameters().get("slices"));

        assertEquals(1, client.getElasticsearchRequestOptions().getRequestHeaders().size());
        assertEquals("application/json", client.getElasticsearchRequestOptions().getRequestHeaders().get("Accept"));
    }

    @Test
    public void testWithQuery() throws Exception {
        final String query = JsonUtils.readString(Paths.get(TEST_DIR, "matchUsingExpressionLanguageQuery.json"));
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.assertValid();
        runner.enqueue("", Collections.singletonMap("field.name", "test_field"));
        runner.run();

        postTest(runner, query.replace("${field.name}", "test_field"));

        runner.clearTransferState();

        final String query2 = JsonUtils.readString(Paths.get(TEST_DIR, "matchQuery.json"));
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query2);
        runner.setIncomingConnection(false);
        runner.assertValid();
        runner.run();
        postTest(runner, query2);
    }

    @Test
    public void testErrorAttribute() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        expectError(client);
        runner.assertValid();
        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_FAILURE, 1);

        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(AbstractByQueryElasticsearch.REL_FAILURE).getFirst();
        final String attr = mockFlowFile.getAttribute(errorAttr());
        assertNotNull(attr);
    }

    @Test
    public void testInputHandling() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractByQueryElasticsearch.REL_FAILURE, 0);
    }

    @Test
    public void testNoInputHandling() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.setIncomingConnection(false);
        runner.assertValid();
        runner.run();

        postTest(runner, query);

        assertTrue(client.getElasticsearchRequestOptions().getRequestParameters().isEmpty());
        assertTrue(client.getElasticsearchRequestOptions().getRequestHeaders().isEmpty());
    }

    @Test
    public void testNoInputHandlingWithRequestParameters() {
        final String query = matchAllQuery;
        runner.setProperty(AbstractByQueryElasticsearch.QUERY, query);
        runner.setProperty(AbstractByQueryElasticsearch.INDEX, INDEX);
        runner.setProperty(AbstractByQueryElasticsearch.TYPE, TYPE);
        runner.setProperty(AbstractByQueryElasticsearch.QUERY_ATTRIBUTE, queryAttr());
        runner.setProperty("refresh", "true");
        runner.setProperty("slices", "${slices}");
        runner.setProperty(ElasticsearchRestProcessor.DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER + "Accept", "${accept}");
        runner.setEnvironmentVariableValue("slices", "auto");
        runner.setEnvironmentVariableValue("accept", "application/json");
        runner.setIncomingConnection(false);
        runner.assertValid();
        runner.run();

        postTest(runner, query);

        assertEquals(2, client.getElasticsearchRequestOptions().getRequestParameters().size());
        assertEquals("true", client.getElasticsearchRequestOptions().getRequestParameters().get("refresh"));
        assertEquals("auto", client.getElasticsearchRequestOptions().getRequestParameters().get("slices"));

        assertEquals(1, client.getElasticsearchRequestOptions().getRequestHeaders().size());
        assertEquals("application/json", client.getElasticsearchRequestOptions().getRequestHeaders().get("Accept"));
    }

    @ParameterizedTest
    @MethodSource
    void testQueryBuilder(final String queryClause, final String script, final String expectedQuery) throws Exception {
        runner.setProperty(ElasticsearchRestProcessor.QUERY_DEFINITION_STYLE, QueryDefinitionType.BUILD_QUERY);

        if (queryClause != null) {
            runner.setProperty(ElasticsearchRestProcessor.QUERY_CLAUSE, queryClause);
        }
        if (script != null) {
            runner.setProperty(ElasticsearchRestProcessor.SCRIPT, script);
        }

        final String query = getTestProcessor().getQuery(null, runner.getProcessContext(), null, TEST_MAPPER);
        assertNotNull(query);
        assertEquals(TEST_MAPPER.readTree(expectedQuery), TEST_MAPPER.readTree(query));
    }

    private static Stream<Arguments> testQueryBuilder() {
        // bool query with range filter as constructed by ConsumeElasticsearch
        final String queryClause = "{\"bool\": {\"filter\": [{\"range\": {\"@timestamp\": {\"gt\": \"123456\"}}}]}}";
        final String script = "{\"source\": \"ctx._source.count++\", \"lang\": \"painless\"}";

        return Stream.of(
                Arguments.of(null, null, "{}"),
                Arguments.of(queryClause, null, String.format("{\"query\": %s}", queryClause)),
                Arguments.of(null, script, String.format("{\"script\": %s}", script)),
                Arguments.of(queryClause, script, String.format("{\"query\": %s, \"script\": %s}", queryClause, script))
        );
    }
}
