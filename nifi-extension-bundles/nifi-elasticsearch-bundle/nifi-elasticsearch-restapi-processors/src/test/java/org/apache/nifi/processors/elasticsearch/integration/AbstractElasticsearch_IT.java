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
package org.apache.nifi.processors.elasticsearch.integration;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl;
import org.apache.nifi.elasticsearch.integration.AbstractElasticsearchITBase;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processors.elasticsearch.ElasticsearchRestProcessor;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

abstract class AbstractElasticsearch_IT<P extends ElasticsearchRestProcessor> extends AbstractElasticsearchITBase {
    static final List<String> TEST_INDICES = Collections.singletonList("messages");

    static final String ID = "1";

    ElasticSearchClientServiceImpl service;

    abstract P getProcessor();

    @BeforeEach
    void before() throws Exception {
        runner = TestRunners.newTestRunner(getProcessor());

        service = new ElasticSearchClientServiceImpl();
        runner.addControllerService(CLIENT_SERVICE_NAME, service);
        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, elasticsearchHost);
        runner.setProperty(service, ElasticSearchClientService.CONNECT_TIMEOUT, "10000");
        runner.setProperty(service, ElasticSearchClientService.SOCKET_TIMEOUT, "60000");
        runner.setProperty(service, ElasticSearchClientService.SUPPRESS_NULLS, ElasticSearchClientService.ALWAYS_SUPPRESS);
        runner.setProperty(service, ElasticSearchClientService.USERNAME, "elastic");
        runner.setProperty(service, ElasticSearchClientService.PASSWORD, ELASTIC_USER_PASSWORD);

        runner.enableControllerService(service);

        runner.setProperty(ElasticsearchRestProcessor.CLIENT_SERVICE, CLIENT_SERVICE_NAME);
        runner.setProperty(ElasticsearchRestProcessor.INDEX, INDEX);
        runner.setProperty(ElasticsearchRestProcessor.TYPE, type);

        service.refresh(null, null);
    }

    @AfterEach
    void after() {
        runner.disableControllerService(service);
    }

    @AfterAll
    static void afterAll() {
        tearDownTestData(TEST_INDICES);
        stopTestcontainer();
    }

    @Test
    void testVerifyIndexExists() {
        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor()).verify(
                runner.getProcessContext(), runner.getLogger(), Collections.emptyMap()
        );

        assertIndexVerificationResults(results, true, String.format("Index [%s] exists", INDEX));
    }

    @Test
    void testVerifyIndexDoesNotExist() {
        final String notExists = "not-exists";
        runner.setProperty(ElasticsearchRestProcessor.INDEX, notExists);

        final List<ConfigVerificationResult> results = ((VerifiableProcessor) runner.getProcessor()).verify(
                runner.getProcessContext(), runner.getLogger(), Collections.emptyMap()
        );

        assertIndexVerificationResults(results, false, String.format("Index [%s] does not exist", notExists));
    }

    private void assertIndexVerificationResults(final List<ConfigVerificationResult> results, final boolean expectedExists, final String expectedExplanation) {
        // allow for extra verification test results beyond Index Exist
        assertFalse(results.isEmpty());
        final List<ConfigVerificationResult> indexResults = results.stream()
                .filter(result -> ElasticsearchRestProcessor.VERIFICATION_STEP_INDEX_EXISTS.equals(result.getVerificationStepName()))
                .toList();
        assertEquals(1, indexResults.size(), results.toString());
        final ConfigVerificationResult result = indexResults.getFirst();

        final ConfigVerificationResult.Outcome expectedOutcome;
        if (getProcessor().isIndexNotExistSuccessful()) {
            expectedOutcome = ConfigVerificationResult.Outcome.SUCCESSFUL;
        } else {
            if (expectedExists) {
                expectedOutcome = ConfigVerificationResult.Outcome.SUCCESSFUL;
            } else {
                expectedOutcome = ConfigVerificationResult.Outcome.FAILED;
            }
        }
        assertEquals(expectedOutcome, result.getOutcome(), results.toString());
        assertEquals(expectedExplanation, result.getExplanation(), results.toString());
    }
}
