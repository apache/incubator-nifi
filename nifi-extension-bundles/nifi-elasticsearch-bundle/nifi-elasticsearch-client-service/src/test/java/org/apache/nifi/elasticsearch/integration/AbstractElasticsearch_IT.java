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

import org.apache.nifi.elasticsearch.AuthorizationScheme;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl;
import org.apache.nifi.elasticsearch.TestControllerServiceProcessor;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.Arrays;
import java.util.List;

abstract class AbstractElasticsearch_IT extends AbstractElasticsearchITBase {
    static final List<String> TEST_INDICES =
            Arrays.asList("user_details", "complex", "nested", "bulk_a", "bulk_b", "bulk_c", "error_handler", "messages");

    ElasticSearchClientService service;

    @BeforeEach
    void before() throws Exception {
        runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        service = new ElasticSearchClientServiceImpl();
        runner.addControllerService(CLIENT_SERVICE_NAME, service);
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, CLIENT_SERVICE_NAME);

        runner.setProperty(service, ElasticSearchClientService.HTTP_HOSTS, elasticsearchHost);
        runner.setProperty(service, ElasticSearchClientService.CONNECT_TIMEOUT, "10000");
        runner.setProperty(service, ElasticSearchClientService.SOCKET_TIMEOUT, "60000");
        runner.setProperty(service, ElasticSearchClientService.SUPPRESS_NULLS, ElasticSearchClientService.ALWAYS_SUPPRESS);
        runner.setProperty(service, ElasticSearchClientService.AUTHORIZATION_SCHEME, AuthorizationScheme.BASIC);
        runner.setProperty(service, ElasticSearchClientService.USERNAME, "elastic");
        runner.setProperty(service, ElasticSearchClientService.PASSWORD, ELASTIC_USER_PASSWORD);
        runner.removeProperty(service, ElasticSearchClientService.API_KEY);
        runner.removeProperty(service, ElasticSearchClientService.API_KEY_ID);
        runner.setProperty(service, ElasticSearchClientService.COMPRESSION, "false");
        runner.setProperty(service, ElasticSearchClientService.SEND_META_HEADER, "true");
        runner.setProperty(service, ElasticSearchClientService.STRICT_DEPRECATION, "false");
        runner.setProperty(service, ElasticSearchClientService.SNIFF_CLUSTER_NODES, "false");
        runner.setProperty(service, ElasticSearchClientService.SNIFF_ON_FAILURE, "false");
        runner.removeProperty(service, ElasticSearchClientService.PATH_PREFIX);
        runner.setProperty(service, ElasticSearchClientService.NODE_SELECTOR, ElasticSearchClientService.NODE_SELECTOR_ANY);

        runner.enableControllerService(service);

        service.refresh(null, null);
    }

    @AfterAll
    static void afterAll() {
        tearDownTestData(TEST_INDICES);
        stopTestcontainer();
    }
}
