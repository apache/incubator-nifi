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

package org.apache.nifi.elasticsearch.unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticSearchStringLookupService;
import org.apache.nifi.elasticsearch.TestControllerServiceProcessor;
import org.apache.nifi.elasticsearch.TestElasticSearchClientService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ElasticSearchStringLookupServiceTest {
    private ElasticSearchClientService mockClientService;
    private ElasticSearchStringLookupService lookupService;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    public void setup() throws Exception {
        mockClientService = new TestElasticSearchClientService();
        lookupService = new ElasticSearchStringLookupService();
        TestRunner runner = TestRunners.newTestRunner(TestControllerServiceProcessor.class);
        runner.addControllerService("clientService", mockClientService);
        runner.addControllerService("lookupService", lookupService);
        runner.enableControllerService(mockClientService);
        runner.setProperty(lookupService, ElasticSearchStringLookupService.CLIENT_SERVICE, "clientService");
        runner.setProperty(lookupService, ElasticSearchStringLookupService.INDEX, "users");
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, "clientService");
        runner.setProperty(TestControllerServiceProcessor.LOOKUP_SERVICE, "lookupService");
        runner.enableControllerService(lookupService);
    }

    @SuppressWarnings("unchecked")
    @Test
    void simpleLookupTest() throws Exception {
        Map<String, Object> coordinates = new HashMap<>();
        coordinates.put(ElasticSearchStringLookupService.ID, "12345");

        Optional<String> result = lookupService.lookup(coordinates);

        assertNotNull(result);
        assertTrue(result.isPresent());
        String json = result.get();
        Map<String, Object> parsed = MAPPER.readValue(json, Map.class);
        assertEquals(((TestElasticSearchClientService) mockClientService).getData(), parsed);
    }
}
