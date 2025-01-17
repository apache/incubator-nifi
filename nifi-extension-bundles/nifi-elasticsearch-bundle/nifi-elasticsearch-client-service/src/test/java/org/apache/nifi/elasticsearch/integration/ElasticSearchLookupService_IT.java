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

import org.apache.nifi.elasticsearch.ElasticSearchLookupService;
import org.apache.nifi.elasticsearch.TestControllerServiceProcessor;
import org.apache.nifi.elasticsearch.TestSchemaRegistry;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ElasticSearchLookupService_IT extends AbstractElasticsearch_IT {
    private static final String LOOKUP_SERVICE_NAME = "Lookup Service";

    private ElasticSearchLookupService lookupService;

    @Override
    @BeforeEach
    void before() throws Exception {
        super.before();

        lookupService = new ElasticSearchLookupService();
        runner.addControllerService(LOOKUP_SERVICE_NAME, lookupService);
        runner.setProperty(TestControllerServiceProcessor.CLIENT_SERVICE, CLIENT_SERVICE_NAME);
        runner.setProperty(TestControllerServiceProcessor.LOOKUP_SERVICE, LOOKUP_SERVICE_NAME);
        runner.setProperty(lookupService, ElasticSearchLookupService.CLIENT_SERVICE, CLIENT_SERVICE_NAME);
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "user_details");
        setTypeOnLookupService();

        runner.enableControllerService(lookupService);
    }

    void setTypeOnLookupService() {
        if (getElasticMajorVersion() == 6) {
            runner.setProperty(lookupService, ElasticSearchLookupService.TYPE, type);
        } else {
            runner.removeProperty(lookupService, ElasticSearchLookupService.TYPE);
        }
    }

    @Test
    void testValidity() throws Exception {
        setDefaultSchema();
        runner.assertValid();
    }

    private void setDefaultSchema() throws Exception {
        runner.disableControllerService(lookupService);
        final SchemaRegistry registry = new TestSchemaRegistry();
        runner.addControllerService("registry", registry);
        runner.setProperty(lookupService, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.enableControllerService(registry);
        runner.enableControllerService(lookupService);
    }

    @Test
    void lookupById() throws LookupFailureException {
        final Map<String, Object> coordinates = new HashMap<>();
        coordinates.put("_id", "2");
        final Optional<Record> result = lookupService.lookup(coordinates);

        assertNotNull(result);
        assertTrue(result.isPresent());
        final Record record = result.get();
        assertEquals("jane.doe@company.com", record.getAsString("email"));
        assertEquals("098-765-4321", record.getAsString("phone"));
        assertEquals("GHIJK", record.getAsString("accessKey"));
    }

    @Test
    void testInvalidIdScenarios() {
        final List<Map<String, Object>> coordinates = new ArrayList<>();
        Map<String, Object> temp = new HashMap<>();
        temp.put("_id", 1);
        coordinates.add(temp);
        temp = new HashMap<>();
        temp.put("_id", 1);
        temp.put("email", "john.smith@company.com");
        coordinates.add(temp);

        for (final Map<String, Object> coordinate: coordinates) {
            Exception exception = null;

            try {
                lookupService.lookup(coordinate);
            } catch (final Exception ex) {
                exception = ex;
            }

            assertNotNull(exception);
            assertInstanceOf(LookupFailureException.class, exception);
        }
    }

    @Test
    void lookupByQuery() throws LookupFailureException {
        final Map<String, Object> coordinates = new HashMap<>();
        coordinates.put("phone", "098-765-4321");
        coordinates.put("email", "jane.doe@company.com");
        final Optional<Record> result = lookupService.lookup(coordinates);

        assertNotNull(result);
        assertTrue(result.isPresent());
        final Record record = result.get();
        assertEquals("jane.doe@company.com", record.getAsString("email"));
        assertEquals("098-765-4321", record.getAsString("phone"));
        assertEquals("GHIJK", record.getAsString("accessKey"));
    }

    @Test
    void testNestedSchema() throws LookupFailureException {
        final Map<String, Object> coordinates = new HashMap<>();
        coordinates.put("subField.deeper.deepest.super_secret", "The sky is blue");
        coordinates.put("subField.deeper.secretz", "Buongiorno, mondo!!");
        coordinates.put("msg", "Hello, world");

        runner.disableControllerService(lookupService);
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "nested");
        setTypeOnLookupService();
        runner.enableControllerService(lookupService);

        final Optional<Record> response = lookupService.lookup(coordinates);
        assertNotNull(response);
        assertTrue(response.isPresent());
        final Record rec = response.get();
        assertEquals("Hello, world", rec.getValue("msg"));
        final Record subRec = getSubRecord(rec, "subField");
        assertNotNull(subRec);
        final Record deeper = getSubRecord(subRec, "deeper");
        assertNotNull(deeper);
        final Record deepest = getSubRecord(deeper, "deepest");
        assertNotNull(deepest);
        assertEquals("The sky is blue", deepest.getAsString("super_secret"));
    }

    @Test
    void testDetectedSchema() throws LookupFailureException {
        runner.disableControllerService(lookupService);
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "complex");
        setTypeOnLookupService();
        runner.enableControllerService(lookupService);
        final Map<String, Object> coordinates = new HashMap<>();
        coordinates.put("_id", "1");

        final Optional<Record> response = lookupService.lookup(coordinates);
        assertNotNull(response);
        assertTrue(response.isPresent());
        final Record rec = response.get();
        final Record subRec = getSubRecord(rec, "subField");

        final Record r2 = new MapRecord(rec.getSchema(), new HashMap<>());
        final RecordPath path = RecordPath.compile("/subField/longField");
        final RecordPathResult result = path.evaluate(r2);
        final Optional<FieldValue> fieldValue = result.getSelectedFields().findFirst();
        assertTrue(fieldValue.isPresent());
        fieldValue.get().updateValue(1234567890L);

        assertNotNull(rec);
        assertNotNull(subRec);
        assertEquals("Hello, world", rec.getValue("msg"));
        assertNotNull(rec.getValue("subField"));
        assertEquals(100000, subRec.getValue("longField"));
        assertEquals("2018-04-10T12:18:05Z", subRec.getValue("dateField"));
    }

    static Record getSubRecord(final Record rec, final String fieldName) {
        final RecordSchema schema = rec.getSchema();
        final Optional<RecordField> recordField = schema.getField(fieldName);
        if (recordField.isPresent()) {
            final RecordSchema subSchema = ((RecordDataType) recordField.get().getDataType()).getChildSchema();
            return rec.getAsRecord(fieldName, subSchema);
        } else {
            throw new IllegalStateException(String.format("Field %s not present in Record", fieldName));
        }
    }

    @Test
    void testMappings() throws LookupFailureException {
        runner.disableControllerService(lookupService);
        runner.setProperty(lookupService, "$.subField.longField", "/longField2");
        runner.setProperty(lookupService, "$.subField.dateField", "/dateField2");
        runner.setProperty(lookupService, ElasticSearchLookupService.INDEX, "nested");
        setTypeOnLookupService();
        runner.enableControllerService(lookupService);

        final Map<String, Object> coordinates = new HashMap<>();
        coordinates.put("msg", "Hello, world");
        final Optional<Record> result = lookupService.lookup(coordinates);
        assertTrue(result.isPresent());
        final Record rec = result.get();
        final Map<String, Object> entries = new HashMap<>();
        entries.put("dateField2", "2018-08-14T10:08:00Z");
        entries.put("longField2", 150000);
        entries.forEach((key, value1) -> {
            final Object value = rec.getValue(key);
            assertEquals(value1, value);
        });
    }
}
