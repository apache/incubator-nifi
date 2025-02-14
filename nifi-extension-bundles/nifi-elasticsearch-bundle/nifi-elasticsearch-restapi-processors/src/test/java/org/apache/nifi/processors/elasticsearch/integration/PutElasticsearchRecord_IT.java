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

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.elasticsearch.AbstractPutElasticsearch;
import org.apache.nifi.processors.elasticsearch.ElasticsearchRestProcessor;
import org.apache.nifi.processors.elasticsearch.PutElasticsearchRecord;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PutElasticsearchRecord_IT extends AbstractElasticsearch_IT<AbstractPutElasticsearch> {
    AbstractPutElasticsearch getProcessor() {
        return new PutElasticsearchRecord();
    }

    @Override
    @BeforeEach
    public void before() throws Exception {
        super.before();

        final RecordReaderFactory reader = new JsonTreeReader();
        runner.addControllerService("reader", reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);
        runner.assertValid(reader);
        runner.enableControllerService(reader);
        runner.setProperty(PutElasticsearchRecord.RECORD_READER, "reader");

        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("writer", writer);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(writer, JsonRecordSetWriter.SUPPRESS_NULLS, JsonRecordSetWriter.ALWAYS_SUPPRESS);
        runner.assertValid(writer);
        runner.enableControllerService(writer);
        runner.setProperty(PutElasticsearchRecord.RESULT_RECORD_WRITER, "writer");
    }

    @Test
    void testErrorRecordsOutputWithoutId() {
        final String json = """
                [
                    {"id": "1", "foo": false},
                    {"id": "2", "foo": 123}
                ]
                """;

        runner.setProperty(ElasticsearchRestProcessor.INDEX, "test-errors");
        runner.setProperty(PutElasticsearchRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchRecord.RETAIN_ID_FIELD, "false");

        runner.enqueue(json);
        runner.run();

        // record 1 should succeed and set the index mapping of "foo" to boolean, but the id field should not be in the successful output
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        final String successfulContent = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_SUCCESSFUL).getFirst().getContent();
        assertFalse(successfulContent.contains("\"id\":"), successfulContent);
        assertTrue(successfulContent.contains("\"foo\":false"), successfulContent);

        // record 2 should fail because an int cannot be indexed into a boolean field, the id field should remain in the error output
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 1);
        final String errorContent = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).getFirst().getContent();
        assertTrue(errorContent.contains("\"id\":\"2\""), errorContent);
        assertTrue(errorContent.contains("\"foo\":123"), errorContent);
    }

    @Test
    void testErrorRecordsOutputMultipleBatches() {
        final String json = """
                [
                    {"id": "1", "foo": false},
                    {"id": "2", "foo": 123},
                    {"id": "3", "foo": true},
                    {"id": "4", "foo": false},
                    {"id": "5", "foo": 456}
                ]
                """;

        runner.setProperty(ElasticsearchRestProcessor.INDEX, "test-errors-batches");
        runner.setProperty(PutElasticsearchRecord.BATCH_SIZE, "2");
        runner.setProperty(PutElasticsearchRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchRecord.RETAIN_ID_FIELD, "true");

        // allow multiple reads of the input FlowFile
        runner.setAllowRecursiveReads(true);

        runner.enqueue(json);
        runner.run();

        // records 1, 3 & 4 (output in 2 FlowFiles) should succeed and set the index mapping of "foo" to boolean, with id field retained
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 2);

        final MockFlowFile successful1 = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_SUCCESSFUL).getFirst();
        successful1.assertAttributeEquals("record.count", "1");
        final String successful1Content = successful1.getContent();
        assertTrue(successful1Content.contains("\"id\":\"1\""), successful1Content);
        assertTrue(successful1Content.contains("\"foo\":false"), successful1Content);

        final MockFlowFile successful2 = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_SUCCESSFUL).get(1);
        successful2.assertAttributeEquals("record.count", "2");
        final String successful2Content = successful2.getContent();
        assertTrue(successful2Content.contains("\"id\":\"3\""), successful2Content);
        assertTrue(successful2Content.contains("\"id\":\"4\""), successful2Content);

        // record 2 & 5 (in 2 batches) should fail because an int cannot be indexed into a boolean field, the id field should remain in the error output
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 2);

        final MockFlowFile error1 = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).getFirst();
        error1.assertAttributeEquals("record.count", "1");
        final String error1Content = error1.getContent();
        assertTrue(error1Content.contains("\"id\":\"2\""), error1Content);
        assertTrue(error1Content.contains("\"foo\":123"), error1Content);

        final MockFlowFile error2 = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).get(1);
        error2.assertAttributeEquals("record.count", "1");
        final String error2Content = error2.getContent();
        assertTrue(error2Content.contains("\"id\":\"5\""), error2Content);
        assertTrue(error2Content.contains("\"foo\":456"), error2Content);
    }

    @Test
    void testUpdateError() {
        final String json = """
                {"id": "123", "foo": "bar"}
                """;

        runner.setProperty(AbstractPutElasticsearch.INDEX_OP, "update");
        runner.setProperty(PutElasticsearchRecord.ID_RECORD_PATH, "/id");

        runner.enqueue(json);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 1);
        final String errorContent = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).getFirst().getContent();
        assertTrue(errorContent.contains("\"id\":\"123\""), errorContent);
        assertTrue(errorContent.contains("\"foo\":\"bar\""), errorContent);
    }

    @Test
    void testUpdateScriptError() {
        final String json = """
                {"id": "123", "script": {"source": "ctx._source.counter += params.count"}}
                """;

        runner.setProperty(AbstractPutElasticsearch.INDEX_OP, "update");
        runner.setProperty(PutElasticsearchRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchRecord.SCRIPT_RECORD_PATH, "/script");

        runner.enqueue(json);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 1);
        final String errorContent = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).getFirst().getContent();
        assertTrue(errorContent.contains("\"id\":\"123\""), errorContent);
        assertTrue(errorContent.contains("\"script\":{"), errorContent);
    }

    @Test
    void testScriptedUpsertError() {
        final String json = """
                {"id": "123", "script": {"source": "ctx._source.counter += params.count"}, "upsert": true}
                """;

        runner.setProperty(AbstractPutElasticsearch.INDEX_OP, "upsert");
        runner.setProperty(PutElasticsearchRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchRecord.SCRIPT_RECORD_PATH, "/script");
        runner.setProperty(PutElasticsearchRecord.SCRIPTED_UPSERT_RECORD_PATH, "/upsert");

        runner.enqueue(json);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 1);
        final String errorContent = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).getFirst().getContent();
        assertTrue(errorContent.contains("\"id\":\"123\""), errorContent);
        assertTrue(errorContent.contains("\"script\":{"), errorContent);
    }
}
