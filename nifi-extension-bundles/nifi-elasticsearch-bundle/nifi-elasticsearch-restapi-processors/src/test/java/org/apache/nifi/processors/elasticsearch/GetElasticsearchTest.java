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

import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GetElasticsearchTest {
    private static final String INDEX_NAME = "messages";
    private static final String CONTROLLER_SERVICE_NAME = "esService";
    private TestRunner runner;

    @BeforeEach
    public void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(new GetElasticsearch());
        final TestElasticsearchClientService service = new TestElasticsearchClientService(false);
        runner.addControllerService(CONTROLLER_SERVICE_NAME, service);
        runner.enableControllerService(service);
        runner.setProperty(GetElasticsearch.CLIENT_SERVICE, CONTROLLER_SERVICE_NAME);
        runner.setProperty(GetElasticsearch.INDEX, INDEX_NAME);
        runner.setProperty(GetElasticsearch.TYPE, "message");
        runner.setProperty(GetElasticsearch.ID, "doc_1");
        runner.setProperty(GetElasticsearch.DESTINATION, GetElasticsearch.FLOWFILE_CONTENT);
        runner.setProperty(GetElasticsearch.ATTRIBUTE_NAME, "elasticsearch.doc");
        runner.setValidateExpressionUsage(true);
    }

    @Test
    void testMandatoryProperties() {
        runner.removeProperty(GetElasticsearch.CLIENT_SERVICE);
        runner.removeProperty(GetElasticsearch.INDEX);
        runner.removeProperty(GetElasticsearch.TYPE);
        runner.removeProperty(GetElasticsearch.ID);
        runner.removeProperty(GetElasticsearch.DESTINATION);
        runner.removeProperty(GetElasticsearch.ATTRIBUTE_NAME);

        final AssertionError assertionError = assertThrows(AssertionError.class, () -> runner.run());
        final String expected = String.format("""
                        Processor has 3 validation failures:
                        '%s' is invalid because %s is required
                        '%s' is invalid because %s is required
                        '%s' is invalid because %s is required
                        """,
                GetElasticsearch.ID.getDisplayName(), GetElasticsearch.ID.getDisplayName(),
                GetElasticsearch.INDEX.getDisplayName(), GetElasticsearch.INDEX.getDisplayName(),
                GetElasticsearch.CLIENT_SERVICE.getDisplayName(), GetElasticsearch.CLIENT_SERVICE.getDisplayName());
        assertEquals(expected, assertionError.getMessage());
    }

    @Test
    void testInvalidProperties() {
        runner.setProperty(GetElasticsearch.CLIENT_SERVICE, "not-a-service");
        runner.setProperty(GetElasticsearch.INDEX, "");
        runner.setProperty(GetElasticsearch.TYPE, "");
        runner.setProperty(GetElasticsearch.ID, "");
        runner.setProperty(GetElasticsearch.DESTINATION, "not-valid");
        runner.setProperty(GetElasticsearch.ATTRIBUTE_NAME, "");

        final AssertionError assertionError = assertThrows(AssertionError.class, () -> runner.run());
        final String expected = String.format("""
                        Processor has 6 validation failures:
                        '%s' validated against '' is invalid because %s cannot be empty
                        '%s' validated against '' is invalid because %s cannot be empty
                        '%s' validated against '' is invalid because %s cannot be empty
                        '%s' validated against 'not-valid' is invalid because Given value not found in allowed set '%s'
                        '%s' validated against 'not-a-service' is invalid because Property references a Controller Service that does not exist
                        '%s' validated against 'not-a-service' is invalid because Invalid Controller Service: not-a-service is not a valid Controller Service Identifier
                        """,
                GetElasticsearch.ID.getName(), GetElasticsearch.ID.getName(),
                GetElasticsearch.INDEX.getName(), GetElasticsearch.INDEX.getName(),
                GetElasticsearch.TYPE.getName(), GetElasticsearch.TYPE.getName(),
                GetElasticsearch.DESTINATION.getName(), String.join(", ", GetElasticsearch.FLOWFILE_CONTENT.getValue(), GetElasticsearch.FLOWFILE_ATTRIBUTE.getValue()),
                GetElasticsearch.CLIENT_SERVICE.getDisplayName(),
                GetElasticsearch.CLIENT_SERVICE.getDisplayName());
        assertEquals(expected, assertionError.getMessage());
    }

    @Test
    void testInvalidAttributeName() {
        runner.setProperty(GetElasticsearch.DESTINATION, GetElasticsearch.FLOWFILE_ATTRIBUTE);
        runner.setProperty(GetElasticsearch.ATTRIBUTE_NAME, "");

        final AssertionError assertionError = assertThrows(AssertionError.class, () -> runner.run());
        final String expected = String.format("""
                        Processor has 1 validation failures:
                        '%s' validated against '' is invalid because %s cannot be empty
                        """,
                GetElasticsearch.ATTRIBUTE_NAME.getName(), GetElasticsearch.ATTRIBUTE_NAME.getName());
        assertEquals(expected, assertionError.getMessage());
    }

    @Test
    void testFetch() {
        runProcessor(runner);
        testCounts(runner, 1, 0, 0);
        final MockFlowFile doc = runner.getFlowFilesForRelationship(GetElasticsearch.REL_DOC).getFirst();
        assertOutputContent(doc.getContent());
        assertEquals(1L, runner.getProvenanceEvents().stream()
                .filter(event -> ProvenanceEventType.RECEIVE.equals(event.getEventType()) && event.getAttribute("uuid").equals(doc.getAttribute("uuid"))).count());
    }

    @Test
    void testInputHandlingDestinationContent() {
        runner.setProperty(GetElasticsearch.DESTINATION, GetElasticsearch.FLOWFILE_CONTENT);
        runner.setIncomingConnection(true);
        runProcessor(runner);
        testCounts(runner, 1, 0, 0);
        MockFlowFile doc = runner.getFlowFilesForRelationship(GetElasticsearch.REL_DOC).getFirst();
        assertOutputContent(doc.getContent());
        assertCommonAttributes(doc);
        assertOutputAttribute(doc, false);

        reset(runner);
        runner.setIncomingConnection(false);
        runner.run();
        testCounts(runner, 1, 0, 0);
        doc = runner.getFlowFilesForRelationship(GetElasticsearch.REL_DOC).getFirst();
        assertOutputContent(doc.getContent());
        assertCommonAttributes(doc);
        assertOutputAttribute(doc, false);
    }

    @Test
    void testDestinationAttribute() {
        runner.setProperty(GetElasticsearch.DESTINATION, GetElasticsearch.FLOWFILE_ATTRIBUTE);
        runner.setIncomingConnection(true);
        runProcessor(runner);
        testCounts(runner, 1, 0, 0);
        MockFlowFile doc = runner.getFlowFilesForRelationship(GetElasticsearch.REL_DOC).getFirst();
        assertEquals("test", doc.getContent());
        assertCommonAttributes(doc);
        assertOutputAttribute(doc, true);

        // non-default attribute name and fetch without type
        reset(runner);
        runner.setProperty(GetElasticsearch.ATTRIBUTE_NAME, "my_attr");
        runner.removeProperty(GetElasticsearch.TYPE);
        runner.setIncomingConnection(false);
        runner.run();
        testCounts(runner, 1, 0, 0);
        doc = runner.getFlowFilesForRelationship(GetElasticsearch.REL_DOC).getFirst();
        assertEquals("", doc.getContent());
        assertCommonAttributes(doc, false);
        assertOutputAttribute(doc, true, "my_attr");
    }

    @Test
    void testErrorDuringFetch() {
        getService(runner).setThrowErrorInGet(true);
        runner.setIncomingConnection(true);
        runProcessor(runner);
        testCounts(runner, 0, 1, 0);

        reset(runner);
        runner.setIncomingConnection(false);
        runner.run();
        testCounts(runner, 0, 0, 0);
    }

    @Test
    void testNotFound() {
        getService(runner).setThrowNotFoundInGet(true);
        runProcessor(runner);
        testCounts(runner, 0, 0, 1);
        reset(runner);
    }

    @Test
    void testRequestParameters() {
        runner.setProperty(GetElasticsearch.ID, "${noAttribute}");
        runProcessor(runner);
        testCounts(runner, 0, 1, 0);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(GetElasticsearch.REL_FAILURE).getFirst();
        failed.assertAttributeEquals("elasticsearch.get.error", GetElasticsearch.ID.getDisplayName() + " is blank (after evaluating attribute expressions), cannot GET document");
        reset(runner);
    }

    @Test
    void testEmptyId() {
        runner.setProperty("refresh", "true");
        runner.setProperty("_source", "${source}");
        runner.setProperty(ElasticsearchRestProcessor.DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER + "Accept", "${accept}");
        runner.setEnvironmentVariableValue("source", "msg");
        runner.setEnvironmentVariableValue("accept", "application/json");
        runProcessor(runner);

        final TestElasticsearchClientService service = getService(runner);
        assertEquals(2, service.getRequestParameters().size());
        assertEquals("true", service.getRequestParameters().get("refresh"));
        assertEquals("msg", service.getRequestParameters().get("_source"));

        assertEquals(1, service.getRequestHeaders().size());
        assertEquals("application/json", service.getRequestHeaders().get("Accept"));
    }

    private static void testCounts(final TestRunner runner, final int doc, final int failure, final int notFound) {
        runner.assertTransferCount(GetElasticsearch.REL_DOC, doc);
        runner.assertTransferCount(GetElasticsearch.REL_FAILURE, failure);
        runner.assertTransferCount(GetElasticsearch.REL_RETRY, 0);
        runner.assertTransferCount(GetElasticsearch.REL_NOT_FOUND, notFound);
    }

    private static void assertOutputContent(final String content) {
        assertEquals(getJsonMsgOneMap(), content);
    }

    private static void assertOutputAttribute(final MockFlowFile doc, final boolean attributeOutput, final String attr) {
        if (attributeOutput) {
            doc.assertAttributeEquals(attr, getJsonMsgOneMap());
        } else {
            doc.assertAttributeNotExists(attr);
        }
    }

    private static String getJsonMsgOneMap() {
        return JsonUtils.toJson(Collections.singletonMap("msg", "one"));
    }

    private static void assertOutputAttribute(final MockFlowFile doc, final boolean attributeOutput) {
        GetElasticsearchTest.assertOutputAttribute(doc, attributeOutput, "elasticsearch.doc");
    }

    private static void assertCommonAttributes(final MockFlowFile doc, final boolean type) {
        doc.assertAttributeEquals("filename", "doc_1");
        doc.assertAttributeEquals("elasticsearch.index", INDEX_NAME);
        if (type) {
            doc.assertAttributeEquals("elasticsearch.type", "message");
        } else {
            doc.assertAttributeNotExists("elasticsearch.type");
        }

        doc.assertAttributeNotExists("elasticsearch.get.error");
    }

    private static void assertCommonAttributes(final MockFlowFile doc) {
        assertCommonAttributes(doc, true);
    }

    private static void runProcessor(final TestRunner runner) {
        runner.enqueue("test");
        runner.run();
    }

    private static TestElasticsearchClientService getService(final TestRunner runner) {
        return runner.getControllerService(CONTROLLER_SERVICE_NAME, TestElasticsearchClientService.class);
    }

    private static void reset(final TestRunner runner) {
        runner.clearProvenanceEvents();
        runner.clearTransferState();
    }
}
