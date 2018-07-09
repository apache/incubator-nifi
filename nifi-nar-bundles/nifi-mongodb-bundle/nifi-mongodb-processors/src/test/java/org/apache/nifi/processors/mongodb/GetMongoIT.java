/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class GetMongoIT {
    private static final String MONGO_URI = "mongodb://localhost";
    private static final String DB_NAME = GetMongoIT.class.getSimpleName().toLowerCase();
    private static final String COLLECTION_NAME = "test";

    private static final List<Document> DOCUMENTS;
    private static final Calendar CAL;

    static {
        CAL = Calendar.getInstance();
        DOCUMENTS = Lists.newArrayList(
            new Document("_id", "doc_1").append("a", 1).append("b", 2).append("c", 3),
            new Document("_id", "doc_2").append("a", 1).append("b", 2).append("c", 4).append("date_field", CAL.getTime()),
            new Document("_id", "doc_3").append("a", 1).append("b", 3)
        );
    }

    private TestRunner runner;
    private MongoClient mongoClient;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(TestGetMongo.class);
        runner.setVariable("uri", MONGO_URI);
        runner.setVariable("db", DB_NAME);
        runner.setVariable("collection", COLLECTION_NAME);
        runner.setProperty(AbstractMongoProcessor.URI, "${uri}");
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, "${db}");
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, "${collection}");
        runner.setProperty(GetMongo.USE_PRETTY_PRINTING, GetMongo.GM_TRUE);
        runner.setIncomingConnection(false);

        mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));

        MongoCollection<Document> collection = mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
        collection.insertMany(DOCUMENTS);

        TestGetMongo.setThrowError(false);
    }

    @After
    public void teardown() {
        runner = null;

        mongoClient.getDatabase(DB_NAME).drop();
    }

    @Test
    public void testValidators() {

        TestRunner runner = TestRunners.newTestRunner(GetMongo.class);
        Collection<ValidationResult> results;
        ProcessContext pc;

        // missing uri, db, collection
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(3, results.size());
        Iterator<ValidationResult> it = results.iterator();
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo URI is required"));
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo Database Name is required"));
        Assert.assertTrue(it.next().toString().contains("is invalid because Mongo Collection Name is required"));

        // missing query - is ok
        runner.setProperty(AbstractMongoProcessor.URI, MONGO_URI);
        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, DB_NAME);
        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, COLLECTION_NAME);
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(0, results.size());

        // invalid query
        runner.setProperty(GetMongo.QUERY, "{a: x,y,z}");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.iterator().next().toString().contains("is invalid because"));

        // invalid projection
        runner.setVariable("projection", "{a: x,y,z}");
        runner.setProperty(GetMongo.QUERY, "{\"a\": 1}");
        runner.setProperty(GetMongo.PROJECTION, "{\"a\": z");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.iterator().next().toString().contains("is invalid"));

        // invalid sort
        runner.removeProperty(GetMongo.PROJECTION);
        runner.setProperty(GetMongo.SORT, "{a: x,y,z}");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        results = new HashSet<>();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.iterator().next().toString().contains("is invalid"));
    }

    @Test
    public void testCleanJson() throws Exception {
        runner.setVariable("query", "{\"_id\": \"doc_2\"}");
        runner.setProperty(GetMongo.QUERY, "${query}");
        runner.setProperty(GetMongo.JSON_TYPE, GetMongo.JSON_STANDARD);
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        byte[] raw = runner.getContentAsByteArray(flowFiles.get(0));
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> parsed = mapper.readValue(raw, Map.class);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        Assert.assertTrue(parsed.get("date_field").getClass() == String.class);
        Assert.assertTrue(((String)parsed.get("date_field")).startsWith(format.format(CAL.getTime())));
    }

    @Test
    public void testReadOneDocument() throws Exception {
        runner.setVariable("query", "{a: 1, b: 3}");
        runner.setProperty(GetMongo.QUERY, "${query}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        flowFiles.get(0).assertContentEquals(DOCUMENTS.get(2).toJson());
    }

    @Test
    public void testReadMultipleDocuments() throws Exception {
        runner.setProperty(GetMongo.QUERY, "{\"a\": {\"$exists\": true}}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        for (int i=0; i < flowFiles.size(); i++) {
            flowFiles.get(i).assertContentEquals(DOCUMENTS.get(i).toJson());
        }
    }

    @Test
    public void testProjection() throws Exception {
        runner.setProperty(GetMongo.QUERY, "{\"a\": 1, \"b\": 3}");
        runner.setProperty(GetMongo.PROJECTION, "{\"_id\": 0, \"a\": 1}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        Document expected = new Document("a", 1);
        flowFiles.get(0).assertContentEquals(expected.toJson());
    }

    @Test
    public void testSort() throws Exception {
        runner.setVariable("sort", "{\"a\": -1, \"b\": -1, \"c\": 1}");
        runner.setProperty(GetMongo.QUERY, "{\"a\": {\"$exists\": true}}");
        runner.setProperty(GetMongo.SORT, "${sort}");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        flowFiles.get(0).assertContentEquals(DOCUMENTS.get(2).toJson());
        flowFiles.get(1).assertContentEquals(DOCUMENTS.get(0).toJson());
        flowFiles.get(2).assertContentEquals(DOCUMENTS.get(1).toJson());
    }

    @Test
    public void testLimit() throws Exception {
        runner.setProperty(GetMongo.QUERY, "{\"a\": {\"$exists\": true}}");
        runner.setProperty(GetMongo.LIMIT, "${limit}");
        runner.setVariable("limit", "1");
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        flowFiles.get(0).assertContentEquals(DOCUMENTS.get(0).toJson());
    }

    @Test
    public void testResultsPerFlowfile() throws Exception {
        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "${results.per.flowfile}");
        runner.setVariable("results.per.flowfile", "2");
        runner.enqueue("{}");
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 2);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        Assert.assertTrue("Flowfile was empty", results.get(0).getSize() > 0);
        Assert.assertEquals("Wrong mime type", results.get(0).getAttribute(CoreAttributes.MIME_TYPE.key()), "application/json");

        runner.clearTransferState();
        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "1");
        runner.enqueue("{}");
        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS);
        results = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        for (MockFlowFile flowFile : results) {
            byte[] raw = runner.getContentAsByteArray(flowFile);
            String json = new String(raw);
            Assert.assertNotNull("JSON was null", json);
            Assert.assertTrue("Did not start with open object character.", json.startsWith("{"));
        }
    }

    @Test
    public void testBatchSize() throws Exception {
        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "2");
        runner.setProperty(GetMongo.BATCH_SIZE, "${batch.size}");
        runner.setVariable("batch.size", "1");
        runner.enqueue("{}");
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 2);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        Assert.assertTrue("Flowfile was empty", results.get(0).getSize() > 0);
        Assert.assertEquals("Wrong mime type", results.get(0).getAttribute(CoreAttributes.MIME_TYPE.key()), "application/json");
    }

    @Test
    public void testConfigurablePrettyPrint() {
        runner.setProperty(GetMongo.JSON_TYPE, GetMongo.JSON_STANDARD);
        runner.setProperty(GetMongo.LIMIT, "1");
        runner.enqueue("{}");
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        byte[] raw = runner.getContentAsByteArray(flowFiles.get(0));
        String json = new String(raw);
        Assert.assertTrue("JSON did not have new lines.", json.contains("\n"));
        runner.clearTransferState();
        runner.setProperty(GetMongo.USE_PRETTY_PRINTING, GetMongo.GM_FALSE);
        runner.enqueue("{}");

        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        raw = runner.getContentAsByteArray(flowFiles.get(0));
        json = new String(raw);
        Assert.assertFalse("New lines detected", json.contains("\n"));
    }

    private void testQueryAttribute(String attr, String expected) {
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        for (MockFlowFile mff : flowFiles) {
            String val = mff.getAttribute(attr);
            Assert.assertNotNull("Missing query attribute", val);
            Assert.assertEquals("Value was wrong", expected, val);
        }
    }

    @Test
    public void testQueryAttribute() {
        /*
         * Test original behavior; Manually set query of {}, no input
         */
        final String attr = "query.attr";
        runner.setProperty(GetMongo.QUERY, "{}");
        runner.setProperty(GetMongo.QUERY_ATTRIBUTE, attr);
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 3);
        testQueryAttribute(attr, "{}");

        runner.clearTransferState();

        /*
         * Test original behavior; No Input/Empty val = {}
         */
        runner.removeProperty(GetMongo.QUERY);
        runner.setIncomingConnection(false);
        runner.run();
        testQueryAttribute(attr, "{}");

        runner.clearTransferState();

        /*
         * Input flowfile with {} as the query
         */

        runner.setIncomingConnection(true);
        runner.enqueue("{}");
        runner.run();
        testQueryAttribute(attr, "{}");

        /*
         * Input flowfile with invalid query
         */

        runner.clearTransferState();
        runner.enqueue("invalid query");
        runner.run();

        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetMongo.REL_FAILURE, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 0);
    }

    /*
     * Query read behavior tests
     */
    @Test
    public void testReadQueryFromBodyWithEL() {
        Map attributes = new HashMap();
        attributes.put("field", "c");
        attributes.put("value", "4");
        String query = "{ \"${field}\": { \"$gte\": ${value}}}";
        runner.setIncomingConnection(true);
        runner.setProperty(GetMongo.QUERY, query);
        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "10");
        runner.enqueue("test", attributes);
        runner.run(1, true, true);

        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
    }

    @Test
    public void testReadQueryFromBodyNoEL() {
        String query = "{ \"c\": { \"$gte\": 4 }}";
        runner.setIncomingConnection(true);
        runner.removeProperty(GetMongo.QUERY);
        runner.enqueue(query);
        runner.run(1, true, true);

        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);

    }

    @Test
    public void testReadQueryFromQueryParamNoConnection() {
        String query = "{ \"c\": { \"$gte\": 4 }}";
        runner.setProperty(GetMongo.QUERY, query);
        runner.setIncomingConnection(false);
        runner.run(1, true, true);
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);

    }

    @Test
    public void testReadQueryFromQueryParamWithConnection() {
        String query = "{ \"c\": { \"$gte\": ${value} }}";
        Map<String, String> attrs = new HashMap<>();
        attrs.put("value", "4");

        runner.setProperty(GetMongo.QUERY, query);
        runner.setIncomingConnection(true);
        runner.enqueue("test", attrs);
        runner.run(1, true, true);
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
    }

    @Test
    public void testQueryParamMissingWithNoFlowfile() {
        Exception ex = null;

        try {
            runner.assertValid();
            runner.setIncomingConnection(false);
            runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "1");
            runner.run(1, true, true);
        } catch (Exception pe) {
            ex = pe;
        }

        Assert.assertNull("An exception was thrown!", ex);
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 0);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 3);
    }

    @Test
    public void testReadCharsetWithEL() {
        String query = "{ \"c\": { \"$gte\": 4 }}";
        Map<String, String> attrs = new HashMap<>();
        attrs.put("charset", "UTF-8");

        runner.setProperty(GetMongo.CHARSET, "${charset}");
        runner.setProperty(GetMongo.BATCH_SIZE, "2");
        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "2");

        runner.setIncomingConnection(true);
        runner.enqueue(query, attrs);
        runner.run(1, true, true);
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
    }

    @Test
    public void testKeepOriginalAttributes() {
        final String query = "{ \"c\": { \"$gte\": 4 }}";
        final Map<String, String> attributesMap = new HashMap<>(1);
        attributesMap.put("property.1", "value-1");

        runner.setIncomingConnection(true);
        runner.removeProperty(GetMongo.QUERY);
        runner.enqueue(query, attributesMap);

        runner.run(1, true, true);

        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS).get(0);
        Assert.assertTrue(flowFile.getAttributes().containsKey("property.1"));
        flowFile.assertAttributeEquals("property.1", "value-1");
    }
    /*
     * End query read behavior tests
     */
    @Test
    public void testEstimates() {
        String keyBase = "est_%s";
        MongoCollection<Document> collection = mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
        collection.deleteMany(Document.parse("{}"));
        for (int x = 0; x < 1000; x++) {
            collection.insertOne(new Document("_id", String.format(keyBase, x)).append("val", UUID.randomUUID()));
        }

        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "100");
        runner.setProperty(GetMongo.QUERY, "{}");
        runner.setProperty(GetMongo.ESTIMATE_PROGRESS, GetMongo.GM_TRUE);
        runner.run();
        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 10);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);
        for (int x = 1; x <= flowFiles.size(); x++) {
            MockFlowFile flowFile = flowFiles.get(x - 1);
            String startAttr = flowFile.getAttribute(GetMongo.PROGRESS_START);
            String endAttr   = flowFile.getAttribute(GetMongo.PROGRESS_END);
            String estimate  = flowFile.getAttribute(GetMongo.PROGRESS_ESTIMATE);
            Assert.assertNotNull("Missing start attribute", startAttr);
            Assert.assertNotNull("Missing end attribute", endAttr);
            Assert.assertNotNull("Missing estimate attribute", estimate);

            Integer start = (x - 1) * 100;
            Integer end   = x * 100;

            Assert.assertEquals("Start didn't match", start, Integer.valueOf(startAttr));
            Assert.assertEquals("End didn't match", end, Integer.valueOf(endAttr));
            Assert.assertEquals("Progress didn't match", Integer.valueOf(1000), Integer.valueOf(estimate));
        }

        runner.clearTransferState();
        runner.removeProperty(GetMongo.RESULTS_PER_FLOWFILE);
        runner.run();

        runner.assertAllFlowFilesTransferred(GetMongo.REL_SUCCESS);
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1000);

        flowFiles = runner.getFlowFilesForRelationship(GetMongo.REL_SUCCESS);

        for (int x = 1; x <= flowFiles.size(); x++) {
            MockFlowFile mockFlowFile = flowFiles.get(x - 1);
            String estimateProgress = mockFlowFile.getAttribute(GetMongo.PROGRESS_INDEX);
            String estimateTotal    = mockFlowFile.getAttribute(GetMongo.PROGRESS_ESTIMATE);
            Assert.assertNotNull("Missing estimate", estimateProgress);
            Assert.assertNotNull("Missing total", estimateTotal);
            Assert.assertEquals(String.format("Wrong total: %s", estimateTotal), estimateTotal, "1000");
        }
    }

    @Test
    public void testProgressiveCommits() {
        String keyBase = "est_%s";
        MongoCollection<Document> collection = mongoClient.getDatabase(DB_NAME).getCollection(COLLECTION_NAME);
        collection.deleteMany(Document.parse("{}"));
        for (int x = 0; x < 1000; x++) {
            collection.insertOne(new Document("_id", String.format(keyBase, x)).append("val", UUID.randomUUID()));
        }

        AllowableValue[] values = new AllowableValue[] { GetMongo.MODE_ONE_COMMIT, GetMongo.MODE_MANY_COMMITS };
        for (AllowableValue value : values) {
            runner.setProperty(GetMongo.OPERATION_MODE, value);
            runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "100");
            runner.setProperty(GetMongo.QUERY, "{}");
            runner.setProperty(GetMongo.ESTIMATE_PROGRESS, GetMongo.GM_TRUE);
            runner.setIncomingConnection(true);
            runner.enqueue("test");
            runner.run();
            runner.assertTransferCount(GetMongo.REL_SUCCESS, 10);
            runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
            runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);

            runner.clearTransferState();
        }

        //Test errors
        TestGetMongo.setThrowError(true);
        runner.enqueue("test");
        runner.run();
        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1);
        runner.assertTransferCount(GetMongo.REL_FAILURE, 1);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
        runner.clearTransferState();

        TestGetMongo.setThrowError(false);

        //Test one flowfile/result
        runner.setProperty(GetMongo.RESULTS_PER_FLOWFILE, "1");
        runner.removeProperty(GetMongo.QUERY);
        runner.enqueue("{}");
        runner.run();

        runner.assertTransferCount(GetMongo.REL_SUCCESS, 1000);
        runner.assertTransferCount(GetMongo.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongo.REL_ORIGINAL, 1);
    }
}
