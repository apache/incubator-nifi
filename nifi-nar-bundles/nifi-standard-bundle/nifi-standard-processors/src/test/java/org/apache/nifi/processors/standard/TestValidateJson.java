/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
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
package org.apache.nifi.processors.standard;

import java.util.List;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestValidateJson {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ValidateJson.class);
    }

    final String testingJson = "{\"FieldOne\":\"stringValue\",\"FieldTwo\":1234,\"FieldThree\":[{\"arrayField\":\"arrayValue\"}]}";
    
    @Test
    public void passSchema() {

        String schema = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"properties\":{\"FieldOne\":{\"type\":\"string\",\"pattern\":\"[a-z]*Value\"},\"FieldTwo\":{\"type\":\"integer\"},\"FieldThree\":{\"type\":\"array\",\"items\":[{\"type\":\"object\",\"properties\":{\"arrayField\":{\"type\":\"string\"}},\"required\":[\"arrayField\"]}]}},\"required\":[\"FieldOne\",\"FieldTwo\",\"FieldThree\"]}";

        testRunner.setProperty(ValidateJson.SCHEMA_TEXT, schema);
        testRunner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.SCHEMA_VERSION_7);

        testRunner.enqueue(testingJson);
        testRunner.run(1);

        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(ValidateJson.REL_FAILURE);
        List<MockFlowFile> valid = testRunner.getFlowFilesForRelationship(ValidateJson.REL_VALID);
        List<MockFlowFile> invalid = testRunner.getFlowFilesForRelationship(ValidateJson.REL_INVALID);

        Assert.assertEquals(0, failure.size());
        Assert.assertEquals(0, invalid.size());
        Assert.assertEquals(1, valid.size());

        Map<String, String> attributes = valid.get(0).getAttributes();
        Assert.assertNull(attributes.get(ValidateJson.ERROR_ATTRIBUTE_KEY));
    }

    @Test
    public void failOnPatternSchemaCheck() {

        String schema = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"properties\":{\"FieldOne\":{\"type\":\"string\",\"pattern\":\"unmatched\"},\"FieldTwo\":{\"type\":\"integer\"},\"FieldThree\":{\"type\":\"array\",\"items\":[{\"type\":\"object\",\"properties\":{\"arrayField\":{\"type\":\"string\"}},\"required\":[\"arrayField\"]}]}},\"required\":[\"FieldOne\",\"FieldTwo\",\"FieldThree\"]}";

        testRunner.setProperty(ValidateJson.SCHEMA_TEXT, schema);
        testRunner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.SCHEMA_VERSION_7);

        testRunner.enqueue(testingJson);
        testRunner.run(1);

        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(ValidateJson.REL_FAILURE);
        List<MockFlowFile> valid = testRunner.getFlowFilesForRelationship(ValidateJson.REL_VALID);
        List<MockFlowFile> invalid = testRunner.getFlowFilesForRelationship(ValidateJson.REL_INVALID);
        
        Assert.assertEquals(0, failure.size());
        Assert.assertEquals(0, valid.size());
        Assert.assertEquals(1, invalid.size());

        Map<String, String> attributes = invalid.get(0).getAttributes();
        Assert.assertEquals("[$.FieldOne: does not match the regex pattern unmatched]", attributes.get(ValidateJson.ERROR_ATTRIBUTE_KEY));
    }

    @Test
    public void failOnMissingRequiredValue() {

        String schema = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"properties\":{\"FieldOne\":{\"type\":\"string\",\"pattern\":\"[a-z]*Value\"},\"FieldTwo\":{\"type\":\"integer\"},\"FieldThree\":{\"type\":\"array\",\"items\":[{\"type\":\"object\",\"properties\":{\"arrayField\":{\"type\":\"string\"}},\"required\":[\"arrayField\"]}]}},\"required\":[\"FieldOne\",\"FieldTwo\",\"FieldThree\",\"FieldFour\"]}";
        
        testRunner.setProperty(ValidateJson.SCHEMA_TEXT, schema);
        testRunner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.SCHEMA_VERSION_7);

        testRunner.enqueue(testingJson);
        testRunner.run(1);

        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(ValidateJson.REL_FAILURE);
        List<MockFlowFile> valid = testRunner.getFlowFilesForRelationship(ValidateJson.REL_VALID);
        List<MockFlowFile> invalid = testRunner.getFlowFilesForRelationship(ValidateJson.REL_INVALID);

        Assert.assertEquals(0, failure.size());
        Assert.assertEquals(0, valid.size());
        Assert.assertEquals(1, invalid.size());

        Map<String, String> attributes = invalid.get(0).getAttributes();
        Assert.assertEquals("[$.FieldFour: is missing but it is required]", attributes.get(ValidateJson.ERROR_ATTRIBUTE_KEY));
    }

    @Test
    public void failOnInvalidJSON() {

        String schema = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"properties\":{\"FieldOne\":{\"type\":\"string\",\"pattern\":\"[a-z]*Value\"},\"FieldTwo\":{\"type\":\"integer\"},\"FieldThree\":{\"type\":\"array\",\"items\":[{\"type\":\"object\",\"properties\":{\"arrayField\":{\"type\":\"string\"}},\"required\":[\"arrayField\"]}]}},\"required\":[\"FieldOne\",\"FieldTwo\",\"FieldThree\",\"FieldFour\"]}";

        testRunner.setProperty(ValidateJson.SCHEMA_TEXT, schema);
        testRunner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.SCHEMA_VERSION_7);

        testRunner.enqueue("This isn't JSON!");
        testRunner.run(1);

        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(ValidateJson.REL_FAILURE);
        List<MockFlowFile> valid = testRunner.getFlowFilesForRelationship(ValidateJson.REL_VALID);
        List<MockFlowFile> invalid = testRunner.getFlowFilesForRelationship(ValidateJson.REL_INVALID);

        Assert.assertEquals(0, valid.size());
        Assert.assertEquals(0, invalid.size());
        Assert.assertEquals(1, failure.size());

    }
}