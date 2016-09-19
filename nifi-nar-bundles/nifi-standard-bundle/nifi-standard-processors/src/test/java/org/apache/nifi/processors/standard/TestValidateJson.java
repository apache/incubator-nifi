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

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;
import org.xml.sax.SAXException;

public class TestValidateJson {

    @Test
    public void testValidJsonArray() throws IOException, SAXException {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateJson());
        runner.setProperty(ValidateJson.SCHEMA_FILE, "src/test/resources/TestJson/json-sample-schema.json");

        runner.enqueue(Paths.get("src/test/resources/TestJson/json-sample.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_VALID, 1);
    }

    @Test
    public void testValidJsonObject() throws IOException, SAXException {
        final TestRunner runner = TestRunners.newTestRunner(new ValidateJson());
        runner.setProperty(ValidateJson.SCHEMA_FILE, "src/test/resources/TestJson/json-object-sample-schema.json");

        runner.enqueue(Paths.get("src/test/resources/TestJson/json-object-sample.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_VALID, 1);
    }

}
