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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the GenerateFlowFile processor.
 */
public class TestGenerateFlowFile {

    @Test
    public void testGenerateCustomText() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateFlowFile());
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "100MB");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.CUSTOM_TEXT, "This is my custom text!");

        runner.run();

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 1);
        MockFlowFile generatedFlowFile = runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0);
        generatedFlowFile.assertContentEquals("This is my custom text!");
        generatedFlowFile.assertAttributeNotExists("mime.type");
    }

    @Test
    public void testInvalidCustomText() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateFlowFile());
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "100MB");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_BINARY);
        runner.setProperty(GenerateFlowFile.CUSTOM_TEXT, "This is my custom text!");
        runner.assertNotValid();

        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.UNIQUE_FLOWFILES, "true");
        runner.assertNotValid();
    }

    @Test
    public void testDynamicPropertiesToAttributes() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateFlowFile());
        runner.setProperty(GenerateFlowFile.FILE_SIZE, "1B");
        runner.setProperty(GenerateFlowFile.DATA_FORMAT, GenerateFlowFile.DATA_FORMAT_TEXT);
        runner.setProperty(GenerateFlowFile.MIME_TYPE, "application/text");
        runner.setProperty("plain.dynamic.property", "Plain Value");
        runner.setProperty("expression.dynamic.property", "${literal('Expression Value')}");
        runner.assertValid();

        runner.run();

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 1);
        MockFlowFile generatedFlowFile = runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0);
        generatedFlowFile.assertAttributeEquals("plain.dynamic.property", "Plain Value");
        generatedFlowFile.assertAttributeEquals("expression.dynamic.property", "Expression Value");
        generatedFlowFile.assertAttributeEquals("mime.type", "application/text");
    }

    @Test
    public void testVariableSize() {
        TestRunner runner = TestRunners.newTestRunner(new GenerateFlowFile());
        runner.setProperty(GenerateFlowFile.VARIABLE_SIZE, "true");
        runner.setProperty(GenerateFlowFile.MINIMUM_FILE_SIZE, "1 B");
        runner.setProperty(GenerateFlowFile.MAXIMUM_FILE_SIZE, "10 B");
        runner.setProperty(GenerateFlowFile.UNIQUE_FLOWFILES, "true");
        runner.assertValid();

        // Execute multiple times to ensure adequate distribution of file sizes
        runner.run(1000);

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 1000);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS);
        Map<Long, Integer> fileSizeDistribution = new HashMap<>();
        flowFiles.forEach(ff -> {
            long fileSize = ff.getSize();
            fileSizeDistribution.put(fileSize, fileSizeDistribution.getOrDefault(fileSize, 0) + 1);
        });
        long minSize = fileSizeDistribution.keySet().stream().min(Long::compareTo).orElse(-1L); //.orElseThrow(() -> new NoSuchElementException("Map is empty"));
        long maxSize = fileSizeDistribution.keySet().stream().max(Long::compareTo).orElse(-1L);

        // Assert all file sizes fall within the range and that there exists more than one unique file size value
        Assertions.assertTrue(minSize > 0 && minSize < maxSize);
        Assertions.assertTrue(maxSize <= 10);
    }

}