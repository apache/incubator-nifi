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
package org.apache.nifi.processors.flume;


import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.AvroSource;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlumeSourceProcessorTest {

    private static final Logger logger = LoggerFactory.getLogger(FlumeSourceProcessorTest.class);

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testValidators() {
        TestRunner runner = TestRunners.newTestRunner(FlumeSourceProcessor.class);
        Collection<ValidationResult> results;
        ProcessContext pc;

        results = new HashSet<>();
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            logger.debug(vr.toString());
            Assert.assertTrue(vr.toString().contains("is invalid because Source Type is required"));
        }

        // non-existent class
        results = new HashSet<>();
        runner.setProperty(FlumeSourceProcessor.SOURCE_TYPE, "invalid.class.name");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            logger.debug(vr.toString());
            Assert.assertTrue(vr.toString().contains("is invalid because unable to load source"));
        }

        // class doesn't implement Source
        results = new HashSet<>();
        runner.setProperty(FlumeSourceProcessor.SOURCE_TYPE, NullSink.class.getName());
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            logger.debug(vr.toString());
            Assert.assertTrue(vr.toString().contains("is invalid because unable to create source"));
        }

        results = new HashSet<>();
        runner.setProperty(FlumeSourceProcessor.SOURCE_TYPE, AvroSource.class.getName());
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(0, results.size());
    }

    @Test
    public void testSequenceSource() {
        TestRunner runner = TestRunners.newTestRunner(FlumeSourceProcessor.class);
        runner.setProperty(FlumeSourceProcessor.SOURCE_TYPE, "seq");
        runner.run();
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(FlumeSourceProcessor.SUCCESS);
        Assert.assertEquals(1, flowFiles.size());
        for (MockFlowFile flowFile : flowFiles) {
            logger.debug(flowFile.toString());
            Assert.assertEquals(1, flowFile.getSize());
        }
    }

    @Test
    public void testSourceWithConfig() throws IOException {
        File spoolDirectory = temp.newFolder("spooldir");
        File dst = new File(spoolDirectory, "records.txt");
        FileUtils.copyFile(getClass().getResourceAsStream("/testdata/records.txt"), dst, true, false);

        TestRunner runner = TestRunners.newTestRunner(FlumeSourceProcessor.class);
        runner.setProperty(FlumeSourceProcessor.SOURCE_TYPE, "spooldir");
        runner.setProperty(FlumeSinkProcessor.FLUME_CONFIG,
            "tier1.sources.src-1.spoolDir = " + spoolDirectory.getAbsolutePath());
        runner.run();
        // No data will be transfered because of how quickly the test runner
        // starts shutting down
        runner.assertTransferCount(FlumeSourceProcessor.SUCCESS, 0);
    }
}
