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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.StandardFlowFileMediaType;
import org.apache.nifi.util.FlowFileUnpackager;
import org.apache.nifi.util.FlowFileUnpackagerV3;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestPackageFlowFile {

    private static final String SAMPLE_CONTENT = "Hello, World!";
    private static final String SAMPLE_ATTR_FILENAME = "test.txt";
    private static final String SAMPLE_ATTR_MIME_TYPE = "text/plain";
    private static final String EXTRA_ATTR_KEY = "myAttribute";
    private static final String EXTRA_ATTR_VALUE = "my value";

    private final TestRunner runner = TestRunners.newTestRunner(new PackageFlowFile());

    @Test
    public void testOne() throws IOException {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), SAMPLE_ATTR_FILENAME);
        attributes.put(CoreAttributes.MIME_TYPE.key(), SAMPLE_ATTR_MIME_TYPE);
        attributes.put(EXTRA_ATTR_KEY, EXTRA_ATTR_VALUE);

        runner.enqueue(SAMPLE_CONTENT, attributes);
        runner.run();

        runner.assertTransferCount(PackageFlowFile.REL_SUCCESS, 1);
        runner.assertTransferCount(PackageFlowFile.REL_ORIGINAL, 1);
        final MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(PackageFlowFile.REL_SUCCESS).getFirst();

        // mime.type has changed
        Assertions.assertEquals(StandardFlowFileMediaType.VERSION_3.getMediaType(),
                outputFlowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        // content can be unpacked with FlowFileUnpackagerV3
        FlowFileUnpackager unpackager = new FlowFileUnpackagerV3();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(outputFlowFile.toByteArray());
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Map<String, String> unpackedAttributes = unpackager.unpackageFlowFile(bais, baos);
            // verify attributes in package
            Assertions.assertEquals(5, unpackedAttributes.size());
            Assertions.assertNotNull(unpackedAttributes.get(CoreAttributes.UUID.key()));
            Assertions.assertNotNull(unpackedAttributes.get(CoreAttributes.PATH.key()));
            Assertions.assertEquals(SAMPLE_ATTR_FILENAME, unpackedAttributes.get(CoreAttributes.FILENAME.key()));
            Assertions.assertEquals(SAMPLE_ATTR_MIME_TYPE, unpackedAttributes.get(CoreAttributes.MIME_TYPE.key()));
            Assertions.assertEquals(EXTRA_ATTR_VALUE, unpackedAttributes.get(EXTRA_ATTR_KEY));
            // verify content in package
            Assertions.assertArrayEquals(SAMPLE_CONTENT.getBytes(), baos.toByteArray());
        }
    }

    @Test
    public void testMany() throws IOException {
        int FILE_COUNT = 10;
        runner.setProperty(PackageFlowFile.BATCH_SIZE, Integer.toString(FILE_COUNT));
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), SAMPLE_ATTR_MIME_TYPE);

        for (int i = 0; i < FILE_COUNT; i++) {
            attributes.put(CoreAttributes.FILENAME.key(), i + SAMPLE_ATTR_FILENAME);
            runner.enqueue(SAMPLE_CONTENT, attributes);
        }
        runner.run();

        runner.assertTransferCount(PackageFlowFile.REL_SUCCESS, 1);
        runner.assertTransferCount(PackageFlowFile.REL_ORIGINAL, FILE_COUNT);
        final MockFlowFile outputFlowFile = runner.getFlowFilesForRelationship(PackageFlowFile.REL_SUCCESS).getFirst();

        // mime.type has changed
        Assertions.assertEquals(StandardFlowFileMediaType.VERSION_3.getMediaType(),
                outputFlowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        // content can be unpacked with FlowFileUnpackagerV3
        FlowFileUnpackager unpackager = new FlowFileUnpackagerV3();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(outputFlowFile.toByteArray());
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            for (int i = 0; i < FILE_COUNT; i++) {
                Map<String, String> unpackedAttributes = unpackager.unpackageFlowFile(bais, baos);
                // verify attributes in package
                Assertions.assertEquals(4, unpackedAttributes.size());
                Assertions.assertNotNull(unpackedAttributes.get(CoreAttributes.UUID.key()));
                Assertions.assertNotNull(unpackedAttributes.get(CoreAttributes.PATH.key()));
                Assertions.assertEquals(i + SAMPLE_ATTR_FILENAME, unpackedAttributes.get(CoreAttributes.FILENAME.key()));
                Assertions.assertEquals(SAMPLE_ATTR_MIME_TYPE, unpackedAttributes.get(CoreAttributes.MIME_TYPE.key()));
                // verify content in package
                Assertions.assertArrayEquals(SAMPLE_CONTENT.getBytes(), baos.toByteArray());
                baos.reset();
            }
        }
    }

    @Test
    public void testBatchSize() {
        int FILE_COUNT = 10;
        int BATCH_SIZE = 2;
        runner.setProperty(PackageFlowFile.BATCH_SIZE, Integer.toString(BATCH_SIZE));
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), SAMPLE_ATTR_MIME_TYPE);

        for (int i = 0; i < FILE_COUNT; i++) {
            attributes.put(CoreAttributes.FILENAME.key(), i + SAMPLE_ATTR_FILENAME);
            runner.enqueue(SAMPLE_CONTENT, attributes);
        }
        runner.run();

        runner.assertTransferCount(PackageFlowFile.REL_SUCCESS, 1);
        runner.assertTransferCount(PackageFlowFile.REL_ORIGINAL, BATCH_SIZE);
        runner.assertQueueNotEmpty();
    }

    @Test
    public void testBatchContentSize() {
        int FILE_COUNT = 10;
        int BATCH_CONTENT_SIZE = 7 * SAMPLE_CONTENT.length();
        runner.setProperty(PackageFlowFile.BATCH_SIZE, Integer.toString(FILE_COUNT));
        runner.setProperty(PackageFlowFile.BATCH_CONTENT_SIZE, BATCH_CONTENT_SIZE + " B");
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), SAMPLE_ATTR_MIME_TYPE);

        for (int i = 0; i < FILE_COUNT; i++) {
            attributes.put(CoreAttributes.FILENAME.key(), i + SAMPLE_ATTR_FILENAME);
            runner.enqueue(SAMPLE_CONTENT, attributes);
        }
        runner.run();

        runner.assertTransferCount(PackageFlowFile.REL_SUCCESS, 1);
        runner.assertTransferCount(PackageFlowFile.REL_ORIGINAL, 7);
        runner.assertQueueNotEmpty();
    }
}
