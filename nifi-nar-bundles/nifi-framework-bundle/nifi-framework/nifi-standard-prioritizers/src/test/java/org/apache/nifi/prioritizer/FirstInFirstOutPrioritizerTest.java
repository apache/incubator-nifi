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
package org.apache.nifi.prioritizer;

import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("EqualsWithItself")
public class FirstInFirstOutPrioritizerTest {

    private final TestRunner testRunner = TestRunners.newTestRunner(NoOpProcessor.class);
    private final FlowFilePrioritizer prioritizer = new FirstInFirstOutPrioritizer();

    @Test
    public void testPrioritizer() {
        MockFlowFile flowFile1 = testRunner.enqueue("created first but 'enqueued' later");
        flowFile1.setLastEnqueuedDate(830822400000L);
        MockFlowFile flowFile2 = testRunner.enqueue("created second but 'enqueued' earlier");
        flowFile2.setLastEnqueuedDate(795916800000L);

        assertEquals(0, prioritizer.compare(null, null));
        assertEquals(-1, prioritizer.compare(flowFile1, null));
        assertEquals(1, prioritizer.compare(null, flowFile1));
        assertEquals(0, prioritizer.compare(flowFile1, flowFile1));
        assertEquals(0, prioritizer.compare(flowFile2, flowFile2));
        assertEquals(1, prioritizer.compare(flowFile1, flowFile2));
        assertEquals(-1, prioritizer.compare(flowFile2, flowFile1));
    }
}
