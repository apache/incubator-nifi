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
package org.apache.nifi.processors.script;

import org.apache.commons.codec.binary.Hex;

import org.apache.nifi.annotation.lifecycle.OnConfigurationRestored;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.MockValidationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.security.MessageDigestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestInvokeGroovy extends BaseScriptTest {
    @BeforeEach
    public void setup() {
        super.setupInvokeScriptProcessor();
    }

    /**
     * Tests a script that has a Groovy Processor that that reads the first line of text from the flowfiles content and stores the value in an attribute of the outgoing flowfile.
     *
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttribute() {
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/groovy/test_reader.groovy");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred("test", 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("test");
        result.getFirst().assertAttributeEquals("from-content", "test content");
    }

    /**
     * Tests a script that has a Groovy Processor that that reads the first line of text from the flowfiles content and
     * stores the value in an attribute of the outgoing flowfile.
     *
     */
    @Test
    public void testScriptDefinedAttribute() {
        InvokeScriptedProcessor processor = new InvokeScriptedProcessor();
        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);

        processor.initialize(initContext);

        context.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        context.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/groovy/test_reader.groovy");
        context.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/groovy");
        // State Manger is unused, and a null reference is specified
        processor.customValidate(new MockValidationContext(context));
        processor.setup(context);

        List<PropertyDescriptor> descriptors = processor.getSupportedPropertyDescriptors();
        assertNotNull(descriptors);
        assertFalse(descriptors.isEmpty());
        boolean found = false;
        for (PropertyDescriptor descriptor : descriptors) {
            if (descriptor.getName().equals("test-attribute")) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    /**
     * Tests a script that has a Groovy Processor that that reads the first line of text from the flowfiles content and
     * stores the value in an attribute of the outgoing flowfile.
     *
     */
    @Test
    public void testScriptDefinedRelationship() {
        InvokeScriptedProcessor processor = new InvokeScriptedProcessor();
        MockProcessContext context = new MockProcessContext(processor);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context);

        processor.initialize(initContext);

        context.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        context.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/groovy/test_reader.groovy");
        // State Manger is unused, and a null reference is specified
        processor.customValidate(new MockValidationContext(context));
        processor.setup(context);

        Set<Relationship> relationships = processor.getRelationships();
        assertNotNull(relationships);
        assertFalse(relationships.isEmpty());
        boolean found = false;
        for (Relationship relationship : relationships) {
            if (relationship.getName().equals("test")) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    /**
     * Tests a script that throws a ProcessException within. The expected result is that the exception will be
     * propagated
     *
     */
    @Test
    public void testInvokeScriptCausesException() {
        final TestRunner runner = TestRunners.newTestRunner(new OverrideInvokeScriptedProcessor());
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, getFileContentsAsString(
                TEST_RESOURCE_LOCATION + "groovy/testInvokeScriptCausesException.groovy")
        );
        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        assertThrows(AssertionError.class, runner::run);
    }

    /**
     * Tests a script that routes the FlowFile to failure.
     *
     */
    @Test
    public void testScriptRoutesToFailure() {
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, getFileContentsAsString(
                TEST_RESOURCE_LOCATION + "groovy/testScriptRoutesToFailure.groovy")
        );
        runner.assertValid();
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred("FAILURE", 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("FAILURE");
        assertFalse(result.isEmpty());
    }

    @Test
    public void testValidationResultsReset() {
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/groovy/test_reader.groovy");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/groovy");

        runner.assertValid();
        runner.setProperty("test-attribute", "test");
        runner.assertValid();
    }

    /**
     * Tests a script that derive from AbstractProcessor as base class
     *
     */
    @Test
    public void testAbstractProcessorImplementationWithBodyScriptFile() {
        runner.setValidateExpressionUsage(false);
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_BODY, getFileContentsAsString(TEST_RESOURCE_LOCATION + "groovy/test_implementingabstractProcessor.groovy"));
        runner.setProperty(ScriptingComponentUtils.MODULES, TEST_RESOURCE_LOCATION + "groovy");
        runner.setProperty("custom_prop", "bla bla");

        runner.assertValid();
        runner.enqueue("test".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred("success", 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
        assertEquals(1, result.size());
        final String expectedOutput = new String(Hex.encodeHex(MessageDigestUtils.getDigest("testbla bla".getBytes())));
        final MockFlowFile outputFlowFile = result.getFirst();
        outputFlowFile.assertContentEquals(expectedOutput);
        outputFlowFile.assertAttributeEquals("outAttr", expectedOutput);
    }

    /**
     * Tests a script that has a Groovy Processor that reads records and outputs a comma-delimited list of fields selected by a given RecordPath expression
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadRecordsWithRecordPath() throws Exception {
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/groovy/test_record_path.groovy");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/groovy");

        final MockRecordParser readerService = new MockRecordParser();
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.setProperty("record-reader", "reader");
        runner.setProperty("record-path", "/age");
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        readerService.addRecord("John Doe", 48);
        readerService.addRecord("Jane Doe", 47);
        readerService.addRecord("Jimmy Doe", 14);

        runner.assertValid();
        runner.enqueue("".getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred("success", 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship("success");
        assertEquals(1, result.size());
        MockFlowFile ff = result.getFirst();
        ff.assertContentEquals("48\n47\n14\n");
    }

    /**
     * Tests a script that has a Groovy Processor that implements its own onPrimaryNodeStateChange
     *
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testOnPrimaryNodeStateChange() {
        runner.setProperty(scriptingComponent.getScriptingComponentHelper().SCRIPT_ENGINE, "Groovy");
        runner.setProperty(ScriptingComponentUtils.SCRIPT_FILE, "target/test/resources/groovy/test_OnPrimaryStateChange.groovy");
        runner.setProperty(ScriptingComponentUtils.MODULES, "target/test/resources/groovy");
        InvokeScriptedProcessor invokeScriptedProcessor = ((InvokeScriptedProcessor) scriptingComponent);
        invokeScriptedProcessor.setup(runner.getProcessContext());
        runner.setIsConfiguredForClustering(true);
        runner.run(1, false, true);
        runner.setPrimaryNode(true);
        runner.clearTransferState();
        runner.run(1, true, false);
        runner.assertAllFlowFilesTransferred("success");
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship("success");
        assertNotNull(flowFiles);
        assertEquals(1, flowFiles.size());
        MockFlowFile flowFile = flowFiles.get(0);
        flowFile.assertAttributeEquals("isPrimaryNode", "true");
    }

    private static class OverrideInvokeScriptedProcessor extends InvokeScriptedProcessor {

        private int numTimesModifiedCalled = 0;

        @OnConfigurationRestored
        @Override
        public void onConfigurationRestored(ProcessContext context) {
            super.onConfigurationRestored(context);
            assertEquals(this.getSupportedPropertyDescriptors().size(), numTimesModifiedCalled);
        }

        @Override
        public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
            super.onPropertyModified(descriptor, oldValue, newValue);
            numTimesModifiedCalled++;
        }
    }

    private static class OnPrimaryNodeStateChangeMethodWasCalledException extends RuntimeException {

    }
}
