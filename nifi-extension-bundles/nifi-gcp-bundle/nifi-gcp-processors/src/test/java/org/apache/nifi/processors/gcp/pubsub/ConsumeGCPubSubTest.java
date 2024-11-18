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
package org.apache.nifi.processors.gcp.pubsub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.processors.gcp.pubsub.consume.OutputStrategy;
import org.apache.nifi.processors.gcp.pubsub.consume.ProcessingStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumeGCPubSubTest {

    private static final String SUBSCRIPTION = "my-subscription";
    private static final String PROJECT = "my-project";
    private static final String SUBSCRIPTION_FULL = "projects/" + PROJECT + "/subscriptions/" + SUBSCRIPTION;

    private SubscriberStub subscriberMock;
    private TestRunner runner;
    private List<ReceivedMessage> messages = new ArrayList<>();
    private ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setRunner() throws InitializationException {
        subscriberMock = mock(SubscriberStub.class);

        UnaryCallable<PullRequest, PullResponse> callable = mock(UnaryCallable.class);
        PullResponse response = mock(PullResponse.class);

        when(subscriberMock.pullCallable()).thenReturn(callable);
        when(callable.call(any())).thenReturn(response);
        when(response.getReceivedMessagesList()).thenReturn(messages);

        UnaryCallable<AcknowledgeRequest, Empty> ackCallable = mock(UnaryCallable.class);
        when(subscriberMock.acknowledgeCallable()).thenReturn(ackCallable);
        when(ackCallable.call(any())).thenReturn(Empty.getDefaultInstance());

        runner = TestRunners.newTestRunner(new ConsumeGCPubSub() {
            @Override
            @OnScheduled
            public void onScheduled(ProcessContext context) {
                subscriber = subscriberMock;

                outputStrategy = context.getProperty(OUTPUT_STRATEGY).asAllowableValue(OutputStrategy.class);
                processingStrategy = context.getProperty(PROCESSING_STRATEGY).asAllowableValue(ProcessingStrategy.class);
                demarcatorValue = context.getProperty(MESSAGE_DEMARCATOR).getValue();
            }
        });

        runner.setProperty(ConsumeGCPubSub.GCP_CREDENTIALS_PROVIDER_SERVICE, getCredentialsServiceId(runner));
        runner.setProperty(ConsumeGCPubSub.PROJECT_ID, PROJECT);
        runner.setProperty(ConsumeGCPubSub.SUBSCRIPTION, SUBSCRIPTION);

        messages.clear();
    }

    @Test
    void testFlowFileStrategy() throws InitializationException {
        messages.add(createMessage("test1"));
        messages.add(createMessage("test2"));
        runner.run(1);
        runner.assertAllFlowFilesTransferred(ConsumeGCPubSub.REL_SUCCESS, 2);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeGCPubSub.REL_SUCCESS).iterator().next();
        flowFile.assertContentEquals("test1");
        flowFile.assertAttributeExists(PubSubAttributes.MESSAGE_ID_ATTRIBUTE);
        flowFile.assertAttributeEquals(PubSubAttributes.SUBSCRIPTION_NAME_ATTRIBUTE, SUBSCRIPTION_FULL);
        flowFile.assertAttributeEquals("attKey", "attValue");
    }

    @Test
    void testDemarcatorStrategy() throws InitializationException {
        runner.setProperty(ConsumeGCPubSub.PROCESSING_STRATEGY, ProcessingStrategy.DEMARCATOR);
        runner.setProperty(ConsumeGCPubSub.MESSAGE_DEMARCATOR, "\n");

        messages.add(createMessage("test1"));
        messages.add(createMessage("test2"));
        runner.run(1);
        runner.assertAllFlowFilesTransferred(ConsumeGCPubSub.REL_SUCCESS, 1);
        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeGCPubSub.REL_SUCCESS).iterator().next();
        flowFile.assertContentEquals("test1\ntest2\n");
        flowFile.assertAttributeNotExists(PubSubAttributes.MESSAGE_ID_ATTRIBUTE);
        flowFile.assertAttributeEquals(PubSubAttributes.SUBSCRIPTION_NAME_ATTRIBUTE, SUBSCRIPTION_FULL);
    }

    @Test
    void testRecordStrategyNoWrapper() throws InitializationException {
        runner.setProperty(ConsumeGCPubSub.PROCESSING_STRATEGY, ProcessingStrategy.RECORD);

        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("json-writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(ConsumeGCPubSub.RECORD_WRITER, "json-writer");

        final JsonTreeReader reader = new JsonTreeReader();
        runner.addControllerService("json-reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(ConsumeGCPubSub.RECORD_READER, "json-reader");

        messages.add(createMessage("{\"foo\":\"foo1\"}"));
        messages.add(createMessage("test2"));
        messages.add(createMessage("{\"foo\":\"foo2\"}"));
        messages.add(createMessage("test3"));
        runner.run(1);

        runner.assertTransferCount(ConsumeGCPubSub.REL_SUCCESS, 1);
        runner.assertTransferCount(ConsumeGCPubSub.REL_PARSE_FAILURE, 2);

        final MockFlowFile flowFileSuccess = runner.getFlowFilesForRelationship(ConsumeGCPubSub.REL_SUCCESS).iterator().next();
        flowFileSuccess.assertContentEquals("[{\"foo\":\"foo1\"},{\"foo\":\"foo2\"}]");
        flowFileSuccess.assertAttributeNotExists(PubSubAttributes.MESSAGE_ID_ATTRIBUTE);
        flowFileSuccess.assertAttributeEquals(PubSubAttributes.SUBSCRIPTION_NAME_ATTRIBUTE, SUBSCRIPTION_FULL);

        final MockFlowFile flowFileParseFailure = runner.getFlowFilesForRelationship(ConsumeGCPubSub.REL_PARSE_FAILURE).iterator().next();
        flowFileParseFailure.assertContentEquals("test2");
        flowFileParseFailure.assertAttributeExists(PubSubAttributes.MESSAGE_ID_ATTRIBUTE);
        flowFileParseFailure.assertAttributeEquals(PubSubAttributes.SUBSCRIPTION_NAME_ATTRIBUTE, SUBSCRIPTION_FULL);
        flowFileParseFailure.assertAttributeEquals("attKey", "attValue");
    }

    @Test
    void testRecordStrategyWithWrapper() throws InitializationException, JsonMappingException, JsonProcessingException {
        runner.setProperty(ConsumeGCPubSub.PROCESSING_STRATEGY, ProcessingStrategy.RECORD);
        runner.setProperty(ConsumeGCPubSub.OUTPUT_STRATEGY, OutputStrategy.USE_WRAPPER);

        final JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("json-writer", writer);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        runner.setProperty(ConsumeGCPubSub.RECORD_WRITER, "json-writer");

        final JsonTreeReader reader = new JsonTreeReader();
        runner.addControllerService("json-reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(ConsumeGCPubSub.RECORD_READER, "json-reader");

        messages.add(createMessage("{\"foo\":\"foo1\"}"));
        messages.add(createMessage("test2"));
        messages.add(createMessage("{\"foo\":\"foo2\"}"));
        messages.add(createMessage("test3"));
        runner.run(1);

        runner.assertTransferCount(ConsumeGCPubSub.REL_SUCCESS, 1);
        runner.assertTransferCount(ConsumeGCPubSub.REL_PARSE_FAILURE, 2);

        final String expected = """
                [ {
                  "metadata" : {
                    "gcp.pubsub.ackId" : "ackId",
                    "gcp.pubsub.messageSize" : 56,
                    "gcp.pubsub.messageId" : "messageId",
                    "gcp.pubsub.attributesCount" : 1,
                    "gcp.pubsub.publishTime" : 0
                  },
                  "attributes" : {
                    "attKey" : "attValue"
                  },
                  "value" : {
                    "foo" : "foo1"
                  }
                }, {
                  "metadata" : {
                    "gcp.pubsub.ackId" : "ackId",
                    "gcp.pubsub.messageSize" : 56,
                    "gcp.pubsub.messageId" : "messageId",
                    "gcp.pubsub.attributesCount" : 1,
                    "gcp.pubsub.publishTime" : 0
                  },
                  "attributes" : {
                    "attKey" : "attValue"
                  },
                  "value" : {
                    "foo" : "foo2"
                  }
                } ]""";

        final MockFlowFile flowFileSuccess = runner.getFlowFilesForRelationship(ConsumeGCPubSub.REL_SUCCESS).iterator().next();
        final String content = flowFileSuccess.getContent();
        assertEquals(mapper.readTree(content), mapper.readTree(expected));
        flowFileSuccess.assertAttributeNotExists(PubSubAttributes.MESSAGE_ID_ATTRIBUTE);
        flowFileSuccess.assertAttributeEquals(PubSubAttributes.SUBSCRIPTION_NAME_ATTRIBUTE, SUBSCRIPTION_FULL);

        final MockFlowFile flowFileParseFailure = runner.getFlowFilesForRelationship(ConsumeGCPubSub.REL_PARSE_FAILURE).iterator().next();
        flowFileParseFailure.assertContentEquals("test2");
        flowFileParseFailure.assertAttributeExists(PubSubAttributes.MESSAGE_ID_ATTRIBUTE);
        flowFileParseFailure.assertAttributeEquals(PubSubAttributes.SUBSCRIPTION_NAME_ATTRIBUTE, SUBSCRIPTION_FULL);
        flowFileParseFailure.assertAttributeEquals("attKey", "attValue");
    }

    private ReceivedMessage createMessage(String content) {
        final byte[] data = content.getBytes();
        return ReceivedMessage.newBuilder()
                .setMessage(PubsubMessage.newBuilder()
                        .setData(ByteString.copyFrom(data))
                        .putAttributes("attKey", "attValue")
                        .setMessageId("messageId")
                        .build())
                .setAckId("ackId")
                .build();
    }

    private static String getCredentialsServiceId(final TestRunner runner) throws InitializationException {
        final ControllerService controllerService = mock(GCPCredentialsControllerService.class);
        final String controllerServiceId = GCPCredentialsControllerService.class.getSimpleName();
        when(controllerService.getIdentifier()).thenReturn(controllerServiceId);
        runner.addControllerService(controllerServiceId, controllerService);
        runner.enableControllerService(controllerService);
        return controllerServiceId;
    }
}
