/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.processors.gcp.vision;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;

import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.Status;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

abstract public class AbstractGetGcpVisionAnnotateOperationStatus extends AbstractGcpVisionProcessor {
    public static final PropertyDescriptor OPERATION_KEY = new PropertyDescriptor.Builder()
            .name("operationKey")
            .displayName("GCP Operation Key")
            .description("The unique identifier of the Vision operation.")
            .defaultValue("${operationKey}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .build();
    public static final Relationship REL_RUNNING = new Relationship.Builder()
            .name("running")
            .description("The job is currently still being processed")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Upon successful completion, the original FlowFile will be routed to this relationship.")
            .autoTerminateDefault(true)
            .build();
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(OPERATION_KEY)
    ).toList();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_SUCCESS,
            REL_FAILURE,
            REL_RUNNING
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            String operationKey = context.getProperty(OPERATION_KEY).evaluateAttributeExpressions(flowFile).getValue();
            Operation operation = getVisionClient().getOperationsClient().getOperation(operationKey);
            getLogger().info("{}", operation);
            if (operation.getDone() && !operation.hasError()) {
                GeneratedMessageV3 response = deserializeResponse(operation.getResponse().getValue());
                FlowFile childFlowFile = session.create(flowFile);
                session.write(childFlowFile, out -> out.write(JsonFormat.printer().print(response).getBytes(StandardCharsets.UTF_8)));
                session.putAttribute(childFlowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
                session.transfer(flowFile, REL_ORIGINAL);
                session.transfer(childFlowFile, REL_SUCCESS);
            } else if (!operation.getDone()) {
                session.transfer(flowFile, REL_RUNNING);
            } else {
                Status error = operation.getError();
                getLogger().error("Failed to execute vision operation. Error code: {}, Error message: {}", error.getCode(), error.getMessage());
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (Exception e) {
            getLogger().error("Fail to get GCP Vision operation's status", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    abstract protected GeneratedMessageV3 deserializeResponse(ByteString responseValue) throws InvalidProtocolBufferException;
}
