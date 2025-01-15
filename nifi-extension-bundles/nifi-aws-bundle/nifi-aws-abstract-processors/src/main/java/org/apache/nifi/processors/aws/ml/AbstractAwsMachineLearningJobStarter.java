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

package org.apache.nifi.processors.aws.ml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.v2.AbstractAwsSyncProcessor;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.SdkClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.flowfile.attributes.CoreAttributes.MIME_TYPE;
import static org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor.TASK_ID;

public abstract class AbstractAwsMachineLearningJobStarter<
        Q extends AwsRequest,
        B extends AwsRequest.Builder,
        R extends AwsResponse,
        T extends SdkClient,
        U extends AwsSyncClientBuilder<U, T> & AwsClientBuilder<U, T>>
        extends AbstractAwsSyncProcessor<T, U> {
    public static final PropertyDescriptor JSON_PAYLOAD = new PropertyDescriptor.Builder()
            .name("json-payload")
            .displayName("JSON Payload")
            .description("JSON request for AWS Machine Learning services. The Processor will use FlowFile content for the request when this property is not specified.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor MANDATORY_AWS_CREDENTIALS_PROVIDER_SERVICE =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(AWS_CREDENTIALS_PROVIDER_SERVICE)
                    .required(true)
                    .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Upon successful completion, the original FlowFile will be routed to this relationship.")
            .autoTerminateDefault(true)
            .build();

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.renameProperty("aws-region", REGION.getName());
    }

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            MANDATORY_AWS_CREDENTIALS_PROVIDER_SERVICE,
            REGION,
            TIMEOUT,
            JSON_PAYLOAD,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE);

    private final static ObjectMapper MAPPER = JsonMapper.builder()
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
            .findAndAddModules()
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_SUCCESS,
            REL_FAILURE
    );

    protected static List<PropertyDescriptor> getCommonPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null && !context.getProperty(JSON_PAYLOAD).isSet()) {
            return;
        }
        final R response;
        FlowFile childFlowFile;
        try {
            response = sendRequest(buildRequest(session, context, flowFile), context, flowFile);
            childFlowFile = writeToFlowFile(session, flowFile, response);
            childFlowFile = postProcessFlowFile(context, session, childFlowFile, response);
            session.transfer(childFlowFile, REL_SUCCESS);
        } catch (final Exception e) {
            if (flowFile != null) {
                session.transfer(flowFile, REL_FAILURE);
            }
            getLogger().error("Sending AWS ML Request failed", e);
            return;
        }
        if (flowFile != null) {
            session.transfer(flowFile, REL_ORIGINAL);
        }

    }

    protected FlowFile postProcessFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final R response) {
        final String awsTaskId = getAwsTaskId(context, response, flowFile);
        FlowFile processedFlowFile = session.putAttribute(flowFile, TASK_ID.getName(), awsTaskId);
        processedFlowFile = session.putAttribute(processedFlowFile, MIME_TYPE.key(), "application/json");
        getLogger().debug("AWS ML Task [{}] started", awsTaskId);
        return processedFlowFile;
    }

    protected Q buildRequest(final ProcessSession session, final ProcessContext context, final FlowFile flowFile) throws JsonProcessingException {
        return (Q) MAPPER.readValue(getPayload(session, context, flowFile), getAwsRequestBuilderClass(context, flowFile)).build();
    }

    protected FlowFile writeToFlowFile(final ProcessSession session, final FlowFile flowFile, final R response) {
        FlowFile childFlowFile = flowFile == null ? session.create() : session.create(flowFile);
        childFlowFile = session.write(childFlowFile, out -> MAPPER.writeValue(out, response.toBuilder()));
        return childFlowFile;
    }

    protected String readFlowFile(final ProcessSession session, final FlowFile flowFile) {
        try (InputStream inputStream = session.read(flowFile)) {
            return new String(IOUtils.toByteArray(inputStream));
        } catch (final IOException e) {
            throw new ProcessException("Read FlowFile Failed", e);
        }
    }

    private String getPayload(final ProcessSession session, final ProcessContext context, final FlowFile flowFile) {
        String payloadPropertyValue = context.getProperty(JSON_PAYLOAD).evaluateAttributeExpressions(flowFile).getValue();
        if (payloadPropertyValue == null) {
            payloadPropertyValue = readFlowFile(session, flowFile);
        }
        return payloadPropertyValue;
    }

    abstract protected R sendRequest(Q request, ProcessContext context, FlowFile flowFile) throws JsonProcessingException;

    abstract protected Class<? extends B> getAwsRequestBuilderClass(ProcessContext context, FlowFile flowFile);

    abstract protected String getAwsTaskId(ProcessContext context, R response, FlowFile flowFile);
}
