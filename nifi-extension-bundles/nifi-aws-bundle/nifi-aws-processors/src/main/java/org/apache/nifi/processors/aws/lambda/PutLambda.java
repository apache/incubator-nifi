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
package org.apache.nifi.processors.aws.lambda;

import com.amazonaws.util.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.v2.AbstractAwsSyncProcessor;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.LambdaClientBuilder;
import software.amazon.awssdk.services.lambda.model.InvalidParameterValueException;
import software.amazon.awssdk.services.lambda.model.InvalidRequestContentException;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LogType;
import software.amazon.awssdk.services.lambda.model.RequestTooLargeException;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;
import software.amazon.awssdk.services.lambda.model.UnsupportedMediaTypeException;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "lambda", "put"})
@CapabilityDescription("Sends the contents to a specified Amazon Lambda Function. "
    + "The AWS credentials used for authentication must have permissions execute the Lambda function (lambda:InvokeFunction)."
    + "The FlowFile content must be JSON.")
@WritesAttributes({
    @WritesAttribute(attribute = "aws.lambda.result.function.error", description = "Function error message in result on posting message to AWS Lambda"),
    @WritesAttribute(attribute = "aws.lambda.result.status.code", description = "Status code in the result for the message when posting to AWS Lambda"),
    @WritesAttribute(attribute = "aws.lambda.result.payload", description = "Payload in the result from AWS Lambda"),
    @WritesAttribute(attribute = "aws.lambda.result.log", description = "Log in the result of the message posted to Lambda"),
    @WritesAttribute(attribute = "aws.lambda.exception.message", description = "Exception message on invoking from AWS Lambda"),
    @WritesAttribute(attribute = "aws.lambda.exception.cause", description = "Exception cause on invoking from AWS Lambda"),
    @WritesAttribute(attribute = "aws.lambda.exception.error.code", description = "Exception error code on invoking from AWS Lambda"),
    @WritesAttribute(attribute = "aws.lambda.exception.request.id", description = "Exception request id on invoking from AWS Lambda"),
    @WritesAttribute(attribute = "aws.lambda.exception.status.code", description = "Exception status code on invoking from AWS Lambda")
    })
public class PutLambda extends AbstractAwsSyncProcessor<LambdaClient, LambdaClientBuilder> {

    public static final String AWS_LAMBDA_RESULT_FUNCTION_ERROR = "aws.lambda.result.function.error";
    public static final String AWS_LAMBDA_RESULT_STATUS_CODE = "aws.lambda.result.status.code";
    public static final String AWS_LAMBDA_RESULT_LOG = "aws.lambda.result.log";
    public static final String AWS_LAMBDA_RESULT_PAYLOAD = "aws.lambda.result.payload";
    public static final String AWS_LAMBDA_EXCEPTION_MESSAGE = "aws.lambda.exception.message";
    public static final String AWS_LAMBDA_EXCEPTION_CAUSE = "aws.lambda.exception.cause";
    public static final String AWS_LAMBDA_EXCEPTION_ERROR_CODE = "aws.lambda.exception.error.code";
    public static final String AWS_LAMBDA_EXCEPTION_REQUEST_ID = "aws.lambda.exception.request.id";
    public static final String AWS_LAMBDA_EXCEPTION_STATUS_CODE = "aws.lambda.exception.status.code";
    public static final long MAX_REQUEST_SIZE = 6 * 1000 * 1000;

    static final PropertyDescriptor AWS_LAMBDA_FUNCTION_NAME = new PropertyDescriptor.Builder()
            .name("Amazon Lambda Name")
            .description("The Lambda Function Name")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor AWS_LAMBDA_FUNCTION_QUALIFIER = new PropertyDescriptor.Builder()
            .name("Amazon Lambda Qualifier (version)")
            .description("The Lambda Function Version")
            .defaultValue("$LATEST")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            AWS_LAMBDA_FUNCTION_NAME,
            AWS_LAMBDA_FUNCTION_QUALIFIER,
            REGION,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            TIMEOUT,
            PROXY_CONFIGURATION_SERVICE,
            ENDPOINT_OVERRIDE);

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String functionName = context.getProperty(AWS_LAMBDA_FUNCTION_NAME).getValue();
        final String qualifier = context.getProperty(AWS_LAMBDA_FUNCTION_QUALIFIER).getValue();

        // Max size of message is 6 MB
        if (flowFile.getSize() > MAX_REQUEST_SIZE) {
            getLogger().error("Max size for request body is 6mb but was {} for flow file {} for function {}", flowFile.getSize(), flowFile, functionName);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final LambdaClient client = getClient(context);

        try {
            final InvokeRequest.Builder invokeRequestBuilder = InvokeRequest.builder()
                    .functionName(functionName)
                    .logType(LogType.TAIL)
                    .invocationType(InvocationType.REQUEST_RESPONSE)
                    .qualifier(qualifier);

            session.read(flowFile, in -> invokeRequestBuilder.payload(SdkBytes.fromInputStream(in)));
            final InvokeRequest invokeRequest = invokeRequestBuilder.build();

            final long startTime = System.nanoTime();
            final InvokeResponse response = client.invoke(invokeRequest);

            flowFile = session.putAttribute(flowFile, AWS_LAMBDA_RESULT_STATUS_CODE, response.statusCode().toString());

            final String logResult = response.logResult();
            if (StringUtils.isNotBlank(logResult)) {
                flowFile = session.putAttribute(flowFile, AWS_LAMBDA_RESULT_LOG, new String(Base64.decode(logResult), DEFAULT_CHARSET));
            }

            if (response.payload() != null) {
                flowFile = session.putAttribute(flowFile, AWS_LAMBDA_RESULT_PAYLOAD, response.payload().asString(DEFAULT_CHARSET));
            }

            final String functionError = response.functionError();
            if (StringUtils.isNotBlank(functionError)) {
                flowFile = session.putAttribute(flowFile, AWS_LAMBDA_RESULT_FUNCTION_ERROR, functionError);
                session.transfer(flowFile, REL_FAILURE);
            } else {
                session.transfer(flowFile, REL_SUCCESS);
                final long totalTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                session.getProvenanceReporter().send(flowFile, functionName, totalTimeMillis);
            }
        } catch (final InvalidRequestContentException | InvalidParameterValueException | RequestTooLargeException | ResourceNotFoundException | UnsupportedMediaTypeException unrecoverableException) {
            getLogger().error("Failed to invoke lambda {} with unrecoverable exception {} for flow file {}", functionName, unrecoverableException, flowFile);
            flowFile = populateExceptionAttributes(session, flowFile, unrecoverableException);
            session.transfer(flowFile, REL_FAILURE);
        } catch (final TooManyRequestsException retryableServiceException) {
            getLogger().error("Failed to invoke lambda {} with exception {} for flow file {}, therefore penalizing flowfile", functionName, retryableServiceException, flowFile);
            flowFile = populateExceptionAttributes(session, flowFile, retryableServiceException);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        } catch (final AwsServiceException unrecoverableServiceException) {
            getLogger().error("Failed to invoke lambda {} with exception {} for flow file {} sending to fail", functionName, unrecoverableServiceException, flowFile);
            flowFile = populateExceptionAttributes(session, flowFile, unrecoverableServiceException);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        } catch (final Exception exception) {
            getLogger().error("Failed to invoke lambda {} with exception {} for flow file {}", functionName, exception, flowFile);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    /**
     * Populate exception attributes in the flow file
     * @param session process session
     * @param flowFile the flow file
     * @param exception exception thrown during invocation
     * @return FlowFile the updated flow file
     */
    private FlowFile populateExceptionAttributes(final ProcessSession session, FlowFile flowFile,
            final AwsServiceException exception) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(AWS_LAMBDA_EXCEPTION_MESSAGE, exception.awsErrorDetails().errorMessage());
        attributes.put(AWS_LAMBDA_EXCEPTION_ERROR_CODE, exception.awsErrorDetails().errorCode());
        attributes.put(AWS_LAMBDA_EXCEPTION_REQUEST_ID, exception.requestId());
        attributes.put(AWS_LAMBDA_EXCEPTION_STATUS_CODE, Integer.toString(exception.statusCode()));
        if (exception.getCause() != null) {
            attributes.put(AWS_LAMBDA_EXCEPTION_CAUSE, exception.getCause().getMessage());
        }
        flowFile = session.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

    @Override
    protected LambdaClientBuilder createClientBuilder(final ProcessContext context) {
        return LambdaClient.builder();
    }
}
