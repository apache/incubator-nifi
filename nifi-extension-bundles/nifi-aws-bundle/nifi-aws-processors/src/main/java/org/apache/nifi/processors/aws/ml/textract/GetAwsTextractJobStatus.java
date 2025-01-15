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

package org.apache.nifi.processors.aws.ml.textract;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.TextractClientBuilder;
import software.amazon.awssdk.services.textract.model.GetDocumentAnalysisRequest;
import software.amazon.awssdk.services.textract.model.GetDocumentTextDetectionRequest;
import software.amazon.awssdk.services.textract.model.GetExpenseAnalysisRequest;
import software.amazon.awssdk.services.textract.model.JobStatus;
import software.amazon.awssdk.services.textract.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.textract.model.TextractResponse;
import software.amazon.awssdk.services.textract.model.ThrottlingException;

import java.util.List;
import java.util.stream.Stream;

import static org.apache.nifi.processors.aws.ml.textract.StartAwsTextractJob.TEXTRACT_TYPE_ATTRIBUTE;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Textract"})
@CapabilityDescription("Retrieves the current status of an AWS Textract job.")
@SeeAlso({StartAwsTextractJob.class})
public class GetAwsTextractJobStatus extends AbstractAwsMachineLearningJobStatusProcessor<TextractClient, TextractClientBuilder> {

    public static final Validator TEXTRACT_TYPE_VALIDATOR = (subject, value, context) -> {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
            return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
        } else if (TextractType.TEXTRACT_TYPES.contains(value)) {
            return new ValidationResult.Builder().subject(subject).input(value).explanation("Supported Value.").valid(true).build();
        } else {
            return new ValidationResult.Builder().subject(subject).input(value).explanation("Not a supported value, flow file attribute or context parameter.").valid(false).build();
        }
    };

    public static final PropertyDescriptor TEXTRACT_TYPE = new PropertyDescriptor.Builder()
            .name("textract-type")
            .displayName("Textract Type")
            .required(true)
            .description("Supported values: \"Document Analysis\", \"Document Text Detection\", \"Expense Analysis\"")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue(String.format("${%s}", TEXTRACT_TYPE_ATTRIBUTE))
            .addValidator(TEXTRACT_TYPE_VALIDATOR)
            .build();
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(TEXTRACT_TYPE)
    ).toList();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected TextractClientBuilder createClientBuilder(final ProcessContext context) {
        return TextractClient.builder();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final String textractType = context.getProperty(TEXTRACT_TYPE).evaluateAttributeExpressions(flowFile).getValue();

        final String awsTaskId = context.getProperty(TASK_ID).evaluateAttributeExpressions(flowFile).getValue();
        try {
            final JobStatus jobStatus = getTaskStatus(TextractType.fromString(textractType), getClient(context), awsTaskId);
            if (JobStatus.SUCCEEDED == jobStatus) {
                final TextractResponse task = getTask(TextractType.fromString(textractType), getClient(context), awsTaskId);
                flowFile = writeToFlowFile(session, flowFile, task);
                session.transfer(flowFile, REL_SUCCESS);
            } else if (JobStatus.IN_PROGRESS == jobStatus) {
                session.transfer(flowFile, REL_RUNNING);
            } else if (JobStatus.PARTIAL_SUCCESS == jobStatus) {
                session.transfer(flowFile, REL_THROTTLED);
            } else if (JobStatus.FAILED == jobStatus) {
                session.transfer(flowFile, REL_FAILURE);
                getLogger().error("Amazon Textract Task [{}] Failed", awsTaskId);
            } else {
                throw new IllegalStateException("Unrecognized job status");
            }
        } catch (final ThrottlingException | ProvisionedThroughputExceededException e) {
            getLogger().info("Request Rate Limit exceeded", e);
            context.yield();
            session.transfer(flowFile, REL_THROTTLED);
        } catch (final Exception e) {
            getLogger().warn("Failed to get Textract Job status", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private TextractResponse getTask(final TextractType typeOfTextract, final TextractClient client, final String awsTaskId) {
        return switch (typeOfTextract) {
            case DOCUMENT_ANALYSIS -> client.getDocumentAnalysis(GetDocumentAnalysisRequest.builder().jobId(awsTaskId).build());
            case DOCUMENT_TEXT_DETECTION -> client.getDocumentTextDetection(GetDocumentTextDetectionRequest.builder().jobId(awsTaskId).build());
            case EXPENSE_ANALYSIS -> client.getExpenseAnalysis(GetExpenseAnalysisRequest.builder().jobId(awsTaskId).build());
        };
    }

    private JobStatus getTaskStatus(final TextractType typeOfTextract, final TextractClient client, final String awsTaskId) {
        return switch (typeOfTextract) {
            case DOCUMENT_ANALYSIS -> client.getDocumentAnalysis(GetDocumentAnalysisRequest.builder().jobId(awsTaskId).build()).jobStatus();
            case DOCUMENT_TEXT_DETECTION -> client.getDocumentTextDetection(GetDocumentTextDetectionRequest.builder().jobId(awsTaskId).build()).jobStatus();
            case EXPENSE_ANALYSIS -> client.getExpenseAnalysis(GetExpenseAnalysisRequest.builder().jobId(awsTaskId).build()).jobStatus();
        };
    }
}
