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

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStarter;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.TextractClientBuilder;
import software.amazon.awssdk.services.textract.model.StartDocumentAnalysisRequest;
import software.amazon.awssdk.services.textract.model.StartDocumentAnalysisResponse;
import software.amazon.awssdk.services.textract.model.StartDocumentTextDetectionRequest;
import software.amazon.awssdk.services.textract.model.StartDocumentTextDetectionResponse;
import software.amazon.awssdk.services.textract.model.StartExpenseAnalysisRequest;
import software.amazon.awssdk.services.textract.model.StartExpenseAnalysisResponse;
import software.amazon.awssdk.services.textract.model.TextractRequest;
import software.amazon.awssdk.services.textract.model.TextractResponse;

import java.util.List;
import java.util.stream.Stream;

import static org.apache.nifi.processors.aws.ml.textract.TextractType.DOCUMENT_ANALYSIS;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Textract"})
@CapabilityDescription("Trigger a AWS Textract job. It should be followed by GetAwsTextractJobStatus processor in order to monitor job status.")
@WritesAttributes({
        @WritesAttribute(attribute = "awsTaskId", description = "The task ID that can be used to poll for Job completion in GetAwsTextractJobStatus"),
        @WritesAttribute(attribute = "awsTextractType", description = "The selected Textract type, which can be used in GetAwsTextractJobStatus")
})
@SeeAlso({GetAwsTextractJobStatus.class})
public class StartAwsTextractJob extends AbstractAwsMachineLearningJobStarter<
        TextractRequest, TextractRequest.Builder, TextractResponse, TextractClient, TextractClientBuilder> {

    public static final String TEXTRACT_TYPE_ATTRIBUTE = "awsTextractType";

    public static final PropertyDescriptor TEXTRACT_TYPE = new PropertyDescriptor.Builder()
            .name("textract-type")
            .displayName("Textract Type")
            .required(true)
            .description("Supported values: \"Document Analysis\", \"Document Text Detection\", \"Expense Analysis\"")
            .allowableValues(TextractType.TEXTRACT_TYPES)
            .defaultValue(DOCUMENT_ANALYSIS.getType())
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
    protected TextractResponse sendRequest(final TextractRequest request, final ProcessContext context, final FlowFile flowFile) {
        TextractType textractType = TextractType.fromString(context.getProperty(TEXTRACT_TYPE.getName()).getValue());
        return switch (textractType) {
            case DOCUMENT_ANALYSIS -> getClient(context).startDocumentAnalysis((StartDocumentAnalysisRequest) request);
            case DOCUMENT_TEXT_DETECTION -> getClient(context).startDocumentTextDetection((StartDocumentTextDetectionRequest) request);
            case EXPENSE_ANALYSIS -> getClient(context).startExpenseAnalysis((StartExpenseAnalysisRequest) request);
        };
    }

    @Override
    protected Class<? extends TextractRequest.Builder> getAwsRequestBuilderClass(final ProcessContext context, final FlowFile flowFile) {
        final TextractType typeOfTextract = TextractType.fromString(context.getProperty(TEXTRACT_TYPE.getName()).getValue());
        return switch (typeOfTextract) {
            case DOCUMENT_ANALYSIS -> StartDocumentAnalysisRequest.serializableBuilderClass();
            case DOCUMENT_TEXT_DETECTION -> StartDocumentTextDetectionRequest.serializableBuilderClass();
            case EXPENSE_ANALYSIS -> StartExpenseAnalysisRequest.serializableBuilderClass();
        };
    }

    @Override
    protected String getAwsTaskId(final ProcessContext context, final TextractResponse textractResponse, final FlowFile flowFile) {
        final TextractType textractType = TextractType.fromString(context.getProperty(TEXTRACT_TYPE.getName()).getValue());
        return switch (textractType) {
            case DOCUMENT_ANALYSIS -> ((StartDocumentAnalysisResponse) textractResponse).jobId();
            case DOCUMENT_TEXT_DETECTION -> ((StartDocumentTextDetectionResponse) textractResponse).jobId();
            case EXPENSE_ANALYSIS -> ((StartExpenseAnalysisResponse) textractResponse).jobId();
        };
    }

    @Override
    protected FlowFile postProcessFlowFile(final ProcessContext context, final ProcessSession session, FlowFile flowFile, final TextractResponse response) {
        flowFile = super.postProcessFlowFile(context, session, flowFile, response);
        return session.putAttribute(flowFile, TEXTRACT_TYPE_ATTRIBUTE, context.getProperty(TEXTRACT_TYPE).getValue());
    }
}
