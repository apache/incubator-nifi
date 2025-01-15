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

import static org.apache.nifi.processors.gcp.util.GoogleUtils.GCP_CREDENTIALS_PROVIDER_SERVICE;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.vision.v1.AsyncBatchAnnotateImagesRequest;

import java.util.List;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"Google", "Cloud", "Machine Learning", "Vision"})
@CapabilityDescription("Trigger a Vision operation on image input. It should be followed by GetGcpVisionAnnotateImagesOperationStatus processor in order to monitor operation status.")
@SeeAlso({GetGcpVisionAnnotateImagesOperationStatus.class})
@WritesAttributes({
        @WritesAttribute(attribute = "operationKey", description = "A unique identifier of the operation returned by the Vision server.")
})
public class StartGcpVisionAnnotateImagesOperation extends AbstractStartGcpVisionOperation<AsyncBatchAnnotateImagesRequest.Builder> {
    static final PropertyDescriptor JSON_PAYLOAD = new PropertyDescriptor.Builder()
            .name("json-payload")
            .displayName("JSON Payload")
            .description("JSON request for AWS Machine Learning services. The Processor will use FlowFile content for the request when this property is not specified.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("""
                    {
                        "requests": [{
                            "image": {
                                "source": {
                                    "imageUri": "gs://${gcs.bucket}/${filename}"
                                }
                            },
                            "features": [{
                                "type": "${vision-feature-type}",
                                "maxResults": 4
                            }]
                        }],
                        "outputConfig": {
                            "gcsDestination": {
                                "uri": "gs://${output-bucket}/${filename}/"
                            },
                            "batchSize": 2
                        }
                    }""")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            JSON_PAYLOAD,
            GCP_CREDENTIALS_PROVIDER_SERVICE,
            OUTPUT_BUCKET,
            FEATURE_TYPE
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    AsyncBatchAnnotateImagesRequest.Builder newBuilder() {
        return AsyncBatchAnnotateImagesRequest.newBuilder();
    }

    @Override
    OperationFuture<?, ?> startOperation(AsyncBatchAnnotateImagesRequest.Builder builder) {
        return getVisionClient().asyncBatchAnnotateImagesAsync(builder.build());
    }

    @Override
    PropertyDescriptor getJsonPayloadPropertyDescriptor() {
        return JSON_PAYLOAD;
    }

}
