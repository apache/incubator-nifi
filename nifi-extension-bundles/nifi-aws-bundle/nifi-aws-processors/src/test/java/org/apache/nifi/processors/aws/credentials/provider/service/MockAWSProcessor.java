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
package org.apache.nifi.processors.aws.credentials.provider.service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import java.util.List;

import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_ARN;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_EXTERNAL_ID;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_CUSTOM_SIGNER_CLASS_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_CUSTOM_SIGNER_MODULE_LOCATION;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_ENDPOINT;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_REGION;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.ASSUME_ROLE_STS_SIGNER_OVERRIDE;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.MAX_SESSION_TIME;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.PROFILE_NAME;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.USE_ANONYMOUS_CREDENTIALS;
import static org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService.USE_DEFAULT_CREDENTIALS;


/**
 * Mock Processor implementation used to test CredentialsProviderFactory.
 */
public class MockAWSProcessor extends AbstractAWSCredentialsProviderProcessor<AmazonS3Client> {

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            USE_DEFAULT_CREDENTIALS,
            PROFILE_NAME,
            USE_ANONYMOUS_CREDENTIALS,
            ASSUME_ROLE_ARN,
            ASSUME_ROLE_NAME,
            MAX_SESSION_TIME,
            ASSUME_ROLE_EXTERNAL_ID,
            ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE,
            ASSUME_ROLE_STS_REGION,
            ASSUME_ROLE_STS_ENDPOINT,
            ASSUME_ROLE_STS_SIGNER_OVERRIDE,
            ASSUME_ROLE_STS_CUSTOM_SIGNER_CLASS_NAME,
            ASSUME_ROLE_STS_CUSTOM_SIGNER_MODULE_LOCATION
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

    }

    @Override
    protected AmazonS3Client createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final Region region, final ClientConfiguration config,
                                          final AwsClientBuilder.EndpointConfiguration endpointConfiguration) {
        return null;
    }
}
