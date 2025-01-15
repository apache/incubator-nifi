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
package org.apache.nifi.processors.aws.s3.service;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.fileresource.service.api.FileResourceService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.processors.aws.s3.AbstractS3Processor;
import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.processors.aws.util.RegionUtilV1;

import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.aws.util.RegionUtilV1.resolveS3Region;
import static org.apache.nifi.util.StringUtils.isBlank;

@Tags({"Amazon", "S3", "AWS", "file", "resource"})
@SeeAlso({FetchS3Object.class})
@CapabilityDescription("Provides an Amazon Web Services (AWS) S3 file resource for other components.")
@UseCase(
        description = "Fetch a specific file from S3. " +
                "The service provides higher performance compared to fetch processors when the data should be moved between different storages without any transformation.",
        configuration = """
                "Bucket" = "${s3.bucket}"
                "Object Key" = "${filename}"

                The "Region" property must be set to denote the S3 region that the Bucket resides in.

                The "AWS Credentials Provider Service" property should specify an instance of the AWSCredentialsProviderService in order to provide credentials for accessing the bucket.
                """
)
public class S3FileResourceService extends AbstractControllerService implements FileResourceService {

    public static final PropertyDescriptor BUCKET_WITH_DEFAULT_VALUE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractS3Processor.BUCKET_WITH_DEFAULT_VALUE)
            .build();

    public static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractS3Processor.KEY)
            .build();

    public static final PropertyDescriptor S3_REGION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(RegionUtilV1.S3_REGION)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BUCKET_WITH_DEFAULT_VALUE,
            KEY,
            S3_REGION,
            AWS_CREDENTIALS_PROVIDER_SERVICE);

    private final Cache<Region, AmazonS3> clientCache = Caffeine.newBuilder().build();

    private volatile PropertyContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.context = context;
    }

    @OnDisabled
    public void onDisabled() {
        this.context = null;
        clientCache.asMap().values().forEach(AmazonS3::shutdown);
        clientCache.invalidateAll();
        clientCache.cleanUp();
    }

    @Override
    public FileResource getFileResource(Map<String, String> attributes) {
        final AWSCredentialsProviderService awsCredentialsProviderService = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(AWSCredentialsProviderService.class);
        final AmazonS3 client = getS3Client(attributes, awsCredentialsProviderService.getCredentialsProvider());

        try {
            return fetchObject(client, attributes);
        } catch (final ProcessException | SdkClientException e) {
            throw new ProcessException("Failed to fetch s3 object", e);
        }
    }

    /**
     * Fetches s3 object from the provided bucket and returns it as FileResource
     *
     * @param client amazon s3 client
     * @param attributes configuration attributes
     * @return fetched s3 object as FileResource
     * @throws ProcessException if the object 'bucketName/key' does not exist
     */
    private FileResource fetchObject(final AmazonS3 client, final Map<String, String> attributes) throws ProcessException,
            SdkClientException {
        final String bucketName = context.getProperty(BUCKET_WITH_DEFAULT_VALUE).evaluateAttributeExpressions(attributes).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(attributes).getValue();

        if (isBlank(bucketName) || isBlank(key)) {
            throw new ProcessException("Bucket name or key value is missing");
        }

        if (!client.doesObjectExist(bucketName, key)) {
            throw new ProcessException(String.format("Object '%s/%s' does not exist in s3", bucketName, key));
        }

        final S3Object object = client.getObject(bucketName, key);
        return new FileResource(object.getObjectContent(), object.getObjectMetadata().getContentLength());
    }

    protected AmazonS3 getS3Client(Map<String, String> attributes, AWSCredentialsProvider credentialsProvider) {
        final Region region = resolveS3Region(context, attributes);
        return clientCache.get(region, ignored -> AmazonS3Client.builder()
                .withRegion(region.getName())
                .withCredentials(credentialsProvider)
                .build());
    }
}
