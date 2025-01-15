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
package org.apache.nifi.processors.aws.s3;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.aws.util.RegionUtilV1.S3_REGION;

@Tags({"Amazon", "S3", "AWS", "Archive", "Copy"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Copies a file from one bucket and key to another in AWS S3")
@SeeAlso({PutS3Object.class, DeleteS3Object.class, ListS3.class, TagS3Object.class, DeleteS3Object.class, FetchS3Object.class})
public class CopyS3Object extends AbstractS3Processor {
    public static final long MULTIPART_THRESHOLD = 5L * 1024L * 1024L * 1024L;

    static final PropertyDescriptor SOURCE_BUCKET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BUCKET_WITH_DEFAULT_VALUE)
            .name("Source Bucket")
            .displayName("Source Bucket")
            .description("The bucket that contains the file to be copied.")
            .build();

    static final PropertyDescriptor SOURCE_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(KEY)
            .name("Source Key")
            .displayName("Source Key")
            .description("The source key in the source bucket")
            .build();

    static final PropertyDescriptor DESTINATION_BUCKET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BUCKET_WITHOUT_DEFAULT_VALUE)
            .name("Destination Bucket")
            .displayName("Destination Bucket")
            .description("The bucket that will receive the copy.")
            .build();

    static final PropertyDescriptor DESTINATION_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(KEY)
            .name("Destination Key")
            .displayName("Destination Key")
            .description("The target key in the target bucket")
            .defaultValue("${filename}-1")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SOURCE_BUCKET,
            SOURCE_KEY,
            DESTINATION_BUCKET,
            DESTINATION_KEY,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            S3_REGION,
            TIMEOUT,
            FULL_CONTROL_USER_LIST,
            READ_USER_LIST,
            WRITE_USER_LIST,
            READ_ACL_LIST,
            WRITE_ACL_LIST,
            CANNED_ACL,
            OWNER,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            SIGNER_OVERRIDE,
            S3_CUSTOM_SIGNER_CLASS_NAME,
            S3_CUSTOM_SIGNER_MODULE_LOCATION,
            PROXY_CONFIGURATION_SERVICE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final AmazonS3Client s3;
        try {
            s3 = getS3Client(context, flowFile.getAttributes());
        } catch (Exception e) {
            getLogger().error("Failed to initialize S3 client", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String sourceBucket = context.getProperty(SOURCE_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String sourceKey = context.getProperty(SOURCE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationBucket = context.getProperty(DESTINATION_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationKey = context.getProperty(DESTINATION_KEY).evaluateAttributeExpressions(flowFile).getValue();



        final AtomicReference<String> multipartIdRef = new AtomicReference<>();
        boolean multipartUploadRequired = false;

        try {
            final GetObjectMetadataRequest sourceMetadataRequest = new GetObjectMetadataRequest(sourceBucket, sourceKey);
            final ObjectMetadata metadataResult = s3.getObjectMetadata(sourceMetadataRequest);
            final long contentLength = metadataResult.getContentLength();
            multipartUploadRequired = contentLength > MULTIPART_THRESHOLD;
            final AccessControlList acl = createACL(context, flowFile);
            final CannedAccessControlList cannedAccessControlList = createCannedACL(context, flowFile);

            if (multipartUploadRequired) {
                copyMultipart(s3, acl, cannedAccessControlList, sourceBucket, sourceKey, destinationBucket,
                        destinationKey, multipartIdRef, contentLength);
            } else {
                copyObject(s3, acl, cannedAccessControlList, sourceBucket, sourceKey, destinationBucket, destinationKey);
            }
            session.getProvenanceReporter().send(flowFile, getTransitUrl(destinationBucket, destinationKey));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            if (multipartUploadRequired && StringUtils.isNotEmpty(multipartIdRef.get())) {
                try {
                    final AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(destinationBucket, destinationKey, multipartIdRef.get());
                    s3.abortMultipartUpload(abortRequest);
                } catch (final AmazonS3Exception s3e) {
                    getLogger().warn("Abort Multipart Upload failed for Bucket [{}] Key [{}]", destinationBucket, destinationKey, s3e);
                }
            }

            flowFile = extractExceptionDetails(e, session, flowFile);
            getLogger().error("Failed to copy S3 object from Bucket [{}] Key [{}]", sourceBucket, sourceKey, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /*
     * Sections of this code were derived from example code from the official AWS S3 documentation. Specifically this example:
     * https://github.com/awsdocs/aws-doc-sdk-examples/blob/df606a664bf2f7cfe3abc76c187e024451d0279c/java/example_code/s3/src/main/java/aws/example/s3/LowLevelMultipartCopy.java
     */
    private void copyMultipart(final AmazonS3Client s3, final AccessControlList acl, final CannedAccessControlList cannedAccessControlList,
                               final String sourceBucket, final String sourceKey,
                               final String destinationBucket, final String destinationKey, final AtomicReference<String> multipartIdRef,
                               final long contentLength) {
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(destinationBucket,
                destinationKey);
        if (acl != null) {
            initRequest.setAccessControlList(acl);
        }
        if (cannedAccessControlList != null) {
            initRequest.setCannedACL(cannedAccessControlList);
        }

        final InitiateMultipartUploadResult initResult = s3.initiateMultipartUpload(initRequest);

        multipartIdRef.set(initResult.getUploadId());

        long bytePosition = 0;
        int partNumber = 1;
        final List<CopyPartResult> copyPartResults = new ArrayList<>();
        while (bytePosition < contentLength) {
            long lastByte = Math.min(bytePosition + MULTIPART_THRESHOLD - 1, contentLength - 1);

            final CopyPartRequest copyPartRequest = new CopyPartRequest()
                    .withSourceBucketName(sourceBucket)
                    .withSourceKey(sourceKey)
                    .withDestinationBucketName(destinationBucket)
                    .withDestinationKey(destinationKey)
                    .withUploadId(initResult.getUploadId())
                    .withFirstByte(bytePosition)
                    .withLastByte(lastByte)
                    .withPartNumber(partNumber++);

            doRetryLoop(partRequest -> copyPartResults.add(s3.copyPart((CopyPartRequest) partRequest)), copyPartRequest);

            bytePosition += MULTIPART_THRESHOLD;
        }

        final CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                destinationBucket,
                destinationKey,
                initResult.getUploadId(),
                copyPartResults.stream().map(response -> new PartETag(response.getPartNumber(), response.getETag()))
                        .collect(Collectors.toList()));
        doRetryLoop(complete -> s3.completeMultipartUpload(completeRequest), completeRequest);
    }

    private void doRetryLoop(Consumer<AmazonWebServiceRequest> consumer, AmazonWebServiceRequest request) {
        boolean requestComplete = false;
        int retryIndex = 0;

        while (!requestComplete) {
            try {
                consumer.accept(request);
                requestComplete = true;
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 503 && retryIndex < 3) {
                    retryIndex++;
                } else {
                    throw e;
                }
            }
        }
    }

    private void copyObject(final AmazonS3Client s3, final AccessControlList acl,
                               final CannedAccessControlList cannedAcl,
                               final String sourceBucket, final String sourceKey,
                               final String destinationBucket, final String destinationKey) {
        final CopyObjectRequest request = new CopyObjectRequest(sourceBucket, sourceKey, destinationBucket, destinationKey);

        if (acl != null) {
            request.setAccessControlList(acl);
        }

        if (cannedAcl != null) {
            request.setCannedAccessControlList(cannedAcl);
        }

        s3.copyObject(request);
    }

    private String getTransitUrl(final String destinationBucket, final String destinationKey) {
        final String spacer = destinationKey.startsWith("/") ? "" : "/";
        return String.format("s3://%s%s%s", destinationBucket, spacer, destinationKey);
    }
}
