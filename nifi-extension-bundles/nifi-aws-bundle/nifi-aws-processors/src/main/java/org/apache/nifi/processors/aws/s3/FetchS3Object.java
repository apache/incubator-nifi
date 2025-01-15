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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.sqs.GetSQS;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.aws.util.RegionUtilV1.S3_REGION;

@SupportsBatching
@SeeAlso({PutS3Object.class, DeleteS3Object.class, ListS3.class, CopyS3Object.class, GetS3ObjectMetadata.class, TagS3Object.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "S3", "AWS", "Get", "Fetch"})
@CapabilityDescription("Retrieves the contents of an S3 Object and writes it to the content of a FlowFile")
@WritesAttributes({
    @WritesAttribute(attribute = "s3.url", description = "The URL that can be used to access the S3 object"),
    @WritesAttribute(attribute = "s3.bucket", description = "The name of the S3 bucket"),
    @WritesAttribute(attribute = "path", description = "The path of the file"),
    @WritesAttribute(attribute = "absolute.path", description = "The path of the file"),
    @WritesAttribute(attribute = "filename", description = "The name of the file"),
    @WritesAttribute(attribute = "hash.value", description = "The MD5 sum of the file"),
    @WritesAttribute(attribute = "hash.algorithm", description = "MD5"),
    @WritesAttribute(attribute = "mime.type", description = "If S3 provides the content type/MIME type, this attribute will hold that file"),
    @WritesAttribute(attribute = "s3.etag", description = "The ETag that can be used to see if the file has changed"),
    @WritesAttribute(attribute = "s3.exception", description = "The class name of the exception thrown during processor execution"),
    @WritesAttribute(attribute = "s3.additionalDetails", description = "The S3 supplied detail from the failed operation"),
    @WritesAttribute(attribute = "s3.statusCode", description = "The HTTP error code (if available) from the failed operation"),
    @WritesAttribute(attribute = "s3.errorCode", description = "The S3 moniker of the failed operation"),
    @WritesAttribute(attribute = "s3.errorMessage", description = "The S3 exception message from the failed operation"),
    @WritesAttribute(attribute = "s3.expirationTime", description = "If the file has an expiration date, this attribute will be set, containing the milliseconds since epoch in UTC time"),
    @WritesAttribute(attribute = "s3.expirationTimeRuleId", description = "The ID of the rule that dictates this object's expiration time"),
    @WritesAttribute(attribute = "s3.sseAlgorithm", description = "The server side encryption algorithm of the object"),
    @WritesAttribute(attribute = "s3.version", description = "The version of the S3 object"),
    @WritesAttribute(attribute = "s3.encryptionStrategy", description = "The name of the encryption strategy that was used to store the S3 object (if it is encrypted)"), })
@UseCase(
    description = "Fetch a specific file from S3",
    configuration = """
        The "Bucket" property should be set to the name of the S3 bucket that contains the file. Typically this is defined as an attribute on an incoming FlowFile, \
        so this property is set to `${s3.bucket}`.
        The "Object Key" property denotes the fully qualified filename of the file to fetch. Typically, the FlowFile's `filename` attribute is used, so this property is \
        set to `${filename}`.
        The "Region" property must be set to denote the S3 region that the Bucket resides in. If the flow being built is to be reused elsewhere, it's a good idea to parameterize \
        this property by setting it to something like `#{S3_REGION}`.

        The "AWS Credentials Provider service" property should specify an instance of the AWSCredentialsProviderControllerService in order to provide credentials for accessing the file.
        """
)
@MultiProcessorUseCase(
    description = "Retrieve all files in an S3 bucket",
    keywords = {"s3", "state", "retrieve", "fetch", "all", "stream"},
    configurations = {
        @ProcessorConfiguration(
            processorClass = ListS3.class,
            configuration = """
                The "Bucket" property should be set to the name of the S3 bucket that files reside in. If the flow being built is to be reused elsewhere, it's a good idea to parameterize \
                    this property by setting it to something like `#{S3_SOURCE_BUCKET}`.
                The "Region" property must be set to denote the S3 region that the Bucket resides in. If the flow being built is to be reused elsewhere, it's a good idea to parameterize \
                    this property by setting it to something like `#{S3_SOURCE_REGION}`.

                The "AWS Credentials Provider service" property should specify an instance of the AWSCredentialsProviderControllerService in order to provide credentials for accessing the bucket.

                The 'success' Relationship of this Processor is then connected to FetchS3Object.
                """
        ),
        @ProcessorConfiguration(
            processorClass = FetchS3Object.class,
            configuration = """
                "Bucket" = "${s3.bucket}"
                "Object Key" = "${filename}"

                The "AWS Credentials Provider service" property should specify an instance of the AWSCredentialsProviderControllerService in order to provide credentials for accessing the bucket.

                The "Region" property must be set to the same value as the "Region" property of the ListS3 Processor.
                """
        )
    }
)
@MultiProcessorUseCase(
    description = "Retrieve only files from S3 that meet some specified criteria",
    keywords = {"s3", "state", "retrieve", "filter", "select", "fetch", "criteria"},
    configurations = {
        @ProcessorConfiguration(
            processorClass = ListS3.class,
            configuration = """
                The "Bucket" property should be set to the name of the S3 bucket that files reside in. If the flow being built is to be reused elsewhere, it's a good idea to parameterize \
                    this property by setting it to something like `#{S3_SOURCE_BUCKET}`.
                The "Region" property must be set to denote the S3 region that the Bucket resides in. If the flow being built is to be reused elsewhere, it's a good idea to parameterize \
                    this property by setting it to something like `#{S3_SOURCE_REGION}`.

                The "AWS Credentials Provider service" property should specify an instance of the AWSCredentialsProviderControllerService in order to provide credentials for accessing the bucket.

                The 'success' Relationship of this Processor is then connected to RouteOnAttribute.
                """
        ),
        @ProcessorConfiguration(
            processorClassName = "org.apache.nifi.processors.standard.RouteOnAttribute",
            configuration = """
                If you would like to "OR" together all of the conditions (i.e., the file should be retrieved if any of the conditions are met), \
                set "Routing Strategy" to "Route to 'matched' if any matches".
                If you would like to "AND" together all of the conditions (i.e., the file should only be retrieved if all of the conditions are met), \
                set "Routing Strategy" to "Route to 'matched' if all match".

                For each condition that you would like to filter on, add a new property. The name of the property should describe the condition. \
                The value of the property should be an Expression Language expression that returns `true` if the file meets the condition or `false` \
                if the file does not meet the condition.

                Some attributes that you may consider filtering on are:
                - `filename` (the name of the file)
                - `s3.length` (the number of bytes in the file)
                - `s3.tag.<tag name>` (the value of the s3 tag with the name `tag name`)
                - `s3.user.metadata.<key name>` (the value of the user metadata with the key named `key name`)

                For example, to fetch only files that are at least 1 MB and have a filename ending in `.zip` we would set the following properties:
                - "Routing Strategy" = "Route to 'matched' if all match"
                - "At least 1 MB" = "${s3.length:ge(1000000)}"
                - "Ends in .zip" = "${filename:endsWith('.zip')}"

                Auto-terminate the `unmatched` Relationship.
                Connect the `matched` Relationship to the FetchS3Object processor.
                """
        ),
        @ProcessorConfiguration(
            processorClass = FetchS3Object.class,
            configuration = """
                "Bucket" = "${s3.bucket}"
                "Object Key" = "${filename}"

                The "AWS Credentials Provider service" property should specify an instance of the AWSCredentialsProviderControllerService in order to provide credentials for accessing the bucket.

                The "Region" property must be set to the same value as the "Region" property of the ListS3 Processor.
                """
        )
    }
)
@MultiProcessorUseCase(
    description = "Retrieve new files as they arrive in an S3 bucket",
    notes = "This method of retrieving files from S3 is more efficient than using ListS3 and more cost effective. It is the pattern recommended by AWS. " +
        "However, it does require that the S3 bucket be configured to place notifications on an SQS queue when new files arrive. For more information, see " +
        "https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html",
    configurations = {
        @ProcessorConfiguration(
            processorClass = GetSQS.class,
            configuration = """
                The "Queue URL" must be set to the appropriate URL for the SQS queue. It is recommended that this property be parameterized, using a value such as `#{SQS_QUEUE_URL}`.
                The "Region" property must be set to denote the SQS region that the queue resides in. It's a good idea to parameterize this property by setting it to something like `#{SQS_REGION}`.

                The "AWS Credentials Provider service" property should specify an instance of the AWSCredentialsProviderControllerService in order to provide credentials for accessing the bucket.

                The 'success' relationship is connected to EvaluateJsonPath.
                """
        ),
        @ProcessorConfiguration(
            processorClassName = "org.apache.nifi.processors.standard.EvaluateJsonPath",
            configuration = """
                "Destination" = "flowfile-attribute"
                "s3.bucket" = "$.Records[0].s3.bucket.name"
                "filename" = "$.Records[0].s3.object.key"

                The 'success' relationship is connected to FetchS3Object.
                """
        ),
        @ProcessorConfiguration(
            processorClass =  FetchS3Object.class,
            configuration = """
                "Bucket" = "${s3.bucket}"
                "Object Key" = "${filename}"

                The "Region" property must be set to the same value as the "Region" property of the GetSQS Processor.
                The "AWS Credentials Provider service" property should specify an instance of the AWSCredentialsProviderControllerService in order to provide credentials for accessing the bucket.
                """
        )
    }
)
public class FetchS3Object extends AbstractS3Processor {

    public static final PropertyDescriptor VERSION_ID = new PropertyDescriptor.Builder()
            .name("Version")
            .description("The Version of the Object to download")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor REQUESTER_PAYS = new PropertyDescriptor.Builder()
            .name("requester-pays")
            .displayName("Requester Pays")
            .required(true)
            .description("If true, indicates that the requester consents to pay any charges associated with retrieving objects from "
                    + "the S3 bucket.  This sets the 'x-amz-request-payer' header to 'requester'.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues(new AllowableValue("true", "True", "Indicates that the requester consents to pay any charges associated "
                    + "with retrieving objects from the S3 bucket."), new AllowableValue("false", "False", "Does not consent to pay "
                            + "requester charges for retrieving objects from the S3 bucket."))
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor RANGE_START = new PropertyDescriptor.Builder()
            .name("range-start")
            .displayName("Range Start")
            .description("The byte position at which to start reading from the object. An empty value or a value of " +
                    "zero will start reading at the beginning of the object.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor RANGE_LENGTH = new PropertyDescriptor.Builder()
            .name("range-length")
            .displayName("Range Length")
            .description("The number of bytes to download from the object, starting from the Range Start. An empty " +
                    "value or a value that extends beyond the end of the object will read to the end of the object.")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, Long.MAX_VALUE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        BUCKET_WITH_DEFAULT_VALUE,
        KEY,
        S3_REGION,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        TIMEOUT,
        VERSION_ID,
        SSL_CONTEXT_SERVICE,
        ENDPOINT_OVERRIDE,
        SIGNER_OVERRIDE,
        S3_CUSTOM_SIGNER_CLASS_NAME,
        S3_CUSTOM_SIGNER_MODULE_LOCATION,
        ENCRYPTION_SERVICE,
        PROXY_CONFIGURATION_SERVICE,
        REQUESTER_PAYS,
        RANGE_START,
        RANGE_LENGTH);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        AmazonS3EncryptionService encryptionService = validationContext.getProperty(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);
        if (encryptionService != null) {
            String strategyName = encryptionService.getStrategyName();
            if (strategyName.equals(AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3) || strategyName.equals(AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS)) {
                problems.add(new ValidationResult.Builder()
                        .subject(ENCRYPTION_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation(encryptionService.getStrategyDisplayName() + " is not a valid encryption strategy for fetching objects. Decryption will be handled automatically " +
                                "during the fetch of S3 objects encrypted with " + encryptionService.getStrategyDisplayName())
                        .build()
                );
            }
        }

        return problems;
    }

    @Override
    public List<ConfigVerificationResult> verify(ProcessContext context, ComponentLog verificationLogger, Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>(super.verify(context, verificationLogger, attributes));

        final String bucket = context.getProperty(BUCKET_WITH_DEFAULT_VALUE).evaluateAttributeExpressions(attributes).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(attributes).getValue();

        final AmazonS3Client client = createClient(context, attributes);
        final GetObjectMetadataRequest request = createGetObjectMetadataRequest(context, attributes);

        try {
            final ObjectMetadata objectMetadata = client.getObjectMetadata(request);
            final long byteCount = objectMetadata.getContentLength();
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("HEAD S3 Object")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation(String.format("Successfully performed HEAD on [%s] (%s bytes) from Bucket [%s]", key, byteCount, bucket))
                    .build());
        } catch (final Exception e) {
            getLogger().error("Failed to fetch [{}] from Bucket [{}]", key, bucket, e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("HEAD S3 Object")
                    .outcome(Outcome.FAILED)
                    .explanation(String.format("Failed to perform HEAD on [%s] from Bucket [%s]: %s", key, bucket, e.getMessage()))
                    .build());
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final AmazonS3Client client;
        try {
            client = getS3Client(context, flowFile.getAttributes());
        } catch (Exception e) {
            getLogger().error("Failed to initialize S3 client", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final long startNanos = System.nanoTime();

        final Map<String, String> attributes = new HashMap<>();

        final AmazonS3EncryptionService encryptionService = context.getProperty(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);
        if (encryptionService != null) {
            attributes.put("s3.encryptionStrategy", encryptionService.getStrategyName());
        }
        final String bucket = context.getProperty(BUCKET_WITH_DEFAULT_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();

        final GetObjectRequest request = createGetObjectRequest(context, flowFile.getAttributes());

        try (final S3Object s3Object = client.getObject(request)) {
            if (s3Object == null) {
                throw new IOException("AWS refused to execute this request.");
            }
            flowFile = session.importFrom(s3Object.getObjectContent(), flowFile);
            attributes.put("s3.bucket", s3Object.getBucketName());

            final ObjectMetadata metadata = s3Object.getObjectMetadata();
            if (metadata.getContentDisposition() != null) {
                final String contentDisposition = URLDecoder.decode(metadata.getContentDisposition(), StandardCharsets.UTF_8);

                if (contentDisposition.equals(PutS3Object.CONTENT_DISPOSITION_INLINE) || contentDisposition.startsWith("attachment; filename=")) {
                    setFilePathAttributes(attributes, key);
                } else {
                    setFilePathAttributes(attributes, contentDisposition);
                }
            }
            if (metadata.getContentMD5() != null) {
                attributes.put("hash.value", metadata.getContentMD5());
                attributes.put("hash.algorithm", "MD5");
            }
            if (metadata.getContentType() != null) {
                attributes.put(CoreAttributes.MIME_TYPE.key(), metadata.getContentType());
            }
            if (metadata.getETag() != null) {
                attributes.put("s3.etag", metadata.getETag());
            }
            if (metadata.getExpirationTime() != null) {
                attributes.put("s3.expirationTime", String.valueOf(metadata.getExpirationTime().getTime()));
            }
            if (metadata.getExpirationTimeRuleId() != null) {
                attributes.put("s3.expirationTimeRuleId", metadata.getExpirationTimeRuleId());
            }
            if (metadata.getUserMetadata() != null) {
                attributes.putAll(metadata.getUserMetadata());
            }
            if (metadata.getSSEAlgorithm() != null) {
                String sseAlgorithmName = metadata.getSSEAlgorithm();
                attributes.put("s3.sseAlgorithm", sseAlgorithmName);
                if (sseAlgorithmName.equals(SSEAlgorithm.AES256.getAlgorithm())) {
                    attributes.put("s3.encryptionStrategy", AmazonS3EncryptionService.STRATEGY_NAME_SSE_S3);
                } else if (sseAlgorithmName.equals(SSEAlgorithm.KMS.getAlgorithm())) {
                    attributes.put("s3.encryptionStrategy", AmazonS3EncryptionService.STRATEGY_NAME_SSE_KMS);
                }
            }
            if (metadata.getVersionId() != null) {
                attributes.put("s3.version", metadata.getVersionId());
            }
        } catch (final IllegalArgumentException | IOException | AmazonClientException ioe) {
            flowFile = extractExceptionDetails(ioe, session, flowFile);
            getLogger().error("Failed to retrieve S3 Object for {}; routing to failure", flowFile, ioe);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        } catch (final FlowFileAccessException ffae) {
            if (ExceptionUtils.indexOfType(ffae, AmazonClientException.class) != -1) {
                getLogger().error("Failed to retrieve S3 Object for {}; routing to failure", flowFile, ffae);
                flowFile = extractExceptionDetails(ffae, session, flowFile);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
            throw ffae;
        }

        final String url = client.getResourceUrl(bucket, key);
        attributes.put("s3.url", url);
        flowFile = session.putAllAttributes(flowFile, attributes);

        session.transfer(flowFile, REL_SUCCESS);
        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully retrieved S3 Object for {} in {} millis; routing to success", flowFile, transferMillis);
        session.getProvenanceReporter().fetch(flowFile, url, transferMillis);
    }

    private GetObjectMetadataRequest createGetObjectMetadataRequest(final ProcessContext context, final Map<String, String> attributes) {
        final String bucket = context.getProperty(BUCKET_WITH_DEFAULT_VALUE).evaluateAttributeExpressions(attributes).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(attributes).getValue();
        final String versionId = context.getProperty(VERSION_ID).evaluateAttributeExpressions(attributes).getValue();
        final boolean requesterPays = context.getProperty(REQUESTER_PAYS).asBoolean();

        final GetObjectMetadataRequest request;
        if (versionId == null) {
            request = new GetObjectMetadataRequest(bucket, key);
        } else {
            request = new GetObjectMetadataRequest(bucket, key, versionId);
        }
        request.setRequesterPays(requesterPays);
        return request;
    }

    private GetObjectRequest createGetObjectRequest(final ProcessContext context, final Map<String, String> attributes) {
        final String bucket = context.getProperty(BUCKET_WITH_DEFAULT_VALUE).evaluateAttributeExpressions(attributes).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(attributes).getValue();
        final String versionId = context.getProperty(VERSION_ID).evaluateAttributeExpressions(attributes).getValue();
        final boolean requesterPays = context.getProperty(REQUESTER_PAYS).asBoolean();
        final long rangeStart = (context.getProperty(RANGE_START).isSet() ? context.getProperty(RANGE_START).evaluateAttributeExpressions(attributes).asDataSize(DataUnit.B).longValue() : 0L);
        final Long rangeLength = (context.getProperty(RANGE_LENGTH).isSet() ? context.getProperty(RANGE_LENGTH).evaluateAttributeExpressions(attributes).asDataSize(DataUnit.B).longValue() : null);

        final GetObjectRequest request;
        if (versionId == null) {
            request = new GetObjectRequest(bucket, key);
        } else {
            request = new GetObjectRequest(bucket, key, versionId);
        }
        request.setRequesterPays(requesterPays);

        // tl;dr don't setRange(0) on GetObjectRequest because it results in
        // InvalidRange errors on zero byte objects.
        //
        // Amazon S3 sets byte ranges using HTTP Range headers as described in
        // https://datatracker.ietf.org/doc/html/rfc2616#section-14.35 and
        // https://datatracker.ietf.org/doc/html/rfc7233#section-2.1. There
        // isn't a satisfiable byte range specification for zero length objects
        // so 416 (Request range not satisfiable) is returned.
        //
        // Since the effect of the byte range 0- is equivalent to not sending a
        // byte range and works for both zero and non-zero length objects,
        // the single argument setRange() only needs to be called when the
        // first byte position is greater than zero.
        if (rangeLength != null) {
            request.setRange(rangeStart, rangeStart + rangeLength - 1);
        } else if (rangeStart > 0) {
            request.setRange(rangeStart);
        }

        final AmazonS3EncryptionService encryptionService = context.getProperty(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);
        if (encryptionService != null) {
            encryptionService.configureGetObjectRequest(request, new ObjectMetadata());
        }
        return request;
    }

    protected void setFilePathAttributes(Map<String, String> attributes, String filePathName) {
        final int lastSlash = filePathName.lastIndexOf("/");
        if (lastSlash > -1 && lastSlash < filePathName.length() - 1) {
            attributes.put(CoreAttributes.PATH.key(), filePathName.substring(0, lastSlash));
            attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), filePathName);
            attributes.put(CoreAttributes.FILENAME.key(), filePathName.substring(lastSlash + 1));
        } else {
            attributes.put(CoreAttributes.FILENAME.key(), filePathName);
        }
    }
}