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
package org.apache.nifi.processors.azure.storage;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureBlobClientSideEncryptionUtils;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({ "azure", "microsoft", "cloud", "storage", "blob" })
@CapabilityDescription("Retrieves contents of an Azure Storage Blob, writing the contents to the content of the FlowFile")
@SeeAlso({ ListAzureBlobStorage.class, PutAzureBlobStorage.class, DeleteAzureBlobStorage.class })
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "azure.length", description = "The length of the blob fetched")
})
public class FetchAzureBlobStorage extends AbstractAzureBlobProcessor {

    public static final PropertyDescriptor RANGE_START = new PropertyDescriptor.Builder()
            .name("range-start")
            .displayName("Range Start")
            .description("The byte position at which to start reading from the blob. An empty value or a value of " +
                    "zero will start reading at the beginning of the blob.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor RANGE_LENGTH = new PropertyDescriptor.Builder()
            .name("range-length")
            .displayName("Range Length")
            .description("The number of bytes to download from the blob, starting from the Range Start. An empty " +
                    "value or a value that extends beyond the end of the blob will read to the end of the blob.")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, Long.MAX_VALUE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
        AzureStorageUtils.CONTAINER_WITH_DEFAULT_VALUE,
        BLOB,
        AzureStorageUtils.STORAGE_CREDENTIALS_SERVICE,
        AzureStorageUtils.ACCOUNT_NAME,
        AzureStorageUtils.ACCOUNT_KEY,
        AzureStorageUtils.PROP_SAS_TOKEN,
        AzureStorageUtils.ENDPOINT_SUFFIX,
        RANGE_START,
        RANGE_LENGTH,
        AzureBlobClientSideEncryptionUtils.CSE_KEY_TYPE,
        AzureBlobClientSideEncryptionUtils.CSE_KEY_ID,
        AzureBlobClientSideEncryptionUtils.CSE_SYMMETRIC_KEY_HEX,
        AzureStorageUtils.PROXY_CONFIGURATION_SERVICE));


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        results.addAll(AzureBlobClientSideEncryptionUtils.validateClientSideEncryptionProperties(validationContext));
        return results;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final String containerName = context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        final String blobPath = context.getProperty(BLOB).evaluateAttributeExpressions(flowFile).getValue();
        final long rangeStart = (context.getProperty(RANGE_START).isSet() ? context.getProperty(RANGE_START).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : 0L);
        final Long rangeLength = (context.getProperty(RANGE_LENGTH).isSet() ? context.getProperty(RANGE_LENGTH).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : null);

        try {
            final CloudBlobClient blobClient = AzureStorageUtils.createCloudBlobClient(context, getLogger(), flowFile);
            final CloudBlobContainer container = blobClient.getContainerReference(containerName);

            final OperationContext operationContext = new OperationContext();
            AzureStorageUtils.setProxy(operationContext, context);

            final CloudBlob blob = container.getBlockBlobReference(blobPath);

            final BlobRequestOptions blobRequestOptions = createBlobRequestOptions(context);

            // TODO - we may be able do fancier things with ranges and
            // distribution of download over threads, investigate
            try (final OutputStream os = session.write(flowFile)) {
                blob.downloadRange(rangeStart, rangeLength, os, null, blobRequestOptions, operationContext);
            }

            final long length = blob.getProperties().getLength();
            flowFile = session.putAllAttributes(flowFile, Collections.singletonMap("azure.length", String.valueOf(length)));

            session.transfer(flowFile, REL_SUCCESS);
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().fetch(flowFile, blob.getSnapshotQualifiedUri().toString(), transferMillis);
        } catch (final Exception e) {
            getLogger().error("Failed to fetch Azure blob {} for {}", blobPath, flowFile, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
