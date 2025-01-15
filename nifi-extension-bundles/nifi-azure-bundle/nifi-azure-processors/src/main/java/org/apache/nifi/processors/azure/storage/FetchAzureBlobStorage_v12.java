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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobRange;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
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
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor_v12;
import org.apache.nifi.processors.azure.ClientSideEncryptionSupport;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.BLOB_STORAGE_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_BLOBTYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_CONTAINER;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_LANG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_MIME_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_DESCRIPTION_TIMESTAMP;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBTYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_CONTAINER;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LANG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_MIME_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_TIMESTAMP;

@Tags({"azure", "microsoft", "cloud", "storage", "blob"})
@CapabilityDescription("Retrieves the specified blob from Azure Blob Storage and writes its content to the content of the FlowFile. The processor uses Azure Blob Storage client library v12.")
@SeeAlso({ListAzureBlobStorage_v12.class, PutAzureBlobStorage_v12.class, DeleteAzureBlobStorage_v12.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({@WritesAttribute(attribute = ATTR_NAME_CONTAINER, description = ATTR_DESCRIPTION_CONTAINER),
        @WritesAttribute(attribute = ATTR_NAME_BLOBNAME, description = ATTR_DESCRIPTION_BLOBNAME),
        @WritesAttribute(attribute = ATTR_NAME_PRIMARY_URI, description = ATTR_DESCRIPTION_PRIMARY_URI),
        @WritesAttribute(attribute = ATTR_NAME_ETAG, description = ATTR_DESCRIPTION_ETAG),
        @WritesAttribute(attribute = ATTR_NAME_BLOBTYPE, description = ATTR_DESCRIPTION_BLOBTYPE),
        @WritesAttribute(attribute = ATTR_NAME_MIME_TYPE, description = ATTR_DESCRIPTION_MIME_TYPE),
        @WritesAttribute(attribute = ATTR_NAME_LANG, description = ATTR_DESCRIPTION_LANG),
        @WritesAttribute(attribute = ATTR_NAME_TIMESTAMP, description = ATTR_DESCRIPTION_TIMESTAMP),
        @WritesAttribute(attribute = ATTR_NAME_LENGTH, description = ATTR_DESCRIPTION_LENGTH)})
@MultiProcessorUseCase(
    description = "Retrieve all files in an Azure Blob Storage container",
    keywords = {"azure", "blob", "storage", "state", "retrieve", "fetch", "all", "stream"},
    configurations = {
        @ProcessorConfiguration(
            processorClass = ListAzureBlobStorage_v12.class,
            configuration = """
                The "Container Name" property should be set to the name of the Blob Storage Container that files reside in. \
                    If the flow being built is to be reused elsewhere, it's a good idea to parameterize this property by setting it to something like `#{AZURE_CONTAINER}`.

                The "Storage Credentials" property should specify an instance of the AzureStorageCredentialsService_v12 in order to provide credentials for accessing the storage container.

                The 'success' Relationship of this Processor is then connected to FetchAzureBlobStorage_v12.
                """
        ),
        @ProcessorConfiguration(
            processorClass = FetchAzureBlobStorage_v12.class,
            configuration = """
                "Container Name" = "${azure.container}"
                "Blob Name" = "${azure.blobname}"

                The "Storage Credentials" property should specify an instance of the AzureStorageCredentialsService_v12 in order to provide credentials for accessing the storage container.
                """
        )
    }
)
public class FetchAzureBlobStorage_v12 extends AbstractAzureBlobProcessor_v12 implements ClientSideEncryptionSupport {

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.CONTAINER)
            .defaultValue(String.format("${%s}", ATTR_NAME_CONTAINER))
            .build();

    public static final PropertyDescriptor BLOB_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractAzureBlobProcessor_v12.BLOB_NAME)
            .defaultValue(String.format("${%s}", ATTR_NAME_BLOBNAME))
            .build();

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

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BLOB_STORAGE_CREDENTIALS_SERVICE,
            CONTAINER,
            BLOB_NAME,
            RANGE_START,
            RANGE_LENGTH,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE,
            CSE_KEY_TYPE,
            CSE_KEY_ID,
            CSE_LOCAL_KEY
    );

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        results.addAll(validateClientSideEncryptionProperties(validationContext));
        return results;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        long startNanos = System.nanoTime();

        String containerName = context.getProperty(CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        String blobName = context.getProperty(BLOB_NAME).evaluateAttributeExpressions(flowFile).getValue();
        long rangeStart = (context.getProperty(RANGE_START).isSet() ? context.getProperty(RANGE_START).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : 0L);
        Long rangeLength = (context.getProperty(RANGE_LENGTH).isSet() ? context.getProperty(RANGE_LENGTH).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : null);

        try {
            BlobServiceClient storageClient = getStorageClient(context, flowFile);
            BlobContainerClient containerClient = storageClient.getBlobContainerClient(containerName);
            final BlobClient blobClient;
            if (isClientSideEncryptionEnabled(context)) {
                blobClient = getEncryptedBlobClient(context, containerClient, blobName);
            } else {
                blobClient = containerClient.getBlobClient(blobName);
            }

            flowFile = session.write(flowFile, os -> blobClient.downloadStreamWithResponse(os, new BlobRange(rangeStart, rangeLength), null, null, false, null, null));

            Map<String, String> attributes = createBlobAttributesMap(blobClient);
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.transfer(flowFile, REL_SUCCESS);

            long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            String transitUri = attributes.get(ATTR_NAME_PRIMARY_URI);
            session.getProvenanceReporter().fetch(flowFile, transitUri, transferMillis);
        } catch (Exception e) {
            getLogger().error("Failure to fetch Azure blob {}", blobName, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
