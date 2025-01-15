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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCases;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.StandardFlowFileMediaType;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.FlowFilePackager;
import org.apache.nifi.util.FlowFilePackagerV3;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"flowfile", "flowfile-stream", "flowfile-stream-v3", "package", "attributes"})
@CapabilityDescription("This processor will package FlowFile attributes and content into an output FlowFile that can be exported from NiFi"
        + " and imported back into NiFi, preserving the original attributes and content.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "The mime.type will be changed to application/flowfile-v3")
})
@SeeAlso({UnpackContent.class, MergeContent.class})
@MultiProcessorUseCases({
    @MultiProcessorUseCase(
        description = "Send FlowFile content and attributes from one NiFi instance to another NiFi instance.",
        notes = "A Remote Process Group is preferred to send FlowFiles between two NiFi instances, but an alternative is"
                + " to use PackageFlowFile then InvokeHTTP sending to ListenHTTP.",
        keywords = {"flowfile", "attributes", "content", "ffv3", "flowfile-stream-v3", "transfer"},
        configurations = {
            @ProcessorConfiguration(
                processorClass = PackageFlowFile.class,
                configuration = """
                    "Maximum Batch Size" > 1 can help improve performance by batching many flowfiles together into 1 larger file that is transmitted by InvokeHTTP.

                    Connect the success relationship of PackageFlowFile to the input of InvokeHTTP.
                """
            ),
            @ProcessorConfiguration(
                processorClass = InvokeHTTP.class,
                configuration = """
                    "HTTP Method" = "POST" to send data to ListenHTTP.
                    "HTTP URL" should include the hostname, port, and path to the ListenHTTP.
                    "Request Content-Type" = "${mime.type}" because PackageFlowFile output files have attribute mime.type=application/flowfile-v3.
                """
            ),
            @ProcessorConfiguration(
                processorClass = ListenHTTP.class,
                configuration = """
                    "Listening Port" = a unique port number.

                    ListenHTTP automatically unpacks files that have attribute mime.type=application/flowfile-v3.
                    If PackageFlowFile batches 99 FlowFiles into 1 file that InvokeHTTP sends, then the original 99 FlowFiles will be output by ListenHTTP.
                """
            )
        }
    ),
    @MultiProcessorUseCase(
        description = "Export FlowFile content and attributes from NiFi to external storage and reimport.",
        keywords = {"flowfile", "attributes", "content", "ffv3", "flowfile-stream-v3", "offline", "storage"},
        configurations = {
            @ProcessorConfiguration(
                processorClass = PackageFlowFile.class,
                configuration = """
                    "Maximum Batch Size" > 1 can improve storage efficiency by batching many FlowFiles together into 1 larger file that is stored.

                    Connect the success relationship to the input of any NiFi egress processor for offline storage.
                """
            ),
            @ProcessorConfiguration(
                processorClass = UnpackContent.class,
                configuration = """
                    "Packaging Format" = "application/flowfile-v3".

                    Connect the output of a NiFi ingress processor that reads files stored offline to the input of UnpackContent.
                    If PackageFlowFile batches 99 FlowFiles into 1 file that is read from storage, then the original 99 FlowFiles will be output by UnpackContent.
                """
            )
        }
    )
})
public class PackageFlowFile extends AbstractProcessor {

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("max-batch-size")
            .displayName("Maximum Batch Size")
            .description("Maximum number of FlowFiles to package into one output FlowFile using a best effort, non guaranteed approach."
                    + " Multiple input queues can produce unexpected batching behavior.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.createLongValidator(1, 10_000, true))
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BATCH_SIZE
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The packaged FlowFile is sent to this relationship")
            .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The FlowFiles that were used to create the package are sent to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_ORIGINAL
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final List<FlowFile> flowFiles = session.get(context.getProperty(BATCH_SIZE).asInteger());
        if (flowFiles.isEmpty()) {
            return;
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), StandardFlowFileMediaType.VERSION_3.getMediaType());

        final FlowFilePackager packager = new FlowFilePackagerV3();

        FlowFile packagedFlowFile = session.create(flowFiles);
        packagedFlowFile = session.write(packagedFlowFile, out -> {
            try (final OutputStream bufferedOut = new BufferedOutputStream(out)) {
                for (final FlowFile flowFile : flowFiles) {
                    session.read(flowFile, in -> {
                        try (final InputStream bufferedIn = new BufferedInputStream(in)) {
                            packager.packageFlowFile(bufferedIn, bufferedOut, flowFile.getAttributes(), flowFile.getSize());
                        }
                    });
                }
            }
        });

        packagedFlowFile = session.putAllAttributes(packagedFlowFile, attributes);
        session.transfer(packagedFlowFile, REL_SUCCESS);
        session.transfer(flowFiles, REL_ORIGINAL);
    }
}
