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
package org.apache.nifi.processors.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.StandardFlowFileMediaType;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.hadoop.util.SequenceFileWriter;
import org.apache.nifi.util.StopWatch;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * <p>
 * This processor is used to create a Hadoop Sequence File, which essentially is a file of key/value pairs. The key will be a file name and the value will be the flow file content. The processor will
 * take either a merged (a.k.a. packaged) flow file or a singular flow file. Historically, this processor handled the merging by type and size or time prior to creating a SequenceFile output; it no
 * longer does this. If creating a SequenceFile that contains multiple files of the same type is desired, precede this processor with a <code>RouteOnAttribute</code> processor to segregate files of
 * the same type and follow that with a <code>MergeContent</code> processor to bundle up files. If the type of files is not important, just use the <code>MergeContent</code> processor. When using the
 * <code>MergeContent</code> processor, the following Merge Formats are supported by this processor:
 * <ul>
 * <li>TAR</li>
 * <li>ZIP</li>
 * <li>FlowFileStream v3</li>
 * </ul>
 * The created SequenceFile is named the same as the incoming FlowFile with the suffix '.sf'. For incoming FlowFiles that are bundled, the keys in the SequenceFile are the individual file names, the
 * values are the contents of each file.
 * </p>
 * NOTE: The value portion of a key/value pair is loaded into memory. While there is a max size limit of 2GB, this could cause memory issues if there are too many concurrent tasks and the flow file
 * sizes are large.
 *
 */
@SideEffectFree
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "sequence file", "create", "sequencefile"})
@CapabilityDescription("Creates Hadoop Sequence Files from incoming flow files")
@SeeAlso(PutHDFS.class)
public class CreateHadoopSequenceFile extends AbstractHadoopProcessor {

    public static final String TAR_FORMAT = "tar";
    public static final String ZIP_FORMAT = "zip";
    public static final String FLOWFILE_STREAM_FORMAT_V3 = "flowfile-stream-v3";
    private static final String NOT_PACKAGED = "not packaged";

    // Relationships.
    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Generated Sequence Files are sent to this relationship")
            .build();
    public static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Incoming files that failed to generate a Sequence File are sent to this relationship")
            .build();

    // Optional Properties.
    static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .displayName("Compression type")
            .name("compression type")
            .description("Type of compression to use when creating Sequence File")
            .allowableValues(SequenceFile.CompressionType.values())
            .build();

    // Default Values.
    public static final String DEFAULT_COMPRESSION_TYPE = "NONE";

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            RELATIONSHIP_SUCCESS,
            RELATIONSHIP_FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                COMPRESSION_TYPE,
                COMPRESSION_CODEC
            )
    ).toList();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
        String packagingFormat = NOT_PACKAGED;
        if (null != mimeType) {
            if (StandardFlowFileMediaType.VERSION_3.getMediaType().equals(mimeType)) {
                packagingFormat = FLOWFILE_STREAM_FORMAT_V3;
            } else {
                switch (mimeType.toLowerCase()) {
                    case "application/tar":
                        packagingFormat = TAR_FORMAT;
                        break;
                    case "application/zip":
                        packagingFormat = ZIP_FORMAT;
                        break;
                    default:
                        getLogger().warn("Cannot unpack {} because its mime.type attribute is set to '{}', which is not a format that can be unpacked", flowFile, mimeType);
                }
            }
        }
        final SequenceFileWriter sequenceFileWriter = switch (packagingFormat) {
            case TAR_FORMAT -> new TarUnpackerSequenceFileWriter();
            case ZIP_FORMAT -> new ZipUnpackerSequenceFileWriter();
            case FLOWFILE_STREAM_FORMAT_V3 -> new FlowFileStreamUnpackerSequenceFileWriter();
            default -> new SequenceFileWriterImpl();
        };

        final Configuration configuration = getConfiguration();
        if (configuration == null) {
            getLogger().error("HDFS not configured properly");
            session.transfer(flowFile, RELATIONSHIP_FAILURE);
            context.yield();
            return;
        }

        final CompressionCodec codec = getCompressionCodec(context, configuration);

        final String value = context.getProperty(COMPRESSION_TYPE).getValue();
        final SequenceFile.CompressionType compressionType = value == null
            ? SequenceFile.CompressionType.valueOf(DEFAULT_COMPRESSION_TYPE) : SequenceFile.CompressionType.valueOf(value);

        final String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".sf";
        flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), fileName);

        try {
            StopWatch stopWatch = new StopWatch(true);
            flowFile = sequenceFileWriter.writeSequenceFile(flowFile, session, configuration, compressionType, codec);
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, RELATIONSHIP_SUCCESS);
            getLogger().info("Transferred flowfile {} to {}", flowFile, RELATIONSHIP_SUCCESS);
        } catch (ProcessException e) {
            getLogger().error("Failed to create Sequence File. Transferring {} to 'failure'", flowFile, e);
            session.transfer(flowFile, RELATIONSHIP_FAILURE);
        }

    }
}
