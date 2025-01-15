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
package org.apache.nifi.processors.parquet;

import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.parquet.hadoop.AvroParquetHDFSRecordWriter;
import org.apache.nifi.parquet.utils.ParquetConfig;
import org.apache.nifi.parquet.utils.ParquetUtils;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processors.hadoop.AbstractPutHDFSRecord;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import static org.apache.nifi.parquet.utils.ParquetUtils.applyCommonConfig;
import static org.apache.nifi.parquet.utils.ParquetUtils.createParquetConfig;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "parquet", "hadoop", "HDFS", "filesystem", "record"})
@CapabilityDescription("Reads records from an incoming FlowFile using the provided Record Reader, and writes those records " +
        "to a Parquet file. The schema for the Parquet file must be provided in the processor properties. This processor will " +
        "first write a temporary dot file and upon successfully writing every record to the dot file, it will rename the " +
        "dot file to it's final name. If the dot file cannot be renamed, the rename operation will be attempted up to 10 times, and " +
        "if still not successful, the dot file will be deleted and the flow file will be routed to failure. " +
        " If any error occurs while reading records from the input, or writing records to the output, " +
        "the entire dot file will be removed and the flow file will be routed to failure or retry, depending on the error.")
@ReadsAttribute(attribute = "filename", description = "The name of the file to write comes from the value of this attribute.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "The absolute path to the file is stored in this attribute."),
        @WritesAttribute(attribute = "hadoop.file.url", description = "The hadoop url for the file is stored in this attribute."),
        @WritesAttribute(attribute = "record.count", description = "The number of records written to the Parquet file")
})
@Restricted(restrictions = {
    @Restriction(
        requiredPermission = RequiredPermission.WRITE_DISTRIBUTED_FILESYSTEM,
        explanation = "Provides operator the ability to write any file that NiFi has access to in HDFS or the local filesystem.")
})
public class PutParquet extends AbstractPutHDFSRecord {

    public static final PropertyDescriptor REMOVE_CRC_FILES = new PropertyDescriptor.Builder()
            .name("remove-crc-files")
            .displayName("Remove CRC Files")
            .description("Specifies whether the corresponding CRC file should be deleted upon successfully writing a Parquet file")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        ParquetUtils.ROW_GROUP_SIZE,
        ParquetUtils.PAGE_SIZE,
        ParquetUtils.DICTIONARY_PAGE_SIZE,
        ParquetUtils.MAX_PADDING_SIZE,
        ParquetUtils.ENABLE_DICTIONARY_ENCODING,
        ParquetUtils.ENABLE_VALIDATION,
        ParquetUtils.WRITER_VERSION,
        ParquetUtils.AVRO_WRITE_OLD_LIST_STRUCTURE,
        ParquetUtils.AVRO_ADD_LIST_ELEMENT_RECORDS,
        REMOVE_CRC_FILES
    );

    @Override
    public List<AllowableValue> getCompressionTypes(final ProcessorInitializationContext context) {
        return ParquetUtils.COMPRESSION_TYPES;
    }

    @Override
    public String getDefaultCompressionType(final ProcessorInitializationContext context) {
        return CompressionCodecName.UNCOMPRESSED.name();
    }

    @Override
    public List<PropertyDescriptor> getAdditionalProperties() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public HDFSRecordWriter createHDFSRecordWriter(final ProcessContext context, final FlowFile flowFile, final Configuration conf, final Path path, final RecordSchema schema) throws IOException {

        final Schema avroSchema = AvroTypeUtil.extractAvroSchema(schema);

        final AvroParquetWriter.Builder<GenericRecord> parquetWriter = AvroParquetWriter
            .<GenericRecord>builder(HadoopOutputFile.fromPath(path, conf))
                .withSchema(avroSchema);

        final ParquetConfig parquetConfig = createParquetConfig(context, flowFile.getAttributes());
        applyCommonConfig(parquetWriter, conf, parquetConfig);

        return new AvroParquetHDFSRecordWriter(parquetWriter.build(), avroSchema);
    }

    @Override
    protected FlowFile postProcess(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Path destFile) {
        final boolean removeCRCFiles = context.getProperty(REMOVE_CRC_FILES).asBoolean();
        if (removeCRCFiles) {
            final String filename = destFile.getName();
            final String hdfsPath = destFile.getParent().toString();

            final Path crcFile = new Path(hdfsPath, "." + filename + ".crc");
            deleteQuietly(getFileSystem(), crcFile);
        }

        return flowFile;
    }
}
