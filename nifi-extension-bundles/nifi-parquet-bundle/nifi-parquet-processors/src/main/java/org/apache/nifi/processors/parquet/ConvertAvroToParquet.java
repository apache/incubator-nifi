/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.parquet;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.parquet.stream.NifiParquetOutputFile;
import org.apache.nifi.parquet.utils.ParquetConfig;
import org.apache.nifi.parquet.utils.ParquetUtils;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.parquet.utils.ParquetUtils.applyCommonConfig;
import static org.apache.nifi.parquet.utils.ParquetUtils.createParquetConfig;

@Tags({"avro", "parquet", "convert"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts Avro records into Parquet file format. The incoming FlowFile should be a valid avro file. If an incoming FlowFile does "
        + "not contain any records, an empty parquet file is the output. NOTE: Many Avro datatypes (collections, primitives, and unions of primitives, e.g.) can "
        + "be converted to parquet, but unions of collections and other complex datatypes may not be able to be converted to Parquet.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "Sets the filename to the existing filename with the extension replaced by / added to by .parquet"),
        @WritesAttribute(attribute = "record.count", description = "Sets the number of records in the parquet file.")
})
public class ConvertAvroToParquet extends AbstractProcessor {

    // Attributes
    public static final String RECORD_COUNT_ATTRIBUTE = "record.count";

    // Relationships
    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Parquet file that was converted successfully from Avro")
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Avro content that could not be processed")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS,
            FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        ParquetUtils.COMPRESSION_TYPE,
        ParquetUtils.ROW_GROUP_SIZE,
        ParquetUtils.PAGE_SIZE,
        ParquetUtils.DICTIONARY_PAGE_SIZE,
        ParquetUtils.MAX_PADDING_SIZE,
        ParquetUtils.ENABLE_DICTIONARY_ENCODING,
        ParquetUtils.ENABLE_VALIDATION,
        ParquetUtils.WRITER_VERSION
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {

            long startTime = System.currentTimeMillis();
            final AtomicInteger totalRecordCount = new AtomicInteger(0);

            final String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());

            FlowFile putFlowFile = flowFile;

            putFlowFile = session.write(flowFile, (rawIn, rawOut) -> {
                try (final InputStream in = new BufferedInputStream(rawIn);
                     final DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, new GenericDatumReader<>())) {

                    Schema avroSchema = dataFileReader.getSchema();
                    getLogger().debug(avroSchema.toString(true));
                    ParquetWriter<GenericRecord> writer = createParquetWriter(context, flowFile, rawOut, avroSchema );

                    try {
                        int recordCount = 0;
                        GenericRecord record = null;
                        while (dataFileReader.hasNext()) {
                            record = dataFileReader.next();
                            writer.write(record);
                            recordCount++;
                        }
                        totalRecordCount.set(recordCount);
                    } finally {
                        writer.close();
                    }
                }
            });

            // Add attributes and transfer to success
            StringBuilder newFilename = new StringBuilder();
            int extensionIndex = fileName.lastIndexOf(".");
            if (extensionIndex != -1) {
                newFilename.append(fileName.substring(0, extensionIndex));
            } else {
                newFilename.append(fileName);
            }
            newFilename.append(".parquet");

            Map<String, String> outAttributes = new HashMap<>();
            outAttributes.put(CoreAttributes.FILENAME.key(), newFilename.toString());
            outAttributes.put(RECORD_COUNT_ATTRIBUTE, Integer.toString(totalRecordCount.get()) );

            putFlowFile = session.putAllAttributes(putFlowFile, outAttributes);
            session.transfer(putFlowFile, SUCCESS);
            session.getProvenanceReporter().modifyContent(putFlowFile, "Converted " + totalRecordCount.get() + " records", System.currentTimeMillis() - startTime);

        } catch (final ProcessException pe) {
            getLogger().error("Transferring to failure since failed to convert {} from Avro to Parquet", flowFile, pe);
            session.transfer(flowFile, FAILURE);
        }

    }

    private ParquetWriter createParquetWriter(final ProcessContext context, final FlowFile flowFile, final OutputStream out, final Schema schema)
            throws IOException {

        NifiParquetOutputFile nifiParquetOutputFile = new NifiParquetOutputFile(out);

        final AvroParquetWriter.Builder<GenericRecord> parquetWriter = AvroParquetWriter
                .<GenericRecord>builder(nifiParquetOutputFile)
                .withSchema(schema);

        final ParquetConfig parquetConfig = createParquetConfig(context, flowFile.getAttributes());
        parquetConfig.setAvroReadCompatibility(true);
        parquetConfig.setAvroAddListElementRecords(false);
        parquetConfig.setAvroWriteOldListStructure(false);

        final Configuration conf = new Configuration();
        applyCommonConfig(parquetWriter, conf, parquetConfig);

        return parquetWriter.build();
    }

}
