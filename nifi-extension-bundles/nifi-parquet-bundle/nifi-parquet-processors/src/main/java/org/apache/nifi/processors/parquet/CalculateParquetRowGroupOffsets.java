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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.parquet.stream.NifiParquetInputFile;
import org.apache.nifi.parquet.utils.ParquetAttribute;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

@Tags({"parquet", "split", "partition", "break apart", "efficient processing", "load balance", "cluster"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription(
        "The processor generates one FlowFile from each Row Group of the input, and adds attributes with the offsets "
                + "required to read the group of rows in the FlowFile's content. Can be used to increase the overall "
                + "efficiency of processing extremely large Parquet files."
)
@WritesAttributes({
        @WritesAttribute(
                attribute = ParquetAttribute.FILE_RANGE_START_OFFSET,
                description = "Sets the start offset of the selected row group in the parquet file."
        ),
        @WritesAttribute(
                attribute = ParquetAttribute.FILE_RANGE_END_OFFSET,
                description = "Sets the end offset of the selected row group in the parquet file."
        ),
        @WritesAttribute(
                attribute = ParquetAttribute.RECORD_COUNT,
                description = "Sets the count of records in the selected row group."
        )
})
@SideEffectFree
public class CalculateParquetRowGroupOffsets extends AbstractProcessor {

    static final PropertyDescriptor PROP_ZERO_CONTENT_OUTPUT = new PropertyDescriptor.Builder()
            .name("Zero Content Output")
            .description("Whether to do, or do not copy the content of input FlowFile.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles, with special attributes that represent a chunk of the input file.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PROP_ZERO_CONTENT_OUTPUT
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
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
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final boolean zeroContentOutput = context.getProperty(PROP_ZERO_CONTENT_OUTPUT).asBoolean();

        final ParquetMetadata parquetMetadata = getParquetMetadata(session, original);
        final List<FlowFile> partitions = getPartitions(session, original, parquetMetadata.getBlocks(), zeroContentOutput);
        session.transfer(partitions, REL_SUCCESS);
        session.adjustCounter("Partitions Created", partitions.size(), false);

        if (zeroContentOutput) {
            session.remove(original);
        }
    }

    private ParquetMetadata getParquetMetadata(ProcessSession session, FlowFile flowFile) {
        final ParquetReadOptions readOptions = ParquetReadOptions.builder().build();
        try (
                final InputStream in = session.read(flowFile);
                ParquetFileReader reader = new ParquetFileReader(
                        new NifiParquetInputFile(in, flowFile.getSize()),
                        readOptions
                )
        ) {
            return reader.getFooter();
        } catch (IOException e) {
            throw new ProcessException(e);
        }
    }

    private List<FlowFile> getPartitions(
            ProcessSession session,
            FlowFile flowFile,
            List<BlockMetaData> blocks,
            boolean zeroContentOutput
    ) {
        final List<FlowFile> results = new ArrayList<>(blocks.size());

        for (int currentPartition = 0; currentPartition < blocks.size(); currentPartition++) {
            final BlockMetaData currentBlock = blocks.get(currentPartition);
            final long currentBlockStartOffset = currentBlock.getStartingPos();
            final long currentBlockEndOffset = currentBlockStartOffset + currentBlock.getCompressedSize();
            final FlowFile outputFlowFile;
            if (zeroContentOutput) {
                outputFlowFile = session.create(flowFile);
            } else if (currentPartition == 0) {
                outputFlowFile = flowFile;
            } else {
                outputFlowFile = session.clone(flowFile);
            }
            results.add(
                    session.putAllAttributes(
                            outputFlowFile,
                            Map.of(ParquetAttribute.FILE_RANGE_START_OFFSET, String.valueOf(currentBlockStartOffset),
                                    ParquetAttribute.FILE_RANGE_END_OFFSET, String.valueOf(currentBlockEndOffset),
                                    ParquetAttribute.RECORD_COUNT, String.valueOf(currentBlock.getRowCount())))
            );
        }

        return results;
    }
}
