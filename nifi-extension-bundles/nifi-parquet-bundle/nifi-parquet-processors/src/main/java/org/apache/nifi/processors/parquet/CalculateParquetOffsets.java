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
import java.util.Optional;
import java.util.Set;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.parquet.stream.NifiParquetInputFile;
import org.apache.nifi.parquet.utils.ParquetAttribute;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;

@Tags({"parquet", "split", "partition", "break apart", "efficient processing", "load balance", "cluster"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription(
        "The processor generates N flow files from the input, and adds attributes with the offsets required to read "
                + "the group of rows in the FlowFile's content. Can be used to increase the overall efficiency of "
                + "processing extremely large Parquet files."
)
@WritesAttributes({
        @WritesAttribute(
                attribute = ParquetAttribute.RECORD_OFFSET,
                description = "Sets the index of first record of the parquet file."
        ),
        @WritesAttribute(
                attribute = ParquetAttribute.RECORD_COUNT,
                description = "Sets the number of records in the parquet file."
        )
})
@ReadsAttributes({
        @ReadsAttribute(
                attribute = ParquetAttribute.RECORD_OFFSET,
                description = "Gets the index of first record in the input."
        ),
        @ReadsAttribute(
                attribute = ParquetAttribute.RECORD_COUNT,
                description = "Gets the number of records in the input."
        ),
        @ReadsAttribute(
                attribute = ParquetAttribute.FILE_RANGE_START_OFFSET,
                description = "Gets the start offset of the selected row group in the parquet file."
        ),
        @ReadsAttribute(
                attribute = ParquetAttribute.FILE_RANGE_END_OFFSET,
                description = "Gets the end offset of the selected row group in the parquet file."
        )
})
@SideEffectFree
public class CalculateParquetOffsets extends AbstractProcessor {

    static final PropertyDescriptor PROP_RECORDS_PER_SPLIT = new PropertyDescriptor.Builder()
            .name("Records Per Split")
            .description("Specifies how many records should be covered in each FlowFile")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

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
            PROP_RECORDS_PER_SPLIT,
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
        final FlowFile inputFlowFile = session.get();
        if (inputFlowFile == null) {
            return;
        }

        final long partitionSize = context.getProperty(PROP_RECORDS_PER_SPLIT).asLong();
        final boolean zeroContentOutput = context.getProperty(PROP_ZERO_CONTENT_OUTPUT).asBoolean();

        final long recordOffset = Optional.ofNullable(inputFlowFile.getAttribute(ParquetAttribute.RECORD_OFFSET))
                .map(Long::valueOf)
                .orElse(0L);

        final long recordCount = Optional.ofNullable(inputFlowFile.getAttribute(ParquetAttribute.RECORD_COUNT))
                .map(Long::valueOf)
                .orElseGet(() -> getRecordCount(session, inputFlowFile) - recordOffset);

        List<FlowFile> partitions = getPartitions(
                session, inputFlowFile, partitionSize, recordCount, recordOffset, zeroContentOutput);
        session.transfer(partitions, REL_SUCCESS);
        session.adjustCounter("Records Split", recordCount, false);
        session.adjustCounter("Partitions Created", partitions.size(), false);

        if (zeroContentOutput) {
            session.remove(inputFlowFile);
        }
    }

    private long getRecordCount(ProcessSession session, FlowFile flowFile) {
        final long fileStartOffset = Optional.ofNullable(flowFile.getAttribute(ParquetAttribute.FILE_RANGE_START_OFFSET))
                .map(Long::parseLong)
                .orElse(0L);
        final long fileEndOffset = Optional.ofNullable(flowFile.getAttribute(ParquetAttribute.FILE_RANGE_END_OFFSET))
                .map(Long::parseLong)
                .orElse(Long.MAX_VALUE);

        final ParquetReadOptions readOptions = ParquetReadOptions.builder()
                .withRange(fileStartOffset, fileEndOffset)
                .build();
        try (
                InputStream in = session.read(flowFile);
                ParquetFileReader reader = new ParquetFileReader(
                        new NifiParquetInputFile(in, flowFile.getSize()),
                        readOptions
                )
        ) {
            return reader.getRecordCount();
        } catch (IOException e) {
            throw new ProcessException(e);
        }
    }

    private List<FlowFile> getPartitions(
            ProcessSession session,
            FlowFile inputFlowFile,
            long partitionSize,
            long recordCount,
            long recordOffset,
            boolean zeroContentOutput
    ) {
        final long numberOfPartitions = (recordCount / partitionSize) + ((recordCount % partitionSize) > 0 ? 1 : 0);
        final List<FlowFile> results = new ArrayList<>((int) Math.min(Integer.MAX_VALUE, numberOfPartitions));

        for (long currentPartition = 0; currentPartition < numberOfPartitions; currentPartition++) {
            long addedOffset = currentPartition * partitionSize;
            final FlowFile outputFlowFile;
            if (zeroContentOutput) {
                outputFlowFile = session.create(inputFlowFile);
            } else if (currentPartition == 0) {
                outputFlowFile = inputFlowFile;
            } else {
                outputFlowFile = session.clone(inputFlowFile);
            }
            results.add(
                    session.putAllAttributes(
                            outputFlowFile,
                            Map.of(ParquetAttribute.RECORD_OFFSET, Long.toString(recordOffset + addedOffset),
                                    ParquetAttribute.RECORD_COUNT, Long.toString(Math.min(partitionSize, recordCount - addedOffset)))
                    )
            );
        }

        return results;
    }
}
