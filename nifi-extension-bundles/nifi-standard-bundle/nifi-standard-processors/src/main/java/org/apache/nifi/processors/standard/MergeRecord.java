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
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.merge.AttributeStrategyUtil;
import org.apache.nifi.processors.standard.merge.RecordBinManager;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


@SideEffectFree
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"merge", "record", "content", "correlation", "stream", "event"})
@CapabilityDescription("This Processor merges together multiple record-oriented FlowFiles into a single FlowFile that contains all of the Records of the input FlowFiles. "
    + "This Processor works by creating 'bins' and then adding FlowFiles to these bins until they are full. Once a bin is full, all of the FlowFiles will be combined into "
    + "a single output FlowFile, and that FlowFile will be routed to the 'merged' Relationship. A bin will consist of potentially many 'like FlowFiles'. In order for two "
    + "FlowFiles to be considered 'like FlowFiles', they must have the same Schema (as identified by the Record Reader) and, if the <Correlation Attribute Name> property "
    + "is set, the same value for the specified attribute. See Processor Usage and Additional Details for more information. NOTE: this processor should NOT be configured "
    + "with Cron Driven for the Scheduling Strategy.")
@ReadsAttributes({
    @ReadsAttribute(attribute = "fragment.identifier", description = "Applicable only if the <Merge Strategy> property is set to Defragment. "
        + "All FlowFiles with the same value for this attribute will be bundled together."),
    @ReadsAttribute(attribute = "fragment.count", description = "Applicable only if the <Merge Strategy> property is set to Defragment. This "
        + "attribute must be present on all FlowFiles with the same value for the fragment.identifier attribute. All FlowFiles in the same "
        + "bundle must have the same value for this attribute. The value of this attribute indicates how many FlowFiles should be expected "
        + "in the given bundle."),
})
@WritesAttributes({
    @WritesAttribute(attribute = "record.count", description = "The merged FlowFile will have a 'record.count' attribute indicating the number of records "
        + "that were written to the FlowFile."),
    @WritesAttribute(attribute = "mime.type", description = "The MIME Type indicated by the Record Writer"),
    @WritesAttribute(attribute = "merge.count", description = "The number of FlowFiles that were merged into this bundle"),
    @WritesAttribute(attribute = "merge.bin.age", description = "The age of the bin, in milliseconds, when it was merged and output. Effectively "
        + "this is the greatest amount of time that any FlowFile in this bundle remained waiting in this processor before it was output"),
    @WritesAttribute(attribute = "merge.uuid", description = "UUID of the merged FlowFile that will be added to the original FlowFiles attributes"),
    @WritesAttribute(attribute = MergeRecord.MERGE_COMPLETION_REASON, description = "This processor allows for several thresholds to be configured for merging FlowFiles. "
        + " This attribute indicates which of the Thresholds resulted in the FlowFiles being merged. For an explanation of each of the possible values "
        + " and their meanings, see the Processor's Usage / documentation and see the 'Additional Details' page."),
    @WritesAttribute(attribute = "<Attributes from Record Writer>", description = "Any Attribute that the configured Record Writer returns will be added to the FlowFile.")
})
@SeeAlso({MergeContent.class, SplitRecord.class, PartitionRecord.class})
@UseCase(
    description = "Combine together many arbitrary Records in order to create a single, larger file",
    configuration = """
        Configure the "Record Reader" to specify a Record Reader that is appropriate for the incoming data type.
        Configure the "Record Writer" to specify a Record Writer that is appropriate for the desired output data type.
        Set "Merge Strategy" to `Bin-Packing Algorithm`.
        Set the "Minimum Bin Size" to desired file size of the merged output file. For example, a value of `1 MB` will result in not merging data until at least
            1 MB of data is available (unless the Max Bin Age is reached first). If there is no desired minimum file size, leave the default value of `0 B`.
        Set the "Minimum Number of Records" property to the minimum number of Records that should be included in the merged output file. For example, setting the value
            to `10000` ensures that the output file will have at least 10,000 Records in it (unless the Max Bin Age is reached first).
        Set the "Max Bin Age" to specify the maximum amount of time to hold data before merging. This can be thought of as a "timeout" at which time the Processor will
            merge whatever data it is, even if the "Minimum Bin Size" and "Minimum Number of Records" has not been reached. It is always recommended to set the value.
            A reasonable default might be `10 mins` if there is no other latency requirement.

        Connect the 'merged' Relationship to the next component in the flow. Auto-terminate the 'original' Relationship.
        """
)
@MultiProcessorUseCase(
    description = "Combine together many Records that have the same value for a particular field in the data, in order to create a single, larger file",
    keywords = {"merge", "combine", "aggregate", "like records", "similar data"},
    configurations = {
        @ProcessorConfiguration(
            processorClass = PartitionRecord.class,
            configuration = """
                Configure the "Record Reader" to specify a Record Reader that is appropriate for the incoming data type.
                Configure the "Record Writer" to specify a Record Writer that is appropriate for the desired output data type.

                Add a single additional property. The name of the property should describe the field on which the data is being merged together.
                The property's value should be a RecordPath that specifies which output FlowFile the Record belongs to.

                For example, to merge together data that has the same value for the "productSku" field, add a property named `productSku` with a value of `/productSku`.

                Connect the "success" Relationship to MergeRecord.
                Auto-terminate the "original" Relationship.
                """
        ),
        @ProcessorConfiguration(
            processorClass = MergeRecord.class,
            configuration = """
                Configure the "Record Reader" to specify a Record Reader that is appropriate for the incoming data type.
                Configure the "Record Writer" to specify a Record Writer that is appropriate for the desired output data type.
                Set "Merge Strategy" to `Bin-Packing Algorithm`.
                Set the "Minimum Bin Size" to desired file size of the merged output file. For example, a value of `1 MB` will result in not merging data until at least
                    1 MB of data is available (unless the Max Bin Age is reached first). If there is no desired minimum file size, leave the default value of `0 B`.
                Set the "Minimum Number of Records" property to the minimum number of Records that should be included in the merged output file. For example, setting the value
                    to `10000` ensures that the output file will have at least 10,000 Records in it (unless the Max Bin Age is reached first).
                Set the "Maximum Number of Records" property to a value at least as large as the "Minimum Number of Records." If there is no need to limit the maximum number of
                    records per file, this number can be set to a value that will never be reached such as `1000000000`.
                Set the "Max Bin Age" to specify the maximum amount of time to hold data before merging. This can be thought of as a "timeout" at which time the Processor will
                    merge whatever data it is, even if the "Minimum Bin Size" and "Minimum Number of Records" has not been reached. It is always recommended to set the value.
                    A reasonable default might be `10 mins` if there is no other latency requirement.
                Set the value of the "Correlation Attribute Name" property to the name of the property that you added in the PartitionRecord Processor. For example, if merging data
                    based on the "productSku" field, the property in PartitionRecord was named `productSku` so the value of the "Correlation Attribute Name" property should
                    be `productSku`.
                Set the "Maximum Number of Bins" property to a value that is at least as large as the different number of values that will be present for the Correlation Attribute.
                    For example, if you expect 1,000 different SKUs, set this value to at least `1001`. It is not advisable, though, to set the value above 10,000.

                Connect the 'merged' Relationship to the next component in the flow.
                Auto-terminate the 'original' Relationship.
                """
        )
    }
)
public class MergeRecord extends AbstractSessionFactoryProcessor {
    // attributes for defragmentation
    public static final String FRAGMENT_ID_ATTRIBUTE = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX_ATTRIBUTE = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT_ATTRIBUTE = FragmentAttributes.FRAGMENT_COUNT.key();

    public static final String MERGE_COUNT_ATTRIBUTE = "merge.count";
    public static final String MERGE_BIN_AGE_ATTRIBUTE = "merge.bin.age";
    public static final String MERGE_UUID_ATTRIBUTE = "merge.uuid";
    public static final String MERGE_COMPLETION_REASON = "merge.completion.reason";

    public static final AllowableValue MERGE_STRATEGY_BIN_PACK = new AllowableValue(
        "Bin-Packing Algorithm",
        "Bin-Packing Algorithm",
        "Generates 'bins' of FlowFiles and fills each bin as full as possible. FlowFiles are placed into a bin based on their size and optionally "
            + "their attributes (if the <Correlation Attribute> property is set)");
    public static final AllowableValue MERGE_STRATEGY_DEFRAGMENT = new AllowableValue(
        "Defragment",
        "Defragment",
        "Combines fragments that are associated by attributes back into a single cohesive FlowFile. If using this strategy, all FlowFiles must "
            + "have the attributes <fragment.identifier> and <fragment.count>. All FlowFiles with the same value for \"fragment.identifier\" "
            + "will be grouped together. All FlowFiles in this group must have the same value for the \"fragment.count\" attribute. The ordering of "
            + "the Records that are output is not guaranteed.");

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for reading incoming data")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();
    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing out the records")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();

    public static final PropertyDescriptor MERGE_STRATEGY = new PropertyDescriptor.Builder()
        .name("merge-strategy")
        .displayName("Merge Strategy")
        .description("Specifies the algorithm used to merge records. The 'Defragment' algorithm combines fragments that are associated by "
            + "attributes back into a single cohesive FlowFile. The 'Bin-Packing Algorithm' generates a FlowFile populated by arbitrarily "
            + "chosen FlowFiles")
        .required(true)
        .allowableValues(MERGE_STRATEGY_BIN_PACK, MERGE_STRATEGY_DEFRAGMENT)
        .defaultValue(MERGE_STRATEGY_BIN_PACK.getValue())
        .build();
    public static final PropertyDescriptor CORRELATION_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
        .name("correlation-attribute-name")
        .displayName("Correlation Attribute Name")
        .description("If specified, two FlowFiles will be binned together only if they have the same value for "
            + "this Attribute. If not specified, FlowFiles are bundled by the order in which they are pulled from the queue.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
        .build();
    public static final PropertyDescriptor MIN_SIZE = new PropertyDescriptor.Builder()
        .name("min-bin-size")
        .displayName("Minimum Bin Size")
        .description("The minimum size of for the bin")
        .required(true)
        .defaultValue("0 B")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();
    public static final PropertyDescriptor MAX_SIZE = new PropertyDescriptor.Builder()
        .name("max-bin-size")
        .displayName("Maximum Bin Size")
        .description("The maximum size for the bundle. If not specified, there is no maximum. This is a 'soft limit' in that if a FlowFile is added to a bin, "
            + "all records in that FlowFile will be added, so this limit may be exceeded by up to the number of bytes in last input FlowFile.")
        .required(false)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();

    public static final PropertyDescriptor MIN_RECORDS = new PropertyDescriptor.Builder()
        .name("min-records")
        .displayName("Minimum Number of Records")
        .description("The minimum number of records to include in a bin")
        .required(true)
        .defaultValue("1")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();
    public static final PropertyDescriptor MAX_RECORDS = new PropertyDescriptor.Builder()
        .name("max-records")
        .displayName("Maximum Number of Records")
        .description("The maximum number of Records to include in a bin. This is a 'soft limit' in that if a FlowFIle is added to a bin, all records in that FlowFile will be added, "
            + "so this limit may be exceeded by up to the number of records in the last input FlowFile.")
        .required(false)
        .defaultValue("1000")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();
    public static final PropertyDescriptor MAX_BIN_COUNT = new PropertyDescriptor.Builder()
        .name("max.bin.count")
        .displayName("Maximum Number of Bins")
        .description("Specifies the maximum number of bins that can be held in memory at any one time. "
            + "This number should not be smaller than the maximum number of concurrent threads for this Processor, "
            + "or the bins that are created will often consist only of a single incoming FlowFile.")
        .defaultValue("10")
        .required(true)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    public static final PropertyDescriptor MAX_BIN_AGE = new PropertyDescriptor.Builder()
        .name("max-bin-age")
        .displayName("Max Bin Age")
        .description("The maximum age of a Bin that will trigger a Bin to be complete. Expected format is <duration> <time unit> "
            + "where <duration> is a positive integer and time unit is one of seconds, minutes, hours")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_READER,
            RECORD_WRITER,
            MERGE_STRATEGY,
            CORRELATION_ATTRIBUTE_NAME,
            AttributeStrategyUtil.ATTRIBUTE_STRATEGY,
            MIN_RECORDS,
            MAX_RECORDS,
            MIN_SIZE,
            MAX_SIZE,
            MAX_BIN_AGE,
            MAX_BIN_COUNT
    );

    public static final Relationship REL_MERGED = new Relationship.Builder()
        .name("merged")
        .description("The FlowFile containing the merged records")
        .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The FlowFiles that were used to create the bundle")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If the bundle cannot be created, all FlowFiles that would have been used to created the bundle will be transferred to failure")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_FAILURE,
            REL_MERGED
    );

    private final AtomicReference<RecordBinManager> binManager = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnStopped
    public final void resetState() {
        final RecordBinManager manager = binManager.get();
        if (manager != null) {
            manager.purge();
        }
        binManager.set(null);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final Integer minRecords = validationContext.getProperty(MIN_RECORDS).evaluateAttributeExpressions().asInteger();
        final Integer maxRecords = validationContext.getProperty(MAX_RECORDS).evaluateAttributeExpressions().asInteger();
        if (minRecords != null && maxRecords != null && maxRecords < minRecords) {
            results.add(new ValidationResult.Builder()
                .subject("Max Records")
                .input(String.valueOf(maxRecords))
                .valid(false)
                .explanation("<Maximum Number of Records> property cannot be smaller than <Minimum Number of Records> property")
                .build());
        }
        if (minRecords != null && minRecords <= 0) {
            results.add(new ValidationResult.Builder()
                    .subject("Min Records")
                    .input(String.valueOf(minRecords))
                    .valid(false)
                    .explanation("<Minimum Number of Records> property cannot be negative or zero")
                    .build());
        }
        if (maxRecords != null && maxRecords <= 0) {
            results.add(new ValidationResult.Builder()
                    .subject("Max Records")
                    .input(String.valueOf(maxRecords))
                    .valid(false)
                    .explanation("<Maximum Number of Records> property cannot be negative or zero")
                    .build());
        }

        final Double minSize = validationContext.getProperty(MIN_SIZE).asDataSize(DataUnit.B);
        final Double maxSize = validationContext.getProperty(MAX_SIZE).asDataSize(DataUnit.B);
        if (minSize != null && maxSize != null && maxSize < minSize) {
            results.add(new ValidationResult.Builder()
                .subject("Max Size")
                .input(validationContext.getProperty(MAX_SIZE).getValue())
                .valid(false)
                .explanation("<Maximum Bin Size> property cannot be smaller than <Minimum Bin Size> property")
                .build());
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        RecordBinManager manager = binManager.get();
        while (manager == null) {
            manager = new RecordBinManager(context, sessionFactory, getLogger());
            manager.setMaxBinAge(context.getProperty(MAX_BIN_AGE).asTimePeriod(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
            final boolean updated = binManager.compareAndSet(null, manager);
            if (!updated) {
                manager = binManager.get();
            }
        }

        boolean flowFilePolled = false;
        while (isScheduled()) {
            final ProcessSession session = sessionFactory.createSession();
            final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(250, DataUnit.KB, 250));
            if (flowFiles.isEmpty()) {
                break;
            }

            flowFilePolled = true;
            if (getLogger().isDebugEnabled()) {
                final List<String> ids = flowFiles.stream().map(ff -> "id=" + ff.getId()).collect(Collectors.toList());
                getLogger().debug("Pulled {} FlowFiles from queue: {}", ids.size(), ids);
            }

            final String mergeStrategy = context.getProperty(MERGE_STRATEGY).getValue();
            final boolean block;
            if (MERGE_STRATEGY_DEFRAGMENT.getValue().equals(mergeStrategy)) {
                block = true;
            } else if (context.getProperty(CORRELATION_ATTRIBUTE_NAME).isSet()) {
                block = true;
            } else {
                block = false;
            }

            try {
                for (final FlowFile flowFile : flowFiles) {
                    try {
                        binFlowFile(context, flowFile, session, manager, block);
                    } catch (final Exception e) {
                        getLogger().error("Failed to bin {} due to {}", flowFile, e, e);
                        session.transfer(flowFile, REL_FAILURE);
                    }
                }
            } finally {
                session.commitAsync();
            }

            // Complete any bins that have reached their expiration date
            try {
                manager.completeExpiredBins();
            } catch (final Exception e) {
                getLogger().error("Failed to merge FlowFiles to create new bin due to {}", e, e);
            }
        }

        if (isScheduled()) {
            // Complete any bins that have reached their expiration date
            try {
                manager.completeExpiredBins();
            } catch (final Exception e) {
                getLogger().error("Failed to merge FlowFiles to create new bin due to {}", e, e);
            }

            try {
                if (flowFilePolled) {
                    // At least one new FlowFile was pulled in. Only complete the bins that are entirely full
                    manager.completeFullBins();
                } else {
                    // No FlowFiles available. Complete any bins that meet their minimum size requirements
                    manager.completeFullEnoughBins();

                    getLogger().debug("No more FlowFiles to bin; will yield");
                    context.yield();
                }
            } catch (final Exception e) {
                getLogger().error("Failed to merge FlowFiles to create new bin due to {}", e, e);
            }
        }
    }


    private void binFlowFile(final ProcessContext context, final FlowFile flowFile, final ProcessSession session, final RecordBinManager binManager, final boolean block) {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        try (final InputStream in = session.read(flowFile);
            final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

            final RecordSchema schema = reader.getSchema();

            final String groupId = getGroupId(context, flowFile, schema, session);
            getLogger().debug("Got Group ID {} for {}", groupId, flowFile);

            binManager.add(groupId, flowFile, reader, session, block);
        } catch (MalformedRecordException | IOException | SchemaNotFoundException e) {
            throw new ProcessException(e);
        }
    }

    protected String getGroupId(final ProcessContext context, final FlowFile flowFile, final RecordSchema schema, final ProcessSession session) {
        final String mergeStrategy = context.getProperty(MERGE_STRATEGY).getValue();
        if (MERGE_STRATEGY_DEFRAGMENT.getValue().equals(mergeStrategy)) {
            return flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);
        }

        final Optional<String> optionalText = schema.getSchemaText();
        final String schemaText = optionalText.orElseGet(() -> AvroTypeUtil.extractAvroSchema(schema).toString());

        final String groupId;
        final String correlationshipAttributeName = context.getProperty(CORRELATION_ATTRIBUTE_NAME).getValue();
        if (correlationshipAttributeName != null) {
            final String correlationAttr = flowFile.getAttribute(correlationshipAttributeName);
            groupId = correlationAttr == null ? schemaText : schemaText + correlationAttr;
        } else {
            groupId = schemaText;
        }

        return groupId;
    }

    int getBinCount() {
        return binManager.get().getBinCount();
    }

}
