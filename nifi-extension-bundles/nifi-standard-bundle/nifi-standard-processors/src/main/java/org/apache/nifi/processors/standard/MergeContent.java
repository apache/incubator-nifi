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

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.flowfile.attributes.StandardFlowFileMediaType;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.bin.Bin;
import org.apache.nifi.processor.util.bin.BinFiles;
import org.apache.nifi.processor.util.bin.BinManager;
import org.apache.nifi.processor.util.bin.BinProcessingResult;
import org.apache.nifi.processors.standard.merge.AttributeStrategy;
import org.apache.nifi.processors.standard.merge.AttributeStrategyUtil;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FlowFilePackager;
import org.apache.nifi.util.FlowFilePackagerV1;
import org.apache.nifi.util.FlowFilePackagerV2;
import org.apache.nifi.util.FlowFilePackagerV3;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;


@SideEffectFree
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"merge", "content", "correlation", "tar", "zip", "stream", "concatenation", "archive", "flowfile-stream", "flowfile-stream-v3"})
@CapabilityDescription("Merges a Group of FlowFiles together based on a user-defined strategy and packages them into a single FlowFile. "
    + "It is recommended that the Processor be configured with only a single incoming connection, as Group of FlowFiles will not be "
    + "created from FlowFiles in different connections. This processor updates the mime.type attribute as appropriate. "
    + "NOTE: this processor should NOT be configured with Cron Driven for the Scheduling Strategy.")
@ReadsAttributes({
    @ReadsAttribute(attribute = "fragment.identifier", description = "Applicable only if the <Merge Strategy> property is set to Defragment. "
        + "All FlowFiles with the same value for this attribute will be bundled together."),
    @ReadsAttribute(attribute = "fragment.index", description = "Applicable only if the <Merge Strategy> property is set to Defragment. "
        + "This attribute indicates the order in which the fragments should be assembled. This "
        + "attribute must be present on all FlowFiles when using the Defragment Merge Strategy and must be a unique (i.e., unique across all "
        + "FlowFiles that have the same value for the \"fragment.identifier\" attribute) integer "
        + "between 0 and the value of the fragment.count attribute. If two or more FlowFiles have the same value for the "
        + "\"fragment.identifier\" attribute and the same value for the \"fragment.index\" attribute, the first FlowFile processed will be "
        + "accepted and subsequent FlowFiles will not be accepted into the Bin."),
    @ReadsAttribute(attribute = "fragment.count", description = "Applicable only if the <Merge Strategy> property is set to Defragment. This "
        + "attribute indicates how many FlowFiles should be expected in the given bundle. At least one FlowFile must have this attribute in "
        + "the bundle. If multiple FlowFiles contain the \"fragment.count\" attribute in a given bundle, all must have the same value."),
    @ReadsAttribute(attribute = "segment.original.filename", description = "Applicable only if the <Merge Strategy> property is set to Defragment. "
        + "This attribute must be present on all FlowFiles with the same value for the fragment.identifier attribute. All FlowFiles in the same "
        + "bundle must have the same value for this attribute. The value of this attribute will be used for the filename of the completed merged "
        + "FlowFile."),
    @ReadsAttribute(attribute = "tar.permissions", description = "Applicable only if the <Merge Format> property is set to TAR. The value of this "
        + "attribute must be 3 characters; each character must be in the range 0 to 7 (inclusive) and indicates the file permissions that should "
        + "be used for the FlowFile's TAR entry. If this attribute is missing or has an invalid value, the default value of 644 will be used") })
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "When more than 1 file is merged, the filename comes from the segment.original.filename "
        + "attribute. If that attribute does not exist in the source FlowFiles, then the filename is set to the number of nanoseconds matching "
        + "system time. Then a filename extension may be applied:"
        + "if Merge Format is TAR, then the filename will be appended with .tar, "
        + "if Merge Format is ZIP, then the filename will be appended with .zip, "
        + "if Merge Format is FlowFileStream, then the filename will be appended with .pkg"),
    @WritesAttribute(attribute = "merge.count", description = "The number of FlowFiles that were merged into this bundle"),
    @WritesAttribute(attribute = "merge.bin.age", description = "The age of the bin, in milliseconds, when it was merged and output. Effectively "
        + "this is the greatest amount of time that any FlowFile in this bundle remained waiting in this processor before it was output"),
    @WritesAttribute(attribute = "merge.uuid", description = "UUID of the merged flow file that will be added to the original flow files attributes."),
    @WritesAttribute(attribute = "merge.reason", description = "This processor allows for several thresholds to be configured for merging FlowFiles. This attribute indicates which of the Thresholds" +
        " resulted in the FlowFiles being merged. For an explanation of each of the possible values and their meanings, see the Processor's Usage / documentation and see the 'Additional Details' " +
        "page.")
})
@SeeAlso({SegmentContent.class, MergeRecord.class})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "While content is not stored in memory, the FlowFiles' attributes are. " +
    "The configuration of MergeContent (maximum bin size, maximum group size, maximum bin age, max number of entries) will influence how much " +
    "memory is used. If merging together many small FlowFiles, a two-stage approach may be necessary in order to avoid excessive use of memory.")
@UseCase(
    description = "Concatenate FlowFiles with textual content together in order to create fewer, larger FlowFiles.",
    keywords = {"concatenate", "bundle", "aggregate", "bin", "merge", "combine", "smash"},
    configuration = """
        "Merge Strategy" = "Bin Packing Algorithm"
        "Merge Format" = "Binary Concatenation"
        "Delimiter Strategy" = "Text"
        "Demarcator" = "\\n" (a newline can be inserted by pressing Shift + Enter)
        "Minimum Number of Entries" = "1"
        "Maximum Number of Entries" = "500000000"
        "Minimum Group Size" = the minimum amount of data to write to an output FlowFile. A reasonable value might be "128 MB"
        "Maximum Group Size" = the maximum amount of data to write to an output FlowFile. A reasonable value might be "256 MB"
        "Max Bin Age" = the maximum amount of time to wait for incoming data before timing out and transferring the FlowFile along even though it is smaller \
        than the Max Bin Age. A reasonable value might be "5 mins"
        """
)
@UseCase(
    description = "Concatenate FlowFiles with binary content together in order to create fewer, larger FlowFiles.",
    notes = "Not all binary data can be concatenated together. Whether or not this configuration is valid depends on the type of your data.",
    keywords = {"concatenate", "bundle", "aggregate", "bin", "merge", "combine", "smash"},
    configuration = """
        "Merge Strategy" = "Bin Packing Algorithm"
        "Merge Format" = "Binary Concatenation"
        "Delimiter Strategy" = "Text"
        "Minimum Number of Entries" = "1"
        "Maximum Number of Entries" = "500000000"
        "Minimum Group Size" = the minimum amount of data to write to an output FlowFile. A reasonable value might be "128 MB"
        "Maximum Group Size" = the maximum amount of data to write to an output FlowFile. A reasonable value might be "256 MB"
        "Max Bin Age" = the maximum amount of time to wait for incoming data before timing out and transferring the FlowFile along even though it is smaller \
        than the Max Bin Age. A reasonable value might be "5 mins"
        """
)
@UseCase(
    description = "Reassemble a FlowFile that was previously split apart into smaller FlowFiles by a processor such as SplitText, UnpackContext, SplitRecord, etc.",
    keywords = {"reassemble", "repack", "merge", "recombine"},
    configuration = """
        "Merge Strategy" = "Defragment"
        "Merge Format" = the value of Merge Format depends on the desired output format. If the file was previously zipped together and was split apart by UnpackContent,
            a Merge Format of "ZIP" makes sense. If it was previously a .tar file, a Merge Format of "TAR" makes sense. If the data is textual, "Binary Concatenation" can be
            used to combine the text into a single document.
        "Delimiter Strategy" = "Text"
        "Max Bin Age" = the maximum amount of time to wait for incoming data before timing out and transferring the fragments to 'failure'. A reasonable value might be "5 mins"

        For textual data, "Demarcator" should be set to a newline (\\n), set by pressing Shift+Enter in the UI. For binary data, "Demarcator" should be left blank.
        """
)

public class MergeContent extends BinFiles {

    // preferred attributes
    public static final String FRAGMENT_ID_ATTRIBUTE = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX_ATTRIBUTE = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT_ATTRIBUTE = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    public static final AllowableValue METADATA_STRATEGY_USE_FIRST = new AllowableValue("Use First Metadata", "Use First Metadata",
        "For any input format that supports metadata (Avro, e.g.), the metadata for the first FlowFile in the bin will be set on the output FlowFile.");

    public static final AllowableValue METADATA_STRATEGY_ALL_COMMON = new AllowableValue("Keep Only Common Metadata", "Keep Only Common Metadata",
        "For any input format that supports metadata (Avro, e.g.), any FlowFile whose metadata values match those of the first FlowFile, any additional metadata "
            + "will be dropped but the FlowFile will be merged. Any FlowFile whose metadata values do not match those of the first FlowFile in the bin will not be merged.");

    public static final AllowableValue METADATA_STRATEGY_IGNORE = new AllowableValue("Ignore Metadata", "Ignore Metadata",
        "Ignores (does not transfer, compare, etc.) any metadata from a FlowFile whose content supports embedded metadata.");

    public static final AllowableValue METADATA_STRATEGY_DO_NOT_MERGE = new AllowableValue("Do Not Merge Uncommon Metadata", "Do Not Merge Uncommon Metadata",
        "For any input format that supports metadata (Avro, e.g.), any FlowFile whose metadata values do not match those of the first FlowFile in the bin will not be merged.");

    public static final AllowableValue MERGE_STRATEGY_BIN_PACK = new AllowableValue(
        "Bin-Packing Algorithm",
        "Bin-Packing Algorithm",
        "Generates 'bins' of FlowFiles and fills each bin as full as possible. FlowFiles are placed into a bin based on their size and optionally "
            + "their attributes (if the <Correlation Attribute> property is set)");
    public static final AllowableValue MERGE_STRATEGY_DEFRAGMENT = new AllowableValue(
        "Defragment",
        "Defragment",
        "Combines fragments that are associated by attributes back into a single cohesive FlowFile. If using this strategy, all FlowFiles must "
            + "have the attributes <fragment.identifier>, <fragment.count>, and <fragment.index>. All FlowFiles with the same value for \"fragment.identifier\" "
            + "will be grouped together. All FlowFiles in this group must have the same value for the \"fragment.count\" attribute. All FlowFiles "
            + "in this group must have a unique value for the \"fragment.index\" attribute between 0 and the value of the \"fragment.count\" attribute.");

    public static final AllowableValue DELIMITER_STRATEGY_FILENAME = new AllowableValue(
        "Filename", "Filename", "The values of Header, Footer, and Demarcator will be retrieved from the contents of a file");
    public static final AllowableValue DELIMITER_STRATEGY_TEXT = new AllowableValue(
        "Text", "Text", "The values of Header, Footer, and Demarcator will be specified as property values");
    public static final AllowableValue DELIMITER_STRATEGY_NONE = new AllowableValue(
        "Do Not Use Delimiters", "Do Not Use Delimiters", "No Header, Footer, or Demarcator will be used");

    public static final String MERGE_FORMAT_TAR_VALUE = "TAR";
    public static final String MERGE_FORMAT_ZIP_VALUE = "ZIP";
    public static final String MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE = "FlowFile Stream, v3";
    public static final String MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE = "FlowFile Stream, v2";
    public static final String MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE = "FlowFile Tar, v1";
    public static final String MERGE_FORMAT_CONCAT_VALUE = "Binary Concatenation";
    public static final String MERGE_FORMAT_AVRO_VALUE = "Avro";

    public static final AllowableValue MERGE_FORMAT_TAR = new AllowableValue(
        MERGE_FORMAT_TAR_VALUE,
        MERGE_FORMAT_TAR_VALUE,
        "A bin of FlowFiles will be combined into a single TAR file. The FlowFiles' <path> attribute will be used to create a directory in the "
            + "TAR file if the <Keep Paths> property is set to true; otherwise, all FlowFiles will be added at the root of the TAR file. "
            + "If a FlowFile has an attribute named <tar.permissions> that is 3 characters, each between 0-7, that attribute will be used "
            + "as the TAR entry's 'mode'.");
    public static final AllowableValue MERGE_FORMAT_ZIP = new AllowableValue(
        MERGE_FORMAT_ZIP_VALUE,
        MERGE_FORMAT_ZIP_VALUE,
        "A bin of FlowFiles will be combined into a single ZIP file. The FlowFiles' <path> attribute will be used to create a directory in the "
            + "ZIP file if the <Keep Paths> property is set to true; otherwise, all FlowFiles will be added at the root of the ZIP file. "
            + "The <Compression Level> property indicates the ZIP compression to use.");
    public static final AllowableValue MERGE_FORMAT_FLOWFILE_STREAM_V3 = new AllowableValue(
        MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE,
        MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE,
        "A bin of FlowFiles will be combined into a single Version 3 FlowFile Stream");
    public static final AllowableValue MERGE_FORMAT_FLOWFILE_STREAM_V2 = new AllowableValue(
        MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE,
        MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE,
        "A bin of FlowFiles will be combined into a single Version 2 FlowFile Stream");
    public static final AllowableValue MERGE_FORMAT_FLOWFILE_TAR_V1 = new AllowableValue(
        MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE,
        MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE,
        "A bin of FlowFiles will be combined into a single Version 1 FlowFile Package");
    public static final AllowableValue MERGE_FORMAT_CONCAT = new AllowableValue(
        MERGE_FORMAT_CONCAT_VALUE,
        MERGE_FORMAT_CONCAT_VALUE,
        "The contents of all FlowFiles will be concatenated together into a single FlowFile");
    public static final AllowableValue MERGE_FORMAT_AVRO = new AllowableValue(
        MERGE_FORMAT_AVRO_VALUE,
        MERGE_FORMAT_AVRO_VALUE,
        "The Avro contents of all FlowFiles will be concatenated together into a single FlowFile");

    public static final String TAR_PERMISSIONS_ATTRIBUTE = "tar.permissions";
    public static final String MERGE_COUNT_ATTRIBUTE = "merge.count";
    public static final String MERGE_BIN_AGE_ATTRIBUTE = "merge.bin.age";
    public static final String MERGE_UUID_ATTRIBUTE = "merge.uuid";
    public static final String REASON_FOR_MERGING = "merge.reason";

    public static final PropertyDescriptor MERGE_STRATEGY = new PropertyDescriptor.Builder()
        .name("Merge Strategy")
        .description("Specifies the algorithm used to merge content. The 'Defragment' algorithm combines fragments that are associated by "
            + "attributes back into a single cohesive FlowFile. The 'Bin-Packing Algorithm' generates a FlowFile populated by arbitrarily "
            + "chosen FlowFiles")
        .required(true)
        .allowableValues(MERGE_STRATEGY_BIN_PACK, MERGE_STRATEGY_DEFRAGMENT)
        .defaultValue(MERGE_STRATEGY_BIN_PACK.getValue())
        .build();
    public static final PropertyDescriptor MERGE_FORMAT = new PropertyDescriptor.Builder()
        .required(true)
        .name("Merge Format")
        .description("Determines the format that will be used to merge the content.")
        .allowableValues(MERGE_FORMAT_TAR, MERGE_FORMAT_ZIP, MERGE_FORMAT_FLOWFILE_STREAM_V3, MERGE_FORMAT_FLOWFILE_STREAM_V2, MERGE_FORMAT_FLOWFILE_TAR_V1, MERGE_FORMAT_CONCAT, MERGE_FORMAT_AVRO)
        .defaultValue(MERGE_FORMAT_CONCAT.getValue())
        .build();

    public static final PropertyDescriptor METADATA_STRATEGY = new PropertyDescriptor.Builder()
        .required(true)
        .name("mergecontent-metadata-strategy")
        .displayName("Metadata Strategy")
        .description("For FlowFiles whose input format supports metadata (Avro, e.g.), this property determines which metadata should be added to the bundle. "
            + "If 'Use First Metadata' is selected, the metadata keys/values from the first FlowFile to be bundled will be used. If 'Keep Only Common Metadata' is selected, "
            + "only the metadata that exists on all FlowFiles in the bundle, with the same value, will be preserved. If 'Ignore Metadata' is selected, no metadata is transferred to "
            + "the outgoing bundled FlowFile. If 'Do Not Merge Uncommon Metadata' is selected, any FlowFile whose metadata values do not match those of the first bundled FlowFile "
            + "will not be merged.")
        .allowableValues(METADATA_STRATEGY_USE_FIRST, METADATA_STRATEGY_ALL_COMMON, METADATA_STRATEGY_DO_NOT_MERGE, METADATA_STRATEGY_IGNORE)
        .defaultValue(METADATA_STRATEGY_DO_NOT_MERGE.getValue())
        .dependsOn(MERGE_FORMAT, MERGE_FORMAT_AVRO)
        .build();

    public static final PropertyDescriptor CORRELATION_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
        .name("Correlation Attribute Name")
        .description("If specified, like FlowFiles will be binned together, where 'like FlowFiles' means FlowFiles that have the same value for "
            + "this Attribute. If not specified, FlowFiles are bundled by the order in which they are pulled from the queue.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
        .dependsOn(MERGE_STRATEGY, MERGE_STRATEGY_BIN_PACK)
        .build();

    public static final PropertyDescriptor DELIMITER_STRATEGY = new PropertyDescriptor.Builder()
        .required(true)
        .name("Delimiter Strategy")
        .description("Determines if Header, Footer, and Demarcator should point to files containing the respective content, or if "
            + "the values of the properties should be used as the content.")
        .allowableValues(DELIMITER_STRATEGY_NONE, DELIMITER_STRATEGY_FILENAME, DELIMITER_STRATEGY_TEXT)
        .defaultValue(DELIMITER_STRATEGY_NONE.getValue())
        .dependsOn(MERGE_FORMAT, MERGE_FORMAT_CONCAT_VALUE)
        .build();
    public static final PropertyDescriptor HEADER = new PropertyDescriptor.Builder()
        .name("Header File")
        .displayName("Header")
        .description("Filename or text specifying the header to use. If not specified, no header is supplied.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(DELIMITER_STRATEGY, DELIMITER_STRATEGY_FILENAME, DELIMITER_STRATEGY_TEXT)
        .dependsOn(MERGE_FORMAT, MERGE_FORMAT_CONCAT)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.TEXT)
        .build();
    public static final PropertyDescriptor FOOTER = new PropertyDescriptor.Builder()
        .name("Footer File")
        .displayName("Footer")
        .description("Filename or text specifying the footer to use. If not specified, no footer is supplied.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(DELIMITER_STRATEGY, DELIMITER_STRATEGY_FILENAME, DELIMITER_STRATEGY_TEXT)
        .dependsOn(MERGE_FORMAT, MERGE_FORMAT_CONCAT)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.TEXT)
        .build();
    public static final PropertyDescriptor DEMARCATOR = new PropertyDescriptor.Builder()
        .name("Demarcator File")
        .displayName("Demarcator")
        .description("Filename or text specifying the demarcator to use. If not specified, no demarcator is supplied.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .dependsOn(DELIMITER_STRATEGY, DELIMITER_STRATEGY_FILENAME, DELIMITER_STRATEGY_TEXT)
        .dependsOn(MERGE_FORMAT, MERGE_FORMAT_CONCAT)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.TEXT)
        .build();
    public static final PropertyDescriptor COMPRESSION_LEVEL = new PropertyDescriptor.Builder()
        .name("Compression Level")
        .description("Specifies the compression level to use when using the Zip Merge Format; if not using the Zip Merge Format, this value is "
            + "ignored")
        .required(true)
        .allowableValues("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
        .defaultValue("1")
        .dependsOn(MERGE_FORMAT, MERGE_FORMAT_ZIP)
        .build();
    public static final PropertyDescriptor KEEP_PATH = new PropertyDescriptor.Builder()
        .name("Keep Path")
        .description("If using the Zip or Tar Merge Format, specifies whether or not the FlowFiles' paths should be included in their entry names.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .dependsOn(MERGE_FORMAT, MERGE_FORMAT_TAR, MERGE_FORMAT_ZIP)
        .build();
    public static final PropertyDescriptor TAR_MODIFIED_TIME = new PropertyDescriptor.Builder()
        .name("Tar Modified Time")
        .description("If using the Tar Merge Format, specifies if the Tar entry should store the modified timestamp either by expression "
            + "(e.g. ${file.lastModifiedTime} or static value, both of which must match the ISO8601 format 'yyyy-MM-dd'T'HH:mm:ssZ'.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("${file.lastModifiedTime}")
        .dependsOn(MERGE_FORMAT, MERGE_FORMAT_TAR)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            MERGE_STRATEGY,
            MERGE_FORMAT,
            AttributeStrategyUtil.ATTRIBUTE_STRATEGY,
            CORRELATION_ATTRIBUTE_NAME,
            METADATA_STRATEGY,
            addBinPackingDependency(MIN_ENTRIES),
            addBinPackingDependency(MAX_ENTRIES),
            addBinPackingDependency(MIN_SIZE),
            addBinPackingDependency(MAX_SIZE),
            MAX_BIN_AGE,
            MAX_BIN_COUNT,
            DELIMITER_STRATEGY,
            HEADER,
            FOOTER,
            DEMARCATOR,
            COMPRESSION_LEVEL,
            KEEP_PATH,
            TAR_MODIFIED_TIME
    );

    public static final Relationship REL_MERGED = new Relationship.Builder().name("merged").description("The FlowFile containing the merged content").build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_FAILURE,
            REL_MERGED
    );

    public static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

    // Convenience method to make creation of property descriptors cleaner
    private static PropertyDescriptor addBinPackingDependency(final PropertyDescriptor original) {
        return new PropertyDescriptor.Builder().fromPropertyDescriptor(original).dependsOn(MERGE_STRATEGY, MERGE_STRATEGY_BIN_PACK).build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> additionalCustomValidation(ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String delimiterStrategy = context.getProperty(DELIMITER_STRATEGY).getValue();
        final String mergeFormat = context.getProperty(MERGE_FORMAT).getValue();
        if (DELIMITER_STRATEGY_FILENAME.getValue().equals(delimiterStrategy) && MERGE_FORMAT_CONCAT.getValue().equals(mergeFormat)) {
            final String headerValue = context.getProperty(HEADER).getValue();
            if (headerValue != null) {
                results.add(StandardValidators.FILE_EXISTS_VALIDATOR.validate(HEADER.getName(), headerValue, context));
            }

            final String footerValue = context.getProperty(FOOTER).getValue();
            if (footerValue != null) {
                results.add(StandardValidators.FILE_EXISTS_VALIDATOR.validate(FOOTER.getName(), footerValue, context));
            }

            final String demarcatorValue = context.getProperty(DEMARCATOR).getValue();
            if (demarcatorValue != null) {
                results.add(StandardValidators.FILE_EXISTS_VALIDATOR.validate(DEMARCATOR.getName(), demarcatorValue, context));
            }
        }
        return results;
    }

    private byte[] readContent(final String filename) throws IOException {
        return Files.readAllBytes(Paths.get(filename));
    }

    @Override
    protected FlowFile preprocessFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        return flowFile;
    }

    @Override
    protected String getGroupId(final ProcessContext context, final FlowFile flowFile, final ProcessSession session) {
        final String correlationAttributeName = context.getProperty(CORRELATION_ATTRIBUTE_NAME)
            .evaluateAttributeExpressions(flowFile).getValue();
        String groupId = correlationAttributeName == null ? null : flowFile.getAttribute(correlationAttributeName);

        // when MERGE_STRATEGY is Defragment and correlationAttributeName is null then bin by fragment.identifier
        if (groupId == null && MERGE_STRATEGY_DEFRAGMENT.getValue().equals(context.getProperty(MERGE_STRATEGY).getValue())) {
            groupId = flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);
        }

        return groupId;
    }

    @Override
    protected void setUpBinManager(final BinManager binManager, final ProcessContext context) {
        if (MERGE_STRATEGY_DEFRAGMENT.getValue().equals(context.getProperty(MERGE_STRATEGY).getValue())) {
            binManager.setFileCountAttribute(FRAGMENT_COUNT_ATTRIBUTE);
        } else {
            binManager.setFileCountAttribute(null);
        }
    }

    @Override
    protected BinProcessingResult processBin(final Bin bin, final ProcessContext context) throws ProcessException {
        final BinProcessingResult binProcessingResult = new BinProcessingResult(true);
        final String mergeFormat = context.getProperty(MERGE_FORMAT).getValue();
        MergeBin merger = switch (mergeFormat) {
            case MERGE_FORMAT_TAR_VALUE -> new TarMerge();
            case MERGE_FORMAT_ZIP_VALUE -> new ZipMerge(context.getProperty(COMPRESSION_LEVEL).asInteger());
            case MERGE_FORMAT_FLOWFILE_STREAM_V3_VALUE ->
                    new FlowFileStreamMerger(new FlowFilePackagerV3(), StandardFlowFileMediaType.VERSION_3.getMediaType());
            case MERGE_FORMAT_FLOWFILE_STREAM_V2_VALUE ->
                    new FlowFileStreamMerger(new FlowFilePackagerV2(), StandardFlowFileMediaType.VERSION_2.getMediaType());
            case MERGE_FORMAT_FLOWFILE_TAR_V1_VALUE ->
                    new FlowFileStreamMerger(new FlowFilePackagerV1(), StandardFlowFileMediaType.VERSION_1.getMediaType());
            case MERGE_FORMAT_CONCAT_VALUE -> new BinaryConcatenationMerge();
            case MERGE_FORMAT_AVRO_VALUE -> new AvroMerge();
            default -> throw new AssertionError();
        };

        final AttributeStrategy attributeStrategy = AttributeStrategyUtil.strategyFor(context);

        final List<FlowFile> contents = bin.getContents();
        final ProcessSession binSession = bin.getSession();

        if (MERGE_STRATEGY_DEFRAGMENT.getValue().equals(context.getProperty(MERGE_STRATEGY).getValue())) {
            final String error = getDefragmentValidationError(bin.getContents());

            // Fail the flow files and commit them
            if (error != null) {
                final String binDescription = contents.size() <= 10 ? contents.toString() : contents.size() + " FlowFiles";
                getLogger().error("{}; routing {} to failure", error, binDescription);
                binSession.transfer(contents, REL_FAILURE);
                binSession.commitAsync();

                return binProcessingResult;
            }

            contents.sort(new FragmentComparator());
        }

        FlowFile bundle = merger.merge(bin, context);

        // keep the filename, as it is added to the bundle.
        final String filename = bundle.getAttribute(CoreAttributes.FILENAME.key());

        // merge all of the attributes
        final Map<String, String> bundleAttributes = attributeStrategy.getMergedAttributes(contents);
        bundleAttributes.put(CoreAttributes.MIME_TYPE.key(), merger.getMergedContentType());
        // restore the filename of the bundle
        bundleAttributes.put(CoreAttributes.FILENAME.key(), filename);
        bundleAttributes.put(MERGE_COUNT_ATTRIBUTE, Integer.toString(contents.size()));
        bundleAttributes.put(MERGE_BIN_AGE_ATTRIBUTE, Long.toString(bin.getBinAge()));

        bundleAttributes.put(REASON_FOR_MERGING, bin.getEvictionReason().name());

        bundle = binSession.putAllAttributes(bundle, bundleAttributes);

        final String inputDescription = contents.size() < 10 ? contents.toString() : contents.size() + " FlowFiles";

        getLogger().info("Merged {} into {}. Reason for merging: {}", inputDescription, bundle, bin.getEvictionReason());

        binSession.transfer(bundle, REL_MERGED);
        binProcessingResult.getAttributes().put(MERGE_UUID_ATTRIBUTE, bundle.getAttribute(CoreAttributes.UUID.key()));

        for (final FlowFile unmerged : merger.getUnmergedFlowFiles()) {
            final FlowFile unmergedCopy = binSession.clone(unmerged);
            binSession.transfer(unmergedCopy, REL_FAILURE);
        }

        // We haven't committed anything, parent will take care of it
        binProcessingResult.setCommitted(false);
        return binProcessingResult;
    }

    private String getDefragmentValidationError(final List<FlowFile> binContents) {
        if (binContents.isEmpty()) {
            return null;
        }

        // If we are defragmenting, all fragments must have the appropriate attributes.
        String decidedFragmentCount = null;
        String fragmentIdentifier = null;
        for (final FlowFile flowFile : binContents) {
            final String fragmentIndex = flowFile.getAttribute(FRAGMENT_INDEX_ATTRIBUTE);
            if (!isNumber(fragmentIndex)) {
                return "Cannot Defragment " + flowFile + " because it does not have an integer value for the " + FRAGMENT_INDEX_ATTRIBUTE + " attribute";
            }

            fragmentIdentifier = flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);

            final String fragmentCount = flowFile.getAttribute(FRAGMENT_COUNT_ATTRIBUTE);
            if (fragmentCount != null) {
                if (!isNumber(fragmentCount)) {
                    return "Cannot Defragment " + flowFile + " because it does not have an integer value for the " + FRAGMENT_COUNT_ATTRIBUTE + " attribute";
                } else if (decidedFragmentCount == null) {
                    decidedFragmentCount = fragmentCount;
                } else if (!decidedFragmentCount.equals(fragmentCount)) {
                    return "Cannot Defragment " + flowFile + " because it is grouped with another FlowFile, and the two have differing values for the "
                        + FRAGMENT_COUNT_ATTRIBUTE + " attribute: " + decidedFragmentCount + " and " + fragmentCount;
                }
            }
        }

        if (decidedFragmentCount == null) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because no FlowFile arrived with the " + FRAGMENT_COUNT_ATTRIBUTE + " attribute "
                + "and the expected number of fragments is unknown";
        }

        final int numericFragmentCount;
        try {
            numericFragmentCount = Integer.parseInt(decidedFragmentCount);
        } catch (final NumberFormatException nfe) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the " + FRAGMENT_COUNT_ATTRIBUTE + " has a non-integer value of " + decidedFragmentCount;
        }

        if (binContents.size() < numericFragmentCount) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the expected number of fragments is " + decidedFragmentCount + " but found only "
                + binContents.size() + " fragments";
        }

        if (binContents.size() > numericFragmentCount) {
            return "Cannot Defragment FlowFiles with Fragment Identifier " + fragmentIdentifier + " because the expected number of fragments is " + decidedFragmentCount + " but found "
                + binContents.size() + " fragments for this identifier";
        }

        return null;
    }

    private boolean isNumber(final String value) {
        if (value == null) {
            return false;
        }

        return NUMBER_PATTERN.matcher(value).matches();
    }

    private void removeFlowFileFromSession(final ProcessSession session, final FlowFile flowFile, final ProcessContext context) {
        try {
            session.remove(flowFile);
        } catch (final Exception e) {
            getLogger().error("Failed to remove merged FlowFile from the session after merge failure during \""
                + context.getProperty(MERGE_FORMAT).getValue() + "\" merge.", e);
        }
    }

    private class BinaryConcatenationMerge implements MergeBin {

        private String mimeType = "application/octet-stream";

        public BinaryConcatenationMerge() {
        }

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final List<FlowFile> contents = bin.getContents();

            final ProcessSession session = bin.getSession();
            FlowFile bundle = session.create(bin.getContents());
            final AtomicReference<String> bundleMimeTypeRef = new AtomicReference<>(null);
            try {
                bundle = session.write(bundle, out -> {
                    final byte[] header = getDelimiterContent(context, contents, HEADER);
                    if (header != null) {
                        out.write(header);
                    }

                    final byte[] demarcator = getDelimiterContent(context, contents, DEMARCATOR);

                    boolean isFirst = true;
                    final Iterator<FlowFile> itr = contents.iterator();
                    while (itr.hasNext()) {
                        final FlowFile flowFile = itr.next();
                        bin.getSession().read(flowFile, in -> StreamUtils.copy(in, out));

                        if (itr.hasNext()) {
                            if (demarcator != null) {
                                out.write(demarcator);
                            }
                        }

                        final String flowFileMimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
                        if (isFirst) {
                            bundleMimeTypeRef.set(flowFileMimeType);
                            isFirst = false;
                        } else {
                            if (bundleMimeTypeRef.get() != null && !bundleMimeTypeRef.get().equals(flowFileMimeType)) {
                                bundleMimeTypeRef.set(null);
                            }
                        }
                    }

                    final byte[] footer = getDelimiterContent(context, contents, FOOTER);
                    if (footer != null) {
                        out.write(footer);
                    }
                });
            } catch (final Exception e) {
                removeFlowFileFromSession(session, bundle, context);
                throw e;
            }

            session.getProvenanceReporter().join(contents, bundle);
            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents));
            if (bundleMimeTypeRef.get() != null) {
                this.mimeType = bundleMimeTypeRef.get();
            }

            return bundle;
        }

        private byte[] getDelimiterContent(final ProcessContext context, final List<FlowFile> wrappers, final PropertyDescriptor descriptor) throws IOException {
            final String delimiterStrategyValue = context.getProperty(DELIMITER_STRATEGY).getValue();
            if (DELIMITER_STRATEGY_FILENAME.getValue().equals(delimiterStrategyValue)) {
                return getDelimiterFileContent(context, wrappers, descriptor);
            } else if (DELIMITER_STRATEGY_TEXT.getValue().equals(delimiterStrategyValue)) {
                return getDelimiterTextContent(context, wrappers, descriptor);
            } else {
                return null;
            }
        }

        private byte[] getDelimiterFileContent(final ProcessContext context, final List<FlowFile> flowFiles, final PropertyDescriptor descriptor)
            throws IOException {
            byte[] property = null;
            if (flowFiles != null && !flowFiles.isEmpty()) {
                final FlowFile flowFile = flowFiles.getFirst();
                if (flowFile != null) {
                    final String value = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();
                    if (value != null) {
                        property = readContent(value);
                    }
                }
            }
            return property;
        }

        private byte[] getDelimiterTextContent(final ProcessContext context, final List<FlowFile> flowFiles, final PropertyDescriptor descriptor) {
            byte[] property = null;
            if (flowFiles != null && !flowFiles.isEmpty()) {
                final FlowFile flowFile = flowFiles.getFirst();
                if (flowFile != null) {
                    final String value = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();
                    if (value != null) {
                        property = value.getBytes(StandardCharsets.UTF_8);
                    }
                }
            }
            return property;
        }

        @Override
        public String getMergedContentType() {
            return mimeType;
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return Collections.emptyList();
        }
    }


    private String getPath(final FlowFile flowFile) {
        Path path = Paths.get(flowFile.getAttribute(CoreAttributes.PATH.key()));
        if (path.getNameCount() == 0) {
            return "";
        }

        if (".".equals(path.getName(0).toString())) {
            path = path.getNameCount() == 1 ? null : path.subpath(1, path.getNameCount());
        }

        return path == null ? "" : path + "/";
    }

    private String createFilename(final List<FlowFile> flowFiles) {
        if (flowFiles.size() == 1) {
            return flowFiles.getFirst().getAttribute(CoreAttributes.FILENAME.key());
        } else {
            final FlowFile ff = flowFiles.getFirst();
            final String origFilename = ff.getAttribute(SEGMENT_ORIGINAL_FILENAME);
            if (origFilename != null) {
                return origFilename;
            } else {
                return String.valueOf(System.nanoTime());
            }
        }
    }

    private class TarMerge implements MergeBin {

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final List<FlowFile> contents = bin.getContents();
            final ProcessSession session = bin.getSession();
            final boolean keepPath = context.getProperty(KEEP_PATH).asBoolean();
            FlowFile bundle = session.create(); // we don't pass the parents to the #create method because the parents belong to different sessions

            try {
                bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents) + ".tar");
                bundle = session.write(bundle, rawOut -> {
                    try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut);
                         final TarArchiveOutputStream out = new TarArchiveOutputStream(bufferedOut)) {

                        out.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
                        // if any one of the FlowFiles is larger than the default maximum tar entry size, then we set bigNumberMode to handle it
                        if (getMaxEntrySize(contents) >= TarConstants.MAXSIZE) {
                            out.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
                        }
                        for (final FlowFile flowFile : contents) {
                            final String path = keepPath ? getPath(flowFile) : "";
                            final String entryName = path + flowFile.getAttribute(CoreAttributes.FILENAME.key());

                            final TarArchiveEntry tarEntry = new TarArchiveEntry(entryName);
                            tarEntry.setSize(flowFile.getSize());
                            final String permissionsVal = flowFile.getAttribute(TAR_PERMISSIONS_ATTRIBUTE);
                            if (permissionsVal != null) {
                                try {
                                    tarEntry.setMode(Integer.parseInt(permissionsVal));
                                } catch (final Exception e) {
                                    getLogger().debug("Attribute {} of {} is set to {}; expected 3 digits between 0-7, so ignoring",
                                            TAR_PERMISSIONS_ATTRIBUTE, flowFile, permissionsVal);
                                }
                            }

                            final String modTime = context.getProperty(TAR_MODIFIED_TIME)
                                .evaluateAttributeExpressions(flowFile).getValue();
                            if (StringUtils.isNotBlank(modTime)) {
                                try {
                                    tarEntry.setModTime(Instant.parse(modTime).toEpochMilli());
                                } catch (final Exception e) {
                                    getLogger().debug("Attribute {} of {} is set to {}; expected ISO8601 format, so ignoring",
                                            TAR_MODIFIED_TIME, flowFile, modTime);
                                }
                            }

                            out.putArchiveEntry(tarEntry);

                            bin.getSession().exportTo(flowFile, out);
                            out.closeArchiveEntry();
                        }
                    }
                });
            } catch (final Exception e) {
                removeFlowFileFromSession(session, bundle, context);
                throw e;
            }

            bin.getSession().getProvenanceReporter().join(contents, bundle);
            return bundle;
        }

        private long getMaxEntrySize(final List<FlowFile> contents) {
            final OptionalLong maxSize = contents.stream()
                .parallel()
                .mapToLong(FlowFile::getSize)
                .max();
            return maxSize.orElse(0L);
        }

        @Override
        public String getMergedContentType() {
            return "application/tar";
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return Collections.emptyList();
        }
    }

    private class FlowFileStreamMerger implements MergeBin {

        private final FlowFilePackager packager;
        private final String mimeType;

        public FlowFileStreamMerger(final FlowFilePackager packager, final String mimeType) {
            this.packager = packager;
            this.mimeType = mimeType;
        }

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final ProcessSession session = bin.getSession();
            final List<FlowFile> contents = bin.getContents();

            FlowFile bundle = session.create(contents);

            try {
                bundle = session.write(bundle, rawOut -> {
                    try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut)) {
                        // we don't want the packager closing the stream. V1 creates a TAR Output Stream, which then gets
                        // closed, which in turn closes the underlying OutputStream, and we want to protect ourselves against that.
                        final OutputStream out = new NonCloseableOutputStream(bufferedOut);

                        for (final FlowFile flowFile : contents) {
                            bin.getSession().read(flowFile, rawIn -> {
                                try (final InputStream in = new BufferedInputStream(rawIn)) {
                                    final Map<String, String> attributes = new HashMap<>(flowFile.getAttributes());
                                    packager.packageFlowFile(in, out, attributes, flowFile.getSize());
                                }
                            });
                        }
                    }
                });
            } catch (final Exception e) {
                removeFlowFileFromSession(session, bundle, context);
                throw e;
            }

            bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents) + ".pkg");
            session.getProvenanceReporter().join(contents, bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return mimeType;
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return Collections.emptyList();
        }
    }

    private class ZipMerge implements MergeBin {

        private final int compressionLevel;

        private final List<FlowFile> unmerged = new ArrayList<>();

        public ZipMerge(final int compressionLevel) {
            this.compressionLevel = compressionLevel;
        }

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final boolean keepPath = context.getProperty(KEEP_PATH).asBoolean();

            final ProcessSession session = bin.getSession();
            final List<FlowFile> contents = bin.getContents();
            unmerged.addAll(contents);

            FlowFile bundle = session.create(contents);

            try {
                bundle = session.putAttribute(bundle, CoreAttributes.FILENAME.key(), createFilename(contents) + ".zip");
                bundle = session.write(bundle, rawOut -> {
                    try (final OutputStream bufferedOut = new BufferedOutputStream(rawOut);
                         final ZipOutputStream out = new ZipOutputStream(bufferedOut)) {
                        out.setLevel(compressionLevel);
                        for (final FlowFile flowFile : contents) {
                            final String path = keepPath ? getPath(flowFile) : "";
                            final String entryName = path + flowFile.getAttribute(CoreAttributes.FILENAME.key());
                            final ZipEntry zipEntry = new ZipEntry(entryName);
                            zipEntry.setSize(flowFile.getSize());
                            try {
                                out.putNextEntry(zipEntry);

                                bin.getSession().exportTo(flowFile, out);
                                out.closeEntry();
                                unmerged.remove(flowFile);
                            } catch (ZipException e) {
                                getLogger().error("Encountered exception merging {}", flowFile, e);
                            }
                        }

                        out.finish();
                        out.flush();
                    }
                });
            } catch (final Exception e) {
                removeFlowFileFromSession(session, bundle, context);
                throw e;
            }

            session.getProvenanceReporter().join(contents, bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return "application/zip";
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return unmerged;
        }
    }

    private class AvroMerge implements MergeBin {

        private final List<FlowFile> unmerged = new ArrayList<>();

        @Override
        public FlowFile merge(final Bin bin, final ProcessContext context) {
            final ProcessSession session = bin.getSession();
            final List<FlowFile> contents = bin.getContents();

            final String metadataStrategy = context.getProperty(METADATA_STRATEGY).getValue();
            final Map<String, byte[]> metadata = new TreeMap<>();
            final AtomicReference<Schema> schema = new AtomicReference<>(null);
            final AtomicReference<String> inputCodec = new AtomicReference<>(null);
            final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>());

            // we don't pass the parents to the #create method because the parents belong to different sessions
            FlowFile bundle = session.create(contents);
            try {
                bundle = session.write(bundle, rawOut -> {
                    try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                        for (final FlowFile flowFile : contents) {
                            bin.getSession().read(flowFile, in -> {
                                boolean canMerge = true;
                                try (DataFileStream<GenericRecord> reader = new DataFileStream<>(in,
                                        new GenericDatumReader<>())) {
                                    if (schema.get() == null) {
                                        // this is the first file - set up the writer, and store the
                                        // Schema & metadata we'll use.
                                        schema.set(reader.getSchema());
                                        if (!METADATA_STRATEGY_IGNORE.getValue().equals(metadataStrategy)) {
                                            for (String key : reader.getMetaKeys()) {
                                                if (!DataFileWriter.isReservedMeta(key)) {
                                                    byte[] metadatum = reader.getMeta(key);
                                                    metadata.put(key, metadatum);
                                                    writer.setMeta(key, metadatum);
                                                }
                                            }
                                        }
                                        inputCodec.set(reader.getMetaString(DataFileConstants.CODEC));
                                        if (inputCodec.get() == null) {
                                            inputCodec.set(DataFileConstants.NULL_CODEC);
                                        }
                                        writer.setCodec(CodecFactory.fromString(inputCodec.get()));
                                        writer.create(schema.get(), out);
                                    } else {
                                        // check that we're appending to the same schema
                                        if (!schema.get().equals(reader.getSchema())) {
                                            getLogger().debug("Input file {} has different schema - {}, not merging",
                                                    flowFile.getId(), reader.getSchema().getName());
                                            canMerge = false;
                                            unmerged.add(flowFile);
                                        }

                                        if (METADATA_STRATEGY_DO_NOT_MERGE.getValue().equals(metadataStrategy)
                                            || METADATA_STRATEGY_ALL_COMMON.getValue().equals(metadataStrategy)) {
                                            // check that we're appending to the same metadata
                                            for (String key : reader.getMetaKeys()) {
                                                if (!DataFileWriter.isReservedMeta(key)) {
                                                    byte[] metadatum = reader.getMeta(key);
                                                    byte[] writersMetadatum = metadata.get(key);
                                                    if (!Arrays.equals(metadatum, writersMetadatum)) {
                                                        // Ignore additional metadata if ALL_COMMON is the strategy, otherwise don't merge
                                                        if (!METADATA_STRATEGY_ALL_COMMON.getValue().equals(metadataStrategy) || writersMetadatum != null) {
                                                            getLogger().debug("Input file {} has different non-reserved metadata, not merging",
                                                                    flowFile.getId());
                                                            canMerge = false;
                                                            unmerged.add(flowFile);
                                                        }
                                                    }
                                                }
                                            }
                                        } // else the metadata in the first FlowFile was either ignored or retained in the if-clause above

                                        // check that we're appending to the same codec
                                        String thisCodec = reader.getMetaString(DataFileConstants.CODEC);
                                        if (thisCodec == null) {
                                            thisCodec = DataFileConstants.NULL_CODEC;
                                        }
                                        if (!inputCodec.get().equals(thisCodec)) {
                                            getLogger().debug("Input file {} has different codec, not merging",
                                                    flowFile.getId());
                                            canMerge = false;
                                            unmerged.add(flowFile);
                                        }
                                    }

                                    // write the Avro content from the current FlowFile to the merged OutputStream
                                    if (canMerge) {
                                        writer.appendAllFrom(reader, false);
                                    }
                                }
                            });
                        }
                        writer.flush();
                    } finally {
                        writer.close();
                    }
                });
            } catch (final Exception e) {
                removeFlowFileFromSession(session, bundle, context);
                throw e;
            }

            final Collection<FlowFile> parents;
            if (unmerged.isEmpty()) {
                parents = contents;
            } else {
                parents = new HashSet<>(contents);
                parents.removeAll(unmerged);
            }

            session.getProvenanceReporter().join(parents, bundle);
            return bundle;
        }

        @Override
        public String getMergedContentType() {
            return "application/avro-binary";
        }

        @Override
        public List<FlowFile> getUnmergedFlowFiles() {
            return unmerged;
        }
    }



    private static class FragmentComparator implements Comparator<FlowFile> {

        @Override
        public int compare(final FlowFile o1, final FlowFile o2) {
            final int fragmentIndex1 = Integer.parseInt(o1.getAttribute(FRAGMENT_INDEX_ATTRIBUTE));
            final int fragmentIndex2 = Integer.parseInt(o2.getAttribute(FRAGMENT_INDEX_ATTRIBUTE));
            return Integer.compare(fragmentIndex1, fragmentIndex2);
        }
    }

    private interface MergeBin {

        FlowFile merge(Bin bin, ProcessContext context);

        String getMergedContentType();

        List<FlowFile> getUnmergedFlowFiles();
    }

}
