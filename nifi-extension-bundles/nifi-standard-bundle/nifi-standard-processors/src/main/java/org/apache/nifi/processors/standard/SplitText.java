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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.util.TextLineDemarcator;
import org.apache.nifi.stream.io.util.TextLineDemarcator.OffsetInfo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@SupportsBatching
@Tags({"split", "text"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Splits a text file into multiple smaller text files on line boundaries limited by maximum number of lines "
        + "or total size of fragment. Each output split file will contain no more than the configured number of lines or bytes. "
        + "If both Line Split Count and Maximum Fragment Size are specified, the split occurs at whichever limit is reached first. "
        + "If the first line of a fragment exceeds the Maximum Fragment Size, that line will be output in a single split file which "
        + "exceeds the configured maximum size limit. This component also allows one to specify that each split should include a header "
        + "lines. Header lines can be computed by either specifying the amount of lines that should constitute a header or by using header "
        + "marker to match against the read lines. If such match happens then the corresponding line will be treated as header. Keep in mind "
        + "that upon the first failure of header marker match, no more matches will be performed and the rest of the data will be parsed as "
        + "regular lines for a given split. If after computation of the header there are no more data, the resulting split will consists "
        + "of only header lines.")
@WritesAttributes({
    @WritesAttribute(attribute = "text.line.count", description = "The number of lines of text from the original FlowFile that were copied to this FlowFile"),
    @WritesAttribute(attribute = "fragment.size", description = "The number of bytes from the original FlowFile that were copied to this FlowFile, " +
            "including header, if applicable, which is duplicated in each split FlowFile"),
    @WritesAttribute(attribute = "fragment.identifier", description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
    @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
    @WritesAttribute(attribute = "fragment.count", description = "The number of split FlowFiles generated from the parent FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")})
@SeeAlso(MergeContent.class)
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The FlowFile with its attributes is stored in memory, not the content of the FlowFile. If many splits are generated " +
        "due to the size of the content, or how the content is configured to be split, a two-phase approach may be necessary to avoid excessive use of memory.")
public class SplitText extends AbstractProcessor {
    // attribute keys
    public static final String SPLIT_LINE_COUNT = "text.line.count";
    public static final String FRAGMENT_SIZE = FragmentAttributes.FRAGMENT_SIZE.key();
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    public static final PropertyDescriptor LINE_SPLIT_COUNT = new PropertyDescriptor.Builder()
            .name("Line Split Count")
            .description("The number of lines that will be added to each split file, excluding header lines. " +
                    "A value of zero requires Maximum Fragment Size to be set, and line count will not be considered in determining splits.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor FRAGMENT_MAX_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Fragment Size")
            .description("The maximum size of each split file, including header lines. NOTE: in the case where a " +
                    "single line exceeds this property (including headers, if applicable), that line will be output " +
                    "in a split of its own which exceeds this Maximum Fragment Size setting.")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static final PropertyDescriptor HEADER_LINE_COUNT = new PropertyDescriptor.Builder()
            .name("Header Line Count")
            .description("The number of lines that should be considered part of the header; the header lines will be duplicated to all split files")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .build();
    public static final PropertyDescriptor HEADER_MARKER = new PropertyDescriptor.Builder()
            .name("Header Line Marker Characters")
            .description("The first character(s) on the line of the datafile which signifies a header line. This value is ignored when Header Line Count is non-zero. " +
                    "The first line not containing the Header Line Marker Characters and all subsequent lines are considered non-header")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor REMOVE_TRAILING_NEWLINES = new PropertyDescriptor.Builder()
            .name("Remove Trailing Newlines")
            .description("Whether to remove newlines at the end of each split file. This should be false if you intend to merge the split files later. If this is set to "
                    + "'true' and a FlowFile is generated that contains only 'empty lines' (i.e., consists only of \\r and \\n characters), the FlowFile will not be emitted. "
                    + "Note, however, that if header lines are specified, the resultant FlowFile will never be empty as it will consist of the header lines, so "
                    + "a FlowFile may be emitted that contains only the header lines.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            LINE_SPLIT_COUNT,
            FRAGMENT_MAX_SIZE,
            HEADER_LINE_COUNT,
            HEADER_MARKER,
            REMOVE_TRAILING_NEWLINES
    );

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input file will be routed to this destination when it has been successfully split into 1 or more files")
            .build();
    public static final Relationship REL_SPLITS = new Relationship.Builder()
            .name("splits")
            .description("The split files will be routed to this destination when an input file is successfully split into 1 or more split files")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a file cannot be split for some reason, the original file will be routed to this destination and nothing will be routed elsewhere")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_SPLITS,
            REL_FAILURE
    );

    private volatile boolean removeTrailingNewLines;

    private volatile long maxSplitSize;

    private volatile int lineCount;

    private volatile int headerLineCount;

    private volatile String headerMarker;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onSchedule(ProcessContext context) {
        this.removeTrailingNewLines = context.getProperty(REMOVE_TRAILING_NEWLINES).isSet()
                ? context.getProperty(REMOVE_TRAILING_NEWLINES).asBoolean() : false;
        this.maxSplitSize = context.getProperty(FRAGMENT_MAX_SIZE).isSet()
                ? context.getProperty(FRAGMENT_MAX_SIZE).asDataSize(DataUnit.B).longValue() : Long.MAX_VALUE;
        this.lineCount = context.getProperty(LINE_SPLIT_COUNT).asInteger();
        this.headerLineCount = context.getProperty(HEADER_LINE_COUNT).asInteger();
        this.headerMarker = context.getProperty(HEADER_MARKER).getValue();
    }

    /**
     * Will split the incoming stream releasing all splits as FlowFile at once.
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession processSession) throws ProcessException {
        FlowFile sourceFlowFile = processSession.get();
        if (sourceFlowFile == null) {
            return;
        }
        AtomicBoolean error = new AtomicBoolean();
        List<SplitInfo> computedSplitsInfo = new ArrayList<>();
        AtomicReference<SplitInfo> headerSplitInfoRef = new AtomicReference<>();
        processSession.read(sourceFlowFile, in -> {
            TextLineDemarcator demarcator = new TextLineDemarcator(in);
            SplitInfo splitInfo = null;
            long startOffset = 0;

            // Compute fragment representing the header (if available)
            long start = System.nanoTime();
            try {
                if (SplitText.this.headerLineCount > 0) {
                    splitInfo = SplitText.this.computeHeader(demarcator, startOffset, SplitText.this.headerLineCount, null, null);
                    if ((splitInfo != null) && (splitInfo.lineCount < SplitText.this.headerLineCount)) {
                        error.set(true);
                        getLogger().error("Unable to split {} due to insufficient amount of header lines. Required {} but was {}. Routing to failure.",
                                sourceFlowFile, SplitText.this.headerLineCount, splitInfo.lineCount);
                    }
                } else if (SplitText.this.headerMarker != null) {
                    splitInfo = SplitText.this.computeHeader(demarcator, startOffset, Long.MAX_VALUE, SplitText.this.headerMarker.getBytes(StandardCharsets.UTF_8), null);
                }
                headerSplitInfoRef.set(splitInfo);
            } catch (IllegalStateException e) {
                error.set(true);
                getLogger().error("Routing to failure.", e);
            }

            // Compute and collect fragments representing the individual splits
            if (!error.get()) {
                if (headerSplitInfoRef.get() != null) {
                    startOffset = headerSplitInfoRef.get().length;
                }
                long preAccumulatedLength = startOffset;
                while ((splitInfo = SplitText.this.nextSplit(demarcator, startOffset, SplitText.this.lineCount, splitInfo, preAccumulatedLength)) != null) {
                    computedSplitsInfo.add(splitInfo);
                    startOffset += splitInfo.length;
                }
                long stop = System.nanoTime();
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Computed splits in {} milliseconds.", (stop - start));
                }
            }
        });

        if (error.get()) {
            processSession.transfer(sourceFlowFile, REL_FAILURE);
        } else {
            final String fragmentId = UUID.randomUUID().toString();
            final List<FlowFile> splitFlowFiles = this.generateSplitFlowFiles(fragmentId, sourceFlowFile, headerSplitInfoRef.get(), computedSplitsInfo, processSession);

            final FlowFile originalFlowFile = FragmentAttributes.copyAttributesToOriginal(processSession, sourceFlowFile, fragmentId, splitFlowFiles.size());
            processSession.transfer(originalFlowFile, REL_ORIGINAL);

            if (!splitFlowFiles.isEmpty()) {
                processSession.transfer(splitFlowFiles, REL_SPLITS);
            }
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();
        boolean invalidState = (validationContext.getProperty(LINE_SPLIT_COUNT).asInteger() == 0
                && !validationContext.getProperty(FRAGMENT_MAX_SIZE).isSet());
        results.add(new ValidationResult.Builder().subject("Maximum Fragment Size").valid(!invalidState)
                .explanation("Property must be specified when Line Split Count is 0").build());
        return results;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * Generates the list of {@link FlowFile}s representing splits. If
     * {@link SplitInfo} provided as an argument to this operation is not null
     * it signifies the header information and its contents will be included in
     * each and every computed split.
     */
    private List<FlowFile> generateSplitFlowFiles(String fragmentId, FlowFile sourceFlowFile, SplitInfo splitInfo, List<SplitInfo> computedSplitsInfo, ProcessSession processSession) {
        List<FlowFile> splitFlowFiles = new ArrayList<>();

        FlowFile headerFlowFile = null;
        long headerCrlfLength = 0;
        if (splitInfo != null) {
            headerFlowFile = processSession.clone(sourceFlowFile, splitInfo.startOffset, splitInfo.length);
            headerCrlfLength = splitInfo.trimmedLength;
        }
        int fragmentIndex = 1; // set to 1 to preserve the existing behavior *only*. Perhaps should be deprecated to follow the 0,1,2... scheme

        if (computedSplitsInfo.isEmpty() && headerFlowFile != null) {
            FlowFile splitFlowFile = processSession.clone(sourceFlowFile, 0, headerFlowFile.getSize() - headerCrlfLength);
            splitFlowFile = this.updateAttributes(processSession, splitFlowFile, 0, splitFlowFile.getSize(),
                    fragmentId, fragmentIndex++, sourceFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
            splitFlowFiles.add(splitFlowFile);
        } else {
            final Iterator<SplitInfo> itr = computedSplitsInfo.iterator();
            while (itr.hasNext()) {
                final SplitInfo computedSplitInfo = itr.next();
                itr.remove();

                long length = this.removeTrailingNewLines ? computedSplitInfo.trimmedLength : computedSplitInfo.length;
                boolean proceedWithClone = headerFlowFile != null || length > 0;
                if (proceedWithClone) {
                    FlowFile splitFlowFile = null;
                    if (headerFlowFile != null) {
                        if (length > 0) {
                            splitFlowFile = processSession.clone(sourceFlowFile, computedSplitInfo.startOffset, length);
                            splitFlowFile = this.concatenateContents(sourceFlowFile, processSession, headerFlowFile, splitFlowFile);
                        } else {
                            splitFlowFile = processSession.clone(sourceFlowFile, 0, headerFlowFile.getSize() - headerCrlfLength); // trim the last CRLF if split consists of only HEADER
                        }
                    } else {
                        splitFlowFile = processSession.clone(sourceFlowFile, computedSplitInfo.startOffset, length);
                    }

                    splitFlowFile = this.updateAttributes(processSession, splitFlowFile, computedSplitInfo.lineCount, splitFlowFile.getSize(), fragmentId, fragmentIndex++,
                            sourceFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
                    splitFlowFiles.add(splitFlowFile);
                }
            }

            // Update fragment.count with real split count (i.e. don't count files for which there was no clone)
            final String fragmentCount = String.valueOf(fragmentIndex - 1); // -1 because the index starts at 1 (see above)

            final ListIterator<FlowFile> flowFileItr = splitFlowFiles.listIterator();
            while (flowFileItr.hasNext()) {
                FlowFile splitFlowFile = flowFileItr.next();

                final FlowFile updated = processSession.putAttribute(splitFlowFile, FRAGMENT_COUNT, fragmentCount);
                flowFileItr.set(updated);
            }
        }

        getLogger().info("Split {} into {} FlowFiles{}", sourceFlowFile, splitFlowFiles.size(), headerFlowFile == null ? " containing headers." : ".");
        if (headerFlowFile != null) {
            processSession.remove(headerFlowFile);
        }

        return splitFlowFiles;
    }

    /**
     * Will concatenate the contents of the provided array of {@link FlowFile}s
     * into a single {@link FlowFile}. While this operation is as general as it
     * is described in the previous sentence, in the context of this processor
     * there can only be two {@link FlowFile}s with the first {@link FlowFile}
     * representing the header content of the split and the second
     * {@link FlowFile} represents the split itself.
     */
    private FlowFile concatenateContents(FlowFile sourceFlowFile, ProcessSession session, FlowFile... flowFiles) {
        FlowFile mergedFlowFile = session.create(sourceFlowFile);
        for (FlowFile flowFile : flowFiles) {
            mergedFlowFile = session.append(mergedFlowFile, out -> {
                try (InputStream is = session.read(flowFile)) {
                    IOUtils.copy(is, out);
                }
            });
        }
        session.remove(flowFiles[1]); // in current usage we always have 2 files
        return mergedFlowFile;
    }

    private FlowFile updateAttributes(ProcessSession processSession, FlowFile splitFlowFile, long splitLineCount, long splitFlowFileSize,
            String splitId, int splitIndex, String origFileName) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(SPLIT_LINE_COUNT, String.valueOf(splitLineCount));
        attributes.put(FRAGMENT_SIZE, String.valueOf(splitFlowFile.getSize()));
        attributes.put(FRAGMENT_ID, splitId);
        attributes.put(FRAGMENT_INDEX, String.valueOf(splitIndex));
        attributes.put(SEGMENT_ORIGINAL_FILENAME, origFileName);
        return processSession.putAllAttributes(splitFlowFile, attributes);
    }

    /**
     * Will generate {@link SplitInfo} for the next fragment that represents the
     * header of the future split.
     * <p>
     * If split size is controlled by the amount of lines in the split then the
     * resulting {@link SplitInfo} line count will always be <= 'splitMaxLineCount'. It can only be less IF it reaches the EOF.
     * If split size is controlled by the {@link #maxSplitSize}, then the resulting {@link SplitInfo} line count
     * will vary but the length of the split will never be > {@link #maxSplitSize} and {@link IllegalStateException} will be thrown.
     * This method also allows one to provide 'startsWithFilter' to allow headers to be determined via such filter (see {@link #HEADER_MARKER}.
     */
    private SplitInfo computeHeader(TextLineDemarcator demarcator, long startOffset, long splitMaxLineCount,
            byte[] startsWithFilter, SplitInfo previousSplitInfo) throws IOException {
        long length = 0;
        long actualLineCount = 0;
        OffsetInfo offsetInfo = null;
        SplitInfo splitInfo = null;
        OffsetInfo previousOffsetInfo = null;
        long lastCrlfLength = 0;
        while ((offsetInfo = demarcator.nextOffsetInfo(startsWithFilter)) != null) {
            lastCrlfLength = offsetInfo.getCrlfLength();

            if (startsWithFilter != null && !offsetInfo.isStartsWithMatch()) {
                if (offsetInfo.getCrlfLength() != -1) {
                    previousOffsetInfo = offsetInfo;
                }
                break;
            } else {
                if (length + offsetInfo.getLength() > this.maxSplitSize) {
                    throw new IllegalStateException( "Computing header resulted in header size being > MAX split size of " + this.maxSplitSize + ".");
                } else {
                    length += offsetInfo.getLength();
                    actualLineCount++;
                    if (actualLineCount == splitMaxLineCount) {
                        break;
                    }
                }
            }
        }

        if (actualLineCount > 0) {
            splitInfo = new SplitInfo(startOffset, length, lastCrlfLength, actualLineCount, previousOffsetInfo);
        }
        return splitInfo;
    }

    /**
     * Will generate {@link SplitInfo} for the next split.
     * <p>
     * If split size is controlled by the amount of lines in the split then the resulting
     * {@link SplitInfo} line count will always be <= 'splitMaxLineCount'.
     * If split size is controlled by the {@link #maxSplitSize}, then the resulting {@link SplitInfo}
     * line count will vary but the length of the split will never be > {@link #maxSplitSize}.
     */
    private SplitInfo nextSplit(TextLineDemarcator demarcator, long startOffset, long splitMaxLineCount,
            SplitInfo remainderSplitInfo, long startingLength) throws IOException {
        long length = 0;
        long trailingCrlfLength = 0;
        long trailingLineCount = 0;
        long actualLineCount = 0;
        OffsetInfo offsetInfo = null;
        SplitInfo splitInfo = null;
        // the remainder from the previous read after which it was determined that adding it would make
        // the split size > 'maxSplitSize'. So it's being carried over to the next line.
        if (remainderSplitInfo != null && remainderSplitInfo.remaningOffsetInfo != null) {
            length += remainderSplitInfo.remaningOffsetInfo.getLength();
            actualLineCount++;
        }
        OffsetInfo remaningOffsetInfo = null;
        long lastCrlfLength = 0;
        while ((offsetInfo = demarcator.nextOffsetInfo()) != null) {
            lastCrlfLength = offsetInfo.getCrlfLength();

            if (length + offsetInfo.getLength() + startingLength > this.maxSplitSize) {
                if (length == 0) { // single line per split
                    length += offsetInfo.getLength();
                    actualLineCount++;
                } else {
                    remaningOffsetInfo = offsetInfo;
                }
                break;
            }

            if (offsetInfo.getLength() == offsetInfo.getCrlfLength()) {
                trailingCrlfLength += offsetInfo.getCrlfLength();
                trailingLineCount++;
            } else if (offsetInfo.getLength() > offsetInfo.getCrlfLength()) {
                trailingCrlfLength = 0; // non-empty line came in, thus resetting counter
            }

            length += offsetInfo.getLength();
            actualLineCount++;
            if (splitMaxLineCount > 0 && actualLineCount >= splitMaxLineCount) {
                break;
            }
        }

        if (actualLineCount > 0) {
            if (length - trailingCrlfLength >= lastCrlfLength) {
                trailingCrlfLength += lastCrlfLength; // trim CRLF from the last line
            }
            actualLineCount -= trailingLineCount;
            splitInfo = new SplitInfo(startOffset, length, length - trailingCrlfLength, actualLineCount, remaningOffsetInfo);
        }
        return splitInfo;
    }

    /**
     * Container for hosting meta-information pertaining to the split so it can
     * be used later to create {@link FlowFile} representing the split.
     */
    private class SplitInfo {
        final long startOffset, length, trimmedLength, lineCount;
        OffsetInfo remaningOffsetInfo;

        SplitInfo(long startOffset, long length, long trimmedLength, long lineCount, OffsetInfo remaningOffsetInfo) {
            this.startOffset = startOffset;
            this.length = length;
            this.lineCount = lineCount;
            this.remaningOffsetInfo = remaningOffsetInfo;
            this.trimmedLength = trimmedLength;
        }

        @Override
        public String toString() {
            return "offset:" + startOffset + "; length:" + length + "; lineCount:" + lineCount;
        }
    }
}
