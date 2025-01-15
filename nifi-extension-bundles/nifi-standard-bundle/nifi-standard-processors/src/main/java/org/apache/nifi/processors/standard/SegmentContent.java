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
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@SideEffectFree
@SupportsBatching
@Tags({"segment", "split"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Segments a FlowFile into multiple smaller segments on byte boundaries. Each segment is given the following attributes: "
        + "fragment.identifier, fragment.index, fragment.count, segment.original.filename; these attributes can then be used by the "
        + "MergeContent processor in order to reconstitute the original FlowFile")
@WritesAttributes({
    @WritesAttribute(attribute = "fragment.identifier",
            description = "All segments produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
    @WritesAttribute(attribute = "fragment.index",
            description = "A one-up number that indicates the ordering of the segments that were created from a single parent FlowFile"),
    @WritesAttribute(attribute = "fragment.count", description = "The number of segments generated from the parent FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ",
            description = "The filename will be updated to include the parent's filename, the segment index, and the segment count")})
@SeeAlso(MergeContent.class)
public class SegmentContent extends AbstractProcessor {

    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();

    public static final PropertyDescriptor SIZE = new PropertyDescriptor.Builder()
            .name("Segment Size")
            .displayName("Segment Size")
            .description("The maximum data size in bytes for each segment")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SIZE
    );

    public static final Relationship REL_SEGMENTS = new Relationship.Builder()
            .name("segments")
            .description("All segments will be sent to this relationship. If the file was small enough that it was not segmented, "
                    + "a copy of the original is sent to this relationship as well as original")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile will be sent to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SEGMENTS,
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String segmentId = UUID.randomUUID().toString();
        final long segmentSize = context.getProperty(SIZE).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue();

        final String originalFileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());

        if (flowFile.getSize() <= segmentSize) {
            flowFile = session.putAttribute(flowFile, SEGMENT_ORIGINAL_FILENAME, originalFileName);

            flowFile = session.putAttribute(flowFile, FRAGMENT_ID, segmentId);
            flowFile = session.putAttribute(flowFile, FRAGMENT_INDEX, "1");
            flowFile = session.putAttribute(flowFile, FRAGMENT_COUNT, "1");

            FlowFile clone = session.clone(flowFile);
            session.transfer(flowFile, REL_ORIGINAL);
            session.transfer(clone, REL_SEGMENTS);
            return;
        }

        int totalSegments = (int) (flowFile.getSize() / segmentSize);
        if (totalSegments * segmentSize < flowFile.getSize()) {
            totalSegments++;
        }

        final Map<String, String> segmentAttributes = new HashMap<>();
        segmentAttributes.put(SEGMENT_ORIGINAL_FILENAME, originalFileName);

        segmentAttributes.put(FRAGMENT_ID, segmentId);
        segmentAttributes.put(FRAGMENT_COUNT, String.valueOf(totalSegments));

        final Set<FlowFile> segmentSet = new LinkedHashSet<>();
        for (int i = 1; i <= totalSegments; i++) {
            final long segmentOffset = segmentSize * (i - 1);
            FlowFile segment = session.clone(flowFile, segmentOffset, Math.min(segmentSize, flowFile.getSize() - segmentOffset));
            segmentAttributes.put(FRAGMENT_INDEX, String.valueOf(i));
            segment = session.putAllAttributes(segment, segmentAttributes);
            segmentSet.add(segment);
        }

        session.transfer(segmentSet, REL_SEGMENTS);
        flowFile = FragmentAttributes.copyAttributesToOriginal(session, flowFile, segmentId, totalSegments);
        session.transfer(flowFile, REL_ORIGINAL);

        if (totalSegments <= 10) {
            getLogger().info("Segmented {} into {} segments: {}", flowFile, totalSegments, segmentSet);
        } else {
            getLogger().info("Segmented {} into {} segments", flowFile, totalSegments);
        }
    }
}
