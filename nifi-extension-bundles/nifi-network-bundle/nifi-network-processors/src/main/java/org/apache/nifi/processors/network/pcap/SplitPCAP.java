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
package org.apache.nifi.processors.network.pcap;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@SideEffectFree
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"PCAP", "Splitter", "Network", "Packet", "Capture", "Wireshark", "TShark", "TcpDump", "WinDump", "sniffers"})
@CapabilityDescription("Splits one pcap file into multiple pcap files based on a maximum size.")
@WritesAttributes(
        {
                @WritesAttribute(
                        attribute = SplitPCAP.ERROR_REASON_LABEL,
                        description = "The reason the FlowFile was sent to the failure relationship."
                ),
                @WritesAttribute(
                        attribute = "fragment.identifier",
                        description = "All split PCAP FlowFiles produced from the same parent PCAP FlowFile will have the same randomly generated UUID added for this attribute"
                ),
                @WritesAttribute(
                        attribute = "fragment.index",
                        description = "A one-up number that indicates the ordering of the split PCAP FlowFiles that were created from a single parent PCAP FlowFile"
                ),
                @WritesAttribute(
                        attribute = "fragment.count",
                        description = "The number of split PCAP FlowFiles generated from the parent PCAP FlowFile"
                ),
                @WritesAttribute(
                        attribute = "segment.original.filename",
                        description = "The filename of the parent PCAP FlowFile"
                )
        }
)
public class SplitPCAP extends AbstractProcessor {

    protected static final String ERROR_REASON_LABEL = "error.reason";
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    public static final PropertyDescriptor PCAP_MAX_SIZE = new PropertyDescriptor
            .Builder().name("PCAP Max Size")
            .displayName("PCAP Max Size")
            .description("Maximum size of each output PCAP file. PCAP packets larger than the configured size result in routing FlowFiles to the failure relationship.")
            .required(true)
            .defaultValue("1 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, nothing will be sent to "
                    + "this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
                    + "the unchanged FlowFile will be routed to this relationship.")
            .build();

    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("The individual PCAP 'segments' of the original PCAP FlowFile will be routed to this relationship.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PCAP_MAX_SIZE
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_FAILURE,
            REL_SPLIT
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * This method is called when a trigger event occurs in the processor.
     * It processes the incoming flow file, splits it into smaller pcap files based on the PCAP Max Size,
     * and transfers the split pcap files to the success relationship.
     * If the flow file is empty or not parseable, it is transferred to the failure relationship.
     *
     * @param context the process context
     * @param session the process session
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        FlowFile originalFlowFile = session.get();
        if (originalFlowFile == null) {
            return;
        }
        final int pcapMaxSize = context.getProperty(PCAP_MAX_SIZE.getName()).asDataSize(DataUnit.B).intValue();
        final PCAPStreamSplitterCallback callback = new PCAPStreamSplitterCallback(session, originalFlowFile, pcapMaxSize);

        try {
            session.read(originalFlowFile, callback);
        } catch (ProcessException e) {
            getLogger().error("Failed to split {}", originalFlowFile, e);
            session.remove(callback.getSplitFiles());
            session.putAttribute(originalFlowFile, ERROR_REASON_LABEL, e.getMessage());
            session.transfer(originalFlowFile, REL_FAILURE);
            return;
        }

        final String fragmentId = UUID.randomUUID().toString();
        final String originalFileName = originalFlowFile.getAttribute(CoreAttributes.FILENAME.key());
        final String originalFileNameWithoutExtension = originalFileName.substring(0, originalFileName.lastIndexOf("."));

        final List<FlowFile> splitFiles = callback.getSplitFiles();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(FRAGMENT_COUNT, String.valueOf(splitFiles.size()));
        attributes.put(FRAGMENT_ID, fragmentId);
        attributes.put(SEGMENT_ORIGINAL_FILENAME, originalFileName);

        IntStream.range(0, splitFiles.size()).forEach(index -> {
            FlowFile split = splitFiles.get(index);
            attributes.put(CoreAttributes.FILENAME.key(), "%s-%d.pcap".formatted(originalFileNameWithoutExtension, index));
            attributes.put(FRAGMENT_INDEX, Integer.toString(index));
            session.transfer(session.putAllAttributes(split, attributes), REL_SPLIT);
        });
        session.transfer(originalFlowFile, REL_ORIGINAL);
    }

    protected static class PCAPStreamSplitterCallback implements InputStreamCallback {
        private final ProcessSession session;
        private final FlowFile originalFlowFile;
        private final int pcapMaxSize;
        private final List<FlowFile> splitFiles = new ArrayList<>();

        public List<FlowFile> getSplitFiles() {
            return splitFiles;
        }

        public PCAPStreamSplitterCallback(ProcessSession session, FlowFile flowFile, int pcapMaxSize) {
            this.session = session;
            this.originalFlowFile = flowFile;
            this.pcapMaxSize = pcapMaxSize;
        }

        private Packet getNextPacket(final BufferedInputStream bufferedStream, final PCAP templatePcap, final int totalPackets) throws IOException {
            final byte[] packetHeader = new byte[Packet.PACKET_HEADER_LENGTH];
            StreamUtils.read(bufferedStream, packetHeader, Packet.PACKET_HEADER_LENGTH);

            final Packet currentPacket = new Packet(packetHeader, templatePcap);

            if (currentPacket.totalLength() > this.pcapMaxSize) {
                throw new ProcessException("PCAP Packet length [%d] larger then configured maximum [%d]".formatted(currentPacket.totalLength(), pcapMaxSize));
            }

            final int expectedLength = (int) currentPacket.expectedLength();
            final byte[] packetBody = new byte[expectedLength];
            StreamUtils.read(bufferedStream, packetBody, expectedLength);
            currentPacket.setBody(packetBody);

            if (currentPacket.isInvalid()) {
                throw new ProcessException("PCAP contains an invalid packet. Packet number [%d] is invalid - [%s]".formatted(totalPackets, currentPacket.invalidityReason()));
            }

            return currentPacket;
        }

        @Override
        public void process(final InputStream inStream) throws IOException {
            final List<Packet> loadedPackets = new ArrayList<>();
            final BufferedInputStream bufferedStream = new BufferedInputStream(inStream);
            int totalPackets = 1;

            if (bufferedStream.available() == 0) {
                throw new ProcessException("Input PCAP file empty");
            }

            final byte[] pcapHeader = new byte[PCAPHeader.PCAP_HEADER_LENGTH];
            StreamUtils.read(bufferedStream, pcapHeader, PCAPHeader.PCAP_HEADER_LENGTH);

            int currentPcapTotalLength = PCAPHeader.PCAP_HEADER_LENGTH;

            final PCAP templatePcap = new PCAP(new ByteBufferReader(pcapHeader));

            while (bufferedStream.available() > 0) {

                Packet currentPacket = getNextPacket(bufferedStream, templatePcap, totalPackets);

                if (currentPcapTotalLength + currentPacket.totalLength() > this.pcapMaxSize) {

                    templatePcap.getPackets().addAll(loadedPackets);
                    FlowFile newFlowFile = session.create(originalFlowFile);
                    try (final OutputStream out = session.write(newFlowFile)) {
                        out.write(templatePcap.toByteArray());
                        this.splitFiles.add(newFlowFile);
                    }

                    loadedPackets.clear();
                    currentPcapTotalLength = PCAPHeader.PCAP_HEADER_LENGTH;
                    templatePcap.getPackets().clear();
                }

                loadedPackets.add(currentPacket);
                totalPackets++;
                currentPcapTotalLength += currentPacket.totalLength();
            }

            // If there are any packets left over, create a new flowfile.
            if (!loadedPackets.isEmpty()) {
                templatePcap.getPackets().addAll(loadedPackets);
                FlowFile newFlowFile = session.create(originalFlowFile);
                try (final OutputStream out = session.write(newFlowFile)) {
                    out.write(templatePcap.toByteArray());
                    this.splitFiles.add(newFlowFile);
                }
            }
        }
    }
}