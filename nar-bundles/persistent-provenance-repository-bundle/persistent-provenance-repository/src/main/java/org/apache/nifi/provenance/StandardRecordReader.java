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
package org.apache.nifi.provenance;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.provenance.serialization.RecordReader;

public class StandardRecordReader implements RecordReader {

    private final DataInputStream dis;
    private final ByteCountingInputStream byteCountingIn;
    private final String filename;
    private final int serializationVersion;

    public StandardRecordReader(final InputStream in, final int serializationVersion, final String filename) {
        if (serializationVersion < 1 || serializationVersion > 7) {
            throw new IllegalArgumentException("Unable to deserialize record because the version is " + serializationVersion + " and supported versions are 1-6");
        }

        byteCountingIn = new ByteCountingInputStream(in);
        this.dis = new DataInputStream(byteCountingIn);
        this.serializationVersion = serializationVersion;
        this.filename = filename;
    }

    private StandardProvenanceEventRecord readPreVersion6Record() throws IOException {
        final long startOffset = byteCountingIn.getBytesConsumed();

        if (!isData(byteCountingIn)) {
            return null;
        }

        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder();

        final long eventId = dis.readLong();
        if (serializationVersion == 4) {
            // notion of a UUID for the event was added in Version 4 so that Events can be referred to uniquely
            // across a cluster. This was then removed in version 5 because it was decided that a unique id
            // could better be generated based on the event id and the cluster node identifier.
            // Therefore, we read in the Event Identifier and throw it away.
            dis.readUTF();
        }
        final String eventTypeName = dis.readUTF();
        final ProvenanceEventType eventType = ProvenanceEventType.valueOf(eventTypeName);
        builder.setEventType(eventType);
        builder.setEventTime(dis.readLong());

        if (serializationVersion > 3) {
            // event duration introduced in version 4.
            builder.setEventDuration(dis.readLong());
        }

        dis.readLong(); // Used to persist FlowFileId
        final long fileSize = dis.readLong();

        builder.setComponentId(readNullableString(dis));
        builder.setComponentType(readNullableString(dis));
        builder.setFlowFileUUID(readNullableString(dis));

        final int numParents = dis.readInt();
        for (int i = 0; i < numParents; i++) {
            builder.addParentUuid(dis.readUTF());
        }

        if (serializationVersion > 2) {
            // notion of child UUID's was introduced in version 3.
            final int numChildren = dis.readInt();
            for (int i = 0; i < numChildren; i++) {
                builder.addChildUuid(dis.readUTF());
            }
        }

        final String sourceSystemUri = readNullableString(dis);

        if (serializationVersion > 3) {
            // notion of a source system flowfile identifier was introduced in version 4.
            builder.setSourceSystemFlowFileIdentifier(readNullableString(dis));
        }

        final String destinationSystemUri = readNullableString(dis);
        if (sourceSystemUri != null) {
            builder.setTransitUri(sourceSystemUri);
        } else if (destinationSystemUri != null) {
            builder.setTransitUri(destinationSystemUri);
        }

        readNullableString(dis);    // Content-Type No longer used

        builder.setAlternateIdentifierUri(readNullableString(dis));

        final Map<String, String> attrs = readAttributes(dis, false);

        builder.setFlowFileEntryDate(System.currentTimeMillis());
        builder.setLineageIdentifiers(Collections.<String>emptySet());
        builder.setLineageStartDate(-1L);
        builder.setAttributes(Collections.<String, String>emptyMap(), attrs);
        builder.setCurrentContentClaim(null, null, null, null, fileSize);

        builder.setStorageLocation(filename, startOffset);

        final StandardProvenanceEventRecord record = builder.build();
        record.setEventId(eventId);
        return record;
    }

    @Override
    public StandardProvenanceEventRecord nextRecord() throws IOException {
        // Schema changed drastically in version 6 so we created a new method to handle old records
        if (serializationVersion < 6) {
            return readPreVersion6Record();
        }

        final long startOffset = byteCountingIn.getBytesConsumed();

        if (!isData(byteCountingIn)) {
            return null;
        }

        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder();

        final long eventId = dis.readLong();
        final String eventTypeName = dis.readUTF();
        final ProvenanceEventType eventType = ProvenanceEventType.valueOf(eventTypeName);
        builder.setEventType(eventType);
        builder.setEventTime(dis.readLong());

        final Long flowFileEntryDate = dis.readLong();
        builder.setEventDuration(dis.readLong());

        final Set<String> lineageIdentifiers = new HashSet<>();
        final int numLineageIdentifiers = dis.readInt();
        for (int i = 0; i < numLineageIdentifiers; i++) {
            lineageIdentifiers.add(readUUID(dis));
        }

        final long lineageStartDate = dis.readLong();

        final long fileSize;
        if (serializationVersion < 7) {
            fileSize = dis.readLong();  // file size moved in version 7 to be with content claims
            builder.setCurrentContentClaim(null, null, null, null, fileSize);
        }

        builder.setComponentId(readNullableString(dis));
        builder.setComponentType(readNullableString(dis));

        final String uuid = readUUID(dis);
        builder.setFlowFileUUID(uuid);
        builder.setDetails(readNullableString(dis));

        // Read in the FlowFile Attributes
        if (serializationVersion >= 7) {
            final Map<String, String> previousAttrs = readAttributes(dis, false);
            final Map<String, String> attrUpdates = readAttributes(dis, true);
            builder.setAttributes(previousAttrs, attrUpdates);

            final boolean hasContentClaim = dis.readBoolean();
            if (hasContentClaim) {
                builder.setCurrentContentClaim(dis.readUTF(), dis.readUTF(), dis.readUTF(), dis.readLong(), dis.readLong());
            } else {
                builder.setCurrentContentClaim(null, null, null, null, 0L);
            }

            final boolean hasPreviousClaim = dis.readBoolean();
            if (hasPreviousClaim) {
                builder.setPreviousContentClaim(dis.readUTF(), dis.readUTF(), dis.readUTF(), dis.readLong(), dis.readLong());
            }

            builder.setSourceQueueIdentifier(readNullableString(dis));
        } else {
            final Map<String, String> attrs = readAttributes(dis, false);
            builder.setAttributes(Collections.<String, String>emptyMap(), attrs);
        }

        // Read Event-Type specific fields.
        if (eventType == ProvenanceEventType.FORK || eventType == ProvenanceEventType.JOIN || eventType == ProvenanceEventType.CLONE || eventType == ProvenanceEventType.REPLAY) {
            final int numParents = dis.readInt();
            for (int i = 0; i < numParents; i++) {
                builder.addParentUuid(readUUID(dis));
            }

            final int numChildren = dis.readInt();
            for (int i = 0; i < numChildren; i++) {
                builder.addChildUuid(readUUID(dis));
            }
        } else if (eventType == ProvenanceEventType.RECEIVE) {
            builder.setTransitUri(readNullableString(dis));
            builder.setSourceSystemFlowFileIdentifier(readNullableString(dis));
        } else if (eventType == ProvenanceEventType.SEND) {
            builder.setTransitUri(readNullableString(dis));
        } else if (eventType == ProvenanceEventType.ADDINFO) {
            builder.setAlternateIdentifierUri(readNullableString(dis));
        } else if (eventType == ProvenanceEventType.ROUTE) {
            builder.setRelationship(readNullableString(dis));
        }

        builder.setFlowFileEntryDate(flowFileEntryDate);
        builder.setLineageIdentifiers(lineageIdentifiers);
        builder.setLineageStartDate(lineageStartDate);
        builder.setStorageLocation(filename, startOffset);

        final StandardProvenanceEventRecord record = builder.build();
        record.setEventId(eventId);
        return record;
    }

    private Map<String, String> readAttributes(final DataInputStream dis, final boolean valueNullable) throws IOException {
        final int numAttributes = dis.readInt();
        final Map<String, String> attrs = new HashMap<>();
        for (int i = 0; i < numAttributes; i++) {
            final String key = readLongString(dis);
            final String value = valueNullable ? readLongNullableString(dis) : readLongString(dis);
            attrs.put(key, value);
        }

        return attrs;
    }

    private String readUUID(final DataInputStream in) throws IOException {
        final long msb = in.readLong();
        final long lsb = in.readLong();
        return new UUID(msb, lsb).toString();
    }

    private String readNullableString(final DataInputStream in) throws IOException {
        final boolean valueExists = in.readBoolean();
        if (valueExists) {
            return in.readUTF();
        } else {
            return null;
        }
    }

    private String readLongNullableString(final DataInputStream in) throws IOException {
        final boolean valueExists = in.readBoolean();
        if (valueExists) {
            return readLongString(in);
        } else {
            return null;
        }
    }

    private String readLongString(final DataInputStream in) throws IOException {
        final int length = in.readInt();
        final byte[] strBytes = new byte[length];
        StreamUtils.fillBuffer(in, strBytes);
        return new String(strBytes, "UTF-8");
    }

    private boolean isData(final InputStream in) throws IOException {
        in.mark(1);
        final int nextByte = in.read();
        in.reset();
        return (nextByte >= 0);
    }

    @Override
    public void close() throws IOException {
        dis.close();
    }

    @Override
    public void skip(final long bytesToSkip) throws IOException {
        StreamUtils.skip(dis, bytesToSkip);
    }

    @Override
    public void skipTo(final long position) throws IOException {
        final long currentPosition = byteCountingIn.getBytesConsumed();
        if (currentPosition == position) {
            return;
        }
        if (currentPosition > position) {
            throw new IOException("Cannot skip to byte offset " + position + " in stream because already at byte offset " + currentPosition);
        }

        final long toSkip = position - currentPosition;
        StreamUtils.skip(dis, toSkip);
    }
}
