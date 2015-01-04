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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.DataOutputStream;
import org.apache.nifi.provenance.serialization.RecordWriter;

public class StandardRecordWriter implements RecordWriter {

    private final File file;
    private final DataOutputStream out;
    private final ByteCountingOutputStream byteCountingOut;
    private final FileOutputStream fos;
    private int recordCount = 0;

    private final Lock lock = new ReentrantLock();

    public StandardRecordWriter(final File file) throws IOException {
        this.file = file;
        this.fos = new FileOutputStream(file);
        this.byteCountingOut = new ByteCountingOutputStream(new BufferedOutputStream(fos, 65536));
        this.out = new DataOutputStream(byteCountingOut);
    }

    static void writeUUID(final DataOutputStream out, final String uuid) throws IOException {
        final UUID uuidObj = UUID.fromString(uuid);
        out.writeLong(uuidObj.getMostSignificantBits());
        out.writeLong(uuidObj.getLeastSignificantBits());
    }

    static void writeUUIDs(final DataOutputStream out, final Collection<String> list) throws IOException {
        if (list == null) {
            out.writeInt(0);
        } else {
            out.writeInt(list.size());
            for (final String value : list) {
                writeUUID(out, value);
            }
        }
    }

    @Override
    public synchronized File getFile() {
        return file;
    }

    @Override
    public synchronized void writeHeader() throws IOException {
        out.writeUTF(PersistentProvenanceRepository.class.getName());
        out.writeInt(PersistentProvenanceRepository.SERIALIZATION_VERSION);
        out.flush();
    }

    @Override
    public synchronized long writeRecord(final ProvenanceEventRecord record, long recordIdentifier) throws IOException {
        final ProvenanceEventType recordType = record.getEventType();
        final long startBytes = byteCountingOut.getBytesWritten();

        out.writeLong(recordIdentifier);
        out.writeUTF(record.getEventType().name());
        out.writeLong(record.getEventTime());
        out.writeLong(record.getFlowFileEntryDate());
        out.writeLong(record.getEventDuration());

        writeUUIDs(out, record.getLineageIdentifiers());
        out.writeLong(record.getLineageStartDate());

        writeNullableString(out, record.getComponentId());
        writeNullableString(out, record.getComponentType());
        writeUUID(out, record.getFlowFileUuid());
        writeNullableString(out, record.getDetails());

        // Write FlowFile attributes
        final Map<String, String> attrs = record.getPreviousAttributes();
        out.writeInt(attrs.size());
        for (final Map.Entry<String, String> entry : attrs.entrySet()) {
            writeLongString(out, entry.getKey());
            writeLongString(out, entry.getValue());
        }

        final Map<String, String> attrUpdates = record.getUpdatedAttributes();
        out.writeInt(attrUpdates.size());
        for (final Map.Entry<String, String> entry : attrUpdates.entrySet()) {
            writeLongString(out, entry.getKey());
            writeLongNullableString(out, entry.getValue());
        }

        // If Content Claim Info is present, write out a 'TRUE' followed by claim info. Else, write out 'false'. 
        if (record.getContentClaimSection() != null && record.getContentClaimContainer() != null && record.getContentClaimIdentifier() != null) {
            out.writeBoolean(true);
            out.writeUTF(record.getContentClaimContainer());
            out.writeUTF(record.getContentClaimSection());
            out.writeUTF(record.getContentClaimIdentifier());
            if (record.getContentClaimOffset() == null) {
                out.writeLong(0L);
            } else {
                out.writeLong(record.getContentClaimOffset());
            }
            out.writeLong(record.getFileSize());
        } else {
            out.writeBoolean(false);
        }

        // If Previous Content Claim Info is present, write out a 'TRUE' followed by claim info. Else, write out 'false'.
        if (record.getPreviousContentClaimSection() != null && record.getPreviousContentClaimContainer() != null && record.getPreviousContentClaimIdentifier() != null) {
            out.writeBoolean(true);
            out.writeUTF(record.getPreviousContentClaimContainer());
            out.writeUTF(record.getPreviousContentClaimSection());
            out.writeUTF(record.getPreviousContentClaimIdentifier());
            if (record.getPreviousContentClaimOffset() == null) {
                out.writeLong(0L);
            } else {
                out.writeLong(record.getPreviousContentClaimOffset());
            }

            if (record.getPreviousFileSize() == null) {
                out.writeLong(0L);
            } else {
                out.writeLong(record.getPreviousFileSize());
            }
        } else {
            out.writeBoolean(false);
        }

        // write out the identifier of the destination queue.
        writeNullableString(out, record.getSourceQueueIdentifier());

        // Write type-specific info
        if (recordType == ProvenanceEventType.FORK || recordType == ProvenanceEventType.JOIN || recordType == ProvenanceEventType.CLONE || recordType == ProvenanceEventType.REPLAY) {
            writeUUIDs(out, record.getParentUuids());
            writeUUIDs(out, record.getChildUuids());
        } else if (recordType == ProvenanceEventType.RECEIVE) {
            writeNullableString(out, record.getTransitUri());
            writeNullableString(out, record.getSourceSystemFlowFileIdentifier());
        } else if (recordType == ProvenanceEventType.SEND) {
            writeNullableString(out, record.getTransitUri());
        } else if (recordType == ProvenanceEventType.ADDINFO) {
            writeNullableString(out, record.getAlternateIdentifierUri());
        } else if (recordType == ProvenanceEventType.ROUTE) {
            writeNullableString(out, record.getRelationship());
        }

        out.flush();
        recordCount++;
        return byteCountingOut.getBytesWritten() - startBytes;
    }

    private void writeNullableString(final DataOutputStream out, final String toWrite) throws IOException {
        if (toWrite == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(toWrite);
        }
    }

    private void writeLongNullableString(final DataOutputStream out, final String toWrite) throws IOException {
        if (toWrite == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeLongString(out, toWrite);
        }
    }

    private void writeLongString(final DataOutputStream out, final String value) throws IOException {
        final byte[] bytes = value.getBytes("UTF-8");
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public synchronized void close() throws IOException {
        lock();
        try {
            out.flush();
            out.close();
        } finally {
            unlock();
        }
    }

    @Override
    public synchronized int getRecordsWritten() {
        return recordCount;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public String toString() {
        return "StandardRecordWriter[file=" + file + "]";
    }

    @Override
    public void sync() throws IOException {
        fos.getFD().sync();
    }
}
