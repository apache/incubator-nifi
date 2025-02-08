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
package org.apache.nifi.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.state.MockStateManager;
import org.junit.jupiter.api.Assertions;

public class MockProcessSession implements ProcessSession {

    private final Map<Relationship, List<MockFlowFile>> transferMap = new ConcurrentHashMap<>();
    private final MockFlowFileQueue processorQueue;
    private final Set<Long> beingProcessed = new HashSet<>();
    private final Set<Long> created = new HashSet<>();
    private final List<MockFlowFile> penalized = new ArrayList<>();
    private final Processor processor;

    private final Map<Long, MockFlowFile> currentVersions = new HashMap<>();
    private final Map<Long, MockFlowFile> originalVersions = new HashMap<>();
    private final SharedSessionState sharedState;
    private final Map<String, Long> counterMap = new HashMap<>();
    private final Map<FlowFile, Integer> readRecursionSet = new HashMap<>();
    private final Set<FlowFile> writeRecursionSet = new HashSet<>();
    private final MockProvenanceReporter provenanceReporter;
    private final boolean enforceStreamsClosed;

    // A List of InputStreams that have been created by calls to {@link #read(FlowFile)} and have not yet been closed.
    private final Map<FlowFile, InputStream> openInputStreams = new HashMap<>();
    // A List of OutputStreams that have been created by calls to {@link #write(FlowFile)} and have not yet been closed.
    private final Map<FlowFile, OutputStream> openOutputStreams = new HashMap<>();
    private final StateManager stateManager;
    private final boolean allowSynchronousCommits;
    private final boolean allowRecursiveReads;

    private boolean committed = false;
    private boolean rolledBack = false;
    private final Set<Long> removedFlowFiles = new HashSet<>();

    private static final AtomicLong enqueuedIndex = new AtomicLong(0L);

    public MockProcessSession(final SharedSessionState sharedState, final Processor processor) {
        this(sharedState, processor, true, new MockStateManager(processor));
    }

    public MockProcessSession(final SharedSessionState sharedState, final Processor processor, final StateManager stateManager) {
        this(sharedState, processor, true, stateManager);
    }

    public MockProcessSession(final SharedSessionState sharedState, final Processor processor, final boolean enforceStreamsClosed, final StateManager stateManager) {
        this(sharedState, processor, enforceStreamsClosed, stateManager, false);
    }

    public MockProcessSession(final SharedSessionState sharedState, final Processor processor, final boolean enforceStreamsClosed, final StateManager stateManager,
                              final boolean allowSynchronousCommits) {
        this(sharedState, processor, enforceStreamsClosed, stateManager, allowSynchronousCommits, false);
    }

    public MockProcessSession(final SharedSessionState sharedState, final Processor processor, final boolean enforceStreamsClosed, final StateManager stateManager,
                              final boolean allowSynchronousCommits, final boolean allowRecursiveReads) {
        this.processor = processor;
        this.enforceStreamsClosed = enforceStreamsClosed;
        this.sharedState = sharedState;
        this.processorQueue = sharedState.getFlowFileQueue();
        this.provenanceReporter = new MockProvenanceReporter(this, sharedState, processor.getIdentifier(), processor.getClass().getSimpleName());
        this.stateManager = stateManager;
        this.allowSynchronousCommits = allowSynchronousCommits;
        this.allowRecursiveReads = allowRecursiveReads;
    }

    @Override
    public void adjustCounter(final String name, final long delta, final boolean immediate) {
        if (immediate) {
            sharedState.adjustCounter(name, delta);
            return;
        }

        Long counter = counterMap.get(name);
        if (counter == null) {
            counter = delta;
            counterMap.put(name, counter);
            return;
        }

        counter = counter + delta;
        counterMap.put(name, counter);
    }

    @Override
    public void migrate(final ProcessSession newOwner) {
        migrate(newOwner, new ArrayList<>((Collection) currentVersions.values()));
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void migrate(final ProcessSession newOwner, final Collection<FlowFile> flowFiles) {
        if (Objects.requireNonNull(newOwner) == this) {
            throw new IllegalArgumentException("Cannot migrate FlowFiles from a Process Session to itself");
        }
        if (flowFiles == null || flowFiles.isEmpty()) {
            throw new IllegalArgumentException("Must supply at least one FlowFile to migrate");
        }

        if (!(newOwner instanceof MockProcessSession)) {
            throw new IllegalArgumentException("Cannot migrate from a StandardProcessSession to a session of type " + newOwner.getClass());
        }

        migrate((MockProcessSession) newOwner, (Collection<MockFlowFile>) (Collection) flowFiles);
    }

    private void migrate(final MockProcessSession newOwner, final Collection<MockFlowFile> flowFiles) {
        for (final FlowFile flowFile : flowFiles) {
            if (openInputStreams.containsKey(flowFile)) {
                throw new IllegalStateException(flowFile + " cannot be migrated to a new Process Session because this session currently "
                    + "has an open InputStream for the FlowFile, created by calling ProcessSession.read(FlowFile)");
            }

            if (openOutputStreams.containsKey(flowFile)) {
                throw new IllegalStateException(flowFile + " cannot be migrated to a new Process Session because this session currently "
                    + "has an open OutputStream for the FlowFile, created by calling ProcessSession.write(FlowFile)");
            }

            if (readRecursionSet.containsKey(flowFile)) {
                throw new IllegalStateException(flowFile + " already in use for an active callback or InputStream created by ProcessSession.read(FlowFile) has not been closed");
            }

            if (writeRecursionSet.contains(flowFile)) {
                throw new IllegalStateException(flowFile + " already in use for an active callback or OutputStream created by ProcessSession.write(FlowFile) has not been closed");
            }

            final FlowFile currentVersion = currentVersions.get(flowFile.getId());
            if (currentVersion == null) {
                throw new FlowFileHandlingException(flowFile + " is not known in this session");
            }
        }

        for (final Map.Entry<Relationship, List<MockFlowFile>> entry : transferMap.entrySet()) {
            final Relationship relationship = entry.getKey();
            final List<MockFlowFile> transferredFlowFiles = entry.getValue();

            for (final MockFlowFile flowFile : flowFiles) {
                if (transferredFlowFiles.remove(flowFile)) {
                    newOwner.transferMap.computeIfAbsent(relationship, rel -> new ArrayList<>()).add(flowFile);
                }
            }
        }

        for (final MockFlowFile flowFile : flowFiles) {
            if (beingProcessed.remove(flowFile.getId())) {
                newOwner.beingProcessed.add(flowFile.getId());
            }

            if (penalized.remove(flowFile)) {
                newOwner.penalized.add(flowFile);
            }

            if (currentVersions.containsKey(flowFile.getId())) {
                newOwner.currentVersions.put(flowFile.getId(), currentVersions.remove(flowFile.getId()));
            }

            if (originalVersions.containsKey(flowFile.getId())) {
                newOwner.originalVersions.put(flowFile.getId(), originalVersions.remove(flowFile.getId()));
            }

            if (removedFlowFiles.remove(flowFile.getId())) {
                newOwner.removedFlowFiles.add(flowFile.getId());
            }

            if (created.remove(flowFile.getId())) {
                newOwner.created.add(flowFile.getId());
            }
        }

        final Set<String> flowFileIds = flowFiles.stream()
            .map(ff -> ff.getAttribute(CoreAttributes.UUID.key()))
            .collect(Collectors.toSet());

        provenanceReporter.migrate(newOwner.provenanceReporter, flowFileIds);
    }

    @Override
    public MockFlowFile clone(FlowFile flowFile) {
        flowFile = validateState(flowFile);
        final MockFlowFile newFlowFile = new MockFlowFile(sharedState.nextFlowFileId(), flowFile);
        updateStateWithNewFlowFile(newFlowFile);
        return newFlowFile;
    }

    @Override
    public MockFlowFile clone(FlowFile flowFile, final long offset, final long size) {
        flowFile = validateState(flowFile);
        if (offset + size > flowFile.getSize()) {
            throw new FlowFileHandlingException("Specified offset of " + offset + " and size " + size + " exceeds size of " + flowFile.toString());
        }

        final MockFlowFile newFlowFile = new MockFlowFile(sharedState.nextFlowFileId(), flowFile);
        final byte[] newContent = Arrays.copyOfRange(((MockFlowFile) flowFile).getData(), (int) offset, (int) (offset + size));
        newFlowFile.setData(newContent);

        updateStateWithNewFlowFile(newFlowFile);
        return newFlowFile;
    }

    private void closeStreams(final Map<FlowFile, ? extends Closeable> streamMap, final boolean enforceClosed) {
        final Map<FlowFile, ? extends Closeable> openStreamCopy = new HashMap<>(streamMap); // avoid ConcurrentModificationException by creating a copy of the List
        for (final Map.Entry<FlowFile, ? extends Closeable> entry : openStreamCopy.entrySet()) {
            final FlowFile flowFile = entry.getKey();
            final Closeable openStream = entry.getValue();

            try {
                openStream.close();
            } catch (IOException e) {
                throw new FlowFileAccessException("Failed to close stream for " + flowFile, e);
            }

            if (enforceClosed) {
                throw new FlowFileHandlingException("Cannot commit session because the following streams were created via "
                    + "calls to ProcessSession.read(FlowFile) or ProcessSession.write(FlowFile) and never closed: " + streamMap);
            }
        }
    }


    @Override
    public void commit() {
        if (!allowSynchronousCommits) {
            throw new RuntimeException("As of version 1.14.0, ProcessSession.commit() should be avoided when possible. See JavaDocs for explanations. Instead, use commitAsync(), " +
                "commitAsync(Runnable), or commitAsync(Runnable, Consumer<Throwable>). However, if this is not possible, ProcessSession.commit() may still be used, but this must be explicitly " +
                "enabled by calling TestRunner.");
        }

        commitInternal();
    }

    private void commitInternal() {
        if (!beingProcessed.isEmpty()) {
            throw new FlowFileHandlingException("Cannot commit session because the following FlowFiles have not been removed or transferred: " + beingProcessed);
        }

        closeStreams(openInputStreams, enforceStreamsClosed);
        closeStreams(openOutputStreams, enforceStreamsClosed);

        committed = true;
        beingProcessed.clear();
        currentVersions.clear();
        originalVersions.clear();
        created.clear();

        for (final Map.Entry<String, Long> entry : counterMap.entrySet()) {
            sharedState.adjustCounter(entry.getKey(), entry.getValue());
        }

        sharedState.addProvenanceEvents(provenanceReporter.getEvents());
        provenanceReporter.clear();
        counterMap.clear();
    }

    @Override
    public void commitAsync() {
        commitInternal();
    }

    @Override
    public void commitAsync(final Runnable onSuccess, final Consumer<Throwable> onFailure) {
        try {
            commitInternal();
        } catch (final Throwable t) {
            rollback();
            onFailure.accept(t);
            throw t;
        }

        onSuccess.run();
    }

    /**
     * Clear the 'committed' flag so that we can test that the next iteration of
     * {@link org.apache.nifi.processor.Processor#onTrigger} commits or rolls back the
     * session
     */
    public void clearCommitted() {
        committed = false;
    }

    /**
     * Clear the 'rolledBack' flag so that we can test that the next iteration
     * of {@link org.apache.nifi.processor.Processor#onTrigger} commits or rolls back the
     * session
     */
    public void clearRollback() {
        rolledBack = false;
    }

    @Override
    public MockFlowFile create() {
        final MockFlowFile flowFile = new MockFlowFile(sharedState.nextFlowFileId());
        updateStateWithNewFlowFile(flowFile);
        return flowFile;
    }

    @Override
    public MockFlowFile create(final FlowFile flowFile) {
        MockFlowFile newFlowFile = create();
        newFlowFile = (MockFlowFile) inheritAttributes(flowFile, newFlowFile);
        updateStateWithNewFlowFile(newFlowFile);
        return newFlowFile;
    }

    @Override
    public MockFlowFile create(final Collection<FlowFile> flowFiles) {
        MockFlowFile newFlowFile = create();
        newFlowFile = (MockFlowFile) inheritAttributes(flowFiles, newFlowFile);
        updateStateWithNewFlowFile(newFlowFile);
        return newFlowFile;
    }

    @Override
    public void exportTo(FlowFile flowFile, final OutputStream out) {
        flowFile = validateState(flowFile);
        if (flowFile == null || out == null) {
            throw new IllegalArgumentException("arguments cannot be null");
        }

        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }

        final MockFlowFile mock = (MockFlowFile) flowFile;

        try {
            out.write(mock.getData());
        } catch (final IOException e) {
            throw new FlowFileAccessException(e.toString(), e);
        }
    }

    @Override
    public void exportTo(FlowFile flowFile, final Path path, final boolean append) {
        flowFile = validateState(flowFile);
        if (flowFile == null || path == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }

        final MockFlowFile mock = (MockFlowFile) flowFile;

        final OpenOption mode = append ? StandardOpenOption.APPEND : StandardOpenOption.CREATE;

        try (final OutputStream out = Files.newOutputStream(path, mode)) {
            out.write(mock.getData());
        } catch (final IOException e) {
            throw new FlowFileAccessException(e.toString(), e);
        }
    }

    @Override
    public MockFlowFile get() {
        final MockFlowFile flowFile = processorQueue.poll();
        if (flowFile != null) {
            beingProcessed.add(flowFile.getId());
            currentVersions.put(flowFile.getId(), flowFile);
            originalVersions.put(flowFile.getId(), flowFile);
        }
        return flowFile;
    }

    @Override
    public List<FlowFile> get(final int maxResults) {
        final List<FlowFile> flowFiles = new ArrayList<>(Math.min(500, maxResults));
        for (int i = 0; i < maxResults; i++) {
            final MockFlowFile nextFlowFile = get();
            if (nextFlowFile == null) {
                return flowFiles;
            }

            flowFiles.add(nextFlowFile);
        }

        return flowFiles;
    }

    @Override
    public List<FlowFile> get(final FlowFileFilter filter) {
        final List<FlowFile> flowFiles = new ArrayList<>();
        final List<MockFlowFile> unselected = new ArrayList<>();

        while (true) {
            final MockFlowFile flowFile = processorQueue.poll();
            if (flowFile == null) {
                break;
            }

            final FlowFileFilter.FlowFileFilterResult filterResult = filter.filter(flowFile);
            if (filterResult.isAccept()) {
                flowFiles.add(flowFile);

                beingProcessed.add(flowFile.getId());
                currentVersions.put(flowFile.getId(), flowFile);
                originalVersions.put(flowFile.getId(), flowFile);
            } else {
                unselected.add(flowFile);
            }

            if (!filterResult.isContinue()) {
                break;
            }
        }

        processorQueue.addAll(unselected);
        return flowFiles;
    }

    @Override
    public QueueSize getQueueSize() {
        return processorQueue.size();
    }

    @Override
    public MockFlowFile importFrom(final InputStream in, FlowFile flowFile) {
        flowFile = validateState(flowFile);
        if (in == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        final MockFlowFile mock = (MockFlowFile) flowFile;

        final MockFlowFile newFlowFile = new MockFlowFile(mock.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        try {
            final byte[] data = readFully(in);
            newFlowFile.setData(data);
            return newFlowFile;
        } catch (final IOException e) {
            throw new FlowFileAccessException(e.toString(), e);
        }
    }

    @Override
    public MockFlowFile importFrom(final Path path, final boolean keepSourceFile, FlowFile flowFile) {
        flowFile = validateState(flowFile);
        if (path == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        final MockFlowFile mock = (MockFlowFile) flowFile;
        MockFlowFile newFlowFile = new MockFlowFile(mock.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            Files.copy(path, baos);
        } catch (final IOException e) {
            throw new FlowFileAccessException(e.toString(), e);
        }

        newFlowFile.setData(baos.toByteArray());
        newFlowFile = putAttribute(newFlowFile, CoreAttributes.FILENAME.key(), path.getFileName().toString());
        return newFlowFile;
    }

    @Override
    public MockFlowFile merge(Collection<FlowFile> sources, FlowFile destination) {
        sources = validateState(sources);
        destination = validateState(destination);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (final FlowFile flowFile : sources) {
            final MockFlowFile mock = (MockFlowFile) flowFile;
            final byte[] data = mock.getData();
            try {
                baos.write(data);
            } catch (final IOException e) {
                throw new AssertionError("Failed to write to BAOS");
            }
        }

        final MockFlowFile newFlowFile = new MockFlowFile(destination.getId(), destination);
        newFlowFile.setData(baos.toByteArray());
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        return newFlowFile;
    }

    @Override
    public MockFlowFile putAllAttributes(FlowFile flowFile, final Map<String, String> attrs) {
        flowFile = validateState(flowFile);
        if (attrs == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("Cannot update attributes of a flow file that I did not create");
        }
        final MockFlowFile mock = (MockFlowFile) flowFile;
        final MockFlowFile newFlowFile = new MockFlowFile(mock.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        final Map<String, String> updatedAttributes;
        if (attrs.containsKey(CoreAttributes.UUID.key())) {
            updatedAttributes = new HashMap<>(attrs);
            updatedAttributes.remove(CoreAttributes.UUID.key());
        } else {
            updatedAttributes = attrs;
        }
        newFlowFile.putAttributes(updatedAttributes);
        return newFlowFile;
    }

    @Override
    public MockFlowFile putAttribute(FlowFile flowFile, final String attrName, final String attrValue) {
        flowFile = validateState(flowFile);
        if (attrName == null || attrValue == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("Cannot update attributes of a flow file that I did not create");
        }

        if ("uuid".equals(attrName)) {
            Assertions.fail("Should not be attempting to set FlowFile UUID via putAttribute. This will be ignored in production");
        }

        final MockFlowFile mock = (MockFlowFile) flowFile;
        final MockFlowFile newFlowFile = new MockFlowFile(mock.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(attrName, attrValue);
        newFlowFile.putAttributes(attrs);
        return newFlowFile;
    }

    @Override
    public void read(FlowFile flowFile, final InputStreamCallback callback) {
        if (callback == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }

        flowFile = validateState(flowFile);
        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        final MockFlowFile mock = (MockFlowFile) flowFile;

        final ByteArrayInputStream bais = new ByteArrayInputStream(mock.getData());
        incrementReadCount(flowFile);
        try {
            callback.process(bais);
            bais.close();
        } catch (final IOException e) {
            throw new ProcessException(e.toString(), e);
        } finally {
            decrementReadCount(flowFile);
        }
    }

    private void incrementReadCount(final FlowFile flowFile) {
        readRecursionSet.compute(flowFile, (ff, count) -> count == null ? 1 : count + 1);
    }

    private void decrementReadCount(final FlowFile flowFile) {
        final Integer count = readRecursionSet.get(flowFile);
        if (count == null) {
            return;
        }

        final int updatedCount = count - 1;
        if (updatedCount == 0) {
            readRecursionSet.remove(flowFile);
        } else {
            readRecursionSet.put(flowFile, updatedCount);
        }
    }

    @Override
    public InputStream read(FlowFile flowFile) {
        if (flowFile == null) {
            throw new IllegalArgumentException("FlowFile cannot be null");
        }

        final MockFlowFile mock = validateState(flowFile);

        final ByteArrayInputStream bais = new ByteArrayInputStream(mock.getData());
        incrementReadCount(flowFile);
        final InputStream errorHandlingStream = new InputStream() {
            @Override
            public int read() throws IOException {
                return bais.read();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return bais.read(b, off, len);
            }

            @Override
            public void close() throws IOException {
                decrementReadCount(flowFile);
                openInputStreams.remove(mock);
                bais.close();
            }

            @Override
            public void mark(final int readlimit) {
                bais.mark(readlimit);
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public void reset() {
                bais.reset();
            }

            @Override
            public int available() throws IOException {
                return bais.available();
            }

            @Override
            public String toString() {
                return "ErrorHandlingInputStream[flowFile=" + mock + "]";
            }
        };

        openInputStreams.put(mock, errorHandlingStream);
        return errorHandlingStream;
    }

    @Override
    public void remove(FlowFile flowFile) {
        flowFile = validateState(flowFile);

        final Iterator<MockFlowFile> penalizedItr = penalized.iterator();
        while (penalizedItr.hasNext()) {
            final MockFlowFile ff = penalizedItr.next();
            if (Objects.equals(ff.getId(), flowFile.getId())) {
                penalizedItr.remove();
                penalized.remove(ff);
                if (originalVersions.get(ff.getId()) != null) {
                    provenanceReporter.drop(ff, ff.getAttribute(CoreAttributes.DISCARD_REASON.key()));
                }
                break;
            }
        }

        final Iterator<Long> processedItr = beingProcessed.iterator();
        while (processedItr.hasNext()) {
            final Long ffId = processedItr.next();
            if (ffId != null && ffId.equals(flowFile.getId())) {
                processedItr.remove();
                beingProcessed.remove(ffId);
                removedFlowFiles.add(flowFile.getId());
                currentVersions.remove(ffId);
                if (originalVersions.get(flowFile.getId()) != null) {
                    provenanceReporter.drop(flowFile, flowFile.getAttribute(CoreAttributes.DISCARD_REASON.key()));
                }
                return;
            }
        }

        throw new ProcessException(flowFile + " not found in queue");
    }

    @Override
    public void remove(Collection<FlowFile> flowFiles) {
        flowFiles = validateState(flowFiles);

        for (final FlowFile flowFile : flowFiles) {
            remove(flowFile);
        }
    }

    @Override
    public MockFlowFile removeAllAttributes(FlowFile flowFile, final Set<String> attrNames) {
        flowFile = validateState(flowFile);
        if (attrNames == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        final MockFlowFile mock = (MockFlowFile) flowFile;

        final MockFlowFile newFlowFile = new MockFlowFile(mock.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        newFlowFile.removeAttributes(attrNames);
        return newFlowFile;
    }

    @Override
    public MockFlowFile removeAllAttributes(FlowFile flowFile, final Pattern keyPattern) {
        flowFile = validateState(flowFile);
        if (flowFile == null) {
            throw new IllegalArgumentException("flowFile cannot be null");
        }
        if (keyPattern == null) {
            return (MockFlowFile) flowFile;
        }

        final Set<String> attrsToRemove = new HashSet<>();
        for (final String key : flowFile.getAttributes().keySet()) {
            if (keyPattern.matcher(key).matches()) {
                attrsToRemove.add(key);
            }
        }

        return removeAllAttributes(flowFile, attrsToRemove);
    }

    @Override
    public MockFlowFile removeAttribute(FlowFile flowFile, final String attrName) {
        flowFile = validateState(flowFile);
        if (attrName == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        final MockFlowFile mock = (MockFlowFile) flowFile;
        final MockFlowFile newFlowFile = new MockFlowFile(mock.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        final Set<String> attrNames = new HashSet<>();
        attrNames.add(attrName);
        newFlowFile.removeAttributes(attrNames);
        return newFlowFile;
    }

    @Override
    public void rollback() {
        rollback(false);
    }

    @Override
    public void rollback(final boolean penalize) {
        //if we've already committed then rollback is basically a no-op
        if (committed) {
            return;
        }

        closeStreams(openInputStreams, false);
        closeStreams(openOutputStreams, false);

        for (final List<MockFlowFile> list : transferMap.values()) {
            for (final MockFlowFile flowFile : list) {
                if (!created.contains(flowFile.getId())) {
                    processorQueue.offer(flowFile);
                    if (penalize) {
                        penalized.add(flowFile);
                    }
                }
            }
        }

        for (final Long flowFileId : beingProcessed) {
            final MockFlowFile flowFile = originalVersions.get(flowFileId);
            if (flowFile != null) {
                if (!created.contains(flowFile.getId())) {
                    processorQueue.offer(flowFile);
                    if (penalize) {
                        penalized.add(flowFile);
                    }
                }
            }
        }

        rolledBack = true;
        beingProcessed.clear();
        currentVersions.clear();
        originalVersions.clear();
        transferMap.clear();
        created.clear();
        clearTransferState();
        if (!penalize) {
            penalized.clear();
        }
    }

    @Override
    public void transfer(FlowFile flowFile) {
        flowFile = validateState(flowFile);
        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("I only accept MockFlowFile");
        }

        // if the flowfile provided was created in this session (i.e. it's in currentVersions and not in original versions),
        // then throw an exception indicating that you can't transfer flowfiles back to self.
        // this mimics the same behavior in StandardProcessSession
        if (currentVersions.get(flowFile.getId()) != null && originalVersions.get(flowFile.getId()) == null) {
            throw new IllegalArgumentException("Cannot transfer FlowFiles that are created in this Session back to self");
        }

        final MockFlowFile mockFlowFile = (MockFlowFile) flowFile;
        beingProcessed.remove(flowFile.getId());
        processorQueue.offer(mockFlowFile);
        updateLastQueuedDate(mockFlowFile);

    }

    private void updateLastQueuedDate(MockFlowFile mockFlowFile) {
        // Simulate StandardProcessSession.updateLastQueuedDate,
        // which is called when a flow file is transferred to a relationship.
        mockFlowFile.setLastEnqueuedDate(System.currentTimeMillis());
        mockFlowFile.setEnqueuedIndex(enqueuedIndex.incrementAndGet());
    }

    private void updateStateWithNewFlowFile(MockFlowFile newFlowFile) {
        if (newFlowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        beingProcessed.add(newFlowFile.getId());
        created.add(newFlowFile.getId());
    }

    @Override
    public void transfer(final Collection<FlowFile> flowFiles) {
        for (final FlowFile flowFile : flowFiles) {
            transfer(flowFile);
        }
    }

    @Override
    public void transfer(FlowFile flowFile, final Relationship relationship) {
        if (relationship == Relationship.SELF) {
            transfer(flowFile);
            return;
        }
        if (!processor.getRelationships().contains(relationship)) {
            throw new IllegalArgumentException("this relationship " + relationship.getName() + " is not known");
        }

        flowFile = validateState(flowFile);
        List<MockFlowFile> list = transferMap.computeIfAbsent(relationship, r -> new ArrayList<>());

        beingProcessed.remove(flowFile.getId());
        list.add((MockFlowFile) flowFile);
        updateLastQueuedDate((MockFlowFile) flowFile);
    }

    @Override
    public void transfer(Collection<FlowFile> flowFiles, final Relationship relationship) {
        if (relationship == Relationship.SELF) {
            transfer(flowFiles);
            return;
        }
        for (final FlowFile flowFile : flowFiles) {
            transfer(flowFile, relationship);
        }
    }

    @Override
    public MockFlowFile write(FlowFile flowFile, final OutputStreamCallback callback) {
        flowFile = validateState(flowFile);
        if (callback == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        final MockFlowFile mock = (MockFlowFile) flowFile;

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeRecursionSet.add(flowFile);
        try {
            callback.process(baos);
        } catch (final IOException e) {
            throw new ProcessException(e.toString(), e);
        } finally {
            writeRecursionSet.remove(flowFile);
        }

        final MockFlowFile newFlowFile = new MockFlowFile(mock.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        newFlowFile.setData(baos.toByteArray());
        return newFlowFile;
    }

    @Override
    public OutputStream write(FlowFile flowFile) {
        if (!(flowFile instanceof MockFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        final MockFlowFile mockFlowFile = validateState(flowFile);
        writeRecursionSet.add(mockFlowFile);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();

                writeRecursionSet.remove(mockFlowFile);
                final MockFlowFile newFlowFile = new MockFlowFile(mockFlowFile.getId(), mockFlowFile);
                currentVersions.put(newFlowFile.getId(), newFlowFile);

                newFlowFile.setData(toByteArray());
            }
        };

        return baos;
    }

    @Override
    public FlowFile append(FlowFile flowFile, final OutputStreamCallback callback) {
        if (callback == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        final MockFlowFile mock = validateState(flowFile);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            baos.write(mock.getData());
            callback.process(baos);
        } catch (final IOException e) {
            throw new ProcessException(e.toString(), e);
        }

        final MockFlowFile newFlowFile = new MockFlowFile(mock.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        newFlowFile.setData(baos.toByteArray());
        return newFlowFile;
    }

    @Override
    public MockFlowFile write(FlowFile flowFile, final StreamCallback callback) {
        flowFile = validateState(flowFile);
        if (callback == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        final MockFlowFile mock = (MockFlowFile) flowFile;
        final ByteArrayInputStream in = new ByteArrayInputStream(mock.getData());
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        writeRecursionSet.add(flowFile);
        try {
            callback.process(in, out);
        } catch (final IOException e) {
            throw new ProcessException(e.toString(), e);
        } finally {
            writeRecursionSet.remove(flowFile);
        }

        final MockFlowFile newFlowFile = new MockFlowFile(flowFile.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        newFlowFile.setData(out.toByteArray());

        return newFlowFile;
    }

    private byte[] readFully(final InputStream in) throws IOException {
        final byte[] buffer = new byte[4096];
        int len;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while ((len = in.read(buffer)) >= 0) {
            baos.write(buffer, 0, len);
        }

        return baos.toByteArray();
    }

    public List<MockFlowFile> getFlowFilesForRelationship(final Relationship relationship) {
        List<MockFlowFile> list = this.transferMap.get(relationship);
        if (list == null) {
            list = new ArrayList<>();
        }

        return list;
    }

    public List<MockFlowFile> getPenalizedFlowFiles() {
        return penalized;
    }

    /**
     * @param relationship to get flowfiles for
     * @return a List of FlowFiles in the order in which they were transferred
     *         to the given relationship
     */
    public List<MockFlowFile> getFlowFilesForRelationship(final String relationship) {
        final Relationship procRel = new Relationship.Builder().name(relationship).build();
        return getFlowFilesForRelationship(procRel);
    }

    public MockFlowFile createFlowFile(final File file) throws IOException {
        return createFlowFile(Files.readAllBytes(file.toPath()));
    }

    public MockFlowFile createFlowFile(final byte[] data) {
        final MockFlowFile flowFile = create();
        flowFile.setData(data);
        return flowFile;
    }

    public MockFlowFile createFlowFile(final byte[] data, final Map<String, String> attrs) {
        final MockFlowFile ff = createFlowFile(data);
        ff.putAttributes(attrs);
        return ff;
    }

    @Override
    public MockFlowFile merge(Collection<FlowFile> sources, FlowFile destination, byte[] header, byte[] footer, byte[] demarcator) {
        sources = validateState(sources);
        destination = validateState(destination);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            if (header != null) {
                baos.write(header);
            }

            int count = 0;
            for (final FlowFile flowFile : sources) {
                baos.write(((MockFlowFile) flowFile).getData());
                if (demarcator != null && ++count != sources.size()) {
                    baos.write(demarcator);
                }
            }

            if (footer != null) {
                baos.write(footer);
            }
        } catch (final IOException e) {
            throw new AssertionError("failed to write data to BAOS");
        }

        final MockFlowFile newFlowFile = new MockFlowFile(destination.getId(), destination);
        newFlowFile.setData(baos.toByteArray());
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        created.add(newFlowFile.getId());

        return newFlowFile;
    }

    private List<FlowFile> validateState(final Collection<FlowFile> flowFiles) {
        return flowFiles.stream()
            .map(this::validateState)
            .collect(Collectors.toList());
    }

    private MockFlowFile validateState(final FlowFile flowFile) {
        Objects.requireNonNull(flowFile);

        final MockFlowFile currentVersion = currentVersions.get(flowFile.getId());
        if (currentVersion == null) {
            throw new FlowFileHandlingException(flowFile + " is not known in this session");
        }

        if (readRecursionSet.containsKey(flowFile) && !allowRecursiveReads) {
            throw new IllegalStateException(flowFile + " already in use for an active callback or InputStream created by ProcessSession.read(FlowFile) has not been closed");
        }

        if (writeRecursionSet.contains(flowFile)) {
            throw new IllegalStateException(flowFile + " already in use for an active callback or OutputStream created by ProcessSession.write(FlowFile) has not been closed");
        }

        for (final List<MockFlowFile> flowFiles : transferMap.values()) {
            if (flowFiles.contains(flowFile)) {
                throw new IllegalStateException(flowFile + " has already been transferred");
            }
        }

        return currentVersion;
    }


    /**
     * Inherits the attributes from the given source flow file into another flow
     * file. The UUID of the source becomes the parent UUID of this flow file.
     * If a parent uuid had previously been established it will be replaced by
     * the uuid of the given source
     *
     * @param source the FlowFile from which to copy attributes
     * @param destination the FlowFile to which to copy attributes
     */
    private FlowFile inheritAttributes(final FlowFile source, final FlowFile destination) {
        if (source == null || destination == null || source == destination) {
            return destination; // don't need to inherit from ourselves
        }
        final FlowFile updated = putAllAttributes(destination, source.getAttributes());
        getProvenanceReporter().fork(source, Collections.singletonList(updated));
        return updated;
    }

    /**
     * Inherits the attributes from the given source flow files into the
     * destination flow file. The UUIDs of the sources becomes the parent UUIDs
     * of the destination flow file. Only attributes which is common to all
     * source items is copied into this flow files attributes. Any previously
     * established parent UUIDs will be replaced by the UUIDs of the sources. It
     * will capture the uuid of a certain number of source objects and may not
     * capture all of them. How many it will capture is unspecified.
     *
     * @param sources to inherit common attributes from
     */
    private FlowFile inheritAttributes(final Collection<FlowFile> sources, final FlowFile destination) {
        final StringBuilder parentUuidBuilder = new StringBuilder();
        int uuidsCaptured = 0;
        for (final FlowFile source : sources) {
            if (source == destination) {
                continue; // don't want to capture parent uuid of this. Something can't be a child of itself
            }
            final String sourceUuid = source.getAttribute(CoreAttributes.UUID.key());
            if (sourceUuid != null && !sourceUuid.trim().isEmpty()) {
                uuidsCaptured++;
                if (parentUuidBuilder.length() > 0) {
                    parentUuidBuilder.append(",");
                }
                parentUuidBuilder.append(sourceUuid);
            }

            if (uuidsCaptured > 100) {
                break;
            }
        }

        final FlowFile updated = putAllAttributes(destination, intersectAttributes(sources));
        getProvenanceReporter().join(sources, updated);
        return updated;
    }

    /**
     * Returns the attributes that are common to every flow file given. The key
     * and value must match exactly.
     *
     * @param flowFileList a list of flow files
     *
     * @return the common attributes
     */
    private static Map<String, String> intersectAttributes(final Collection<FlowFile> flowFileList) {
        final Map<String, String> result = new HashMap<>();
        // trivial cases
        if (flowFileList == null || flowFileList.isEmpty()) {
            return result;
        } else if (flowFileList.size() == 1) {
            result.putAll(flowFileList.iterator().next().getAttributes());
        }

        /*
         * Start with the first attribute map and only put an entry to the
         * resultant map if it is common to every map.
         */
        final Map<String, String> firstMap = flowFileList.iterator().next().getAttributes();

        outer: for (final Map.Entry<String, String> mapEntry : firstMap.entrySet()) {
            final String key = mapEntry.getKey();
            final String value = mapEntry.getValue();
            for (final FlowFile flowFile : flowFileList) {
                final Map<String, String> currMap = flowFile.getAttributes();
                final String curVal = currMap.get(key);
                if (curVal == null || !curVal.equals(value)) {
                    continue outer;
                }
            }
            result.put(key, value);
        }

        return result;
    }

    /**
     * Assert that the session has been committed
     */
    public void assertCommitted() {
        Assertions.assertTrue(committed, "Session was not committed");
    }

    /**
     * Assert that the session has not been committed
     */
    public void assertNotCommitted() {
        Assertions.assertFalse(committed, "Session was committed");
    }

    /**
     * Assert that {@link #rollback()} has been called
     */
    public void assertRolledBack() {
        Assertions.assertTrue(rolledBack, "Session was not rolled back");
    }

    /**
     * Assert that {@link #rollback()} has not been called
     */
    public void assertNotRolledBack() {
        Assertions.assertFalse(rolledBack, "Session was rolled back");
    }

    /**
     * Assert that the number of FlowFiles transferred to the given relationship
     * is equal to the given count
     *
     * @param relationship to validate transfer count of
     * @param count items transfer to given relationship
     */
    public void assertTransferCount(final Relationship relationship, final int count) {
        final int transferCount = getFlowFilesForRelationship(relationship).size();
        Assertions.assertEquals(count, transferCount, "Expected " + count + " FlowFiles to be transferred to "
            + relationship + " but actual transfer count was " + transferCount);
    }

    /**
     * Assert that the number of FlowFiles transferred to the given relationship
     * is equal to the given count
     *
     * @param relationship to validate transfer count of
     * @param count items transfer to given relationship
     */
    public void assertTransferCount(final String relationship, final int count) {
        assertTransferCount(new Relationship.Builder().name(relationship).build(), count);
    }

    /**
     * Assert that there are no FlowFiles left on the input queue.
     */
    public void assertQueueEmpty() {
        Assertions.assertTrue(this.processorQueue.isEmpty(), "FlowFile Queue has " + this.processorQueue.size() + " items");
    }

    /**
     * Assert that at least one FlowFile is on the input queue
     */
    public void assertQueueNotEmpty() {
        Assertions.assertFalse(this.processorQueue.isEmpty(), "FlowFile Queue is empty");
    }

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship
     *
     * @param relationship to check for transferred flow files
     */
    public void assertAllFlowFilesTransferred(final String relationship) {
        assertAllFlowFilesTransferred(new Relationship.Builder().name(relationship).build());
    }

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship
     *
     * @param relationship to validate
     */
    public void assertAllFlowFilesTransferred(final Relationship relationship) {
        for (final Map.Entry<Relationship, List<MockFlowFile>> entry : transferMap.entrySet()) {
            final Relationship rel = entry.getKey();
            final List<MockFlowFile> flowFiles = entry.getValue();

            if (!rel.equals(relationship) && flowFiles != null && !flowFiles.isEmpty()) {
                Assertions.fail("Expected all Transferred FlowFiles to go to " + relationship + " but " + flowFiles.size() + " were routed to " + rel);
            }
        }
    }

    /**
     * Asserts that all FlowFiles that were transferred are compliant with the
     * given validator.
     *
     * @param validator validator to use
     */
    public void assertAllFlowFiles(FlowFileValidator validator) {
        for (final Map.Entry<Relationship, List<MockFlowFile>> entry : transferMap.entrySet()) {
            final List<MockFlowFile> flowFiles = entry.getValue();
            for (MockFlowFile mockFlowFile : flowFiles) {
                validator.assertFlowFile(mockFlowFile);
            }
        }
    }

    /**
     * Asserts that all FlowFiles that were transferred in the given relationship
     * are compliant with the given validator.
     *
     * @param validator validator to use
     */
    public void assertAllFlowFiles(Relationship relationship, FlowFileValidator validator) {
        for (final Map.Entry<Relationship, List<MockFlowFile>> entry : transferMap.entrySet()) {
            final List<MockFlowFile> flowFiles = entry.getValue();
            final Relationship rel = entry.getKey();
            for (MockFlowFile mockFlowFile : flowFiles) {
                if (rel.equals(relationship)) {
                    validator.assertFlowFile(mockFlowFile);
                }
            }
        }
    }

    /**
     * Removes all state information about FlowFiles that have been transferred
     */
    public void clearTransferState() {
        this.transferMap.clear();
    }

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship and that the number of FlowFiles transferred is equal
     * to <code>count</code>
     *
     * @param relationship to validate
     * @param count number of items sent to that relationship (expected)
     */
    public void assertAllFlowFilesTransferred(final Relationship relationship, final int count) {
        assertAllFlowFilesTransferred(relationship);
        assertTransferCount(relationship, count);
    }

    /**
     * Asserts that all FlowFiles that were transferred were transferred to the
     * given relationship and that the number of FlowFiles transferred is equal
     * to <code>count</code>
     *
     * @param relationship to validate
     * @param count number of items sent to that relationship (expected)
     */
    public void assertAllFlowFilesTransferred(final String relationship, final int count) {
        assertAllFlowFilesTransferred(new Relationship.Builder().name(relationship).build(), count);
    }

    /**
     * @return the number of FlowFiles that were removed
     */
    public int getRemovedCount() {
        return removedFlowFiles.size();
    }

    @Override
    public ProvenanceReporter getProvenanceReporter() {
        return provenanceReporter;
    }

    @Override
    public void setState(final Map<String, String> state, final Scope scope) throws IOException {
        stateManager.setState(state, scope);
    }

    @Override
    public StateMap getState(final Scope scope) throws IOException {
        return stateManager.getState(scope);
    }

    @Override
    public boolean replaceState(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) throws IOException {
        return stateManager.replace(oldValue, newValue, scope);
    }

    @Override
    public void clearState(final Scope scope) throws IOException {
        stateManager.clear(scope);
    }

    @Override
    public MockFlowFile penalize(FlowFile flowFile) {
        flowFile = validateState(flowFile);
        final MockFlowFile mockFlowFile = (MockFlowFile) flowFile;
        final MockFlowFile newFlowFile = new MockFlowFile(mockFlowFile.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        newFlowFile.setPenalized(true);
        penalized.add(newFlowFile);
        return newFlowFile;
    }

    public MockFlowFile unpenalize(FlowFile flowFile) {
        flowFile = validateState(flowFile);
        final MockFlowFile newFlowFile = new MockFlowFile(flowFile.getId(), flowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        newFlowFile.setPenalized(false);
        penalized.remove(newFlowFile);
        return newFlowFile;
    }

    public byte[] getContentAsByteArray(MockFlowFile flowFile) {
        flowFile = validateState(flowFile);
        return flowFile.getData();
    }

    /**
     * Checks if a FlowFile is known in this session.
     *
     * @param flowFile
     *            the FlowFile to check
     * @return <code>true</code> if the FlowFile is known in this session,
     *         <code>false</code> otherwise.
     */
    boolean isFlowFileKnown(final FlowFile flowFile) {
        final FlowFile curFlowFile = currentVersions.get(flowFile.getId());
        if (curFlowFile == null) {
            return false;
        }

        final String curUuid = curFlowFile.getAttribute(CoreAttributes.UUID.key());
        final String providedUuid = curFlowFile.getAttribute(CoreAttributes.UUID.key());
        return curUuid.equals(providedUuid);
    }
}
