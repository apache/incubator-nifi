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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.Charsets;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.util.ListableEntity;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.state.MockStateManager;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAbstractListProcessor {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void testOnlyNewEntriesEmitted() {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        proc.addEntity("name", "id", 1492L);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1492L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1492L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id3", 1491L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1492L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1493L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1493L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id2", 1493L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        runner.clearTransferState();

        proc.addEntity("name", "id", 1494L);
        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        runner.clearTransferState();
    }

    @Test
    public void testStateStoredInClusterStateManagement() throws InitializationException {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final DistributedCache cache = new DistributedCache();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(AbstractListProcessor.DISTRIBUTED_CACHE_SERVICE, "cache");

        runner.run();

        proc.addEntity("name", "id", 1492L);
        runner.run();

        final Map<String, String> expectedState = new HashMap<>();
        expectedState.put(AbstractListProcessor.TIMESTAMP, "1492");
        expectedState.put(AbstractListProcessor.IDENTIFIER_PREFIX + ".1", "id");
        runner.getStateManager().assertStateEquals(expectedState, Scope.CLUSTER);
    }

    @Test
    public void testStateMigratedFromCacheService() throws InitializationException {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final DistributedCache cache = new DistributedCache();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(AbstractListProcessor.DISTRIBUTED_CACHE_SERVICE, "cache");

        final String serviceState = "{\"latestTimestamp\":1492,\"matchingIdentifiers\":[\"id\"]}";
        final String cacheKey = runner.getProcessor().getIdentifier() + ".lastListingTime./path";
        cache.stored.put(cacheKey, serviceState);

        runner.run();

        final MockStateManager stateManager = runner.getStateManager();
        final Map<String, String> expectedState = new HashMap<>();
        expectedState.put(AbstractListProcessor.TIMESTAMP, "1492");
        expectedState.put(AbstractListProcessor.IDENTIFIER_PREFIX + ".1", "id");
        stateManager.assertStateEquals(expectedState, Scope.CLUSTER);
    }

    @Test
    public void testNoStateToMigrate() throws Exception {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        runner.run();

        final MockStateManager stateManager = runner.getStateManager();
        final Map<String, String> expectedState = new HashMap<>();
        stateManager.assertStateEquals(expectedState, Scope.CLUSTER);
    }

    @Test
    public void testStateMigratedFromLocalFile() throws Exception {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);

        // Create a file that we will populate with the desired state
        File persistenceFile = testFolder.newFile(proc.persistenceFilename);
        // Override the processor's internal persistence file
        proc.persistenceFile = persistenceFile;

        // Local File persistence was a properties file format of <key>=<JSON entity listing representation>
        // Our ConcreteListProcessor is centered around files which are provided for a given path
        final String serviceState = proc.getPath(runner.getProcessContext()) + "={\"latestTimestamp\":1492,\"matchingIdentifiers\":[\"id\"]}";

        // Create a persistence file of the format anticipated
        try (FileOutputStream fos = new FileOutputStream(persistenceFile);) {
            fos.write(serviceState.getBytes(Charsets.UTF_8));
        }

        runner.run();

        // Verify the local persistence file is removed
        Assert.assertTrue("Failed to remove persistence file", !persistenceFile.exists());

        // Verify the state manager now maintains the associated state
        final Map<String, String> expectedState = new HashMap<>();
        expectedState.put(AbstractListProcessor.TIMESTAMP, "1492");
        expectedState.put(AbstractListProcessor.IDENTIFIER_PREFIX + ".1", "id");

        runner.getStateManager().assertStateEquals(expectedState, Scope.CLUSTER);
    }

    @Test
    public void testFetchOnStart() throws InitializationException {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        final DistributedCache cache = new DistributedCache();
        runner.addControllerService("cache", cache);
        runner.enableControllerService(cache);
        runner.setProperty(AbstractListProcessor.DISTRIBUTED_CACHE_SERVICE, "cache");

        runner.run();

        assertEquals(1, cache.fetchCount);
    }

    @Test
    public void testOnlyNewStateStored() throws IOException {
        final ConcreteListProcessor proc = new ConcreteListProcessor();
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 0);
        proc.addEntity("name", "id", 1492L);
        proc.addEntity("name", "id2", 1492L);

        runner.run();
        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 2);
        runner.clearTransferState();

        final StateMap stateMap = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(1, stateMap.getVersion());

        final Map<String, String> map = stateMap.toMap();
        assertEquals(3, map.size());
        assertEquals("1492", map.get("timestamp"));
        assertTrue(map.containsKey("id.1"));
        assertTrue(map.containsKey("id.2"));

        proc.addEntity("new name", "new id", 1493L);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConcreteListProcessor.REL_SUCCESS, 1);
        final StateMap updatedStateMap = runner.getStateManager().getState(Scope.CLUSTER);
        assertEquals(2, updatedStateMap.getVersion());

        final Map<String, String> updatedValues = updatedStateMap.toMap();
        assertEquals(2, updatedValues.size());
        assertEquals("1493", updatedValues.get("timestamp"));
        assertEquals("new id", updatedValues.get("id.1"));
    }


    private static class DistributedCache extends AbstractControllerService implements DistributedMapCacheClient {
        private final Map<Object, Object> stored = new HashMap<>();
        private int fetchCount = 0;

        @Override
        public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            return false;
        }

        @Override
        public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
            return null;
        }

        @Override
        public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
            return false;
        }

        @Override
        public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
            stored.put(key, value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
            fetchCount++;
            return (V) stored.get(key);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public <K> boolean remove(K key, Serializer<K> serializer) throws IOException {
            final Object value = stored.remove(key);
            return value != null;
        }
    }


    private static class ConcreteListProcessor extends AbstractListProcessor<ListableEntity> {
        private final List<ListableEntity> entities = new ArrayList<>();

        public final String persistenceFilename = "ListProcessor-local-state-" + UUID.randomUUID().toString() + ".json";
        public String persistenceFolder = "target/";
        public File persistenceFile = new File(persistenceFolder + persistenceFilename);

        @Override
        protected File getPersistenceFile() {
            return persistenceFile;
        }

        public void addEntity(final String name, final String identifier, final long timestamp) {
            final ListableEntity entity = new ListableEntity() {
                @Override
                public String getName() {
                    return name;
                }

                @Override
                public String getIdentifier() {
                    return identifier;
                }

                @Override
                public long getTimestamp() {
                    return timestamp;
                }
            };

            entities.add(entity);
        }

        @Override
        protected Map<String, String> createAttributes(final ListableEntity entity, final ProcessContext context) {
            return Collections.emptyMap();
        }

        @Override
        protected String getPath(final ProcessContext context) {
            return "/path";
        }

        @Override
        protected List<ListableEntity> performListing(final ProcessContext context, final Long minTimestamp) throws IOException {
            return Collections.unmodifiableList(entities);
        }

        @Override
        protected boolean isListingResetNecessary(PropertyDescriptor property) {
            return false;
        }

        @Override
        protected Scope getStateScope(final ProcessContext context) {
            return Scope.CLUSTER;
        }
    }
}
