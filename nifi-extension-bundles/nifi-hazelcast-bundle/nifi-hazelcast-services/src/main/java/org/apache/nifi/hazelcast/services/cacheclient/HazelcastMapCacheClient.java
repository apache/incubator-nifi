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
package org.apache.nifi.hazelcast.services.cacheclient;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hazelcast.services.cache.HazelcastCache;
import org.apache.nifi.hazelcast.services.cachemanager.HazelcastCacheManager;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of DistributedMapCacheClient that uses Hazelcast as the backing cache.
 *
 * Note: By design, the client should not directly depend on Hazelcast specific classes to allow easy version and implementation changes.
 */
@Tags({"hazelcast", "cache", "map"})
@CapabilityDescription("An implementation of DistributedMapCacheClient that uses Hazelcast as the backing cache. This service relies on " +
        "an other controller service, manages the actual Hazelcast calls, set in Hazelcast Cache Manager.")
public class HazelcastMapCacheClient extends AbstractControllerService implements AtomicDistributedMapCacheClient<Long> {

    public static final PropertyDescriptor HAZELCAST_CACHE_MANAGER = new PropertyDescriptor.Builder()
            .name("hazelcast-cache-manager")
            .displayName("Hazelcast Cache Manager")
            .description("A Hazelcast Cache Manager which manages connections to Hazelcast and provides cache instances.")
            .identifiesControllerService(HazelcastCacheManager.class)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor HAZELCAST_CACHE_NAME = new PropertyDescriptor.Builder()
            .name("hazelcast-cache-name")
            .displayName("Hazelcast Cache Name")
            .description("The name of a given cache. A Hazelcast cluster may handle multiple independent caches, each identified by a name." +
                    " Clients using caches with the same name are working on the same data structure within Hazelcast.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor HAZELCAST_ENTRY_TTL = new PropertyDescriptor.Builder()
            .name("hazelcast-entry-ttl")
            .displayName("Hazelcast Entry Lifetime")
            .description("Indicates how long the written entries should exist in Hazelcast. Setting it to '0 secs' means that the data" +
                    "will exists until its deletion or until the Hazelcast server is shut down. Using `EmbeddedHazelcastCacheManager` as" +
                    "cache manager will not provide policies to limit the size of the cache.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 min") // Note: in case of Hazelcast IMap, negative value would mean "map default" which might be overridden by a different client.
            .build();

    private static final long STARTING_REVISION = 1;
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        HAZELCAST_CACHE_MANAGER,
        HAZELCAST_CACHE_NAME,
        HAZELCAST_ENTRY_TTL
    );

    private volatile HazelcastCache cache = null;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final HazelcastCacheManager hazelcastCacheManager = context.getProperty(HAZELCAST_CACHE_MANAGER).asControllerService(HazelcastCacheManager.class);
        cache = hazelcastCacheManager.getCache(
                context.getProperty(HAZELCAST_CACHE_NAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(HAZELCAST_ENTRY_TTL).asTimePeriod(TimeUnit.MILLISECONDS));
        getLogger().debug("Enable Hazelcast cache client for cache {}", cache.name());
    }

    @OnDisabled
    public void onDisabled() {
        if (cache != null) {
            // The cache state will be preserved until the Service is not stopped!
            getLogger().debug("Disable Hazelcast cache client for cache {}", cache.name());
            cache = null;
        }
    }

    @Override
    public <K> Set<K> keySet(Deserializer<K> keyDeserializer) throws IOException {
        final HashSet<K> keySet = new HashSet<>();
        for (String key : cache.keySet()) {
            keySet.add(parseCacheEntryKey(key, keyDeserializer));
        }
        return keySet;
    }

    @Override
    public <K, V> AtomicCacheEntry<K, V, Long> fetch(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] result = cache.get(serializeCacheEntryKey(key, keySerializer));
        return (result == null) ? null : new AtomicCacheEntry<>(key, parsePayload(valueDeserializer, result), parseRevision(result));
    }

    @Override
    public <K, V> boolean replace(final AtomicCacheEntry<K, V, Long> entry, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        if (entry.getKey() == null) {
            return false;
        }

        final String key = serializeCacheEntryKey(entry.getKey(), keySerializer);

        try (final HazelcastCache.HazelcastCacheEntryLock lock = cache.acquireLock(key)) {
            final byte[] oldValue = cache.get(key);

            if (oldValue == null && (entry.getRevision().isEmpty() || entry.getRevision().get() < STARTING_REVISION)) {
                cache.put(key, serializeCacheEntryValue(entry.getValue(), valueSerializer, STARTING_REVISION));
                getLogger().debug("Entry with key {} was added during replace", key);
                return true;
            } else if (oldValue != null && Objects.equals(entry.getRevision().get(), parseRevision(oldValue))) {
                final long newRevision = entry.getRevision().get() + 1;
                cache.put(key, serializeCacheEntryValue(entry.getValue(), valueSerializer, newRevision));
                getLogger().debug("Entry with key {} was updated during replace, with revision {}", key, newRevision);
                return true;
            }
        }

        return false;
    }

    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        return cache.putIfAbsent(serializeCacheEntryKey(key, keySerializer), serializeCacheEntryValue(value, valueSerializer, STARTING_REVISION)) == null;
    }

    @Override
    public <K, V> V getAndPutIfAbsent(
            final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer
    ) throws IOException {
        final byte[] result = cache.putIfAbsent(serializeCacheEntryKey(key, keySerializer), serializeCacheEntryValue(value, valueSerializer, STARTING_REVISION));
        return (result == null) ? null : parsePayload(valueDeserializer, result);
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
        return cache.contains(serializeCacheEntryKey(key, keySerializer));
    }

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        cache.put(serializeCacheEntryKey(key, keySerializer), serializeCacheEntryValue(value, valueSerializer, STARTING_REVISION));
    }

    @Override
    public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] result = cache.get(serializeCacheEntryKey(key, keySerializer));
        return result == null ? null : parsePayload(valueDeserializer, result);
    }

    @Override
    public <K> boolean remove(final K key, final Serializer<K> keySerializer) throws IOException {
        return cache.remove(serializeCacheEntryKey(key, keySerializer));
    }

    @Override
    public void close() {
        getLogger().debug("Closing {}", getClass().getSimpleName());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private <S> String serializeCacheEntryKey(final S key, final Serializer<S> serializer) throws IOException {
        final String result;

        if (key instanceof String) {
            result = (String) key;
        } else {
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            serializer.serialize(key, stream);
            result = stream.toString(StandardCharsets.UTF_8);
        }

        if (result.isEmpty()) {
            throw new IOException("Cache record key cannot be empty!");
        }

        return result;
    }

    private static <K> K parseCacheEntryKey(final String key, final Deserializer<K> keyDeserializer) throws IOException {
        final byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        return keyDeserializer.deserialize(keyBytes);
    }

    /**
     * Serializes a value using the given serializer. The first eight bytes of the array contains the revision.
     * The rest holds the actual serialized value.
     *
     * @param value      The value to serialize.
     * @param serializer The serializer to use in order to serialize the incoming value.
     * @param version    The version of the entry.
     * @param <S>        The type of the value to be serialized.
     * @return Byte array containing both version and value of the cache entry.
     * @throws IOException In case of any issue during working with intermediate byte stream.
     */
    private <S> byte[] serializeCacheEntryValue(final S value, final Serializer<S> serializer, final long version) throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();

        stream.write(getVersionByteArray(version));
        serializer.serialize(value, stream);
        return stream.toByteArray();
    }

    private static long parseRevision(final byte[] value) {
        return ByteBuffer.wrap(Arrays.copyOfRange(value, 0, Long.BYTES)).getLong();
    }

    private static <V> V parsePayload(final Deserializer<V> deserializer, final byte[] value) throws IOException {
        return deserializer.deserialize(Arrays.copyOfRange(value, Long.BYTES, value.length));
    }

    private byte[] getVersionByteArray(final long version) {
        return ByteBuffer.allocate(Long.BYTES).putLong(version).array();
    }
}
