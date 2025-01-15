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
package org.apache.nifi.redis.service;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.util.RedisAction;
import org.apache.nifi.util.Tuple;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.types.Expiration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.redis.util.RedisUtils.REDIS_CONNECTION_POOL;
import static org.apache.nifi.redis.util.RedisUtils.TTL;

@Tags({ "redis", "distributed", "cache", "map" })
@CapabilityDescription("An implementation of DistributedMapCacheClient that uses Redis as the backing cache. " +
        "This service is intended to be used when a non-atomic DistributedMapCacheClient is required.")
public class SimpleRedisDistributedMapCacheClientService extends AbstractControllerService implements DistributedMapCacheClient {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REDIS_CONNECTION_POOL,
            TTL
    );

    private volatile RedisConnectionPool redisConnectionPool;
    private Long ttl;

    protected Long getTtl() {
        return ttl;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.redisConnectionPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPool.class);
        this.ttl = context.getProperty(TTL).asTimePeriod(TimeUnit.SECONDS);

        if (ttl == 0) {
            this.ttl = -1L;
        }
    }

    @OnDisabled
    public void onDisabled() {
        this.redisConnectionPool = null;
    }

    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        return withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            boolean set = redisConnection.setNX(kv.getKey(), kv.getValue());

            if (ttl != -1L && set) {
                redisConnection.expire(kv.getKey(), ttl);
            }

            return set;
        });
    }

    @Override
    public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer) throws IOException {
        return withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            do {
                // start a watch on the key and retrieve the current value
                redisConnection.watch(kv.getKey());
                final byte[] existingValue = redisConnection.get(kv.getKey());

                // start a transaction and perform the put-if-absent
                redisConnection.multi();
                redisConnection.setNX(kv.getKey(), kv.getValue());

                // Set the TTL only if the key doesn't exist already
                if (ttl != -1L && existingValue == null) {
                    redisConnection.expire(kv.getKey(), ttl);
                }

                // execute the transaction
                final List<Object> results = redisConnection.exec();

                // if the results list was empty, then the transaction failed (i.e. key was modified after we started watching), so keep looping to retry
                // if the results list was null, then the transaction failed
                // if the results list has results, then the transaction succeeded and it should have the result of the setNX operation
                if (results != null && results.size() > 0) {
                    final Object firstResult = results.get(0);
                    if (firstResult instanceof Boolean) {
                        final Boolean absent = (Boolean) firstResult;
                        return absent ? null : valueDeserializer.deserialize(existingValue);
                    } else {
                        // this shouldn't really happen, but just in case there is a non-boolean result then bounce out of the loop
                        throw new IOException("Unexpected result from Redis transaction: Expected Boolean result, but got "
                                + firstResult.getClass().getName() + " with value " + firstResult.toString());
                    }
                }
            } while (isEnabled());

            return null;
        });
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            return redisConnection.exists(k);
        });
    }

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        withConnection(redisConnection -> {
            final Tuple<byte[], byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
            redisConnection.set(kv.getKey(), kv.getValue(), Expiration.seconds(ttl), RedisStringCommands.SetOption.upsert());
            return null;
        });
    }

    @Override
    public <K, V> void putAll(Map<K, V> keysAndValues, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        withConnection(redisConnection -> {
            Map<byte[], byte[]> values = new HashMap<>();
            for (Map.Entry<K, V> entry : keysAndValues.entrySet()) {
                final Tuple<byte[], byte[]> kv = serialize(entry.getKey(), entry.getValue(), keySerializer, valueSerializer);
                values.put(kv.getKey(), kv.getValue());
            }

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Queued up %d tuples to mset on Redis connection.", values.size()));
            }

            if (!values.isEmpty()) {
                redisConnection.mSet(values);
                if (ttl != -1L) {
                    values.keySet().forEach(k -> redisConnection.expire(k, ttl));
                }
            }
            return null;
        });
    }

    @Override
    public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            final byte[] v = redisConnection.get(k);
            return v == null ? null : valueDeserializer.deserialize(v);
        });
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public <K> boolean remove(final K key, final Serializer<K> keySerializer) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] k = serialize(key, keySerializer);
            final long numRemoved = redisConnection.del(k);
            return numRemoved > 0;
        });
    }

    /**
     *  Convert the list of all keys to an array.
     */
    protected byte[][] getKeys(final List<byte[]> keys) {
        final byte[][] allKeysArray = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            allKeysArray[i] = keys.get(i);
        }
        return  allKeysArray;
    }

    protected <K, V> Tuple<byte[], byte[]> serialize(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        keySerializer.serialize(key, out);
        final byte[] k = out.toByteArray();

        out.reset();

        valueSerializer.serialize(value, out);
        final byte[] v = out.toByteArray();

        return new Tuple<>(k, v);
    }

    protected <K> byte[] serialize(final K key, final Serializer<K> keySerializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        keySerializer.serialize(key, out);
        return out.toByteArray();
    }

    protected <T> T withConnection(final RedisAction<T> action) throws IOException {
        RedisConnection redisConnection = null;
        try {
            redisConnection = redisConnectionPool.getConnection();
            return action.execute(redisConnection);
        } finally {
            if (redisConnection != null) {
                try {
                    redisConnection.close();
                } catch (Exception e) {
                    getLogger().warn("Error closing connection", e);
                }
            }
        }
    }
}
