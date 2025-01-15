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
package org.apache.nifi.distributed.cache.client;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.adapter.AtomicCacheEntryInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.MapValuesInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.SetInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.ValueInboundAdapter;
import org.apache.nifi.distributed.cache.protocol.ProtocolVersion;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.StandardVersionNegotiatorFactory;
import org.apache.nifi.remote.VersionNegotiatorFactory;
import org.apache.nifi.ssl.SSLContextProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"distributed", "cache", "state", "map", "cluster"})
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.server.map.MapCacheServer"})
@CapabilityDescription("Provides the ability to communicate with a MapCacheServer. This can be used in order to share a Map "
    + "between nodes in a NiFi cluster")
public class MapCacheClientService extends AbstractControllerService implements AtomicDistributedMapCacheClient<Long> {

    private static final long DEFAULT_CACHE_REVISION = 0L;

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
        .name("Server Hostname")
        .description("The name of the server that is running the DistributedMapCacheServer service")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
        .name("Server Port")
        .description("The port on the remote server that is to be used when communicating with the DistributedMapCacheServer service")
        .required(true)
        .addValidator(StandardValidators.PORT_VALIDATOR)
        .defaultValue("4557")
        .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL Context Service")
        .description("If specified, indicates the SSL Context Service that is used to communicate with the "
                + "remote server. If not specified, communications will not be encrypted")
        .required(false)
        .identifiesControllerService(SSLContextProvider.class)
        .build();
    public static final PropertyDescriptor COMMUNICATIONS_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Communications Timeout")
        .description("Specifies how long to wait when communicating with the remote server before determining that "
                + "there is a communications failure if data cannot be sent or received")
        .required(true)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("30 secs")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        HOSTNAME,
        PORT,
        SSL_CONTEXT_SERVICE,
        COMMUNICATIONS_TIMEOUT
    );

    private volatile NettyMapCacheClient cacheClient = null;

    /**
     * Creator of object used to broker the version of the distributed cache protocol with the service.
     */
    private volatile VersionNegotiatorFactory versionNegotiatorFactory = null;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        getLogger().debug("Enabling Map Cache Client Service [{}]", context.getName());
        this.versionNegotiatorFactory  = new StandardVersionNegotiatorFactory(
                ProtocolVersion.V3.value(), ProtocolVersion.V2.value(), ProtocolVersion.V1.value());
        this.cacheClient = new NettyMapCacheClient(
                context.getProperty(HOSTNAME).getValue(),
                context.getProperty(PORT).asInteger(),
                context.getProperty(COMMUNICATIONS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(),
                context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class),
                versionNegotiatorFactory,
                this.getIdentifier(),
                getLogger());
    }

    @OnShutdown
    @OnDisabled
    public void onDisabled() throws IOException {
        if (cacheClient != null) {
            this.cacheClient.close();
        }
        this.versionNegotiatorFactory = null;
        this.cacheClient = null;
    }

    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final byte[] bytesValue = CacheClientSerde.serialize(value, valueSerializer);
        return cacheClient.putIfAbsent(bytesKey, bytesValue);
    }

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final byte[] bytesValue = CacheClientSerde.serialize(value, valueSerializer);
        cacheClient.put(bytesKey, bytesValue);
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        return cacheClient.containsKey(bytesKey);
    }

    @Override
    public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final byte[] bytesValue = CacheClientSerde.serialize(value, valueSerializer);
        final ValueInboundAdapter<V> inboundAdapter = new ValueInboundAdapter<>(valueDeserializer);
        return cacheClient.getAndPutIfAbsent(bytesKey, bytesValue, inboundAdapter);
    }

    @Override
    public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final ValueInboundAdapter<V> inboundAdapter = new ValueInboundAdapter<>(valueDeserializer);
        return cacheClient.get(bytesKey, inboundAdapter);
    }

    @Override
    public <K, V> Map<K, V> subMap(Set<K> keys, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        Collection<byte[]> bytesKeys = CacheClientSerde.serialize(keys, keySerializer);
        final MapValuesInboundAdapter<K, V> inboundAdapter =
                new MapValuesInboundAdapter<>(keys, valueDeserializer, new HashMap<>());
        return cacheClient.subMap(bytesKeys, inboundAdapter);
    }

    @Override
    public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, serializer);
        return cacheClient.remove(bytesKey);
    }

    @Override
    public <K, V> V removeAndGet(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final ValueInboundAdapter<V> inboundAdapter = new ValueInboundAdapter<>(valueDeserializer);
        return cacheClient.removeAndGet(bytesKey, inboundAdapter);
    }

    @Override
    public <K, V> AtomicCacheEntry<K, V, Long> fetch(final K key, final Serializer<K> keySerializer,
                                                     final Deserializer<V> valueDeserializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(key, keySerializer);
        final AtomicCacheEntryInboundAdapter<K, V> inboundAdapter =
                new AtomicCacheEntryInboundAdapter<>(key, valueDeserializer);
        return cacheClient.fetch(bytesKey, inboundAdapter);
    }

    @Override
    public <K, V> boolean replace(AtomicCacheEntry<K, V, Long> entry, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final byte[] bytesKey = CacheClientSerde.serialize(entry.getKey(), keySerializer);
        final byte[] bytesValue = CacheClientSerde.serialize(entry.getValue(), valueSerializer);
        final long revision = entry.getRevision().orElse(DEFAULT_CACHE_REVISION);
        return cacheClient.replace(bytesKey, bytesValue, revision);
    }

    @Override
    public <K> Set<K> keySet(Deserializer<K> keyDeserializer) throws IOException {
        final SetInboundAdapter<K> inboundAdapter = new SetInboundAdapter<>(keyDeserializer, new HashSet<>());
        return cacheClient.keySet(inboundAdapter);
    }

    @Override
    public void close() throws IOException {
        if (isEnabled()) {
            onDisabled();
        }
    }
}
