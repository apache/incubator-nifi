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

package org.apache.nifi.lookup;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;

@Tags({"lookup", "enrich", "key", "value", "map", "cache", "distributed"})
@CapabilityDescription("Allows to choose a distributed map cache client to retrieve the value associated to a key. "
    + "The coordinates that are passed to the lookup must contain the key 'key'.")
public class DistributedMapCacheLookupService extends AbstractControllerService implements StringLookupService {

    private static final List<Charset> STANDARD_CHARSETS = Arrays.asList(
            StandardCharsets.UTF_8,
            StandardCharsets.US_ASCII,
            StandardCharsets.ISO_8859_1,
            StandardCharsets.UTF_16,
            StandardCharsets.UTF_16LE,
            StandardCharsets.UTF_16BE);

    private static final String KEY = "key";
    private static final Set<String> REQUIRED_KEYS = Stream.of(KEY).collect(Collectors.toSet());

    private volatile DistributedMapCacheClient cache;
    private volatile static Charset charset;
    private final Serializer<String> keySerializer = new StringSerializer();
    private final Deserializer<String> valueDeserializer = new StringDeserializer();

    public static final PropertyDescriptor PROP_DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("distributed-map-cache-service")
            .displayName("Distributed Cache Service")
            .description("The Controller Service that is used to get the cached values.")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor CHARACTER_ENCODING = new PropertyDescriptor.Builder()
            .name("character-encoding")
            .displayName("Character Encoding")
            .description("Specifies a character encoding to use.")
            .required(true)
            .allowableValues(getStandardCharsetNames())
            .defaultValue(StandardCharsets.UTF_8.displayName())
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        PROP_DISTRIBUTED_CACHE_SERVICE,
        CHARACTER_ENCODING
    );

    private static Set<String> getStandardCharsetNames() {
        return STANDARD_CHARSETS.stream().map(c -> c.displayName()).collect(Collectors.toSet());
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        cache = context.getProperty(PROP_DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
        charset = Charset.forName(context.getProperty(CHARACTER_ENCODING).getValue());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Optional<String> lookup(final Map<String, Object> coordinates) {
        if (coordinates == null) {
            return Optional.empty();
        }

        final String key = coordinates.get(KEY).toString();
        if (key == null) {
            return Optional.empty();
        }

        try {
            return Optional.ofNullable(cache.get(key, keySerializer, valueDeserializer));
        } catch (IOException e) {
            getLogger().error("Error while trying to get the value from distributed map cache with key = {}", key, e);
            return Optional.empty();
        }
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

    public static class StringDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(final byte[] input) throws DeserializationException, IOException {
            if (input == null || input.length == 0) {
                return null;
            }
            return new String(input, 0, input.length, charset);
        }
    }

    public static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(charset));
        }
    }

}
