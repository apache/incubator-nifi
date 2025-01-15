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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

@SupportsBatching
@Tags({"map", "cache", "put", "distributed"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Gets the content of a FlowFile and puts it to a distributed map cache, using a cache key " +
    "computed from FlowFile attributes. If the cache already contains the entry and the cache update strategy is " +
    "'keep original' the entry is not replaced.'")
@WritesAttribute(attribute = "cached", description = "All FlowFiles will have an attribute 'cached'. The value of this " +
    "attribute is true, is the FlowFile is cached, otherwise false.")
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService", "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "org.apache.nifi.processors.standard.FetchDistributedMapCache"})
public class PutDistributedMapCache extends AbstractProcessor {

    public static final String CACHED_ATTRIBUTE_NAME = "cached";

    // Identifies the distributed map cache client
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
        .name("Distributed Cache Service")
        .description("The Controller Service that is used to cache flow files")
        .required(true)
        .identifiesControllerService(DistributedMapCacheClient.class)
        .build();

    // Selects the FlowFile attribute, whose value is used as cache key
    public static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder()
        .name("Cache Entry Identifier")
        .description("A FlowFile attribute, or the results of an Attribute Expression Language statement, which will " +
            "be evaluated against a FlowFile in order to determine the cache key")
        .required(true)
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    public static final AllowableValue CACHE_UPDATE_REPLACE = new AllowableValue("replace", "Replace if present",
        "Adds the specified entry to the cache, replacing any value that is currently set.");

    public static final AllowableValue CACHE_UPDATE_KEEP_ORIGINAL = new AllowableValue("keeporiginal", "Keep original",
        "Adds the specified entry to the cache, if the key does not exist.");

    public static final PropertyDescriptor CACHE_UPDATE_STRATEGY = new PropertyDescriptor.Builder()
        .name("Cache update strategy")
        .description("Determines how the cache is updated if the cache already contains the entry")
        .required(true)
        .allowableValues(CACHE_UPDATE_REPLACE, CACHE_UPDATE_KEEP_ORIGINAL)
        .defaultValue(CACHE_UPDATE_REPLACE.getValue())
        .build();

    public static final PropertyDescriptor CACHE_ENTRY_MAX_BYTES = new PropertyDescriptor.Builder()
        .name("Max cache entry size")
        .description("The maximum amount of data to put into cache")
        .required(false)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("1 MB")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CACHE_ENTRY_IDENTIFIER,
            DISTRIBUTED_CACHE_SERVICE,
            CACHE_UPDATE_STRATEGY,
            CACHE_ENTRY_MAX_BYTES
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that is successfully inserted into cache will be routed to this relationship")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that cannot be inserted into the cache will be routed to this relationship")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private final Serializer<String> keySerializer = new StringSerializer();
    private final Serializer<byte[]> valueSerializer = new CacheValueSerializer();
    private final Deserializer<byte[]> valueDeserializer = new CacheValueDeserializer();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        // cache key is computed from attribute 'CACHE_ENTRY_IDENTIFIER' with expression language support
        final String cacheKey = context.getProperty(CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();

        // if the computed value is null, or empty, we transfer the flow file to failure relationship
        if (StringUtils.isBlank(cacheKey)) {
            logger.error("FlowFile {} has no attribute for given Cache Entry Identifier", flowFile);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // the cache client used to interact with the distributed cache
        final DistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        try {

            final long maxCacheEntrySize = context.getProperty(CACHE_ENTRY_MAX_BYTES).asDataSize(DataUnit.B).longValue();
            long flowFileSize = flowFile.getSize();

            // too big flow file
            if (flowFileSize > maxCacheEntrySize) {
                logger.warn("Flow file {} size {} exceeds the max cache entry size ({} B).", flowFile, flowFileSize, maxCacheEntrySize);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            if (flowFileSize == 0) {
                logger.warn("Flow file {} is empty, there is nothing to cache.", flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;

            }

            // get flow file content
            final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            session.exportTo(flowFile, byteStream);
            byte[] cacheValue = byteStream.toByteArray();
            final String updateStrategy = context.getProperty(CACHE_UPDATE_STRATEGY).getValue();
            boolean cached = false;

            if (updateStrategy.equals(CACHE_UPDATE_REPLACE.getValue())) {
                cache.put(cacheKey, cacheValue, keySerializer, valueSerializer);
                cached = true;
            } else if (updateStrategy.equals(CACHE_UPDATE_KEEP_ORIGINAL.getValue())) {
                final byte[] oldValue = cache.getAndPutIfAbsent(cacheKey, cacheValue, keySerializer, valueSerializer, valueDeserializer);
                if (oldValue == null) {
                    cached = true;
                }
            }

            // set 'cached' attribute
            flowFile = session.putAttribute(flowFile, CACHED_ATTRIBUTE_NAME, String.valueOf(cached));

            if (cached) {
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (final IOException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with cache when processing {}", flowFile, e);
        }
    }

    public static class CacheValueSerializer implements Serializer<byte[]> {

        @Override
        public void serialize(final byte[] bytes, final OutputStream out) throws SerializationException, IOException {
            out.write(bytes);
        }
    }

    public static class CacheValueDeserializer implements Deserializer<byte[]> {

        @Override
        public byte[] deserialize(final byte[] input) throws DeserializationException, IOException {
            if (input == null || input.length == 0) {
                return null;
            }
            return input;
        }
    }

    /**
     * Simple string serializer, used for serializing the cache key
     */
    public static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

}
