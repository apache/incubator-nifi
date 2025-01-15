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
import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@Tags({"hash", "dupe", "duplicate", "dedupe"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Caches a value, computed from FlowFile attributes, for each incoming FlowFile and determines if the cached value has already been seen. "
        + "If so, routes the FlowFile to 'duplicate' with an attribute named 'original.identifier' that specifies the original FlowFile's "
        + "\"description\", which is specified in the <FlowFile Description> property. If the FlowFile is not determined to be a duplicate, the Processor "
        + "routes the FlowFile to 'non-duplicate'")
@WritesAttribute(attribute = "original.flowfile.description", description = "All FlowFiles routed to the duplicate relationship will have "
        + "an attribute added named original.flowfile.description. The value of this attribute is determined by the attributes of the original "
        + "copy of the data and by the FlowFile Description property.")
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService", "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer"})
public class DetectDuplicate extends AbstractProcessor {

    public static final String ORIGINAL_DESCRIPTION_ATTRIBUTE_NAME = "original.flowfile.description";

    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("Distributed Cache Service")
            .description("The Controller Service that is used to cache unique identifiers, used to determine duplicates")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();
    public static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Cache Entry Identifier")
            .description(
                    "A FlowFile attribute, or the results of an Attribute Expression Language statement, which will be evaluated "
                    + "against a FlowFile in order to determine the value used to identify duplicates; it is this value that is cached")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .defaultValue("${hash.value}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor FLOWFILE_DESCRIPTION = new PropertyDescriptor.Builder()
            .name("FlowFile Description")
            .description("When a FlowFile is added to the cache, this value is stored along with it so that if a duplicate is found, this "
                    + "description of the original FlowFile will be added to the duplicate's \""
                    + ORIGINAL_DESCRIPTION_ATTRIBUTE_NAME + "\" attribute")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("")
            .build();

    public static final PropertyDescriptor AGE_OFF_DURATION = new PropertyDescriptor.Builder()
            .name("Age Off Duration")
            .description("Time interval to age off cached FlowFiles")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Cache The Entry Identifier")
            .description("When true this cause the processor to check for duplicates and cache the Entry Identifier. When false, "
                    + "the processor would only check for duplicates and not cache the Entry Identifier, requiring another "
                    + "processor to add identifiers to the distributed cache.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CACHE_ENTRY_IDENTIFIER,
            FLOWFILE_DESCRIPTION,
            AGE_OFF_DURATION,
            DISTRIBUTED_CACHE_SERVICE,
            CACHE_IDENTIFIER
    );

    public static final Relationship REL_DUPLICATE = new Relationship.Builder()
            .name("duplicate")
            .description("If a FlowFile has been detected to be a duplicate, it will be routed to this relationship")
            .build();
    public static final Relationship REL_NON_DUPLICATE = new Relationship.Builder()
            .name("non-duplicate")
            .description("If a FlowFile's Cache Entry Identifier was not found in the cache, it will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If unable to communicate with the cache, the FlowFile will be penalized and routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_DUPLICATE,
            REL_NON_DUPLICATE,
            REL_FAILURE
    );

    private final Serializer<String> keySerializer = new StringSerializer();
    private final Serializer<CacheValue> valueSerializer = new CacheValueSerializer();
    private final Deserializer<CacheValue> valueDeserializer = new CacheValueDeserializer();

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
        final String cacheKey = context.getProperty(CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isBlank(cacheKey)) {
            logger.error("FlowFile {} has no attribute for given Cache Entry Identifier", flowFile);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        final DistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);
        final Long durationMS = context.getProperty(AGE_OFF_DURATION).asTimePeriod(TimeUnit.MILLISECONDS);
        final long now = System.currentTimeMillis();

        try {
            final String flowFileDescription = context.getProperty(FLOWFILE_DESCRIPTION).evaluateAttributeExpressions(flowFile).getValue();
            final CacheValue cacheValue = new CacheValue(flowFileDescription, now);
            final CacheValue originalCacheValue;

            final boolean shouldCacheIdentifier = context.getProperty(CACHE_IDENTIFIER).asBoolean();
            if (shouldCacheIdentifier) {
                originalCacheValue = cache.getAndPutIfAbsent(cacheKey, cacheValue, keySerializer, valueSerializer, valueDeserializer);
            } else {
                originalCacheValue = cache.get(cacheKey, keySerializer, valueDeserializer);
            }

            boolean duplicate = originalCacheValue != null;
            if (duplicate && durationMS != null && (now >= originalCacheValue.getEntryTimeMS() + durationMS)) {
                boolean status = cache.remove(cacheKey, keySerializer);
                logger.debug("Removal of expired cached entry with key {} returned {}", cacheKey, status);

                // both should typically result in duplicate being false...but, better safe than sorry
                if (shouldCacheIdentifier) {
                    duplicate = !cache.putIfAbsent(cacheKey, cacheValue, keySerializer, valueSerializer);
                } else {
                    duplicate = cache.containsKey(cacheKey, keySerializer);
                }
            }

            if (duplicate) {
                session.getProvenanceReporter().route(flowFile, REL_DUPLICATE, "Duplicate of: " + ORIGINAL_DESCRIPTION_ATTRIBUTE_NAME);
                String originalFlowFileDescription = originalCacheValue.getDescription();
                flowFile = session.putAttribute(flowFile, ORIGINAL_DESCRIPTION_ATTRIBUTE_NAME, originalFlowFileDescription);
                session.transfer(flowFile, REL_DUPLICATE);
                logger.info("Found {} to be a duplicate of FlowFile with description {}", flowFile, originalFlowFileDescription);
                session.adjustCounter("Duplicates Detected", 1L, false);
            } else {
                session.getProvenanceReporter().route(flowFile, REL_NON_DUPLICATE);
                session.transfer(flowFile, REL_NON_DUPLICATE);
                logger.info("Could not find a duplicate entry in cache for {}; routing to non-duplicate", flowFile);
                session.adjustCounter("Non-Duplicate Files Processed", 1L, false);
            }
        } catch (final IOException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with cache when processing {}", flowFile, e);
        }
    }

    private static class CacheValue {

        private final String description;
        private final long entryTimeMS;

        public CacheValue(String description, long entryTimeMS) {
            this.description = description;
            this.entryTimeMS = entryTimeMS;
        }

        public String getDescription() {
            return description;
        }

        public long getEntryTimeMS() {
            return entryTimeMS;
        }

    }

    private static class CacheValueSerializer implements Serializer<CacheValue> {

        @Override
        public void serialize(final CacheValue entry, final OutputStream out) throws SerializationException, IOException {
            long time = entry.getEntryTimeMS();
            byte[] writeBuffer = new byte[8];
            writeBuffer[0] = (byte) (time >>> 56);
            writeBuffer[1] = (byte) (time >>> 48);
            writeBuffer[2] = (byte) (time >>> 40);
            writeBuffer[3] = (byte) (time >>> 32);
            writeBuffer[4] = (byte) (time >>> 24);
            writeBuffer[5] = (byte) (time >>> 16);
            writeBuffer[6] = (byte) (time >>> 8);
            writeBuffer[7] = (byte) (time);
            out.write(writeBuffer, 0, 8);
            out.write(entry.getDescription().getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class CacheValueDeserializer implements Deserializer<CacheValue> {

        @Override
        public CacheValue deserialize(final byte[] input) throws DeserializationException, IOException {
            if (input.length == 0) {
                return null;
            }
            long time = ((long) input[0] << 56)
                    + ((long) (input[1] & 255) << 48)
                    + ((long) (input[2] & 255) << 40)
                    + ((long) (input[3] & 255) << 32)
                    + ((long) (input[4] & 255) << 24)
                    + ((input[5] & 255) << 16)
                    + ((input[6] & 255) << 8)
                    + ((input[7] & 255));
            String description = new String(input, 8, input.length - 8, StandardCharsets.UTF_8);
            return new CacheValue(description, time);
        }
    }

    private static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

}
