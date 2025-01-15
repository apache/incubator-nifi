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

import com.google.common.base.Joiner;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.codec.binary.StringUtils.getBytesUtf8;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@SystemResourceConsideration(resource = SystemResource.MEMORY,
        description = "The HashSet filter type will grow memory space proportionate to the number of unique records processed. " +
                "The BloomFilter type will use constant memory regardless of the number of records processed.")
@SystemResourceConsideration(resource = SystemResource.CPU,
        description = "If a more advanced hash algorithm is chosen, the amount of time required to hash any particular " +
                "record could increase substantially."
)
@Tags({"text", "record", "update", "change", "replace", "modify", "distinct", "unique",
        "filter", "hash", "dupe", "duplicate", "dedupe"})
@CapabilityDescription("This processor de-duplicates individual records within a record set. " +
        "It can operate on a per-file basis using an in-memory hashset or bloom filter. " +
        "When configured with a distributed map cache, it de-duplicates records across multiple files.")
@WritesAttribute(attribute = DeduplicateRecord.RECORD_COUNT_ATTRIBUTE, description = "Number of records written to the destination FlowFile.")
@DynamicProperty(
        name = "Name of the property.",
        value = "A valid RecordPath to the record field to be included in the cache key used for deduplication.",
        description = "A record's cache key is generated by combining the name of each dynamic property with its evaluated record value " +
                "(as specified by the corresponding RecordPath).")
@SeeAlso(classNames = {
        "org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
        "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "org.apache.nifi.processors.standard.DetectDuplicate"
})
public class DeduplicateRecord extends AbstractProcessor {
    public static final char JOIN_CHAR = '~';
    public static final String RECORD_COUNT_ATTRIBUTE = "record.count";
    public static final String RECORD_HASH_VALUE_ATTRIBUTE = "record.hash.value";

    private volatile RecordPathCache recordPathCache;
    private volatile List<PropertyDescriptor> dynamicProperties;

    // VALUES

    static final AllowableValue NONE_ALGORITHM_VALUE = new AllowableValue("none", "None",
            "Do not use a hashing algorithm. The value of resolved RecordPaths will be combined with a delimiter " +
                    "(" + DeduplicateRecord.JOIN_CHAR + ") to form the unique cache key. " +
                    "This may use significantly more storage depending on the size and shape or your data.");
    static final AllowableValue SHA256_ALGORITHM_VALUE = new AllowableValue(MessageDigestAlgorithms.SHA_256, "SHA-256",
            "SHA-256 cryptographic hashing algorithm.");
    static final AllowableValue SHA512_ALGORITHM_VALUE = new AllowableValue(MessageDigestAlgorithms.SHA_512, "SHA-512",
            "SHA-512 cryptographic hashing algorithm.");

    static final AllowableValue HASH_SET_VALUE = new AllowableValue("hash-set", "HashSet",
            "Exactly matches records seen before with 100% accuracy at the expense of more storage usage. " +
                    "Stores the filter data in a single cache entry in the distributed cache, and is loaded entirely into memory during duplicate detection. " +
                    "This filter is preferred for small to medium data sets and offers high performance, being loaded into memory when this processor is running.");
    static final AllowableValue BLOOM_FILTER_VALUE = new AllowableValue("bloom-filter", "BloomFilter",
            "Space-efficient data structure ideal for large data sets using probability to determine if a record was seen previously. " +
                    "False positive matches are possible, but false negatives are not – in other words, a query returns either \"possibly in the set\" or \"definitely not in the set\". " +
                    "You should use this option if the FlowFile content is large and you can tolerate some duplication in the data. Uses constant storage space regardless of the record set size.");

    // PROPERTIES

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final AllowableValue OPTION_SINGLE_FILE = new AllowableValue("single", "Single File");
    static final AllowableValue OPTION_MULTIPLE_FILES = new AllowableValue("multiple", "Multiple Files");

    static final PropertyDescriptor DEDUPLICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("deduplication-strategy")
            .displayName("Deduplication Strategy")
            .description("The strategy to use for detecting and routing duplicate records. The option for detecting " +
                    "duplicates across a single FlowFile operates in-memory, whereas detection spanning multiple FlowFiles " +
                    "utilises a distributed map cache.")
            .allowableValues(OPTION_SINGLE_FILE, OPTION_MULTIPLE_FILES)
            .defaultValue(OPTION_SINGLE_FILE.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor DISTRIBUTED_MAP_CACHE = new PropertyDescriptor.Builder()
            .name("distributed-map-cache")
            .displayName("Distributed Map Cache client")
            .description("This property is required when the deduplication strategy is set to 'multiple files.' The map " +
                    "cache will for each record, atomically check whether the cache key exists and if not, set it.")
            .identifiesControllerService(DistributedMapCacheClient.class)
            .required(false)
            .addValidator(Validator.VALID)
            .dependsOn(DEDUPLICATION_STRATEGY, OPTION_MULTIPLE_FILES)
            .build();

    static final PropertyDescriptor CACHE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("cache-identifier")
            .displayName("Cache Identifier")
            .description("An optional expression language field that overrides the record's computed cache key. " +
                    "This field has an additional attribute available: ${" + RECORD_HASH_VALUE_ATTRIBUTE + "}, " +
                    "which contains the cache key derived from dynamic properties (if set) or record fields.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(Validator.VALID)
            .dependsOn(DEDUPLICATION_STRATEGY, OPTION_MULTIPLE_FILES)
            .build();

    static final PropertyDescriptor PUT_CACHE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("put-cache-identifier")
            .displayName("Cache the Entry Identifier")
            .description("For each record, check whether the cache identifier exists in the distributed map cache. " +
                    "If it doesn't exist and this property is true, put the identifier to the cache.")
            .required(true)
            .allowableValues("true", "false")
            .dependsOn(DISTRIBUTED_MAP_CACHE)
            .defaultValue("false")
            .build();

    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
            .name("include-zero-record-flowfiles")
            .displayName("Include Zero Record FlowFiles")
            .description("If a FlowFile sent to either the duplicate or non-duplicate relationships contains no records, " +
                    "a value of `false` in this property causes the FlowFile to be dropped. Otherwise, the empty FlowFile " +
                    "is emitted.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_HASHING_ALGORITHM = new PropertyDescriptor.Builder()
            .name("record-hashing-algorithm")
            .displayName("Record Hashing Algorithm")
            .description("The algorithm used to hash the cache key.")
            .allowableValues(
                    NONE_ALGORITHM_VALUE,
                    SHA256_ALGORITHM_VALUE,
                    SHA512_ALGORITHM_VALUE
            )
            .defaultValue(SHA256_ALGORITHM_VALUE.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    static final PropertyDescriptor FILTER_TYPE = new PropertyDescriptor.Builder()
            .name("filter-type")
            .displayName("Filter Type")
            .description("The filter used to determine whether a record has been seen before based on the matching RecordPath " +
                    "criteria. If hash set is selected, a Java HashSet object will be used to deduplicate all encountered " +
                    "records. If the bloom filter option is selected, a bloom filter will be used. The bloom filter option is " +
                    "less memory intensive, but has a chance of having false positives.")
            .allowableValues(
                    HASH_SET_VALUE,
                    BLOOM_FILTER_VALUE
            )
            .defaultValue(HASH_SET_VALUE.getValue())
            .dependsOn(DEDUPLICATION_STRATEGY, OPTION_SINGLE_FILE)
            .required(true)
            .build();

    static final PropertyDescriptor FILTER_CAPACITY_HINT = new PropertyDescriptor.Builder()
            .name("filter-capacity-hint")
            .displayName("Filter Capacity Hint")
            .description("An estimation of the total number of unique records to be processed. " +
                    "The more accurate this number is will lead to fewer false negatives on a BloomFilter.")
            .defaultValue("25000")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .dependsOn(FILTER_TYPE, BLOOM_FILTER_VALUE)
            .required(true)
            .build();

    static final PropertyDescriptor BLOOM_FILTER_FPP = new PropertyDescriptor.Builder()
            .name("bloom-filter-certainty")
            .displayName("Bloom Filter Certainty")
            .description("The desired false positive probability when using the BloomFilter type. " +
                    "Using a value of .05 for example, guarantees a five-percent probability that the result is a false positive. " +
                    "The closer to 1 this value is set, the more precise the result at the expense of more storage space utilization.")
            .defaultValue("0.10")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            DEDUPLICATION_STRATEGY,
            DISTRIBUTED_MAP_CACHE,
            CACHE_IDENTIFIER,
            PUT_CACHE_IDENTIFIER,
            RECORD_READER,
            RECORD_WRITER,
            INCLUDE_ZERO_RECORD_FLOWFILES,
            RECORD_HASHING_ALGORITHM,
            FILTER_TYPE,
            FILTER_CAPACITY_HINT,
            BLOOM_FILTER_FPP
    );

    static final Relationship REL_DUPLICATE = new Relationship.Builder()
            .name("duplicate")
            .description("Records detected as duplicates are routed to this relationship.")
            .build();

    static final Relationship REL_NON_DUPLICATE = new Relationship.Builder()
            .name("non-duplicate")
            .description("Records not found in the cache are routed to this relationship.")
            .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original input FlowFile is sent to this relationship unless a fatal error occurs.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If unable to communicate with the cache, the FlowFile will be penalized and routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_DUPLICATE,
            REL_NON_DUPLICATE,
            REL_ORIGINAL,
            REL_FAILURE
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("The property's value is a RecordPath, which is evaluated when testing whether a record is a duplicate or not. " +
                        "Multiple dynamic properties are supported. The key used to de-duplicate a record is determined by concatenating " +
                        "the following for each dynamic property: property name, a fixed delimiter and the evaluated RecordPath for the record. " +
                        "If a hashing algorithm is configured, the key is hashed prior to being used in the state cache. "
                )
                .required(false)
                .dynamic(true)
                .addValidator(new RecordPathValidator())
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        List<ValidationResult> validationResults = new ArrayList<>();

        boolean useSingleFile = context.getProperty(DEDUPLICATION_STRATEGY).getValue().equals(OPTION_SINGLE_FILE.getValue());

        if (useSingleFile && context.getProperty(BLOOM_FILTER_FPP).isSet()) {
            final double falsePositiveProbability = context.getProperty(BLOOM_FILTER_FPP).asDouble();
            if (falsePositiveProbability < 0 || falsePositiveProbability > 1) {
                validationResults.add(
                        new ValidationResult.Builder()
                                .subject(BLOOM_FILTER_FPP.getName() + " out of range.")
                                .input(String.valueOf(falsePositiveProbability))
                                .explanation("Valid values are 0.0 - 1.0 inclusive")
                                .valid(false).build());
            }
        } else if (!useSingleFile) {
            if (!context.getProperty(DISTRIBUTED_MAP_CACHE).isSet()) {
                validationResults.add(new ValidationResult.Builder()
                        .subject(DISTRIBUTED_MAP_CACHE.getName())
                        .explanation("Multiple files deduplication was chosen, but a distributed map cache client was " +
                                "not configured")
                        .valid(false).build());
            }
        }

        return validationResults;
    }

    private DistributedMapCacheClient mapCacheClient;
    private RecordReaderFactory readerFactory;
    private RecordSetWriterFactory writerFactory;

    private boolean useInMemoryStrategy;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        dynamicProperties = context.getProperties().keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toList());

        int cacheSize = dynamicProperties.size();

        recordPathCache = new RecordPathCache(cacheSize);

        if (context.getProperty(DISTRIBUTED_MAP_CACHE).isSet()) {
            mapCacheClient = context.getProperty(DISTRIBUTED_MAP_CACHE).asControllerService(DistributedMapCacheClient.class);
        }

        readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        String strategy = context.getProperty(DEDUPLICATION_STRATEGY).getValue();

        useInMemoryStrategy = strategy.equals(OPTION_SINGLE_FILE.getValue());
    }

    private FilterWrapper getFilter(ProcessContext context) {
        if (useInMemoryStrategy) {
            boolean useHashSet = context.getProperty(FILTER_TYPE).getValue()
                    .equals(HASH_SET_VALUE.getValue());
            final int filterCapacity = context.getProperty(FILTER_CAPACITY_HINT).asInteger();
            return useHashSet
                    ? new HashSetFilterWrapper(new HashSet<>(filterCapacity))
                    : new BloomFilterWrapper(BloomFilter.create(
                    Funnels.stringFunnel(Charset.defaultCharset()),
                    filterCapacity,
                    context.getProperty(BLOOM_FILTER_FPP).asDouble()
            ));
        } else {
            return new DistributedMapCacheClientWrapper(mapCacheClient, context.getProperty(PUT_CACHE_IDENTIFIER).asBoolean());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        FlowFile nonDuplicatesFlowFile = session.create(flowFile);
        FlowFile duplicatesFlowFile = session.create(flowFile);

        long index = 0;

        WriteResult nonDuplicatesWriteResult = null;
        WriteResult duplicatesWriteResult = null;
        String duplicateMimeType = null;
        String nonDuplicateMimeType = null;

        boolean error = false;
        try (
                final InputStream inputStream = session.read(flowFile);
                final RecordReader reader = readerFactory.createRecordReader(flowFile, inputStream, logger);
                final OutputStream nonDupeStream = session.write(nonDuplicatesFlowFile);
                final OutputStream dupeStream = session.write(duplicatesFlowFile);
                final RecordSetWriter nonDuplicatesWriter = writerFactory
                        .createWriter(getLogger(), writerFactory.getSchema(flowFile.getAttributes(), reader.getSchema()), nonDupeStream, nonDuplicatesFlowFile);
                final RecordSetWriter duplicatesWriter = writerFactory
                        .createWriter(getLogger(), writerFactory.getSchema(flowFile.getAttributes(), reader.getSchema()), dupeStream, duplicatesFlowFile);
        ) {
            final FilterWrapper filter = getFilter(context);

            final String recordHashingAlgorithm = context.getProperty(RECORD_HASHING_ALGORITHM).getValue();
            final MessageDigest messageDigest = recordHashingAlgorithm.equals(NONE_ALGORITHM_VALUE.getValue())
                    ? null
                    : DigestUtils.getDigest(recordHashingAlgorithm);
            final boolean matchWholeRecord = context.getProperties().keySet().stream().noneMatch(PropertyDescriptor::isDynamic);

            nonDuplicatesWriter.beginRecordSet();
            duplicatesWriter.beginRecordSet();
            Record record;

            while ((record = reader.nextRecord()) != null) {
                String recordValue;

                if (matchWholeRecord) {
                    recordValue = Joiner.on(JOIN_CHAR).join(record.getValues());
                } else {
                    recordValue = evaluateKeyFromDynamicProperties(context, record, flowFile);
                }

                // Hash the record value if a hashing algorithm is specified
                String recordHash = recordValue;
                if (messageDigest != null) {
                    recordHash = Hex.encodeHexString(messageDigest.digest(getBytesUtf8(recordValue)));
                    messageDigest.reset();
                }

                // If a cache entry identifier is specified, apply any expressions to determine the final cache value
                if (!useInMemoryStrategy && context.getProperty(CACHE_IDENTIFIER).isSet()) {
                    Map<String, String> additional = new HashMap<>();
                    additional.put(RECORD_HASH_VALUE_ATTRIBUTE, recordHash);
                    recordHash = context.getProperty(CACHE_IDENTIFIER).evaluateAttributeExpressions(flowFile, additional).getValue();
                }

                if (filter.contains(recordHash)) {
                    duplicatesWriter.write(record);
                } else {
                    nonDuplicatesWriter.write(record);
                    filter.put(recordHash);
                }

                index++;
            }

            duplicateMimeType = duplicatesWriter.getMimeType();
            nonDuplicateMimeType = nonDuplicatesWriter.getMimeType();
            // Route Non-Duplicates FlowFile
            nonDuplicatesWriteResult = nonDuplicatesWriter.finishRecordSet();
            // Route Duplicates FlowFile
            duplicatesWriteResult = duplicatesWriter.finishRecordSet();

        } catch (final Exception e) {
            logger.error("Failed in detecting duplicate records at index {}", index, e);
            error = true;
        } finally {
            if (!error) {
                final boolean includeZeroRecordFlowFiles = context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean();

                session.adjustCounter("Records Processed",
                        nonDuplicatesWriteResult.getRecordCount() + duplicatesWriteResult.getRecordCount(), false);

                sendOrRemove(session, duplicatesFlowFile, REL_DUPLICATE, duplicateMimeType,
                        includeZeroRecordFlowFiles, duplicatesWriteResult);

                sendOrRemove(session, nonDuplicatesFlowFile, REL_NON_DUPLICATE, nonDuplicateMimeType,
                        includeZeroRecordFlowFiles, nonDuplicatesWriteResult);

                session.transfer(flowFile, REL_ORIGINAL);
            } else {
                session.remove(duplicatesFlowFile);
                session.remove(nonDuplicatesFlowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    private void sendOrRemove(ProcessSession session,
                              FlowFile outputFlowFile,
                              Relationship targetRelationship,
                              String mimeType,
                              boolean includeZeroRecordFlowFiles,
                              WriteResult writeResult) {
        if (!includeZeroRecordFlowFiles && writeResult.getRecordCount() == 0) {
            session.remove(outputFlowFile);
        } else {
            Map<String, String> attributes = new HashMap<>(writeResult.getAttributes());
            attributes.put(RECORD_COUNT_ATTRIBUTE, String.valueOf(writeResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), mimeType);
            outputFlowFile = session.putAllAttributes(outputFlowFile, attributes);
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Successfully found {} unique records for {}",
                        writeResult.getRecordCount(), outputFlowFile);
            }

            session.transfer(outputFlowFile, targetRelationship);
        }
    }

    private String evaluateKeyFromDynamicProperties(ProcessContext context, Record record, FlowFile flowFile) {
        final List<String> fieldValues = new ArrayList<>();
        for (final PropertyDescriptor propertyDescriptor : dynamicProperties) {
            final String value = context.getProperty(propertyDescriptor).evaluateAttributeExpressions(flowFile).getValue();
            final RecordPath recordPath = recordPathCache.getCompiled(value);
            final RecordPathResult result = recordPath.evaluate(record);
            final List<FieldValue> selectedFields = result.getSelectedFields().toList();

            // Add the name of the dynamic property
            fieldValues.add(propertyDescriptor.getName());

            // Add any non-null record field values
            fieldValues.addAll(selectedFields.stream()
                    .filter(f -> f.getValue() != null)
                    .map(f -> f.getValue().toString())
                    .toList()
            );
        }

        return Joiner.on(JOIN_CHAR).join(fieldValues);
    }

    private abstract static class FilterWrapper {
        public abstract boolean contains(String value);

        public abstract void put(String value);
    }

    private static class HashSetFilterWrapper extends FilterWrapper {

        private final HashSet<String> filter;

        public HashSetFilterWrapper(HashSet<String> filter) {
            this.filter = filter;
        }

        @Override
        public boolean contains(String value) {
            return filter.contains(value);
        }

        @Override
        public void put(String value) {
            filter.add(value);
        }
    }

    private static class BloomFilterWrapper extends FilterWrapper {

        private final BloomFilter<String> filter;

        public BloomFilterWrapper(BloomFilter<String> filter) {
            this.filter = filter;
        }

        @Override
        public boolean contains(String value) {
            return filter.mightContain(value);
        }

        @Override
        public void put(String value) {
            filter.put(value);
        }
    }

    private static class DistributedMapCacheClientWrapper extends FilterWrapper {
        private final DistributedMapCacheClient client;
        private final boolean putCacheIdentifier;

        public DistributedMapCacheClientWrapper(final DistributedMapCacheClient client,
                                                final boolean putCacheIdentifier) {
            this.client = client;
            this.putCacheIdentifier = putCacheIdentifier;
        }

        @Override
        public boolean contains(String value) {
            try {
                if (putCacheIdentifier) {
                    return !client.putIfAbsent(value, "", STRING_SERIALIZER, STRING_SERIALIZER);
                } else {
                    return client.containsKey(value, STRING_SERIALIZER);
                }
            } catch (IOException e) {
                throw new ProcessException("Distributed Map lookup failed", e);
            }
        }

        @Override
        public void put(String value) {
            // Do nothing as the key is queried and put to the cache atomically in the `contains` method.
        }
    }

    private static final Serializer<String> STRING_SERIALIZER = (value, output) -> output.write(value.getBytes(StandardCharsets.UTF_8));
}
