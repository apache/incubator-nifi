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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.expression.AttributeExpression;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SupportsBatching
@Tags({"map", "cache", "fetch", "distributed"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Computes cache key(s) from FlowFile attributes, for each incoming FlowFile, and fetches the value(s) from the Distributed Map Cache associated "
        + "with each key. If configured without a destination attribute, the incoming FlowFile's content is replaced with the binary data received by the Distributed Map Cache. "
        + "If there is no value stored under that key then the flow file will be routed to 'not-found'. Note that the processor will always attempt to read the entire cached value into "
        + "memory before placing it in it's destination. This could be potentially problematic if the cached value is very large.")
@WritesAttribute(attribute = "user-defined", description = "If the 'Put Cache Value In Attribute' property is set then whatever it is set to "
        + "will become the attribute key and the value would be whatever the response was from the Distributed Map Cache. If multiple cache entry identifiers are selected, "
        + "multiple attributes will be written, using the evaluated value of this property, appended by a period (.) and the name of the cache entry identifier. For example, if "
        + "the Cache Entry Identifier property is set to 'id,name', and the user-defined property is named 'fetched', then two attributes will be written, "
        + "fetched.id and fetched.name, containing their respective values.")
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService", "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "org.apache.nifi.processors.standard.PutDistributedMapCache"})
public class FetchDistributedMapCache extends AbstractProcessor {

    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("Distributed Cache Service")
            .description("The Controller Service that is used to get the cached values.")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Cache Entry Identifier")
            .description("A comma-delimited list of FlowFile attributes, or the results of Attribute Expression Language statements, which will be evaluated "
                    + "against a FlowFile in order to determine the value(s) used to identify duplicates; it is these values that are cached. NOTE: Only a single "
                    + "Cache Entry Identifier is allowed unless Put Cache Value In Attribute is specified. Multiple cache lookups are only supported when the destination "
                    + "is a set of attributes (see the documentation for 'Put Cache Value In Attribute' for more details including naming convention.")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .defaultValue("${hash.value}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PUT_CACHE_VALUE_IN_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Put Cache Value In Attribute")
            .description("If set, the cache value received will be put into an attribute of the FlowFile instead of a the content of the"
                    + "FlowFile. The attribute key to put to is determined by evaluating value of this property. If multiple Cache Entry Identifiers are selected, "
                    + "multiple attributes will be written, using the evaluated value of this property, appended by a period (.) and the name of the cache entry identifier.")
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PUT_ATTRIBUTE_MAX_LENGTH = new PropertyDescriptor.Builder()
            .name("Max Length To Put In Attribute")
            .description("If routing the cache value to an attribute of the FlowFile (by setting the \"Put Cache Value in attribute\" "
                    + "property), the number of characters put to the attribute value will be at most this amount. This is important because "
                    + "attributes are held in memory and large attributes will quickly cause out of memory issues. If the output goes "
                    + "longer than this value, it will be truncated to fit. Consider making this smaller if able.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("256")
            .build();

    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the cached value is encoded. This will only be used when routing to an attribute.")
            .required(false)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CACHE_ENTRY_IDENTIFIER,
            DISTRIBUTED_CACHE_SERVICE,
            PUT_CACHE_VALUE_IN_ATTRIBUTE,
            PUT_ATTRIBUTE_MAX_LENGTH,
            CHARACTER_SET
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("If the cache was successfully communicated with it will be routed to this relationship")
            .build();
    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not-found")
            .description("If a FlowFile's Cache Entry Identifier was not found in the cache, it will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If unable to communicate with the cache or if the cache entry is evaluated to be blank, the FlowFile will be penalized and routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_NOT_FOUND,
            REL_FAILURE
    );

    private final Serializer<String> keySerializer = new StringSerializer();
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
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        PropertyValue cacheEntryIdentifier = validationContext.getProperty(CACHE_ENTRY_IDENTIFIER);
        boolean elPresent = false;
        try {
            elPresent = cacheEntryIdentifier.isExpressionLanguagePresent();
        } catch (NullPointerException npe) {
            // Unfortunate workaround to a mock framework bug (NIFI-4590)
        }

        if (elPresent) {
            // This doesn't do a full job of validating against the requirement that Put Cache Value In Attribute must be set if multiple
            // Cache Entry Identifiers are supplied (if Expression Language is used). The user could conceivably have a comma-separated list of EL statements,
            // or a single EL statement with commas inside it but that evaluates to a single item.
            results.add(new ValidationResult.Builder().valid(true).explanation("Contains Expression Language").build());
        } else {
            if (!validationContext.getProperty(FetchDistributedMapCache.PUT_CACHE_VALUE_IN_ATTRIBUTE).isSet()) {
                String identifierString = cacheEntryIdentifier.getValue();
                if (identifierString.contains(",")) {
                    results.add(new ValidationResult.Builder().valid(false)
                            .explanation("Multiple Cache Entry Identifiers specified without Put Cache Value In Attribute set").build());
                }
            }
        }
        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final String cacheKey = context.getProperty(CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();
        // This block retains the previous behavior when only one Cache Entry Identifier was allowed, so as not to change the expected error message
        if (StringUtils.isBlank(cacheKey)) {
            logger.error("FlowFile {} has no attribute for given Cache Entry Identifier", flowFile);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        List<String> cacheKeys = Arrays.stream(cacheKey.split(",")).filter(path -> !StringUtils.isEmpty(path)).map(String::trim).toList();
        for (int i = 0; i < cacheKeys.size(); i++) {
            if (StringUtils.isBlank(cacheKeys.get(i))) {
                // Log first missing identifier, route to failure, and return
                logger.error("FlowFile {} has no attribute for Cache Entry Identifier in position {}", flowFile, i);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        final DistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        try {
            final Map<String, byte[]> cacheValues;
            final boolean singleKey = cacheKeys.size() == 1;
            if (singleKey) {
                cacheValues = new HashMap<>(1);
                cacheValues.put(cacheKeys.getFirst(), cache.get(cacheKey, keySerializer, valueDeserializer));
            } else {
                cacheValues = cache.subMap(new HashSet<>(cacheKeys), keySerializer, valueDeserializer);
            }
            boolean notFound = false;
            for (Map.Entry<String, byte[]> cacheValueEntry : cacheValues.entrySet()) {
                final byte[] cacheValue = cacheValueEntry.getValue();

                if (cacheValue == null) {
                    logger.info("Could not find an entry in cache for {}; routing to not-found", flowFile);
                    notFound = true;
                    break;
                } else {
                    boolean putInAttribute = context.getProperty(PUT_CACHE_VALUE_IN_ATTRIBUTE).isSet();
                    if (putInAttribute) {
                        String attributeName = context.getProperty(PUT_CACHE_VALUE_IN_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
                        if (!singleKey) {
                            // Append key to attribute name if multiple keys
                            attributeName += "." + cacheValueEntry.getKey();
                        }
                        String attributeValue = new String(cacheValue, context.getProperty(CHARACTER_SET).getValue());

                        int maxLength = context.getProperty(PUT_ATTRIBUTE_MAX_LENGTH).asInteger();
                        if (maxLength < attributeValue.length()) {
                            attributeValue = attributeValue.substring(0, maxLength);
                        }

                        flowFile = session.putAttribute(flowFile, attributeName, attributeValue);

                    } else if (cacheKeys.size() > 1) {
                        throw new IOException("Multiple Cache Value Identifiers specified without Put Cache Value In Attribute set");
                    } else {
                        // Write single value to content
                        flowFile = session.write(flowFile, out -> out.write(cacheValue));
                    }

                    if (putInAttribute) {
                        logger.info("Found a cache key of {} and added an attribute to {} with it's value.", cacheKey, flowFile);
                    } else {
                        logger.info("Found a cache key of {} and replaced the contents of {} with it's value.", cacheKey, flowFile);
                    }
                }
            }
            // If the loop was exited because a cache entry was not found, route to REL_NOT_FOUND; otherwise route to REL_SUCCESS
            if (notFound) {
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (final IOException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with cache when processing {}", flowFile, e);
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

    public static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

}
