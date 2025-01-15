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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.hash.HashAlgorithm;
import org.apache.nifi.processors.standard.hash.HashService;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@SupportsBatching
@Tags({"content", "hash", "sha", "blake2", "md5", "cryptography"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Calculates a cryptographic hash value for the flowfile content using the given algorithm and writes it to an output attribute. Please refer to https://csrc.nist.gov/Projects/Hash-Functions/NIST-Policy-on-Hash-Functions for help to decide which algorithm to use.")
@WritesAttribute(attribute = "content_<algorithm>", description = "This processor adds an attribute whose value is the result of "
        + "hashing the flowfile content. The name of this attribute is specified by the value of the algorithm, e.g. 'content_SHA-256'.")
public class CryptographicHashContent extends AbstractProcessor {

    static final PropertyDescriptor FAIL_WHEN_EMPTY = new PropertyDescriptor.Builder()
            .name("fail_when_empty")
            .displayName("Fail if the content is empty")
            .description("Route to failure if the content is empty. " +
                    "While hashing an empty value is valid, some flows may want to detect empty input.")
            .allowableValues("true", "false")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    static final PropertyDescriptor HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("hash_algorithm")
            .displayName("Hash Algorithm")
            .description("The hash algorithm to use. Note that not all of the algorithms available are recommended for use (some are provided for legacy compatibility). " +
                    "There are many things to consider when picking an algorithm; it is recommended to use the most secure algorithm possible.")
            .required(true)
            .allowableValues(HashService.buildHashAlgorithmAllowableValues())
            .defaultValue(HashAlgorithm.SHA256.getName())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FAIL_WHEN_EMPTY,
            HASH_ALGORITHM
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Used for flowfiles that have a hash value added")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Used for flowfiles that have no content if the 'fail on empty' setting is enabled")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_FAILURE,
            REL_SUCCESS
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final ComponentLog logger = getLogger();

        // Determine the algorithm to use
        final String algorithmName = context.getProperty(HASH_ALGORITHM).getValue();
        logger.debug("Using algorithm {}", algorithmName);
        HashAlgorithm algorithm = HashAlgorithm.fromName(algorithmName);

        if (flowFile.getSize() == 0) {
            if (context.getProperty(FAIL_WHEN_EMPTY).asBoolean()) {
                logger.info("Routing {} to 'failure' because content is empty (and FAIL_WHEN_EMPTY is true)");
                session.transfer(flowFile, REL_FAILURE);
                return;
            } else {
                logger.debug("Flowfile content is empty; hashing with {} anyway", algorithmName);
            }
        }

        // Generate a hash with the configured algorithm for the content
        // and create a new attribute with the configured name
        logger.debug("Generating {} hash of content", algorithmName);
        final AtomicReference<String> hashValueHolder = new AtomicReference<>(null);

        try {
            // Read the flowfile content via a lambda InputStreamCallback and hash the content
            session.read(flowFile, in -> hashValueHolder.set(HashService.hashValueStreaming(algorithm, in)));

            // Determine the destination attribute name
            final String attributeName = "content_" + algorithmName;
            logger.debug("Writing {} hash to attribute '{}'", algorithmName, attributeName);

            // Write the attribute
            flowFile = session.putAttribute(flowFile, attributeName, hashValueHolder.get());
            logger.info("Successfully added attribute '{}' to {} with a value of {}; routing to success", attributeName, flowFile, hashValueHolder.get());

            // Update provenance and route to success
            session.getProvenanceReporter().modifyAttributes(flowFile);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (ProcessException e) {
            logger.error("Routing to failure since failed to process {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
