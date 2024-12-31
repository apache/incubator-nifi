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

package org.apache.nifi.processors.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticsearchException;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"json", "elasticsearch", "elasticsearch5", "elasticsearch6", "elasticsearch7", "elasticsearch8", "put", "index", "record"})
@CapabilityDescription("Elasticsearch get processor that uses the official Elastic REST client libraries " +
        "to fetch a single document from Elasticsearch by _id. " +
        "Note that the full body of the document will be read into memory before being written to a FlowFile for transfer.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename attribute is set to the document identifier"),
        @WritesAttribute(attribute = "elasticsearch.index", description = "The Elasticsearch index containing the document"),
        @WritesAttribute(attribute = "elasticsearch.type", description = "The Elasticsearch document type"),
        @WritesAttribute(attribute = "elasticsearch.get.error", description = "The error message provided by Elasticsearch if there is an error fetching the document.")
})
@SeeAlso(JsonQueryElasticsearch.class)
@DynamicProperties({
        @DynamicProperty(
                name = "The name of the HTTP request header",
                value = "A Record Path expression to retrieve the HTTP request header value",
                expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description = "Prefix: " + ElasticsearchRestProcessor.DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER +
                        " - adds the specified property name/value as a HTTP request header in the Elasticsearch request. " +
                        "If the Record Path expression results in a null or blank value, the HTTP request header will be omitted."),
        @DynamicProperty(
                name = "The name of a URL query parameter to add",
                value = "The value of the URL query parameter",
                expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing.")
})
public class GetElasticsearch extends AbstractProcessor implements ElasticsearchRestProcessor {
    static final AllowableValue FLOWFILE_CONTENT = new AllowableValue(
            "flowfile-content",
            "FlowFile Content",
            "Output the retrieved document as the FlowFile content."
    );

    static final AllowableValue FLOWFILE_ATTRIBUTE = new AllowableValue(
            "flowfile-attribute",
            "FlowFile Attribute",
            "Output the retrieved document as a FlowFile attribute specified by the Attribute Name."
    );

    public static final PropertyDescriptor ID = new PropertyDescriptor.Builder()
            .name("get-es-id")
            .displayName("Document Id")
            .description("The _id of the document to retrieve.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("get-es-destination")
            .displayName("Destination")
            .description("Indicates whether the retrieved document is written to the FlowFile content or a FlowFile attribute.")
            .required(true)
            .allowableValues(FLOWFILE_CONTENT, FLOWFILE_ATTRIBUTE)
            .defaultValue(FLOWFILE_CONTENT.getValue())
            .build();

    static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("get-es-attribute-name")
            .displayName("Attribute Name")
            .description("The name of the FlowFile attribute to use for the retrieved document output.")
            .required(true)
            .defaultValue("elasticsearch.doc")
            .dependsOn(DESTINATION, FLOWFILE_ATTRIBUTE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final Relationship REL_DOC = new Relationship.Builder().name("document")
            .description("Fetched documents are routed to this relationship.")
            .build();

    static final Relationship REL_NOT_FOUND = new Relationship.Builder().name("not_found")
            .description("A FlowFile is routed to this relationship if the specified document does not exist in the Elasticsearch cluster.")
            .build();

    public static final String VERIFICATION_STEP_DOCUMENT_EXISTS = "Elasticsearch Document Exists";

    static final List<PropertyDescriptor> DESCRIPTORS =
            List.of(ID, INDEX, TYPE, DESTINATION, ATTRIBUTE_NAME, CLIENT_SERVICE);
    static final Set<Relationship> RELATIONSHIPS = Set.of(REL_DOC, REL_FAILURE, REL_RETRY, REL_NOT_FOUND);

    private ObjectMapper mapper;

    private final AtomicReference<ElasticSearchClientService> clientService = new AtomicReference<>(null);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    public boolean isIndexNotExistSuccessful() {
        return false;
    }

    @Override
    public ObjectMapper buildObjectMapper(final ProcessContext context) {
        // GetElasticsearch only serialises results using the ObjectMapper, no need to override the Reader's Max String Length
        return new ObjectMapper();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientService.set(context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class));

        mapper = buildObjectMapper(context);
    }

    @OnStopped
    public void onStopped() {
        clientService.set(null);
    }

    @Override
    public List<ConfigVerificationResult> verifyAfterIndex(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes,
                                                           final ElasticSearchClientService verifyClientService, final String index, final boolean indexExists, final ObjectMapper mapper) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        final ConfigVerificationResult.Builder documentExistsResult = new ConfigVerificationResult.Builder()
                .verificationStepName(VERIFICATION_STEP_DOCUMENT_EXISTS);

        if (indexExists && context.getProperty(ID).isSet()) {
            final String type = context.getProperty(TYPE).evaluateAttributeExpressions(attributes).getValue();
            final String id = context.getProperty(ID).evaluateAttributeExpressions(attributes).getValue();
            try {
                final Map<String, String> requestParameters = new HashMap<>(getRequestParametersFromDynamicProperties(context, attributes));
                requestParameters.putIfAbsent("_source", "false");
                if (verifyClientService.documentExists(index, type, id, requestParameters, getRequestHeadersFromDynamicProperties(context, attributes))) {
                    documentExistsResult.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                            .explanation(String.format("Document [%s] exists in index [%s]", id, index));
                } else {
                    documentExistsResult.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                            .explanation(String.format("Document [%s] does not exist in index [%s]", id, index));
                }
            } catch (final Exception ex) {
                handleDocumentExistsCheckException(ex, documentExistsResult, verificationLogger, index, id);
            }
        } else {
            String skippedReason;
            if (indexExists) {
                skippedReason = String.format("No %s specified for document existence check", ID.getDisplayName());
            } else {
                skippedReason = String.format("Index %s does not exist for document existence check", index);
            }
            documentExistsResult.outcome(ConfigVerificationResult.Outcome.SKIPPED).explanation(skippedReason);
        }
        results.add(documentExistsResult.build());

        return results;
    }

    private void handleDocumentExistsCheckException(final Exception ex, final ConfigVerificationResult.Builder documentExistsResult,
                                                    final ComponentLog verificationLogger, final String index, final String id) {
        verificationLogger.error("Error checking whether document [{}] exists in index [{}]", id, index, ex);
        documentExistsResult.outcome(ConfigVerificationResult.Outcome.FAILED)
                .explanation(String.format("Failed to check whether document [%s] exists in index [%s]", id, index));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile input = session.get();

        final String id = context.getProperty(ID).evaluateAttributeExpressions(input).getValue();
        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(input).getValue();
        final String type  = context.getProperty(TYPE).evaluateAttributeExpressions(input).getValue();

        final String destination = context.getProperty(DESTINATION).getValue();
        final String attributeName = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions(input).getValue();

        try {
            if (StringUtils.isBlank(id)) {
                throw new ProcessException(ID.getDisplayName() + " is blank (after evaluating attribute expressions), cannot GET document");
            }

            final StopWatch stopWatch = new StopWatch(true);
            final Map<String, Object> doc = clientService.get().get(index, type, id,
                    getRequestParametersFromDynamicProperties(context, input), getRequestHeadersFromDynamicProperties(context, input));

            final Map<String, String> attributes = new HashMap<>(4, 1);
            attributes.put("filename", id);
            attributes.put("elasticsearch.index", index);
            if (type != null) {
                attributes.put("elasticsearch.type", type);
            }
            final String json = mapper.writeValueAsString(doc);
            FlowFile documentFlowFile = input != null ? input : session.create();
            if (FLOWFILE_CONTENT.getValue().equals(destination)) {
                documentFlowFile = session.write(documentFlowFile, out -> out.write(json.getBytes()));
            } else {
                attributes.put(attributeName, json);
            }

            documentFlowFile = session.putAllAttributes(documentFlowFile, attributes);
            session.getProvenanceReporter().receive(documentFlowFile, clientService.get().getTransitUrl(index, type), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(documentFlowFile, REL_DOC);
        } catch (final ElasticsearchException ese) {
            handleElasticsearchException(ese, input, session, index, type, id);
        } catch (final Exception ex) {
            getLogger().error("Could not fetch document.", ex);
            if (input != null) {
                input = session.putAttribute(input, "elasticsearch.get.error", ex.getMessage());
                session.transfer(input, REL_FAILURE);
            }
            context.yield();
        }
    }

    private void handleElasticsearchException(final ElasticsearchException ese, FlowFile input, final ProcessSession session,
                                               final String index, final String type, final String id) {
        if (ese.isNotFound()) {
            if (input != null) {
                session.transfer(input, REL_NOT_FOUND);
            } else {
                getLogger().warn("Document with _id {} not found in index {} (and type {})", id, index, type);
            }
        } else {
            final String msg = String.format("Encountered a server-side problem with Elasticsearch. %s",
                    ese.isElastic() ? "Routing to retry." : "Routing to failure");
            getLogger().error(msg, ese);
            if (input != null) {
                session.penalize(input);
                input = session.putAttribute(input, "elasticsearch.get.error", ese.getMessage());
                session.transfer(input, ese.isElastic() ? REL_RETRY : REL_FAILURE);
            }
        }
    }
}
