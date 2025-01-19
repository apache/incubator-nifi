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
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticsearchException;
import org.apache.nifi.elasticsearch.OperationResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractByQueryElasticsearch extends AbstractProcessor implements ElasticsearchRestProcessor {
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("If the \"by query\" operation fails, and a flowfile was read, it will be sent to this relationship.")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("If the \"by query\" operation succeeds, and a flowfile was read, it will be sent to this relationship.")
            .build();

    private static final Set<Relationship> relationships = Set.of(REL_SUCCESS, REL_FAILURE, REL_RETRY);
    static final List<PropertyDescriptor> byQueryPropertyDescriptors = List.of(
            QUERY_DEFINITION_STYLE,
            QUERY,
            QUERY_CLAUSE,
            SCRIPT,
            QUERY_ATTRIBUTE,
            INDEX,
            TYPE,
            MAX_JSON_FIELD_STRING_LENGTH,
            CLIENT_SERVICE
    );

    ObjectMapper mapper;

    private final AtomicReference<ElasticSearchClientService> clientService = new AtomicReference<>(null);

    abstract String getTookAttribute();

    abstract String getErrorAttribute();

    abstract OperationResponse performOperation(final ElasticSearchClientService clientService, final String query,
                                                final String index, final String type,
                                                final Map<String, String> requestParameters, final Map<String, String> requestHeaders);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return byQueryPropertyDescriptors;
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile input = null;
        if (context.hasIncomingConnection()) {
            input = session.get();

            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        try {
            final String query = getQuery(input, context, session, mapper);
            final String index = context.getProperty(INDEX).evaluateAttributeExpressions(input).getValue();
            final String type = context.getProperty(TYPE).isSet()
                    ? context.getProperty(TYPE).evaluateAttributeExpressions(input).getValue()
                    : null;
            final String queryAttr = context.getProperty(QUERY_ATTRIBUTE).isSet()
                    ? context.getProperty(QUERY_ATTRIBUTE).evaluateAttributeExpressions(input).getValue()
                    : null;

            final OperationResponse or = performOperation(clientService.get(), query, index, type,
                    getRequestParametersFromDynamicProperties(context, input), getRequestHeadersFromDynamicProperties(context, input));

            if (input == null) {
                input = session.create();
            }

            final Map<String, String> attrs = new HashMap<>();
            attrs.put(getTookAttribute(), String.valueOf(or.getTook()));
            if (!StringUtils.isBlank(queryAttr)) {
                attrs.put(queryAttr, query);
            }

            input = session.putAllAttributes(input, attrs);

            session.transfer(input, REL_SUCCESS);
        } catch (final ElasticsearchException ese) {
            final String msg = String.format("Encountered a server-side problem with Elasticsearch. %s",
                    ese.isElastic() ? "Routing to retry." : "Routing to failure");
            getLogger().error(msg, ese);
            if (input != null) {
                session.penalize(input);
                input = session.putAttribute(input, getErrorAttribute(), ese.getMessage());
                session.transfer(input, ese.isElastic() ? REL_RETRY : REL_FAILURE);
            }
        } catch (final Exception e) {
            getLogger().error("Error running \"by query\" operation: ", e);
            if (input != null) {
                input = session.putAttribute(input, getErrorAttribute(), e.getMessage());
                session.transfer(input, REL_FAILURE);
            }
            context.yield();
        }
    }
}
