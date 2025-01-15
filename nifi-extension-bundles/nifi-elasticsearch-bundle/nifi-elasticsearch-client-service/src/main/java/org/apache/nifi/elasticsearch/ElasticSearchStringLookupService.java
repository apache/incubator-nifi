/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

package org.apache.nifi.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@CapabilityDescription("Lookup a string value from Elasticsearch Server associated with the specified document ID. " +
        "The coordinates that are passed to the lookup must contain the key 'id'.")
@Tags({"lookup", "enrich", "value", "key", "elasticsearch"})
public class ElasticSearchStringLookupService extends AbstractControllerService implements StringLookupService {
    public static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("el-rest-client-service")
            .displayName("Client Service")
            .description("An ElasticSearch client service to use for running queries.")
            .identifiesControllerService(ElasticSearchClientService.class)
            .required(true)
            .build();
    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("el-lookup-index")
            .displayName("Index")
            .description("The name of the index to read from")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("el-lookup-type")
            .displayName("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CLIENT_SERVICE,
            INDEX,
            TYPE
    );

    private static final ObjectMapper mapper = new ObjectMapper();
    public static final String ID = "es_document_id";
    private ElasticSearchClientService esClient;
    private String index;
    private String type;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        esClient = context.getProperty(CLIENT_SERVICE).asControllerService(ElasticSearchClientService.class);
        index = context.getProperty(INDEX).evaluateAttributeExpressions().getValue();
        type = context.getProperty(TYPE).evaluateAttributeExpressions().getValue();
    }

    @Override
    public Optional<String> lookup(final Map<String, Object> coordinates) throws LookupFailureException {
        try {
            final String id = (String) coordinates.get(ID);
            final Map<String, Object> enums = esClient.get(index, type, id, null);
            if (enums == null) {
                return Optional.empty();
            } else {
                return Optional.ofNullable(mapper.writeValueAsString(enums));
            }
        } catch (final IOException | ElasticsearchException e) {
            throw new LookupFailureException(e);
        }
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.singleton(ID);
    }
}
