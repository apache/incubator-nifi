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
package org.apache.nifi.processors.hubspot;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@PrimaryNodeOnly
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"hubspot"})
@CapabilityDescription("Retrieves JSON data from a private HubSpot application."
        + " Supports incremental retrieval: Users can set the \"limit\" property which serves as the upper limit of the retrieved objects."
        + " When this property is set the processor will retrieve new records. This processor is intended to be run on the Primary Node only.")
@Stateful(scopes = Scope.CLUSTER, description = "When the 'Limit' attribute is set, the paging cursor is saved after executing a request."
        + " Only the objects after the paging cursor will be retrieved. The maximum number of retrieved objects is the 'Limit' attribute."
        + " State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary Node is selected,"
        + " the new node can pick up where the previous node left off, without duplicating the data.")
@DefaultSettings(yieldDuration = "10 sec")
public class GetHubSpot extends AbstractProcessor {

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("admin-api-access-token")
            .displayName("Access Token")
            .description("Access Token to authenticate requests")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor CRM_ENDPOINT = new PropertyDescriptor.Builder()
            .name("crm-endpoint")
            .displayName("HubSpot CRM API Endpoint")
            .description("The HubSpot CRM API endpoint to get")
            .required(true)
            .allowableValues(CrmEndpoint.class)
            .build();

    static final PropertyDescriptor LIMIT = new PropertyDescriptor.Builder()
            .name("crm-limit")
            .displayName("Limit")
            .description("The maximum number of results to display per page")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor WEB_CLIENT_SERVICE_PROVIDER = new PropertyDescriptor.Builder()
            .name("web-client-service-provider")
            .displayName("Web Client Service Provider")
            .description("Controller service for HTTP client operations")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful HTTP request.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("In case of HTTP client errors the flowfile will be routed to this relationship")
            .build();

    private static final String API_BASE_URI = "api.hubapi.com";
    private static final String HTTPS = "https";
    private static final String CURSOR_PARAMETER = "after";
    private static final String LIMIT_PARAMETER = "limit";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();

    private volatile WebClientServiceProvider webClientServiceProvider;

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            ACCESS_TOKEN,
            CRM_ENDPOINT,
            LIMIT,
            WEB_CLIENT_SERVICE_PROVIDER
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
    }

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
        final String accessToken = context.getProperty(ACCESS_TOKEN).getValue();
        final String endpoint = context.getProperty(CRM_ENDPOINT).getValue();

        final StateMap state = getStateMap(context);
        final URI uri = createUri(context, state);

        final HttpResponseEntity response = getHttpResponseEntity(accessToken, uri);
        final AtomicInteger objectCountHolder = new AtomicInteger();

        FlowFile flowFile = session.create();
        flowFile = session.putAttribute(flowFile, "statusCode", String.valueOf(response.statusCode()));

        flowFile = session.write(flowFile, out -> {

            try (JsonParser jsonParser = JSON_FACTORY.createParser(response.body());
                 final JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8)) {
                while (jsonParser.nextToken() != null) {
                    if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME && jsonParser.getCurrentName().equals("results")) {
                        jsonParser.nextToken();
                        jsonGenerator.copyCurrentStructure(jsonParser);
                        objectCountHolder.incrementAndGet();
                    }
                    String fieldName = jsonParser.getCurrentName();
                    if (CURSOR_PARAMETER.equals(fieldName)) {
                        jsonParser.nextToken();
                        Map<String, String> newStateMap = new HashMap<>(state.toMap());
                        newStateMap.put(endpoint, jsonParser.getText());
                        updateState(context, newStateMap);
                        break;
                    }
                }
            }
        });
        if (response.statusCode() >= 400) {
            if (response.statusCode() == 429) {
                context.yield();
                throw new ProcessException("Rate limit exceeded, yielding for 10 seconds before retrying request.");
            } else {
                getLogger().warn("HTTP [{}] client error occurred at endpoint [{}]", response.statusCode(), endpoint);
                session.transfer(flowFile, REL_FAILURE);
            }
        } else if (objectCountHolder.get() > 0) {
            session.transfer(flowFile, REL_SUCCESS);
        } else {
            getLogger().debug("Empty response when requested HubSpot endpoint: [{}]", endpoint);
        }
    }

    HttpResponseEntity getHttpResponseEntity(final String accessToken, final URI uri) {
        return webClientServiceProvider.getWebClientService()
                .get()
                .uri(uri)
                .header("Authorization", "Bearer " + accessToken)
                .retrieve();
    }

    HttpUriBuilder getBaseUri(final ProcessContext context) {
        final String path = context.getProperty(CRM_ENDPOINT).getValue();
        return webClientServiceProvider.getHttpUriBuilder()
                .scheme(HTTPS)
                .host(API_BASE_URI)
                .encodedPath(path);
    }

    URI createUri(final ProcessContext context, final StateMap state) {
        final String path = context.getProperty(CRM_ENDPOINT).getValue();
        final HttpUriBuilder uriBuilder = getBaseUri(context);

        final boolean isLimitSet = context.getProperty(LIMIT).isSet();
        if (isLimitSet) {
            final String limit = context.getProperty(LIMIT).getValue();
            uriBuilder.addQueryParameter(LIMIT_PARAMETER, limit);
        }

        final String cursor = state.get(path);
        if (cursor != null) {
            uriBuilder.addQueryParameter(CURSOR_PARAMETER, cursor);
        }
        return uriBuilder.build();
    }

    private StateMap getStateMap(final ProcessContext context) {
        final StateMap stateMap;
        try {
            stateMap = context.getStateManager().getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("State retrieval failed", e);
        }
        return stateMap;
    }

    private void updateState(ProcessContext context, Map<String, String> newState) {
        try {
            context.getStateManager().setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Page cursor update failed", e);
        }
    }
}
