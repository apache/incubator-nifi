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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"hubspot"})
@CapabilityDescription("Retrieves JSON data from a private HubSpot application."
        + " This processor is intended to be run on the Primary Node only.")
@Stateful(scopes = Scope.CLUSTER, description = "In case of incremental loading, the start and end timestamps of the last" +
        " query time window are stored in the state. When the 'Result Limit' property is set, the paging cursor is saved after" +
        " executing a request. Only the objects after the paging cursor will be retrieved. The maximum number of retrieved" +
        " objects can be set in the 'Result Limit' property.")
@DefaultSettings(yieldDuration = "10 sec")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
@WritesAttribute(attribute = "mime.type", description = "Sets the MIME type to application/json")
public class GetHubSpot extends AbstractProcessor {

    static final PropertyDescriptor OBJECT_TYPE = new PropertyDescriptor.Builder()
            .name("object-type")
            .displayName("Object Type")
            .description("The HubSpot Object Type requested")
            .required(true)
            .allowableValues(HubSpotObjectType.class)
            .build();

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("access-token")
            .displayName("Access Token")
            .description("Access Token to authenticate requests")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor RESULT_LIMIT = new PropertyDescriptor.Builder()
            .name("result-limit")
            .displayName("Result Limit")
            .description("The maximum number of results to request for each invocation of the Processor")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 100, true))
            .build();

    static final PropertyDescriptor IS_INCREMENTAL = new PropertyDescriptor.Builder()
            .name("is-incremental")
            .displayName("Incremental Loading")
            .description("The processor can incrementally load the queried objects so that each object is queried exactly once." +
                    " For each query, the processor queries objects within a time window where the objects were modified between" +
                    " the previous run time and the current time (optionally adjusted by the Incremental Delay property).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor INCREMENTAL_DELAY = new PropertyDescriptor.Builder()
            .name("incremental-delay")
            .displayName("Incremental Delay")
            .description(("The ending timestamp of the time window will be adjusted earlier by the amount configured in this property." +
                    " For example, with a property value of 10 seconds, an ending timestamp of 12:30:45 would be changed to 12:30:35." +
                    " Set this property to avoid missing objects when the clock of your local machines and HubSpot servers' clock are not in sync" +
                    " and to protect against HubSpot's mechanism that changes last updated timestamps after object creation."))
            .required(true)
            .defaultValue("30 sec")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .dependsOn(IS_INCREMENTAL, "true")
            .build();

    static final PropertyDescriptor INCREMENTAL_INITIAL_START_TIME = new PropertyDescriptor.Builder()
            .name("incremental-initial-start-time")
            .displayName("Incremental Initial Start Time")
            .description("This property specifies the start time that the processor applies when running the first request." +
                    " The expected format is a UTC date-time such as '2011-12-03T10:15:30Z'")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.ISO8601_INSTANT_VALIDATOR)
            .dependsOn(IS_INCREMENTAL, "true")
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

    private static final String API_BASE_URI = "api.hubapi.com";
    private static final String HTTPS = "https";
    private static final int TOO_MANY_REQUESTS = 429;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();
    private static final Map<String, HubSpotObjectType> OBJECT_TYPE_LOOKUP_MAP = createObjectTypeLookupMap();
    private static final String NO_PAGING = "no paging";
    private static final String PAGING_CURSOR = "after";
    static final String CURSOR_KEY = "paging_next";
    static final String START_INCREMENTAL_KEY = "time_window_start";
    static final String END_INCREMENTAL_KEY = "time_window_end";

    private static Map<String, HubSpotObjectType> createObjectTypeLookupMap() {
        return Arrays.stream(HubSpotObjectType.values())
                .collect(Collectors.toMap(HubSpotObjectType::getValue, Function.identity()));
    }

    private volatile WebClientServiceProvider webClientServiceProvider;
    private volatile boolean isObjectTypeModified;

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            OBJECT_TYPE,
            ACCESS_TOKEN,
            RESULT_LIMIT,
            IS_INCREMENTAL,
            INCREMENTAL_DELAY,
            INCREMENTAL_INITIAL_START_TIME,
            WEB_CLIENT_SERVICE_PROVIDER
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (isConfigurationRestored() && (OBJECT_TYPE.equals(descriptor) || IS_INCREMENTAL.equals(descriptor))) {
            isObjectTypeModified = true;
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (isObjectTypeModified) {
            clearState(context);
            isObjectTypeModified = false;
        }
        final String accessToken = context.getProperty(ACCESS_TOKEN).getValue();
        final String endpoint = context.getProperty(OBJECT_TYPE).getValue();

        final URI uri = getBaseUri(context);

        final AtomicInteger total = new AtomicInteger(-1);
        final Map<String, String> stateMap = getStateMap(session);
        final String filters = createIncrementalFilters(context, stateMap);
        final HttpResponseEntity response = getHttpResponseEntity(accessToken, uri, filters);

        if (response.statusCode() == HttpResponseStatus.OK.getCode()) {
            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, parseHttpResponse(response, total, stateMap));
            if (total.get() > 0) {
                flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().receive(flowFile, uri.toString());
            } else {
                getLogger().debug("Empty response when requested HubSpot endpoint: [{}]", endpoint);
                context.yield();
                session.remove(flowFile);
            }
            updateState(session, stateMap);
        } else if (response.statusCode() == TOO_MANY_REQUESTS) {
            context.yield();
            throw new ProcessException(String.format("Rate limit exceeded, yielding before retrying request. HTTP %d error for requested URI [%s]", response.statusCode(), uri));
        } else {
            final String responseBody = getResponseBodyAsString(context, response, uri);
            getLogger().warn("HTTP {} error for requested URI [{}] with response [{}]", response.statusCode(), uri, responseBody);
        }
    }

    private String getResponseBodyAsString(final ProcessContext context, final HttpResponseEntity response, final URI uri) {
        try {
            return IOUtils.toString(response.body(), StandardCharsets.UTF_8);
        } catch (final IOException e) {
            context.yield();
            throw new UncheckedIOException(String.format("Reading HTTP response body for requested URI [%s] failed", uri), e);
        }
    }

    private OutputStreamCallback parseHttpResponse(final HttpResponseEntity response, final AtomicInteger total,
                                                   final Map<String, String> stateMap) {
        return out -> {
            try (final JsonParser jsonParser = JSON_FACTORY.createParser(response.body());
                 final JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8)) {
                boolean isCursorAvailable = false;
                while (jsonParser.nextToken() != null) {
                    if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME && jsonParser.currentName()
                            .equals("total")) {
                        jsonParser.nextToken();
                        total.set(jsonParser.getIntValue());
                    }
                    if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME && jsonParser.currentName()
                            .equals("results")) {
                        jsonParser.nextToken();
                        jsonGenerator.copyCurrentStructure(jsonParser);
                    }
                    final String fieldName = jsonParser.currentName();
                    if (PAGING_CURSOR.equals(fieldName)) {
                        isCursorAvailable = true;
                        jsonParser.nextToken();
                        stateMap.put(CURSOR_KEY, jsonParser.getText());
                        break;
                    }
                }
                if (!isCursorAvailable) {
                    stateMap.put(CURSOR_KEY, NO_PAGING);
                }
            }
        };
    }

    URI getBaseUri(final ProcessContext context) {
        final String path = context.getProperty(OBJECT_TYPE).getValue();
        return webClientServiceProvider.getHttpUriBuilder()
                .scheme(HTTPS)
                .host(API_BASE_URI)
                .encodedPath(path + "/search")
                .build();
    }

    private HttpResponseEntity getHttpResponseEntity(final String accessToken, final URI uri, final String filters) {
        final InputStream inputStream = IOUtils.toInputStream(filters, StandardCharsets.UTF_8);
        try {
            return webClientServiceProvider.getWebClientService()
                    .post()
                    .uri(uri)
                    .header("Authorization", "Bearer " + accessToken)
                    .header("Content-Type", "application/json")
                    .body(inputStream, OptionalLong.of(inputStream.available()))
                    .retrieve();
        } catch (IOException e) {
            throw new ProcessException("Could not transform incremental filters to input stream", e);
        }
    }

    private String createIncrementalFilters(final ProcessContext context, final Map<String, String> stateMap) {
        final String limit = context.getProperty(RESULT_LIMIT).evaluateAttributeExpressions().getValue();
        final String objectType = context.getProperty(OBJECT_TYPE).getValue();
        final HubSpotObjectType hubSpotObjectType = OBJECT_TYPE_LOOKUP_MAP.get(objectType);
        final Long incrDelayMs = context.getProperty(INCREMENTAL_DELAY).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        final ObjectNode root = OBJECT_MAPPER.createObjectNode();
        if (limit != null) {
            root.put("limit", limit);
        }

        final String cursor = stateMap.get(CURSOR_KEY);
        if (cursor != null && !NO_PAGING.equals(cursor)) {
            root.put(PAGING_CURSOR, cursor);
        }
        final boolean isIncremental = context.getProperty(IS_INCREMENTAL).asBoolean();
        if (isIncremental) {
            final String initialStartTimeValue = context.getProperty(INCREMENTAL_INITIAL_START_TIME).evaluateAttributeExpressions().getValue();
            final String hubspotSpecificIncrementalFieldName = hubSpotObjectType.getLastModifiedDateType().getValue();
            final String lastStartTime = stateMap.get(START_INCREMENTAL_KEY);
            final String lastEndTime = stateMap.get(END_INCREMENTAL_KEY);

            final String currentStartTime;
            final String currentEndTime;

            if (cursor != null && !NO_PAGING.equals(cursor)) {
                currentStartTime = lastStartTime;
                currentEndTime = lastEndTime;
            } else {
                currentStartTime = lastEndTime != null ? lastEndTime : getInitialStartTimeEpoch(initialStartTimeValue);
                final long delayedCurrentEndTime = incrDelayMs != null ? getCurrentEpochTime() - incrDelayMs : getCurrentEpochTime();
                currentEndTime = String.valueOf(delayedCurrentEndTime);

                stateMap.put(START_INCREMENTAL_KEY, currentStartTime);
                stateMap.put(END_INCREMENTAL_KEY, currentEndTime);
            }

            final ArrayNode filters = OBJECT_MAPPER.createArrayNode();

            if (currentStartTime != null) {
                final ObjectNode greaterThanFilterNode = OBJECT_MAPPER.createObjectNode();
                greaterThanFilterNode.put("propertyName", hubspotSpecificIncrementalFieldName);
                greaterThanFilterNode.put("operator", "GTE");
                greaterThanFilterNode.put("value", currentStartTime);
                filters.add(greaterThanFilterNode);
            }

            final ObjectNode lessThanFilterNode = OBJECT_MAPPER.createObjectNode();
            lessThanFilterNode.put("propertyName", hubspotSpecificIncrementalFieldName);
            lessThanFilterNode.put("operator", "LT");
            lessThanFilterNode.put("value", currentEndTime);
            filters.add(lessThanFilterNode);

            root.set("filters", filters);
        }
        return root.toString();
    }

    private String getInitialStartTimeEpoch(String initialStartTimeValue) {
        if (initialStartTimeValue != null) {
            return String.valueOf(Instant.parse(initialStartTimeValue).toEpochMilli());
        }
        return null;
    }

    long getCurrentEpochTime() {
        return Instant.now().toEpochMilli();
    }

    private Map<String, String> getStateMap(final ProcessSession session) {
        final StateMap stateMap;
        try {
            stateMap = session.getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("State retrieval failed", e);
        }
        return new HashMap<>(stateMap.toMap());
    }

    private void updateState(ProcessSession session, Map<String, String> newState) {
        try {
            session.setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Page cursor update failed", e);
        }
    }

    private void clearState(ProcessContext context) {
        try {
            context.getStateManager().clear(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Clearing state failed", e);
        }
    }
}
