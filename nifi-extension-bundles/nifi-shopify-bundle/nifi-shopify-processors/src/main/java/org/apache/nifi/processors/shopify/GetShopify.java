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
package org.apache.nifi.processors.shopify;

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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
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
import org.apache.nifi.processors.shopify.model.IncrementalLoadingParameter;
import org.apache.nifi.processors.shopify.model.ResourceType;
import org.apache.nifi.processors.shopify.model.ShopifyResource;
import org.apache.nifi.processors.shopify.rest.ShopifyRestService;
import org.apache.nifi.processors.shopify.util.IncrementalTimers;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"shopify"})
@Stateful(scopes = Scope.CLUSTER, description = "For a few resources the processor supports incremental loading." +
        " The list of the resources with the supported parameters can be found in the additional details.")
@CapabilityDescription("Retrieves objects from a custom Shopify store. The processor yield time must be set to the account's rate limit accordingly.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
@WritesAttribute(attribute = "mime.type", description = "Sets the MIME type to application/json")
public class GetShopify extends AbstractProcessor {

    static final PropertyDescriptor STORE_DOMAIN = new PropertyDescriptor.Builder()
            .name("store-domain")
            .displayName("Store Domain")
            .description("The domain of the Shopify store, e.g. nifistore.myshopify.com")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("access-token")
            .displayName("Access Token")
            .description("Access Token to authenticate requests")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor API_VERSION = new PropertyDescriptor.Builder()
            .name("api-version")
            .displayName("API Version")
            .description("The Shopify REST API version")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("2022-10")
            .build();

    static final PropertyDescriptor OBJECT_CATEGORY = new PropertyDescriptor.Builder()
            .name("object-category")
            .displayName("Object Category")
            .description("Shopify object category")
            .required(true)
            .allowableValues(ResourceType.class)
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
                    " For each query, the processor queries objects which were created or modified after the previous run time" +
                    " but before the current time.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor INCREMENTAL_DELAY = new PropertyDescriptor.Builder()
            .name("incremental-delay")
            .displayName("Incremental Delay")
            .description("The ending timestamp of the time window will be adjusted earlier by the amount configured in this property." +
                    " For example, with a property value of 10 seconds, an ending timestamp of 12:30:45 would be changed to 12:30:35." +
                    " Set this property to avoid missing objects when the clock of your local machines and Shopify servers' clock are not in sync.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .dependsOn(IS_INCREMENTAL, "true")
            .defaultValue("3 sec")
            .build();

    static final PropertyDescriptor INCREMENTAL_INITIAL_START_TIME = new PropertyDescriptor.Builder()
            .name("incremental-initial-start-time")
            .displayName("Incremental Initial Start Time")
            .description("This property specifies the start time when running the first request." +
                    " Represents an ISO 8601-encoded date and time string. For example, 3:50 pm on September 7, 2019" +
                    " in the time zone of UTC (Coordinated Universal Time) is represented as \"2019-09-07T15:50:00Z\".")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.ISO8601_INSTANT_VALIDATOR)
            .dependsOn(IS_INCREMENTAL, "true")
            .build();

    public static final PropertyDescriptor WEB_CLIENT_PROVIDER = new PropertyDescriptor.Builder()
            .name("web-client-service-provider")
            .displayName("Web Client Service Provider")
            .description("Controller service for HTTP client operations")
            .required(true)
            .identifiesControllerService(WebClientServiceProvider.class)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful query.")
            .build();

    private static final Map<ResourceType, PropertyDescriptor> propertyMap = new EnumMap<>(ResourceType.class);
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = createPropertyDescriptors();
    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);

    private static List<PropertyDescriptor> createPropertyDescriptors() {
        final List<PropertyDescriptor> resourceDescriptors = Arrays.stream(ResourceType.values())
                .map(resourceType -> {
                    final PropertyDescriptor resourceDescriptor = new PropertyDescriptor.Builder()
                            .name(resourceType.getValue())
                            .displayName(resourceType.getPropertyDisplayName())
                            .description(resourceType.getPropertyDescription())
                            .required(true)
                            .dependsOn(OBJECT_CATEGORY, resourceType.getValue())
                            .allowableValues(resourceType.getResourcesAsAllowableValues())
                            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                            .build();
                    propertyMap.put(resourceType, resourceDescriptor);
                    return resourceDescriptor;
                })
                .toList();
        final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(List.of(
                STORE_DOMAIN,
                ACCESS_TOKEN,
                API_VERSION,
                OBJECT_CATEGORY
        ));
        propertyDescriptors.addAll(resourceDescriptors);
        propertyDescriptors.addAll(List.of(
                RESULT_LIMIT,
                IS_INCREMENTAL,
                INCREMENTAL_DELAY,
                INCREMENTAL_INITIAL_START_TIME,
                WEB_CLIENT_PROVIDER)
        );
        return Collections.unmodifiableList(propertyDescriptors);
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();
    private static final int TOO_MANY_REQUESTS = 429;
    private static final Pattern CURSOR_PATTERN = Pattern.compile("<([^<]*)>; rel=\"next\"");
    private static final String LAST_EXECUTION_TIME_KEY = "last_execution_time";
    private static final List<String> RESET_STATE_PROPERTY_NAMES;

    static {
        RESET_STATE_PROPERTY_NAMES = Arrays.stream(ResourceType.values())
                .map(ResourceType::getValue)
                .collect(Collectors.toList());
        RESET_STATE_PROPERTY_NAMES.add(OBJECT_CATEGORY.getName());
        RESET_STATE_PROPERTY_NAMES.add(IS_INCREMENTAL.getName());
    }


    private volatile ShopifyRestService shopifyRestService;
    private volatile String resourceName;
    private volatile boolean isResetState;

    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (isConfigurationRestored() && RESET_STATE_PROPERTY_NAMES.contains(descriptor.getName())) {
            isResetState = true;
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (isResetState) {
            clearState(context);
            isResetState = false;
        }
        final WebClientServiceProvider webClientServiceProvider =
                context.getProperty(WEB_CLIENT_PROVIDER).asControllerService(WebClientServiceProvider.class);

        final String apiVersion = context.getProperty(API_VERSION).evaluateAttributeExpressions().getValue();
        final String baseUrl = context.getProperty(STORE_DOMAIN).evaluateAttributeExpressions().getValue();
        final String accessToken = context.getProperty(ACCESS_TOKEN).evaluateAttributeExpressions().getValue();

        final String category = context.getProperty(OBJECT_CATEGORY).getValue();
        final ResourceType resourceType = ResourceType.valueOf(category);
        resourceName = context.getProperty(propertyMap.get(resourceType)).getValue();
        final String limit = context.getProperty(RESULT_LIMIT).evaluateAttributeExpressions().getValue();

        final ShopifyResource shopifyResource = resourceType.getResource(resourceName);

        shopifyRestService = getShopifyRestService(webClientServiceProvider, apiVersion, baseUrl, accessToken, resourceName,
                limit, shopifyResource.getIncrementalLoadingParameter());
    }

    ShopifyRestService getShopifyRestService(final WebClientServiceProvider webClientServiceProvider, final String apiVersion,
                                             final String baseUrl, final String accessToken, final String resourceName,
                                             final String limit, final IncrementalLoadingParameter incrementalLoadingParameter) {
        return new ShopifyRestService(
                webClientServiceProvider,
                apiVersion,
                baseUrl,
                accessToken,
                resourceName,
                limit,
                incrementalLoadingParameter
        );
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
        final Map<String, String> stateMap = getStateMap(session);
        final boolean isIncremental = context.getProperty(IS_INCREMENTAL).asBoolean();
        final String initialStartTime = context.getProperty(INCREMENTAL_INITIAL_START_TIME).evaluateAttributeExpressions().getValue();
        final Long incrDelayMs = context.getProperty(INCREMENTAL_DELAY).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        IncrementalTimers timers = null;
        if (isIncremental) {
            timers = IncrementalTimers.ofState(stateMap, initialStartTime, incrDelayMs, Instant.now());
        }
        String cursor = null;
        HttpResponseEntity response = null;

        do {
            try {
                if (cursor != null) {
                    response = shopifyRestService.getShopifyObjects(cursor);
                } else if (isIncremental) {
                    response = shopifyRestService.getShopifyObjects(timers.getStartTime(), timers.getExclusiveEndTime());
                } else {
                    response = shopifyRestService.getShopifyObjects();
                }

                final AtomicInteger objectCountHolder = new AtomicInteger();
                cursor = getPageCursor(response);
                if (response.statusCode() == HttpResponseStatus.OK.getCode()) {
                    FlowFile flowFile = session.create();
                    flowFile = session.write(flowFile, parseHttpResponse(response, objectCountHolder));
                    if (cursor == null && isIncremental) {
                        final Map<String, String> updatedStateMap = new HashMap<>(stateMap);
                        updatedStateMap.put(LAST_EXECUTION_TIME_KEY, timers.getEndTime());
                        updateState(session, updatedStateMap);
                    }
                    if (objectCountHolder.get() > 0) {
                        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
                        session.transfer(flowFile, REL_SUCCESS);
                        session.getProvenanceReporter().receive(flowFile, shopifyRestService.getBaseUriString());
                    } else {
                        getLogger().debug("Empty response when requested Shopify resource: [{}]", resourceName);
                        session.remove(flowFile);
                        context.yield();
                    }
                } else if (response.statusCode() < 200 || response.statusCode() >= 300) {
                    if (response.statusCode() == TOO_MANY_REQUESTS) {
                        context.yield();
                        throw new ProcessException(String.format(
                                "Rate limit exceeded, yielding before retrying request. HTTP %d error for requested URI [%s]",
                                response.statusCode(), resourceName));
                    } else {
                        context.yield();
                        getLogger().warn("HTTP {} error for requested Shopify resource [{}]", response.statusCode(),
                                resourceName);
                    }
                }
            } finally {
                try {
                    if (response != null) {
                        response.close();
                    }
                } catch (IOException e) {
                    getLogger().error("Could not close http response", e);
                }
            }

        } while (cursor != null);
    }

    private String getPageCursor(HttpResponseEntity response) {
        Optional<String> link = response.headers().getFirstHeader("Link");
        String s = null;
        if (link.isPresent()) {
            final Matcher matcher = CURSOR_PATTERN.matcher(link.get());
            while (matcher.find()) {
                s = matcher.group(1);
            }
        }
        return s;
    }

    private OutputStreamCallback parseHttpResponse(final HttpResponseEntity response,
                                                   final AtomicInteger objectCountHolder) {
        return out -> {
            try (JsonParser jsonParser = JSON_FACTORY.createParser(response.body());
                 final JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8)) {
                while (jsonParser.nextToken() != null) {
                    if (jsonParser.getCurrentToken() == JsonToken.FIELD_NAME && jsonParser.currentName()
                            .equals(resourceName)) {
                        jsonParser.nextToken();
                        jsonGenerator.writeStartArray();
                        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                            jsonGenerator.copyCurrentStructure(jsonParser);
                            objectCountHolder.incrementAndGet();
                        }
                        jsonGenerator.writeEndArray();
                    }
                }
            }
        };
    }

    private Map<String, String> getStateMap(final ProcessSession session) {
        StateMap state;
        try {
            state = session.getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("State retrieval failed", e);
        }
        return state.toMap();
    }

    void updateState(ProcessSession session, Map<String, String> newState) {
        try {
            session.setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("State update failed", e);
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
