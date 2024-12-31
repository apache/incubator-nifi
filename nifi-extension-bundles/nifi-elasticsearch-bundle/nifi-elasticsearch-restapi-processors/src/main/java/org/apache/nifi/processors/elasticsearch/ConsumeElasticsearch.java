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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.elasticsearch.api.PaginatedJsonQueryParameters;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "application/json"),
        @WritesAttribute(attribute = "page.number", description = "The number of the page (request), starting from 1, in which the results were returned that are in the output flowfile"),
        @WritesAttribute(attribute = "hit.count", description = "The number of hits that are in the output flowfile"),
        @WritesAttribute(attribute = "elasticsearch.query.error", description = "The error message provided by Elasticsearch if there is an error querying the index.")
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@PrimaryNodeOnly
@DefaultSchedule(period = "1 min")
@Tags({"elasticsearch", "elasticsearch5", "elasticsearch6", "elasticsearch7", "elasticsearch8", "query", "scroll", "page", "search", "json"})
@CapabilityDescription("A processor that repeatedly runs a paginated query against a field using a Range query to consume new Documents from an Elasticsearch index/query. " +
        "The processor will retrieve multiple pages of results until either no more results are available or the Pagination Keep Alive expiration is reached, " +
        "after which the Range query will automatically update the field constraint based on the last retrieved Document value.")
@SeeAlso({SearchElasticsearch.class, PaginatedJsonQueryElasticsearch.class})
@DynamicProperties({
        @DynamicProperty(
                name = "The name of the HTTP request header",
                value = "A Record Path expression to retrieve the HTTP request header value",
                expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
                description = "Prefix: " + ElasticsearchRestProcessor.DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER +
                        " - adds the specified property name/value as a HTTP request header in the Elasticsearch request. " +
                        "If the Record Path expression results in a null or blank value, the HTTP request header will be omitted."),
        @DynamicProperty(
                name = "The name of a URL query parameter to add",
                value = "The value of the URL query parameter",
                expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
                description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing. " +
                        "These parameters will override any matching parameters in the query request body. " +
                        "For SCROLL type queries, these parameters are only used in the initial (first page) query as the " +
                        "Elasticsearch Scroll API does not support the same query parameters for subsequent pages of data.")
})
@Stateful(scopes = Scope.CLUSTER, description = "The pagination state (scrollId, searchAfter, pitId, hitCount, pageCount, pageExpirationTimestamp, trackingRangeValue) " +
        "is retained in between invocations of this processor until the Scroll/PiT has expired " +
        "(when the current time is later than the last query execution plus the Pagination Keep Alive interval).")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Care should be taken on the size of each page because each response " +
        "from Elasticsearch will be loaded into memory all at once and converted into the resulting flowfiles.")
public class ConsumeElasticsearch extends SearchElasticsearch {
    static final String STATE_RANGE_VALUE = "trackingRangeValue";

    public static final PropertyDescriptor SIZE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ElasticsearchRestProcessor.SIZE)
            .clearDependsOn() // always show the Query Builder properties for ConsumeElasticsearch
            .build();

    public static final PropertyDescriptor AGGREGATIONS = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ElasticsearchRestProcessor.AGGREGATIONS)
            .clearDependsOn() // always show the Query Builder properties for ConsumeElasticsearch
            .build();

    public static final PropertyDescriptor SORT = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ElasticsearchRestProcessor.SORT)
            .clearDependsOn() // always show the Query Builder properties for ConsumeElasticsearch
            .build();

    public static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ElasticsearchRestProcessor.FIELDS)
            .clearDependsOn() // always show the Query Builder properties for ConsumeElasticsearch
            .build();

    public static final PropertyDescriptor SCRIPT_FIELDS = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ElasticsearchRestProcessor.SCRIPT_FIELDS)
            .clearDependsOn() // always show the Query Builder properties for ConsumeElasticsearch
            .build();

    public static final PropertyDescriptor RANGE_FIELD = new PropertyDescriptor.Builder()
            .name("es-rest-range-field")
            .displayName("Range Query Field")
            .description("Field to be tracked as part of an Elasticsearch Range query using a \"gt\" bound match. " +
                    "This field must exist within the Elasticsearch document for it to be retrieved.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor RANGE_FIELD_SORT_ORDER = new PropertyDescriptor.Builder()
            .name("es-rest-sort-order")
            .displayName("Sort Order")
            .description("The order in which to sort the \"" + RANGE_FIELD.getDisplayName() + "\". " +
                    "A \"sort\" clause for the \"" + RANGE_FIELD.getDisplayName() +
                    "\" field will be prepended to any provided \"" + SORT.getDisplayName() + "\" clauses. " +
                    "If a \"sort\" clause already exists for the \"" + RANGE_FIELD.getDisplayName() +
                    "\" field, it will not be updated.")
            .allowableValues("asc", "desc")
            .defaultValue("asc")
            .required(true)
            .build();

    public static final PropertyDescriptor RANGE_INITIAL_VALUE = new PropertyDescriptor.Builder()
            .name("es-rest-range-initial-value")
            .displayName("Initial Value")
            .description("The initial value to use for the query if the processor has not run previously. " +
                    "If the processor has run previously and stored a value in its state, this property will be ignored. " +
                    "If no value is provided, and the processor has not previously run, no Range query bounds will be used, " +
                    "i.e. all documents will be retrieved in the specified \"" + RANGE_FIELD_SORT_ORDER.getDisplayName() + "\".")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor RANGE_DATE_FORMAT = new PropertyDescriptor.Builder()
            .name("es-rest-range-format")
            .displayName(RANGE_INITIAL_VALUE.getDisplayName() + " Date Format")
            .description("If the \"" + RANGE_FIELD.getDisplayName() + "\" is a Date field, convert the \"" + RANGE_INITIAL_VALUE.getDisplayName() + "\" to a date with this format. " +
                    "If not specified, Elasticsearch will use the date format provided by the \"" + RANGE_FIELD.getDisplayName() + "\"'s mapping. " +
                    "For valid syntax, see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(RANGE_INITIAL_VALUE)
            .required(false)
            .build();

    public static final PropertyDescriptor RANGE_TIME_ZONE = new PropertyDescriptor.Builder()
            .name("es-rest-range-time-zone")
            .displayName(RANGE_INITIAL_VALUE.getDisplayName() + " Date Time Zone")
            .description("If the \"" + RANGE_FIELD.getDisplayName() + "\" is a Date field, convert the \"" + RANGE_INITIAL_VALUE.getDisplayName() + "\" to UTC with this time zone. " +
                    "Valid values are ISO 8601 UTC offsets, such as \"+01:00\" or \"-08:00\", and IANA time zone IDs, such as \"Europe/London\".")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(RANGE_INITIAL_VALUE)
            .required(false)
            .build();

    public static final PropertyDescriptor ADDITIONAL_FILTERS = new PropertyDescriptor.Builder()
            .name("es-rest-additional-filters")
            .displayName("Additional Filters")
            .description("One or more query filters in JSON syntax, not Lucene syntax. " +
                    "Ex: [{\"match\":{\"somefield\":\"somevalue\"}}, {\"match\":{\"anotherfield\":\"anothervalue\"}}]. " +
                    "These filters wil be used as part of a Bool query's filter.")
            .addValidator(JsonValidator.INSTANCE)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> propertyDescriptors = Stream.concat(
            Stream.of(RANGE_FIELD, RANGE_FIELD_SORT_ORDER, RANGE_INITIAL_VALUE, RANGE_DATE_FORMAT, RANGE_TIME_ZONE, ADDITIONAL_FILTERS),
            scrollPropertyDescriptors.stream()
                    .filter(pd -> !QUERY.equals(pd) && !QUERY_CLAUSE.equals(pd) && !QUERY_DEFINITION_STYLE.equals(pd))
                    .map(property -> {
                        if (property == ElasticsearchRestProcessor.SIZE) return SIZE;
                        if (property == ElasticsearchRestProcessor.AGGREGATIONS) return AGGREGATIONS;
                        if (property == ElasticsearchRestProcessor.SORT) return SORT;
                        if (property == ElasticsearchRestProcessor.FIELDS) return FIELDS;
                        if (property == ElasticsearchRestProcessor.SCRIPT_FIELDS) return SCRIPT_FIELDS;
                        return property;
                    })
    ).toList();

    protected String trackingRangeField;
    protected String trackingSortOrder;

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    Scope getStateScope() {
        return Scope.CLUSTER;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);

        // set tracking field information, so it can be used to update the StateMap after query execution (where ProcessContext is not available)
        trackingRangeField = context.getProperty(RANGE_FIELD).getValue();
        trackingSortOrder = context.getProperty(RANGE_FIELD_SORT_ORDER).getValue();
    }

    @Override
    @OnStopped
    public void onStopped() {
        super.onStopped();

        // reset tracking fields, so that we don't retain incorrect values between processor restarts
        trackingRangeField = null;
        trackingSortOrder = null;
    }

    private String getTrackingRangeField(final ProcessContext context) {
        final String field;
        if (trackingRangeField != null) {
            field = trackingRangeField;
        } else if (context != null) {
            field = context.getProperty(RANGE_FIELD).getValue();
        } else {
            field = null;
        }
        return field;
    }

    private String getTrackingSortOrder(final ProcessContext context) {
        final String sortOrder;
        if (trackingSortOrder != null) {
            sortOrder = trackingSortOrder;
        } else if (context != null) {
            sortOrder = context.getProperty(RANGE_FIELD_SORT_ORDER).getValue();
        } else {
            sortOrder = null;
        }
        return sortOrder;
    }

    @Override
    PaginatedJsonQueryParameters buildJsonQueryParameters(final FlowFile input, final ProcessContext context, final ProcessSession session) throws IOException {
        final PaginatedJsonQueryParameters paginatedQueryJsonParameters = super.buildJsonQueryParameters(input, context, session);
        paginatedQueryJsonParameters.setTrackingRangeValue(getTrackingRangeValueOrDefault(context));
        return paginatedQueryJsonParameters;
    }

    @Override
    public void addQueryClause(final Map<String, Object> query, final Map<String, String> attributes, final ProcessContext context, final ObjectMapper mapper) throws IOException {
        final List<Map<String, Object>> filters = new ArrayList<>(10);

        // only retrieve documents with values greater than the last queried value (if present)
        final String trackingRangeValue = getTrackingRangeValueOrDefault(context);
        if (StringUtils.isNotBlank(trackingRangeValue)) {
            filters.add(Collections.singletonMap("range", Collections.singletonMap(getTrackingRangeField(context),
                    new HashMap<String, String>(3, 1) {{
                        put("gt", trackingRangeValue);
                        if (context.getProperty(RANGE_DATE_FORMAT).isSet()) {
                            put("format", context.getProperty(RANGE_DATE_FORMAT).getValue());
                        }
                        if (context.getProperty(RANGE_TIME_ZONE).isSet()) {
                            put("time_zone", context.getProperty(RANGE_TIME_ZONE).getValue());
                        }
                    }})));
        }

        // add any additional filters specified as a property, allowing for one (Object) or multiple (Array of Objects) filters
        if (context.getProperty(ADDITIONAL_FILTERS).isSet()) {
            final JsonNode additionalFilters = mapper.readTree(context.getProperty(ADDITIONAL_FILTERS).getValue());
            if (additionalFilters.isArray()) {
                filters.addAll(mapper.convertValue(additionalFilters, new TypeReference<List<Map<String, Object>>>() {
                }));
            } else {
                filters.add(mapper.convertValue(additionalFilters, new TypeReference<>() {
                }));
            }
        }

        if (!filters.isEmpty()) {
            final Map<String, Object> bool = Collections.singletonMap("bool", Collections.singletonMap("filter", filters));
            query.put("query", bool);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addSortClause(final Map<String, Object> query, final Map<String, String> attributes, final ProcessContext context, final ObjectMapper mapper) throws IOException {
        super.addSortClause(query, attributes, context, mapper);

        final List<Map<String, Object>> sort;
        if (query.containsKey("sort")) {
            sort = (List<Map<String, Object>>) query.get("sort");
        } else {
            sort = new ArrayList<>(1);
            query.put("sort", sort);
        }

        if (sort.stream().noneMatch(s -> s.containsKey(getTrackingRangeField(context)))) {
            sort.addFirst(Collections.singletonMap(getTrackingRangeField(context), getTrackingSortOrder(context)));
        }
    }

    @Override
    void additionalState(final Map<String, String> newStateMap, final PaginatedJsonQueryParameters paginatedJsonQueryParameters) {
        newStateMap.put(STATE_RANGE_VALUE, paginatedJsonQueryParameters.getTrackingRangeValue());
    }

    @Override
    void updateQueryParameters(final PaginatedJsonQueryParameters paginatedJsonQueryParameters, final SearchResponse response) {
        super.updateQueryParameters(paginatedJsonQueryParameters, response);

        // update the tracking range value with first/last hit (depending upon sort order)
        if (!response.getHits().isEmpty()) {
            final int trackingHitIndex;
            if ("desc".equals(getTrackingSortOrder(null)) && paginatedJsonQueryParameters.getPageCount() == 1) {
                trackingHitIndex = 0;
            } else if ("asc".equals(getTrackingSortOrder(null))) {
                trackingHitIndex = response.getHits().size() - 1;
            } else {
                return;
            }

            @SuppressWarnings("unchecked") final String nextValue = String.valueOf(((Map<String, Object>) response.getHits().get(trackingHitIndex).get("_source"))
                    .get(getTrackingRangeField(null)));
            if (StringUtils.isNotBlank(nextValue)) {
                paginatedJsonQueryParameters.setTrackingRangeValue(nextValue);
            }
        }
    }

    private String getTrackingRangeValueOrDefault(final ProcessContext context) throws IOException {
        final StateMap stateMap = context.getStateManager().getState(getStateScope());
        return stateMap == null || stateMap.get(STATE_RANGE_VALUE) == null
                ? context.getProperty(RANGE_INITIAL_VALUE).getValue()
                : stateMap.get(STATE_RANGE_VALUE);
    }
}
