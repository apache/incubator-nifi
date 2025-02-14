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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.elasticsearch.api.PaginatedJsonQueryParameters;
import org.apache.nifi.processors.elasticsearch.api.PaginationType;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "application/json"),
        @WritesAttribute(attribute = "aggregation.name", description = "The name of the aggregation whose results are in the output flowfile"),
        @WritesAttribute(attribute = "aggregation.number", description = "The number of the aggregation whose results are in the output flowfile"),
        @WritesAttribute(attribute = "page.number", description = "The number of the page (request), starting from 1, in which the results were returned that are in the output flowfile"),
        @WritesAttribute(attribute = "hit.count", description = "The number of hits that are in the output flowfile"),
        @WritesAttribute(attribute = "elasticsearch.query.error", description = "The error message provided by Elasticsearch if there is an error querying the index.")
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@PrimaryNodeOnly
@DefaultSchedule(period = "1 min")
@Tags({"elasticsearch", "elasticsearch5", "elasticsearch6", "elasticsearch7", "elasticsearch8", "query", "scroll", "page", "search", "json"})
@CapabilityDescription("A processor that allows the user to repeatedly run a paginated query (with aggregations) written with the Elasticsearch JSON DSL. " +
        "Search After/Point in Time queries must include a valid \"sort\" field. The processor will retrieve multiple pages of results " +
        "until either no more results are available or the Pagination Keep Alive expiration is reached, after which the query will " +
        "restart with the first page of results being retrieved.")
@SeeAlso({PaginatedJsonQueryElasticsearch.class, ConsumeElasticsearch.class})
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
@Stateful(scopes = Scope.LOCAL, description = "The pagination state (scrollId, searchAfter, pitId, hitCount, pageCount, pageExpirationTimestamp) " +
        "is retained in between invocations of this processor until the Scroll/PiT has expired " +
        "(when the current time is later than the last query execution plus the Pagination Keep Alive interval).")
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "Care should be taken on the size of each page because each response " +
        "from Elasticsearch will be loaded into memory all at once and converted into the resulting flowfiles.")
public class SearchElasticsearch extends AbstractPaginatedJsonQueryElasticsearch {
    static final String STATE_SCROLL_ID = "scrollId";
    static final String STATE_PIT_ID = "pitId";
    static final String STATE_SEARCH_AFTER = "searchAfter";
    static final String STATE_PAGE_EXPIRATION_TIMESTAMP = "pageExpirationTimestamp";
    static final String STATE_PAGE_COUNT = "pageCount";
    static final String STATE_HIT_COUNT = "hitCount";

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder().fromPropertyDescriptor(ElasticsearchRestProcessor.QUERY)
            .description("A query in JSON syntax, not Lucene syntax. Ex: {\"query\":{\"match\":{\"somefield\":\"somevalue\"}}}. " +
                    "If the query is empty, a default JSON Object will be used, which will result in a \"match_all\" query in Elasticsearch.")
            .build();

    private static final Set<Relationship> relationships = Set.of(REL_HITS, REL_AGGREGATIONS);

    static final List<PropertyDescriptor> scrollPropertyDescriptors = Stream.concat(
            Stream.of(
                    // ensure QUERY_DEFINITION_STYLE first for consistency between Elasticsearch processors
                    QUERY_DEFINITION_STYLE,
                    QUERY
            ),
            paginatedPropertyDescriptors.stream().filter(
                    // override QUERY to change description (no FlowFile content used by SearchElasticsearch)
                    pd -> !ElasticsearchRestProcessor.QUERY.equals(pd) && !QUERY_DEFINITION_STYLE.equals(pd)
            )
    ).toList();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return scrollPropertyDescriptors;
    }

    Scope getStateScope() {
        return Scope.LOCAL;
    }

    @Override
    PaginatedJsonQueryParameters buildJsonQueryParameters(final FlowFile input, final ProcessContext context, final ProcessSession session) throws IOException {
        final PaginatedJsonQueryParameters paginatedQueryJsonParameters = super.buildJsonQueryParameters(input, context, session);

        final StateMap stateMap = context.getStateManager().getState(getStateScope());
        paginatedQueryJsonParameters.setHitCount(stateMap.get(STATE_HIT_COUNT) == null ? 0 : Integer.parseInt(stateMap.get(STATE_HIT_COUNT)));
        paginatedQueryJsonParameters.setPageCount(stateMap.get(STATE_PAGE_COUNT) == null ? 0 : Integer.parseInt(stateMap.get(STATE_PAGE_COUNT)));
        paginatedQueryJsonParameters.setScrollId(stateMap.get(STATE_SCROLL_ID));
        paginatedQueryJsonParameters.setSearchAfter(stateMap.get(STATE_SEARCH_AFTER));
        paginatedQueryJsonParameters.setPitId(stateMap.get(STATE_PIT_ID));
        paginatedQueryJsonParameters.setPageExpirationTimestamp(stateMap.get(STATE_PAGE_EXPIRATION_TIMESTAMP));

        return paginatedQueryJsonParameters;
    }

    @Override
    void finishQuery(final FlowFile input, final PaginatedJsonQueryParameters paginatedJsonQueryParameters,
                     final ProcessSession session, final ProcessContext context, final SearchResponse response) throws IOException {
        final Map<String, String> newStateMap = new HashMap<>(10, 1);
        additionalState(newStateMap, paginatedJsonQueryParameters);

        if (response.getHits().isEmpty()) {
            getLogger().debug("No more results for paginated query, resetting state for future queries");
        } else {
            getLogger().debug("Updating state for next execution");

            if (paginationType == PaginationType.SCROLL) {
                newStateMap.put(STATE_SCROLL_ID, response.getScrollId());
            } else {
                newStateMap.put(STATE_SEARCH_AFTER, response.getSearchAfter());

                if (paginationType == PaginationType.POINT_IN_TIME) {
                    newStateMap.put(STATE_PIT_ID, response.getPitId());
                }
            }
            newStateMap.put(STATE_HIT_COUNT, Integer.toString(paginatedJsonQueryParameters.getHitCount()));
            newStateMap.put(STATE_PAGE_COUNT, Integer.toString(paginatedJsonQueryParameters.getPageCount()));
            newStateMap.put(STATE_PAGE_EXPIRATION_TIMESTAMP, paginatedJsonQueryParameters.getPageExpirationTimestamp());
        }
        updateProcessorState(context, newStateMap);
    }

    void additionalState(final Map<String, String> newStateMap, final PaginatedJsonQueryParameters paginatedJsonQueryParameters) {
        // intentionally blank, allows ConsumeElasticsearch to track range value between sessions
    }

    @Override
    boolean isExpired(final PaginatedJsonQueryParameters paginatedJsonQueryParameters, final ProcessContext context,
                      final SearchResponse response) throws IOException {
        final boolean expiredQuery = StringUtils.isNotEmpty(paginatedJsonQueryParameters.getPageExpirationTimestamp())
                && Instant.ofEpochMilli(Long.parseLong(paginatedJsonQueryParameters.getPageExpirationTimestamp())).isBefore(Instant.now());

        if (expiredQuery) {
            getLogger().debug("Existing paginated query has expired, resetting for new query");

            final Map<String, String> newStateMap = new HashMap<>(1, 1);
            additionalState(newStateMap, paginatedJsonQueryParameters);
            updateProcessorState(context, newStateMap);

            paginatedJsonQueryParameters.setPageCount(0);
            paginatedJsonQueryParameters.setHitCount(0);
            paginatedJsonQueryParameters.setPageExpirationTimestamp(null);
            paginatedJsonQueryParameters.setPitId(null);
            paginatedJsonQueryParameters.setScrollId(null);
            paginatedJsonQueryParameters.setSearchAfter(null);
        }
        return expiredQuery;
    }

    @Override
    String getScrollId(final ProcessContext context, final SearchResponse response) throws IOException {
        return response == null || StringUtils.isBlank(response.getScrollId())
                ? context.getStateManager().getState(getStateScope()).get(STATE_SCROLL_ID)
                : response.getScrollId();
    }

    @Override
    String getPitId(final ProcessContext context, final SearchResponse response) throws IOException {
        return response == null || StringUtils.isBlank(response.getScrollId())
                ? context.getStateManager().getState(getStateScope()).get(STATE_PIT_ID)
                : response.getPitId();
    }

    void updateProcessorState(final ProcessContext context, final Map<String, String> newStateMap) throws IOException {
        // using ProcessContext#stateManager instead of ProcessSession#*State methods because the latter don't
        // seem to persist things properly between sessions if the processor is scheduled to run very quickly, e.g. every second (NIFI-9050)
        if (newStateMap == null || newStateMap.isEmpty()) {
            context.getStateManager().clear(getStateScope());
        } else {
            context.getStateManager().setState(newStateMap, getStateScope());
        }
    }
}
