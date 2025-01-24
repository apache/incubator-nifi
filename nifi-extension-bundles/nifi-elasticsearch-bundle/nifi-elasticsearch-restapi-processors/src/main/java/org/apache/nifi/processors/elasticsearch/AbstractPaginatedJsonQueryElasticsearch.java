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

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.elasticsearch.ElasticsearchRequestOptions;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.elasticsearch.api.PaginatedJsonQueryParameters;
import org.apache.nifi.processors.elasticsearch.api.PaginationType;
import org.apache.nifi.processors.elasticsearch.api.ResultOutputStrategy;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public abstract class AbstractPaginatedJsonQueryElasticsearch extends AbstractJsonQueryElasticsearch<PaginatedJsonQueryParameters> {
    public static final PropertyDescriptor SEARCH_RESULTS_SPLIT = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT)
            .description("Output a flowfile containing all hits or one flowfile for each individual hit " +
                    "or one flowfile containing all hits from all paged responses.")
            .allowableValues(ResultOutputStrategy.class)
            .build();

    public static final PropertyDescriptor PAGINATION_TYPE = new PropertyDescriptor.Builder()
            .name("el-rest-pagination-type")
            .displayName("Pagination Type")
            .description("Pagination method to use. Not all types are available for all Elasticsearch versions, " +
                    "check the Elasticsearch docs to confirm which are applicable and recommended for your service.")
            .allowableValues(PaginationType.class)
            .defaultValue(PaginationType.SCROLL)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor PAGINATION_KEEP_ALIVE = new PropertyDescriptor.Builder()
            .name("el-rest-pagination-keep-alive")
            .displayName("Pagination Keep Alive")
            .description("Pagination \"keep_alive\" period. Period Elasticsearch will keep the scroll/pit cursor alive " +
                    "in between requests (this is not the time expected for all pages to be returned, but the maximum " +
                    "allowed time for requests between page retrievals).")
            .required(true)
            .defaultValue("10 mins")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, 24, TimeUnit.HOURS))
            .build();

    static final List<PropertyDescriptor> paginatedPropertyDescriptors =         Stream.concat(
            // replace SEARCH_RESULTS_SPLIT property to allow additional output strategies
            queryPropertyDescriptors.stream().map(pd -> AbstractJsonQueryElasticsearch.SEARCH_RESULTS_SPLIT.equals(pd) ? SEARCH_RESULTS_SPLIT : pd),
            Stream.of(PAGINATION_TYPE, PAGINATION_KEEP_ALIVE)
    ).toList();

    private ObjectWriter writer;

    PaginationType paginationType;

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);

        paginationType = context.getProperty(PAGINATION_TYPE).asAllowableValue(PaginationType.class);
        // output as newline delimited JSON (allows for multiple pages of results to be appended to existing FlowFiles without retaining all hits in memory)
        writer = mapper.writer().withRootValueSeparator("\n");
    }

    @Override
    SearchResponse doQuery(final PaginatedJsonQueryParameters paginatedJsonQueryParameters, List<FlowFile> hitsFlowFiles,
                           final ProcessSession session, final ProcessContext context, final FlowFile input,
                           final StopWatch stopWatch) throws IOException {
        SearchResponse response = null;
        do {
            // check any previously started query hasn't expired
            final boolean expiredQuery = isExpired(paginatedJsonQueryParameters, context, response);
            final boolean newQuery = StringUtils.isBlank(paginatedJsonQueryParameters.getPageExpirationTimestamp()) || expiredQuery;

            // execute query/scroll
            final String queryJson = updateQueryJson(newQuery, paginatedJsonQueryParameters, context, input);
            final Map<String, String> requestParameters = getRequestParametersFromDynamicProperties(context, input);
            final Map<String, String> requestHeaders = getRequestHeadersFromDynamicProperties(context, input);
            if (!newQuery && paginationType == PaginationType.SCROLL) {
                if (!requestParameters.isEmpty()) {
                    getLogger().warn("Elasticsearch _scroll API does not accept query parameters, ignoring dynamic properties {}", requestParameters.keySet());
                }
                response = clientService.get().scroll(queryJson, new ElasticsearchRequestOptions(null, requestHeaders));
            } else {
                if (paginationType == PaginationType.SCROLL) {
                    requestParameters.put("scroll", paginatedJsonQueryParameters.getKeepAlive());
                }

                response = clientService.get().search(
                        queryJson,
                        // Point in Time uses general /_search API not /index/_search
                        paginationType == PaginationType.POINT_IN_TIME ? null : paginatedJsonQueryParameters.getIndex(),
                        paginatedJsonQueryParameters.getType(),
                        new ElasticsearchRequestOptions(requestParameters, requestHeaders)
                );
                paginatedJsonQueryParameters.setPitId(response.getPitId());
                paginatedJsonQueryParameters.setSearchAfter(response.getSearchAfter());
            }
            paginatedJsonQueryParameters.setScrollId(response.getScrollId());
            if (newQuery && input != null) {
                session.getProvenanceReporter().send(
                        input,
                        clientService.get().getTransitUrl(paginatedJsonQueryParameters.getIndex(), paginatedJsonQueryParameters.getType()),
                        stopWatch.getElapsed(TimeUnit.MILLISECONDS)
                );
            }

            updateQueryParameters(paginatedJsonQueryParameters, response);

            hitsFlowFiles = handleResponse(response, newQuery, paginatedJsonQueryParameters, hitsFlowFiles, session, input, stopWatch);
        } while (!response.getHits().isEmpty() && (input != null || hitStrategy == ResultOutputStrategy.PER_QUERY));

        if (response.getHits().isEmpty()) {
            getLogger().debug("No more results for paginated query, clearing Elasticsearch resources");
            clearElasticsearchState(context, response, input);
        }

        return response;
    }

    @Override
    PaginatedJsonQueryParameters buildJsonQueryParameters(final FlowFile input, final ProcessContext context, final ProcessSession session) throws IOException {
        final PaginatedJsonQueryParameters paginatedJsonQueryParameters = new PaginatedJsonQueryParameters();
        populateCommonJsonQueryParameters(paginatedJsonQueryParameters, input, context, session);

        paginatedJsonQueryParameters.setKeepAlive(context.getProperty(PAGINATION_KEEP_ALIVE).asTimePeriod(TimeUnit.SECONDS) + "s");

        return paginatedJsonQueryParameters;
    }

    abstract boolean isExpired(final PaginatedJsonQueryParameters paginatedQueryParameters, final ProcessContext context,
                               final SearchResponse response) throws IOException;

    abstract String getScrollId(final ProcessContext context, final SearchResponse response) throws IOException;

    abstract String getPitId(final ProcessContext context, final SearchResponse response) throws IOException;

    private void prepareNextPageQuery(final ObjectNode queryJson, final PaginatedJsonQueryParameters paginatedJsonQueryParameters) throws IOException {
        // prepare to get next page of results (depending on pagination type)
        if (paginationType == PaginationType.SCROLL) {
            // overwrite query JSON with existing Scroll details
            queryJson.removeAll().put("scroll_id", paginatedJsonQueryParameters.getScrollId());
            if (StringUtils.isNotBlank(paginatedJsonQueryParameters.getKeepAlive())) {
                queryJson.put("scroll", paginatedJsonQueryParameters.getKeepAlive());
            }
        } else {
            // add search_after to query JSON
            queryJson.set("search_after", mapper.readValue(paginatedJsonQueryParameters.getSearchAfter(), ArrayNode.class));

            // remove any request for aggregations as they were dealt with in the first page
            if (queryJson.has("aggs")) {
                getLogger().debug("Removing \"aggs\" from non-initial paged query");
                queryJson.remove("aggs");
            }
        }
    }

    private String updateQueryJson(final boolean newQuery, final PaginatedJsonQueryParameters paginatedJsonQueryParameters, final ProcessContext context, final FlowFile input) throws IOException {
        final ObjectNode queryJson = mapper.readValue(paginatedJsonQueryParameters.getQuery(), ObjectNode.class);

        if (!newQuery) {
            prepareNextPageQuery(queryJson, paginatedJsonQueryParameters);
        } else if ((paginationType == PaginationType.POINT_IN_TIME || paginationType == PaginationType.SEARCH_AFTER)
                && !queryJson.has("sort")) {
            // verify query contains a "sort" field if pit/search_after requested
            throw new IllegalArgumentException("Query using pit/search_after must contain a \"sort\" field");
        }

        if (paginationType == PaginationType.POINT_IN_TIME) {
            // add pit_id to query JSON
            final String queryPitId = newQuery
                    ? clientService.get().initialisePointInTime(
                            paginatedJsonQueryParameters.getIndex(), paginatedJsonQueryParameters.getKeepAlive(),
                            new ElasticsearchRequestOptions(null, getRequestHeadersFromDynamicProperties(context, input))
                    )
                    : paginatedJsonQueryParameters.getPitId();

            final ObjectNode pit = JsonNodeFactory.instance.objectNode().put("id", queryPitId);
            if (StringUtils.isNotBlank(paginatedJsonQueryParameters.getKeepAlive())) {
                pit.put("keep_alive", paginatedJsonQueryParameters.getKeepAlive());
            }
            queryJson.set("pit", pit);
        }

        return mapper.writeValueAsString(queryJson);
    }

    private FlowFile writeCombinedHitFlowFile(final int count, final List<Map<String, Object>> hits, final ProcessSession session,
                                              final FlowFile hitFlowFile, final Map<String, String> attributes, final boolean append) {
        FlowFile ff;
        if (append) {
            // separate new from existing hits with a newline
            ff = session.append(hitFlowFile, out -> out.write('\n'));
            ff = session.append(ff, out -> writer.writeValues(out).writeAll(hits));
        } else {
            ff = session.write(hitFlowFile, out -> writer.writeValues(out).writeAll(hits));
        }

        attributes.put("hit.count", Integer.toString(count));
        return session.putAllAttributes(ff, attributes);
    }

    private void combineHits(final List<Map<String, Object>> hits, final PaginatedJsonQueryParameters paginatedJsonQueryParameters,
                             final ProcessSession session, final FlowFile parent,
                             final Map<String, String> attributes, final List<FlowFile> hitsFlowFiles, final boolean newQuery) {
        if (hits != null && !hits.isEmpty()) {
            final FlowFile hitFlowFile;
            final boolean append = !hitsFlowFiles.isEmpty();
            if (!hitsFlowFiles.isEmpty()) {
                hitFlowFile = hitsFlowFiles.removeFirst();
            } else {
                hitFlowFile = createChildFlowFile(session, parent);
            }

            hitsFlowFiles.add(writeCombinedHitFlowFile(paginatedJsonQueryParameters.getHitCount() + hits.size(),
                    hits, session, hitFlowFile, attributes, append));
        } else if (isOutputNoHits() && newQuery) {
            final FlowFile hitFlowFile = createChildFlowFile(session, parent);
            hitsFlowFiles.add(writeHitFlowFile(0, "", session, hitFlowFile, attributes));
        }
    }

    /*
     * The List<FlowFile> hitsFlowFiles parameter and return value are used in order to allow pagination of query results.
     * The List is created in AbstractJsonQueryElasticsearch#onTrigger and passed to doQuery => handleResponse => handleHits,
     * for paginated queries, the List could contain one (or more) FlowFiles, to which further hits may be appended when the next
     * SearchResponse is processed, i.e. this approach allows recursion for paginated queries, but is unnecessary for single-response queries.
     */
    @Override
    List<FlowFile> handleHits(final List<Map<String, Object>> hits, final boolean newQuery, final PaginatedJsonQueryParameters paginatedJsonQueryParameters,
                              final ProcessSession session, final FlowFile parent, final Map<String, String> attributes,
                              final List<FlowFile> hitsFlowFiles, final String transitUri, final StopWatch stopWatch) throws IOException {
        attributes.put("page.number", Integer.toString(paginatedJsonQueryParameters.getPageCount()));

        if (hitStrategy == ResultOutputStrategy.PER_QUERY) {
            final List<Map<String, Object>> formattedHits = formatHits(hits);
            combineHits(formattedHits, paginatedJsonQueryParameters, session, parent, attributes, hitsFlowFiles, newQuery);

            // output results if it seems we've combined all available results (i.e. no hits in this page and therefore no more expected)
            if (!hitsFlowFiles.isEmpty() && (hits == null || hits.isEmpty())) {
                session.transfer(hitsFlowFiles, REL_HITS);
                hitsFlowFiles.forEach(ff -> session.getProvenanceReporter().receive(ff, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS)));
                hitsFlowFiles.clear();
            }
        } else {
            super.handleHits(hits, newQuery, paginatedJsonQueryParameters, session, parent, attributes, hitsFlowFiles, transitUri, stopWatch);
        }

        return hitsFlowFiles;
    }

    void updateQueryParameters(final PaginatedJsonQueryParameters paginatedJsonQueryParameters, final SearchResponse response) {
        paginatedJsonQueryParameters.incrementPageCount();

        // mark the paginated query for expiry if there are no hits (no more pages to obtain so stop looping on this query)
        final String keepAliveDuration = "PT" + (!response.getHits().isEmpty() ? paginatedJsonQueryParameters.getKeepAlive() : "0s");
        paginatedJsonQueryParameters.setPageExpirationTimestamp(
                String.valueOf(Instant.now().plus(Duration.parse(keepAliveDuration)).toEpochMilli())
        );
    }

    void clearElasticsearchState(final ProcessContext context, final SearchResponse response, final FlowFile input) {
        try {
            final Map<String, String> requestHeaders = getRequestHeadersFromDynamicProperties(context, input);
            if (paginationType == PaginationType.SCROLL) {
                final String scrollId = getScrollId(context, response);

                if (StringUtils.isNotBlank(scrollId)) {
                    clientService.get().deleteScroll(scrollId, new ElasticsearchRequestOptions(null, requestHeaders));
                }
            } else if (paginationType == PaginationType.POINT_IN_TIME) {
                final String pitId = getPitId(context, response);

                if (StringUtils.isNotBlank(pitId)) {
                    clientService.get().deletePointInTime(pitId, new ElasticsearchRequestOptions(null, requestHeaders));
                }
            }
        } catch (final Exception ex) {
            getLogger().warn("Error while cleaning up Elasticsearch pagination resources, ignoring", ex);
        }
    }
}
