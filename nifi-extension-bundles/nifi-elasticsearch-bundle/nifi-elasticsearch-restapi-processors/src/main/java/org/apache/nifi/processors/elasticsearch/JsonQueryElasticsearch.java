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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.elasticsearch.ElasticsearchRequestOptions;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.elasticsearch.api.JsonQueryParameters;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "application/json"),
    @WritesAttribute(attribute = "aggregation.name", description = "The name of the aggregation whose results are in the output flowfile"),
    @WritesAttribute(attribute = "aggregation.number", description = "The number of the aggregation whose results are in the output flowfile"),
    @WritesAttribute(attribute = "hit.count", description = "The number of hits that are in the output flowfile"),
    @WritesAttribute(attribute = "elasticsearch.query.error", description = "The error message provided by Elasticsearch if there is an error querying the index.")
})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"elasticsearch", "elasticsearch5", "elasticsearch6", "elasticsearch7", "elasticsearch8", "query", "read", "get", "json"})
@CapabilityDescription("A processor that allows the user to run a query (with aggregations) written with the " +
        "Elasticsearch JSON DSL. It does not automatically paginate queries for the user. If an incoming relationship is added to this " +
        "processor, it will use the flowfile's content for the query. Care should be taken on the size of the query because the entire response " +
        "from Elasticsearch will be loaded into memory all at once and converted into the resulting flowfiles.")
@SeeAlso(PaginatedJsonQueryElasticsearch.class)
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
                description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing. " +
                        "These parameters will override any matching parameters in the query request body")
})
public class JsonQueryElasticsearch extends AbstractJsonQueryElasticsearch<JsonQueryParameters> {
    @Override
    JsonQueryParameters buildJsonQueryParameters(final FlowFile input, final ProcessContext context, final ProcessSession session)
            throws IOException {
        final JsonQueryParameters jsonQueryParameters = new JsonQueryParameters();
        populateCommonJsonQueryParameters(jsonQueryParameters, input, context, session);
        return jsonQueryParameters;
    }

    @Override
    SearchResponse doQuery(final JsonQueryParameters queryJsonParameters, final List<FlowFile> hitsFlowFiles,
                           final ProcessSession session, final ProcessContext context, final FlowFile input,
                           final StopWatch stopWatch) throws IOException {
        final SearchResponse response = clientService.get().search(
                queryJsonParameters.getQuery(),
                queryJsonParameters.getIndex(),
                queryJsonParameters.getType(),
                new ElasticsearchRequestOptions(getRequestParametersFromDynamicProperties(context, input), getRequestHeadersFromDynamicProperties(context, input))
        );
        if (input != null) {
            session.getProvenanceReporter().send(
                    input,
                    clientService.get().getTransitUrl(queryJsonParameters.getIndex(), queryJsonParameters.getType()),
                    stopWatch.getElapsed(TimeUnit.MILLISECONDS)
            );
        }

        handleResponse(response, true, queryJsonParameters, hitsFlowFiles, session, input, stopWatch);

        return response;
    }

    @Override
    void finishQuery(final FlowFile input, final JsonQueryParameters jsonQueryParameters, final ProcessSession session,
                     final ProcessContext context, final SearchResponse response) {
        if (input != null) {
            session.transfer(input, REL_ORIGINAL);
        }
    }
}
