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
package org.apache.nifi.elasticsearch;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.ssl.SSLContextProvider;

import java.util.List;
import java.util.Map;

public interface ElasticSearchClientService extends ControllerService, VerifiableControllerService {
    PropertyDescriptor HTTP_HOSTS = new PropertyDescriptor.Builder()
            .name("el-cs-http-hosts")
            .displayName("HTTP Hosts")
            .description("A comma-separated list of HTTP hosts that host Elasticsearch query nodes. " +
                    "Note that the Host is included in requests as a header (typically including domain and port, e.g. elasticsearch:9200).")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("el-cs-ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections. This service only applies if the Elasticsearch endpoint(s) have been secured with TLS/SSL.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .addValidator(Validator.VALID)
            .build();

    PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(ProxySpec.HTTP);

    PropertyDescriptor AUTHORIZATION_SCHEME = new PropertyDescriptor.Builder()
            .name("authorization-scheme")
            .displayName("Authorization Scheme")
            .description("Authorization Scheme used for optional authentication to Elasticsearch.")
            .allowableValues(AuthorizationScheme.class)
            .defaultValue(AuthorizationScheme.BASIC)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor OAUTH2_ACCESS_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("el-cs-oauth2-token-provider")
            .displayName("OAuth2 Access Token Provider")
            .description("The OAuth2 Access Token Provider used to provide JWTs for Bearer Token Authorization with Elasticsearch.")
            .dependsOn(AUTHORIZATION_SCHEME, AuthorizationScheme.JWT)
            .required(true)
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .addValidator(Validator.VALID)
            .build();

    PropertyDescriptor JWT_SHARED_SECRET = new PropertyDescriptor.Builder()
            .name("jwt-shared-secret")
            .displayName("JWT Shared Secret")
            .description("JWT realm Shared Secret.")
            .dependsOn(AUTHORIZATION_SCHEME, AuthorizationScheme.JWT)
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor RUN_AS_USER = new PropertyDescriptor.Builder()
            .name("el-cs-run-as-user")
            .displayName("Run As User")
            .description("The username to impersonate within Elasticsearch.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("el-cs-username")
            .displayName("Username")
            .description("The username to use with XPack security.")
            .dependsOn(AUTHORIZATION_SCHEME, AuthorizationScheme.BASIC)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("el-cs-password")
            .displayName("Password")
            .description("The password to use with XPack security.")
            .dependsOn(AUTHORIZATION_SCHEME, AuthorizationScheme.BASIC)
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor API_KEY_ID = new PropertyDescriptor.Builder()
            .name("api-key-id")
            .displayName("API Key ID")
            .description("Unique identifier of the API key.")
            .dependsOn(AUTHORIZATION_SCHEME, AuthorizationScheme.API_KEY)
            .required(true)
            .sensitive(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("api-key")
            .displayName("API Key")
            .description("Encoded API key.")
            .dependsOn(AUTHORIZATION_SCHEME, AuthorizationScheme.API_KEY)
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("el-cs-connect-timeout")
            .displayName("Connect timeout")
            .description("Controls the amount of time, in milliseconds, before a timeout occurs when trying to connect.")
            .required(true)
            .defaultValue("5000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    PropertyDescriptor SOCKET_TIMEOUT = new PropertyDescriptor.Builder()
            .name("el-cs-socket-timeout")
            .displayName("Read timeout")
            .description("Controls the amount of time, in milliseconds, before a timeout occurs when waiting for a response.")
            .required(true)
            .defaultValue("60000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("el-cs-charset")
            .displayName("Charset")
            .description("The charset to use for interpreting the response from Elasticsearch.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    AllowableValue ALWAYS_SUPPRESS = new AllowableValue("always-suppress", "Always Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null/empty, will not be written out");

    AllowableValue NEVER_SUPPRESS = new AllowableValue("never-suppress", "Never Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null/empty, will be written out as a null/empty value");

    PropertyDescriptor SUPPRESS_NULLS = new PropertyDescriptor.Builder()
            .name("el-cs-suppress-nulls")
            .displayName("Suppress Null/Empty Values")
            .description("Specifies how the writer should handle null and empty fields (including objects and arrays)")
            .allowableValues(NEVER_SUPPRESS, ALWAYS_SUPPRESS)
            .defaultValue(ALWAYS_SUPPRESS)
            .required(true)
            .build();

    PropertyDescriptor COMPRESSION = new PropertyDescriptor.Builder()
            .name("el-cs-enable-compression")
            .displayName("Enable Compression")
            .description("Whether the REST client should compress requests using gzip content encoding and add the " +
                    "\"Accept-Encoding: gzip\" header to receive compressed responses")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    PropertyDescriptor SEND_META_HEADER = new PropertyDescriptor.Builder()
            .name("el-cs-send-meta-header")
            .displayName("Send Meta Header")
            .description("Whether to send a \"X-Elastic-Client-Meta\" header that describes the runtime environment. " +
                    "It contains information that is similar to what could be found in User-Agent. " +
                    "Using a separate header allows applications to use User-Agent for their own needs, " +
                    "e.g. to identify application version or other environment information")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    PropertyDescriptor STRICT_DEPRECATION = new PropertyDescriptor.Builder()
            .name("el-cs-strict-deprecation")
            .displayName("Strict Deprecation")
            .description("Whether the REST client should return any response containing at least one warning header as a failure")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    AllowableValue NODE_SELECTOR_ANY = new AllowableValue("ANY", "Any",
            "Select any Elasticsearch node to handle requests");
    AllowableValue NODE_SELECTOR_SKIP_DEDICATED_MASTERS = new AllowableValue("SKIP_DEDICATED_MASTERS", "Skip Dedicated Masters",
            "Skip dedicated Elasticsearch master nodes for handling request");

    PropertyDescriptor NODE_SELECTOR = new PropertyDescriptor.Builder()
            .name("el-cs-node-selector")
            .displayName("Node Selector")
            .description("Selects Elasticsearch nodes that can receive requests. Used to keep requests away from dedicated Elasticsearch master nodes")
            .allowableValues(NODE_SELECTOR_ANY, NODE_SELECTOR_SKIP_DEDICATED_MASTERS)
            .defaultValue(NODE_SELECTOR_ANY)
            .required(true)
            .build();

    PropertyDescriptor PATH_PREFIX = new PropertyDescriptor.Builder()
            .name("el-cs-path-prefix")
            .displayName("Path Prefix")
            .description("Sets the path's prefix for every request used by the http client. " +
                    "For example, if this is set to \"/my/path\", then any client request will become \"/my/path/\" + endpoint. " +
                    "In essence, every request's endpoint is prefixed by this pathPrefix. " +
                    "The path prefix is useful for when Elasticsearch is behind a proxy that provides a base path or a proxy that requires all paths to start with '/'; " +
                    "it is not intended for other purposes and it should not be supplied in other scenarios")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    PropertyDescriptor SNIFF_CLUSTER_NODES = new PropertyDescriptor.Builder()
            .name("el-cs-sniff-cluster-nodes")
            .displayName("Sniff Cluster Nodes")
            .description("Periodically sniff for nodes within the Elasticsearch cluster via the Elasticsearch Node Info API. " +
                    "If Elasticsearch security features are enabled (default to \"true\" for 8.x+), the Elasticsearch user must " +
                    "have the \"monitor\" or \"manage\" cluster privilege to use this API." +
                    "Note that all " + HTTP_HOSTS.getDisplayName() + " (and those that may be discovered within the cluster " +
                    "using the Sniffer) must use the same protocol, e.g. http or https, and be contactable using the same client settings. " +
                    "Finally the Elasticsearch \"network.publish_host\" must match one of the \"network.bind_host\" list entries " +
                    "see https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-network.html for more information")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    PropertyDescriptor SNIFF_ON_FAILURE = new PropertyDescriptor.Builder()
            .name("el-cs-sniff-failure")
            .displayName("Sniff on Failure")
            .description("Enable sniffing on failure, meaning that after each failure the Elasticsearch nodes list gets updated " +
                    "straight away rather than at the following ordinary sniffing round")
            .dependsOn(SNIFF_CLUSTER_NODES, "true")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    PropertyDescriptor SNIFFER_INTERVAL = new PropertyDescriptor.Builder()
            .name("el-cs-sniffer-interval")
            .displayName("Sniffer Interval")
            .description("Interval between Cluster sniffer operations")
            .dependsOn(SNIFF_CLUSTER_NODES, "true")
            .defaultValue("5 mins")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .build();

    PropertyDescriptor SNIFFER_REQUEST_TIMEOUT = new PropertyDescriptor.Builder()
            .name("el-cs-sniffer-request-timeout")
            .displayName("Sniffer Request Timeout")
            .description("Cluster sniffer timeout for node info requests")
            .dependsOn(SNIFF_CLUSTER_NODES, "true")
            .defaultValue("1 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .build();

    PropertyDescriptor SNIFFER_FAILURE_DELAY = new PropertyDescriptor.Builder()
            .name("el-cs-sniffer-failure-delay")
            .displayName("Sniffer Failure Delay")
            .description("Delay between an Elasticsearch request failure and updating available Cluster nodes using the Sniffer")
            .dependsOn(SNIFF_ON_FAILURE, "true")
            .defaultValue("1 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .build();

    /**
     * Index a document.
     *
     * @param operation A document to index.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     * @return IndexOperationResponse if successful
     */
    IndexOperationResponse add(IndexOperationRequest operation, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Bulk process multiple documents.
     *
     * @param operations A list of index operations.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     * @return IndexOperationResponse if successful.
     */
    IndexOperationResponse bulk(List<IndexOperationRequest> operations, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Count the documents that match the criteria.
     *
     * @param query A query in the JSON DSL syntax
     * @param index The index to target.
     * @param type The type to target. Will not be used in future versions of Elasticsearch.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     * @return number of documents matching the query
     */
    Long count(String query, String index, String type, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Delete a document by its ID from an index.
     *
     * @param index The index to target.
     * @param type The type to target. Optional. Will not be used in future versions of Elasticsearch.
     * @param id The document ID to remove from the selected index.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     * @return A DeleteOperationResponse object if successful.
     */
    DeleteOperationResponse deleteById(String index, String type, String id, ElasticsearchRequestOptions elasticsearchRequestOptions);


    /**
     * Delete multiple documents by ID from an index.
     * @param index The index to target.
     * @param type The type to target. Optional. Will not be used in future versions of Elasticsearch.
     * @param ids A list of document IDs to remove from the selected index.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     * @return A DeleteOperationResponse object if successful.
     */
    DeleteOperationResponse deleteById(String index, String type, List<String> ids, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Delete documents by query.
     *
     * @param query A valid JSON query to be used for finding documents to delete.
     * @param index The index to target.
     * @param type The type to target within the index. Optional. Will not be used in future versions of Elasticsearch.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     * @return A DeleteOperationResponse object if successful.
     */
    DeleteOperationResponse deleteByQuery(String query, String index, String type, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Update documents by query.
     *
     * @param query A valid JSON query to be used for finding documents to update.
     * @param index The index to target.
     * @param type The type to target within the index. Optional. Will not be used in future versions of Elasticsearch.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     * @return An UpdateOperationResponse object if successful.
     */
    UpdateOperationResponse updateByQuery(String query, String index, String type, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Refresh index/indices.
     *
     * @param index The index to target, if omitted then all indices will be updated.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     */
    void refresh(String index, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Check whether an index exists.
     *
     * @param index The index to check.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     * @return true if index exists, false otherwise
     */
    boolean exists(String index, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Check whether a document exists.
     *
     * @param index The index that holds the document.
     * @param type The document type. Optional. Will not be used in future versions of Elasticsearch.
     * @param id The document ID
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     * @return true if doc exists in index, false otherwise
     */
    boolean documentExists(String index, String type, String id, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Get a document by ID.
     *
     * @param index The index that holds the document.
     * @param type The document type. Optional. Will not be used in future versions of Elasticsearch.
     * @param id The document ID
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     * @return Map if successful, null if not found.
     */
    Map<String, Object> get(String index, String type, String id, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Perform a search using the JSON DSL.
     *
     * @param query A JSON string representing the query.
     * @param index The index to target. Optional.
     * @param type The type to target. Optional. Will not be used in future versions of Elasticsearch.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     * @return A SearchResponse object if successful.
     */
    SearchResponse search(String query, String index, String type, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Retrieve next page of results from a Scroll.
     *
     * @param scroll A JSON string containing scrollId and optional scroll (keep alive) retention period.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     *                                    Request Parameters will be ignored from the Request Options as unusable for this API.
     * @return A SearchResponse object if successful.
     */
    SearchResponse scroll(String scroll, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Initialise a Point in Time for paginated queries.
     * Requires Elasticsearch 7.10+ and XPack features.
     *
     * @param index Index targeted.
     * @param keepAlive Point in Time's retention period (maximum time Elasticsearch will retain the PiT between requests). Optional.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     *                                    Request Parameters will be ignored from the Request Options as unusable for this API.
     * @return the Point in Time Id (pit_id)
     */
    String initialisePointInTime(String index, String keepAlive, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Delete a Point in Time.
     * Requires Elasticsearch 7.10+ and XPack features.
     *
     * @param pitId Point in Time Id to be deleted.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     *                                    Request Parameters will be ignored from the Request Options as unusable for this API.
     * @return A DeleteOperationResponse object if successful.
     */
    DeleteOperationResponse deletePointInTime(String pitId, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Delete a Scroll.
     *
     * @param scrollId Scroll Id to be deleted.
     * @param elasticsearchRequestOptions A collection of request options (parameters and header). Optional.
     *                                    Request Parameters will be ignored from the Request Options as unusable for this API.
     * @return A DeleteOperationResponse object if successful.
     */
    DeleteOperationResponse deleteScroll(String scrollId, ElasticsearchRequestOptions elasticsearchRequestOptions);

    /**
     * Build a transit URL to use with the provenance reporter.
     * @param index Index targeted. Optional.
     * @param type Type targeted. Optional
     * @return a URL describing the Elasticsearch cluster.
     */
    String getTransitUrl(String index, String type);
}
