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
package org.apache.nifi.processors.elasticsearch.mock;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.elasticsearch.DeleteOperationResponse;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticsearchRequestOptions;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.elasticsearch.SearchResponse;
import org.apache.nifi.elasticsearch.UpdateOperationResponse;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.StringUtils;

import java.util.List;
import java.util.Map;

public class AbstractMockElasticsearchClient extends AbstractControllerService implements ElasticSearchClientService {
    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        return null;
    }

    @Override
    public IndexOperationResponse add(final IndexOperationRequest operation, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public IndexOperationResponse bulk(final List<IndexOperationRequest> operations, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public Long count(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteById(final String index, final String type, final String id, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteById(final String index, final String type, final List<String> ids, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteByQuery(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public UpdateOperationResponse updateByQuery(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public void refresh(final String index, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        // intentionally blank
    }

    @Override
    public boolean exists(final String index, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return true;
    }

    @Override
    public boolean documentExists(final String index, final String type, final String id, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return true;
    }

    @Override
    public Map<String, Object> get(final String index, final String type, final String id, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public SearchResponse search(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public SearchResponse scroll(final String scroll, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public String initialisePointInTime(final String index, final String keepAlive, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public DeleteOperationResponse deletePointInTime(final String pitId, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public DeleteOperationResponse deleteScroll(final String scrollId, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return null;
    }

    @Override
    public String getTransitUrl(final String index, final String type) {
        return String.format("http://localhost:9200/%s/%s", index, StringUtils.isNotBlank(type) ? type : "");
    }

    public boolean isThrowRetriableError() {
        return throwRetriableError;
    }

    public void setThrowRetriableError(final boolean throwRetriableError) {
        this.throwRetriableError = throwRetriableError;
    }

    public boolean isThrowFatalError() {
        return throwFatalError;
    }

    public void setThrowFatalError(final boolean throwFatalError) {
        this.throwFatalError = throwFatalError;
    }

    private boolean throwRetriableError;
    private boolean throwFatalError;
}
