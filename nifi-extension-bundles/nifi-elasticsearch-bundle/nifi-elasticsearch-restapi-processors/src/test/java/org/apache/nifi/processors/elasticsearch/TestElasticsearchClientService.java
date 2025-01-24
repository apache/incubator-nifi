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
package org.apache.nifi.processors.elasticsearch;

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
import org.apache.nifi.processors.elasticsearch.mock.MockElasticsearchException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestElasticsearchClientService extends AbstractControllerService implements ElasticSearchClientService {
    private static final String AGGS_RESULT;
    private static final String HITS_RESULT;

    static {
        try {
            AGGS_RESULT = JsonUtils.readString(Paths.get("src/test/resources/TestElasticsearchClientService/aggsResult.json"));
            HITS_RESULT = JsonUtils.readString(Paths.get("src/test/resources/TestElasticsearchClientService/hitsResult.json"));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final boolean returnAggs;
    private boolean throwErrorInSearch;
    private boolean throwErrorInGet;
    private boolean throwNotFoundInGet;
    private boolean throwErrorInDelete;
    private boolean throwErrorInPit;
    private boolean throwErrorInUpdate;
    private int pageCount = 0;
    private int maxPages = 1;
    private ElasticsearchRequestOptions elasticsearchRequestOptions;

    private boolean scrolling = false;
    private String query;

    public TestElasticsearchClientService(final boolean returnAggs) {
        this.returnAggs = returnAggs;
    }

    private void common(final boolean throwError, final ElasticsearchRequestOptions elasticsearchRequestOptions) throws IOException {
        if (throwError) {
            if (throwNotFoundInGet) {
                throw new MockElasticsearchException(false, true);
            } else {
                throw new IOException("Simulated IOException");
            }
        }

        if (!scrolling && elasticsearchRequestOptions != null) {
            // ignore the request options for scroll operations if there are request options already being tracked
            this.elasticsearchRequestOptions = elasticsearchRequestOptions;
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        return null;
    }

    @Override
    public IndexOperationResponse add(final IndexOperationRequest operation, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return bulk(Collections.singletonList(operation), elasticsearchRequestOptions);
    }

    @Override
    public IndexOperationResponse bulk(final List<IndexOperationRequest> operations, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        try {
            common(false, elasticsearchRequestOptions);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return new IndexOperationResponse(100L);
    }

    @Override
    public Long count(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        try {
            common(false, elasticsearchRequestOptions);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        this.query = query;
        return null;
    }

    @Override
    public DeleteOperationResponse deleteById(final String index, final String type, final String id, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        return deleteById(index, type, Collections.singletonList(id), elasticsearchRequestOptions);
    }

    @Override
    public DeleteOperationResponse deleteById(final String index, final String type, final List<String> ids, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        try {
            common(throwErrorInDelete, elasticsearchRequestOptions);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        return new DeleteOperationResponse(100L);
    }

    @Override
    public DeleteOperationResponse deleteByQuery(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        this.query = query;
        return deleteById(index, type, Collections.singletonList("1"), elasticsearchRequestOptions);
    }

    @Override
    public UpdateOperationResponse updateByQuery(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        try {
            common(throwErrorInUpdate, elasticsearchRequestOptions);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        this.query = query;
        return new UpdateOperationResponse(100L);
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
        try {
            common(throwErrorInGet || throwNotFoundInGet, elasticsearchRequestOptions);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final Map<String, Object> map = new LinkedHashMap<>(1);
        map.put("msg", "one");
        return map;
    }

    @Override
    public SearchResponse search(final String query, final String index, final String type, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        try {
            common(throwErrorInSearch, elasticsearchRequestOptions);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        if (!scrolling) {
            this.query = query;
        }
        final SearchResponse response;
        if (pageCount++ < maxPages) {
            final List<Map<String, Object>> hits = JsonUtils.readListOfMaps(HITS_RESULT);
            final Map<String, Object> aggs = returnAggs && pageCount == 1 ? JsonUtils.readMap(AGGS_RESULT) : null;
            response = new SearchResponse(hits, aggs, "pitId-" + pageCount, "scrollId-" + pageCount, "[\"searchAfter-" + pageCount + "\"]", 15, 5, false, null);
        } else {
            response = new SearchResponse(new ArrayList<>(), new LinkedHashMap<>(), "pitId-" + pageCount, "scrollId-" + pageCount, "[\"searchAfter-" + pageCount + "\"]", 0, 1, false, null);
        }

        return response;
    }

    @Override
    public SearchResponse scroll(final String scroll, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        if (throwErrorInSearch) {
            throw new RuntimeException(new IOException("Simulated IOException - scroll"));
        }

        scrolling = true;
        final SearchResponse response = search(null, null, null, elasticsearchRequestOptions);
        scrolling = false;
        return response;
    }

    @Override
    public String initialisePointInTime(final String index, final String keepAlive, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        if (throwErrorInPit) {
            throw new RuntimeException(new IOException("Simulated IOException - initialisePointInTime"));
        }

        pageCount = 0;

        return "123";
    }

    @Override
    public DeleteOperationResponse deletePointInTime(final String pitId, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        if (throwErrorInDelete) {
            throw new RuntimeException(new IOException("Simulated IOException - deletePointInTime"));
        }

        return new DeleteOperationResponse(100L);
    }

    @Override
    public DeleteOperationResponse deleteScroll(final String scrollId, final ElasticsearchRequestOptions elasticsearchRequestOptions) {
        if (throwErrorInDelete) {
            throw new RuntimeException(new IOException("Simulated IOException - deleteScroll"));
        }

        return new DeleteOperationResponse(100L);
    }

    @Override
    public String getTransitUrl(final String index, final String type) {
        return "http://localhost:9400/" + index + "/" + type;
    }

    public void setThrowNotFoundInGet(final boolean throwNotFoundInGet) {
        this.throwNotFoundInGet = throwNotFoundInGet;
    }

    public void setThrowErrorInGet(final boolean throwErrorInGet) {
        this.throwErrorInGet = throwErrorInGet;
    }

    public void setThrowErrorInSearch(final boolean throwErrorInSearch) {
        this.throwErrorInSearch = throwErrorInSearch;
    }

    public void setThrowErrorInDelete(final boolean throwErrorInDelete) {
        this.throwErrorInDelete = throwErrorInDelete;
    }

    public void setThrowErrorInPit(final boolean throwErrorInPit) {
        this.throwErrorInPit = throwErrorInPit;
    }

    public void setThrowErrorInUpdate(final boolean throwErrorInUpdate) {
        this.throwErrorInUpdate = throwErrorInUpdate;
    }

    public void resetPageCount() {
        this.pageCount = 0;
    }

    public void setMaxPages(final int maxPages) {
        this.maxPages = maxPages;
    }

    public ElasticsearchRequestOptions getElasticsearchRequestOptions() {
        return this.elasticsearchRequestOptions;
    }

    public String getQuery() {
        return query;
    }
}
