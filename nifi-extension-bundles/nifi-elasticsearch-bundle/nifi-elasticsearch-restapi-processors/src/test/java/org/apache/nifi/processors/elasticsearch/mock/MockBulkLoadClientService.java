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

import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class MockBulkLoadClientService extends AbstractMockElasticsearchClient {
    private IndexOperationResponse response;
    private Consumer<List<IndexOperationRequest>> evalConsumer;
    private Consumer<Map<String, String>> evalParametersConsumer;

    @Override
    public IndexOperationResponse bulk(final List<IndexOperationRequest> items, final Map<String, String> requestParameters) {
        if (isThrowRetryableError()) {
            throw new MockElasticsearchException(true, false);
        } else if (isThrowFatalError()) {
            throw new MockElasticsearchException(false, false);
        }

        if (evalConsumer != null) {
            evalConsumer.accept(items);
        }

        if (evalParametersConsumer != null) {
            evalParametersConsumer.accept(requestParameters);
        }

        return response;
    }

    public void setResponse(final IndexOperationResponse response) {
        this.response = response;
    }

    public void setEvalConsumer(final Consumer<List<IndexOperationRequest>> evalConsumer) {
        this.evalConsumer = evalConsumer;
    }

    public void setEvalParametersConsumer(final Consumer<Map<String, String>> evalParametersConsumer) {
        this.evalParametersConsumer = evalParametersConsumer;
    }
}
