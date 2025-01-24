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

import java.util.HashMap;
import java.util.Map;

public class ElasticsearchRequestOptions {
    private final Map<String, String> requestParameters;
    private final Map<String, String> requestHeaders;

    public ElasticsearchRequestOptions(final Map<String, String> requestParameters, final Map<String, String> requestHeaders) {
        this.requestParameters = requestParameters == null ? new HashMap<>() : requestParameters;
        this.requestHeaders = requestHeaders == null ? new HashMap<>() : requestHeaders;
    }

    public ElasticsearchRequestOptions() {
        this(new HashMap<>(), new HashMap<>());
    }

    public Map<String, String> getRequestParameters() {
        return requestParameters;
    }

    public Map<String, String> getRequestHeaders() {
        return requestHeaders;
    }
}
