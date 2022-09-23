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
package org.apache.nifi.processors.asana.mocks;

import com.asana.models.Resource;
import org.apache.nifi.processors.asana.utils.GenericAsanaObjectFetcher;

import java.util.Collection;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toMap;

public class MockGenericAsanaObjectFetcher extends GenericAsanaObjectFetcher<Resource> {

    public Collection<Resource> items = emptyList();
    public int refreshCount = 0;

    @Override
    protected Map<String, Resource> refreshObjects() {
        refreshCount++;
        return items.stream().collect(toMap(item -> item.gid, item -> item));
    }
}
