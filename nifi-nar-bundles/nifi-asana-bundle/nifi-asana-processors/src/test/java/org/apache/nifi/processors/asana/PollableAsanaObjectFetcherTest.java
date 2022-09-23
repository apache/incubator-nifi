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
package org.apache.nifi.processors.asana;

import com.google.common.collect.Lists;
import org.apache.nifi.processors.asana.mocks.MockPollableAsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PollableAsanaObjectFetcherTest {

    @Test
    public void testFetchNext() {
        final MockPollableAsanaObjectFetcher fetcher = new MockPollableAsanaObjectFetcher();

        fetcher.items = emptyList();
        assertNull(fetcher.fetchNext());
        assertEquals(1, fetcher.pollCount);
        assertNull(fetcher.fetchNext());
        assertEquals(2, fetcher.pollCount);

        final AsanaObject oneObject = new AsanaObject(AsanaObjectState.NEW, "1234");
        final AsanaObject otherObject = new AsanaObject(AsanaObjectState.REMOVED, "1234");

        fetcher.items = singletonList(oneObject);
        assertEquals(oneObject, fetcher.fetchNext());
        assertEquals(3, fetcher.pollCount);
        assertNull(fetcher.fetchNext());
        assertEquals(4, fetcher.pollCount);
        assertNull(fetcher.fetchNext());
        assertEquals(5, fetcher.pollCount);

        fetcher.items = Lists.newArrayList(oneObject, otherObject);
        assertEquals(oneObject, fetcher.fetchNext());
        assertEquals(6, fetcher.pollCount);
        assertEquals(otherObject, fetcher.fetchNext());
        assertEquals(6, fetcher.pollCount);
        assertNull(fetcher.fetchNext());
        assertEquals(7, fetcher.pollCount);
        assertNull(fetcher.fetchNext());
        assertEquals(8, fetcher.pollCount);

        fetcher.items = Lists.newArrayList(oneObject, otherObject);
        assertEquals(oneObject, fetcher.fetchNext());
        assertEquals(9, fetcher.pollCount);
        fetcher.items = Lists.newArrayList(oneObject, otherObject);
        assertEquals(otherObject, fetcher.fetchNext());
        assertEquals(9, fetcher.pollCount);
        assertEquals(oneObject, fetcher.fetchNext());
        assertEquals(10, fetcher.pollCount);
        assertEquals(otherObject, fetcher.fetchNext());
        assertEquals(10, fetcher.pollCount);
        fetcher.items = singletonList(oneObject);
        assertEquals(oneObject, fetcher.fetchNext());
        assertEquals(11, fetcher.pollCount);
        assertNull(fetcher.fetchNext());
        assertEquals(12, fetcher.pollCount);
        assertNull(fetcher.fetchNext());
        assertEquals(13, fetcher.pollCount);
    }
}
