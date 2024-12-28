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
package org.apache.nifi.processor.util;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.FlowFileFilter;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.REJECT_AND_TERMINATE;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FlowFileFiltersTest {

    @Nested
    class SizeBasedFilter {
        @Test
        void acceptsOnlyFirstFlowFileWhenMaxCountIs0() {
            final FlowFileFilter filter = FlowFileFilters.newSizeBasedFilter(1, DataUnit.MB, 0);

            assertEquals(ACCEPT_AND_CONTINUE, filter.filter(emptyFlowFile()));
            assertEquals(REJECT_AND_TERMINATE, filter.filter(emptyFlowFile()));
        }

        @Test
        void rejectsFlowFilesOnceTheMaxCountWasReached() {
            final FlowFileFilter filter = FlowFileFilters.newSizeBasedFilter(1, DataUnit.MB, 3);

            assertEquals(ACCEPT_AND_CONTINUE, filter.filter(emptyFlowFile()));
            assertEquals(ACCEPT_AND_CONTINUE, filter.filter(emptyFlowFile()));
            assertEquals(ACCEPT_AND_CONTINUE, filter.filter(emptyFlowFile()));

            assertEquals(REJECT_AND_TERMINATE, filter.filter(emptyFlowFile()));
        }

        @Test
        void acceptsOnlyFirstFlowFileWhenItsContentSizeExceedsTheMaxSize() {
            final FlowFileFilter filter = FlowFileFilters.newSizeBasedFilter(10, DataUnit.B, 10_000);

            assertEquals(ACCEPT_AND_CONTINUE, filter.filter(flowFileOfSize(20)));
            assertEquals(REJECT_AND_TERMINATE, filter.filter(emptyFlowFile()));
        }

        @Test
        void rejectsFlowFilesOnceMaxSizeWasReached() {
            final FlowFileFilter filter = FlowFileFilters.newSizeBasedFilter(30, DataUnit.B, 10_000);

            assertEquals(ACCEPT_AND_CONTINUE, filter.filter(flowFileOfSize(5)));
            assertEquals(ACCEPT_AND_CONTINUE, filter.filter(flowFileOfSize(10)));
            assertEquals(ACCEPT_AND_CONTINUE, filter.filter(flowFileOfSize(15)));

            // empty content FlowFiles are still accepted
            assertEquals(ACCEPT_AND_CONTINUE, filter.filter(emptyFlowFile()));

            assertEquals(REJECT_AND_TERMINATE, filter.filter(flowFileOfSize(1)));
            // empty content FlowFiles are no longer accepted
            assertEquals(REJECT_AND_TERMINATE, filter.filter(emptyFlowFile()));
        }

        private FlowFile emptyFlowFile() {
            return flowFileOfSize(0);
        }

        private FlowFile flowFileOfSize(final long byteSize)  {
            final int id = claimFlowFileId();

            return new FlowFile() {
                @Override
                public long getId() {
                    return id;
                }

                @Override
                public long getEntryDate() {
                    return 0;
                }

                @Override
                public long getLineageStartDate() {
                    return 0;
                }

                @Override
                public long getLineageStartIndex() {
                    return 0;
                }

                @Override
                public Long getLastQueueDate() {
                    return 0L;
                }

                @Override
                public long getQueueDateIndex() {
                    return 0;
                }

                @Override
                public boolean isPenalized() {
                    return false;
                }

                @Override
                public String getAttribute(String key) {
                    return null;
                }

                @Override
                public long getSize() {
                    return byteSize;
                }

                @Override
                public Map<String, String> getAttributes() {
                    return Map.of();
                }

                @Override
                public int compareTo(FlowFile o) {
                    return 0;
                }
            };
        }
    }

    private int flowFileId = 0;

    private int claimFlowFileId() {
        return flowFileId++;
    }
}