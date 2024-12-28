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

public class FlowFileFilters {

    /**
     * Returns a new {@link FlowFileFilter} that will pull FlowFiles until the
     * maximum file size has been reached, or the maximum FlowFile Count was
     * been reached (this is important because FlowFiles may be 0 bytes!). If
     * the first FlowFile exceeds the max size, the FlowFile will be selected
     * and no other FlowFile will be.
     *
     * @param maxSize the maximum size of the group of FlowFiles
     * @param unit the unit of the <code>maxSize</code> argument
     * @param maxCount the maximum number of FlowFiles to pull
     * @return filter
     */
    public static FlowFileFilter newSizeBasedFilter(final double maxSize, final DataUnit unit, final int maxCount) {
        final double maxBytes = DataUnit.B.convert(maxSize, unit);

        return new FlowFileFilter() {
            int count = 0;
            long size = 0L;

            @Override
            public FlowFileFilterResult filter(final FlowFile flowFile) {
                count += 1;
                size += flowFile.getSize();

                if (count == 1) {
                    // first FlowFile is always accepted
                    return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                }

                if (size > maxBytes || count > maxCount) {
                    return FlowFileFilterResult.REJECT_AND_TERMINATE;
                }

                return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
            }
        };
    }

}
