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

package org.apache.nifi.groups;

/**
 * Specifies the concurrency level of a Process Group
 */
public enum FlowFileConcurrency {

    /**
     * Only a single FlowFile is to be allowed to enter the Process Group at a time.
     * While that FlowFile may be split into many or spawn many children, no additional FlowFiles will be
     * allowed to enter the Process Group through a Local Input Port until the previous FlowFile - and all of its
     * child/descendent FlowFiles - have been processed. In a clustered instance, each node may allow through
     * a single FlowFile at a time, so multiple FlowFiles may still be processed concurrently across the cluster.
     */
    SINGLE_FLOWFILE_PER_NODE,

    /**
     * The number of FlowFiles that can be processed concurrently is unbounded.
     */
    UNBOUNDED;

}
