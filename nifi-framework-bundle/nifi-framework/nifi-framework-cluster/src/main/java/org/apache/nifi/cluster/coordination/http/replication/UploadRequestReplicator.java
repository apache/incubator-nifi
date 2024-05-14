/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.cluster.coordination.http.replication;

import java.io.IOException;

/**
 * A request replicator for replicating file uploads to all nodes in the cluster.
 */
public interface UploadRequestReplicator {

    String FILENAME_HEADER = "Filename";
    String CONTENT_TYPE_HEADER = "Content-Type";
    String UPLOAD_CONTENT_TYPE = "application/octet-stream";

    /**
     * Replicates the request to upload a file to all nodes in the cluster.
     *
     * @param uploadRequest the upload request
     * @return the replication response
     *
     * @param <T> the type of the response
     * @throws IOException if error occurs during replication
     */
    <T> T upload(UploadRequest<T> uploadRequest) throws IOException;

}
