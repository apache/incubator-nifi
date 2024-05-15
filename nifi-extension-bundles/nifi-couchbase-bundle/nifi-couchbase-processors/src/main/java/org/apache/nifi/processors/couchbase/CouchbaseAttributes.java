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
package org.apache.nifi.processors.couchbase;

import org.apache.nifi.flowfile.attributes.FlowFileAttributeKey;

/**
 * Couchbase related attribute keys.
 */
public enum CouchbaseAttributes implements FlowFileAttributeKey {

    /**
     * A reference to the related cluster.
     */
    Cluster("couchbase.cluster"),
    /**
     * A related bucket name.
     */
    Bucket("couchbase.bucket"),
    /**
     * A related collection name.
     */
    Collection("couchbase.collection"),
    /**
     * The id of a related document.
     */
    DocId("couchbase.doc.id"),
    /**
     * The CAS value of a related document.
     */
    Cas("couchbase.doc.cas"),
    /**
     * The expiration of a related document.
     */
    Expiry("couchbase.doc.expiry"),
    /**
     * The thrown CouchbaseException class.
     */
    Exception("couchbase.exception"),
    ;

    private final String key;

    private CouchbaseAttributes(final String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }

}
