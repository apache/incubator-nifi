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
package org.apache.nifi.registry.security.authorization.database;

import org.apache.nifi.registry.security.authorization.AccessPolicy;
import org.apache.nifi.registry.security.authorization.util.AccessPolicyProviderUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A holder to provide atomic access to data structures.
 */
public class DatabaseAccessPolicyHolder {

    private final Set<AccessPolicy> allPolicies;
    private final Map<String, Set<AccessPolicy>> policiesByResource;
    private final Map<String, AccessPolicy> policiesById;

    /**
     * Creates a new holder and populates all convenience access policies data structures.
     *
     * @param allPolicies all access policies
     */
    public DatabaseAccessPolicyHolder(final Set<AccessPolicy> allPolicies) {
        this.allPolicies = allPolicies;
        this.policiesByResource = Collections.unmodifiableMap(AccessPolicyProviderUtils.createResourcePolicyMap(allPolicies));
        this.policiesById = Collections.unmodifiableMap(AccessPolicyProviderUtils.createPoliciesByIdMap(allPolicies));
    }

    public Set<AccessPolicy> getAllPolicies() {
        return allPolicies;
    }

    public Map<String, Set<AccessPolicy>> getPoliciesByResource() {
        return policiesByResource;
    }

    public Map<String, AccessPolicy> getPoliciesById() {
        return policiesById;
    }

}
