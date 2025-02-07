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
package org.apache.nifi.registry.security.authorization.file;


import org.apache.nifi.registry.security.authorization.file.generated.Authorizations;
import org.apache.nifi.registry.security.authorization.file.generated.Policies;
import org.apache.nifi.registry.security.authorization.AccessPolicy;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.util.AccessPolicyProviderUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A holder to provide atomic access to data structures.
 */
public class AuthorizationsHolder {

    private final Authorizations authorizations;

    private final Set<AccessPolicy> allPolicies;
    private final Map<String, Set<AccessPolicy>> policiesByResource;
    private final Map<String, AccessPolicy> policiesById;

    /**
     * Creates a new holder and populates all convenience authorizations data structures.
     *
     * @param authorizations the current authorizations instance
     */
    public AuthorizationsHolder(final Authorizations authorizations) {
        this.authorizations = authorizations;

        // load all access policies
        final Policies policies = authorizations.getPolicies();
        final Set<AccessPolicy> allPolicies = Collections.unmodifiableSet(createAccessPolicies(policies));

        // create a convenience map from resource id to policies
        final Map<String, Set<AccessPolicy>> policiesByResourceMap = Collections.unmodifiableMap(AccessPolicyProviderUtils.createResourcePolicyMap(allPolicies));

        // create a convenience map from policy id to policy
        final Map<String, AccessPolicy> policiesByIdMap = Collections.unmodifiableMap(AccessPolicyProviderUtils.createPoliciesByIdMap(allPolicies));

        // set all the holders
        this.allPolicies = allPolicies;
        this.policiesByResource = policiesByResourceMap;
        this.policiesById = policiesByIdMap;
    }

    /**
     * Creates AccessPolicies from the JAXB Policies.
     *
     * @param policies the JAXB Policies element
     * @return a set of AccessPolicies corresponding to the provided Resources
     */
    private Set<AccessPolicy> createAccessPolicies(org.apache.nifi.registry.security.authorization.file.generated.Policies policies) {
        Set<AccessPolicy> allPolicies = new HashSet<>();
        if (policies == null || policies.getPolicy() == null) {
            return allPolicies;
        }

        // load the new authorizations
        for (final org.apache.nifi.registry.security.authorization.file.generated.Policy policy : policies.getPolicy()) {
            final String policyIdentifier = policy.getIdentifier();
            final String resourceIdentifier = policy.getResource();

            // start a new builder and set the policy and resource identifiers
            final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                    .identifier(policyIdentifier)
                    .resource(resourceIdentifier);

            // add each user identifier
            for (org.apache.nifi.registry.security.authorization.file.generated.Policy.User user : policy.getUser()) {
                builder.addUser(user.getIdentifier());
            }

            // add each group identifier
            for (org.apache.nifi.registry.security.authorization.file.generated.Policy.Group group : policy.getGroup()) {
                builder.addGroup(group.getIdentifier());
            }

            // add the appropriate request actions
            final String authorizationCode = policy.getAction();
            if (authorizationCode.equals(FileAccessPolicyProvider.READ_CODE)) {
                builder.action(RequestAction.READ);
            } else if (authorizationCode.equals(FileAccessPolicyProvider.WRITE_CODE)) {
                builder.action(RequestAction.WRITE);
            } else if (authorizationCode.equals(FileAccessPolicyProvider.DELETE_CODE)) {
                builder.action(RequestAction.DELETE);
            } else {
                throw new IllegalStateException("Unknown Policy Action: " + authorizationCode);
            }

            // build the policy and add it to the map
            allPolicies.add(builder.build());
        }

        return allPolicies;
    }

    public Authorizations getAuthorizations() {
        return authorizations;
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
