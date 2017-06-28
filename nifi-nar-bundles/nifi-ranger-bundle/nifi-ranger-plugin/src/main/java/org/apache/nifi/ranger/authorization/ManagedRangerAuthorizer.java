/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.ranger.authorization;

import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.AccessPolicyProvider;
import org.apache.nifi.authorization.AccessPolicyProviderInitializationContext;
import org.apache.nifi.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.authorization.AuthorizerInitializationContext;
import org.apache.nifi.authorization.ManagedAuthorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.UserGroupProvider;
import org.apache.nifi.authorization.UserGroupProviderLookup;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.exception.UninheritableAuthorizationsException;

import java.util.Set;

public class ManagedRangerAuthorizer extends RangerNiFiAuthorizer implements ManagedAuthorizer {

    private static final String MANAGED_RANGER_FINGERPRINT = "managed-ranger-fingerprint";

    private UserGroupProviderLookup userGroupProviderLookup;
    private UserGroupProvider userGroupProvider;
    private RangerBasePluginWithPolicies nifiPlugin;

    @Override
    public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
        userGroupProviderLookup = initializationContext.getUserGroupProviderLookup();

        super.initialize(initializationContext);
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
        final String userGroupProviderKey = configurationContext.getProperty("User Group Provider").getValue();
        userGroupProvider = userGroupProviderLookup.getUserGroupProvider(userGroupProviderKey);

        // ensure the desired access policy provider has a user group provider
        if (userGroupProvider == null) {
            throw new AuthorizerCreationException(String.format("Unable to locate configured User Group Provider: %s", userGroupProviderKey));
        }

        super.onConfigured(configurationContext);
    }

    @Override
    protected RangerBasePluginWithPolicies createRangerBasePlugin(final String serviceType, final String appId) {
        // override the method for creating the ranger base plugin so a user group provider can be specified
        nifiPlugin = new RangerBasePluginWithPolicies(serviceType, appId, userGroupProvider);
        return nifiPlugin;
    }

    @Override
    public AccessPolicyProvider getAccessPolicyProvider() {
        return new AccessPolicyProvider() {
            @Override
            public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
                return nifiPlugin.getAccessPolicies();
            }

            @Override
            public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
                return nifiPlugin.getAccessPolicy(identifier);
            }

            @Override
            public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
                return nifiPlugin.getAccessPolicy(resourceIdentifier, action);
            }

            @Override
            public UserGroupProvider getUserGroupProvider() {
                return userGroupProvider;
            }

            @Override
            public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws AuthorizerCreationException {
            }

            @Override
            public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
            }

            @Override
            public void preDestruction() throws AuthorizerDestructionException {
            }
        };
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        return MANAGED_RANGER_FINGERPRINT;
    }

    @Override
    public void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
    }

    @Override
    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
    }
}
