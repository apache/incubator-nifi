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
package org.apache.nifi.registry.security.authorization.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.security.authorization.AccessPolicy;
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.UserGroupProvider;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.identity.IdentityMapper;
import org.apache.nifi.registry.util.PropertyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods related to access policies for use by various {@link org.apache.nifi.registry.security.authorization.AccessPolicyProvider} implementations.
 */
public final class AccessPolicyProviderUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccessPolicyProviderUtils.class);

    /**
     * The prefix of a property from an AuthorizerConfigurationContext that specifies a NiFi Identity.
     */
    public static final String PROP_NIFI_IDENTITY_PREFIX = "NiFi Identity ";

    /**
     * The name of the property from an AuthorizerConfigurationContext that specifies the initial admin identity.
     */
    public static final String PROP_INITIAL_ADMIN_IDENTITY = "Initial Admin Identity";

    /**
     * A Pattern for identifying properties that represent NiFi Identities.
     */
    public static final Pattern NIFI_IDENTITY_PATTERN = Pattern.compile(PROP_NIFI_IDENTITY_PREFIX + "\\S+");

    /**
     * The name of the property from AuthorizerConfigurationContext that specifies a name of a group for NiFi Identities.
     */
    public static final String PROP_NIFI_GROUP_NAME = "NiFi Group Name";

    /**
     * Returns the value of the 'Initial Admin Identity' property with any identity mappings applied.
     *
     * @param configurationContext the configuration context
     * @param identityMapper the identity mapper
     * @return the value for the initial admin identity
     */
    public static String getInitialAdminIdentity(final AuthorizerConfigurationContext configurationContext, final IdentityMapper identityMapper) {
        final PropertyValue initialAdminIdentityProp = configurationContext.getProperty(PROP_INITIAL_ADMIN_IDENTITY);
        return initialAdminIdentityProp.isSet() ? identityMapper.mapUser(initialAdminIdentityProp.getValue()) : null;
    }

    /**
     * Returns the values for the 'NiFi Identity' properties with any identity mappings applied.
     *
     * @param configurationContext the configuration context
     * @param identityMapper the identity mapper
     * @return the values for the NiFi identities
     */
    public static Set<String> getNiFiIdentities(final AuthorizerConfigurationContext configurationContext, final IdentityMapper identityMapper) {
        final Set<String> nifiIdentities = new HashSet<>();

        for (final Map.Entry<String, String> entry : configurationContext.getProperties().entrySet()) {
            final Matcher matcher = NIFI_IDENTITY_PATTERN.matcher(entry.getKey());
            if (matcher.matches() && !StringUtils.isBlank(entry.getValue())) {
                nifiIdentities.add(identityMapper.mapUser(entry.getValue()));
            }
        }

        return nifiIdentities;
    }

    /**
     * Returns the value for the property 'NiFi Group Name' from the given configuration context.
     *
     * @param configurationContext the configuration context
     * @return the group name, or null if not specified
     */
    public static String getNiFiGroupName(final AuthorizerConfigurationContext configurationContext, final IdentityMapper identityMapper) {
        final PropertyValue nifiGroupNameProp = configurationContext.getProperty(PROP_NIFI_GROUP_NAME);
        final String nifiGroupName = (nifiGroupNameProp != null && nifiGroupNameProp.isSet()) ? nifiGroupNameProp.getValue() : null;

        if (StringUtils.isBlank(nifiGroupName)) {
            LOGGER.debug("NiFi Group Name was not specified");
            return null;
        }

        return identityMapper.mapGroup(nifiGroupName);
    }

    /**
     * Returns the identifier of the group with the given group name.
     *
     * If no group exists with the given name then SecurityProviderCreationException is thrown.
     *
     * @param groupName the group name
     * @param userGroupProvider the UserGroupProvider
     * @return the identifier of the group, or null
     */
    public static Group getGroup(final String groupName, final UserGroupProvider userGroupProvider) {
        final Set<Group> groups = userGroupProvider.getGroups();
        LOGGER.trace("All groups: {}", groups);

        final Optional<Group> groupOptional = groups.stream()
                .filter(group -> group.getName().equals(groupName))
                .findFirst();

        final Group group = groupOptional.orElseThrow(() ->
                        new SecurityProviderCreationException(
                                String.format("Group '%s' could not be found", groupName))
        );

        LOGGER.debug("Group identifier is: {}", group);
        return group;
    }

    /**
     * Creates a Map from resource identifier to the set of policies for the given resource.
     *
     * @param allPolicies the set of all policies
     * @return a map from resource identifier to policies
     */
    public static Map<String, Set<AccessPolicy>> createResourcePolicyMap(final Set<AccessPolicy> allPolicies) {
        Map<String, Set<AccessPolicy>> resourcePolicies = new HashMap<>();

        for (AccessPolicy policy : allPolicies) {
            Set<AccessPolicy> policies = resourcePolicies.computeIfAbsent(policy.getResource(), k -> new HashSet<>());
            policies.add(policy);
        }

        return resourcePolicies;
    }

    /**
     * Creates a Map from policy identifier to AccessPolicy.
     *
     * @param policies the set of all access policies
     * @return the Map from policy identifier to AccessPolicy
     */
    public static Map<String, AccessPolicy> createPoliciesByIdMap(final Set<AccessPolicy> policies) {
        Map<String, AccessPolicy> policyMap = new HashMap<>();
        for (AccessPolicy policy : policies) {
            policyMap.put(policy.getIdentifier(), policy);
        }
        return policyMap;
    }

    /**
     * Returns the access policy by resource identifier and request action from the map of policies
     *
     * @param resourceIdentifier the resource identifier
     * @param action the request action
     * @param policiesByResource the map from resource identifier to policies
     * @return the access policy
     */
    public static AccessPolicy getAccessPolicy(
            final String resourceIdentifier, final RequestAction action, final Map<String, Set<AccessPolicy>> policiesByResource) {
        if (resourceIdentifier == null) {
            throw new IllegalArgumentException("Resource Identifier cannot be null");
        }

        final Set<AccessPolicy> resourcePolicies = policiesByResource.get(resourceIdentifier);
        if (resourcePolicies == null) {
            return null;
        }

        for (AccessPolicy accessPolicy : resourcePolicies) {
            if (accessPolicy.getAction() == action) {
                return accessPolicy;
            }
        }

        return null;
    }

    private AccessPolicyProviderUtils() {

    }

}
