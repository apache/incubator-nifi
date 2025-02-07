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
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.User;
import org.apache.nifi.registry.security.identity.IdentityMapper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods related to access policies for use by various {@link org.apache.nifi.registry.security.authorization.UserGroupProvider} implementations.
 */
public final class UserGroupProviderUtils {

    public static final String PROP_INITIAL_USER_IDENTITY_PREFIX = "Initial User Identity ";
    public static final Pattern INITIAL_USER_IDENTITY_PATTERN = Pattern.compile(PROP_INITIAL_USER_IDENTITY_PREFIX + "\\S+");

    public static Set<String> getInitialUserIdentities(final AuthorizerConfigurationContext configurationContext, final IdentityMapper identityMapper) {
        final Set<String> initialUserIdentities = new HashSet<>();
        for (Map.Entry<String, String> entry : configurationContext.getProperties().entrySet()) {
            Matcher matcher = UserGroupProviderUtils.INITIAL_USER_IDENTITY_PATTERN.matcher(entry.getKey());
            if (matcher.matches() && !StringUtils.isBlank(entry.getValue())) {
                initialUserIdentities.add(identityMapper.mapUser(entry.getValue()));
            }
        }
        return initialUserIdentities;
    }

    /**
     * Creates a Map from user identifier to User.
     *
     * @param users the set of all users
     * @return the Map from user identifier to User
     */
    public static Map<String, User> createUserByIdMap(final Set<User> users) {
        Map<String, User> usersMap = new HashMap<>();
        for (User user : users) {
            usersMap.put(user.getIdentifier(), user);
        }
        return usersMap;
    }

    /**
     * Creates a Map from user identity to User.
     *
     * @param users the set of all users
     * @return the map from user identity to User
     */
    public static Map<String, User> createUserByIdentityMap(final Set<User> users) {
        Map<String, User> usersMap = new HashMap<>();
        for (User user : users) {
            usersMap.put(user.getIdentity(), user);
        }
        return usersMap;
    }

    /**
     * Creates a Map from group identifier to Group.
     *
     * @param groups the set of all groups
     * @return the map from group identifier to Group
     */
    public static Map<String, Group> createGroupByIdMap(final Set<Group> groups) {
        Map<String, Group> groupsMap = new HashMap<>();
        for (Group group : groups) {
            groupsMap.put(group.getIdentifier(), group);
        }
        return groupsMap;
    }

    /**
     * Creates a Map from user identity to the set of Groups for that identity.
     *
     * @param groups all groups
     * @param users all users
     * @return a Map from User identity to the set of Groups for that identity
     */
    public static Map<String, Set<Group>> createGroupsByUserIdentityMap(final Set<Group> groups, final Set<User> users) {
        Map<String, Set<Group>> groupsByUserIdentity = new HashMap<>();

        for (User user : users) {
            Set<Group> userGroups = new HashSet<>();
            for (Group group : groups) {
                for (String groupUser : group.getUsers()) {
                    if (groupUser.equals(user.getIdentifier())) {
                        userGroups.add(group);
                    }
                }
            }

            groupsByUserIdentity.put(user.getIdentity(), userGroups);
        }

        return groupsByUserIdentity;
    }

    private UserGroupProviderUtils() {

    }
}
