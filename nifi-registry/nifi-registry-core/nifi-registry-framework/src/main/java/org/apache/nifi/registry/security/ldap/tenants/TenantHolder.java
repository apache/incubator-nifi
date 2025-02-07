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
package org.apache.nifi.registry.security.ldap.tenants;


import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.User;
import org.apache.nifi.registry.security.authorization.util.UserGroupProviderUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A holder to provide atomic access to user group data structures.
 */
public class TenantHolder {

    private final Set<User> allUsers;
    private final Map<String, User> usersById;
    private final Map<String, User> usersByIdentity;

    private final Set<Group> allGroups;
    private final Map<String, Group> groupsById;
    private final Map<String, Set<Group>> groupsByUserIdentity;

    /**
     * Creates a new holder and populates all convenience data structures.
     */
    public TenantHolder(final Set<User> allUsers, final Set<Group> allGroups) {
        // create a convenience map to retrieve a user by id
        final Map<String, User> userByIdMap = Collections.unmodifiableMap(createUserByIdMap(allUsers));

        // create a convenience map to retrieve a user by identity
        final Map<String, User> userByIdentityMap = Collections.unmodifiableMap(createUserByIdentityMap(allUsers));

        // create a convenience map to retrieve a group by id
        final Map<String, Group> groupByIdMap = Collections.unmodifiableMap(createGroupByIdMap(allGroups));

        // create a convenience map to retrieve the groups for a user identity
        final Map<String, Set<Group>> groupsByUserIdentityMap = Collections.unmodifiableMap(
                UserGroupProviderUtils.createGroupsByUserIdentityMap(allGroups, allUsers));

        // set all the holders
        this.allUsers = allUsers;
        this.allGroups = allGroups;
        this.usersById = userByIdMap;
        this.usersByIdentity = userByIdentityMap;
        this.groupsById = groupByIdMap;
        this.groupsByUserIdentity = groupsByUserIdentityMap;
    }

    /**
     * Creates a Map from user identifier to User.
     *
     * @param users the set of all users
     * @return the Map from user identifier to User
     */
    private Map<String, User> createUserByIdMap(final Set<User> users) {
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
     * @return the Map from user identity to User
     */
    private Map<String, User> createUserByIdentityMap(final Set<User> users) {
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
     * @return the Map from group identifier to Group
     */
    private Map<String, Group> createGroupByIdMap(final Set<Group> groups) {
        Map<String, Group> groupsMap = new HashMap<>();
        for (Group group : groups) {
            groupsMap.put(group.getIdentifier(), group);
        }
        return groupsMap;
    }

    Set<User> getAllUsers() {
        return allUsers;
    }

    Map<String, User> getUsersById() {
        return usersById;
    }

    Set<Group> getAllGroups() {
        return allGroups;
    }

    Map<String, Group> getGroupsById() {
        return groupsById;
    }

    public User getUser(String identity) {
        if (identity == null) {
            throw new IllegalArgumentException("Identity cannot be null");
        }
        return usersByIdentity.get(identity);
    }

    public Set<Group> getGroups(String userIdentity) {
        if (userIdentity == null) {
            throw new IllegalArgumentException("User Identity cannot be null");
        }
        return groupsByUserIdentity.get(userIdentity);
    }

}
