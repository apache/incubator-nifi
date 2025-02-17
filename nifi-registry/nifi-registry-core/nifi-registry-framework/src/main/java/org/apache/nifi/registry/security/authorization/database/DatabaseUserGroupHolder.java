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

import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.User;
import org.apache.nifi.registry.security.authorization.util.UserGroupProviderUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A holder to provide atomic access to user group data structures.
 */
public class DatabaseUserGroupHolder {

    private final Set<User> allUsers;
    private final Map<String, User> usersById;
    private final Map<String, User> usersByIdentity;

    private final Set<Group> allGroups;
    private final Map<String, Group> groupsById;
    private final Map<String, Set<Group>> groupsByUserIdentity;

    /**
     * Creates a new holder and populates all convenience data structures.
     *
     * @param allUsers all users
     */
    public DatabaseUserGroupHolder(final Set<User> allUsers, final Set<Group> allGroups) {
        this.allUsers = allUsers;
        this.allGroups = allGroups;
        this.usersById = Collections.unmodifiableMap(UserGroupProviderUtils.createUserByIdMap(allUsers));
        this.usersByIdentity = Collections.unmodifiableMap(UserGroupProviderUtils.createUserByIdentityMap(allUsers));
        this.groupsById = Collections.unmodifiableMap(UserGroupProviderUtils.createGroupByIdMap(allGroups));
        this.groupsByUserIdentity = Collections.unmodifiableMap(
                UserGroupProviderUtils.createGroupsByUserIdentityMap(allGroups, allUsers));
    }

    public Set<User> getAllUsers() {
        return allUsers;
    }

    public Map<String, User> getUsersById() {
        return usersById;
    }

    public Map<String, User> getUsersByIdentity() {
        return usersByIdentity;
    }

    public Set<Group> getAllGroups() {
        return allGroups;
    }

    public Map<String, Group> getGroupsById() {
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
