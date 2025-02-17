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


import org.apache.nifi.registry.security.authorization.file.tenants.generated.Groups;
import org.apache.nifi.registry.security.authorization.file.tenants.generated.Tenants;
import org.apache.nifi.registry.security.authorization.file.tenants.generated.Users;
import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.User;
import org.apache.nifi.registry.security.authorization.util.UserGroupProviderUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A holder to provide atomic access to user group data structures.
 */
public class UserGroupHolder {

    private final Tenants tenants;

    private final Set<User> allUsers;
    private final Map<String, User> usersById;
    private final Map<String, User> usersByIdentity;

    private final Set<Group> allGroups;
    private final Map<String, Group> groupsById;
    private final Map<String, Set<Group>> groupsByUserIdentity;

    /**
     * Creates a new holder and populates all convenience data structures.
     *
     * @param tenants the current tenants instance
     */
    public UserGroupHolder(final Tenants tenants) {
        this.tenants = tenants;

        // load all users
        final Users users = tenants.getUsers();
        final Set<User> allUsers = Collections.unmodifiableSet(createUsers(users));

        // load all groups
        final Groups groups = tenants.getGroups();
        final Set<Group> allGroups = Collections.unmodifiableSet(createGroups(groups, users));

        // create a convenience map to retrieve a user by id
        final Map<String, User> userByIdMap = Collections.unmodifiableMap(
                UserGroupProviderUtils.createUserByIdMap(allUsers));

        // create a convenience map to retrieve a user by identity
        final Map<String, User> userByIdentityMap = Collections.unmodifiableMap(
                UserGroupProviderUtils.createUserByIdentityMap(allUsers));

        // create a convenience map to retrieve a group by id
        final Map<String, Group> groupByIdMap = Collections.unmodifiableMap(
                UserGroupProviderUtils.createGroupByIdMap(allGroups));

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
     * Creates a set of Users from the JAXB Users.
     *
     * @param users the JAXB Users
     * @return a set of API Users matching the provided JAXB Users
     */
    private Set<User> createUsers(Users users) {
        Set<User> allUsers = new HashSet<>();
        if (users == null || users.getUser() == null) {
            return allUsers;
        }

        for (org.apache.nifi.registry.security.authorization.file.tenants.generated.User user : users.getUser()) {
            final User.Builder builder = new User.Builder()
                    .identity(user.getIdentity())
                    .identifier(user.getIdentifier());

            allUsers.add(builder.build());
        }

        return allUsers;
    }

    /**
     * Creates a set of Groups from the JAXB Groups.
     *
     * @param groups the JAXB Groups
     * @return a set of API Groups matching the provided JAXB Groups
     */
    private Set<Group> createGroups(Groups groups,
                                    Users users) {
        Set<Group> allGroups = new HashSet<>();
        if (groups == null || groups.getGroup() == null) {
            return allGroups;
        }

        for (org.apache.nifi.registry.security.authorization.file.tenants.generated.Group group : groups.getGroup()) {
            final Group.Builder builder = new Group.Builder()
                    .identifier(group.getIdentifier())
                    .name(group.getName());

            for (org.apache.nifi.registry.security.authorization.file.tenants.generated.Group.User groupUser : group.getUser()) {
                builder.addUser(groupUser.getIdentifier());
            }

            allGroups.add(builder.build());
        }

        return allGroups;
    }

    public Tenants getTenants() {
        return tenants;
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
