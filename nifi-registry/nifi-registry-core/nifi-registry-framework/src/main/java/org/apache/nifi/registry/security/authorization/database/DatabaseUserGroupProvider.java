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

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.authorization.ConfigurableUserGroupProvider;
import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.User;
import org.apache.nifi.registry.security.authorization.UserAndGroups;
import org.apache.nifi.registry.security.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.registry.security.authorization.annotation.AuthorizerContext;
import org.apache.nifi.registry.security.authorization.database.entity.DatabaseGroup;
import org.apache.nifi.registry.security.authorization.database.entity.DatabaseUser;
import org.apache.nifi.registry.security.authorization.database.mapper.DatabaseGroupRowMapper;
import org.apache.nifi.registry.security.authorization.database.mapper.DatabaseUserRowMapper;
import org.apache.nifi.registry.security.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.registry.security.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.registry.security.authorization.util.UserGroupProviderUtils;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.exception.SecurityProviderDestructionException;
import org.apache.nifi.registry.security.identity.IdentityMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link org.apache.nifi.registry.security.authorization.ConfigurableUserGroupProvider} backed by a relational database.
 */
public class DatabaseUserGroupProvider implements ConfigurableUserGroupProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseUserGroupProvider.class);

    private DataSource dataSource;
    private IdentityMapper identityMapper;

    private JdbcTemplate jdbcTemplate;

    private final AtomicReference<DatabaseUserGroupHolder> userGroupHolder = new AtomicReference<>();

    @AuthorizerContext
    public void setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @AuthorizerContext
    public void setIdentityMapper(final IdentityMapper identityMapper) {
        this.identityMapper = identityMapper;
    }

    @Override
    public void initialize(final UserGroupProviderInitializationContext initializationContext) throws SecurityProviderCreationException {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public void onConfigured(final AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
        refreshUserGroupHolder();

        final Set<String> initialUserIdentities = UserGroupProviderUtils.getInitialUserIdentities(configurationContext, identityMapper);

        for (final String initialUserIdentity : initialUserIdentities) {
            final User existingUser = getUserByIdentity(initialUserIdentity);
            if (existingUser == null) {
                final User initialUser = new User.Builder()
                        .identifierGenerateFromSeed(initialUserIdentity)
                        .identity(initialUserIdentity)
                        .build();
                addUser(initialUser);
                LOGGER.info("Created initial user with identity {}", initialUserIdentity);
            } else {
                LOGGER.debug("User already exists with identity {}", initialUserIdentity);
            }
        }
    }

    @Override
    public void preDestruction() throws SecurityProviderDestructionException {

    }

    //-- fingerprint methods

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        throw new UnsupportedOperationException("Fingerprinting is not supported by this provider");
    }

    @Override
    public void inheritFingerprint(final String fingerprint) throws AuthorizationAccessException {
        throw new UnsupportedOperationException("Fingerprinting is not supported by this provider");
    }

    @Override
    public void checkInheritability(final String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
        throw new UnsupportedOperationException("Fingerprinting is not supported by this provider");
    }

    //-- User CRUD

    @Override
    public synchronized User addUser(final User user) throws AuthorizationAccessException {
        Objects.requireNonNull(user);
        final String sql = "INSERT INTO UGP_USER(IDENTIFIER, IDENTITY) VALUES (?, ?)";
        jdbcTemplate.update(sql, user.getIdentifier(), user.getIdentity());

        refreshUserGroupHolder();

        return user;
    }

    @Override
    public synchronized User updateUser(final User user) throws AuthorizationAccessException {
        Objects.requireNonNull(user);

        // update the user identity
        final String sql = "UPDATE UGP_USER SET IDENTITY = ? WHERE IDENTIFIER = ?";
        final int updated = jdbcTemplate.update(sql, user.getIdentity(), user.getIdentifier());

        // if no rows were updated then there is no user with the given identifier, so return null
        if (updated <= 0) {
            return null;
        }

        refreshUserGroupHolder();

        return user;
    }

    @Override
    public Set<User> getUsers() throws AuthorizationAccessException {
        return userGroupHolder.get().getAllUsers();
    }

    private Set<User> getDatabaseUsers() throws AuthorizationAccessException {
        final String sql = "SELECT * FROM UGP_USER";
        final List<DatabaseUser> databaseUsers = jdbcTemplate.query(sql, new DatabaseUserRowMapper());

        final Set<User> users = new HashSet<>();
        databaseUsers.forEach(u -> {
            users.add(mapToUser(u));
        });
        return users;
    }

    @Override
    public User getUser(final String identifier) throws AuthorizationAccessException {
        Validate.notBlank(identifier);
        return userGroupHolder.get().getUsersById().get(identifier);
    }

    @Override
    public User getUserByIdentity(final String identity) throws AuthorizationAccessException {
        Validate.notBlank(identity);
        return userGroupHolder.get().getUsersByIdentity().get(identity);
    }

    @Override
    public UserAndGroups getUserAndGroups(final String userIdentity) throws AuthorizationAccessException {
        Validate.notBlank(userIdentity);

        final User user = userGroupHolder.get().getUser(userIdentity);
        final Set<Group> groups = userGroupHolder.get().getGroups(userIdentity);

        return new UserAndGroups() {
            @Override
            public User getUser() {
                return user;
            }

            @Override
            public Set<Group> getGroups() {
                return groups;
            }
        };
    }

    @Override
    public synchronized User deleteUser(final User user) throws AuthorizationAccessException {
        Objects.requireNonNull(user);

        final String deleteFromUserGroupSql = "DELETE FROM UGP_USER_GROUP WHERE USER_IDENTIFIER = ?";
        jdbcTemplate.update(deleteFromUserGroupSql, user.getIdentifier());

        final String deleteFromUserSql = "DELETE FROM UGP_USER WHERE IDENTIFIER = ?";
        final int rowsDeletedFromUser = jdbcTemplate.update(deleteFromUserSql, user.getIdentifier());
        if (rowsDeletedFromUser <= 0) {
            return null;
        }

        refreshUserGroupHolder();

        return user;
    }

    private User mapToUser(final DatabaseUser databaseUser) {
        return new User.Builder()
                .identifier(databaseUser.getIdentifier())
                .identity(databaseUser.getIdentity())
                .build();
    }

    //-- Group CRUD

    @Override
    public synchronized Group addGroup(final Group group) throws AuthorizationAccessException {
        Objects.requireNonNull(group);

        // insert to the group table...
        final String groupSql = "INSERT INTO UGP_GROUP(IDENTIFIER, IDENTITY) VALUES (?, ?)";
        jdbcTemplate.update(groupSql, group.getIdentifier(), group.getName());

        // insert to the user-group table...
        createUserGroups(group);

        refreshUserGroupHolder();

        return group;
    }

    @Override
    public synchronized Group updateGroup(final Group group) throws AuthorizationAccessException {
        Objects.requireNonNull(group);

        // update the group identity
        final String updateGroupSql = "UPDATE UGP_GROUP SET IDENTITY = ? WHERE IDENTIFIER = ?";
        final int updated = jdbcTemplate.update(updateGroupSql, group.getName(), group.getIdentifier());

        // if no rows were updated then a group does not exist for the given identifier, so return null
        if (updated <= 0) {
            return null;
        }

        // delete any user-group associations
        final String deleteUserGroups = "DELETE FROM UGP_USER_GROUP WHERE GROUP_IDENTIFIER = ?";
        jdbcTemplate.update(deleteUserGroups, group.getIdentifier());

        // re-create any user-group associations
        createUserGroups(group);

        refreshUserGroupHolder();

        return group;
    }

    @Override
    public Set<Group> getGroups() throws AuthorizationAccessException {
        return userGroupHolder.get().getAllGroups();
    }

    private Set<Group> getDatabaseGroups() throws AuthorizationAccessException {
        // retrieve all the groups
        final String sql = "SELECT * FROM UGP_GROUP";
        final List<DatabaseGroup> databaseGroups = jdbcTemplate.query(sql, new DatabaseGroupRowMapper());

        // retrieve all the users in the groups, mapped by group id
        final Map<String, Set<String>> groupToUsers = new HashMap<>();
        jdbcTemplate.query("SELECT * FROM UGP_USER_GROUP", (rs) -> {
            final String groupIdentifier = rs.getString("GROUP_IDENTIFIER");
            final String userIdentifier = rs.getString("USER_IDENTIFIER");

            final Set<String> userIdentifiers = groupToUsers.computeIfAbsent(groupIdentifier, (k) -> new HashSet<>());
            userIdentifiers.add(userIdentifier);
        });

        // convert from database model to api model
        final Set<Group> groups = new HashSet<>();
        databaseGroups.forEach(g -> {
            groups.add(mapToGroup(g, groupToUsers.get(g.getIdentifier())));
        });
        return groups;
    }

    @Override
    public Group getGroup(final String groupIdentifier) throws AuthorizationAccessException {
        Validate.notBlank(groupIdentifier);
        return userGroupHolder.get().getGroupsById().get(groupIdentifier);
    }

    @Override
    public synchronized Group deleteGroup(final Group group) throws AuthorizationAccessException {
        Objects.requireNonNull(group);

        final String sql = "DELETE FROM UGP_GROUP WHERE IDENTIFIER = ?";
        final int rowsUpdated = jdbcTemplate.update(sql, group.getIdentifier());
        if (rowsUpdated <= 0) {
            return null;
        }

        refreshUserGroupHolder();

        return group;
    }

    private void createUserGroups(final Group group) {
        if (group.getUsers() != null) {
            for (final String userIdentifier : group.getUsers()) {
                final String userGroupSql = "INSERT INTO UGP_USER_GROUP (USER_IDENTIFIER, GROUP_IDENTIFIER) VALUES (?, ?)";
                jdbcTemplate.update(userGroupSql, userIdentifier, group.getIdentifier());
            }
        }
    }

    private Group mapToGroup(final DatabaseGroup databaseGroup, final Set<String> userIdentifiers) {
        return new Group.Builder()
                .identifier(databaseGroup.getIdentifier())
                .name(databaseGroup.getIdentity())
                .addUsers(userIdentifiers == null ? Collections.emptySet() : userIdentifiers)
                .build();
    }

    private synchronized void refreshUserGroupHolder() {
        final Set<User> allUsers = getDatabaseUsers();
        final Set<Group> allGroups = getDatabaseGroups();
        this.userGroupHolder.set(new DatabaseUserGroupHolder(allUsers, allGroups));
    }

    //-- util methods

    private <T> T queryForObject(final String sql, final RowMapper<T> rowMapper, final Object... args) {
        try {
            return jdbcTemplate.queryForObject(sql, rowMapper, args);
        } catch (final EmptyResultDataAccessException e) {
            return null;
        }
    }
}
