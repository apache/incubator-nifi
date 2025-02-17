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

import org.apache.nifi.registry.db.DatabaseBaseTest;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.authorization.ConfigurableUserGroupProvider;
import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.User;
import org.apache.nifi.registry.security.authorization.UserAndGroups;
import org.apache.nifi.registry.security.authorization.UserGroupProviderInitializationContext;
import org.apache.nifi.registry.security.authorization.util.UserGroupProviderUtils;
import org.apache.nifi.registry.security.identity.DefaultIdentityMapper;
import org.apache.nifi.registry.security.identity.IdentityMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDatabaseUserGroupProvider extends DatabaseBaseTest {

    @Autowired
    private DataSource dataSource;
    private NiFiRegistryProperties properties;
    private IdentityMapper identityMapper;

    private ConfigurableUserGroupProvider userGroupProvider;

    private Method refreshUserGroupHolderMethod;

    @BeforeEach
    public void setup() throws NoSuchMethodException {
        properties = new NiFiRegistryProperties();
        identityMapper = new DefaultIdentityMapper(properties);

        final DatabaseUserGroupProvider databaseUserGroupProvider = new DatabaseUserGroupProvider();
        databaseUserGroupProvider.setDataSource(dataSource);
        databaseUserGroupProvider.setIdentityMapper(identityMapper);

        final UserGroupProviderInitializationContext initializationContext = mock(UserGroupProviderInitializationContext.class);
        databaseUserGroupProvider.initialize(initializationContext);

        userGroupProvider = databaseUserGroupProvider;

        refreshUserGroupHolderMethod = DatabaseUserGroupProvider.class.getDeclaredMethod("refreshUserGroupHolder");
        refreshUserGroupHolderMethod.setAccessible(true);
    }

    /**
     * Helper method to call onConfigured with a configuration context that has initial users.
     *
     * @param initialUserIdentities the initial user identities to place in the configuration context
     */
    private void configureWithInitialUsers(final String... initialUserIdentities) {
        final Map<String, String> configProperties = new HashMap<>();

        for (int i = 0; i < initialUserIdentities.length; i++) {
            final String initialUserIdentity = initialUserIdentities[i];
            configProperties.put(UserGroupProviderUtils.PROP_INITIAL_USER_IDENTITY_PREFIX + (i + 1), initialUserIdentity);
        }

        final AuthorizerConfigurationContext configurationContext = mock(AuthorizerConfigurationContext.class);
        when(configurationContext.getProperties()).thenReturn(configProperties);

        userGroupProvider.onConfigured(configurationContext);
    }

    /**
     * Helper method to create a user outside of the provider.
     *
     * @param userIdentifier the user identifier
     * @param userIdentity the user identity
     */
    private void createUser(final String userIdentifier, final String userIdentity) {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        final String sql = "INSERT INTO UGP_USER(IDENTIFIER, IDENTITY) VALUES (?, ?)";
        final int updatedRows1 = jdbcTemplate.update(sql, userIdentifier, userIdentity);
        assertEquals(1, updatedRows1);
        invokeRefreshUserGroupHolder();
    }

    /**
     * Helper method to create a group outside of the provider.
     *
     * @param groupIdentifier the group identifier
     * @param groupIdentity the group identity
     */
    private void createGroup(final String groupIdentifier, final String groupIdentity) {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        final String sql = "INSERT INTO UGP_GROUP(IDENTIFIER, IDENTITY) VALUES (?, ?)";
        final int updatedRows1 = jdbcTemplate.update(sql, groupIdentifier, groupIdentity);
        assertEquals(1, updatedRows1);
        invokeRefreshUserGroupHolder();
    }

    /**
     * Helper method to add a user to a group outside of the provider
     *
     * @param userIdentifier the user identifier
     * @param groupIdentifier the group identifier
     */
    private void addUserToGroup(final String userIdentifier, final String groupIdentifier) {
        final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        final String sql = "INSERT INTO UGP_USER_GROUP(USER_IDENTIFIER, GROUP_IDENTIFIER) VALUES (?, ?)";
        final int updatedRows1 = jdbcTemplate.update(sql, userIdentifier, groupIdentifier);
        assertEquals(1, updatedRows1);
        invokeRefreshUserGroupHolder();
    }

    /**
     * Helper method to refresh the provider cache.
     */
    private void invokeRefreshUserGroupHolder() {
        try {
            refreshUserGroupHolderMethod.invoke(userGroupProvider);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new AssertionError(e);
        }
    }

    // -- Test onConfigured

    @Test
    public void testOnConfiguredCreatesInitialUsersWhenNoUsersAndGroups() {
        final String userIdentity1 = "user1";
        final String userIdentity2 = "user2";
        configureWithInitialUsers(userIdentity1, userIdentity2);

        final Set<User> users = userGroupProvider.getUsers();
        assertEquals(2, users.size());
        assertNotNull(users.stream().filter(u -> u.getIdentity().equals(userIdentity1)).findFirst().orElse(null));
        assertNotNull(users.stream().filter(u -> u.getIdentity().equals(userIdentity2)).findFirst().orElse(null));
    }

    @Test
    public void testOnConfiguredStillCreatesInitialUsersWhenExistingUsersAndGroups() {
        // Create a user in the DB before we call onConfigured
        final String existingUserIdentity = "existingUser";
        final String existingUserIdentifier = UUID.randomUUID().toString();
        createUser(existingUserIdentifier, existingUserIdentity);

        // Call onConfigured with initial users
        final String userIdentity1 = "user1";
        final String userIdentity2 = "user2";
        configureWithInitialUsers(userIdentity1, userIdentity2);

        // Verify the initial users were not created
        final Set<User> users = userGroupProvider.getUsers();
        assertEquals(3, users.size());
        assertNotNull(users.stream().filter(u -> u.getIdentity().equals(existingUserIdentity)).findFirst().orElse(null));
        assertNotNull(users.stream().filter(u -> u.getIdentity().equals(userIdentity1)).findFirst().orElse(null));
        assertNotNull(users.stream().filter(u -> u.getIdentity().equals(userIdentity2)).findFirst().orElse(null));
    }

    @Test
    public void testOnConfiguredWithSameUsers() {
        // Create a user in the DB before we call onConfigured
        final String existingUserIdentity = "existingUser";
        final String existingUserIdentifier = UUID.randomUUID().toString();
        createUser(existingUserIdentifier, existingUserIdentity);

        // Call onConfigured with same identity that already exists
        configureWithInitialUsers(existingUserIdentity);

        // Verify there is only one user
        final Set<User> users = userGroupProvider.getUsers();
        assertEquals(1, users.size());
        assertNotNull(users.stream().filter(u -> u.getIdentity().equals(existingUserIdentity)).findFirst().orElse(null));
    }

    @Test
    public void testOnConfiguredAppliesIdentityMappingsToInitialUsers() {
        final Properties props = new Properties();
        // Set up an identity mapping for kerberos principals
        props.setProperty("nifi.registry.security.identity.mapping.pattern.kerb", "^(.*?)@(.*?)$");
        props.setProperty("nifi.registry.security.identity.mapping.value.kerb", "$1");
        properties = new NiFiRegistryProperties(props);

        identityMapper = new DefaultIdentityMapper(properties);
        ((DatabaseUserGroupProvider) userGroupProvider).setIdentityMapper(identityMapper);

        // Call onConfigured with two initial users - one kerberos principal, one DN
        final String userIdentity1 = "user1@NIFI.COM";
        final String userIdentity2 = "CN=user2, OU=NIFI";
        configureWithInitialUsers(userIdentity1, userIdentity2);

        // Verify the kerberos principal had the mapping applied
        final Set<User> users = userGroupProvider.getUsers();
        assertEquals(2, users.size());
        assertNotNull(users.stream().filter(u -> u.getIdentity().equals("user1")).findFirst().orElse(null));
        assertNotNull(users.stream().filter(u -> u.getIdentity().equals(userIdentity2)).findFirst().orElse(null));
    }

    // -- Test User Methods

    @Test
    public void testAddUser() {
        configureWithInitialUsers();

        final Set<User> users = userGroupProvider.getUsers();
        assertEquals(0, users.size());

        final User user1 = new User.Builder()
                .identifier(UUID.randomUUID().toString())
                .identity("user1")
                .build();

        final User createdUser1 = userGroupProvider.addUser(user1);
        assertNotNull(createdUser1);

        final Set<User> usersAfterCreate = userGroupProvider.getUsers();
        assertEquals(1, usersAfterCreate.size());

        final User retrievedUser1 = usersAfterCreate.stream().findFirst().get();
        assertEquals(user1.getIdentifier(), retrievedUser1.getIdentifier());
        assertEquals(user1.getIdentity(), retrievedUser1.getIdentity());
    }

    @Test
    public void testGetUserByIdentifierWhenExists() {
        configureWithInitialUsers();

        final String userIdentifier = UUID.randomUUID().toString();
        final String userIdentity = "user1";
        createUser(userIdentifier, userIdentity);

        final User retrievedUser1 = userGroupProvider.getUser(userIdentifier);
        assertNotNull(retrievedUser1);
        assertEquals(userIdentifier, retrievedUser1.getIdentifier());
        assertEquals(userIdentity, retrievedUser1.getIdentity());
    }

    @Test
    public void testGetUserByIdentifierWhenDoesNotExist() {
        configureWithInitialUsers();

        final User retrievedUser1 = userGroupProvider.getUser("does-not-exist");
        assertNull(retrievedUser1);
    }

    @Test
    public void testGetUserByIdentityWhenExists() {
        configureWithInitialUsers();

        final String userIdentifier = UUID.randomUUID().toString();
        final String userIdentity = "user1";
        createUser(userIdentifier, userIdentity);

        final User retrievedUser1 = userGroupProvider.getUserByIdentity(userIdentity);
        assertNotNull(retrievedUser1);
        assertEquals(userIdentifier, retrievedUser1.getIdentifier());
        assertEquals(userIdentity, retrievedUser1.getIdentity());
    }

    @Test
    public void testGetUserByIdentityWhenDoesNotExist() {
        configureWithInitialUsers();

        final User retrievedUser1 = userGroupProvider.getUserByIdentity("does-not-exist");
        assertNull(retrievedUser1);
    }

    @Test
    public void testGetUserAndGroupsWhenExists() {
        configureWithInitialUsers();

        // Create some users...
        final User user1 = new User.Builder().identifier(UUID.randomUUID().toString()).identity("user1").build();
        createUser(user1.getIdentifier(), user1.getIdentity());

        final User user2 = new User.Builder().identifier(UUID.randomUUID().toString()).identity("user2").build();
        createUser(user2.getIdentifier(), user2.getIdentity());

        // Create some groups...
        final Group group1 = new Group.Builder().identifier(UUID.randomUUID().toString()).name("group1").build();
        createGroup(group1.getIdentifier(), group1.getName());

        final Group group2 = new Group.Builder().identifier(UUID.randomUUID().toString()).name("group2").build();
        createGroup(group2.getIdentifier(), group2.getName());

        // Add users to groups...
        addUserToGroup(user1.getIdentifier(), group1.getIdentifier());
        addUserToGroup(user2.getIdentifier(), group2.getIdentifier());

        // Retrieve UserAndGroups...
        final UserAndGroups user1AndGroups = userGroupProvider.getUserAndGroups(user1.getIdentity());
        assertNotNull(user1AndGroups);

        // Verify retrieved user..
        final User retrievedUser1 = user1AndGroups.getUser();
        assertNotNull(retrievedUser1);
        assertEquals(user1.getIdentifier(), retrievedUser1.getIdentifier());
        assertEquals(user1.getIdentity(), retrievedUser1.getIdentity());

        // Verify retrieved groups..
        final Set<Group> user1Groups = user1AndGroups.getGroups();
        assertNotNull(user1Groups);
        assertEquals(1, user1Groups.size());

        final Group user1Group = user1Groups.stream().findFirst().get();
        assertEquals(group1.getIdentifier(), user1Group.getIdentifier());
        assertEquals(group1.getName(), user1Group.getName());

        assertNotNull(user1Group.getUsers());
        assertEquals(1, user1Group.getUsers().size());
        assertTrue(user1Group.getUsers().contains(user1.getIdentifier()));
    }

    @Test
    public void testGetUserAndGroupsWhenDoesNotExist() {
        configureWithInitialUsers();

        final UserAndGroups userAndGroups = userGroupProvider.getUserAndGroups("does-not-exist");
        assertNotNull(userAndGroups);
        assertNull(userAndGroups.getUser());
        assertNull(userAndGroups.getGroups());
    }

    @Test
    public void testUpdateUserWhenExists() {
        configureWithInitialUsers();

        final User user1 = new User.Builder().identifier(UUID.randomUUID().toString()).identity("user1").build();
        createUser(user1.getIdentifier(), user1.getIdentity());

        final User retrievedUser1 = userGroupProvider.getUser(user1.getIdentifier());
        assertNotNull(retrievedUser1);

        final User modifiedUser1 = new User.Builder(retrievedUser1).identity("user1 updated").build();

        final User updatedUser1 = userGroupProvider.updateUser(modifiedUser1);
        assertNotNull(updatedUser1);
        assertEquals(modifiedUser1.getIdentity(), updatedUser1.getIdentity());
    }

    @Test
    public void testUpdateUserWhenDoesNotExist() {
        configureWithInitialUsers();

        final User user1 = new User.Builder().identifier(UUID.randomUUID().toString()).identity("user1").build();
        final User updatedUser1 = userGroupProvider.updateUser(user1);
        assertNull(updatedUser1);
    }

    @Test
    public void testDeleteUserWhenExists() {
        final String user1Identity = "user1";
        configureWithInitialUsers(user1Identity);

        // verify user1 exists
        final User retrievedUser1 = userGroupProvider.getUserByIdentity(user1Identity);
        assertNotNull(retrievedUser1);
        assertEquals(user1Identity, retrievedUser1.getIdentity());

        // add user1 to a group to test deleting group association when deleting a user
        final Group group1 = new Group.Builder()
                .identifier(UUID.randomUUID().toString())
                .name("group1")
                .addUser(retrievedUser1.getIdentifier())
                .build();

        final Group createdGroup1 = userGroupProvider.addGroup(group1);
        assertNotNull(createdGroup1);

        // delete user1
        final User deletedUser1 = userGroupProvider.deleteUser(retrievedUser1);
        assertNotNull(deletedUser1);

        // verify user1 no longer exists
        final User retrievedUser1AfterDelete = userGroupProvider.getUserByIdentity(user1Identity);
        assertNull(retrievedUser1AfterDelete);

        // verify user1 no longer a member of group1
        final Group retrievedGroup1 = userGroupProvider.getGroup(group1.getIdentifier());
        assertNotNull(retrievedGroup1);
        assertEquals(0, retrievedGroup1.getUsers().size());
    }

    @Test
    public void testDeleteUserWhenDoesNotExist() {
        configureWithInitialUsers();

        final User user1 = new User.Builder().identifier(UUID.randomUUID().toString()).identity("user1").build();
        final User updatedUser1 = userGroupProvider.deleteUser(user1);
        assertNull(updatedUser1);
    }

    // -- Test Group Methods

    @Test
    public void testAddGroupWithoutUsers() {
        configureWithInitialUsers();

        final Set<Group> groupsBefore = userGroupProvider.getGroups();
        assertEquals(0, groupsBefore.size());

        final Group group1 = new Group.Builder().identifier(UUID.randomUUID().toString()).name("group1").build();

        final Group createdGroup1 = userGroupProvider.addGroup(group1);
        assertNotNull(createdGroup1);

        final Set<Group> groupsAfter = userGroupProvider.getGroups();
        assertEquals(1, groupsAfter.size());

        final Group retrievedGroup1 = groupsAfter.stream().findFirst().get();
        assertEquals(group1.getIdentifier(), retrievedGroup1.getIdentifier());
        assertEquals(group1.getName(), retrievedGroup1.getName());
        assertNotNull(retrievedGroup1.getUsers());
        assertEquals(0, retrievedGroup1.getUsers().size());
    }

    @Test
    public void testAddGroupWithUsers() {
        configureWithInitialUsers();

        final String user1Identifier = UUID.randomUUID().toString();
        final String user1Identity = "user1";
        createUser(user1Identifier, user1Identity);

        final Set<Group> groupsBefore = userGroupProvider.getGroups();
        assertEquals(0, groupsBefore.size());

        final Group group1 = new Group.Builder()
                .identifier(UUID.randomUUID().toString())
                .name("group1")
                .addUser(user1Identifier)
                .build();

        final Group createdGroup1 = userGroupProvider.addGroup(group1);
        assertNotNull(createdGroup1);

        final Set<Group> groupsAfter = userGroupProvider.getGroups();
        assertEquals(1, groupsAfter.size());

        final Group retrievedGroup1 = groupsAfter.stream().findFirst().get();
        assertEquals(group1.getIdentifier(), retrievedGroup1.getIdentifier());
        assertEquals(group1.getName(), retrievedGroup1.getName());
        assertNotNull(retrievedGroup1.getUsers());
        assertEquals(1, retrievedGroup1.getUsers().size());
        assertTrue(retrievedGroup1.getUsers().contains(user1Identifier));
    }

    @Test
    public void testGetGroupWhenExists() {
        configureWithInitialUsers();

        final String group1Identifier = UUID.randomUUID().toString();
        final String group1Identity = "group1";
        createGroup(group1Identifier, group1Identity);

        final Group group1 = userGroupProvider.getGroup(group1Identifier);
        assertNotNull(group1);
        assertEquals(group1Identifier, group1.getIdentifier());
        assertEquals(group1Identity, group1.getName());

        assertNotNull(group1.getUsers());
        assertEquals(0, group1.getUsers().size());
    }

    @Test
    public void testGetGroupWhenDoesNotExist() {
        configureWithInitialUsers();

        final Group group1 = userGroupProvider.getGroup("does-not-exist");
        assertNull(group1);
    }

    @Test
    public void testUpdateGroupWhenExists() {
        configureWithInitialUsers();

        // Create some users...
        final User user1 = new User.Builder().identifier(UUID.randomUUID().toString()).identity("user1").build();
        createUser(user1.getIdentifier(), user1.getIdentity());

        final User user2 = new User.Builder().identifier(UUID.randomUUID().toString()).identity("user2").build();
        createUser(user2.getIdentifier(), user2.getIdentity());

        // Create a group and add user1 to it...
        final Group group1 = new Group.Builder().identifier(UUID.randomUUID().toString()).name("group1").build();
        createGroup(group1.getIdentifier(), group1.getName());
        addUserToGroup(user1.getIdentifier(), group1.getIdentifier());

        // Retrieve the created group...
        final Group retrievedGroup1 = userGroupProvider.getGroup(group1.getIdentifier());
        assertNotNull(retrievedGroup1);
        assertEquals(group1.getName(), retrievedGroup1.getName());
        assertEquals(1, retrievedGroup1.getUsers().size());

        // Modify the name and add a user...
        final Group modifiedGroup1 = new Group.Builder(retrievedGroup1)
                .name(retrievedGroup1.getName() + " updated")
                .addUser(user2.getIdentifier())
                .build();

        // Perform the update...
        final Group updatedGroup1 = userGroupProvider.updateGroup(modifiedGroup1);
        assertNotNull(updatedGroup1);

        // Re-retrieve and verify the updates were made...
        final Group retrievedGroup1AfterUpdate = userGroupProvider.getGroup(group1.getIdentifier());
        assertNotNull(retrievedGroup1AfterUpdate);
        assertEquals(modifiedGroup1.getName(), retrievedGroup1AfterUpdate.getName());
        assertEquals(2, retrievedGroup1AfterUpdate.getUsers().size());
    }

    @Test
    public void testUpdateGroupWhenDoesNotExist() {
        configureWithInitialUsers();
        final Group group1 = new Group.Builder().identifier(UUID.randomUUID().toString()).name("group1").build();
        final Group updatedGroup1 = userGroupProvider.updateGroup(group1);
        assertNull(updatedGroup1);
    }

    @Test
    public void testUpdateGroupRemoveAllUsers() {
        configureWithInitialUsers();

        // Create some users...
        final User user1 = new User.Builder().identifier(UUID.randomUUID().toString()).identity("user1").build();
        createUser(user1.getIdentifier(), user1.getIdentity());

        final User user2 = new User.Builder().identifier(UUID.randomUUID().toString()).identity("user2").build();
        createUser(user2.getIdentifier(), user2.getIdentity());

        // Create a group and add user1 to it...
        final Group group1 = new Group.Builder().identifier(UUID.randomUUID().toString()).name("group1").build();
        createGroup(group1.getIdentifier(), group1.getName());
        addUserToGroup(user1.getIdentifier(), group1.getIdentifier());

        // Retrieve the created group...
        final Group retrievedGroup1 = userGroupProvider.getGroup(group1.getIdentifier());
        assertNotNull(retrievedGroup1);
        assertEquals(group1.getName(), retrievedGroup1.getName());
        assertEquals(1, retrievedGroup1.getUsers().size());

        // Modify the name and add a user...
        final Group modifiedGroup1 = new Group.Builder(retrievedGroup1)
                .name(retrievedGroup1.getName() + " updated")
                .clearUsers()
                .build();

        // Perform the update...
        final Group updatedGroup1 = userGroupProvider.updateGroup(modifiedGroup1);
        assertNotNull(updatedGroup1);

        // Re-retrieve and verify the updates were made...
        final Group retrievedGroup1AfterUpdate = userGroupProvider.getGroup(group1.getIdentifier());
        assertNotNull(retrievedGroup1AfterUpdate);
        assertEquals(modifiedGroup1.getName(), retrievedGroup1AfterUpdate.getName());
        assertEquals(0, retrievedGroup1AfterUpdate.getUsers().size());
    }

    @Test
    public void testDeleteGroupWhenExists() {
        configureWithInitialUsers();

        // Create some users...
        final User user1 = new User.Builder().identifier(UUID.randomUUID().toString()).identity("user1").build();
        createUser(user1.getIdentifier(), user1.getIdentity());

        final User user2 = new User.Builder().identifier(UUID.randomUUID().toString()).identity("user2").build();
        createUser(user2.getIdentifier(), user2.getIdentity());

        // Create a group and add user1 to it...
        final Group group1 = new Group.Builder().identifier(UUID.randomUUID().toString()).name("group1").build();
        createGroup(group1.getIdentifier(), group1.getName());
        addUserToGroup(user1.getIdentifier(), group1.getIdentifier());

        // Retrieve the created group...
        final Group retrievedGroup1 = userGroupProvider.getGroup(group1.getIdentifier());
        assertNotNull(retrievedGroup1);

        // Delete the group...
        final Group deletedGroup1 = userGroupProvider.deleteGroup(retrievedGroup1);
        assertNotNull(deletedGroup1);

        assertNull(userGroupProvider.getGroup(group1.getIdentifier()));
    }

    @Test
    public void testDeleteGroupWhenDoesNotExist() {
        configureWithInitialUsers();
        final Group group1 = new Group.Builder().identifier(UUID.randomUUID().toString()).name("group1").build();
        final Group updatedGroup1 = userGroupProvider.deleteGroup(group1);
        assertNull(updatedGroup1);
    }
}
