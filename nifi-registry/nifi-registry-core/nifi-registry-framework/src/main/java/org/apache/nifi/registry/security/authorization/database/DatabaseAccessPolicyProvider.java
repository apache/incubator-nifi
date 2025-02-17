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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.security.authorization.AbstractConfigurableAccessPolicyProvider;
import org.apache.nifi.registry.security.authorization.AccessPolicy;
import org.apache.nifi.registry.security.authorization.AccessPolicyProviderInitializationContext;
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.User;
import org.apache.nifi.registry.security.authorization.annotation.AuthorizerContext;
import org.apache.nifi.registry.security.authorization.database.entity.DatabaseAccessPolicy;
import org.apache.nifi.registry.security.authorization.database.mapper.DatabaseAccessPolicyRowMapper;
import org.apache.nifi.registry.security.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.registry.security.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.registry.security.authorization.util.AccessPolicyProviderUtils;
import org.apache.nifi.registry.security.authorization.util.InitialPolicies;
import org.apache.nifi.registry.security.authorization.util.ResourceAndAction;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.exception.SecurityProviderDestructionException;
import org.apache.nifi.registry.security.identity.IdentityMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link org.apache.nifi.registry.security.authorization.ConfigurableAccessPolicyProvider} backed by a relational database.
 */
public class DatabaseAccessPolicyProvider extends AbstractConfigurableAccessPolicyProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseAccessPolicyProvider.class);

    private DataSource dataSource;
    private IdentityMapper identityMapper;

    private JdbcTemplate jdbcTemplate;

    private final AtomicReference<DatabaseAccessPolicyHolder> accessPoliciesHolder = new AtomicReference<>();

    @AuthorizerContext
    public void setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @AuthorizerContext
    public void setIdentityMapper(final IdentityMapper identityMapper) {
        this.identityMapper = identityMapper;
    }

    @Override
    protected void doInitialize(AccessPolicyProviderInitializationContext initializationContext) throws SecurityProviderCreationException {
        super.doInitialize(initializationContext);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public void doOnConfigured(final AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
        refreshAccessPolicyHolder();

        final String initialAdminIdentity = AccessPolicyProviderUtils.getInitialAdminIdentity(configurationContext, identityMapper);
        final Set<String> nifiIdentities = AccessPolicyProviderUtils.getNiFiIdentities(configurationContext, identityMapper);
        final String nifiGroupName = AccessPolicyProviderUtils.getNiFiGroupName(configurationContext, identityMapper);

        if (!StringUtils.isBlank(initialAdminIdentity)) {
            LOGGER.info("Populating authorizations for Initial Admin: '{}'", initialAdminIdentity);
            populateInitialAdmin(initialAdminIdentity);
        }

        if (!CollectionUtils.isEmpty(nifiIdentities)) {
            LOGGER.info("Populating authorizations for NiFi identities: [{}]", StringUtils.join(nifiIdentities, ";"));
            populateNiFiIdentities(nifiIdentities);
        }

        if (!StringUtils.isBlank(nifiGroupName)) {
            LOGGER.info("Populating authorizations for NiFi Group: '{}'", nifiGroupName);
            populateNiFiGroup(nifiGroupName);
        }
    }

    private void populateInitialAdmin(final String initialAdminIdentity) {
        final User initialAdmin = getUserGroupProvider().getUserByIdentity(initialAdminIdentity);
        if (initialAdmin == null) {
            throw new SecurityProviderCreationException("Unable to locate initial admin '" + initialAdminIdentity + "' to seed policies");
        }

        for (final ResourceAndAction resourceAction : InitialPolicies.ADMIN_POLICIES) {
            populateInitialPolicy(initialAdmin, resourceAction);
        }
    }

    private void populateNiFiIdentities(final Set<String> nifiIdentities) {
        for (final String nifiIdentity : nifiIdentities) {
            final User nifiUser = getUserGroupProvider().getUserByIdentity(nifiIdentity);
            if (nifiUser == null) {
                throw new SecurityProviderCreationException("Unable to locate NiFi identity '" + nifiIdentity + "' to seed policies.");
            }

            for (final ResourceAndAction resourceAction : InitialPolicies.NIFI_POLICIES) {
                populateInitialPolicy(nifiUser, resourceAction);
            }
        }
    }

    private void populateNiFiGroup(final String nifiGroupName) {
        final Group nifiGroup = AccessPolicyProviderUtils.getGroup(nifiGroupName, getUserGroupProvider());

        for (final ResourceAndAction resourceAction : InitialPolicies.NIFI_POLICIES) {
            populateInitialPolicy(nifiGroup, resourceAction);
        }
    }

    @Override
    public void preDestruction() throws SecurityProviderDestructionException {

    }

    // ---- fingerprinting methods

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        throw new UnsupportedOperationException("Fingerprinting is not supported by this provider");
    }

    @Override
    public void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
        throw new UnsupportedOperationException("Fingerprinting is not supported by this provider");
    }

    @Override
    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
        throw new UnsupportedOperationException("Fingerprinting is not supported by this provider");
    }

    // ---- access policy methods

    @Override
    public synchronized AccessPolicy addAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        Objects.requireNonNull(accessPolicy);

        // insert to the policy table
        final String policySql = "INSERT INTO APP_POLICY(IDENTIFIER, RESOURCE, ACTION) VALUES (?, ?, ?)";
        jdbcTemplate.update(policySql, accessPolicy.getIdentifier(), accessPolicy.getResource(), accessPolicy.getAction().toString());

        // insert to the policy-user and policy groups table
        createPolicyUserAndGroups(accessPolicy);

        refreshAccessPolicyHolder();

        return accessPolicy;
    }

    @Override
    public synchronized AccessPolicy updateAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        Objects.requireNonNull(accessPolicy);

        // determine if policy exists
        final DatabaseAccessPolicy existingPolicy = getDatabaseAcessPolicy(accessPolicy.getIdentifier());
        if (existingPolicy == null) {
            return null;
        }

        // delete any policy-user associations
        final String deletePolicyUsersSql = "DELETE FROM APP_POLICY_USER WHERE POLICY_IDENTIFIER = ?";
        jdbcTemplate.update(deletePolicyUsersSql, accessPolicy.getIdentifier());

        // delete any policy-group associations
        final String deletePolicyGroupsSql = "DELETE FROM APP_POLICY_GROUP WHERE POLICY_IDENTIFIER = ?";
        jdbcTemplate.update(deletePolicyGroupsSql, accessPolicy.getIdentifier());


        // re-create the associations
        createPolicyUserAndGroups(accessPolicy);

        refreshAccessPolicyHolder();

        return accessPolicy;
    }

    @Override
    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return accessPoliciesHolder.get().getAllPolicies();
    }

    private Set<AccessPolicy> getDatabaseAccessPolicies() throws AuthorizationAccessException {
        // retrieve all the policies
        final String sql = "SELECT * FROM APP_POLICY";
        final List<DatabaseAccessPolicy> databasePolicies = jdbcTemplate.query(sql, new DatabaseAccessPolicyRowMapper());

        // retrieve all users in policies, mapped by policy id
        final Map<String, Set<String>> policyToUsers = new HashMap<>();
        jdbcTemplate.query("SELECT * FROM APP_POLICY_USER", (rs) -> {
            final String policyIdentifier = rs.getString("POLICY_IDENTIFIER");
            final String userIdentifier = rs.getString("USER_IDENTIFIER");

            final Set<String> userIdentifiers = policyToUsers.computeIfAbsent(policyIdentifier, (k) -> new HashSet<>());
            userIdentifiers.add(userIdentifier);
        });

        // retrieve all groups in policies, mapped by policy id
        final Map<String, Set<String>> policyToGroups = new HashMap<>();
        jdbcTemplate.query("SELECT * FROM APP_POLICY_GROUP", (rs) -> {
            final String policyIdentifier = rs.getString("POLICY_IDENTIFIER");
            final String groupIdentifier = rs.getString("GROUP_IDENTIFIER");

            final Set<String> groupIdentifiers = policyToGroups.computeIfAbsent(policyIdentifier, (k) -> new HashSet<>());
            groupIdentifiers.add(groupIdentifier);
        });

        // convert the database model to the api model
        final Set<AccessPolicy> policies = new HashSet<>();

        databasePolicies.forEach(p -> {
            final Set<String> userIdentifiers = policyToUsers.get(p.getIdentifier());
            final Set<String> groupIdentifiers = policyToGroups.get(p.getIdentifier());
            policies.add(mapTopAccessPolicy(p, userIdentifiers, groupIdentifiers));
        });

        return policies;
    }

    @Override
    public AccessPolicy getAccessPolicy(final String identifier) throws AuthorizationAccessException {
        Validate.notBlank(identifier);
        return accessPoliciesHolder.get().getPoliciesById().get(identifier);
    }

    @Override
    public AccessPolicy getAccessPolicy(final String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
        Validate.notBlank(resourceIdentifier);
        Objects.requireNonNull(action);
        return AccessPolicyProviderUtils.getAccessPolicy(resourceIdentifier, action, accessPoliciesHolder.get().getPoliciesByResource());
    }

    @Override
    public synchronized AccessPolicy deleteAccessPolicy(final AccessPolicy accessPolicy) throws AuthorizationAccessException {
        Objects.requireNonNull(accessPolicy);

        final String sql = "DELETE FROM APP_POLICY WHERE IDENTIFIER = ?";
        final int rowsUpdated = jdbcTemplate.update(sql, accessPolicy.getIdentifier());
        if (rowsUpdated <= 0) {
            return null;
        }

        refreshAccessPolicyHolder();

        return accessPolicy;
    }

    protected void createPolicyUserAndGroups(final AccessPolicy accessPolicy) {
        if (accessPolicy.getUsers() != null) {
            for (final String userIdentifier : accessPolicy.getUsers()) {
                insertPolicyUser(accessPolicy.getIdentifier(), userIdentifier);
            }
        }

        if (accessPolicy.getGroups() != null) {
            for (final String groupIdentifier : accessPolicy.getGroups()) {
                insertPolicyGroup(accessPolicy.getIdentifier(), groupIdentifier);
            }
        }
    }

    protected void insertPolicyGroup(final String policyIdentifier, final String groupIdentifier) {
        final String policyGroupSql = "INSERT INTO APP_POLICY_GROUP(POLICY_IDENTIFIER, GROUP_IDENTIFIER) VALUES (?, ?)";
        jdbcTemplate.update(policyGroupSql, policyIdentifier, groupIdentifier);
        refreshAccessPolicyHolder();
    }

    protected void insertPolicyUser(final String policyIdentifier, final String userIdentifier) {
        final String policyUserSql = "INSERT INTO APP_POLICY_USER(POLICY_IDENTIFIER, USER_IDENTIFIER) VALUES (?, ?)";
        jdbcTemplate.update(policyUserSql, policyIdentifier, userIdentifier);
        refreshAccessPolicyHolder();
    }

    protected DatabaseAccessPolicy getDatabaseAcessPolicy(final String policyIdentifier) {
        final String sql = "SELECT * FROM APP_POLICY WHERE IDENTIFIER = ?";
        return queryForObject(sql, new DatabaseAccessPolicyRowMapper(), policyIdentifier);
    }

    protected AccessPolicy mapTopAccessPolicy(final DatabaseAccessPolicy databaseAccessPolicy, final Set<String> userIdentifiers, final Set<String> groupIdentifiers) {
        return new AccessPolicy.Builder()
                .identifier(databaseAccessPolicy.getIdentifier())
                .resource(databaseAccessPolicy.getResource())
                .action(RequestAction.valueOfValue(databaseAccessPolicy.getAction()))
                .addUsers(userIdentifiers)
                .addGroups(groupIdentifiers)
                .build();
    }

    protected void populateInitialPolicy(final User initialUser, final ResourceAndAction resourceAndAction) {
        final String userIdentifier = initialUser.getIdentifier();
        final String resourceIdentifier = resourceAndAction.getResource().getIdentifier();
        final RequestAction action = resourceAndAction.getAction();

        final AccessPolicy existingPolicy = getAccessPolicy(resourceIdentifier, action);
        if (existingPolicy == null) {
            // no policy exists for the given resource and action, so create a new one and add the given user
            // we don't need to seed the identifier here since there is only a single external DB
            final AccessPolicy accessPolicy = new AccessPolicy.Builder()
                    .identifierGenerateRandom()
                    .resource(resourceIdentifier)
                    .action(action)
                    .addUser(userIdentifier)
                    .build();

            addAccessPolicy(accessPolicy);
        } else {
            // a policy already exists for the given resource and action, so just associate the user with that policy
            if (existingPolicy.getUsers().contains(initialUser.getIdentifier())) {
                LOGGER.debug("'{}' is already part of the policy for {} {}", initialUser.getIdentity(), action, resourceIdentifier);
            } else {
                LOGGER.debug("Adding '{}' to the policy for {} {}", initialUser.getIdentity(), action, resourceIdentifier);
                insertPolicyUser(existingPolicy.getIdentifier(), userIdentifier);
            }
        }
    }

    protected void populateInitialPolicy(final Group initialGroup, final ResourceAndAction resourceAndAction) {
        final String resourceIdentifier = resourceAndAction.getResource().getIdentifier();
        final RequestAction action = resourceAndAction.getAction();

        final AccessPolicy existingPolicy = getAccessPolicy(resourceIdentifier, action);
        if (existingPolicy == null) {
            // no policy exists for the given resource and action, so create a new one and add the given group
            // we don't need to seed the identifier here since there is only a single external DB
            final AccessPolicy accessPolicy = new AccessPolicy.Builder()
                    .identifierGenerateRandom()
                    .resource(resourceIdentifier)
                    .action(action)
                    .addGroup(initialGroup.getIdentifier())
                    .build();

            addAccessPolicy(accessPolicy);
        } else {
            // a policy already exists for the given resource and action, so just associate the group with that policy
            insertPolicyGroup(existingPolicy.getIdentifier(), initialGroup.getIdentifier());
        }
    }

    private synchronized void refreshAccessPolicyHolder() {
        final Set<AccessPolicy> allPolicies = getDatabaseAccessPolicies();
        this.accessPoliciesHolder.set(new DatabaseAccessPolicyHolder(allPolicies));
    }

    //-- util methods

    protected <T> T queryForObject(final String sql, final RowMapper<T> rowMapper, final Object... args) {
        try {
            return jdbcTemplate.queryForObject(sql, rowMapper, args);
        } catch (final EmptyResultDataAccessException e) {
            return null;
        }
    }

}
