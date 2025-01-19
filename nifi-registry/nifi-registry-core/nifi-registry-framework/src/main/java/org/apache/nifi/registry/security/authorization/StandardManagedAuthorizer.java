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
package org.apache.nifi.registry.security.authorization;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.security.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.registry.security.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.exception.SecurityProviderDestructionException;
import org.apache.nifi.registry.util.PropertyValue;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.parsers.DocumentProvider;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class StandardManagedAuthorizer implements ManagedAuthorizer {

    private static final XMLOutputFactory XML_OUTPUT_FACTORY = XMLOutputFactory.newInstance();

    private static final String USER_GROUP_PROVIDER_ELEMENT = "userGroupProvider";
    private static final String ACCESS_POLICY_PROVIDER_ELEMENT = "accessPolicyProvider";

    private AccessPolicyProviderLookup accessPolicyProviderLookup;
    private AccessPolicyProvider accessPolicyProvider;
    private UserGroupProvider userGroupProvider;

    public StandardManagedAuthorizer() { }

    // exposed for testing to inject mocks
    public StandardManagedAuthorizer(AccessPolicyProvider accessPolicyProvider, UserGroupProvider userGroupProvider) {
        this.accessPolicyProvider = accessPolicyProvider;
        this.userGroupProvider = userGroupProvider;
    }

    @Override
    public void initialize(AuthorizerInitializationContext initializationContext) throws SecurityProviderCreationException {
        accessPolicyProviderLookup = initializationContext.getAccessPolicyProviderLookup();
    }

    @Override
    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
        final PropertyValue accessPolicyProviderKey = configurationContext.getProperty("Access Policy Provider");
        if (!accessPolicyProviderKey.isSet()) {
            throw new SecurityProviderCreationException("The Access Policy Provider must be set.");
        }

        accessPolicyProvider = accessPolicyProviderLookup.getAccessPolicyProvider(accessPolicyProviderKey.getValue());

        // ensure the desired access policy provider was found
        if (accessPolicyProvider == null) {
            throw new SecurityProviderCreationException(String.format("Unable to locate configured Access Policy Provider: %s", accessPolicyProviderKey));
        }

        userGroupProvider = accessPolicyProvider.getUserGroupProvider();

        // ensure the desired access policy provider has a user group provider
        if (userGroupProvider == null) {
            throw new SecurityProviderCreationException(String.format("Configured Access Policy Provider %s does not contain a User Group Provider", accessPolicyProviderKey));
        }
    }

    @Override
    public AuthorizationResult authorize(AuthorizationRequest request) throws AuthorizationAccessException {
        final String resourceIdentifier = request.getResource().getIdentifier();
        final AccessPolicy policy = accessPolicyProvider.getAccessPolicy(resourceIdentifier, request.getAction());
        if (policy == null) {
            return AuthorizationResult.resourceNotFound();
        }

        final UserAndGroups userAndGroups = userGroupProvider.getUserAndGroups(request.getIdentity());

        // combine groups from incoming request with groups from UserAndGroups because the request may contain groups from
        // an external identity provider and the membership may not be maintained within any of the UserGroupProviders
        final Set<Group> userGroups = new HashSet<>();
        userGroups.addAll(userAndGroups.getGroups() == null ? Collections.emptySet() : userAndGroups.getGroups());
        userGroups.addAll(getGroups(request.getGroups()));

        if (containsUser(userAndGroups.getUser(), policy) || containsGroup(userGroups, policy)) {
            return AuthorizationResult.approved();
        }

        return AuthorizationResult.denied(request.getExplanationSupplier().get());
    }

    private Set<Group> getGroups(final Set<String> groupNames) {
        if (groupNames == null || groupNames.isEmpty()) {
            return Collections.emptySet();
        }

        return userGroupProvider.getGroups().stream()
                .filter(group -> groupNames.contains(group.getName()))
                .collect(Collectors.toSet());
    }

    /**
     * Determines if the policy contains one of the user's groups.
     *
     * @param userGroups the set of the user's groups
     * @param policy the policy
     * @return true if one of the Groups in userGroups is contained in the policy
     */
    private boolean containsGroup(final Set<Group> userGroups, final AccessPolicy policy) {
        if (userGroups == null || userGroups.isEmpty() || policy.getGroups().isEmpty()) {
            return false;
        }

        for (Group userGroup : userGroups) {
            if (policy.getGroups().contains(userGroup.getIdentifier())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Determines if the policy contains the user's identifier.
     *
     * @param user the user
     * @param policy the policy
     * @return true if the user is non-null and the user's identifies is contained in the policy's users
     */
    private boolean containsUser(final User user, final AccessPolicy policy) {
        if (user == null || policy.getUsers().isEmpty()) {
            return false;
        }
        return policy.getUsers().contains(user.getIdentifier());
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        XMLStreamWriter writer = null;
        final StringWriter out = new StringWriter();
        try {
            writer = XML_OUTPUT_FACTORY.createXMLStreamWriter(out);
            writer.writeStartDocument();
            writer.writeStartElement("managedAuthorizations");

            writer.writeStartElement(ACCESS_POLICY_PROVIDER_ELEMENT);
            if (accessPolicyProvider instanceof ConfigurableAccessPolicyProvider) {
                writer.writeCharacters(((ConfigurableAccessPolicyProvider) accessPolicyProvider).getFingerprint());
            }
            writer.writeEndElement();

            writer.writeStartElement(USER_GROUP_PROVIDER_ELEMENT);
            if (userGroupProvider instanceof ConfigurableUserGroupProvider) {
                writer.writeCharacters(((ConfigurableUserGroupProvider) userGroupProvider).getFingerprint());
            }
            writer.writeEndElement();

            writer.writeEndElement();
            writer.writeEndDocument();
            writer.flush();
        } catch (XMLStreamException e) {
            throw new AuthorizationAccessException("Unable to generate fingerprint", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (XMLStreamException e) {
                    // nothing to do here
                }
            }
        }

        return out.toString();
    }

    @Override
    public void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
        if (StringUtils.isBlank(fingerprint)) {
            return;
        }

        final FingerprintHolder fingerprintHolder = parseFingerprint(fingerprint);

        if (StringUtils.isNotBlank(fingerprintHolder.getPolicyFingerprint()) && accessPolicyProvider instanceof ConfigurableAccessPolicyProvider) {
            ((ConfigurableAccessPolicyProvider) accessPolicyProvider).inheritFingerprint(fingerprintHolder.getPolicyFingerprint());
        }

        if (StringUtils.isNotBlank(fingerprintHolder.getUserGroupFingerprint()) && userGroupProvider instanceof ConfigurableUserGroupProvider) {
            ((ConfigurableUserGroupProvider) userGroupProvider).inheritFingerprint(fingerprintHolder.getUserGroupFingerprint());
        }
    }

    @Override
    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
        final FingerprintHolder fingerprintHolder = parseFingerprint(proposedFingerprint);

        if (StringUtils.isNotBlank(fingerprintHolder.getPolicyFingerprint())) {
            if (accessPolicyProvider instanceof ConfigurableAccessPolicyProvider) {
                ((ConfigurableAccessPolicyProvider) accessPolicyProvider).checkInheritability(fingerprintHolder.getPolicyFingerprint());
            } else {
                throw new UninheritableAuthorizationsException("Policy fingerprint is not blank and the configured AccessPolicyProvider does not support fingerprinting.");
            }
        }

        if (StringUtils.isNotBlank(fingerprintHolder.getUserGroupFingerprint())) {
            if (userGroupProvider instanceof ConfigurableUserGroupProvider) {
                ((ConfigurableUserGroupProvider) userGroupProvider).checkInheritability(fingerprintHolder.getUserGroupFingerprint());
            } else {
                throw new UninheritableAuthorizationsException("User/Group fingerprint is not blank and the configured UserGroupProvider does not support fingerprinting.");
            }
        }
    }

    private final FingerprintHolder parseFingerprint(final String fingerprint) throws AuthorizationAccessException {
        final byte[] fingerprintBytes = fingerprint.getBytes(StandardCharsets.UTF_8);

        try (final ByteArrayInputStream in = new ByteArrayInputStream(fingerprintBytes)) {
            final DocumentProvider documentProvider = new StandardDocumentProvider();
            final Document document = documentProvider.parse(in);
            final Element rootElement = document.getDocumentElement();

            final NodeList accessPolicyProviderList = rootElement.getElementsByTagName(ACCESS_POLICY_PROVIDER_ELEMENT);
            if (accessPolicyProviderList.getLength() != 1) {
                throw new AuthorizationAccessException(String.format("Only one %s element is allowed: %s", ACCESS_POLICY_PROVIDER_ELEMENT, fingerprint));
            }

            final NodeList userGroupProviderList = rootElement.getElementsByTagName(USER_GROUP_PROVIDER_ELEMENT);
            if (userGroupProviderList.getLength() != 1) {
                throw new AuthorizationAccessException(String.format("Only one %s element is allowed: %s", USER_GROUP_PROVIDER_ELEMENT, fingerprint));
            }

            final Node accessPolicyProvider = accessPolicyProviderList.item(0);
            final Node userGroupProvider = userGroupProviderList.item(0);
            return new FingerprintHolder(accessPolicyProvider.getTextContent(), userGroupProvider.getTextContent());
        } catch (final ProcessingException | IOException e) {
            throw new AuthorizationAccessException("Unable to parse fingerprint", e);
        }
    }

    @Override
    public AccessPolicyProvider getAccessPolicyProvider() {
        return accessPolicyProvider;
    }

    @Override
    public void preDestruction() throws SecurityProviderDestructionException {

    }

    private static class FingerprintHolder {
        private final String policyFingerprint;
        private final String userGroupFingerprint;

        public FingerprintHolder(String policyFingerprint, String userGroupFingerprint) {
            this.policyFingerprint = policyFingerprint;
            this.userGroupFingerprint = userGroupFingerprint;
        }

        public String getPolicyFingerprint() {
            return policyFingerprint;
        }

        public String getUserGroupFingerprint() {
            return userGroupFingerprint;
        }
    }
}
