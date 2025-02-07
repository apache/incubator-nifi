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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.security.authorization.AbstractConfigurableAccessPolicyProvider;
import org.apache.nifi.registry.security.authorization.AccessPolicy;
import org.apache.nifi.registry.security.authorization.AccessPolicyProviderInitializationContext;
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.authorization.Group;
import org.apache.nifi.registry.security.authorization.RequestAction;
import org.apache.nifi.registry.security.authorization.User;
import org.apache.nifi.registry.security.authorization.annotation.AuthorizerContext;
import org.apache.nifi.registry.security.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.registry.security.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.registry.security.authorization.file.generated.Authorizations;
import org.apache.nifi.registry.security.authorization.file.generated.Policies;
import org.apache.nifi.registry.security.authorization.file.generated.Policy;
import org.apache.nifi.registry.security.authorization.util.AccessPolicyProviderUtils;
import org.apache.nifi.registry.security.authorization.util.InitialPolicies;
import org.apache.nifi.registry.security.authorization.util.ResourceAndAction;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.exception.SecurityProviderDestructionException;
import org.apache.nifi.registry.security.identity.IdentityMapper;
import org.apache.nifi.registry.util.PropertyValue;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.parsers.DocumentProvider;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class FileAccessPolicyProvider extends AbstractConfigurableAccessPolicyProvider {

    private static final Logger logger = LoggerFactory.getLogger(FileAccessPolicyProvider.class);

    private static final String AUTHORIZATIONS_XSD = "/authorizations.xsd";
    private static final String JAXB_AUTHORIZATIONS_PATH = "org.apache.nifi.registry.security.authorization.file.generated";

    private static final JAXBContext JAXB_AUTHORIZATIONS_CONTEXT = initializeJaxbContext(JAXB_AUTHORIZATIONS_PATH);

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext(final String contextPath) {
        try {
            return JAXBContext.newInstance(contextPath, FileAuthorizer.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    private static final XMLOutputFactory XML_OUTPUT_FACTORY = XMLOutputFactory.newInstance();

    private static final String POLICY_ELEMENT = "policy";
    private static final String POLICY_USER_ELEMENT = "policyUser";
    private static final String POLICY_GROUP_ELEMENT = "policyGroup";
    private static final String IDENTIFIER_ATTR = "identifier";
    private static final String RESOURCE_ATTR = "resource";
    private static final String ACTIONS_ATTR = "actions";

    /* These codes must match the enumeration values set in authorizations.xsd */
    static final String READ_CODE = "R";
    static final String WRITE_CODE = "W";
    static final String DELETE_CODE = "D";

    static final String PROP_AUTHORIZATIONS_FILE = "Authorizations File";

    private IdentityMapper identityMapper;
    private Schema authorizationsSchema;
    private File authorizationsFile;
    private String initialAdminIdentity;
    private Set<String> nifiIdentities;
    private String nifiGroupName;

    private final AtomicReference<AuthorizationsHolder> authorizationsHolder = new AtomicReference<>();

    @Override
    public void doInitialize(final AccessPolicyProviderInitializationContext initializationContext) throws SecurityProviderCreationException {
        try {
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            authorizationsSchema = schemaFactory.newSchema(FileAuthorizer.class.getResource(AUTHORIZATIONS_XSD));
        } catch (Exception e) {
            throw new SecurityProviderCreationException(e);
        }
    }

    @Override
    public void doOnConfigured(final AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
        try {
            final PropertyValue authorizationsPath = configurationContext.getProperty(PROP_AUTHORIZATIONS_FILE);
            if (StringUtils.isBlank(authorizationsPath.getValue())) {
                throw new SecurityProviderCreationException("The authorizations file must be specified.");
            }

            // get the authorizations file and ensure it exists
            authorizationsFile = new File(authorizationsPath.getValue());
            if (!authorizationsFile.exists()) {
                logger.info("Creating new authorizations file at {}", authorizationsFile.getAbsolutePath());
                saveAuthorizations(new Authorizations());
            }

            // get the value of the initial admin identity
            initialAdminIdentity = AccessPolicyProviderUtils.getInitialAdminIdentity(configurationContext, identityMapper);

            // extract any nifi identities
            nifiIdentities = AccessPolicyProviderUtils.getNiFiIdentities(configurationContext, identityMapper);

            // extract the group for nifi identities, if one exists
            nifiGroupName = AccessPolicyProviderUtils.getNiFiGroupName(configurationContext, identityMapper);

            // load the authorizations
            load();

            logger.info("Authorizations file loaded at {}", new Date());
        } catch (JAXBException | SAXException e) {
            throw new SecurityProviderCreationException(e);
        }
    }

    @Override
    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
        return authorizationsHolder.get().getAllPolicies();
    }

    @Override
    public synchronized AccessPolicy addAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        // create the new JAXB Policy
        final Policy policy = createJAXBPolicy(accessPolicy);

        // add the new Policy to the top-level list of policies
        final AuthorizationsHolder holder = authorizationsHolder.get();
        final Authorizations authorizations = holder.getAuthorizations();
        authorizations.getPolicies().getPolicy().add(policy);

        saveAndRefreshHolder(authorizations);

        return authorizationsHolder.get().getPoliciesById().get(accessPolicy.getIdentifier());
    }

    @Override
    public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
        if (identifier == null) {
            return null;
        }

        final AuthorizationsHolder holder = authorizationsHolder.get();
        return holder.getPoliciesById().get(identifier);
    }

    @Override
    public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
        return AccessPolicyProviderUtils.getAccessPolicy(
                resourceIdentifier, action, authorizationsHolder.get().getPoliciesByResource());
    }

    @Override
    public synchronized AccessPolicy updateAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        final AuthorizationsHolder holder = this.authorizationsHolder.get();
        final Authorizations authorizations = holder.getAuthorizations();

        // try to find an existing Authorization that matches the policy id
        Policy updatePolicy = null;
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            if (policy.getIdentifier().equals(accessPolicy.getIdentifier())) {
                updatePolicy = policy;
                break;
            }
        }

        // no matching Policy so return null
        if (updatePolicy == null) {
            return null;
        }

        // update the Policy, save, reload, and return
        transferUsersAndGroups(accessPolicy, updatePolicy);
        saveAndRefreshHolder(authorizations);

        return this.authorizationsHolder.get().getPoliciesById().get(accessPolicy.getIdentifier());
    }

    @Override
    public synchronized AccessPolicy deleteAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
        if (accessPolicy == null) {
            throw new IllegalArgumentException("AccessPolicy cannot be null");
        }

        return deleteAccessPolicy(accessPolicy.getIdentifier());
    }

    private synchronized AccessPolicy deleteAccessPolicy(String accessPolicyIdentifer) throws AuthorizationAccessException {
        if (accessPolicyIdentifer == null) {
            throw new IllegalArgumentException("Access policy identifier cannot be null");
        }

        final AuthorizationsHolder holder = this.authorizationsHolder.get();
        AccessPolicy deletedPolicy = holder.getPoliciesById().get(accessPolicyIdentifer);
        if (deletedPolicy == null) {
            return null;
        }

        // find the matching Policy and remove it
        final Authorizations authorizations = holder.getAuthorizations();
        Iterator<Policy> policyIter = authorizations.getPolicies().getPolicy().iterator();
        while (policyIter.hasNext()) {
            final Policy policy = policyIter.next();
            if (policy.getIdentifier().equals(accessPolicyIdentifer)) {
                policyIter.remove();
                break;
            }
        }

        saveAndRefreshHolder(authorizations);
        return deletedPolicy;
    }

    AuthorizationsHolder getAuthorizationsHolder() {
        return authorizationsHolder.get();
    }

    @AuthorizerContext
    public void setIdentityMapper(final IdentityMapper identityMapper) {
        this.identityMapper = identityMapper;
    }

    @Override
    public synchronized void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
        parsePolicies(fingerprint).forEach(policy -> addAccessPolicy(policy));
    }

    @Override
    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
        try {
            // ensure we can understand the proposed fingerprint
            parsePolicies(proposedFingerprint);
        } catch (final AuthorizationAccessException e) {
            throw new UninheritableAuthorizationsException("Unable to parse the proposed fingerprint: " + e);
        }

        // ensure we are in a proper state to inherit the fingerprint
        if (!getAccessPolicies().isEmpty()) {
            throw new UninheritableAuthorizationsException("Proposed fingerprint is not inheritable because the current access policies is not empty.");
        }
    }

    @Override
    public String getFingerprint() throws AuthorizationAccessException {
        final List<AccessPolicy> policies = new ArrayList<>(getAccessPolicies());
        Collections.sort(policies, Comparator.comparing(AccessPolicy::getIdentifier));

        XMLStreamWriter writer = null;
        final StringWriter out = new StringWriter();
        try {
            writer = XML_OUTPUT_FACTORY.createXMLStreamWriter(out);
            writer.writeStartDocument();
            writer.writeStartElement("accessPolicies");

            for (AccessPolicy policy : policies) {
                writePolicy(writer, policy);
            }

            writer.writeEndElement();
            writer.writeEndDocument();
            writer.flush();
        } catch (XMLStreamException e) {
            throw new AuthorizationAccessException("Unable to generate fingerprint", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (XMLStreamException ignored) {
                    // nothing to do here
                }
            }
        }

        return out.toString();
    }

    private List<AccessPolicy> parsePolicies(final String fingerprint) {
        final List<AccessPolicy> policies = new ArrayList<>();

        final byte[] fingerprintBytes = fingerprint.getBytes(StandardCharsets.UTF_8);
        try (final ByteArrayInputStream in = new ByteArrayInputStream(fingerprintBytes)) {
            final DocumentProvider documentProvider = new StandardDocumentProvider();
            final Document document = documentProvider.parse(in);
            final Element rootElement = document.getDocumentElement();

            // parse all the policies and add them to the current access policy provider
            NodeList policyNodes = rootElement.getElementsByTagName(POLICY_ELEMENT);
            for (int i = 0; i < policyNodes.getLength(); i++) {
                Node policyNode = policyNodes.item(i);
                policies.add(parsePolicy((Element) policyNode));
            }
        } catch (final ProcessingException | IOException e) {
            throw new AuthorizationAccessException("Unable to parse fingerprint", e);
        }

        return policies;
    }

    private AccessPolicy parsePolicy(final Element element) {
        final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                .identifier(element.getAttribute(IDENTIFIER_ATTR))
                .resource(element.getAttribute(RESOURCE_ATTR));

        final String actions = element.getAttribute(ACTIONS_ATTR);
        if (actions.equals(RequestAction.READ.name())) {
            builder.action(RequestAction.READ);
        } else if (actions.equals(RequestAction.WRITE.name())) {
            builder.action(RequestAction.WRITE);
        } else if (actions.equals(RequestAction.DELETE.name())) {
            builder.action(RequestAction.DELETE);
        } else {
            throw new IllegalStateException("Unknown Policy Action: " + actions);
        }

        NodeList policyUsers = element.getElementsByTagName(POLICY_USER_ELEMENT);
        for (int i = 0; i < policyUsers.getLength(); i++) {
            Element policyUserNode = (Element) policyUsers.item(i);
            builder.addUser(policyUserNode.getAttribute(IDENTIFIER_ATTR));
        }

        NodeList policyGroups = element.getElementsByTagName(POLICY_GROUP_ELEMENT);
        for (int i = 0; i < policyGroups.getLength(); i++) {
            Element policyGroupNode = (Element) policyGroups.item(i);
            builder.addGroup(policyGroupNode.getAttribute(IDENTIFIER_ATTR));
        }

        return builder.build();
    }

    private void writePolicy(final XMLStreamWriter writer, final AccessPolicy policy) throws XMLStreamException {
        // sort the users for the policy
        List<String> policyUsers = new ArrayList<>(policy.getUsers());
        Collections.sort(policyUsers);

        // sort the groups for this policy
        List<String> policyGroups = new ArrayList<>(policy.getGroups());
        Collections.sort(policyGroups);

        writer.writeStartElement(POLICY_ELEMENT);
        writer.writeAttribute(IDENTIFIER_ATTR, policy.getIdentifier());
        writer.writeAttribute(RESOURCE_ATTR, policy.getResource());
        writer.writeAttribute(ACTIONS_ATTR, policy.getAction().name());

        for (String policyUser : policyUsers) {
            writer.writeStartElement(POLICY_USER_ELEMENT);
            writer.writeAttribute(IDENTIFIER_ATTR, policyUser);
            writer.writeEndElement();
        }

        for (String policyGroup : policyGroups) {
            writer.writeStartElement(POLICY_GROUP_ELEMENT);
            writer.writeAttribute(IDENTIFIER_ATTR, policyGroup);
            writer.writeEndElement();
        }

        writer.writeEndElement();
    }

    /**
     * Loads the authorizations file and populates the AuthorizationsHolder, only called during start-up.
     *
     * @throws JAXBException            Unable to reload the authorized users file
     */
    private synchronized void load() throws JAXBException, SAXException {
        // attempt to unmarshal
        final Authorizations authorizations = unmarshallAuthorizations();
        if (authorizations.getPolicies() == null) {
            authorizations.setPolicies(new Policies());
        }

        final AuthorizationsHolder authorizationsHolder = new AuthorizationsHolder(authorizations);
        final boolean emptyAuthorizations = authorizationsHolder.getAllPolicies().isEmpty();
        final boolean hasInitialAdminIdentity = (initialAdminIdentity != null && !StringUtils.isBlank(initialAdminIdentity));
        final boolean hasNiFiIdentities = (nifiIdentities != null && !nifiIdentities.isEmpty());

        // if we are starting fresh then we might need to populate an initial admin
        if (emptyAuthorizations) {
            if (hasInitialAdminIdentity) {
               logger.info("Populating authorizations for Initial Admin: '{}'", initialAdminIdentity);
               populateInitialAdmin(authorizations);
            }

            if (hasNiFiIdentities) {
                logger.info("Populating authorizations for NiFi identities: [{}]", StringUtils.join(nifiIdentities, ";"));
                populateNiFiIdentities(authorizations);
            }

            if (!StringUtils.isEmpty(nifiGroupName)) {
                logger.info("Populating authorizations for NiFi identity group: [{}]", nifiGroupName);
                populateNiFiGroup(authorizations);
            }

            saveAndRefreshHolder(authorizations);
        } else {
            this.authorizationsHolder.set(authorizationsHolder);
        }
    }

    private void saveAuthorizations(final Authorizations authorizations) throws JAXBException {
        final Marshaller marshaller = JAXB_AUTHORIZATIONS_CONTEXT.createMarshaller();
        marshaller.setSchema(authorizationsSchema);
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        marshaller.marshal(authorizations, authorizationsFile);
    }

    private Authorizations unmarshallAuthorizations() throws JAXBException {
        final Unmarshaller unmarshaller = JAXB_AUTHORIZATIONS_CONTEXT.createUnmarshaller();
        unmarshaller.setSchema(authorizationsSchema);

        final JAXBElement<Authorizations> element = unmarshaller.unmarshal(new StreamSource(authorizationsFile), Authorizations.class);
        return element.getValue();
    }

    /**
     *  Creates the initial admin user and sets policies managing buckets, users, and policies.
     */
    private void populateInitialAdmin(final Authorizations authorizations) {
        final User initialAdmin = getUserGroupProvider().getUserByIdentity(initialAdminIdentity);
        if (initialAdmin == null) {
            throw new SecurityProviderCreationException("Unable to locate initial admin " + initialAdminIdentity + " to seed policies");
        }

        for (final ResourceAndAction resourceAction : InitialPolicies.ADMIN_POLICIES) {
            final String resource = resourceAction.getResource().getIdentifier();
            final String actionCode = getActionCode(resourceAction.getAction());
            addUserToAccessPolicy(authorizations, resource, initialAdmin.getIdentifier(), actionCode);
        }
    }

    /**
     * Creates a user for each NiFi client and gives each one write permission to /proxy.
     *
     * @param authorizations the overall authorizations
     */
    private void populateNiFiIdentities(final Authorizations authorizations) {
        for (String nifiIdentity : nifiIdentities) {
            final User nifiUser = getUserGroupProvider().getUserByIdentity(nifiIdentity);
            if (nifiUser == null) {
                throw new SecurityProviderCreationException("Unable to locate NiFi identities'" + nifiIdentity + "' to seed policies.");
            }

            // grant access to the resources needed for initial nifi identities
            for (final ResourceAndAction resourceAction : InitialPolicies.NIFI_POLICIES) {
                final String resource = resourceAction.getResource().getIdentifier();
                final String actionCode = getActionCode(resourceAction.getAction());
                addUserToAccessPolicy(authorizations, resource, nifiUser.getIdentifier(), actionCode);
            }
        }
    }

    /**
     * Populates the authorizations for the NiFi Group.
     *
     * @param authorizations the overall authorizations
     */
    private void populateNiFiGroup(final Authorizations authorizations) {
        final Group nifiGroup = AccessPolicyProviderUtils.getGroup(nifiGroupName, getUserGroupProvider());

        // grant access to the resources needed for initial nifi-proxy identities
        for (final ResourceAndAction resourceAction : InitialPolicies.NIFI_POLICIES) {
            final String resource = resourceAction.getResource().getIdentifier();
            final String actionCode = getActionCode(resourceAction.getAction());
            addGroupToAccessPolicy(authorizations, resource, nifiGroup.getIdentifier(), actionCode);
        }
    }

    private void addGroupToAccessPolicy(final Authorizations authorizations, final String resource, final String groupIdentifier, final String actionCode) {
        Optional<Policy> policyOptional = authorizations.getPolicies().getPolicy().stream()
                .filter(policy -> policy.getResource().equals(resource))
                .filter(policy -> policy.getAction().equals(actionCode))
            .findAny();
        if (policyOptional.isPresent()) {
            Policy policy = policyOptional.get();
            Policy.Group group = new Policy.Group();
            group.setIdentifier(groupIdentifier);
            policy.getGroup().add(group);
        } else {
            AccessPolicy.Builder accessPolicyBuilder =
                    new AccessPolicy.Builder()
                            .identifierGenerateFromSeed(resource + actionCode)
                            .resource(resource)
                            .addGroup(groupIdentifier)
                            .action(getAction(actionCode));

            authorizations.getPolicies().getPolicy().add(createJAXBPolicy(accessPolicyBuilder.build()));
        }
    }

    /**
     * Creates and adds an access policy for the given resource, identity, and actions to the specified authorizations.
     *
     * @param authorizations the Authorizations instance to add the policy to
     * @param resource the resource for the policy
     * @param userIdentifier the identifier for the user to add to the policy
     * @param actionCode the action for the policy
     */
    private void addUserToAccessPolicy(final Authorizations authorizations, final String resource, final String userIdentifier, final String actionCode) {
        // first try to find an existing policy for the given resource and action
        Policy foundPolicy = null;
        for (Policy policy : authorizations.getPolicies().getPolicy()) {
            if (policy.getResource().equals(resource) && policy.getAction().equals(actionCode)) {
                foundPolicy = policy;
                break;
            }
        }

        if (foundPolicy == null) {
            // if we didn't find an existing policy create a new one
            final String uuidSeed = resource + actionCode;

            final AccessPolicy.Builder builder = new AccessPolicy.Builder()
                    .identifierGenerateFromSeed(uuidSeed)
                    .resource(resource)
                    .addUser(userIdentifier)
                    .action(getAction(actionCode));

            final AccessPolicy accessPolicy = builder.build();
            final Policy jaxbPolicy = createJAXBPolicy(accessPolicy);
            authorizations.getPolicies().getPolicy().add(jaxbPolicy);
        } else {
            // otherwise add the user to the existing policy
            Policy.User policyUser = new Policy.User();
            policyUser.setIdentifier(userIdentifier);
            foundPolicy.getUser().add(policyUser);
        }
    }

    private Policy createJAXBPolicy(final AccessPolicy accessPolicy) {
        final Policy policy = new Policy();
        policy.setIdentifier(accessPolicy.getIdentifier());
        policy.setResource(accessPolicy.getResource());
        policy.setAction(getActionCode(accessPolicy.getAction()));
        transferUsersAndGroups(accessPolicy, policy);
        return policy;
    }

    private String getActionCode(final RequestAction action) {
        switch (action) {
            case READ:
                return READ_CODE;
            case WRITE:
                return WRITE_CODE;
            case DELETE:
                return DELETE_CODE;
            default:
                throw new IllegalStateException("Unknown action: " + action);
        }
    }

    private RequestAction getAction(final String actionCode) {
        switch (actionCode) {
            case READ_CODE:
                return RequestAction.READ;
            case WRITE_CODE:
                return RequestAction.WRITE;
            case DELETE_CODE:
                return RequestAction.DELETE;
            default:
                throw new IllegalStateException("Unknown action: " + actionCode);
        }
    }

    /**
     * Sets the given Policy to the state of the provided AccessPolicy. Users and Groups will be cleared and
     * set to match the AccessPolicy, the resource and action will be set to match the AccessPolicy.
     *
     * Does not set the identifier.
     *
     * @param accessPolicy the AccessPolicy to transfer state from
     * @param policy the Policy to transfer state to
     */
    private void transferUsersAndGroups(AccessPolicy accessPolicy, Policy policy) {
        // add users to the policy
        policy.getUser().clear();
        for (String userIdentifier : accessPolicy.getUsers()) {
            Policy.User policyUser = new Policy.User();
            policyUser.setIdentifier(userIdentifier);
            policy.getUser().add(policyUser);
        }

        // add groups to the policy
        policy.getGroup().clear();
        for (String groupIdentifier : accessPolicy.getGroups()) {
            Policy.Group policyGroup = new Policy.Group();
            policyGroup.setIdentifier(groupIdentifier);
            policy.getGroup().add(policyGroup);
        }
    }

    /**
     * Saves the Authorizations instance by marshalling to a file, then re-populates the
     * in-memory data structures and sets the new holder.
     *
     * Synchronized to ensure only one thread writes the file at a time.
     *
     * @param authorizations the authorizations to save and populate from
     * @throws AuthorizationAccessException if an error occurs saving the authorizations
     */
    private synchronized void saveAndRefreshHolder(final Authorizations authorizations) throws AuthorizationAccessException {
        try {
            saveAuthorizations(authorizations);

            this.authorizationsHolder.set(new AuthorizationsHolder(authorizations));
        } catch (JAXBException e) {
            throw new AuthorizationAccessException("Unable to save Authorizations", e);
        }
    }

    @Override
    public void preDestruction() throws SecurityProviderDestructionException {
    }

}
