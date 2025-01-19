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
package org.apache.nifi.registry.properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.ApplicationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class NiFiRegistryProperties extends ApplicationProperties {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryProperties.class);

    public static final String NIFI_REGISTRY_PROPERTIES_FILE_PATH_PROPERTY = "nifi.registry.properties.file.path";
    public static final String NIFI_REGISTRY_BOOTSTRAP_FILE_PATH_PROPERTY = "nifi.registry.bootstrap.config.file.path";
    public static final String NIFI_REGISTRY_BOOTSTRAP_DOCS_DIR_PROPERTY = "nifi.registry.bootstrap.config.docs.dir";

    public static final String RELATIVE_BOOTSTRAP_FILE_LOCATION = "conf/bootstrap.conf";
    public static final String RELATIVE_PROPERTIES_FILE_LOCATION = "conf/nifi-registry.properties";
    public static final String RELATIVE_DOCS_LOCATION = "docs";

    // Keys
    public static final String PROPERTIES_FILE_PATH = "nifi.registry.properties.file.path";
    public static final String WEB_WAR_DIR = "nifi.registry.web.war.directory";
    public static final String WEB_HTTP_PORT = "nifi.registry.web.http.port";
    public static final String WEB_HTTP_HOST = "nifi.registry.web.http.host";
    public static final String WEB_HTTPS_PORT = "nifi.registry.web.https.port";
    public static final String WEB_HTTPS_HOST = "nifi.registry.web.https.host";
    public static final String WEB_HTTPS_NETWORK_INTERFACE_PREFIX = "nifi.registry.web.https.network.interface.";
    public static final String WEB_HTTPS_CIPHERSUITES_INCLUDE = "nifi.registry.web.https.ciphersuites.include";
    public static final String WEB_HTTPS_CIPHERSUITES_EXCLUDE = "nifi.registry.web.https.ciphersuites.exclude";
    public static final String WEB_HTTPS_APPLICATION_PROTOCOLS = "nifi.registry.web.https.application.protocols";


    public static final String WEB_WORKING_DIR = "nifi.registry.web.jetty.working.directory";
    public static final String WEB_THREADS = "nifi.registry.web.jetty.threads";
    public static final String WEB_SHOULD_SEND_SERVER_VERSION = "nifi.registry.web.should.send.server.version";

    public static final String SECURITY_KEYSTORE = "nifi.registry.security.keystore";
    public static final String SECURITY_KEYSTORE_TYPE = "nifi.registry.security.keystoreType";
    public static final String SECURITY_KEYSTORE_PASSWD = "nifi.registry.security.keystorePasswd";
    public static final String SECURITY_KEY_PASSWD = "nifi.registry.security.keyPasswd";
    public static final String SECURITY_TRUSTSTORE = "nifi.registry.security.truststore";
    public static final String SECURITY_TRUSTSTORE_TYPE = "nifi.registry.security.truststoreType";
    public static final String SECURITY_TRUSTSTORE_PASSWD = "nifi.registry.security.truststorePasswd";
    public static final String SECURITY_NEED_CLIENT_AUTH = "nifi.registry.security.needClientAuth";
    public static final String SECURITY_AUTHORIZERS_CONFIGURATION_FILE = "nifi.registry.security.authorizers.configuration.file";
    public static final String SECURITY_AUTHORIZER = "nifi.registry.security.authorizer";
    public static final String SECURITY_IDENTITY_PROVIDERS_CONFIGURATION_FILE = "nifi.registry.security.identity.providers.configuration.file";
    public static final String SECURITY_IDENTITY_PROVIDER = "nifi.registry.security.identity.provider";
    public static final String SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX = "nifi.registry.security.identity.mapping.pattern.";
    public static final String SECURITY_IDENTITY_MAPPING_VALUE_PREFIX = "nifi.registry.security.identity.mapping.value.";
    public static final String SECURITY_IDENTITY_MAPPING_TRANSFORM_PREFIX = "nifi.registry.security.identity.mapping.transform.";
    public static final String SECURITY_GROUP_MAPPING_PATTERN_PREFIX = "nifi.registry.security.group.mapping.pattern.";
    public static final String SECURITY_GROUP_MAPPING_VALUE_PREFIX = "nifi.registry.security.group.mapping.value.";
    public static final String SECURITY_GROUP_MAPPING_TRANSFORM_PREFIX = "nifi.registry.security.group.mapping.transform.";

    public static final String EXTENSION_DIR_PREFIX = "nifi.registry.extension.dir.";

    public static final String PROVIDERS_CONFIGURATION_FILE = "nifi.registry.providers.configuration.file";
    public static final String REGISTRY_ALIAS_CONFIGURATION_FILE = "nifi.registry.registry.alias.configuration.file";

    public static final String EXTENSIONS_WORKING_DIR = "nifi.registry.extensions.working.directory";

    // Original DB properties
    public static final String DATABASE_DIRECTORY = "nifi.registry.db.directory";
    public static final String DATABASE_URL_APPEND = "nifi.registry.db.url.append";

    // New style DB properties
    public static final String DATABASE_URL = "nifi.registry.db.url";
    public static final String DATABASE_DRIVER_CLASS_NAME = "nifi.registry.db.driver.class";
    public static final String DATABASE_DRIVER_DIR = "nifi.registry.db.driver.directory";
    public static final String DATABASE_USERNAME = "nifi.registry.db.username";
    public static final String DATABASE_PASSWORD = "nifi.registry.db.password";
    public static final String DATABASE_MAX_CONNECTIONS = "nifi.registry.db.maxConnections";
    public static final String DATABASE_SQL_DEBUG = "nifi.registry.db.sql.debug";

    // Kerberos properties
    public static final String KERBEROS_KRB5_FILE = "nifi.registry.kerberos.krb5.file";
    public static final String KERBEROS_SPNEGO_PRINCIPAL = "nifi.registry.kerberos.spnego.principal";
    public static final String KERBEROS_SPNEGO_KEYTAB_LOCATION = "nifi.registry.kerberos.spnego.keytab.location";
    public static final String KERBEROS_SPNEGO_AUTHENTICATION_EXPIRATION = "nifi.registry.kerberos.spnego.authentication.expiration";
    public static final String KERBEROS_SERVICE_PRINCIPAL = "nifi.registry.kerberos.service.principal";
    public static final String KERBEROS_SERVICE_KEYTAB_LOCATION = "nifi.registry.kerberos.service.keytab.location";

    // OIDC properties
    public static final String SECURITY_USER_OIDC_DISCOVERY_URL = "nifi.registry.security.user.oidc.discovery.url";
    public static final String SECURITY_USER_OIDC_CONNECT_TIMEOUT = "nifi.registry.security.user.oidc.connect.timeout";
    public static final String SECURITY_USER_OIDC_READ_TIMEOUT = "nifi.registry.security.user.oidc.read.timeout";
    public static final String SECURITY_USER_OIDC_CLIENT_ID = "nifi.registry.security.user.oidc.client.id";
    public static final String SECURITY_USER_OIDC_CLIENT_SECRET = "nifi.registry.security.user.oidc.client.secret";
    public static final String SECURITY_USER_OIDC_PREFERRED_JWSALGORITHM = "nifi.registry.security.user.oidc.preferred.jwsalgorithm";
    public static final String SECURITY_USER_OIDC_ADDITIONAL_SCOPES = "nifi.registry.security.user.oidc.additional.scopes";
    public static final String SECURITY_USER_OIDC_CLAIM_IDENTIFYING_USER = "nifi.registry.security.user.oidc.claim.identifying.user";
    public static final String SECURITY_USER_OIDC_CLAIM_GROUPS = "nifi.registry.security.user.oidc.claim.groups";

    // Revision Management Properties
    public static final String REVISIONS_ENABLED = "nifi.registry.revisions.enabled";

    // Defaults
    public static final String DEFAULT_WEB_WORKING_DIR = "./work/jetty";
    public static final String DEFAULT_WEB_HTTPS_APPLICATION_PROTOCOLS = "h2 http/1.1";
    public static final String DEFAULT_WAR_DIR = "./lib";
    public static final String DEFAULT_PROVIDERS_CONFIGURATION_FILE = "./conf/providers.xml";
    public static final String DEFAULT_REGISTRY_ALIAS_CONFIGURATION_FILE = "./conf/registry-aliases.xml";
    public static final String DEFAULT_SECURITY_AUTHORIZERS_CONFIGURATION_FILE = "./conf/authorizers.xml";
    public static final String DEFAULT_SECURITY_IDENTITY_PROVIDER_CONFIGURATION_FILE = "./conf/identity-providers.xml";
    public static final String DEFAULT_AUTHENTICATION_EXPIRATION = "12 hours";
    public static final String DEFAULT_EXTENSIONS_WORKING_DIR = "./work/extensions";
    public static final String DEFAULT_WEB_SHOULD_SEND_SERVER_VERSION = "true";
    public static final String DEFAULT_SECURITY_USER_OIDC_CONNECT_TIMEOUT = "5 secs";
    public static final String DEFAULT_SECURITY_USER_OIDC_READ_TIMEOUT = "5 secs";

    public NiFiRegistryProperties() {
        this(Collections.EMPTY_MAP);
    }

    public NiFiRegistryProperties(final Map<String, String> props) {
        super(props);
    }

    public NiFiRegistryProperties(final Properties props) {
        super(props);
    }

    public int getWebThreads() {
        int webThreads = 200;
        try {
            webThreads = Integer.parseInt(getProperty(WEB_THREADS));
        } catch (final NumberFormatException nfe) {
            logger.warn("{} must be an integer value. Defaulting to {}", WEB_THREADS, webThreads);
        }
        return webThreads;
    }

    public Integer getPort() {
        return getPropertyAsInteger(WEB_HTTP_PORT);
    }

    public String getHttpHost() {
        return getProperty(WEB_HTTP_HOST);
    }

    public Integer getSslPort() {
        return getPropertyAsInteger(WEB_HTTPS_PORT);
    }

    public boolean isHTTPSConfigured() {
        return getSslPort() != null;
    }

    public boolean shouldSendServerVersion() {
        return Boolean.parseBoolean(getProperty(WEB_SHOULD_SEND_SERVER_VERSION, DEFAULT_WEB_SHOULD_SEND_SERVER_VERSION));
    }

    public String getHttpsHost() {
        return getProperty(WEB_HTTPS_HOST);
    }

    public String getHttpsCipherSuitesInclude() {
        return getProperty(WEB_HTTPS_CIPHERSUITES_INCLUDE);
    }

    public String getHttpsCipherSuitesExclude() {
        return getProperty(WEB_HTTPS_CIPHERSUITES_EXCLUDE);
    }

    public boolean getNeedClientAuth() {
        boolean needClientAuth = true;
        String rawNeedClientAuth = getProperty(SECURITY_NEED_CLIENT_AUTH);
        if ("false".equalsIgnoreCase(rawNeedClientAuth)) {
            needClientAuth = false;
        }
        return needClientAuth;
    }

    public String getKeyStorePath() {
        return getProperty(SECURITY_KEYSTORE);
    }

    public String getKeyStoreType() {
        return getProperty(SECURITY_KEYSTORE_TYPE);
    }

    public String getKeyStorePassword() {
        return getProperty(SECURITY_KEYSTORE_PASSWD);
    }

    public String getKeyPassword() {
        return getProperty(SECURITY_KEY_PASSWD);
    }

    public String getTrustStorePath() {
        return getProperty(SECURITY_TRUSTSTORE);
    }

    public String getTrustStoreType() {
        return getProperty(SECURITY_TRUSTSTORE_TYPE);
    }

    public String getTrustStorePassword() {
        return getProperty(SECURITY_TRUSTSTORE_PASSWD);
    }

    public File getWarLibDirectory() {
        return new File(getProperty(WEB_WAR_DIR, DEFAULT_WAR_DIR));
    }

    public File getWebWorkingDirectory() {
        return new File(getProperty(WEB_WORKING_DIR, DEFAULT_WEB_WORKING_DIR));
    }

    /**
     * Get Web HTTPS Application Protocols defaults to HTTP/2 and HTTP/1.1
     *
     * @return Set of configured HTTPS Application Protocols
     */
    public Set<String> getWebHttpsApplicationProtocols() {
        final String protocols = getProperty(WEB_HTTPS_APPLICATION_PROTOCOLS, DEFAULT_WEB_HTTPS_APPLICATION_PROTOCOLS);
        return Arrays.stream(protocols.split("\\s+")).collect(Collectors.toSet());
    }

    public File getExtensionsWorkingDirectory() {
        return  new File(getProperty(EXTENSIONS_WORKING_DIR, DEFAULT_EXTENSIONS_WORKING_DIR));
    }

    public File getProvidersConfigurationFile() {
        return getPropertyAsFile(PROVIDERS_CONFIGURATION_FILE, DEFAULT_PROVIDERS_CONFIGURATION_FILE);
    }

    public File getRegistryAliasConfigurationFile() {
        return getPropertyAsFile(REGISTRY_ALIAS_CONFIGURATION_FILE, DEFAULT_REGISTRY_ALIAS_CONFIGURATION_FILE);
    }

    public String getLegacyDatabaseDirectory() {
        return getProperty(DATABASE_DIRECTORY);
    }

    public String getLegacyDatabaseUrlAppend() {
        return getProperty(DATABASE_URL_APPEND);
    }

    public String getDatabaseUrl() {
        return getProperty(DATABASE_URL);
    }

    public String getDatabaseDriverClassName() {
        return getProperty(DATABASE_DRIVER_CLASS_NAME);
    }

    public String getDatabaseDriverDirectory() {
        return getProperty(DATABASE_DRIVER_DIR);
    }

    public String getDatabaseUsername() {
        return getProperty(DATABASE_USERNAME);
    }

    public String getDatabasePassword() {
        return getProperty(DATABASE_PASSWORD);
    }

    public Integer getDatabaseMaxConnections() {
        return getPropertyAsInteger(DATABASE_MAX_CONNECTIONS);
    }

    public boolean getDatabaseSqlDebug() {
        final String value = getProperty(DATABASE_SQL_DEBUG);

        if (StringUtils.isBlank(value)) {
            return false;
        }

        return "true".equalsIgnoreCase(value.trim());
    }

    public File getAuthorizersConfigurationFile() {
        return getPropertyAsFile(SECURITY_AUTHORIZERS_CONFIGURATION_FILE, DEFAULT_SECURITY_AUTHORIZERS_CONFIGURATION_FILE);
    }

    public File getIdentityProviderConfigurationFile() {
        return getPropertyAsFile(SECURITY_IDENTITY_PROVIDERS_CONFIGURATION_FILE, DEFAULT_SECURITY_IDENTITY_PROVIDER_CONFIGURATION_FILE);
    }

    public File getKerberosConfigurationFile() {
        return getPropertyAsFile(KERBEROS_KRB5_FILE);
    }

    public String getKerberosSpnegoAuthenticationExpiration() {
        return getProperty(KERBEROS_SPNEGO_AUTHENTICATION_EXPIRATION, DEFAULT_AUTHENTICATION_EXPIRATION);
    }

    public String getKerberosSpnegoPrincipal() {
        return getPropertyAsTrimmedString(KERBEROS_SPNEGO_PRINCIPAL);
    }

    public String getKerberosSpnegoKeytabLocation() {
        return getPropertyAsTrimmedString(KERBEROS_SPNEGO_KEYTAB_LOCATION);
    }

    public boolean isKerberosSpnegoSupportEnabled() {
        return !StringUtils.isBlank(getKerberosSpnegoPrincipal()) && !StringUtils.isBlank(getKerberosSpnegoKeytabLocation());
    }

    public String getKerberosServicePrincipal() {
        return getPropertyAsTrimmedString(KERBEROS_SERVICE_PRINCIPAL);
    }

    public String getKerberosServiceKeytabLocation() {
        return getPropertyAsTrimmedString(KERBEROS_SERVICE_KEYTAB_LOCATION);
    }

    public Set<String> getExtensionsDirs() {
        final Set<String> extensionDirs = new HashSet<>();
        getPropertyKeys().stream().filter(key -> key.startsWith(EXTENSION_DIR_PREFIX)).forEach(key -> extensionDirs.add(getProperty(key)));
        return extensionDirs;
    }

    public boolean areRevisionsEnabled() {
        return Boolean.parseBoolean(getPropertyAsTrimmedString(REVISIONS_ENABLED));
    }

    // Helper functions for common ways of interpreting property values

    private String getPropertyAsTrimmedString(String key) {
        final String value = getProperty(key);
        if (!StringUtils.isBlank(value)) {
            return value.trim();
        } else {
            return null;
        }
    }

    private Integer getPropertyAsInteger(String key) {
        final String value = getProperty(key);
        if (StringUtils.isBlank(value)) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (final NumberFormatException nfe) {
            throw new IllegalStateException(String.format("%s must be an integer value.", key));
        }
    }

    private File getPropertyAsFile(String key) {
        final String filePath = getProperty(key);
        if (filePath != null && filePath.trim().length() > 0) {
            return new File(filePath.trim());
        } else {
            return null;
        }
    }

    private File getPropertyAsFile(String propertyKey, String defaultFileLocation) {
        final String value = getProperty(propertyKey);
        if (StringUtils.isBlank(value)) {
            return new File(defaultFileLocation);
        } else {
            return new File(value);
        }
    }

    /**
     * Returns true if the login identity provider has been configured.
     *
     * @return true if the login identity provider has been configured
     */
    public boolean isLoginIdentityProviderEnabled() {
        return !StringUtils.isBlank(getProperty(NiFiRegistryProperties.SECURITY_IDENTITY_PROVIDER));
    }

    /**
     * Returns whether an OpenId Connect (OIDC) URL is set.
     *
     * @return whether an OpenId Connect URL is set
     */
    public boolean isOidcEnabled() {
        return !StringUtils.isBlank(getOidcDiscoveryUrl());
    }

    /**
     * Returns the OpenId Connect (OIDC) URL. Null otherwise.
     *
     * @return OIDC discovery url
     */
    public String getOidcDiscoveryUrl() {
        return getProperty(SECURITY_USER_OIDC_DISCOVERY_URL);
    }

    /**
     * Returns the OpenId Connect connect timeout. Non null.
     *
     * @return OIDC connect timeout
     */
    public String getOidcConnectTimeout() {
        return getProperty(SECURITY_USER_OIDC_CONNECT_TIMEOUT, DEFAULT_SECURITY_USER_OIDC_CONNECT_TIMEOUT);
    }

    /**
     * Returns the OpenId Connect read timeout. Non null.
     *
     * @return OIDC read timeout
     */
    public String getOidcReadTimeout() {
        return getProperty(SECURITY_USER_OIDC_READ_TIMEOUT, DEFAULT_SECURITY_USER_OIDC_READ_TIMEOUT);
    }

    /**
     * Returns the OpenId Connect client id.
     *
     * @return OIDC client id
     */
    public String getOidcClientId() {
        return getProperty(SECURITY_USER_OIDC_CLIENT_ID);
    }

    /**
     * Returns the OpenId Connect client secret.
     *
     * @return OIDC client secret
     */
    public String getOidcClientSecret() {
        return getProperty(SECURITY_USER_OIDC_CLIENT_SECRET);
    }

    /**
     * Returns the preferred json web signature algorithm. May be null/blank.
     *
     * @return OIDC preferred json web signature algorithm
     */
    public String getOidcPreferredJwsAlgorithm() {
        return getProperty(SECURITY_USER_OIDC_PREFERRED_JWSALGORITHM);
    }

    /**
     * Returns additional scopes to be sent when requesting the access token from the IDP.
     *
     * @return List of additional scopes to be sent
     */
    public List<String> getOidcAdditionalScopes() {
        String rawProperty = getProperty(SECURITY_USER_OIDC_ADDITIONAL_SCOPES, "");
        if (rawProperty.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> additionalScopes = Arrays.asList(rawProperty.split(","));
        return additionalScopes.stream().map(String::trim).collect(Collectors.toList());
    }

    /**
     * Returns the claim to be used to identify a user.
     * Claim must be requested by adding the scope for it.
     * Default is 'email'.
     *
     * @return The claim to be used to identify the user.
     */
    public String getOidcClaimIdentifyingUser() {
        return getProperty(SECURITY_USER_OIDC_CLAIM_IDENTIFYING_USER, "email").trim();
    }
    /**
     * Returns the claim to be used to extract user groups from the OIDC payload.
     * Claim must be requested by adding the scope for it.
     * Default is 'groups'.
     *
     * @return The claim to be used to extract user groups.
     */
    public String getOidcClaimGroups() {
        return getProperty(SECURITY_USER_OIDC_CLAIM_GROUPS, "groups").trim();
    }

    /**
     * Returns the network interface list to use for HTTPS
     *
     * @return Network interface names of all HTTPS network interface properties
     */
    public Set<String> getHttpsNetworkInterfaceNames() {
        final Set<String> networkInterfaceNames = new HashSet<>();

        // go through each property
        for (String propertyName : getPropertyKeys()) {
            // determine if the property is a network interface name
            if (StringUtils.startsWith(propertyName, WEB_HTTPS_NETWORK_INTERFACE_PREFIX)) {
                // get the network interface property value
                final String interfaceName = getProperty(propertyName);
                if (StringUtils.isNotBlank(interfaceName)) {
                    networkInterfaceNames.add(getProperty(propertyName));
                }
            }
        }
        return networkInterfaceNames;
    }
}
