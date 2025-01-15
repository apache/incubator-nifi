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
package org.apache.nifi.vault.hashicorp;

import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.vault.hashicorp.config.HashiCorpVaultConfiguration;
import org.springframework.core.env.PropertySource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Tags({"hashicorp", "vault", "client"})
@CapabilityDescription("A controller service for interacting with HashiCorp Vault.")
@SupportsSensitiveDynamicProperties
@DynamicProperties(
        @DynamicProperty(name = "A Spring Vault configuration property name",
                value = "The property value",
                description = "Allows any Spring Vault property keys to be specified, as described in " +
                        "(https://docs.spring.io/spring-vault/docs/2.3.x/reference/html/#vault.core.environment-vault-configuration). " +
                        "See Additional Details for more information.",
                expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT
        )
)
public class StandardHashiCorpVaultClientService extends AbstractControllerService implements HashiCorpVaultClientService {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        CONFIGURATION_STRATEGY,
        VAULT_URI,
        VAULT_AUTHENTICATION,
        SSL_CONTEXT_SERVICE,
        VAULT_PROPERTIES_FILES,
        CONNECTION_TIMEOUT,
        READ_TIMEOUT
    );

    private HashiCorpVaultCommunicationService communicationService;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger,
                                                 final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        HashiCorpVaultCommunicationService service = null;
        try {
            service = createCommunicationService(context);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .verificationStepName("Configure HashiCorp Vault Client")
                    .explanation("Successfully configured HashiCorp Vault Client")
                    .build());
        } catch (final Exception e) {
            verificationLogger.error("Failed to configure HashiCorp Vault Client", e);

            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .verificationStepName("Configure HashiCorp Vault Client")
                    .explanation("Failed to configure HashiCorp Vault Client: " + e.getMessage())
                    .build());
        }
        if (service != null) {
            try {
                service.getServerVersion();
                results.add(new ConfigVerificationResult.Builder()
                        .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                        .verificationStepName("Connect to HashiCorp Vault Server")
                        .explanation("Successfully connected to HashiCorp Vault Server")
                        .build());
            } catch (final Exception e) {
                verificationLogger.error("Failed to connect to HashiCorp Vault Server", e);

                results.add(new ConfigVerificationResult.Builder()
                        .outcome(ConfigVerificationResult.Outcome.FAILED)
                        .verificationStepName("Connect to HashiCorp Vault Server")
                        .explanation("Failed to connect to HashiCorp Vault Server: " + e.getMessage())
                        .build());
            }
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            communicationService = createCommunicationService(context);
        } catch (final Exception e) {
            throw new InitializationException("Failed to initialize HashiCorp Vault client", e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        communicationService = null;
    }

    @Override
    public HashiCorpVaultCommunicationService getHashiCorpVaultCommunicationService() {
        return communicationService;
    }

    private HashiCorpVaultCommunicationService createCommunicationService(final ConfigurationContext context) throws IOException {
        final List<PropertySource<?>> propertySources = new ArrayList<>();

        final String configurationStrategy = context.getProperty(CONFIGURATION_STRATEGY).getValue();
        if (DIRECT_PROPERTIES.getValue().equals(configurationStrategy)) {
            final PropertySource<?> configurationPropertySource = new DirectPropertySource("Direct Properties", context);
            propertySources.add(configurationPropertySource);
        } else {
            for (final ResourceReference resourceReference : context.getProperty(VAULT_PROPERTIES_FILES).asResources().asList()) {
                final String propertiesFile = resourceReference.getLocation();
                propertySources.add(HashiCorpVaultConfiguration.createPropertiesFileSource(propertiesFile));
            }
        }

        return new StandardHashiCorpVaultCommunicationService(propertySources.toArray(new PropertySource[0]));
    }

    static class DirectPropertySource extends PropertySource<ConfigurationContext> {

        private static final String VAULT_SSL_KEY_PATTERN = "vault.ssl.(key.*|trust.*|enabledProtocols)";

        public DirectPropertySource(final String name, final ConfigurationContext source) {
            super(name, source);
        }

        @Override
        public Object getProperty(final String name) {
            if (name.matches(VAULT_SSL_KEY_PATTERN)) {
                return getSslProperty(name);
            }

            return getSource().getAllProperties().get(name);
        }

        private String getSslProperty(final String name) {
            if (getSource().getProperty(SSL_CONTEXT_SERVICE).isSet()) {
                final SSLContextService sslContextService = getSource().getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
                switch (name) {
                    case "vault.ssl.key-store":
                        return sslContextService.getKeyStoreFile();
                    case "vault.ssl.key-store-password":
                        return sslContextService.getKeyStorePassword();
                    case "vault.ssl.key-store-type":
                        return sslContextService.getKeyStoreType();
                    case "vault.ssl.trust-store":
                        return sslContextService.getTrustStoreFile();
                    case "vault.ssl.trust-store-password":
                        return sslContextService.getTrustStorePassword();
                    case "vault.ssl.trust-store-type":
                        return sslContextService.getTrustStoreType();
                    case "vault.ssl.enabledProtocols":
                        return sslContextService.getSslAlgorithm();
                }
            }

            return null;
        }
    }
}
