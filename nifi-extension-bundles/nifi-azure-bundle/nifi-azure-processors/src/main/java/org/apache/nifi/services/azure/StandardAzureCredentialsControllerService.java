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
package org.apache.nifi.services.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

/**
 * Provides credentials used by Azure clients.
 *
 * @see AbstractControllerService
 */
@Tags({"azure", "security", "credentials", "provider", "session"})
@CapabilityDescription("Provide credentials to use with an Azure client.")
public class StandardAzureCredentialsControllerService extends AbstractControllerService implements AzureCredentialsService {
    public static AllowableValue DEFAULT_CREDENTIAL = new AllowableValue("default-credential",
            "Default Credential",
            "Uses default credential chain. It first checks environment variables, before trying managed identity.");
    public static AllowableValue MANAGED_IDENTITY = new AllowableValue("managed-identity",
            "Managed Identity",
            "Azure Virtual Machine Managed Identity (it can only be used when NiFi is running on Azure)");
    public static final PropertyDescriptor CREDENTIAL_CONFIGURATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("credential-configuration-strategy")
            .displayName("Credential Configuration Strategy")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .sensitive(false)
            .allowableValues(DEFAULT_CREDENTIAL, MANAGED_IDENTITY)
            .defaultValue(DEFAULT_CREDENTIAL)
            .build();

    public static final PropertyDescriptor MANAGED_IDENTITY_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("managed-identity-client-id")
            .displayName("Managed Identity Client ID")
            .description("Client ID of the managed identity. The property is required when User Assigned Managed Identity is used for authentication. " +
                    "It must be empty in case of System Assigned Managed Identity.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(CREDENTIAL_CONFIGURATION_STRATEGY, MANAGED_IDENTITY)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CREDENTIAL_CONFIGURATION_STRATEGY,
            MANAGED_IDENTITY_CLIENT_ID
    );

    private TokenCredential credentials;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public TokenCredential getCredentials() throws ProcessException {
        return credentials;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        final String configurationStrategy = context.getProperty(CREDENTIAL_CONFIGURATION_STRATEGY).getValue();

        if (DEFAULT_CREDENTIAL.getValue().equals(configurationStrategy)) {
            credentials = getDefaultAzureCredential();
        } else if (MANAGED_IDENTITY.getValue().equals(configurationStrategy)) {
            credentials = getManagedIdentityCredential(context);
        } else {
            final String errorMsg = String.format("Configuration Strategy [%s] not recognized", configurationStrategy);
            getLogger().error(errorMsg);
            throw new ProcessException(errorMsg);
        }
    }

    private TokenCredential getDefaultAzureCredential() {
        return new DefaultAzureCredentialBuilder().build();
    }

    private TokenCredential getManagedIdentityCredential(final ConfigurationContext context) {
        final String clientId = context.getProperty(MANAGED_IDENTITY_CLIENT_ID).getValue();

        return new ManagedIdentityCredentialBuilder()
                .clientId(clientId)
                .build();
    }

    @Override
    public String toString() {
        return "StandardAzureCredentialsControllerService[id=" + getIdentifier() + "]";
    }
}
