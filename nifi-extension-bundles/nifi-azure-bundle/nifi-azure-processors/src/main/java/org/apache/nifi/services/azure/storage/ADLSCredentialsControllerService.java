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
package org.apache.nifi.services.azure.storage;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processors.azure.AzureServiceEndpoints;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.CREDENTIALS_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.MANAGED_IDENTITY_CLIENT_ID;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.SERVICE_PRINCIPAL_CLIENT_ID;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.SERVICE_PRINCIPAL_CLIENT_SECRET;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.SERVICE_PRINCIPAL_TENANT_ID;

/**
 * Provides credentials details for ADLS
 *
 * @see AbstractControllerService
 */
@Tags({"azure", "microsoft", "cloud", "storage", "adls", "credentials"})
@CapabilityDescription("Defines credentials for ADLS processors.")
public class ADLSCredentialsControllerService extends AbstractControllerService implements ADLSCredentialsService {

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.ACCOUNT_NAME)
            .description(AzureStorageUtils.ACCOUNT_NAME_BASE_DESCRIPTION + AzureStorageUtils.ACCOUNT_NAME_SECURITY_DESCRIPTION)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.ACCOUNT_KEY)
            .description(AzureStorageUtils.ACCOUNT_KEY_BASE_DESCRIPTION + AzureStorageUtils.ACCOUNT_KEY_SECURITY_DESCRIPTION)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SAS_TOKEN = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.SAS_TOKEN)
            .description(AzureStorageUtils.SAS_TOKEN_BASE_DESCRIPTION + AzureStorageUtils.SAS_TOKEN_SECURITY_DESCRIPTION)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor ENDPOINT_SUFFIX = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.ENDPOINT_SUFFIX)
            .defaultValue(AzureServiceEndpoints.DEFAULT_ADLS_ENDPOINT_SUFFIX)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.PROXY_CONFIGURATION_SERVICE)
            .dependsOn(CREDENTIALS_TYPE, AzureStorageCredentialsType.SERVICE_PRINCIPAL, AzureStorageCredentialsType.MANAGED_IDENTITY)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ACCOUNT_NAME,
            ENDPOINT_SUFFIX,
            CREDENTIALS_TYPE,
            ACCOUNT_KEY,
            SAS_TOKEN,
            MANAGED_IDENTITY_CLIENT_ID,
            SERVICE_PRINCIPAL_TENANT_ID,
            SERVICE_PRINCIPAL_CLIENT_ID,
            SERVICE_PRINCIPAL_CLIENT_SECRET,
            PROXY_CONFIGURATION_SERVICE
    );

    private ConfigurationContext context;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        if (!config.hasProperty(CREDENTIALS_TYPE)) {
            final String propNameUseManagedIdentity = "storage-use-managed-identity";

            if (config.isPropertySet(ACCOUNT_KEY)) {
                config.setProperty(CREDENTIALS_TYPE, AzureStorageCredentialsType.ACCOUNT_KEY.getValue());
            } else if (config.isPropertySet(SAS_TOKEN)) {
                config.setProperty(CREDENTIALS_TYPE, AzureStorageCredentialsType.SAS_TOKEN.getValue());
            } else if (config.isPropertySet(SERVICE_PRINCIPAL_TENANT_ID)) {
                config.setProperty(CREDENTIALS_TYPE, AzureStorageCredentialsType.SERVICE_PRINCIPAL.getValue());
            } else {
                config.getPropertyValue(propNameUseManagedIdentity).ifPresent(value -> {
                    if ("true".equals(value)) {
                        config.setProperty(CREDENTIALS_TYPE, AzureStorageCredentialsType.MANAGED_IDENTITY.getValue());
                    }
                });
            }

            config.removeProperty(propNameUseManagedIdentity);
        }
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        this.context = context;
    }

    @Override
    public ADLSCredentialsDetails getCredentialsDetails(Map<String, String> attributes) {
        ADLSCredentialsDetails.Builder credentialsBuilder = ADLSCredentialsDetails.Builder.newBuilder();

        setValue(credentialsBuilder, ACCOUNT_NAME, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setAccountName, attributes);
        setValue(credentialsBuilder, ACCOUNT_KEY, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setAccountKey, attributes);
        setValue(credentialsBuilder, SAS_TOKEN, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setSasToken, attributes);
        setValue(credentialsBuilder, ENDPOINT_SUFFIX, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setEndpointSuffix, attributes);
        setValue(credentialsBuilder, CREDENTIALS_TYPE, property -> property.asAllowableValue(AzureStorageCredentialsType.class) == AzureStorageCredentialsType.MANAGED_IDENTITY,
                ADLSCredentialsDetails.Builder::setUseManagedIdentity, attributes);
        setValue(credentialsBuilder, MANAGED_IDENTITY_CLIENT_ID, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setManagedIdentityClientId, attributes);
        setValue(credentialsBuilder, SERVICE_PRINCIPAL_TENANT_ID, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setServicePrincipalTenantId, attributes);
        setValue(credentialsBuilder, SERVICE_PRINCIPAL_CLIENT_ID, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setServicePrincipalClientId, attributes);
        setValue(credentialsBuilder, SERVICE_PRINCIPAL_CLIENT_SECRET, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setServicePrincipalClientSecret, attributes);

        credentialsBuilder.setProxyOptions(AzureStorageUtils.getProxyOptions(context));

        return credentialsBuilder.build();
    }

    private <T> void setValue(
            ADLSCredentialsDetails.Builder credentialsBuilder,
            PropertyDescriptor propertyDescriptor, Function<PropertyValue, T> getPropertyValue,
            BiConsumer<ADLSCredentialsDetails.Builder, T> setBuilderValue, Map<String, String> attributes
    ) {
        PropertyValue property = context.getProperty(propertyDescriptor);

        if (property.isSet()) {
            if (propertyDescriptor.isExpressionLanguageSupported()) {
                if (propertyDescriptor.getExpressionLanguageScope() == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) {
                    property = property.evaluateAttributeExpressions(attributes);
                } else {
                    property = property.evaluateAttributeExpressions();
                }
            }
            T value = getPropertyValue.apply(property);
            setBuilderValue.accept(credentialsBuilder, value);
        }
    }
}
