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

import com.azure.core.http.ProxyOptions;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processors.azure.AzureServiceEndpoints;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.ACCOUNT_KEY;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.ACCOUNT_NAME;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.CREDENTIALS_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.MANAGED_IDENTITY_CLIENT_ID;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.SAS_TOKEN;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.SERVICE_PRINCIPAL_CLIENT_ID;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.SERVICE_PRINCIPAL_CLIENT_SECRET;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.SERVICE_PRINCIPAL_TENANT_ID;

/**
 * Provides credentials details for Azure Storage processors
 *
 * @see AbstractControllerService
 */
@Tags({"azure", "microsoft", "cloud", "storage", "blob", "credentials", "queue"})
@CapabilityDescription("Provides credentials for Azure Storage processors using Azure Storage client library v12.")
public class AzureStorageCredentialsControllerService_v12 extends AbstractControllerService implements AzureStorageCredentialsService_v12 {

    public static final PropertyDescriptor ENDPOINT_SUFFIX = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.ENDPOINT_SUFFIX)
            .defaultValue(AzureServiceEndpoints.DEFAULT_BLOB_ENDPOINT_SUFFIX)
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

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        this.context = context;
    }

    @Override
    public AzureStorageCredentialsDetails_v12 getCredentialsDetails(Map<String, String> attributes) {
        String accountName = context.getProperty(ACCOUNT_NAME).getValue();
        String endpointSuffix = context.getProperty(ENDPOINT_SUFFIX).getValue();
        AzureStorageCredentialsType credentialsType = context.getProperty(CREDENTIALS_TYPE).asAllowableValue(AzureStorageCredentialsType.class);
        ProxyOptions proxyOptions = AzureStorageUtils.getProxyOptions(context);

        switch (credentialsType) {
            case ACCOUNT_KEY:
                String accountKey = context.getProperty(ACCOUNT_KEY).getValue();
                return AzureStorageCredentialsDetails_v12.createWithAccountKey(accountName, endpointSuffix, accountKey);
            case SAS_TOKEN:
                String sasToken = context.getProperty(SAS_TOKEN).getValue();
                return AzureStorageCredentialsDetails_v12.createWithSasToken(accountName, endpointSuffix, sasToken);
            case MANAGED_IDENTITY:
                String managedIdentityClientId = context.getProperty(MANAGED_IDENTITY_CLIENT_ID).getValue();
                return AzureStorageCredentialsDetails_v12.createWithManagedIdentity(accountName, endpointSuffix, managedIdentityClientId, proxyOptions);
            case SERVICE_PRINCIPAL:
                String servicePrincipalTenantId = context.getProperty(SERVICE_PRINCIPAL_TENANT_ID).getValue();
                String servicePrincipalClientId = context.getProperty(SERVICE_PRINCIPAL_CLIENT_ID).getValue();
                String servicePrincipalClientSecret = context.getProperty(SERVICE_PRINCIPAL_CLIENT_SECRET).getValue();
                return AzureStorageCredentialsDetails_v12.createWithServicePrincipal(accountName, endpointSuffix,
                        servicePrincipalTenantId, servicePrincipalClientId, servicePrincipalClientSecret, proxyOptions);
            default:
                throw new IllegalArgumentException("Unhandled credentials type: " + credentialsType);
        }
    }
}
