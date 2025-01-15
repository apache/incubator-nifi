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

package org.apache.nifi.services.azure.cosmos.document;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processors.azure.cosmos.document.AzureCosmosDBUtils;
import org.apache.nifi.services.azure.cosmos.AzureCosmosDBConnectionService;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Tags({"azure", "cosmos", "document", "service"})
@CapabilityDescription(
        "Provides a controller service that configures a connection to Cosmos DB (Core SQL API) " +
        " and provides access to that connection to other Cosmos DB-related components."
)
public class AzureCosmosDBClientService extends AbstractControllerService implements AzureCosmosDBConnectionService {
    private String uri;
    private String accessKey;
    private String consistencyLevel;
    private CosmosClient cosmosClient;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.uri = context.getProperty(AzureCosmosDBUtils.URI).getValue();
        this.accessKey = context.getProperty(AzureCosmosDBUtils.DB_ACCESS_KEY).getValue();
        final String selectedConsistency = context.getProperty(AzureCosmosDBUtils.CONSISTENCY).getValue();
        final ConsistencyLevel consistencyLevel = AzureCosmosDBUtils.determineConsistencyLevel(selectedConsistency);
        if (this.cosmosClient != null) {
            onStopped();
        }
        this.consistencyLevel = consistencyLevel.toString();
        createCosmosClient(uri, accessKey, consistencyLevel);
    }

    @OnStopped
    public final void onStopped() {
        if (this.cosmosClient != null) {
            try {
                cosmosClient.close();
            } catch (CosmosException e) {
                getLogger().error("Closing cosmosClient Failed", e);
            } finally {
                this.cosmosClient = null;
            }
        }
    }

    protected void createCosmosClient(final String uri, final String accessKey, final ConsistencyLevel clevel) {
        this.cosmosClient = new CosmosClientBuilder()
                                .endpoint(uri)
                                .key(accessKey)
                                .consistencyLevel(clevel)
                                .buildClient();
    }

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            AzureCosmosDBUtils.URI,
            AzureCosmosDBUtils.DB_ACCESS_KEY,
            AzureCosmosDBUtils.CONSISTENCY
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public String getURI() {
        return this.uri;
    }

    @Override
    public String getAccessKey() {
        return this.accessKey;
    }
    @Override
    public String getConsistencyLevel() {
        return this.consistencyLevel;
    }

    @Override
    public CosmosClient getCosmosClient() {
        return this.cosmosClient;
    }
    public void setCosmosClient(CosmosClient client) {
        this.cosmosClient = client;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String uri = validationContext.getProperty(AzureCosmosDBUtils.URI).getValue();
        final String accessKey = validationContext.getProperty(AzureCosmosDBUtils.DB_ACCESS_KEY).getValue();

        if (StringUtils.isBlank(uri) || StringUtils.isBlank(accessKey)) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .valid(false)
                    .explanation(
                        "either " + AzureCosmosDBUtils.URI.getDisplayName()
                        + " or " + AzureCosmosDBUtils.DB_ACCESS_KEY.getDisplayName() + " is required")
                    .build());
        }
        return results;
    }

}
