/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.azure.cosmos.document;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.cosmos.AzureCosmosDBConnectionService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class AbstractAzureCosmosDBProcessor extends AbstractProcessor {

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles that are written to Cosmos DB are routed to this relationship")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("All FlowFiles that cannot be written to Cosmos DB are routed to this relationship")
        .build();

    static final PropertyDescriptor CONNECTION_SERVICE = new PropertyDescriptor.Builder()
        .name("azure-cosmos-db-connection-service")
        .displayName("Cosmos DB Connection Service")
        .description("If configured, the controller service used to obtain the connection string and access key")
        .required(false)
        .identifiesControllerService(AzureCosmosDBConnectionService.class)
        .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
        .name("azure-cosmos-db-name")
        .displayName("Cosmos DB Name")
        .description("The database name or id. This is used as the namespace for document collections or containers")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    static final PropertyDescriptor CONTAINER_ID = new PropertyDescriptor.Builder()
        .name("azure-cosmos-db-container-id")
        .displayName("Cosmos DB Container ID")
        .description("The unique identifier for the container")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    static final PropertyDescriptor PARTITION_KEY = new PropertyDescriptor.Builder()
        .name("azure-cosmos-db-partition-key")
        .displayName("Cosmos DB Partition Key")
        .description("The partition key used to distribute data among servers")
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_SERVICE,
            AzureCosmosDBUtils.URI,
            AzureCosmosDBUtils.DB_ACCESS_KEY,
            AzureCosmosDBUtils.CONSISTENCY,
            DATABASE_NAME,
            CONTAINER_ID,
            PARTITION_KEY
    );

    private CosmosClient cosmosClient;
    private CosmosContainer container;
    private AzureCosmosDBConnectionService connectionService;

    protected static List<PropertyDescriptor> getCommonPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws CosmosException {
        final ComponentLog logger = getLogger();

        if (context.getProperty(CONNECTION_SERVICE).isSet()) {
            this.connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(AzureCosmosDBConnectionService.class);
            this.cosmosClient = this.connectionService.getCosmosClient();
        } else {
            final String uri = context.getProperty(AzureCosmosDBUtils.URI).getValue();
            final String accessKey = context.getProperty(AzureCosmosDBUtils.DB_ACCESS_KEY).getValue();
            final String selectedConsistency = context.getProperty(AzureCosmosDBUtils.CONSISTENCY).getValue();
            final ConsistencyLevel consistencyLevel = AzureCosmosDBUtils.determineConsistencyLevel(selectedConsistency);
            if (cosmosClient != null) {
                onStopped();
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Creating CosmosClient");
            }
            createCosmosClient(uri, accessKey, consistencyLevel);
        }
        getCosmosDocumentContainer(context);
        doPostActionOnSchedule(context);
    }

    protected void createCosmosClient(final String uri, final String accessKey, final ConsistencyLevel consistencyLevel) {
        this.cosmosClient = new CosmosClientBuilder()
                                .endpoint(uri)
                                .key(accessKey)
                                .consistencyLevel(consistencyLevel)
                                .buildClient();
    }

    protected abstract void doPostActionOnSchedule(final ProcessContext context);

    protected void getCosmosDocumentContainer(final ProcessContext context) throws CosmosException {
        final String databaseName = context.getProperty(DATABASE_NAME).getValue();
        final String containerID = context.getProperty(CONTAINER_ID).getValue();
        final String partitionKey = context.getProperty(PARTITION_KEY).getValue();

        final CosmosDatabaseResponse databaseResponse = this.cosmosClient.createDatabaseIfNotExists(databaseName);
        final CosmosDatabase database = this.cosmosClient.getDatabase(databaseResponse.getProperties().getId());

        final CosmosContainerProperties containerProperties =
            new CosmosContainerProperties(containerID, "/" + partitionKey);

        //  Create container by default if Not exists.
        final CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties);
        this.container =  database.getContainer(containerResponse.getProperties().getId());
    }

    @OnStopped
    public final void onStopped() {
        final ComponentLog logger = getLogger();
        if (connectionService == null && cosmosClient != null) {
            // close client only when cosmoClient is created in Processor.
            if (logger.isDebugEnabled()) {
                logger.debug("Closing CosmosClient");
            }
            try {
                this.container = null;
                this.cosmosClient.close();
            } catch (CosmosException e) {
                logger.error("Error closing Cosmos DB client due to {}", e.getMessage(), e);
            } finally {
                this.cosmosClient = null;
            }
        }
    }

    protected String getURI(final ProcessContext context) {
        if (this.connectionService != null) {
            return this.connectionService.getURI();
        } else {
            return context.getProperty(AzureCosmosDBUtils.URI).getValue();
        }
    }

    protected String getAccessKey(final ProcessContext context) {
        if (this.connectionService != null) {
            return this.connectionService.getAccessKey();
        } else {
            return context.getProperty(AzureCosmosDBUtils.DB_ACCESS_KEY).getValue();
        }
    }

    protected String getConsistencyLevel(final ProcessContext context) {
        if (this.connectionService != null) {
            return this.connectionService.getConsistencyLevel();
        } else {
            return context.getProperty(AzureCosmosDBUtils.CONSISTENCY).getValue();
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> validationResults = new ArrayList<>();

        boolean connectionServiceIsSet = context.getProperty(CONNECTION_SERVICE).isSet();
        boolean uriIsSet = context.getProperty(AzureCosmosDBUtils.URI).isSet();
        boolean accessKeyIsSet = context.getProperty(AzureCosmosDBUtils.DB_ACCESS_KEY).isSet();
        boolean databaseIsSet = context.getProperty(DATABASE_NAME).isSet();
        boolean collectionIsSet = context.getProperty(CONTAINER_ID).isSet();
        boolean partitionIsSet = context.getProperty(PARTITION_KEY).isSet();

        if (connectionServiceIsSet && (uriIsSet || accessKeyIsSet) ) {
            // If connection Service is set, None of the Processor variables URI and accessKey
            // should be set.
            final String msg = String.format(
                "If connection service is used for DB connection, none of %s and %s should be set",
                AzureCosmosDBUtils.URI.getDisplayName(),
                AzureCosmosDBUtils.DB_ACCESS_KEY.getDisplayName()
            );
            validationResults.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        } else if (!connectionServiceIsSet && (!uriIsSet || !accessKeyIsSet)) {
            // If connection Service is not set, Both of the Processor variable URI and accessKey
            // should be set.
            final String msg = String.format(
                "If connection service is not used for DB connection, both %s and %s should be set",
                AzureCosmosDBUtils.URI.getDisplayName(),
                AzureCosmosDBUtils.DB_ACCESS_KEY.getDisplayName()
            );
            validationResults.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }
        if (!databaseIsSet) {
            final String msg = AbstractAzureCosmosDBProcessor.DATABASE_NAME.getDisplayName() + " must be set.";
            validationResults.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }
        if (!collectionIsSet) {
            final String msg = AbstractAzureCosmosDBProcessor.CONTAINER_ID.getDisplayName() + " must be set.";
            validationResults.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }
        if (!partitionIsSet) {
            final String msg = AbstractAzureCosmosDBProcessor.PARTITION_KEY.getDisplayName() + " must be set.";
            validationResults.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }
        return validationResults;
    }

    protected CosmosClient getCosmosClient() {
        return cosmosClient;
    }

    protected void setCosmosClient(CosmosClient cosmosClient) {
        this.cosmosClient = cosmosClient;
    }

    protected CosmosContainer getContainer() {
        return container;
    }

    protected void setContainer(CosmosContainer container) {
        this.container = container;
    }

    protected AzureCosmosDBConnectionService getConnectionService() {
        return connectionService;
    }

    protected void setConnectionService(AzureCosmosDBConnectionService connectionService) {
        this.connectionService = connectionService;
    }
}
