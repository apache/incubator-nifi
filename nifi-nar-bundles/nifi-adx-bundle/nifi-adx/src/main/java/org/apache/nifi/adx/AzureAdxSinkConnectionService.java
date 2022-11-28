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
package org.apache.nifi.adx;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


import com.google.common.base.Strings;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import org.apache.nifi.adx.model.ADXConnectionParams;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@Tags({ "Azure", "ADX", "Kusto", "ingest", "azure"})
@CapabilityDescription("Sends batches of flowfile content or stream flowfile content to an Azure ADX cluster.")
@ReadsAttributes({
        @ReadsAttribute(attribute= "AUTH_STRATEGY", description = "The strategy/method to authenticate against Azure Active Directory, either 'application' or 'managed_identity'."),
        @ReadsAttribute(attribute= "INGEST_URL", description="Specifies the URL of ingestion endpoint of the Azure Data Explorer cluster."),
        @ReadsAttribute(attribute= "APP_ID", description="Specifies Azure application id for accessing the ADX-Cluster."),
        @ReadsAttribute(attribute= "APP_KEY", description="Specifies Azure application key for accessing the ADX-Cluster."),
        @ReadsAttribute(attribute= "APP_TENANT", description="Azure application tenant for accessing the ADX-Cluster."),
        @ReadsAttribute(attribute= "CLUSTER_URL", description="Endpoint of ADX cluster. This is required only when streaming data to ADX cluster is enabled."),
})
public class AzureAdxSinkConnectionService extends AbstractControllerService implements AdxSinkConnectionService {

    public static final PropertyDescriptor INGEST_URL = new PropertyDescriptor
            .Builder().name(AzureAdxConnectionServiceParamsEnum.INGEST_URL.name())
            .displayName(AzureAdxConnectionServiceParamsEnum.INGEST_URL.getParamDisplayName())
            .description(AzureAdxConnectionServiceParamsEnum.INGEST_URL.getDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor KUSTO_AUTH_STRATEGY = new PropertyDescriptor
            .Builder().name(AzureAdxConnectionServiceParamsEnum.AUTH_STRATEGY.name())
            .displayName(AzureAdxConnectionServiceParamsEnum.AUTH_STRATEGY.getParamDisplayName())
            .description(AzureAdxConnectionServiceParamsEnum.AUTH_STRATEGY.getDescription())
            .required(false)
            .defaultValue("application")
            .allowableValues("application","managed_identity")
            .build();

    public static final PropertyDescriptor APP_ID = new PropertyDescriptor
            .Builder().name(AzureAdxConnectionServiceParamsEnum.APP_ID.name())
            .displayName(AzureAdxConnectionServiceParamsEnum.APP_ID.getParamDisplayName())
            .description(AzureAdxConnectionServiceParamsEnum.APP_ID.getDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor APP_KEY = new PropertyDescriptor
            .Builder().name(AzureAdxConnectionServiceParamsEnum.APP_KEY.name())
            .displayName(AzureAdxConnectionServiceParamsEnum.APP_KEY.getParamDisplayName())
            .description(AzureAdxConnectionServiceParamsEnum.APP_KEY.getDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor APP_TENANT = new PropertyDescriptor
            .Builder().name(AzureAdxConnectionServiceParamsEnum.APP_TENANT.name())
            .displayName(AzureAdxConnectionServiceParamsEnum.APP_TENANT.getParamDisplayName())
            .description(AzureAdxConnectionServiceParamsEnum.APP_TENANT.getDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLUSTER_URL = new PropertyDescriptor
            .Builder().name(AzureAdxConnectionServiceParamsEnum.CLUSTER_URL.name())
            .displayName(AzureAdxConnectionServiceParamsEnum.CLUSTER_URL.getParamDisplayName())
            .description(AzureAdxConnectionServiceParamsEnum.CLUSTER_URL.getDescription())
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(
            Arrays.asList(
                    KUSTO_AUTH_STRATEGY,
                    INGEST_URL,
                    APP_ID,
                    APP_KEY,
                    APP_TENANT,
                    CLUSTER_URL
            )
    );

    private IngestClient ingestClient;

    private Client executionClient;

    private ADXConnectionParams adxConnectionParams;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws ProcessException {

        getLogger().info("Starting Azure ADX Connection Service...");
        adxConnectionParams = new ADXConnectionParams();
        adxConnectionParams.setKustoAuthStrategy(context.getProperty(KUSTO_AUTH_STRATEGY).evaluateAttributeExpressions().getValue());
        adxConnectionParams.setIngestURL(context.getProperty(INGEST_URL).evaluateAttributeExpressions().getValue());
        adxConnectionParams.setAppId(context.getProperty(APP_ID).evaluateAttributeExpressions().getValue());
        adxConnectionParams.setAppKey(context.getProperty(APP_KEY).evaluateAttributeExpressions().getValue());
        adxConnectionParams.setAppTenant(context.getProperty(APP_TENANT).evaluateAttributeExpressions().getValue());
        adxConnectionParams.setKustoEngineURL(context.getProperty(CLUSTER_URL).evaluateAttributeExpressions().getValue());

        if(this.ingestClient != null || this.executionClient != null) {
            onStopped();
        }

    }

    @OnStopped
    public final void onStopped() {
        if (this.ingestClient != null) {
            try {
                this.ingestClient.close();
            } catch(IOException e) {
                getLogger().error("Closing Azure ADX Client failed with: " + e.getMessage(), e);
            } finally {
                this.ingestClient = null;
            }
        }
        this.executionClient = null;
    }


    protected IngestClient createAdxClient(final String ingestUrl,
                                           final String appId,
                                           final String appKey,
                                           final String appTenant,
                                           final Boolean isStreamingEnabled,
                                           final String kustoEngineUrl,
                                           final String kustoAuthStrategy) {
        return createKustoIngestClient(ingestUrl,appId,appKey,appTenant,isStreamingEnabled,kustoEngineUrl,kustoAuthStrategy);
    }
    @Override
    public IngestClient getAdxClient(boolean isStreamingEnabled) {
        return createAdxClient(adxConnectionParams.getIngestURL(),
                adxConnectionParams.getAppId(),
                adxConnectionParams.getAppKey(),
                adxConnectionParams.getAppTenant(),
                isStreamingEnabled, adxConnectionParams.getKustoEngineURL(),
                adxConnectionParams.getKustoAuthStrategy());
    }

    @Override
    public Client getKustoExecutionClient(){
        return createKustoExecutionClient(adxConnectionParams.getKustoEngineURL(),
                adxConnectionParams.getAppId(),
                adxConnectionParams.getAppKey(),
                adxConnectionParams.getAppTenant(),
                adxConnectionParams.getKustoAuthStrategy());
    }


    public IngestClient createKustoIngestClient(final String ingestUrl,
                                                final String appId,
                                                final String appKey,
                                                final String appTenant,
                                                final Boolean isStreamingEnabled,
                                                final String kustoEngineUrl,
                                                final String kustoAuthStrategy) {
        IngestClient kustoIngestClient;
        try {
            ConnectionStringBuilder ingestConnectionStringBuilder = createKustoEngineConnectionString(ingestUrl,appId,appKey,appTenant,kustoAuthStrategy);

            if (isStreamingEnabled) {
                ConnectionStringBuilder streamingConnectionStringBuilder = createKustoEngineConnectionString(kustoEngineUrl,appId,appKey,appTenant,kustoAuthStrategy);
                kustoIngestClient = IngestClientFactory.createManagedStreamingIngestClient(ingestConnectionStringBuilder, streamingConnectionStringBuilder);
            }else{
                kustoIngestClient = IngestClientFactory.createClient(ingestConnectionStringBuilder);
            }
        } catch (Exception e) {
            throw new ProcessException("Failed to initialize KustoIngestClient", e);
        }
        return kustoIngestClient;
    }

    public ConnectionStringBuilder createKustoEngineConnectionString(final String clusterUrl,final String appId,final String appKey,final String appTenant, final String kustoAuthStrategy) {
        final ConnectionStringBuilder kcsb;
        switch (kustoAuthStrategy) {
            case "application":
                if (!Strings.isNullOrEmpty(appId) && !Strings.isNullOrEmpty(appKey)) {
                    kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                            clusterUrl,
                            appId,
                            appKey,
                            appTenant);
                } else {
                    throw new ProcessException("Kusto authentication missing App Key.");
                }
                break;

            case "managed_identity":
                kcsb = ConnectionStringBuilder.createWithAadManagedIdentity(
                        clusterUrl,
                        appId);
                break;

            default:
                throw new ProcessException("Failed to initialize KustoIngestClient, please " +
                        "provide valid credentials. Either Kusto managed identity or " +
                        "Kusto appId, appKey, and authority should be configured.");
        }
        kcsb.setClientVersionForTracing(Version.CLIENT_NAME + ":" + Version.getVersion());
        return kcsb;
    }

    public Client createKustoExecutionClient(final String clusterUrl,final String appId,final String appKey,final String appTenant, final String kustoAuthStrategy) {
        try {
            return ClientFactory.createClient(createKustoEngineConnectionString(clusterUrl,appId,appKey,appTenant,kustoAuthStrategy));
        } catch (Exception e) {
            throw new ProcessException("Failed to initialize KustoEngineClient", e);
        }
    }


}
