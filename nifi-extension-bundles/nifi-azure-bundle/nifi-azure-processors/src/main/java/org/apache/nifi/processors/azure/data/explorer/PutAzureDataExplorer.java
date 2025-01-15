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
package org.apache.nifi.processors.azure.data.explorer;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.data.explorer.KustoIngestService;
import org.apache.nifi.services.azure.data.explorer.KustoIngestionRequest;
import org.apache.nifi.services.azure.data.explorer.KustoIngestionResult;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"Azure", "Kusto", "ADX", "Explorer", "Data"})
@CapabilityDescription("Acts as an Azure Data Explorer sink which sends FlowFiles to the provided endpoint. " +
        "Data can be sent through queued ingestion or streaming ingestion to the Azure Data Explorer cluster.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutAzureDataExplorer extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Ingest processing succeeded")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Ingest processing failed")
            .build();

    public static final PropertyDescriptor INGEST_SERVICE = new PropertyDescriptor
            .Builder().name("Kusto Ingest Service")
            .displayName("Kusto Ingest Service")
            .description("Azure Data Explorer Kusto Ingest Service")
            .required(true)
            .identifiesControllerService(KustoIngestService.class)
            .build();

    public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Database Name")
            .displayName("Database Name")
            .description("Azure Data Explorer Database Name for ingesting data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .displayName("Table Name")
            .description("Azure Data Explorer Table Name for ingesting data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAPPING_NAME = new PropertyDescriptor
            .Builder().name("Ingest Mapping Name")
            .displayName("Ingest Mapping Name")
            .description("The name of the mapping responsible for storing the data in the appropriate columns.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor.Builder()
            .name("Data Format")
            .displayName("Data Format")
            .description("The format of the data that is sent to Azure Data Explorer. Supported formats include: avro, csv, json")
            .required(true)
            .allowableValues(KustoIngestDataFormat.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PARTIALLY_SUCCEEDED_ROUTING_STRATEGY = new PropertyDescriptor.Builder()
            .name("Partially Succeeded Routing Strategy")
            .displayName("Partially Succeeded Routing Strategy")
            .description("Defines where to route FlowFiles that resulted in a partially succeeded status.")
            .required(true)
            .allowableValues(SUCCESS.getName(), FAILURE.getName())
            .defaultValue(FAILURE.getName())
            .build();

    public static final PropertyDescriptor STREAMING_ENABLED = new PropertyDescriptor
            .Builder().name("Streaming Enabled")
            .displayName("Streaming Enabled")
            .description("Whether to stream data to Azure Data Explorer.")
            .required(true)
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue(Boolean.FALSE.toString())
            .build();

    public static final PropertyDescriptor IGNORE_FIRST_RECORD = new PropertyDescriptor.Builder()
            .name("Ingestion Ignore First Record")
            .displayName("Ingestion Ignore First Record")
            .description("Defines whether ignore first record while ingestion.")
            .required(true)
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .defaultValue(Boolean.FALSE.toString())
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor POLL_FOR_INGEST_STATUS = new PropertyDescriptor
            .Builder().name("Poll for Ingest Status")
            .displayName("Poll for Ingest Status")
            .description("Determines whether to poll on ingestion status after an ingestion to Azure Data Explorer is completed")
            .required(true)
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue(Boolean.FALSE.toString())
            .build();

    public static final PropertyDescriptor INGEST_STATUS_POLLING_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Ingest Status Polling Timeout")
            .displayName("Ingest Status Polling Timeout")
            .description("Defines the total amount time to poll for ingestion status")
            .required(true)
            .dependsOn(POLL_FOR_INGEST_STATUS, Boolean.TRUE.toString())
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 m")
            .build();

    public static final PropertyDescriptor INGEST_STATUS_POLLING_INTERVAL = new PropertyDescriptor.Builder()
            .name("Ingest Status Polling Interval")
            .displayName("Ingest Status Polling Interval")
            .description("Defines the value of interval of time to poll for ingestion status")
            .required(true)
            .dependsOn(POLL_FOR_INGEST_STATUS, Boolean.TRUE.toString())
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 s")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            INGEST_SERVICE,
            DATABASE_NAME,
            TABLE_NAME,
            MAPPING_NAME,
            DATA_FORMAT,
            PARTIALLY_SUCCEEDED_ROUTING_STRATEGY,
            STREAMING_ENABLED,
            IGNORE_FIRST_RECORD,
            POLL_FOR_INGEST_STATUS,
            INGEST_STATUS_POLLING_TIMEOUT,
            INGEST_STATUS_POLLING_INTERVAL
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS,
            FAILURE
    );

    private transient KustoIngestService service;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        service = context.getProperty(INGEST_SERVICE).asControllerService(KustoIngestService.class);

        final String database = context.getProperty(DATABASE_NAME).getValue();
        final String tableName = context.getProperty(TABLE_NAME).getValue();
        if (!service.isTableReadable(database, tableName)) {
            throw new ProcessException(String.format("Database [%s] Table [%s] not readable", database, tableName));
        }

        final boolean streamingEnabled = context.getProperty(STREAMING_ENABLED).evaluateAttributeExpressions().asBoolean();
        if (streamingEnabled && !service.isStreamingPolicyEnabled(database)) {
            throw new ProcessException(String.format("Database [%s] streaming policy not enabled", database));
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String dataFormat = context.getProperty(DATA_FORMAT).evaluateAttributeExpressions(flowFile).getValue();
        final String mappingName = context.getProperty(MAPPING_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String partiallySucceededRoutingStrategy = context.getProperty(PARTIALLY_SUCCEEDED_ROUTING_STRATEGY).getValue();
        final Duration ingestionStatusPollingTimeout = Duration.ofSeconds(context.getProperty(INGEST_STATUS_POLLING_TIMEOUT).asTimePeriod(TimeUnit.SECONDS));
        final Duration ingestionStatusPollingInterval = Duration.ofSeconds(context.getProperty(INGEST_STATUS_POLLING_INTERVAL).asTimePeriod(TimeUnit.SECONDS));
        final boolean ignoreFirstRecord = context.getProperty(IGNORE_FIRST_RECORD).asBoolean();
        final boolean streamingEnabled = context.getProperty(STREAMING_ENABLED).evaluateAttributeExpressions().asBoolean();
        final boolean pollOnIngestionStatus = context.getProperty(POLL_FOR_INGEST_STATUS).evaluateAttributeExpressions().asBoolean();

        Relationship transferRelationship = FAILURE;
        try (final InputStream inputStream = session.read(flowFile)) {
            final KustoIngestionRequest request = new KustoIngestionRequest(streamingEnabled, pollOnIngestionStatus, inputStream, databaseName,
                    tableName, dataFormat, mappingName, ignoreFirstRecord, ingestionStatusPollingTimeout, ingestionStatusPollingInterval);

            final KustoIngestionResult result = service.ingestData(request);
            if (result == KustoIngestionResult.SUCCEEDED) {
                getLogger().info("Ingest {} for {}", result.getStatus(), flowFile);
                transferRelationship = SUCCESS;
            } else if (result == KustoIngestionResult.FAILED) {
                getLogger().error("Ingest {} for {}", result.getStatus(), flowFile);
            } else if (result == KustoIngestionResult.PARTIALLY_SUCCEEDED) {
                getLogger().warn("Ingest {} for {}", result.getStatus(), flowFile);
                flowFile = session.putAttribute(flowFile, "ingestion_status", KustoIngestionResult.PARTIALLY_SUCCEEDED.getStatus());
                if (StringUtils.equalsIgnoreCase(partiallySucceededRoutingStrategy, SUCCESS.getName())) {
                    transferRelationship = SUCCESS;
                }
            }
        } catch (final IOException e) {
            getLogger().error("Azure Data Explorer Ingest processing failed {}", e, flowFile);
        }

        session.transfer(flowFile, transferRelationship);
    }
}