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
package org.apache.nifi.reporting.azure.loganalytics;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Tags({ "azure", "provenace", "reporting", "log analytics" })
@CapabilityDescription("Publishes Provenance events to to a Azure Log Analytics workspace.")
public class AzureLogAnalyticsProvenanceReportingTask extends AbstractAzureLogAnalyticsReportingTask {

        protected static final String LAST_EVENT_ID_KEY = "last_event_id";
        protected static final String DESTINATION_URL_PATH = "/nifi";
        protected static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT);

        static final PropertyDescriptor LOG_ANALYTICS_CUSTOM_LOG_NAME = new PropertyDescriptor.Builder()
                        .name("Log Analytics Custom Log Name").description("Log Analytics Custom Log Name").required(false)
                        .defaultValue("nifiprovenance").addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT).build();

        static final AllowableValue BEGINNING_OF_STREAM = new AllowableValue("beginning-of-stream",
                        "Beginning of Stream",
                        "Start reading provenance Events from the beginning of the stream (the oldest event first)");

        static final AllowableValue END_OF_STREAM = new AllowableValue("end-of-stream", "End of Stream",
                        "Start reading provenance Events from the end of the stream, ignoring old events");

        static final PropertyDescriptor FILTER_EVENT_TYPE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-event-filter").displayName("Event Type to Include")
                        .description("Comma-separated list of event types that will be used to filter the provenance events sent by the reporting task. "
                                        + "Available event types are "
                                        + Arrays.deepToString(ProvenanceEventType.values())
                                        + ". If no filter is set, all the events are sent. If "
                                        + "multiple filters are set, the filters are cumulative.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor FILTER_EVENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-event-filter-exclude").displayName("Event Type to Exclude")
                        .description("Comma-separated list of event types that will be used to exclude the provenance events sent by the reporting task. "
                                        + "Available event types are "
                                        + Arrays.deepToString(ProvenanceEventType.values())
                                        + ". If no filter is set, all the events are sent. If "
                                        + "multiple filters are set, the filters are cumulative. If an event type is included in Event Type to Include and excluded here, then the "
                                        + "exclusion takes precedence and the event will not be sent.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_TYPE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-type-filter").displayName("Component Type to Include")
                        .description("Regular expression to filter the provenance events based on the component type. Only the events matching the regular "
                                        + "expression will be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-type-filter-exclude").displayName("Component Type to Exclude")
                        .description("Regular expression to exclude the provenance events based on the component type. The events matching the regular "
                                        + "expression will not be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative. "
                                        + "If a component type is included in Component Type to Include and excluded here, then the exclusion takes precedence and the event will not be sent.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_ID = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-id-filter").displayName("Component ID to Include")
                        .description("Comma-separated list of component UUID that will be used to filter the provenance events sent by the reporting task. If no "
                                        + "filter is set, all the events are sent. If multiple filters are set, the filters are cumulative.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_ID_EXCLUDE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-id-filter-exclude").displayName("Component ID to Exclude")
                        .description("Comma-separated list of component UUID that will be used to exclude the provenance events sent by the reporting task. If no "
                                        + "filter is set, all the events are sent. If multiple filters are set, the filters are cumulative. If a component UUID is included in "
                                        + "Component ID to Include and excluded here, then the exclusion takes precedence and the event will not be sent.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_NAME = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-name-filter").displayName("Component Name to Include")
                        .description("Regular expression to filter the provenance events based on the component name. Only the events matching the regular "
                                        + "expression will be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

        static final PropertyDescriptor FILTER_COMPONENT_NAME_EXCLUDE = new PropertyDescriptor.Builder()
                        .name("s2s-prov-task-name-filter-exclude").displayName("Component Name to Exclude")
                        .description("Regular expression to exclude the provenance events based on the component name. The events matching the regular "
                                        + "expression will not be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative. "
                                        + "If a component name is included in Component Name to Include and excluded here, then the exclusion takes precedence and the event will not be sent.")
                        .required(false).expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).build();

        static final PropertyDescriptor START_POSITION = new PropertyDescriptor.Builder().name("start-position")
                        .displayName("Start Position")
                        .description("If the Reporting Task has never been run, or if its state has been reset by a user, "
                                        + "specifies where in the stream of Provenance Events the Reporting Task should start")
                        .allowableValues(BEGINNING_OF_STREAM, END_OF_STREAM)
                        .defaultValue(BEGINNING_OF_STREAM).required(true).build();

        static final PropertyDescriptor ALLOW_NULL_VALUES = new PropertyDescriptor.Builder().name("include-null-values")
                        .displayName("Include Null Values")
                        .description("Indicate if null values should be included in records. Default will be false")
                        .required(true).allowableValues("true", "false").defaultValue("false").build();

        static final PropertyDescriptor PLATFORM = new PropertyDescriptor.Builder().name("Platform")
                        .description("The value to use for the platform field in each event.").required(true)
                        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT).defaultValue("nifi")
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor INSTANCE_URL = new PropertyDescriptor.Builder().name("Instance URL")
                        .displayName("Instance URL")
                        .description("The URL of this instance to use in the Content URI of each event.").required(true)
                        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                        .defaultValue("http://${hostname(true)}:8080/nifi")
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder().name("Batch Size")
                        .displayName("Batch Size")
                        .description("Specifies how many records to send in a single batch, at most.").required(true)
                        .defaultValue("1000").addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();

        private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
                LOG_ANALYTICS_WORKSPACE_ID,
                LOG_ANALYTICS_CUSTOM_LOG_NAME,
                LOG_ANALYTICS_WORKSPACE_KEY,
                APPLICATION_ID,
                INSTANCE_ID,
                JOB_NAME,
                LOG_ANALYTICS_URL_ENDPOINT_FORMAT,
                FILTER_EVENT_TYPE,
                FILTER_EVENT_TYPE_EXCLUDE,
                FILTER_COMPONENT_TYPE,
                FILTER_COMPONENT_TYPE_EXCLUDE,
                FILTER_COMPONENT_ID,
                FILTER_COMPONENT_ID_EXCLUDE,
                FILTER_COMPONENT_NAME,
                FILTER_COMPONENT_NAME_EXCLUDE,
                START_POSITION,
                ALLOW_NULL_VALUES,
                PLATFORM,
                INSTANCE_URL,
                BATCH_SIZE
        );

        private volatile ProvenanceEventConsumer consumer;

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                return PROPERTY_DESCRIPTORS;
        }

        public void CreateConsumer(final ReportingContext context) {
                if (consumer != null)
                        return;
                consumer = new ProvenanceEventConsumer();
                consumer.setStartPositionValue(context.getProperty(START_POSITION).getValue());
                consumer.setBatchSize(context.getProperty(BATCH_SIZE).asInteger());
                consumer.setLogger(getLogger());
                // initialize component type filtering
                consumer.setComponentTypeRegex(
                                context.getProperty(FILTER_COMPONENT_TYPE).evaluateAttributeExpressions().getValue());
                consumer.setComponentTypeRegexExclude(context.getProperty(FILTER_COMPONENT_TYPE_EXCLUDE)
                                .evaluateAttributeExpressions().getValue());
                consumer.setComponentNameRegex(
                                context.getProperty(FILTER_COMPONENT_NAME).evaluateAttributeExpressions().getValue());
                consumer.setComponentNameRegexExclude(context.getProperty(FILTER_COMPONENT_NAME_EXCLUDE)
                                .evaluateAttributeExpressions().getValue());

                final String[] targetEventTypes = StringUtils.stripAll(StringUtils.split(
                                context.getProperty(FILTER_EVENT_TYPE).evaluateAttributeExpressions().getValue(), ','));
                if (targetEventTypes != null) {
                        for (final String type : targetEventTypes) {
                                try {
                                        consumer.addTargetEventType(ProvenanceEventType.valueOf(type));
                                } catch (final Exception e) {
                                        getLogger().warn(type
                                                        + " is not a correct event type, removed from the filtering.");
                                }
                        }
                }

                final String[] targetEventTypesExclude = StringUtils
                                .stripAll(StringUtils.split(context.getProperty(FILTER_EVENT_TYPE_EXCLUDE)
                                                .evaluateAttributeExpressions().getValue(), ','));
                if (targetEventTypesExclude != null) {
                        for (final String type : targetEventTypesExclude) {
                                try {
                                        consumer.addTargetEventTypeExclude(ProvenanceEventType.valueOf(type));
                                } catch (final Exception e) {
                                        getLogger().warn(type
                                                        + " is not a correct event type, removed from the exclude filtering.");
                                }
                        }
                }

                // initialize component ID filtering
                final String[] targetComponentIds = StringUtils.stripAll(StringUtils.split(
                                context.getProperty(FILTER_COMPONENT_ID).evaluateAttributeExpressions().getValue(),
                                ','));
                if (targetComponentIds != null) {
                        consumer.addTargetComponentId(targetComponentIds);
                }

                final String[] targetComponentIdsExclude = StringUtils
                                .stripAll(StringUtils.split(context.getProperty(FILTER_COMPONENT_ID_EXCLUDE)
                                                .evaluateAttributeExpressions().getValue(), ','));
                if (targetComponentIdsExclude != null) {
                        consumer.addTargetComponentIdExclude(targetComponentIdsExclude);
                }

                consumer.setScheduled(true);
        }

        @Override
        public void onTrigger(ReportingContext context) {
                final boolean isClustered = context.isClustered();
                final String nodeId = context.getClusterNodeIdentifier();
                if (nodeId == null && isClustered) {
                        getLogger().debug(
                                        "This instance of NiFi is configured for clustering, but the Cluster Node Identifier is not yet available. "
                                                        + "Will wait for Node Identifier to be established.");
                        return;
                }

                try {
                        processProvenanceData(context);

                } catch (final Exception e) {
                        getLogger().error("Failed to publish metrics to Azure Log Analytics", e);
                }
        }

        public void processProvenanceData(final ReportingContext context) {
                getLogger().debug("Starting to process provenance data");
                final String workspaceId = context.getProperty(LOG_ANALYTICS_WORKSPACE_ID)
                                .evaluateAttributeExpressions().getValue();
                final String linuxPrimaryKey = context.getProperty(LOG_ANALYTICS_WORKSPACE_KEY)
                                .evaluateAttributeExpressions().getValue();
                final String logName = context.getProperty(LOG_ANALYTICS_CUSTOM_LOG_NAME).evaluateAttributeExpressions()
                                .getValue();
                final String urlEndpointFormat = context.getProperty(LOG_ANALYTICS_URL_ENDPOINT_FORMAT)
                                .evaluateAttributeExpressions().getValue();
                final Integer batchSize = context.getProperty(BATCH_SIZE).asInteger();
                final String dataCollectorEndpoint = MessageFormat.format(urlEndpointFormat, workspaceId);
                final ProcessGroupStatus procGroupStatus = context.getEventAccess().getControllerStatus();
                final String nodeId = context.getClusterNodeIdentifier();
                final String rootGroupName = procGroupStatus == null ? null : procGroupStatus.getName();
                final String platform = context.getProperty(PLATFORM).evaluateAttributeExpressions().getValue();
                final Boolean allowNullValues = context.getProperty(ALLOW_NULL_VALUES).asBoolean();
                final String nifiUrl = context.getProperty(INSTANCE_URL).evaluateAttributeExpressions().getValue();
                URL url;
                try {
                        url = URI.create(nifiUrl).toURL();
                } catch (IllegalArgumentException | MalformedURLException e) {
                        throw new AssertionError();
                }

                final String hostname = url.getHost();
                final Map<String, Object> config = Collections.emptyMap();
                final JsonBuilderFactory factory = Json.createBuilderFactory(config);
                final JsonObjectBuilder builder = factory.createObjectBuilder();
                CreateConsumer(context);
                consumer.consumeEvents(context, (mapHolder, events) -> {
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append('[');
                        for (final ProvenanceEventRecord event : events) {
                                final String componentName = mapHolder.getComponentName(event.getComponentId());
                                final String processGroupId = mapHolder.getProcessGroupId(event.getComponentId(),
                                                event.getComponentType());
                                final String processGroupName = mapHolder.getComponentName(processGroupId);
                                final JsonObject jo = serialize(factory, builder, event, componentName,
                                                processGroupId, processGroupName, hostname, url, rootGroupName,
                                                platform, nodeId, allowNullValues);
                                stringBuilder.append(jo.toString());
                                stringBuilder.append(',');
                        }
                        if (stringBuilder.charAt(stringBuilder.length() - 1) == ',')
                                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                        stringBuilder.append(']');
                        String str = stringBuilder.toString();
                        if (!str.equals("[]")) {
                                final HttpPost httpPost = new HttpPost(dataCollectorEndpoint);
                                httpPost.addHeader("Content-Type", "application/json");
                                httpPost.addHeader("Log-Type", logName);
                                getLogger().debug("Sending {} events of length {} to azure log analytics {}", batchSize, str.length(), logName);
                                try {
                                        sendToLogAnalytics(httpPost, workspaceId, linuxPrimaryKey, str);

                                } catch (final Exception e) {
                                        getLogger().error("Failed to publish provenance data to Azure Log Analytics", e);
                                }
                        }
                });
                getLogger().debug("Done processing provenance data");
        }

        private JsonObject serialize(final JsonBuilderFactory factory, final JsonObjectBuilder builder,
                        final ProvenanceEventRecord event, final String componentName,
                        final String processGroupId, final String processGroupName, final String hostname,
                        final URL nifiUrl, final String applicationName, final String platform,
                        final String nodeIdentifier, Boolean allowNullValues) {
                addField(builder, "eventId", UUID.randomUUID().toString(), allowNullValues);
                addField(builder, "eventOrdinal", event.getEventId(), allowNullValues);
                addField(builder, "eventType", event.getEventType().name(), allowNullValues);
                addField(builder, "timestampMillis", event.getEventTime(), allowNullValues);
                addField(builder, "timestamp", DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(event.getEventTime()).atOffset(ZoneOffset.UTC)), allowNullValues);
                addField(builder, "durationMillis", event.getEventDuration(), allowNullValues);
                addField(builder, "lineageStart", event.getLineageStartDate(), allowNullValues);
                addField(builder, "details", event.getDetails(), allowNullValues);
                addField(builder, "componentId", event.getComponentId(), allowNullValues);
                addField(builder, "componentType", event.getComponentType(), allowNullValues);
                addField(builder, "componentName", componentName, allowNullValues);
                addField(builder, "processGroupId", processGroupId, allowNullValues);
                addField(builder, "processGroupName", processGroupName, allowNullValues);
                addField(builder, "entityId", event.getFlowFileUuid(), allowNullValues);
                addField(builder, "entityType", "org.apache.nifi.flowfile.FlowFile", allowNullValues);
                addField(builder, "entitySize", event.getFileSize(), allowNullValues);
                addField(builder, "previousEntitySize", event.getPreviousFileSize(), allowNullValues);
                addField(builder, factory, "updatedAttributes", event.getUpdatedAttributes(), allowNullValues);
                addField(builder, factory, "previousAttributes", event.getPreviousAttributes(), allowNullValues);

                addField(builder, "actorHostname", hostname, allowNullValues);
                if (nifiUrl != null) {
                        // TO get URL Prefix, we just remove the /nifi from the end of the URL. We know
                        // that the URL ends with
                        // "/nifi" because the Property Validator enforces it
                        final String urlString = nifiUrl.toString();
                        final String urlPrefix = urlString.substring(0,
                                        urlString.length() - DESTINATION_URL_PATH.length());

                        final String contentUriBase = urlPrefix + "/nifi-api/provenance-events/" + event.getEventId()
                                        + "/content/";
                        final String nodeIdSuffix = nodeIdentifier == null ? "" : "?clusterNodeId=" + nodeIdentifier;
                        addField(builder, "contentURI", contentUriBase + "output" + nodeIdSuffix, allowNullValues);
                        addField(builder, "previousContentURI", contentUriBase + "input" + nodeIdSuffix,
                                        allowNullValues);
                }

                addField(builder, factory, "parentIds", event.getParentUuids(), allowNullValues);
                addField(builder, factory, "childIds", event.getChildUuids(), allowNullValues);
                addField(builder, "transitUri", event.getTransitUri(), allowNullValues);
                addField(builder, "remoteIdentifier", event.getSourceSystemFlowFileIdentifier(), allowNullValues);
                addField(builder, "alternateIdentifier", event.getAlternateIdentifierUri(), allowNullValues);
                addField(builder, "platform", platform, allowNullValues);
                addField(builder, "application", applicationName, allowNullValues);
                return builder.build();
        }

        @OnUnscheduled
        public void onUnscheduled() {
                if (consumer != null) {
                        getLogger().debug("Disabling schedule to consume provenance data.");
                        consumer.setScheduled(false);
                }
        }

        public static void addField(final JsonObjectBuilder builder, final String key, final Object value,
                        boolean allowNullValues) {
                switch (value) {
                        case String s -> builder.add(key, s);
                        case Integer i -> builder.add(key, i);
                        case Boolean b -> builder.add(key, b);
                        case Long l -> builder.add(key, l);
                        case null -> {
                                if (allowNullValues) builder.add(key, JsonValue.NULL);
                        }
                        default -> builder.add(key, value.toString());
                }
        }

        public static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key,
                        final Map<String, String> values, Boolean allowNullValues) {
                if (values != null) {
                        final JsonObjectBuilder mapBuilder = factory.createObjectBuilder();
                        for (final Map.Entry<String, String> entry : values.entrySet()) {
                                if (entry.getKey() == null) continue;
                                if (entry.getValue() == null) {
                                        if (allowNullValues) {
                                                mapBuilder.add(entry.getKey(), JsonValue.NULL);
                                        }
                                } else {
                                        mapBuilder.add(entry.getKey(), entry.getValue());
                                }
                        }

                        builder.add(key, mapBuilder);
                } else if (allowNullValues) {
                        builder.add(key, JsonValue.NULL);
                }
        }

        public static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key,
                        final Collection<String> values, Boolean allowNullValues) {
                if (values != null) {
                        builder.add(key, createJsonArray(factory, values));
                } else if (allowNullValues) {
                        builder.add(key, JsonValue.NULL);
                }
        }

        private static JsonArrayBuilder createJsonArray(JsonBuilderFactory factory, final Collection<String> values) {
                final JsonArrayBuilder builder = factory.createArrayBuilder();
                for (final String value : values) {
                        if (value != null) {
                                builder.add(value);
                        }
                }
                return builder;
        }
}
