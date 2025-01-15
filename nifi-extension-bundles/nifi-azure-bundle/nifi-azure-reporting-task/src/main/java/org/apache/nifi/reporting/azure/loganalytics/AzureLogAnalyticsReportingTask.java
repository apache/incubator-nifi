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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.http.client.methods.HttpPost;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.metrics.jvm.JvmMetrics;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.azure.loganalytics.api.AzureLogAnalyticsMetricsFactory;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ReportingTask to send metrics from Apache NiFi and JVM to Azure Monitor.
 */
@Tags({ "azure", "metrics", "reporting", "log analytics" })
@CapabilityDescription("Sends JVM-metrics as well as Apache NiFi-metrics to a Azure Log Analytics workspace."
        + "Apache NiFi-metrics can be either configured global or on process-group level.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class AzureLogAnalyticsReportingTask extends AbstractAzureLogAnalyticsReportingTask {

    private static final String JVM_JOB_NAME = "jvm_global";
    private final JvmMetrics virtualMachineMetrics = JmxJvmMetrics.getInstance();

    static final PropertyDescriptor SEND_JVM_METRICS = new PropertyDescriptor.Builder().name("Send JVM Metrics")
            .description("Send JVM Metrics in addition to the NiFi-metrics").allowableValues("true", "false")
            .defaultValue("false").required(true).build();
    static final PropertyDescriptor LOG_ANALYTICS_CUSTOM_LOG_NAME = new PropertyDescriptor.Builder()
            .name("Log Analytics Custom Log Name").description("Log Analytics Custom Log Name").required(false)
            .defaultValue("nifimetrics").addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT).build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SEND_JVM_METRICS,
            LOG_ANALYTICS_WORKSPACE_ID,
            LOG_ANALYTICS_CUSTOM_LOG_NAME,
            LOG_ANALYTICS_WORKSPACE_KEY,
            APPLICATION_ID,
            INSTANCE_ID,
            PROCESS_GROUP_IDS,
            JOB_NAME,
            LOG_ANALYTICS_URL_ENDPOINT_FORMAT
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final String workspaceId = context.getProperty(LOG_ANALYTICS_WORKSPACE_ID).evaluateAttributeExpressions().getValue();
        final String linuxPrimaryKey = context.getProperty(LOG_ANALYTICS_WORKSPACE_KEY).evaluateAttributeExpressions().getValue();
        final boolean jvmMetricsCollected = context.getProperty(SEND_JVM_METRICS).asBoolean();
        final String logName = context.getProperty(LOG_ANALYTICS_CUSTOM_LOG_NAME).evaluateAttributeExpressions().getValue();
        final String instanceId = context.getProperty(INSTANCE_ID).evaluateAttributeExpressions().getValue();
        final String groupIds = context.getProperty(PROCESS_GROUP_IDS).evaluateAttributeExpressions().getValue();
        final String urlEndpointFormat = context.getProperty(LOG_ANALYTICS_URL_ENDPOINT_FORMAT)
                .evaluateAttributeExpressions().getValue();

        try {
            final List<Metric> allMetrics;
            if (groupIds == null || groupIds.isEmpty()) {
                ProcessGroupStatus status = context.getEventAccess().getControllerStatus();
                String processGroupName = status.getName();
                allMetrics = collectMetrics(instanceId, status, processGroupName, jvmMetricsCollected);
            } else {
                allMetrics = new ArrayList<>();
                for (String groupId : groupIds.split(",")) {
                    groupId = groupId.trim();
                    ProcessGroupStatus status = context.getEventAccess().getGroupStatus(groupId);
                    String processGroupName = status.getName();
                    allMetrics.addAll(collectMetrics(instanceId, status, processGroupName, jvmMetricsCollected));
                }
            }
            HttpPost httpPost = getHttpPost(urlEndpointFormat, workspaceId, logName);
            sendMetrics(httpPost, workspaceId, linuxPrimaryKey, allMetrics);
        } catch (Exception e) {
            getLogger().error("Failed to publish metrics to Azure Log Analytics", e);
        }
    }

    /**
     * send collected metrics to azure log analytics workspace
     *
     * @param request         HttpPost to Azure Log Analytics Endpoint
     * @param workspaceId     your azure log analytics workspace id
     * @param linuxPrimaryKey your azure log analytics workspace key
     * @param allMetrics      collected metrics to be sent
     * @throws IOException              when there is an error in https url
     *                                  connection or read/write to the onnection
     * @throws IllegalArgumentException when there a exception in converting metrics
     *                                  to json string with Gson.toJson
     * @throws RuntimeException         when httpPost fails with none 200 status
     *                                  code
     */
    protected void sendMetrics(final HttpPost request, final String workspaceId, final String linuxPrimaryKey,
            final List<Metric> allMetrics) throws IOException, IllegalArgumentException, RuntimeException {
        Gson gson = new GsonBuilder().create();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        for (Metric current : allMetrics) {
            builder.append(gson.toJson(current));
            builder.append(',');
        }
        builder.append(']');
        sendToLogAnalytics(request, workspaceId, linuxPrimaryKey, builder.toString());
    }

    /**
     * collect metrics to be sent to azure log analytics workspace
     *
     * @param instanceId          instance id
     * @param status              process group status
     * @param processGroupName    process group name
     * @param jvmMetricsCollected whether we want to collect jvm metrics or not
     * @return list of metrics collected
     */
    protected List<Metric> collectMetrics(final String instanceId, final ProcessGroupStatus status,
            final String processGroupName, final boolean jvmMetricsCollected) {
        List<Metric> allMetrics = new ArrayList<>();

        // dataflow process group level metrics
        allMetrics.addAll(AzureLogAnalyticsMetricsFactory.getDataFlowMetrics(status, instanceId));

        // connections process group level metrics
        final List<ConnectionStatus> connectionStatuses = new ArrayList<>();
        populateConnectionStatuses(status, connectionStatuses);
        for (ConnectionStatus connectionStatus : connectionStatuses) {
            allMetrics.addAll(AzureLogAnalyticsMetricsFactory.getConnectionStatusMetrics(connectionStatus, instanceId,
                    processGroupName));
        }

        // processor level metrics
        final List<ProcessorStatus> processorStatuses = new ArrayList<>();
        populateProcessorStatuses(status, processorStatuses);
        for (final ProcessorStatus processorStatus : processorStatuses) {
            allMetrics.addAll(
                    AzureLogAnalyticsMetricsFactory.getProcessorMetrics(processorStatus, instanceId, processGroupName));
        }

        if (jvmMetricsCollected) {
            allMetrics.addAll(
                    AzureLogAnalyticsMetricsFactory.getJvmMetrics(virtualMachineMetrics, instanceId, JVM_JOB_NAME));

        }
        return allMetrics;
    }

    private void populateProcessorStatuses(final ProcessGroupStatus groupStatus, final List<ProcessorStatus> statuses) {
        statuses.addAll(groupStatus.getProcessorStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateProcessorStatuses(childGroupStatus, statuses);
        }
    }

    private void populateConnectionStatuses(final ProcessGroupStatus groupStatus,
            final List<ConnectionStatus> statuses) {
        statuses.addAll(groupStatus.getConnectionStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateConnectionStatuses(childGroupStatus, statuses);
        }
    }
}
