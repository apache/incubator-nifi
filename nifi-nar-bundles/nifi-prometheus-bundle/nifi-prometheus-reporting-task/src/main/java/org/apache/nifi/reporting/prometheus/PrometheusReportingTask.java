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

package org.apache.nifi.reporting.prometheus;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.eclipse.jetty.server.Server;

@Tags({ "reporting", "prometheus", "metrics" })
@CapabilityDescription("Reports metrics in Prometheus format by creating /metrics http endpoint which can be used for external monitoring of the application."
        + "The reporting task reports a set of metrics regarding the JVM (optional) and the NiFi instance")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "60 sec")

public class PrometheusReportingTask extends AbstractReportingTask {

    private PrometheusServer prometheusServer;
    private SSLContextService sslContextService;

    public static final PropertyDescriptor METRICS_ENDPOINT_PORT = new PropertyDescriptor.Builder()
            .name("Prometheus Metrics Endpoint Port")
            .description("The Port where prometheus metrics can be accessed")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("9092")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor APPLICATION_ID = new PropertyDescriptor.Builder()
            .name("Application ID")
            .description("The Application ID to be included in the metrics sent to Prometheus")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INSTANCE_ID = new PropertyDescriptor.Builder()
            .name("Instance ID")
            .description("Id of this NiFi instance to be included in the metrics sent to Prometheus")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("${hostname(true)}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SEND_JVM_METRICS = new PropertyDescriptor.Builder()
            .name("Send JVM-metrics")
            .description("Send JVM-metrics in addition to the Nifi-metrics")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service to use in order to secure the server. If specified, the server will"
                    + "accept only HTTPS requests; otherwise, the server will accept only HTTP requests")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();
    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(METRICS_ENDPOINT_PORT);
        props.add(APPLICATION_ID);
        props.add(INSTANCE_ID);
        props.add(SEND_JVM_METRICS);
        props.add(SSL_CONTEXT);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) {
        sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        final String metricsEndpointPort = context.getProperty(METRICS_ENDPOINT_PORT).getValue();

        try {
            if (sslContextService == null) {
                this.prometheusServer = new PrometheusServer(new InetSocketAddress(Integer.parseInt(metricsEndpointPort)), getLogger());
            } else {
                this.prometheusServer = new PrometheusServer(Integer.parseInt(metricsEndpointPort), sslContextService, getLogger());
            }
            this.prometheusServer.setApplicationId(context.getProperty(APPLICATION_ID).evaluateAttributeExpressions().getValue());
            this.prometheusServer.setSendJvmMetrics(context.getProperty(SEND_JVM_METRICS).asBoolean());
            getLogger().info("Started JETTY server");
        } catch (Exception e) {
            getLogger().error("Failed to start Jetty server", e);
        }
    }

    @OnStopped
    public void OnStopped() throws Exception {
        Server server = this.prometheusServer.getServer();
        server.stop();
    }

    @OnShutdown
    public void onShutDown() throws Exception {
        Server server = prometheusServer.getServer();
        server.stop();
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        this.prometheusServer.setReportingContext(context);
    }
}
