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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.prometheus.api.PrometheusMetricsUtil;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.yammer.metrics.core.VirtualMachineMetrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

public class PrometheusServer {
    private static ComponentLog logger;
    private Server server;
    private ServletContextHandler handler;
    private ReportingContext context;
    private boolean sendJvmMetrics;
    private String applicationId;

    class MetricsServlet extends HttpServlet {
        private CollectorRegistry nifiRegistry, jvmRegistry;
        private ProcessGroupStatus rootGroupStatus;

        @Override
        protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
            logger.info("PrometheusServer Do get called");

            rootGroupStatus = PrometheusServer.this.context.getEventAccess().getControllerStatus();
            ServletOutputStream response = resp.getOutputStream();
            OutputStreamWriter osw = new OutputStreamWriter(response);
            nifiRegistry = PrometheusMetricsUtil.createNifiMetrics(rootGroupStatus, PrometheusServer.this.applicationId);
            TextFormat.write004(osw, nifiRegistry.metricFamilySamples());

            if (PrometheusServer.this.sendJvmMetrics == true) {
                jvmRegistry = PrometheusMetricsUtil.createJvmMetrics(VirtualMachineMetrics.getInstance());
                TextFormat.write004(osw, jvmRegistry.metricFamilySamples());
            }

            osw.flush();
            osw.close();
            response.flush();
            response.close();
            resp.setHeader("Content-Type", TextFormat.CONTENT_TYPE_004);
            resp.setStatus(HttpURLConnection.HTTP_OK);
            resp.flushBuffer();
        }
    }

    public PrometheusServer(InetSocketAddress addr, ComponentLog logger) throws Exception {
        PrometheusServer.logger = logger;
        this.server = new Server(addr);

        this.handler = new ServletContextHandler(server, "/metrics");
        this.handler.addServlet(new ServletHolder(new MetricsServlet()), "/");
        this.server.start();
    }

    public Server getServer() {
        return this.server;
    }

    public ReportingContext getReportingContext() {
        return this.context;
    }

    public boolean getSendJvmMetrics() {
        return this.sendJvmMetrics;
    }

    public String getApplicationId() {
        return this.applicationId;
    }

    public void setReportingContext(ReportingContext rc) {
        this.context = rc;
    }

    public void setSendJvmMetrics(boolean jvm) {
        this.sendJvmMetrics = jvm;
    }

    public void setApplicationId(String aid) {
        this.applicationId = aid;
    }

}
