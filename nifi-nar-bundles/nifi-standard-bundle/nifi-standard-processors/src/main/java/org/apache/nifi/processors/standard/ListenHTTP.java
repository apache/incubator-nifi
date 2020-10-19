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
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Path;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.servlets.ContentAcknowledgmentServlet;
import org.apache.nifi.processors.standard.servlets.HealthCheckServlet;
import org.apache.nifi.processors.standard.servlets.ListenHTTPServlet;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.stream.io.LeakyBucketStreamThrottler;
import org.apache.nifi.stream.io.StreamThrottler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"ingest", "http", "https", "rest", "listen"})
@CapabilityDescription("Starts an HTTP Server and listens on a given base path to transform incoming requests into FlowFiles. "
        + "The default URI of the Service will be http://{hostname}:{port}/contentListener. Only HEAD and POST requests are "
        + "supported. GET, PUT, and DELETE will result in an error and the HTTP response status code 405. "
        + "GET is supported on <service_URI>/healthcheck. If the service is available, it returns \"200 OK\" with an empty response body. "
        + "The health check functionality can be configured to be accessible via a different port. "
        + "For details see the documentation of the \"Listening Port for health check requests\" property.")
public class ListenHTTP extends AbstractSessionFactoryProcessor {

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicBoolean runOnPrimary = new AtomicBoolean(false);

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Relationship for successfully received FlowFiles")
        .build();

    public static final PropertyDescriptor BASE_PATH = new PropertyDescriptor.Builder()
        .name("Base Path")
        .description("Base path for incoming connections")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .defaultValue("contentListener")
        .addValidator(StandardValidators.URI_VALIDATOR)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("(^[^/]+.*[^/]+$|^[^/]+$|^$)"))) // no start with / or end with /
        .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
        .name("Listening Port")
        .description("The Port to listen on for incoming connections")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();
    public static final PropertyDescriptor HEALTH_CHECK_PORT = new PropertyDescriptor.Builder()
            .name("health-check-port")
            .displayName("Listening Port for health check requests")
            .description("The port to listen on for incoming health check requests. " +
                    "If set, it must be different from the Listening Port. " +
                    "Configure this port if the processor is set to use two-way SSL and a load balancer that does not support client authentication for " +
                    "health check requests is used. " +
                    "Only /<base_path>/healthcheck service is available via this port and only GET and HEAD requests are supported. " +
                    "If the processor is set not to use SSL, SSL will not be used on this port, either. " +
                    "If the processor is set to use one-way SSL, one-way SSL will be used on this port. " +
                    "If the processor is set to use two-way SSL, one-way SSL will be used on this port (client authentication not required).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor AUTHORIZED_DN_PATTERN = new PropertyDescriptor.Builder()
        .name("Authorized DN Pattern")
        .description("A Regular Expression to apply against the Distinguished Name of incoming connections. If the Pattern does not match the DN, the connection will be refused.")
        .required(true)
        .defaultValue(".*")
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
        .build();
    public static final PropertyDescriptor MAX_UNCONFIRMED_TIME = new PropertyDescriptor.Builder()
        .name("Max Unconfirmed Flowfile Time")
        .description("The maximum amount of time to wait for a FlowFile to be confirmed before it is removed from the cache")
        .required(true)
        .defaultValue("60 secs")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();
    public static final PropertyDescriptor MAX_DATA_RATE = new PropertyDescriptor.Builder()
        .name("Max Data to Receive per Second")
        .description("The maximum amount of data to receive per second; this allows the bandwidth to be throttled to a specified data rate; if not specified, the data rate is not throttled")
        .required(false)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL Context Service")
        .description("The Controller Service to use in order to obtain an SSL Context")
        .required(false)
        .identifiesControllerService(RestrictedSSLContextService.class)
        .build();
    public static final PropertyDescriptor HEADERS_AS_ATTRIBUTES_REGEX = new PropertyDescriptor.Builder()
        .name("HTTP Headers to receive as Attributes (Regex)")
        .description("Specifies the Regular Expression that determines the names of HTTP Headers that should be passed along as FlowFile attributes")
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
        .required(false)
        .build();
    public static final PropertyDescriptor RETURN_CODE = new PropertyDescriptor.Builder()
        .name("Return Code")
        .description("The HTTP return code returned after every HTTP call")
        .defaultValue(String.valueOf(HttpServletResponse.SC_OK))
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .build();
    public static final PropertyDescriptor MULTIPART_REQUEST_MAX_SIZE = new PropertyDescriptor.Builder()
        .name("multipart-request-max-size")
        .displayName("Multipart Request Max Size")
        .description("The max size of the request. Only applies for requests with Content-Type: multipart/form-data, "
                + "and is used to prevent denial of service type of attacks, to prevent filling up the heap or disk space")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("1 MB")
        .build();
    public static final PropertyDescriptor MULTIPART_READ_BUFFER_SIZE = new PropertyDescriptor.Builder()
        .name("multipart-read-buffer-size")
        .displayName("Multipart Read Buffer Size")
        .description("The threshold size, at which the contents of an incoming file would be written to disk. "
                + "Only applies for requests with Content-Type: multipart/form-data. "
                + "It is used to prevent denial of service type of attacks, to prevent filling up the heap or disk space.")
        .required(true)
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("512 KB")
        .build();

    public static final String CONTEXT_ATTRIBUTE_PROCESSOR = "processor";
    public static final String CONTEXT_ATTRIBUTE_LOGGER = "logger";
    public static final String CONTEXT_ATTRIBUTE_SESSION_FACTORY_HOLDER = "sessionFactoryHolder";
    public static final String CONTEXT_ATTRIBUTE_PROCESS_CONTEXT_HOLDER = "processContextHolder";
    public static final String CONTEXT_ATTRIBUTE_AUTHORITY_PATTERN = "authorityPattern";
    public static final String CONTEXT_ATTRIBUTE_HEADER_PATTERN = "headerPattern";
    public static final String CONTEXT_ATTRIBUTE_FLOWFILE_MAP = "flowFileMap";
    public static final String CONTEXT_ATTRIBUTE_STREAM_THROTTLER = "streamThrottler";
    public static final String CONTEXT_ATTRIBUTE_BASE_PATH = "basePath";
    public static final String CONTEXT_ATTRIBUTE_RETURN_CODE = "returnCode";
    public static final String CONTEXT_ATTRIBUTE_MULTIPART_REQUEST_MAX_SIZE = "multipartRequestMaxSize";
    public static final String CONTEXT_ATTRIBUTE_MULTIPART_READ_BUFFER_SIZE = "multipartReadBufferSize";
    public static final String CONTEXT_ATTRIBUTE_PORT = "port";

    private volatile Server server = null;
    private final ConcurrentMap<String, FlowFileEntryTimeWrapper> flowFileMap = new ConcurrentHashMap<>();
    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();
    private final AtomicReference<StreamThrottler> throttlerRef = new AtomicReference<>();

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> results = new ArrayList<>(1);

        validatePortsAreNotEqual(context, results);

        return results;
    }

    private void validatePortsAreNotEqual(ValidationContext context, Collection<ValidationResult> validationResults) {
        String healthCheckPortString = context.getProperty(HEALTH_CHECK_PORT).evaluateAttributeExpressions().getValue();
        if (healthCheckPortString != null) {
            int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
            int healthCheckPort = Integer.parseInt(healthCheckPortString);
            if (port == healthCheckPort) {
                String explanation = String.format("'%s' and '%s' cannot have the same value.", PORT.getDisplayName(), HEALTH_CHECK_PORT.getDisplayName());
                validationResults.add(createValidationResult(HEALTH_CHECK_PORT.getDisplayName(), explanation));
            }
        }
    }

    private ValidationResult createValidationResult(String subject, String explanation) {
        return new ValidationResult.Builder().subject(subject).valid(false).explanation(explanation).build();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RELATIONSHIP_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(BASE_PATH);
        descriptors.add(PORT);
        descriptors.add(HEALTH_CHECK_PORT);
        descriptors.add(MAX_DATA_RATE);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(AUTHORIZED_DN_PATTERN);
        descriptors.add(MAX_UNCONFIRMED_TIME);
        descriptors.add(HEADERS_AS_ATTRIBUTES_REGEX);
        descriptors.add(RETURN_CODE);
        descriptors.add(MULTIPART_REQUEST_MAX_SIZE);
        descriptors.add(MULTIPART_READ_BUFFER_SIZE);
        this.properties = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnStopped
    public void shutdownHttpServer() {
        final StreamThrottler throttler = throttlerRef.getAndSet(null);
        if(throttler != null) {
            try {
                throttler.close();
            } catch (IOException e) {
                getLogger().error("Failed to close StreamThrottler", e);
            }
        }

        final Server toShutdown = this.server;
        if (toShutdown == null) {
            return;
        }

        shutdownHttpServer(toShutdown);
    }

    private void shutdownHttpServer(Server toShutdown) {
        try {
            toShutdown.stop();
            toShutdown.destroy();
            clearInit();
        } catch (final Exception ex) {
            getLogger().warn("unable to cleanly shutdown embedded server due to {}", new Object[] {ex});
            this.server = null;
        }
    }

    synchronized private void createHttpServerFromService(final ProcessContext context) throws Exception {
        if(initialized.get()) {
            return;
        }
        runOnPrimary.set(context.getExecutionNode().equals(ExecutionNode.PRIMARY));
        final String basePath = context.getProperty(BASE_PATH).evaluateAttributeExpressions().getValue();
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final Double maxBytesPerSecond = context.getProperty(MAX_DATA_RATE).asDataSize(DataUnit.B);
        final StreamThrottler streamThrottler = (maxBytesPerSecond == null) ? null : new LeakyBucketStreamThrottler(maxBytesPerSecond.intValue());
        final int returnCode = context.getProperty(RETURN_CODE).asInteger();
        long requestMaxSize = context.getProperty(MULTIPART_REQUEST_MAX_SIZE).asDataSize(DataUnit.B).longValue();
        int readBufferSize = context.getProperty(MULTIPART_READ_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        throttlerRef.set(streamThrottler);

        final boolean needClientAuth = sslContextService != null && sslContextService.getTrustStoreFile() != null;
        final boolean sslRequired = ((sslContextService != null) && (sslContextService.getKeyStoreFile() != null));

        // thread pool for the jetty instance
        final QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setName(String.format("%s (%s) Web Server", getClass().getSimpleName(), getIdentifier()));

        // create the server instance
        final Server server = new Server(threadPool);

        // get the configured port
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();

        final ServerConnector connector = createServerConnector(server, port, sslContextService, sslRequired, needClientAuth);

        List<Connector> connectors = new ArrayList<>(2);
        connectors.add(connector);

        // Add a separate connector for the health check port (if specified)
        final String healthCheckPortString = context.getProperty(HEALTH_CHECK_PORT).evaluateAttributeExpressions().getValue();
        if (healthCheckPortString != null) {
            final int healthCheckPort = Integer.parseInt(healthCheckPortString);
            final ServerConnector healthCheckConnector = createServerConnector(server, healthCheckPort, sslContextService, sslRequired, false);
            connectors.add(healthCheckConnector);
        }

        // add the connector(s) to the server
        server.setConnectors(connectors.toArray(new Connector[0]));

        final ServletContextHandler contextHandler = new ServletContextHandler(server, "/", true, sslRequired);
        for (final Class<? extends Servlet> cls : getServerClasses()) {
            final Path path = cls.getAnnotation(Path.class);
            // Note: servlets must have a path annotation - this will NPE otherwise
            // also, servlets other than ListenHttpServlet must have a path starting with /
            if (basePath.isEmpty() && !path.value().isEmpty()) {
                // Note: this is to handle the condition of an empty uri, otherwise pathSpec would start with //
                contextHandler.addServlet(cls, path.value());
            } else {
                contextHandler.addServlet(cls, "/" + basePath + path.value());
            }
        }

        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_PROCESSOR, this);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_LOGGER, getLogger());
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_SESSION_FACTORY_HOLDER, sessionFactoryReference);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_PROCESS_CONTEXT_HOLDER, context);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_FLOWFILE_MAP, flowFileMap);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_AUTHORITY_PATTERN, Pattern.compile(context.getProperty(AUTHORIZED_DN_PATTERN).getValue()));
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_STREAM_THROTTLER, streamThrottler);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_BASE_PATH, basePath);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_RETURN_CODE, returnCode);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_MULTIPART_REQUEST_MAX_SIZE, requestMaxSize);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_MULTIPART_READ_BUFFER_SIZE, readBufferSize);
        contextHandler.setAttribute(CONTEXT_ATTRIBUTE_PORT, port);

        if (context.getProperty(HEADERS_AS_ATTRIBUTES_REGEX).isSet()) {
            contextHandler.setAttribute(CONTEXT_ATTRIBUTE_HEADER_PATTERN, Pattern.compile(context.getProperty(HEADERS_AS_ATTRIBUTES_REGEX).getValue()));
        }
        try {
            server.start();
        } catch (Exception e) {
            shutdownHttpServer(server);
            throw e;
        }

        this.server = server;
        initialized.set(true);
    }

    private ServerConnector createServerConnector(Server server, int port, SSLContextService sslContextService, boolean sslRequired, boolean needClientAuth) {
        final SslContextFactory contextFactory = createSslContextFactory(sslContextService, sslRequired, needClientAuth);
        return createServerConnector(server, port, contextFactory, sslRequired);
    }

    private ServerConnector createServerConnector(Server server, int port, SslContextFactory contextFactory, boolean sslRequired) {
        final ServerConnector connector;
        final HttpConfiguration httpConfiguration = new HttpConfiguration();
        if (sslRequired) {
            // configure the ssl connector
            httpConfiguration.setSecureScheme("https");
            httpConfiguration.setSecurePort(port);
            httpConfiguration.addCustomizer(new SecureRequestCustomizer());

            // build the connector
            connector = new ServerConnector(server, new SslConnectionFactory(contextFactory, "http/1.1"), new HttpConnectionFactory(httpConfiguration));
        } else {
            // create the connector
            connector = new ServerConnector(server, new HttpConnectionFactory(httpConfiguration));
        }

        // configure the port
        connector.setPort(port);
        return connector;
    }

    private SslContextFactory createSslContextFactory(SSLContextService sslContextService, boolean sslRequired, boolean needClientAuth) {
        final SslContextFactory contextFactory = new SslContextFactory.Server();
        contextFactory.setNeedClientAuth(needClientAuth);

        if (needClientAuth) {
            contextFactory.setTrustStorePath(sslContextService.getTrustStoreFile());
            contextFactory.setTrustStoreType(sslContextService.getTrustStoreType());
            contextFactory.setTrustStorePassword(sslContextService.getTrustStorePassword());
        }

        if (sslRequired) {
            final String keystorePassword = sslContextService.getKeyStorePassword();
            final String keyStoreType = sslContextService.getKeyStoreType();
            final String keyStorePath = sslContextService.getKeyStoreFile();

            contextFactory.setKeyStorePath(keyStorePath);
            contextFactory.setKeyManagerPassword(keystorePassword);
            contextFactory.setKeyStorePassword(keystorePassword);
            contextFactory.setKeyStoreType(keyStoreType);
        }

        if (sslContextService != null) {
            contextFactory.setProtocol(sslContextService.getSslAlgorithm());
        }
        return contextFactory;
    }

    @OnScheduled
    public void clearInit(){
        initialized.set(false);
    }

    protected Set<Class<? extends Servlet>> getServerClasses() {
        final Set<Class<? extends Servlet>> s = new HashSet<>();
        // NOTE: Servlets added below MUST have a Path annotation
        // any servlets other than ListenHTTPServlet must have a Path annotation start with /
        s.add(ListenHTTPServlet.class);
        s.add(ContentAcknowledgmentServlet.class);
        s.add(HealthCheckServlet.class);
        return s;
    }

    private Set<String> findOldFlowFileIds(final ProcessContext ctx) {
        final Set<String> old = new HashSet<>();

        final long expiryMillis = ctx.getProperty(MAX_UNCONFIRMED_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
        final long cutoffTime = System.currentTimeMillis() - expiryMillis;
        for (final Map.Entry<String, FlowFileEntryTimeWrapper> entry : flowFileMap.entrySet()) {
            final FlowFileEntryTimeWrapper wrapper = entry.getValue();
            if (wrapper != null && wrapper.getEntryTime() < cutoffTime) {
                old.add(entry.getKey());
            }
        }

        return old;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        try {
            if (!initialized.get()) {
                createHttpServerFromService(context);
            }
        } catch (Exception e) {
            getLogger().warn("Failed to start http server during initialization: " + e);
            context.yield();
            throw new ProcessException("Failed to initialize the server", e);
        }

        sessionFactoryReference.compareAndSet(null, sessionFactory);

        for (final String id : findOldFlowFileIds(context)) {
            final FlowFileEntryTimeWrapper wrapper = flowFileMap.remove(id);
            if (wrapper != null) {
                getLogger().warn("failed to received acknowledgment for HOLD with ID {} sent by {}; rolling back session", new Object[] {id, wrapper.getClientIP()});
                wrapper.session.rollback();
            }
        }

        context.yield();
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange(final PrimaryNodeState newState) {
        if (runOnPrimary.get() && newState.equals(PrimaryNodeState.PRIMARY_NODE_REVOKED)) {
            try {
                shutdownHttpServer();
            } catch (final Exception shutdownException) {
                getLogger().warn("Processor is configured to run only on Primary Node, but failed to shutdown HTTP server following revocation of primary node status due to {}",
                        shutdownException);
            }
        }
    }

    public static class FlowFileEntryTimeWrapper {

        private final Set<FlowFile> flowFiles;
        private final long entryTime;
        private final ProcessSession session;
        private final String clientIP;

        public FlowFileEntryTimeWrapper(final ProcessSession session, final Set<FlowFile> flowFiles, final long entryTime, final String clientIP) {
            this.flowFiles = flowFiles;
            this.entryTime = entryTime;
            this.session = session;
            this.clientIP = clientIP;
        }

        public Set<FlowFile> getFlowFiles() {
            return flowFiles;
        }

        public long getEntryTime() {
            return entryTime;
        }

        public ProcessSession getSession() {
            return session;
        }

        public String getClientIP() {
            return clientIP;
        }
    }
}
