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

import jakarta.servlet.AsyncContext;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.MultipartConfigElement;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.Part;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.jetty.configuration.connector.StandardServerConnectorFactory;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.http.CertificateAttribute;
import org.apache.nifi.processors.standard.http.CertificateAttributesProvider;
import org.apache.nifi.processors.standard.http.HandleHttpRequestCertificateAttributesProvider;
import org.apache.nifi.processors.standard.http.HttpProtocolStrategy;
import org.apache.nifi.processors.standard.util.HTTPUtils;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.stream.io.StreamUtils;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletContextRequest;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static jakarta.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static jakarta.servlet.http.HttpServletResponse.SC_METHOD_NOT_ALLOWED;
import static jakarta.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static jakarta.servlet.http.HttpServletResponse.SC_SERVICE_UNAVAILABLE;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"http", "https", "request", "listen", "ingress", "web service"})
@CapabilityDescription("Starts an HTTP Server and listens for HTTP Requests. For each request, creates a FlowFile and transfers to 'success'. "
        + "This Processor is designed to be used in conjunction with the HandleHttpResponse Processor in order to create a Web Service. In case "
        + " of a multipart request, one FlowFile is generated for each part.")
@WritesAttributes({
    @WritesAttribute(attribute = HTTPUtils.HTTP_CONTEXT_ID, description = "An identifier that allows the HandleHttpRequest and HandleHttpResponse "
            + "to coordinate which FlowFile belongs to which HTTP Request/Response."),
    @WritesAttribute(attribute = "mime.type", description = "The MIME Type of the data, according to the HTTP Header \"Content-Type\""),
    @WritesAttribute(attribute = "http.servlet.path", description = "The part of the request URL that is considered the Servlet Path"),
    @WritesAttribute(attribute = "http.context.path", description = "The part of the request URL that is considered to be the Context Path"),
    @WritesAttribute(attribute = "http.method", description = "The HTTP Method that was used for the request, such as GET or POST"),
    @WritesAttribute(attribute = HTTPUtils.HTTP_LOCAL_NAME, description = "IP address/hostname of the server"),
    @WritesAttribute(attribute = HTTPUtils.HTTP_PORT, description = "Listening port of the server"),
    @WritesAttribute(attribute = "http.query.string", description = "The query string portion of the Request URL"),
    @WritesAttribute(attribute = HTTPUtils.HTTP_REMOTE_HOST, description = "The hostname of the requestor"),
    @WritesAttribute(attribute = "http.remote.addr", description = "The hostname:port combination of the requestor"),
    @WritesAttribute(attribute = "http.remote.user", description = "The username of the requestor"),
    @WritesAttribute(attribute = "http.protocol", description = "The protocol used to communicate"),
    @WritesAttribute(attribute = HTTPUtils.HTTP_REQUEST_URI, description = "The full Request URL"),
    @WritesAttribute(attribute = "http.auth.type", description = "The type of HTTP Authorization used"),
    @WritesAttribute(attribute = "http.principal.name", description = "The name of the authenticated user making the request"),
    @WritesAttribute(attribute = "http.query.param.XXX", description = "Each of query parameters in the request will be added as an attribute, "
            + "prefixed with \"http.query.param.\""),
    @WritesAttribute(attribute = "http.param.XXX", description = "Form parameters in the request that are configured by \"Parameters to Attributes List\" will be added as an attribute, "
        + "prefixed with \"http.param.\". Putting form parameters of large size is not recommended."),
    @WritesAttribute(attribute = HTTPUtils.HTTP_SSL_CERT, description = "The Distinguished Name of the requestor. This value will not be populated "
            + "unless the Processor is configured to use an SSLContext Service"),
    @WritesAttribute(attribute = "http.issuer.dn", description = "The Distinguished Name of the entity that issued the Subject's certificate. "
            + "This value will not be populated unless the Processor is configured to use an SSLContext Service"),
    @WritesAttribute(attribute = "http.certificate.sans.N.name", description = "X.509 Client Certificate Subject Alternative Name value from mutual TLS authentication. "
            + "The attribute name has a zero-based index ordered according to the content of Client Certificate"),
    @WritesAttribute(attribute = "http.certificate.sans.N.nameType", description = "X.509 Client Certificate Subject Alternative Name type from mutual TLS authentication. "
            + "The attribute name has a zero-based index ordered according to the content of Client Certificate. The attribute value is one of the General Names from RFC 3280 Section 4.1.2.7"),
    @WritesAttribute(attribute = "http.headers.XXX", description = "Each of the HTTP Headers that is received in the request will be added as an "
            + "attribute, prefixed with \"http.headers.\" For example, if the request contains an HTTP Header named \"x-my-header\", then the value "
            + "will be added to an attribute named \"http.headers.x-my-header\""),
    @WritesAttribute(attribute = "http.headers.multipart.XXX", description = "Each of the HTTP Headers that is received in the multipart request will be added as an "
        + "attribute, prefixed with \"http.headers.multipart.\" For example, if the multipart request contains an HTTP Header named \"content-disposition\", then the value "
        + "will be added to an attribute named \"http.headers.multipart.content-disposition\""),
    @WritesAttribute(attribute = "http.multipart.size",
        description = "For requests with Content-Type \"multipart/form-data\", the part's content size is recorded into this attribute"),
    @WritesAttribute(attribute = "http.multipart.content.type",
        description = "For requests with Content-Type \"multipart/form-data\", the part's content type is recorded into this attribute"),
    @WritesAttribute(attribute = "http.multipart.name",
        description = "For requests with Content-Type \"multipart/form-data\", the part's name is recorded into this attribute"),
    @WritesAttribute(attribute = "http.multipart.filename",
        description = "For requests with Content-Type \"multipart/form-data\", when the part contains an uploaded file, the name of the file is recorded into this attribute. "
                    + "Files are stored temporarily at the default temporary-file directory specified in \"java.io.File\" Java Docs)"),
    @WritesAttribute(attribute = "http.multipart.fragments.sequence.number",
        description = "For requests with Content-Type \"multipart/form-data\", the part's index is recorded into this attribute. The index starts with 1."),
    @WritesAttribute(attribute = "http.multipart.fragments.total.number",
      description = "For requests with Content-Type \"multipart/form-data\", the count of all parts is recorded into this attribute.")})
@SeeAlso(value = {HandleHttpResponse.class})
public class HandleHttpRequest extends AbstractProcessor {

    private static final String MIME_TYPE__MULTIPART_FORM_DATA = "multipart/form-data";

    private static final Pattern URL_QUERY_PARAM_DELIMITER = Pattern.compile("&");

    // Allowable values for client auth
    public static final AllowableValue CLIENT_NONE = new AllowableValue("No Authentication", "No Authentication",
            "Processor will not authenticate clients. Anyone can communicate with this Processor anonymously");
    public static final AllowableValue CLIENT_WANT = new AllowableValue("Want Authentication", "Want Authentication",
            "Processor will try to verify the client but if unable to verify will allow the client to communicate anonymously");
    public static final AllowableValue CLIENT_NEED = new AllowableValue("Need Authentication", "Need Authentication",
            "Processor will reject communications from any client unless the client provides a certificate that is trusted by the TrustStore"
            + "specified in the SSL Context Service");

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Listening Port")
            .description("The Port to listen on for incoming HTTP requests")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("80")
            .build();
    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The Hostname to bind to. If not specified, will bind to all hosts")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor HTTP_CONTEXT_MAP = new PropertyDescriptor.Builder()
            .name("HTTP Context Map")
            .description("The HTTP Context Map Controller Service to use for caching the HTTP Request Information")
            .required(true)
            .identifiesControllerService(HttpContextMap.class)
            .build();
    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service to use in order to secure the server. If specified, the server will accept only HTTPS requests; "
                    + "otherwise, the server will accept only HTTP requests")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();
    public static final PropertyDescriptor HTTP_PROTOCOL_STRATEGY = new PropertyDescriptor.Builder()
            .name("HTTP Protocols")
            .description("HTTP Protocols supported for Application Layer Protocol Negotiation with TLS")
            .required(true)
            .allowableValues(HttpProtocolStrategy.class)
            .defaultValue(HttpProtocolStrategy.HTTP_1_1)
            .dependsOn(SSL_CONTEXT)
            .build();
    public static final PropertyDescriptor URL_CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Default URL Character Set")
            .description("The character set to use for decoding URL parameters if the HTTP Request does not supply one")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();
    public static final PropertyDescriptor PATH_REGEX = new PropertyDescriptor.Builder()
            .name("Allowed Paths")
            .description("A Regular Expression that specifies the valid HTTP Paths that are allowed in the incoming URL Requests. If this value is "
                    + "specified and the path of the HTTP Requests does not match this Regular Expression, the Processor will respond with a "
                    + "404: NotFound")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor ALLOW_GET = new PropertyDescriptor.Builder()
            .name("Allow GET")
            .description("Allow HTTP GET Method")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor ALLOW_POST = new PropertyDescriptor.Builder()
            .name("Allow POST")
            .description("Allow HTTP POST Method")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor ALLOW_PUT = new PropertyDescriptor.Builder()
            .name("Allow PUT")
            .description("Allow HTTP PUT Method")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor ALLOW_DELETE = new PropertyDescriptor.Builder()
            .name("Allow DELETE")
            .description("Allow HTTP DELETE Method")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor ALLOW_HEAD = new PropertyDescriptor.Builder()
            .name("Allow HEAD")
            .description("Allow HTTP HEAD Method")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor ALLOW_OPTIONS = new PropertyDescriptor.Builder()
            .name("Allow OPTIONS")
            .description("Allow HTTP OPTIONS Method")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor MAXIMUM_THREADS = new PropertyDescriptor.Builder()
            .name("Maximum Threads")
            .displayName("Maximum Threads")
            .description("The maximum number of threads that the embedded HTTP server will use for handling requests.")
            .required(true)
            .defaultValue("200")
            .addValidator(StandardValidators.createLongValidator(8, 1000, true))
            .build();
    public static final PropertyDescriptor REQUEST_HEADER_MAX_SIZE = new PropertyDescriptor.Builder()
            .name("Request Header Maximum Size")
            .description("The maximum supported size of HTTP headers in requests sent to this processor")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("8 KB")
            .build();
    public static final PropertyDescriptor ADDITIONAL_METHODS = new PropertyDescriptor.Builder()
            .name("Additional HTTP Methods")
            .description("A comma-separated list of non-standard HTTP Methods that should be allowed")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor PARAMETERS_TO_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("parameters-to-attributes")
            .displayName("Parameters to Attributes List")
            .description("A comma-separated list of HTTP parameters or form data to output as attributes")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Authentication")
            .description("Specifies whether or not the Processor should authenticate clients. This value is ignored if the <SSL Context Service> "
                    + "Property is not specified or the SSL Context provided uses only a KeyStore and not a TrustStore.")
            .required(true)
            .allowableValues(CLIENT_NONE, CLIENT_WANT, CLIENT_NEED)
            .defaultValue(CLIENT_NONE.getValue())
            .build();
    public static final PropertyDescriptor CONTAINER_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("container-queue-size").displayName("Container Queue Size")
            .description("The size of the queue for Http Request Containers").required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).defaultValue("50").build();
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
            .description("The threshold size, at which the contents of an incoming file would be written to disk. "
                    + "Only applies for requests with Content-Type: multipart/form-data. "
                    + "It is used to prevent denial of service type of attacks, to prevent filling up the heap or disk space.")
            .displayName("Multipart Read Buffer Size")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("512 KB")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PORT,
            HOSTNAME,
            SSL_CONTEXT,
            HTTP_PROTOCOL_STRATEGY,
            HTTP_CONTEXT_MAP,
            PATH_REGEX,
            URL_CHARACTER_SET,
            ALLOW_GET,
            ALLOW_POST,
            ALLOW_PUT,
            ALLOW_DELETE,
            ALLOW_HEAD,
            ALLOW_OPTIONS,
            MAXIMUM_THREADS,
            REQUEST_HEADER_MAX_SIZE,
            ADDITIONAL_METHODS,
            CLIENT_AUTH,
            CONTAINER_QUEUE_SIZE,
            MULTIPART_REQUEST_MAX_SIZE,
            MULTIPART_READ_BUFFER_SIZE,
            PARAMETERS_TO_ATTRIBUTES
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All content that is received is routed to the 'success' relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private volatile Server server;
    private volatile boolean ready;
    private volatile BlockingQueue<HttpRequestContainer> containerQueue;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean runOnPrimary = new AtomicBoolean(false);
    private final AtomicReference<Set<String>> parameterToAttributesReference = new AtomicReference<>(null);
    private final CertificateAttributesProvider certificateAttributesProvider = new HandleHttpRequestCertificateAttributesProvider();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void clearInit() {
        initialized.set(false);
    }

    synchronized void initializeServer(final ProcessContext context) throws Exception {
        if (initialized.get()) {
            return;
        }
        runOnPrimary.set(context.getExecutionNode().equals(ExecutionNode.PRIMARY));
        this.containerQueue = new LinkedBlockingQueue<>(context.getProperty(CONTAINER_QUEUE_SIZE).asInteger());
        final String host = context.getProperty(HOSTNAME).getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextProvider.class);
        final HttpContextMap httpContextMap = context.getProperty(HTTP_CONTEXT_MAP).asControllerService(HttpContextMap.class);
        final long requestTimeout = httpContextMap.getRequestTimeout(TimeUnit.MILLISECONDS);

        final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
        final Server server = createServer(context);

        final int requestHeaderSize = context.getProperty(REQUEST_HEADER_MAX_SIZE).asDataSize(DataUnit.B).intValue();
        final StandardServerConnectorFactory serverConnectorFactory = new StandardServerConnectorFactory(server, port);
        serverConnectorFactory.setRequestHeaderSize(requestHeaderSize);

        final boolean needClientAuth = CLIENT_NEED.getValue().equals(clientAuthValue);
        serverConnectorFactory.setNeedClientAuth(needClientAuth);
        final boolean wantClientAuth = CLIENT_WANT.getValue().equals(clientAuthValue);
        serverConnectorFactory.setWantClientAuth(wantClientAuth);
        final SSLContext sslContext = sslContextProvider == null ? null : sslContextProvider.createContext();
        serverConnectorFactory.setSslContext(sslContext);
        final HttpProtocolStrategy httpProtocolStrategy = context.getProperty(HTTP_PROTOCOL_STRATEGY).asAllowableValue(HttpProtocolStrategy.class);
        serverConnectorFactory.setApplicationLayerProtocols(httpProtocolStrategy.getApplicationLayerProtocols());

        final ServerConnector serverConnector = serverConnectorFactory.getServerConnector();
        serverConnector.setIdleTimeout(Math.max(serverConnector.getIdleTimeout(), requestTimeout));
        if (StringUtils.isNotBlank(host)) {
            serverConnector.setHost(host);
        }
        server.addConnector(serverConnector);

        final Set<String> allowedMethods = new HashSet<>();
        if (context.getProperty(ALLOW_GET).asBoolean()) {
            allowedMethods.add("GET");
        }
        if (context.getProperty(ALLOW_POST).asBoolean()) {
            allowedMethods.add("POST");
        }
        if (context.getProperty(ALLOW_PUT).asBoolean()) {
            allowedMethods.add("PUT");
        }
        if (context.getProperty(ALLOW_DELETE).asBoolean()) {
            allowedMethods.add("DELETE");
        }
        if (context.getProperty(ALLOW_HEAD).asBoolean()) {
            allowedMethods.add("HEAD");
        }
        if (context.getProperty(ALLOW_OPTIONS).asBoolean()) {
            allowedMethods.add("OPTIONS");
        }

        final String additionalMethods = context.getProperty(ADDITIONAL_METHODS).getValue();
        if (additionalMethods != null) {
            for (final String additionalMethod : additionalMethods.split(",")) {
                final String trimmed = additionalMethod.trim();
                if (!trimmed.isEmpty()) {
                    allowedMethods.add(trimmed.toUpperCase());
                }
            }
        }

        final Set<String> parametersToMakeAttributes = new HashSet<>();
        final String parametersToAttributesPropertyValue = context.getProperty(PARAMETERS_TO_ATTRIBUTES).getValue();
        if (parametersToAttributesPropertyValue != null) {
            for (final String paremeterName : parametersToAttributesPropertyValue.split(",")) {
                final String trimmed = paremeterName.trim();
                if (!trimmed.isEmpty()) {
                    parametersToMakeAttributes.add(trimmed);
                }
            }
            parameterToAttributesReference.set(parametersToMakeAttributes);
        }

        final String pathRegex = context.getProperty(PATH_REGEX).getValue();
        final Pattern pathPattern = (pathRegex == null) ? null : Pattern.compile(pathRegex);


        final HttpServlet standardServlet = new HttpServlet() {
            @Override
            protected void service(final HttpServletRequest request, final HttpServletResponse response) {
                final String requestUri = request.getRequestURI();
                final String method = request.getMethod().toUpperCase();
                if (!allowedMethods.contains(method)) {
                    sendError(SC_METHOD_NOT_ALLOWED, "Method Not Allowed", request, response);
                    return;
                }

                if (pathPattern != null) {
                    final URI uri = URI.create(requestUri);
                    if (!pathPattern.matcher(uri.getPath()).matches()) {
                        sendError(SC_NOT_FOUND, "Path Not Found", request, response);
                        return;
                    }
                }

                if (context.getAvailableRelationships().isEmpty()) {
                    sendError(SC_SERVICE_UNAVAILABLE, "No Available Relationships", request, response);
                    return;
                } else if (!ready) {
                    sendError(SC_SERVICE_UNAVAILABLE, "Server Not Ready", request, response);
                    return;
                }

                final AsyncContext async = request.startAsync();
                // disable timeout handling on AsyncContext, timeout will be handled in HttpContextMap
                async.setTimeout(0);

                final HttpRequestContainer container = new HttpRequestContainer(request, response, async);
                final boolean added = containerQueue.offer(container);
                if (added) {
                    getLogger().debug("Request Queued: Method [{}] URI [{}] Address [{}]", method, requestUri, request.getRemoteAddr());
                } else {
                    sendError(SC_SERVICE_UNAVAILABLE, "Request Queue Full", container);
                }
            }
        };
        final ServletContextHandler handler = new ServletContextHandler();
        handler.addServlet(standardServlet, "/");
        server.setHandler(handler);

        this.server = server;
        server.start();

        for (final Connector connector : server.getConnectors()) {
            getLogger().info("Started Connector {}", connector);
        }

        initialized.set(true);
        ready = true;
    }

    protected int getPort() {
        for (final Connector connector : server.getConnectors()) {
            if (connector instanceof ServerConnector) {
                return ((ServerConnector) connector).getLocalPort();
            }
        }

        throw new IllegalStateException("Server is not listening on any ports");
    }

    protected int getRequestQueueSize() {
        return containerQueue.size();
    }

    @OnUnscheduled
    public void shutdown() throws Exception {
        ready = false;

        if (server == null) {
            getLogger().debug("Server not configured");
        } else {
            if (server.isStopped()) {
                getLogger().debug("Server Stopped {}", server);
            } else {
                for (final Connector connector : server.getConnectors()) {
                    getLogger().debug("Stopping Connector {}", connector);
                }

                drainContainerQueue();
                server.stop();
                server.destroy();
                server.join();
                clearInit();

                for (final Connector connector : server.getConnectors()) {
                    getLogger().info("Stopped Connector {}", connector);
                }
            }
        }
    }

    void drainContainerQueue() {
        if (containerQueue.isEmpty()) {
            getLogger().debug("No Pending Requests Queued");
        } else {
            final List<HttpRequestContainer> pendingContainers = new ArrayList<>();
            containerQueue.drainTo(pendingContainers);
            getLogger().warn("Pending Requests Queued [{}]", pendingContainers.size());
            for (final HttpRequestContainer container : pendingContainers) {
                sendError(SC_SERVICE_UNAVAILABLE, "Stopping Server", container);
            }
        }
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange(final PrimaryNodeState state) {
        if (runOnPrimary.get() && state.equals(PrimaryNodeState.PRIMARY_NODE_REVOKED)) {
            getLogger().info("Server Shutdown Started: Primary Node State Changed [{}]", state);
            try {
                shutdown();
            } catch (final Exception e) {
                getLogger().warn("Server Shutdown Failed: Primary Node State Changed [{}]", state, e);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        try {
            if (!initialized.get()) {
                initializeServer(context);
            }
        } catch (Exception e) {
            context.yield();

            try {
                // shutdown to release any resources allocated during the failed initialization
                shutdown();
            } catch (final Exception shutdownException) {
                getLogger().debug("Server Shutdown Failed after Initialization Failed", shutdownException);
            }

            throw new ProcessException("Failed to initialize the server", e);
        }

        HttpRequestContainer container;
        try {
            container = containerQueue.poll(2, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e1) {
            Thread.currentThread().interrupt();
            return;
        }

        if (container == null) {
            return;
        }

        final long start = System.nanoTime();
        final HttpServletRequest request = container.getRequest();

        if (StringUtils.contains(request.getContentType(), MIME_TYPE__MULTIPART_FORM_DATA)) {
          final long requestMaxSize = context.getProperty(MULTIPART_REQUEST_MAX_SIZE).asDataSize(DataUnit.B).longValue();
          final int readBufferSize = context.getProperty(MULTIPART_READ_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
          String tempDir = System.getProperty("java.io.tmpdir");
          request.setAttribute(ServletContextRequest.MULTIPART_CONFIG_ELEMENT, new MultipartConfigElement(tempDir, requestMaxSize, requestMaxSize, readBufferSize));
          List<Part> parts = null;
          try {
            parts = List.copyOf(request.getParts());
            int allPartsCount = parts.size();
            final String contextIdentifier = UUID.randomUUID().toString();
            for (int i = 0; i < allPartsCount; i++) {
              Part part = parts.get(i);
              FlowFile flowFile = session.create();
              try (OutputStream flowFileOut = session.write(flowFile)) {
                StreamUtils.copy(part.getInputStream(), flowFileOut);
              } catch (IOException e) {
                handleFlowContentStreamingError(session, container, Optional.of(flowFile), e);
                return;
              }
              flowFile = savePartAttributes(session, part, flowFile, i, allPartsCount);
              flowFile = saveRequestAttributes(context, session, request, flowFile, contextIdentifier);
              if (i == 0) {
                // each one of multipart comes from a single request, thus registering only once per loop.
                boolean requestRegistrationSuccess = registerRequest(context, session, container, flowFile);
                if (!requestRegistrationSuccess)
                  break;
              }
              forwardFlowFile(session, start, request, flowFile);
            }
          } catch (IOException | ServletException | IllegalStateException e) {
            handleFlowContentStreamingError(session, container, Optional.empty(), e);
          } finally {
            if (parts != null) {
              for (Part part : parts) {
                try {
                  part.delete();
                } catch (Exception e) {
                  getLogger().error("Couldn't delete underlying storage for {}", part, e);
                }
              }
            }
          }
        } else {
          FlowFile flowFile = session.create();
          try (OutputStream flowFileOut = session.write(flowFile)) {
            StreamUtils.copy(request.getInputStream(), flowFileOut);
          } catch (final IOException e) {
            handleFlowContentStreamingError(session, container, Optional.of(flowFile), e);
            return;
          }
          final String contextIdentifier = UUID.randomUUID().toString();
          flowFile = saveRequestAttributes(context, session, request, flowFile, contextIdentifier);
          boolean requestRegistrationSuccess = registerRequest(context, session, container, flowFile);
          if (requestRegistrationSuccess)
            forwardFlowFile(session, start, request, flowFile);
        }
    }

    private FlowFile savePartAttributes(ProcessSession session, Part part, FlowFile flowFile, final int i, final int allPartsCount) {
      final Map<String, String> attributes = new HashMap<>();
      for (String headerName : part.getHeaderNames()) {
        final String headerValue = part.getHeader(headerName);
        putAttribute(attributes, "http.headers.multipart." + headerName, headerValue);
      }
      putAttribute(attributes, "http.multipart.size", part.getSize());
      putAttribute(attributes, "http.multipart.content.type", part.getContentType());
      putAttribute(attributes, "http.multipart.name", part.getName());
      putAttribute(attributes, "http.multipart.filename", part.getSubmittedFileName());
      putAttribute(attributes, "http.multipart.fragments.sequence.number", i + 1);
      putAttribute(attributes, "http.multipart.fragments.total.number", allPartsCount);
      return session.putAllAttributes(flowFile, attributes);
    }

    private FlowFile saveRequestAttributes(final ProcessContext context, final ProcessSession session, HttpServletRequest request, FlowFile flowFile, String contextIdentifier) {
      final String charset = request.getCharacterEncoding() == null ? context.getProperty(URL_CHARACTER_SET).getValue() : request.getCharacterEncoding();

      final Map<String, String> attributes = new HashMap<>();
      try {
          putAttribute(attributes, HTTPUtils.HTTP_CONTEXT_ID, contextIdentifier);
          putAttribute(attributes, "mime.type", request.getContentType());
          putAttribute(attributes, "http.servlet.path", request.getServletPath());
          putAttribute(attributes, "http.context.path", request.getContextPath());
          putAttribute(attributes, "http.method", request.getMethod());
          putAttribute(attributes, "http.local.addr", request.getLocalAddr());
          putAttribute(attributes, HTTPUtils.HTTP_LOCAL_NAME, request.getLocalName());
          final String queryString = request.getQueryString();
          if (queryString != null) {
              putAttribute(attributes, "http.query.string", URLDecoder.decode(queryString, charset));
          }
          putAttribute(attributes, HTTPUtils.HTTP_REMOTE_HOST, request.getRemoteHost());
          putAttribute(attributes, "http.remote.addr", request.getRemoteAddr());
          putAttribute(attributes, "http.remote.user", request.getRemoteUser());
          putAttribute(attributes, "http.protocol", request.getProtocol());
          putAttribute(attributes, HTTPUtils.HTTP_REQUEST_URI, request.getRequestURI());
          putAttribute(attributes, "http.request.url", request.getRequestURL().toString());
          putAttribute(attributes, "http.auth.type", request.getAuthType());

          putAttribute(attributes, "http.requested.session.id", request.getRequestedSessionId());
          final DispatcherType dispatcherType = request.getDispatcherType();
          if (dispatcherType != null) {
              putAttribute(attributes, "http.dispatcher.type", dispatcherType.name());
          }
          putAttribute(attributes, "http.character.encoding", request.getCharacterEncoding());
          putAttribute(attributes, "http.locale", request.getLocale());
          putAttribute(attributes, "http.server.name", request.getServerName());
          putAttribute(attributes, HTTPUtils.HTTP_PORT, request.getServerPort());

          Set<String> parametersToAttributes = parameterToAttributesReference.get();
          if (parametersToAttributes != null && !parametersToAttributes.isEmpty()) {
              final Enumeration<String> paramEnumeration = request.getParameterNames();
              while (paramEnumeration.hasMoreElements()) {
                  final String paramName = paramEnumeration.nextElement();
                  if (parametersToAttributes.contains(paramName)) {
                    attributes.put("http.param." + paramName, request.getParameter(paramName));
                }
              }
          }

          final Cookie[] cookies = request.getCookies();
          if (cookies != null) {
              for (final Cookie cookie : cookies) {
                  final String name = cookie.getName();
                  final String cookiePrefix = "http.cookie." + name + ".";
                  attributes.put(cookiePrefix + "value", cookie.getValue());
                  attributes.put(cookiePrefix + "domain", cookie.getDomain());
                  attributes.put(cookiePrefix + "path", cookie.getPath());
                  attributes.put(cookiePrefix + "max.age", String.valueOf(cookie.getMaxAge()));
                  attributes.put(cookiePrefix + "secure", String.valueOf(cookie.getSecure()));
              }
          }

          if (queryString != null) {
              final String[] params = URL_QUERY_PARAM_DELIMITER.split(queryString);
              for (final String keyValueString : params) {
                  final int indexOf = keyValueString.indexOf("=");
                  if (indexOf < 0) {
                      // no =, then it's just a key with no value
                      attributes.put("http.query.param." + URLDecoder.decode(keyValueString, charset), "");
                  } else {
                      final String key = keyValueString.substring(0, indexOf);
                      final String value;

                      if (indexOf == keyValueString.length() - 1) {
                          value = "";
                      } else {
                          value = keyValueString.substring(indexOf + 1);
                      }

                      attributes.put("http.query.param." + URLDecoder.decode(key, charset), URLDecoder.decode(value, charset));
                  }
              }
          }
      } catch (final UnsupportedEncodingException uee) {
          throw new ProcessException("Invalid character encoding", uee);  // won't happen because charset has been validated
      }

      final Enumeration<String> headerNames = request.getHeaderNames();
      while (headerNames.hasMoreElements()) {
          final String headerName = headerNames.nextElement();
          final String headerValue = request.getHeader(headerName);
          putAttribute(attributes, "http.headers." + headerName, headerValue);
      }

      final Principal principal = request.getUserPrincipal();
      if (principal != null) {
          putAttribute(attributes, "http.principal.name", principal.getName());
      }

      final Map<String, String> certificateAttributes = certificateAttributesProvider.getCertificateAttributes(request);
      attributes.putAll(certificateAttributes);

      return session.putAllAttributes(flowFile, attributes);
    }

    private void forwardFlowFile(final ProcessSession session, final long start, final HttpServletRequest request, final FlowFile flowFile) {
      final long receiveMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      final String subjectDn = flowFile.getAttribute(CertificateAttribute.HTTP_SUBJECT_DN.getName());
      session.getProvenanceReporter().receive(flowFile, HTTPUtils.getURI(flowFile.getAttributes()),
          "Received from " + request.getRemoteAddr() + (subjectDn == null ? "" : " with DN=" + subjectDn), receiveMillis);
      session.transfer(flowFile, REL_SUCCESS);
      getLogger().debug("Transferred {} to [{}] Remote Address [{}] ", flowFile, REL_SUCCESS, request.getRemoteAddr());
    }


    private boolean registerRequest(final ProcessContext context, final ProcessSession session,
                                    final HttpRequestContainer container, final FlowFile flowFile) {
        final HttpContextMap contextMap = context.getProperty(HTTP_CONTEXT_MAP).asControllerService(HttpContextMap.class);
        final String contextIdentifier = flowFile.getAttribute(HTTPUtils.HTTP_CONTEXT_ID);
        final HttpServletRequest request = container.getRequest();
        final boolean registered = contextMap.register(contextIdentifier, request, container.getResponse(), container.getContext());
        if (registered) {
            return true;
        }

        session.remove(flowFile);
        sendError(SC_SERVICE_UNAVAILABLE, "Request Registration Failed", container);
        return false;
    }

    protected void handleFlowContentStreamingError(final ProcessSession session, final HttpRequestContainer container, final Optional<FlowFile> flowFile, final Exception e) {
        // There may be many reasons which can produce an IOException on the HTTP stream and in some of them, eg.
        // bad requests, the connection to the client is not closed. In order to address also these cases, we try
        // and answer with a BAD_REQUEST, which lets the client know that the request has not been correctly
        // processed and makes it aware that the connection can be closed.
        final HttpServletRequest request = container.getRequest();
        getLogger().error("Stream Processing Failed: Method [{}] URI [{}] Address [{}]", request.getMethod(), request.getRequestURI(), request.getRemoteAddr(), e);
        flowFile.ifPresent(session::remove);
        sendError(SC_BAD_REQUEST, "Stream Processing Failed", container);
    }

    private void putAttribute(final Map<String, String> map, final String key, final Object value) {
        if (value == null) {
            return;
        }

        putAttribute(map, key, value.toString());
    }

    private void putAttribute(final Map<String, String> map, final String key, final String value) {
        if (value == null) {
            return;
        }

        map.put(key, value);
    }

    private void sendError(final int statusCode, final String message, final HttpRequestContainer container) {
        sendError(statusCode, message, container.getRequest(), container.getResponse());
        final AsyncContext asyncContext = container.getContext();
        try {
            asyncContext.complete();
        } catch (final RuntimeException e) {
            final HttpServletRequest request = container.getRequest();
            final String method = request.getMethod();
            final String uri = request.getRequestURI();
            final String remoteAddr = request.getRemoteAddr();
            getLogger().error("Complete Request Failed: Method [{}] URI [{}] Address [{}]", method, uri, remoteAddr, e);
        }
    }

    private void sendError(final int statusCode, final String message, final HttpServletRequest request, final HttpServletResponse response) {
        final String method = request.getMethod();
        final String uri = request.getRequestURI();
        final String remoteAddr = request.getRemoteAddr();

        try {
            response.sendError(statusCode, message);
            getLogger().warn("Send Error Completed: HTTP {} [{}] Method [{}] URI [{}] Address [{}]", statusCode, message, method, uri, remoteAddr);
        } catch (final Exception e) {
            getLogger().error("Send Error Failed: HTTP {} [{}] Method [{}] URI [{}] Address [{}]", statusCode, message, method, uri, remoteAddr, e);
        }
    }

    private Server createServer(final ProcessContext context) {
        final int maximumThreads = context.getProperty(MAXIMUM_THREADS).asInteger();
        final QueuedThreadPool queuedThreadPool = new QueuedThreadPool(maximumThreads);
        queuedThreadPool.setName(String.format("%s[id=%s] Server", getClass().getSimpleName(), getIdentifier()));
        return new Server(queuedThreadPool);
    }

    private static class HttpRequestContainer {
        private final HttpServletRequest request;
        private final HttpServletResponse response;
        private final AsyncContext context;

        public HttpRequestContainer(final HttpServletRequest request, final HttpServletResponse response, final AsyncContext async) {
            this.request = request;
            this.response = response;
            this.context = async;
        }

        public HttpServletRequest getRequest() {
            return request;
        }

        public HttpServletResponse getResponse() {
            return response;
        }

        public AsyncContext getContext() {
            return context;
        }
    }
}
