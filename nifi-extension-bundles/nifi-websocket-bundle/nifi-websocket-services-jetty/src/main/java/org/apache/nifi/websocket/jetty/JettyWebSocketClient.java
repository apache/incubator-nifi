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
package org.apache.nifi.websocket.jetty;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.websocket.WebSocketClientService;
import org.apache.nifi.websocket.WebSocketConfigurationException;
import org.apache.nifi.websocket.WebSocketMessageRouter;
import org.apache.nifi.websocket.jetty.dto.SessionInfo;
import org.apache.nifi.websocket.jetty.util.HeaderMapExtractor;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.transport.HttpClientTransportDynamic;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

@Tags({"WebSocket", "Jetty", "client"})
@CapabilityDescription("Implementation of WebSocketClientService." +
        " This service uses Jetty WebSocket client module to provide" +
        " WebSocket session management throughout the application.")
public class JettyWebSocketClient extends AbstractJettyWebSocketService implements WebSocketClientService {

    public static final PropertyDescriptor WS_URI = new PropertyDescriptor.Builder()
            .name("websocket-uri")
            .displayName("WebSocket URI")
            .description("The WebSocket URI this client connects to.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .addValidator((subject, input, context) -> {
                final ValidationResult.Builder result = new ValidationResult.Builder()
                        .valid(input.startsWith("/"))
                        .subject(subject);

                if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                    result.explanation("Expression Language Present").valid(true);
                } else {
                    result.explanation("Protocol should be either 'ws' or 'wss'.")
                            .valid(input.startsWith("ws://") || input.startsWith("wss://"));
                }

                return result.build();
            })
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connection-timeout")
            .displayName("Connection Timeout")
            .description("The timeout to connect the WebSocket URI.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("3 sec")
            .build();

    public static final PropertyDescriptor CONNECTION_ATTEMPT_COUNT = new PropertyDescriptor.Builder()
            .name("connection-attempt-timeout")
            .displayName("Connection Attempt Count")
            .description("The number of times to try and establish a connection.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("3")
            .build();

    public static final PropertyDescriptor SESSION_MAINTENANCE_INTERVAL = new PropertyDescriptor.Builder()
            .name("session-maintenance-interval")
            .displayName("Session Maintenance Interval")
            .description("The interval between session maintenance activities." +
                    " A WebSocket session established with a WebSocket server can be terminated due to different reasons" +
                    " including restarting the WebSocket server or timing out inactive sessions." +
                    " This session maintenance activity is periodically executed in order to reconnect those lost sessions," +
                    " so that a WebSocket client can reuse the same session id transparently after it reconnects successfully. " +
                    " The maintenance activity is executed until corresponding processors or this controller service is stopped.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 sec")
            .build();

    public static final PropertyDescriptor USER_NAME = new PropertyDescriptor.Builder()
            .name("user-name")
            .displayName("User Name")
            .description("The user name for Basic Authentication.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor USER_PASSWORD = new PropertyDescriptor.Builder()
            .name("user-password")
            .displayName("User Password")
            .description("The user password for Basic Authentication.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor AUTH_CHARSET = new PropertyDescriptor.Builder()
            .name("authentication-charset")
            .displayName("Authentication Header Charset")
            .description("The charset for Basic Authentication header base64 string.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .defaultValue("US-ASCII")
            .build();

    public static final PropertyDescriptor CUSTOM_AUTH = new PropertyDescriptor.Builder()
            .name("custom-authorization")
            .displayName("Custom Authorization")
            .description(
                    "Configures a custom HTTP Authorization Header as described in RFC 7235 Section 4.2." +
                            " Setting a custom Authorization Header excludes configuring the User Name and User Password properties for Basic Authentication.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();


    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
            .name("proxy-host")
            .displayName("HTTP Proxy Host")
            .description("The host name of the HTTP Proxy.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor.Builder()
            .name("proxy-port")
            .displayName("HTTP Proxy Port")
            .description("The port number of the HTTP Proxy.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    private static final int INITIAL_BACKOFF_MILLIS = 100;
    private static final int MAXIMUM_BACKOFF_MILLIS = 3200;

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getAbstractPropertyDescriptors().stream(),
            Stream.of(
                    WS_URI,
                    SSL_CONTEXT,
                    CONNECTION_TIMEOUT,
                    CONNECTION_ATTEMPT_COUNT,
                    SESSION_MAINTENANCE_INTERVAL,
                    USER_NAME,
                    USER_PASSWORD,
                    AUTH_CHARSET,
                    CUSTOM_AUTH,
                    PROXY_HOST,
                    PROXY_PORT
            )
    ).toList();

    private final Map<String, SessionInfo> activeSessions = new ConcurrentHashMap<>();
    private final ReentrantLock connectionLock = new ReentrantLock();
    private WebSocketClient client;
    private URI webSocketUri;
    private long connectionTimeoutMillis;
    private int connectCount;
    private volatile ScheduledExecutorService sessionMaintenanceScheduler;
    private ConfigurationContext configurationContext;
    protected String authorizationHeader;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    @Override
    public void startClient(final ConfigurationContext context) throws Exception {
        configurationContext = context;

        connectCount = configurationContext.getProperty(CONNECTION_ATTEMPT_COUNT).evaluateAttributeExpressions().asInteger();

        final HttpClient httpClient;
        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextProvider.class);
        if (sslContextProvider == null) {
            httpClient = new HttpClient();
        } else {
            final SslContextFactory.Client sslContextFactory = new SslContextFactory.Client();
            final SSLContext sslContext = sslContextProvider.createContext();
            sslContextFactory.setSslContext(sslContext);
            final ClientConnector clientConnector = new ClientConnector();
            clientConnector.setSslContextFactory(sslContextFactory);
            httpClient = new HttpClient(new HttpClientTransportDynamic(clientConnector));
        }

        final String proxyHost = context.getProperty(PROXY_HOST).evaluateAttributeExpressions().getValue();
        final Integer proxyPort = context.getProperty(PROXY_PORT).evaluateAttributeExpressions().asInteger();

        if (proxyHost != null && proxyPort != null) {
            HttpProxy httpProxy = new HttpProxy(proxyHost, proxyPort);
            httpClient.getProxyConfiguration().addProxy(httpProxy);
        }

        client = new WebSocketClient(httpClient);

        configurePolicy(context, client);
        final String userName = context.getProperty(USER_NAME).evaluateAttributeExpressions().getValue();
        final String userPassword = context.getProperty(USER_PASSWORD).evaluateAttributeExpressions().getValue();
        final String customAuth = context.getProperty(CUSTOM_AUTH).evaluateAttributeExpressions().getValue();

        if (!StringUtils.isEmpty(customAuth)) {
            authorizationHeader = customAuth;
        } else if (!StringUtils.isEmpty(userName) && !StringUtils.isEmpty(userPassword)) {
            final String charsetName = context.getProperty(AUTH_CHARSET).evaluateAttributeExpressions().getValue();
            if (StringUtils.isEmpty(charsetName)) {
                throw new IllegalArgumentException(AUTH_CHARSET.getDisplayName() + " was not specified.");
            }
            final Charset charset = Charset.forName(charsetName);
            final String base64String = Base64.getEncoder().encodeToString((userName + ":" + userPassword).getBytes(charset));
            authorizationHeader = "Basic " + base64String;
        } else {
            authorizationHeader = null;
        }

        client.start();
        activeSessions.clear();
        webSocketUri = new URI(context.getProperty(WS_URI).evaluateAttributeExpressions(new HashMap<>()).getValue());
        connectionTimeoutMillis = context.getProperty(CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        final Long sessionMaintenanceInterval = context.getProperty(SESSION_MAINTENANCE_INTERVAL).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        sessionMaintenanceScheduler = Executors.newSingleThreadScheduledExecutor();
        sessionMaintenanceScheduler.scheduleAtFixedRate(() -> {
            try {
                maintainSessions();
            } catch (final Exception e) {
                getLogger().warn("Failed to maintain sessions due to {}", e, e);
            }
        }, sessionMaintenanceInterval, sessionMaintenanceInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(1);
        final boolean proxyHostSet = validationContext.getProperty(PROXY_HOST).isSet();
        final boolean proxyPortSet = validationContext.getProperty(PROXY_PORT).isSet();

        if ((proxyHostSet && !proxyPortSet) || (!proxyHostSet && proxyPortSet)) {
            results.add(new ValidationResult.Builder().subject("HTTP Proxy Host and Port").valid(false).explanation(
                    "If HTTP Proxy Host or HTTP Proxy Port is set, both must be set").build());
        }

        final boolean isBaseAuthUsed = validationContext.getProperty(USER_NAME).isSet() || validationContext.getProperty(USER_PASSWORD).isSet();

        if (isBaseAuthUsed && validationContext.getProperty(CUSTOM_AUTH).isSet()) {
            results.add((new ValidationResult.Builder().subject("Authentication").valid(false).explanation(
                    "Properties related to Basic Authentication (\"User Name\" and \"User Password\") cannot be used together with \"Custom Authorization\"")).build());
        }

        return results;
    }

    @OnDisabled
    @OnShutdown
    @Override
    public void stopClient() throws Exception {
        activeSessions.clear();

        if (sessionMaintenanceScheduler != null) {
            try {
                sessionMaintenanceScheduler.shutdown();
            } catch (Exception e) {
                getLogger().warn("Failed to shutdown session maintainer due to {}", e, e);
            }
            sessionMaintenanceScheduler = null;
        }

        if (client == null) {
            return;
        }

        client.stop();
        client = null;
    }

    @Override
    public void connect(final String clientId) throws IOException {
        connect(clientId, null, Collections.emptyMap());
    }

    @Override
    public void connect(final String clientId, final Map<String, String> flowFileAttributes) throws IOException {
        connect(clientId, null, flowFileAttributes);
    }

    private void connect(final String clientId, final String sessionId, final Map<String, String> flowFileAttributes) throws IOException {
        try {
            webSocketUri = new URI(configurationContext.getProperty(WS_URI).evaluateAttributeExpressions(flowFileAttributes).getValue());
        } catch (URISyntaxException e) {
            throw new ProcessException("Could not create websocket URI", e);
        }

        connectionLock.lock();

        try {
            final WebSocketMessageRouter router;
            try {
                router = routers.getRouterOrFail(clientId);
            } catch (WebSocketConfigurationException e) {
                throw new IllegalStateException("Failed to get router due to: " + e, e);
            }
            final RoutingWebSocketListener listener = new RoutingWebSocketListener(router);
            listener.setSessionId(sessionId);

            final ClientUpgradeRequest request = new ClientUpgradeRequest();

            if (!flowFileAttributes.isEmpty()) {
                request.setHeaders(HeaderMapExtractor.getHeaderMap(flowFileAttributes));
            }
            if (!StringUtils.isEmpty(authorizationHeader)) {
                request.setHeader(HttpHeader.AUTHORIZATION.asString(), authorizationHeader);
            }

            final Session session = attemptConnection(listener, request, connectCount);

            getLogger().info("Connected, session={}", session);
            activeSessions.put(clientId, new SessionInfo(listener.getSessionId(), flowFileAttributes));

        } finally {
            connectionLock.unlock();
        }
    }

    private Session attemptConnection(RoutingWebSocketListener listener, ClientUpgradeRequest request, int connectCount) throws IOException {
        int backoffMillis = INITIAL_BACKOFF_MILLIS;
        int backoffJitterMillis;
        for (int i = 0; i < connectCount; i++) {
            backoffJitterMillis = (int) (INITIAL_BACKOFF_MILLIS * getBackoffJitter(-0.2, 0.2));
            final Future<Session> connect = createWebsocketSession(listener, request);
            getLogger().info("Connecting to : {}", webSocketUri);
            try {
                return connect.get(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                getLogger().warn("Connection attempt to {} timed out", webSocketUri);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                final String errorMessage = String.format("Thread was interrupted while attempting to connect to %s", webSocketUri);
                throw new ProcessException(errorMessage, e);
            } catch (Exception e) {
                getLogger().warn("Failed to connect to {}, reconnection attempt {}", webSocketUri, i + 1, e);
            }

            if (i < connectCount - 1) {
                final int sleepTime = backoffMillis + backoffJitterMillis;
                try {
                    getLogger().info("Sleeping {} ms before new connection attempt.", sleepTime);
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    final String errorMessage = String.format("Thread was interrupted while reconnecting to %s with %s backoffMillis", webSocketUri, sleepTime);
                    throw new ProcessException(errorMessage, e);
                }
                backoffMillis = Math.min(backoffMillis * 2, MAXIMUM_BACKOFF_MILLIS);
            }
        }
        throw new IOException("Failed to connect " + webSocketUri + " after " + connectCount + " attempts");
    }

    Future<Session> createWebsocketSession(RoutingWebSocketListener listener, ClientUpgradeRequest request) throws IOException {
        return client.connect(listener, webSocketUri, request);
    }

    void maintainSessions() throws Exception {
        if (client == null) {
            return;
        }

        connectionLock.lock();

        final ComponentLog logger = getLogger();
        try {
            // Loop through existing sessions and reconnect.
            for (String clientId : activeSessions.keySet()) {
                final WebSocketMessageRouter router;
                try {
                    router = routers.getRouterOrFail(clientId);
                } catch (final WebSocketConfigurationException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("The clientId {} is no longer active. Discarding the clientId.", clientId);
                    }
                    activeSessions.remove(clientId);
                    continue;
                }

                final SessionInfo sessionInfo = activeSessions.get(clientId);
                // If this session is still alive, do nothing.
                if (!router.containsSession(sessionInfo.getSessionId())) {
                    // This session is no longer active, reconnect it.
                    // If it fails, the sessionId will remain in activeSessions, and retries later.
                    // This reconnect attempt is continued until user explicitly stops a processor or this controller service.
                    connect(clientId, sessionInfo.getSessionId(), sessionInfo.getFlowFileAttributes());
                }
            }
        } finally {
            connectionLock.unlock();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Session maintenance completed. activeSessions={}", activeSessions);
        }
    }

    @Override
    public String getTargetUri() {
        return webSocketUri.toString();
    }

    private void configurePolicy(final ConfigurationContext context, final WebSocketClient policy) {
        final int inputBufferSize = context.getProperty(INPUT_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxTextMessageSize = context.getProperty(MAX_TEXT_MESSAGE_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxBinaryMessageSize = context.getProperty(MAX_BINARY_MESSAGE_SIZE).asDataSize(DataUnit.B).intValue();
        policy.setInputBufferSize(inputBufferSize);
        policy.setMaxTextMessageSize(maxTextMessageSize);
        policy.setMaxBinaryMessageSize(maxBinaryMessageSize);
    }

    public double getBackoffJitter(final double min, final double max) {
        return Math.random() * (max - min) + min;
    }
}
