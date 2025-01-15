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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.jetty.configuration.connector.StandardServerConnectorFactory;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.websocket.WebSocketConfigurationException;
import org.apache.nifi.websocket.WebSocketMessageRouter;
import org.apache.nifi.websocket.WebSocketServerService;
import org.eclipse.jetty.ee10.servlet.security.ConstraintMapping;
import org.eclipse.jetty.ee10.servlet.security.ConstraintSecurityHandler;
import org.eclipse.jetty.ee10.websocket.server.JettyServerUpgradeRequest;
import org.eclipse.jetty.ee10.websocket.server.JettyServerUpgradeResponse;
import org.eclipse.jetty.ee10.websocket.server.JettyWebSocketCreator;
import org.eclipse.jetty.ee10.websocket.server.JettyWebSocketServlet;
import org.eclipse.jetty.ee10.websocket.server.JettyWebSocketServletFactory;
import org.eclipse.jetty.ee10.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.eclipse.jetty.security.DefaultAuthenticatorFactory;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.security.Constraint;
import org.eclipse.jetty.util.resource.PathResourceFactory;
import org.eclipse.jetty.util.resource.Resource;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

@Tags({"WebSocket", "Jetty", "server"})
@CapabilityDescription("Implementation of WebSocketServerService." +
        " This service uses Jetty WebSocket server module to provide" +
        " WebSocket session management throughout the application.")
public class JettyWebSocketServer extends AbstractJettyWebSocketService implements WebSocketServerService {

    /**
     * A global map to refer a controller service instance by requested port number.
     */
    private static final Map<Integer, JettyWebSocketServer> portToControllerService = new ConcurrentHashMap<>();

    // Allowable values for client auth
    public static final AllowableValue CLIENT_NONE = new AllowableValue("no", "No Authentication",
            "Processor will not authenticate clients. Anyone can communicate with this Processor anonymously");
    public static final AllowableValue CLIENT_WANT = new AllowableValue("want", "Want Authentication",
            "Processor will try to verify the client but if unable to verify will allow the client to communicate anonymously");
    public static final AllowableValue CLIENT_NEED = new AllowableValue("need", "Need Authentication",
            "Processor will reject communications from any client unless the client provides a certificate that is trusted by the TrustStore "
                    + "specified in the SSL Context Service");

    public static final AllowableValue LOGIN_SERVICE_HASH = new AllowableValue("hash", "HashLoginService",
            "See http://www.eclipse.org/jetty/javadoc/current/org/eclipse/jetty/security/HashLoginService.html for detail.");

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("client-authentication")
            .displayName("SSL Client Authentication")
            .description("Specifies whether or not the Processor should authenticate client by its certificate. "
                    + "This value is ignored if the <SSL Context Service> "
                    + "Property is not specified or the SSL Context provided uses only a KeyStore and not a TrustStore.")
            .required(true)
            .allowableValues(CLIENT_NONE, CLIENT_WANT, CLIENT_NEED)
            .defaultValue(CLIENT_NONE.getValue())
            .build();

    public static final PropertyDescriptor LISTEN_PORT = new PropertyDescriptor.Builder()
            .name("listen-port")
            .displayName("Listen Port")
            .description("The port number on which this WebSocketServer listens to.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor BASIC_AUTH = new PropertyDescriptor.Builder()
            .name("basic-auth")
            .displayName("Enable Basic Authentication")
            .description("If enabled, client connection requests are authenticated with "
                    + "Basic authentication using the specified Login Provider.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor AUTH_PATH_SPEC = new PropertyDescriptor.Builder()
            .name("auth-path-spec")
            .displayName("Basic Authentication Path Spec")
            .description("Specify a Path Spec to apply Basic Authentication.")
            .required(false)
            .defaultValue("/*")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AUTH_ROLES = new PropertyDescriptor.Builder()
            .name("auth-roles")
            .displayName("Basic Authentication Roles")
            .description("The authenticated user must have one of specified role. "
                    + "Multiple roles can be set as comma separated string. "
                    + "'*' represents any role and so does '**' any role including no role.")
            .required(false)
            .defaultValue("**")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOGIN_SERVICE = new PropertyDescriptor.Builder()
            .name("login-service")
            .displayName("Login Service")
            .description("Specify which Login Service to use for Basic Authentication.")
            .required(false)
            .allowableValues(LOGIN_SERVICE_HASH)
            .defaultValue(LOGIN_SERVICE_HASH.getValue())
            .build();


    public static final PropertyDescriptor USERS_PROPERTIES_FILE = new PropertyDescriptor.Builder()
            .name("users-properties-file")
            .displayName("Users Properties File")
            .description("Specify a property file containing users for Basic Authentication using HashLoginService. "
                    + "See http://www.eclipse.org/jetty/documentation/current/configuring-security.html for detail.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getAbstractPropertyDescriptors().stream(),
            Stream.of(
                LISTEN_PORT,
                SSL_CONTEXT,
                CLIENT_AUTH,
                BASIC_AUTH,
                AUTH_PATH_SPEC,
                AUTH_ROLES,
                LOGIN_SERVICE,
                USERS_PROPERTIES_FILE
            )
    ).toList();

    private Server server;
    private Integer listenPort;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }


    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {

        final List<ValidationResult> results = new ArrayList<>();
        if (validationContext.getProperty(BASIC_AUTH).asBoolean()) {
            final String loginServiceValue = validationContext.getProperty(LOGIN_SERVICE).getValue();
            if (LOGIN_SERVICE_HASH.getValue().equals(loginServiceValue)) {
                if (!validationContext.getProperty(USERS_PROPERTIES_FILE).isSet()) {
                    results.add(new ValidationResult.Builder().subject(USERS_PROPERTIES_FILE.getDisplayName())
                            .explanation("it is required by HashLoginService").valid(false).build());
                }
            }
        }

        return results;
    }

    public static class StandardJettyWebSocketServlet extends JettyWebSocketServlet implements JettyWebSocketCreator {
        private final ConfigurationContext context;

        public StandardJettyWebSocketServlet(final ConfigurationContext context) {
            this.context = context;
        }

        @Override
        public void configure(final JettyWebSocketServletFactory webSocketServletFactory) {
            final int inputBufferSize = context.getProperty(INPUT_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
            final int maxTextMessageSize = context.getProperty(MAX_TEXT_MESSAGE_SIZE).asDataSize(DataUnit.B).intValue();
            final int maxBinaryMessageSize = context.getProperty(MAX_BINARY_MESSAGE_SIZE).asDataSize(DataUnit.B).intValue();
            webSocketServletFactory.setInputBufferSize(inputBufferSize);
            webSocketServletFactory.setMaxTextMessageSize(maxTextMessageSize);
            webSocketServletFactory.setMaxBinaryMessageSize(maxBinaryMessageSize);
            webSocketServletFactory.setCreator(this);
        }

        @Override
        public Object createWebSocket(JettyServerUpgradeRequest servletUpgradeRequest, JettyServerUpgradeResponse servletUpgradeResponse) {
            final URI requestURI = servletUpgradeRequest.getRequestURI();
            final int port = ((InetSocketAddress) servletUpgradeRequest.getLocalSocketAddress()).getPort();
            final JettyWebSocketServer service = portToControllerService.get(port);

            if (service == null) {
                throw new RuntimeException("No controller service is bound with port: " + port);
            }

            final String path = requestURI.getPath();
            final WebSocketMessageRouter router;
            try {
                router = service.routers.getRouterOrFail(path);
            } catch (WebSocketConfigurationException e) {
                throw new IllegalStateException("Failed to get router due to: "  + e, e);
            }

            return new RoutingWebSocketListener(router);
        }
    }

    @OnEnabled
    @Override
    public void startServer(final ConfigurationContext context) throws Exception {
        if (server != null && server.isRunning()) {
            getLogger().info("Jetty WebSocket Server running {}", server);
            return;
        }

        server = new Server();

        final ContextHandlerCollection handlerCollection = new ContextHandlerCollection();

        final ServletContextHandler contextHandler = new ServletContextHandler();
        // Set ClassLoader so that jetty-server classes are available to WebSocketServletFactory.Loader
        contextHandler.setClassLoader(getClass().getClassLoader());

        // Add basic auth.
        if (context.getProperty(BASIC_AUTH).asBoolean()) {
            final ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
            contextHandler.insertHandler(securityHandler);

            // Accessible from any role and any auth once authentication succeeds.
            final String roles = context.getProperty(AUTH_ROLES).evaluateAttributeExpressions().getValue();
            final Constraint constraint = Constraint.from(roles.split(","));

            final ConstraintMapping constraintMapping = new ConstraintMapping();
            constraintMapping.setPathSpec(context.getProperty(AUTH_PATH_SPEC).evaluateAttributeExpressions().getValue());
            constraintMapping.setConstraint(constraint);

            final DefaultAuthenticatorFactory authenticatorFactory = new DefaultAuthenticatorFactory();
            securityHandler.setAuthenticatorFactory(authenticatorFactory);
            securityHandler.setAuthenticationType("BASIC");
            securityHandler.setRealmName(getClass().getSimpleName());
            securityHandler.setConstraintMappings(Collections.singletonList(constraintMapping));

            final LoginService loginService;
            final String loginServiceValue = context.getProperty(LOGIN_SERVICE).getValue();
            if (LOGIN_SERVICE_HASH.getValue().equals(loginServiceValue)) {
                final String usersFilePath = context.getProperty(USERS_PROPERTIES_FILE).evaluateAttributeExpressions().getValue();
                final Path resourcePath = Paths.get(usersFilePath);
                final PathResourceFactory pathResourceFactory = new PathResourceFactory();
                final Resource pathResource = pathResourceFactory.newResource(resourcePath);
                loginService = new HashLoginService("HashLoginService", pathResource);
            } else {
                throw new IllegalArgumentException("Unsupported Login Service: " + loginServiceValue);
            }

            server.addBean(loginService);
            securityHandler.setLoginService(loginService);
        }

        ServletHandler servletHandler = new ServletHandler();
        JettyWebSocketServletContainerInitializer.configure(contextHandler, null);
        contextHandler.insertHandler(servletHandler);

        handlerCollection.setHandlers(contextHandler);

        server.setHandler(handlerCollection);

        listenPort = context.getProperty(LISTEN_PORT).evaluateAttributeExpressions().asInteger();
        final ServerConnector serverConnector = getServerConnector(context);
        server.setConnectors(new Connector[] {serverConnector});

        final StandardJettyWebSocketServlet webSocketServlet = new StandardJettyWebSocketServlet(context);
        servletHandler.addServletWithMapping(new ServletHolder(webSocketServlet), "/*");

        getLogger().info("Starting Jetty WebSocket Server on Port {}", listenPort);
        server.start();
        listenPort = serverConnector.getLocalPort();

        portToControllerService.put(listenPort, this);
    }

    public int getListeningPort() {
        return listenPort;
    }

    private ServerConnector getServerConnector(final ConfigurationContext context) {
        final StandardServerConnectorFactory serverConnectorFactory = new StandardServerConnectorFactory(server, listenPort);
        final ServerConnector serverConnector;

        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextProvider.class);
        if (sslContextProvider == null) {
            serverConnector = serverConnectorFactory.getServerConnector();
        } else {
            final SSLContext sslContext = sslContextProvider.createContext();
            serverConnectorFactory.setSslContext(sslContext);

            final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
            if (CLIENT_NEED.getValue().equals(clientAuthValue)) {
                serverConnectorFactory.setNeedClientAuth(true);
            } else if (CLIENT_WANT.getValue().equals(clientAuthValue)) {
                serverConnectorFactory.setWantClientAuth(true);
            }

            serverConnector = serverConnectorFactory.getServerConnector();
        }

        return serverConnector;
    }

    @OnDisabled
    @OnShutdown
    @Override
    public void stopServer() throws Exception {
        if (server == null) {
            return;
        }

        getLogger().info("Stopping Jetty WebSocket Server");
        server.stop();
        if (portToControllerService.containsKey(listenPort)
                && this.getIdentifier().equals(portToControllerService.get(listenPort).getIdentifier())) {
            portToControllerService.remove(listenPort);
        }
    }
}
