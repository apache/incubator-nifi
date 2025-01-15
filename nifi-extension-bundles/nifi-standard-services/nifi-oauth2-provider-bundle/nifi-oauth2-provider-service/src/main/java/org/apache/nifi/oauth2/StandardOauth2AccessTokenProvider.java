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
package org.apache.nifi.oauth2;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.ssl.SSLContextProvider;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Tags({"oauth2", "provider", "authorization", "access token", "http"})
@CapabilityDescription("Provides OAuth 2.0 access tokens that can be used as Bearer authorization header in HTTP requests." +
    " Can use either Resource Owner Password Credentials Grant or Client Credentials Grant." +
    " Client authentication can be done with either HTTP Basic authentication or in the request body.")
public class StandardOauth2AccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider, VerifiableControllerService {
    public static final PropertyDescriptor AUTHORIZATION_SERVER_URL = new PropertyDescriptor.Builder()
        .name("authorization-server-url")
        .displayName("Authorization Server URL")
        .description("The URL of the authorization server that issues access tokens.")
        .required(true)
        .addValidator(StandardValidators.URL_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor CLIENT_AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
        .name("client-authentication-strategy")
        .displayName("Client Authentication Strategy")
        .description("Strategy for authenticating the client against the OAuth2 token provider service.")
        .required(true)
        .allowableValues(ClientAuthenticationStrategy.class)
        .defaultValue(ClientAuthenticationStrategy.REQUEST_BODY.getValue())
        .build();

    public static AllowableValue RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE = new AllowableValue(
        "password",
        "User Password",
        "Resource Owner Password Credentials Grant. Used to access resources available to users. Requires username and password and usually Client ID and Client Secret."
    );

    public static AllowableValue CLIENT_CREDENTIALS_GRANT_TYPE = new AllowableValue(
        "client_credentials",
        "Client Credentials",
        "Client Credentials Grant. Used to access resources available to clients. Requires Client ID and Client Secret."
    );

    public static AllowableValue REFRESH_TOKEN_GRANT_TYPE = new AllowableValue(
        "refresh_token",
        "Refresh Token",
        "Refresh Token Grant. Used to get fresh access tokens based on a previously acquired refresh token. Requires Client ID and Client Secret (apart from Refresh Token)."
    );

    public static final PropertyDescriptor GRANT_TYPE = new PropertyDescriptor.Builder()
        .name("grant-type")
        .displayName("Grant Type")
        .description("The OAuth2 Grant Type to be used when acquiring an access token.")
        .required(true)
        .allowableValues(RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE, CLIENT_CREDENTIALS_GRANT_TYPE, REFRESH_TOKEN_GRANT_TYPE)
        .defaultValue(RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE.getValue())
        .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("service-user-name")
        .displayName("Username")
        .description("Username on the service that is being accessed.")
        .dependsOn(GRANT_TYPE, RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE)
        .required(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
        .name("service-password")
        .displayName("Password")
        .description("Password for the username on the service that is being accessed.")
        .dependsOn(GRANT_TYPE, RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE)
        .required(true)
        .sensitive(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor REFRESH_TOKEN = new PropertyDescriptor.Builder()
        .name("refresh-token")
        .displayName("Refresh Token")
        .description("Refresh Token.")
        .dependsOn(GRANT_TYPE, REFRESH_TOKEN_GRANT_TYPE)
        .required(true)
        .sensitive(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
        .name("client-id")
        .displayName("Client ID")
        .required(false)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor CLIENT_SECRET = new PropertyDescriptor.Builder()
        .name("client-secret")
        .displayName("Client secret")
        .dependsOn(CLIENT_ID)
        .required(true)
        .sensitive(true)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static final PropertyDescriptor SCOPE = new PropertyDescriptor.Builder()
        .name("scope")
        .displayName("Scope")
        .description("Space-delimited, case-sensitive list of scopes of the access request (as per the OAuth 2.0 specification)")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor RESOURCE = new PropertyDescriptor.Builder()
        .name("resource")
        .displayName("Resource")
        .description("Resource URI for the access token request defined in RFC 8707 Section 2")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor AUDIENCE = new PropertyDescriptor.Builder()
        .name("audience")
        .displayName("Audience")
        .description("Audience for the access token request defined in RFC 8693 Section 2.1")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor REFRESH_WINDOW = new PropertyDescriptor.Builder()
        .name("refresh-window")
        .displayName("Refresh Window")
        .description("The service will attempt to refresh tokens expiring within the refresh window, subtracting the configured duration from the token expiration.")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("0 s")
        .required(true)
        .build();

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
        .name("ssl-context-service")
        .displayName("SSL Context Service")
        .addValidator(Validator.VALID)
        .identifiesControllerService(SSLContextProvider.class)
        .required(false)
        .build();

    public static final PropertyDescriptor HTTP_PROTOCOL_STRATEGY = new PropertyDescriptor.Builder()
        .name("HTTP Protocols")
        .description("HTTP Protocols supported for Application Layer Protocol Negotiation with TLS")
        .required(true)
        .allowableValues(HttpProtocolStrategy.class)
        .defaultValue(HttpProtocolStrategy.H2_HTTP_1_1.getValue())
        .dependsOn(SSL_CONTEXT)
        .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH};

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        AUTHORIZATION_SERVER_URL,
        CLIENT_AUTHENTICATION_STRATEGY,
        GRANT_TYPE,
        USERNAME,
        PASSWORD,
        REFRESH_TOKEN,
        CLIENT_ID,
        CLIENT_SECRET,
        SCOPE,
        RESOURCE,
        AUDIENCE,
        REFRESH_WINDOW,
        SSL_CONTEXT,
        HTTP_PROTOCOL_STRATEGY,
        ProxyConfiguration.createProxyConfigPropertyDescriptor(PROXY_SPECS)
    );

    private static final String AUTHORIZATION_HEADER = "Authorization";

    public static final ObjectMapper ACCESS_DETAILS_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    private volatile String authorizationServerUrl;
    private volatile OkHttpClient httpClient;

    private volatile ClientAuthenticationStrategy clientAuthenticationStrategy;
    private volatile String grantType;
    private volatile String username;
    private volatile String password;
    private volatile String clientId;
    private volatile String clientSecret;
    private volatile String scope;
    private volatile String resource;
    private volatile String audience;
    private volatile long refreshWindowSeconds;

    private volatile AccessToken accessDetails;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        authorizationServerUrl = context.getProperty(AUTHORIZATION_SERVER_URL).evaluateAttributeExpressions().getValue();

        httpClient = createHttpClient(context);

        clientAuthenticationStrategy = ClientAuthenticationStrategy.valueOf(context.getProperty(CLIENT_AUTHENTICATION_STRATEGY).getValue());
        grantType = context.getProperty(GRANT_TYPE).getValue();
        username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        password = context.getProperty(PASSWORD).getValue();
        clientId = context.getProperty(CLIENT_ID).evaluateAttributeExpressions().getValue();
        clientSecret = context.getProperty(CLIENT_SECRET).getValue();
        scope = context.getProperty(SCOPE).getValue();
        resource = context.getProperty(RESOURCE).getValue();
        audience = context.getProperty(AUDIENCE).getValue();

        if (context.getProperty(REFRESH_TOKEN).isSet()) {
            String refreshToken = context.getProperty(REFRESH_TOKEN).evaluateAttributeExpressions().getValue();

            AccessToken accessDetailsWithRefreshTokenOnly = new AccessToken();
            accessDetailsWithRefreshTokenOnly.setRefreshToken(refreshToken);
            accessDetailsWithRefreshTokenOnly.setExpiresIn(-1);

            this.accessDetails = accessDetailsWithRefreshTokenOnly;
        }

        refreshWindowSeconds = context.getProperty(REFRESH_WINDOW).asTimePeriod(TimeUnit.SECONDS);
    }

    @OnDisabled
    public void onDisabled() {
        accessDetails = null;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        if (
            validationContext.getProperty(GRANT_TYPE).getValue().equals(CLIENT_CREDENTIALS_GRANT_TYPE.getValue())
                && !validationContext.getProperty(CLIENT_ID).isSet()
        ) {
            validationResults.add(new ValidationResult.Builder().subject(CLIENT_ID.getDisplayName())
                .valid(false)
                .explanation(String.format(
                    "When '%s' is set to '%s', '%s' is required",
                    GRANT_TYPE.getDisplayName(),
                    CLIENT_CREDENTIALS_GRANT_TYPE.getDisplayName(),
                    CLIENT_ID.getDisplayName())
                )
                .build());
        }

        ProxyConfiguration.validateProxySpec(validationContext, validationResults, PROXY_SPECS);

        return validationResults;
    }

    protected OkHttpClient createHttpClient(ConfigurationContext context) {
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();

        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextProvider.class);
        if (sslContextProvider != null) {
            final X509TrustManager trustManager = sslContextProvider.createTrustManager();
            final SSLContext sslContext = sslContextProvider.createContext();
            clientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
        }

        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context);

        final Proxy proxy = proxyConfig.createProxy();
        if (!Proxy.Type.DIRECT.equals(proxy.type())) {
            clientBuilder.proxy(proxy);
            if (proxyConfig.hasCredential()) {
                clientBuilder.proxyAuthenticator((route, response) -> {
                    final String credential = Credentials.basic(proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword());
                    return response.request().newBuilder()
                                   .header("Proxy-Authorization", credential)
                                   .build();
                });
            }
        }

        final HttpProtocolStrategy httpProtocolStrategy = HttpProtocolStrategy.valueOf(context.getProperty(HTTP_PROTOCOL_STRATEGY).getValue());
        clientBuilder.protocols(httpProtocolStrategy.getProtocols());

        return clientBuilder.build();
    }

    @Override
    public AccessToken getAccessDetails() {
        if (this.accessDetails == null) {
            acquireAccessDetails();
        } else if (isRefreshRequired()) {
            if (this.accessDetails.getRefreshToken() == null) {
                acquireAccessDetails();
            } else {
                try {
                    refreshAccessDetails();
                } catch (Exception e) {
                    getLogger().info("Refresh Access Token request failed [{}]", authorizationServerUrl, e);
                    acquireAccessDetails();
                }
            }
        }

        return accessDetails;
    }

    private boolean isRefreshRequired() {
        final Instant expirationRefreshTime = accessDetails.getFetchTime()
                .plusSeconds(accessDetails.getExpiresIn())
                .minusSeconds(refreshWindowSeconds);

        return Instant.now().isAfter(expirationRefreshTime);
    }

    private void acquireAccessDetails() {
        getLogger().debug("New Access Token request started [{}]", authorizationServerUrl);

        FormBody.Builder acquireTokenBuilder = new FormBody.Builder();

        if (grantType.equals(RESOURCE_OWNER_PASSWORD_CREDENTIALS_GRANT_TYPE.getValue())) {
            acquireTokenBuilder.add("grant_type", "password")
                .add("username", username)
                .add("password", password);
        } else if (grantType.equals(CLIENT_CREDENTIALS_GRANT_TYPE.getValue())) {
            acquireTokenBuilder.add("grant_type", "client_credentials");
        }

        addFormData(acquireTokenBuilder);

        this.accessDetails = requestToken(acquireTokenBuilder);
    }

    private void refreshAccessDetails() {
        getLogger().debug("Refresh Access Token request started [{}]", authorizationServerUrl);

        FormBody.Builder refreshTokenBuilder = new FormBody.Builder()
                .add("grant_type", "refresh_token")
                .add("refresh_token", this.accessDetails.getRefreshToken());

        addFormData(refreshTokenBuilder);

        AccessToken newAccessDetails = requestToken(refreshTokenBuilder);

        if (newAccessDetails.getRefreshToken() == null) {
            newAccessDetails.setRefreshToken(this.accessDetails.getRefreshToken());
        }

        this.accessDetails = newAccessDetails;
    }

    private void addFormData(FormBody.Builder formBuilder) {
        if (clientAuthenticationStrategy == ClientAuthenticationStrategy.REQUEST_BODY && clientId != null) {
            formBuilder.add("client_id", clientId);
            formBuilder.add("client_secret", clientSecret);
        }
        if (scope != null) {
            formBuilder.add("scope", scope);
        }
        if (resource != null) {
            formBuilder.add("resource", resource);
        }
        if (audience != null) {
            formBuilder.add("audience", audience);
        }
    }

    private AccessToken requestToken(FormBody.Builder formBuilder) {
        RequestBody requestBody = formBuilder.build();

        Request.Builder requestBuilder = new Request.Builder()
                .url(authorizationServerUrl)
                .post(requestBody);

        if (ClientAuthenticationStrategy.BASIC_AUTHENTICATION == clientAuthenticationStrategy && clientId != null) {
            requestBuilder.addHeader(AUTHORIZATION_HEADER, Credentials.basic(clientId, clientSecret));
        }

        Request request = requestBuilder.build();

        return getAccessDetails(request);
    }

    private AccessToken getAccessDetails(final Request newRequest) {
        try {
            final Response response = httpClient.newCall(newRequest).execute();
            final String responseBody = response.body().string();
            if (response.isSuccessful()) {
                getLogger().debug("OAuth2 Access Token retrieved [HTTP {}]", response.code());
                return ACCESS_DETAILS_MAPPER.readValue(responseBody, AccessToken.class);
            } else {
                getLogger().error(String.format("OAuth2 access token request failed [HTTP %d], response:%n%s", response.code(), responseBody));
                throw new ProcessException(String.format("OAuth2 access token request failed [HTTP %d]", response.code()));
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("OAuth2 access token request failed", e);
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(ConfigurationContext context, ComponentLog verificationLogger, Map<String, String> variables) {
        ConfigVerificationResult.Builder builder = new ConfigVerificationResult.Builder()
                .verificationStepName("Can acquire token");

        try {
            getAccessDetails();
            builder.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL);
        } catch (Exception ex) {
            builder.outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation(ex.getMessage());
        }

        return Arrays.asList(builder.build());
    }
}
