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
package org.apache.nifi.web.client.provider.service;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.proxy.ProxyContext;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.redirect.RedirectHandling;
import org.apache.nifi.web.client.ssl.TlsContext;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import java.net.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.nifi.proxy.ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE;

@CapabilityDescription("Web Client Service Provider with support for configuring standard HTTP connection properties")
@Tags({ "HTTP", "Web", "Client" })
public class StandardWebClientServiceProvider extends AbstractControllerService implements WebClientServiceProvider {

    static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connect-timeout")
            .displayName("Connect Timeout")
            .description("Maximum amount of time to wait before failing during initial socket connection")
            .required(true)
            .defaultValue("10 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("read-timeout")
            .displayName("Read Timeout")
            .description("Maximum amount of time to wait before failing while reading socket responses")
            .required(true)
            .defaultValue("10 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor WRITE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("write-timeout")
            .displayName("Write Timeout")
            .description("Maximum amount of time to wait before failing while writing socket requests")
            .required(true)
            .defaultValue("10 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor REDIRECT_HANDLING_STRATEGY = new PropertyDescriptor.Builder()
            .name("redirect-handling-strategy")
            .displayName("Redirect Handling Strategy")
            .description("Handling strategy for responding to HTTP 301 or 302 redirects received with a Location header")
            .required(true)
            .defaultValue(RedirectHandling.FOLLOWED.name())
            .allowableValues(RedirectHandling.values())
            .build();

    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("SSL Context Service overrides system default TLS settings for HTTPS communication")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECT_TIMEOUT,
            READ_TIMEOUT,
            WRITE_TIMEOUT,
            REDIRECT_HANDLING_STRATEGY,
            SSL_CONTEXT_SERVICE,
            PROXY_CONFIGURATION_SERVICE
    );

    private StandardWebClientService webClientService;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final StandardWebClientService standardWebClientService = new StandardWebClientService();

        final Duration connectTimeout = getDuration(context, CONNECT_TIMEOUT);
        standardWebClientService.setConnectTimeout(connectTimeout);

        final Duration readTimeout = getDuration(context, READ_TIMEOUT);
        standardWebClientService.setReadTimeout(readTimeout);

        final Duration writeTimeout = getDuration(context, WRITE_TIMEOUT);
        standardWebClientService.setReadTimeout(writeTimeout);

        final String redirectHandlingStrategy = context.getProperty(REDIRECT_HANDLING_STRATEGY).getValue();
        final RedirectHandling redirectHandling = RedirectHandling.valueOf(redirectHandlingStrategy);
        standardWebClientService.setRedirectHandling(redirectHandling);

        final PropertyValue sslContextServiceProperty = context.getProperty(SSL_CONTEXT_SERVICE);
        if (sslContextServiceProperty.isSet()) {
            final SSLContextProvider sslContextProvider = sslContextServiceProperty.asControllerService(SSLContextProvider.class);
            final TlsContext tlsContext = getTlsContext(sslContextProvider);
            standardWebClientService.setTlsContext(tlsContext);
        }

        final PropertyValue proxyConfigurationServiceProperty = context.getProperty(PROXY_CONFIGURATION_SERVICE);
        if (proxyConfigurationServiceProperty.isSet()) {
            final ProxyConfigurationService proxyConfigurationService = context.getProperty(PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);
            final ProxyConfiguration proxyConfiguration = proxyConfigurationService.getConfiguration();
            final ProxyContext proxyContext = getProxyContext(proxyConfiguration);
            standardWebClientService.setProxyContext(proxyContext);
        }

        webClientService = standardWebClientService;
    }

    @OnDisabled
    public void onDisabled() {
        webClientService.close();
    }

    @Override
    public HttpUriBuilder getHttpUriBuilder() {
        return new StandardHttpUriBuilder();
    }

    @Override
    public WebClientService getWebClientService() {
        return webClientService;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private Duration getDuration(final ConfigurationContext context, final PropertyDescriptor propertyDescriptor) {
        final long millis = context.getProperty(propertyDescriptor).asTimePeriod(TimeUnit.MILLISECONDS);
        return Duration.ofMillis(millis);
    }

    private TlsContext getTlsContext(final SSLContextProvider sslContextProvider) {
        final X509TrustManager trustManager = sslContextProvider.createTrustManager();
        final Optional<X509ExtendedKeyManager> keyManager = sslContextProvider.createKeyManager();
        final SSLContext sslContext = sslContextProvider.createContext();

        return new TlsContext() {
            @Override
            public String getProtocol() {
                return sslContext.getProtocol();
            }

            @Override
            public X509TrustManager getTrustManager() {
                return trustManager;
            }

            @Override
            public Optional<X509KeyManager> getKeyManager() {
                return keyManager.map(Function.identity());
            }
        };
    }

    private ProxyContext getProxyContext(final ProxyConfiguration proxyConfiguration) {
        return new ProxyContext() {
            @Override
            public Proxy getProxy() {
                return proxyConfiguration.createProxy();
            }

            @Override
            public Optional<String> getUsername() {
                return Optional.ofNullable(proxyConfiguration.getProxyUserName());
            }

            @Override
            public Optional<String> getPassword() {
                return Optional.ofNullable(proxyConfiguration.getProxyUserPassword());
            }
        };
    }
}
