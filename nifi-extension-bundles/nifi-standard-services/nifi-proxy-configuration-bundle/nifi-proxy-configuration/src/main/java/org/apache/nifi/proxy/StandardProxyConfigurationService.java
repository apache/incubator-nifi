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
package org.apache.nifi.proxy;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@CapabilityDescription("Provides a set of configurations for different NiFi components to use a proxy server.")
@Tags({"Proxy"})
public class StandardProxyConfigurationService extends AbstractControllerService implements ProxyConfigurationService {

    public static final PropertyDescriptor PROXY_TYPE = new PropertyDescriptor.Builder()
            .name("proxy-type")
            .displayName("Proxy Type")
            .description("Proxy type.")
            .allowableValues(Proxy.Type.values())
            .defaultValue(Proxy.Type.DIRECT.name())
            .required(true)
            .build();

    public static final PropertyDescriptor SOCKS_VERSION = new PropertyDescriptor.Builder()
            .name("socks-version")
            .displayName("SOCKS Version")
            .description("SOCKS Protocol Version")
            .allowableValues(SocksVersion.values())
            .defaultValue(SocksVersion.SOCKS5.name())
            .dependsOn(PROXY_TYPE, Proxy.Type.SOCKS.name())
            .required(true)
            .build();

    public static final PropertyDescriptor PROXY_SERVER_HOST = new PropertyDescriptor.Builder()
            .name("proxy-server-host")
            .displayName("Proxy Server Host")
            .description("Proxy server hostname or ip-address.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PROXY_SERVER_PORT = new PropertyDescriptor.Builder()
            .name("proxy-server-port")
            .displayName("Proxy Server Port")
            .description("Proxy server port number.")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PROXY_USER_NAME = new PropertyDescriptor.Builder()
            .name("proxy-user-name")
            .displayName("Proxy User Name")
            .description("The name of the proxy client for user authentication.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PROXY_USER_PASSWORD = new PropertyDescriptor.Builder()
            .name("proxy-user-password")
            .displayName("Proxy User Password")
            .description("The password of the proxy client for user authentication.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .sensitive(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        PROXY_TYPE,
        SOCKS_VERSION,
        PROXY_SERVER_HOST,
        PROXY_SERVER_PORT,
        PROXY_USER_NAME,
        PROXY_USER_PASSWORD
    );

    private volatile ProxyConfiguration configuration = ProxyConfiguration.DIRECT_CONFIGURATION;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void setConfiguredValues(final ConfigurationContext context) {
        configuration = new ProxyConfiguration();

        final Proxy.Type proxyType = Proxy.Type.valueOf(context.getProperty(PROXY_TYPE).getValue());
        configuration.setProxyType(proxyType);
        configuration.setSocksVersion(proxyType == Proxy.Type.SOCKS ? SocksVersion.valueOf(context.getProperty(SOCKS_VERSION).getValue()) : null);
        configuration.setProxyServerHost(context.getProperty(PROXY_SERVER_HOST).evaluateAttributeExpressions().getValue());
        configuration.setProxyServerPort(context.getProperty(PROXY_SERVER_PORT).evaluateAttributeExpressions().asInteger());
        configuration.setProxyUserName(context.getProperty(PROXY_USER_NAME).evaluateAttributeExpressions().getValue());
        configuration.setProxyUserPassword(context.getProperty(PROXY_USER_PASSWORD).evaluateAttributeExpressions().getValue());
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Proxy.Type proxyType = Proxy.Type.valueOf(validationContext.getProperty(PROXY_TYPE).getValue());
        if (Proxy.Type.DIRECT.equals(proxyType)) {
            return Collections.emptyList();
        }

        final List<ValidationResult> results = new ArrayList<>();
        if (!validationContext.getProperty(PROXY_SERVER_HOST).isSet()) {
            results.add(new ValidationResult.Builder().subject(PROXY_SERVER_HOST.getDisplayName())
                    .explanation("required").valid(false).build());
        }
        if (!validationContext.getProperty(PROXY_SERVER_PORT).isSet()) {
            results.add(new ValidationResult.Builder().subject(PROXY_SERVER_PORT.getDisplayName())
                    .explanation("required").valid(false).build());
        }
        return results;
    }

    @Override
    public ProxyConfiguration getConfiguration() {
        return configuration;
    }
}
