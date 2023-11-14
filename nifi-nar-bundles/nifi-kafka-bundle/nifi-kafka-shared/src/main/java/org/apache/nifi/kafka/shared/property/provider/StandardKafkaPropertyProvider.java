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
package org.apache.nifi.kafka.shared.property.provider;

import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.SASL_MECHANISM;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.SECURITY_PROTOCOL;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.SSL_CONTEXT_SERVICE;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.AZURE_APP_ID;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.AZURE_APP_SECRET;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.AZURE_TENANT_ID;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.BOOTSTRAP_SERVERS;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.SASL_MECHANISM;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SASL_CLIENT_CALLBACK_HANDLER_CLASS;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SASL_JAAS_CONFIG;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SASL_LOGIN_CLASS;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_LOCATION;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_TYPE;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEY_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_LOCATION;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_TYPE;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SASL_LOGIN_CALLBACK_HANDLER_CLASS;

import org.apache.nifi.kafka.shared.aad.AADAuthenticateCallbackHandler;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.kafka.shared.login.DelegatingLoginConfigProvider;
import org.apache.nifi.kafka.shared.login.LoginConfigProvider;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.kafka.shared.property.SecurityProtocol;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.FormatUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * Standard implementation of Kafka Property Provider based on shared Kafka Property Descriptors
 */
public class StandardKafkaPropertyProvider implements KafkaPropertyProvider {
    private static final String MILLISECOND_PROPERTY_SUFFIX = ".ms";

    private static final String SASL_GSSAPI_CUSTOM_LOGIN_CLASS = "org.apache.nifi.processors.kafka.pubsub.CustomKerberosLogin";

    public static final String AAD_AUTHENTICATION_CALLBACK_HANDLER_CLASS = "org.apache.nifi.kafka.shared.aad.AADAuthenticateCallbackHandler";

    public static final String SASL_AWS_MSK_IAM_CLIENT_CALLBACK_HANDLER_CLASS = "software.amazon.msk.auth.iam.IAMClientCallbackHandler";

    private static final LoginConfigProvider LOGIN_CONFIG_PROVIDER = new DelegatingLoginConfigProvider();

    private final Set<String> clientPropertyNames;

    public StandardKafkaPropertyProvider(final Class<?> kafkaClientClass) {
        final KafkaPropertyNameProvider provider = new StandardKafkaPropertyNameProvider(kafkaClientClass);
        clientPropertyNames = provider.getPropertyNames();
    }

    @Override
    public Map<String, Object> getProperties(final PropertyContext context) {
        final Map<String, Object> properties = new LinkedHashMap<>();
        setClientProperties(properties, context);
        setSecurityProperties(properties, context);
        setSslProperties(properties, context);
        return properties;
    }

    private void setSecurityProperties(final Map<String, Object> properties, final PropertyContext context) {
        final String protocol = context.getProperty(SECURITY_PROTOCOL).getValue();
        properties.put(SECURITY_PROTOCOL.getName(), protocol);

        final SecurityProtocol securityProtocol = SecurityProtocol.valueOf(protocol);
        if (SecurityProtocol.SASL_PLAINTEXT == securityProtocol || SecurityProtocol.SASL_SSL == securityProtocol) {
            final String loginConfig = LOGIN_CONFIG_PROVIDER.getConfiguration(context);
            properties.put(SASL_JAAS_CONFIG.getProperty(), loginConfig);

            final SaslMechanism saslMechanism = SaslMechanism.getSaslMechanism(context.getProperty(SASL_MECHANISM).getValue());
            if (SaslMechanism.GSSAPI == saslMechanism && isCustomKerberosLoginFound()) {
                properties.put(SASL_LOGIN_CLASS.getProperty(), SASL_GSSAPI_CUSTOM_LOGIN_CLASS);
            } else if (SaslMechanism.AWS_MSK_IAM == saslMechanism && isAwsMskIamCallbackHandlerFound()) {
                properties.put(SASL_CLIENT_CALLBACK_HANDLER_CLASS.getProperty(), SASL_AWS_MSK_IAM_CLIENT_CALLBACK_HANDLER_CLASS);
            } else if (SaslMechanism.AADOAUTHBEARER == saslMechanism && isCustomAADLoginFound()) {
                properties.put(SASL_LOGIN_CALLBACK_HANDLER_CLASS.getProperty(), AAD_AUTHENTICATION_CALLBACK_HANDLER_CLASS);
                AADAuthenticateCallbackHandler.authority = context.getProperty(AZURE_TENANT_ID).evaluateAttributeExpressions().getValue();
                AADAuthenticateCallbackHandler.appId = context.getProperty(AZURE_APP_ID).evaluateAttributeExpressions().getValue();
                AADAuthenticateCallbackHandler.appSecret = context.getProperty(AZURE_APP_SECRET).evaluateAttributeExpressions().getValue();
                AADAuthenticateCallbackHandler.bootstrapServer = context.getProperty(BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();
            }
        }
    }

    private void setSslProperties(final Map<String, Object> properties, final PropertyContext context) {
        final PropertyValue sslContextServiceProperty = context.getProperty(SSL_CONTEXT_SERVICE);
        if (sslContextServiceProperty.isSet()) {
            final SSLContextService sslContextService = sslContextServiceProperty.asControllerService(SSLContextService.class);
            if (sslContextService.isKeyStoreConfigured()) {
                properties.put(SSL_KEYSTORE_LOCATION.getProperty(), sslContextService.getKeyStoreFile());
                properties.put(SSL_KEYSTORE_TYPE.getProperty(), sslContextService.getKeyStoreType());

                final String keyStorePassword = sslContextService.getKeyStorePassword();
                properties.put(SSL_KEYSTORE_PASSWORD.getProperty(), keyStorePassword);

                final String keyPassword = sslContextService.getKeyPassword();
                final String configuredKeyPassword = keyPassword == null ? keyStorePassword : keyPassword;
                properties.put(SSL_KEY_PASSWORD.getProperty(), configuredKeyPassword);
            }
            if (sslContextService.isTrustStoreConfigured()) {
                properties.put(SSL_TRUSTSTORE_LOCATION.getProperty(), sslContextService.getTrustStoreFile());
                properties.put(SSL_TRUSTSTORE_TYPE.getProperty(), sslContextService.getTrustStoreType());
                properties.put(SSL_TRUSTSTORE_PASSWORD.getProperty(), sslContextService.getTrustStorePassword());
            }
        }
    }

    private void setClientProperties(final Map<String, Object> properties, final PropertyContext context) {
        final Set<PropertyDescriptor> propertyDescriptors = getPropertyDescriptors(context).stream()
                .filter(propertyDescriptor -> clientPropertyNames.contains(propertyDescriptor.getName()))
                .collect(Collectors.toSet());

        for (final PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            final PropertyValue property = context.getProperty(propertyDescriptor);
            final String propertyValue = propertyDescriptor.isExpressionLanguageSupported()
                    ? property.evaluateAttributeExpressions().getValue()
                    : property.getValue();
            if (propertyValue == null) {
                continue;
            }

            final String propertyName = propertyDescriptor.getName();
            setProperty(properties, propertyName, propertyValue);
        }
    }

    private Set<PropertyDescriptor> getPropertyDescriptors(final PropertyContext context) {
        final Set<PropertyDescriptor> propertyDescriptors;
        if (context instanceof ConfigurationContext) {
            final ConfigurationContext configurationContext = (ConfigurationContext) context;
            propertyDescriptors = configurationContext.getProperties().keySet();
        } else if (context instanceof ProcessContext) {
            final ProcessContext processContext = (ProcessContext) context;
            propertyDescriptors = processContext.getProperties().keySet();
        } else {
            throw new IllegalArgumentException(String.format("Property Context [%s] not supported", context.getClass().getName()));
        }
        return propertyDescriptors;
    }

    private void setProperty(final Map<String, Object> properties, final String propertyName, final String propertyValue) {
        if (propertyName.endsWith(MILLISECOND_PROPERTY_SUFFIX)) {
            final Matcher durationMatcher = FormatUtils.TIME_DURATION_PATTERN.matcher(propertyValue);
            if (durationMatcher.matches()) {
                final long milliseconds = Math.round(FormatUtils.getPreciseTimeDuration(propertyValue, TimeUnit.MILLISECONDS));
                properties.put(propertyName, Long.toString(milliseconds));
            } else {
                properties.put(propertyName, propertyValue);
            }
        } else {
            properties.put(propertyName, propertyValue);
        }
    }

    private static boolean isCustomKerberosLoginFound() {
        return isClassFound(SASL_GSSAPI_CUSTOM_LOGIN_CLASS);
    }

    public static boolean isCustomAADLoginFound() {
        return isClassFound(AAD_AUTHENTICATION_CALLBACK_HANDLER_CLASS);
    }

    public static boolean isAwsMskIamCallbackHandlerFound() {
        return isClassFound(SASL_AWS_MSK_IAM_CLIENT_CALLBACK_HANDLER_CLASS);
    }

    private static boolean isClassFound(final String className) {
        try {
            Class.forName(className);
            return true;
        } catch (final ClassNotFoundException e) {
            return false;
        }
    }
}
