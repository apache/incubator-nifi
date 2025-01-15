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
package org.apache.nifi.processors.splunk;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.HttpException;
import com.splunk.RequestMessage;
import com.splunk.ResponseMessage;
import com.splunk.SSLSecurityProtocol;
import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

abstract class SplunkAPICall extends AbstractProcessor {
    private static final String REQUEST_CHANNEL_HEADER_NAME = "X-Splunk-Request-Channel";

    private static final String HTTP_SCHEME = "http";
    private static final String HTTPS_SCHEME = "https";

    private static final AllowableValue TLS_1_2_VALUE = new AllowableValue(SSLSecurityProtocol.TLSv1_2.name(), SSLSecurityProtocol.TLSv1_2.name());
    private static final AllowableValue TLS_1_1_VALUE = new AllowableValue(SSLSecurityProtocol.TLSv1_1.name(), SSLSecurityProtocol.TLSv1_1.name());
    private static final AllowableValue TLS_1_VALUE = new AllowableValue(SSLSecurityProtocol.TLSv1.name(), SSLSecurityProtocol.TLSv1.name());
    private static final AllowableValue SSL_3_VALUE = new AllowableValue(SSLSecurityProtocol.SSLv3.name(), SSLSecurityProtocol.SSLv3.name());

    static final String ACKNOWLEDGEMENT_ID_ATTRIBUTE = "splunk.acknowledgement.id";
    static final String RESPONDED_AT_ATTRIBUTE = "splunk.responded.at";

    static final PropertyDescriptor SCHEME = new PropertyDescriptor.Builder()
            .name("Scheme")
            .displayName("Scheme")
            .description("The scheme for connecting to Splunk.")
            .allowableValues(HTTPS_SCHEME, HTTP_SCHEME)
            .defaultValue(HTTPS_SCHEME)
            .required(true)
            .build();

    static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .displayName("Hostname")
            .description("The ip address or hostname of the Splunk server.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .displayName("HTTP Event Collector Port")
            .description("The HTTP Event Collector HTTP Port Number.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .defaultValue("8088")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor SECURITY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("Security Protocol")
            .displayName("Security Protocol")
            .description("The security protocol to use for communicating with Splunk.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(TLS_1_2_VALUE, TLS_1_1_VALUE, TLS_1_VALUE, SSL_3_VALUE)
            .defaultValue(TLS_1_2_VALUE.getValue())
            .build();

    static final PropertyDescriptor OWNER = new PropertyDescriptor.Builder()
            .name("Owner")
            .displayName("Owner")
            .description("The owner to pass to Splunk.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor TOKEN = new PropertyDescriptor.Builder()
            .name("Token")
            .displayName("HTTP Event Collector Token")
            .description("HTTP Event Collector token starting with the string Splunk. For example \'Splunk 1234578-abcd-1234-abcd-1234abcd\'")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .displayName("Username")
            .description("The username to authenticate to Splunk.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .displayName("Password")
            .description("The password to authenticate to Splunk.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .build();

    static final PropertyDescriptor REQUEST_CHANNEL = new PropertyDescriptor.Builder()
            .name("request-channel")
            .displayName("Splunk Request Channel")
            .description("Identifier of the used request channel.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SCHEME,
            HOSTNAME,
            PORT,
            SECURITY_PROTOCOL,
            OWNER,
            TOKEN,
            USERNAME,
            PASSWORD,
            REQUEST_CHANNEL
    );

    private final JsonFactory jsonFactory = new JsonFactory();
    private final ObjectMapper jsonObjectMapper = new ObjectMapper(jsonFactory);

    private volatile ServiceArgs splunkServiceArguments;
    private volatile Service splunkService;
    private volatile String requestChannel;

    protected static List<PropertyDescriptor> getCommonPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        splunkServiceArguments = getSplunkServiceArgs(context);
        splunkService = getSplunkService(splunkServiceArguments);
        requestChannel = context.getProperty(SplunkAPICall.REQUEST_CHANNEL).evaluateAttributeExpressions().getValue();
    }

    private ServiceArgs getSplunkServiceArgs(final ProcessContext context) {
        final ServiceArgs splunkServiceArguments = new ServiceArgs();

        splunkServiceArguments.setScheme(context.getProperty(SCHEME).getValue());
        splunkServiceArguments.setHost(context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue());
        splunkServiceArguments.setPort(context.getProperty(PORT).evaluateAttributeExpressions().asInteger());

        if (context.getProperty(OWNER).isSet()) {
            splunkServiceArguments.setOwner(context.getProperty(OWNER).evaluateAttributeExpressions().getValue());
        }

        if (context.getProperty(TOKEN).isSet()) {
            splunkServiceArguments.setToken(context.getProperty(TOKEN).evaluateAttributeExpressions().getValue());
        }

        if (context.getProperty(USERNAME).isSet()) {
            splunkServiceArguments.setUsername(context.getProperty(USERNAME).evaluateAttributeExpressions().getValue());
        }

        if (context.getProperty(PASSWORD).isSet()) {
            splunkServiceArguments.setPassword(context.getProperty(PASSWORD).getValue());
        }

        if (HTTPS_SCHEME.equals(context.getProperty(SCHEME).getValue()) && context.getProperty(SECURITY_PROTOCOL).isSet()) {
            splunkServiceArguments.setSSLSecurityProtocol(SSLSecurityProtocol.valueOf(context.getProperty(SECURITY_PROTOCOL).getValue()));
        }

        return splunkServiceArguments;
    }

    protected Service getSplunkService(final ServiceArgs splunkServiceArguments) {
        return Service.connect(splunkServiceArguments);
    }

    @OnStopped
    public void onStopped() {
        if (splunkService != null) {
            splunkService.logout();
            splunkService = null;
        }

        requestChannel = null;
        splunkServiceArguments = null;
    }

    protected ResponseMessage call(final String endpoint, final RequestMessage request)  {
        request.getHeader().put(REQUEST_CHANNEL_HEADER_NAME, requestChannel);

        try {
            return splunkService.send(endpoint, request);
            //Catch Stale connection exception, reinitialize, and retry
        } catch (final HttpException e) {
            getLogger().error("Splunk request status code: {}. Retrying the request.", e.getStatus());
            splunkService.logout();
            splunkService = getSplunkService(splunkServiceArguments);
            return splunkService.send(endpoint, request);
        }
    }

    protected <T> T unmarshallResult(final InputStream responseBody, final Class<T> type) throws IOException {
        return jsonObjectMapper.readValue(responseBody, type);
    }

    protected String marshalRequest(final Object request) throws IOException {
        return jsonObjectMapper.writeValueAsString(request);
    }
}
