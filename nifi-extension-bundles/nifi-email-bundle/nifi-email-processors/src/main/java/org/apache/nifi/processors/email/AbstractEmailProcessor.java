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
package org.apache.nifi.processors.email;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.integration.mail.AbstractMailReceiver;

import jakarta.mail.Address;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Base processor for implementing processors to consume messages from Email
 * servers using Spring Integration libraries.
 *
 * @param <T> the type of {@link AbstractMailReceiver}.
 */
abstract class AbstractEmailProcessor<T extends AbstractMailReceiver> extends AbstractProcessor {
    public static final AllowableValue PASSWORD_BASED_AUTHORIZATION_MODE = new AllowableValue(
            "password-based-authorization-mode",
            "Use Password",
            "Use password"
    );

    public static final AllowableValue OAUTH_AUTHORIZATION_MODE = new AllowableValue(
            "oauth-based-authorization-mode",
            "Use OAuth2",
            "Use OAuth2 to acquire access token"
    );
    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("host")
            .displayName("Host Name")
            .description("Network address of Email server (e.g., pop.gmail.com, imap.gmail.com . . .)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("port")
            .displayName("Port")
            .description("Numeric value identifying Port of Email server (e.g., 993)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor AUTHORIZATION_MODE = new PropertyDescriptor.Builder()
            .name("authorization-mode")
            .displayName("Authorization Mode")
            .description("How to authorize sending email on the user's behalf.")
            .required(true)
            .allowableValues(PASSWORD_BASED_AUTHORIZATION_MODE, OAUTH_AUTHORIZATION_MODE)
            .defaultValue(PASSWORD_BASED_AUTHORIZATION_MODE.getValue())
            .build();
    public static final PropertyDescriptor OAUTH2_ACCESS_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("oauth2-access-token-provider")
            .displayName("OAuth2 Access Token Provider")
            .description("OAuth2 service that can provide access tokens.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .dependsOn(AUTHORIZATION_MODE, OAUTH_AUTHORIZATION_MODE)
            .required(true)
            .build();
    public static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("user")
            .displayName("User Name")
            .description("User Name used for authentication and authorization with Email server.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("Password used for authentication and authorization with Email server.")
            .dependsOn(AUTHORIZATION_MODE, PASSWORD_BASED_AUTHORIZATION_MODE)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor FOLDER = new PropertyDescriptor.Builder()
            .name("folder")
            .displayName("Folder")
            .description("Email folder to retrieve messages from (e.g., INBOX)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("INBOX")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("fetch.size")
            .displayName("Fetch Size")
            .description("Specify the maximum number of Messages to fetch per call to Email Server.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("10")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor SHOULD_DELETE_MESSAGES = new PropertyDescriptor.Builder()
            .name("delete.messages")
            .displayName("Delete Messages")
            .description("Specify whether mail messages should be deleted after retrieval.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connection.timeout")
            .displayName("Connection timeout")
            .description("The amount of time to wait to connect to Email server")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("30 sec")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All messages that are the are successfully received from Email server and converted to FlowFiles are routed to this relationship")
            .build();

    /*
     * Will ensure that list of PropertyDescriptors is build only once, since
     * all other lifecycle methods are invoked multiple times.
     */
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            HOST,
            PORT,
            AUTHORIZATION_MODE,
            OAUTH2_ACCESS_TOKEN_PROVIDER,
            USER,
            PASSWORD,
            FOLDER,
            FETCH_SIZE,
            SHOULD_DELETE_MESSAGES,
            CONNECTION_TIMEOUT
    );

    final static Set<Relationship> SHARED_RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected volatile T messageReceiver;

    private volatile BlockingQueue<Message> messageQueue;

    private volatile String displayUrl;

    private volatile ProcessSession processSession;

    protected volatile Optional<OAuth2AccessTokenProvider> oauth2AccessTokenProviderOptional;
    protected volatile AccessToken oauth2AccessDetails;

    protected static List<PropertyDescriptor> getCommonPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (context.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).isSet()) {
            OAuth2AccessTokenProvider oauth2AccessTokenProvider = context.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);

            oauth2AccessDetails = oauth2AccessTokenProvider.getAccessDetails();

            oauth2AccessTokenProviderOptional = Optional.of(oauth2AccessTokenProvider);
        } else {
            oauth2AccessTokenProviderOptional = Optional.empty();
        }
    }

    @OnStopped
    public void stop(ProcessContext processContext) {
        this.flushRemainingMessages(processContext);
        try {
            this.messageReceiver.destroy();
            this.messageReceiver = null;
        } catch (Exception e) {
            this.logger.warn("Failure while closing processor", e);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return SHARED_RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession processSession) throws ProcessException {
        this.initializeIfNecessary(context, processSession);

        Message emailMessage = this.receiveMessage();
        if (emailMessage != null) {
            this.transfer(emailMessage, context, processSession);
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName + "' Java Mail property.")
                .name(propertyDescriptorName).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true)
                .build();
    }

    /**
     * Delegates to sub-classes to build the target receiver as
     * {@link AbstractMailReceiver}
     *
     * @param context instance of {@link ProcessContext}
     * @return new instance of {@link AbstractMailReceiver}
     */
    protected abstract T buildMessageReceiver(ProcessContext context);

    /**
     * Return the target receiver's mail protocol (e.g., imap, pop etc.)
     */
    protected abstract String getProtocol(ProcessContext processContext);

    /**
     * Builds the url used to connect to the email server.
     */
    String buildUrl(ProcessContext processContext) {
        String host = processContext.getProperty(HOST).evaluateAttributeExpressions().getValue();
        String port = processContext.getProperty(PORT).evaluateAttributeExpressions().getValue();
        String user = processContext.getProperty(USER).evaluateAttributeExpressions().getValue();

        String password = oauth2AccessTokenProviderOptional.map(oauth2AccessTokenProvider ->
            oauth2AccessTokenProvider.getAccessDetails().getAccessToken()
        ).orElse(processContext.getProperty(PASSWORD).evaluateAttributeExpressions().getValue());

        String folder = processContext.getProperty(FOLDER).evaluateAttributeExpressions().getValue();

        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append(URLEncoder.encode(user, StandardCharsets.UTF_8));

        urlBuilder.append(":");
        urlBuilder.append(URLEncoder.encode(password, StandardCharsets.UTF_8));
        urlBuilder.append("@");
        urlBuilder.append(host);
        urlBuilder.append(":");
        urlBuilder.append(port);
        urlBuilder.append("/");
        urlBuilder.append(folder);

        String protocol = this.getProtocol(processContext);
        String finalUrl = protocol + "://" + urlBuilder;

        // build display-safe URL
        int passwordStartIndex = urlBuilder.indexOf(":") + 1;
        int passwordEndIndex = urlBuilder.indexOf("@");
        urlBuilder.replace(passwordStartIndex, passwordEndIndex, "[password]");
        this.displayUrl = protocol + "://" + urlBuilder;
        if (this.logger.isInfoEnabled()) {
            this.logger.info("Connecting to Email server at the following URL: {}", this.displayUrl);
        }

        return finalUrl;
    }

    /**
     * Builds and initializes the target message receiver if necessary (if it's
     * null). Upon execution of this operation the receiver is fully functional
     * and is ready to receive messages.
     */
    private synchronized void initializeIfNecessary(ProcessContext context, ProcessSession processSession) {
        if (this.messageReceiver == null || isOauth2AccessDetailsRefreshed()) {
            this.processSession = processSession;
            this.messageReceiver = this.buildMessageReceiver(context);

            int fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();

            this.messageReceiver.setMaxFetchSize(fetchSize);
            this.messageReceiver.setJavaMailProperties(this.buildJavaMailProperties(context));
            // need to avoid spring warning messages
            this.messageReceiver.setBeanFactory(new StaticListableBeanFactory());
            this.messageReceiver.afterPropertiesSet();

            this.messageQueue = new ArrayBlockingQueue<>(fetchSize);
        }
    }

    private boolean isOauth2AccessDetailsRefreshed() {
        boolean oauthDetailsRefreshed = this.oauth2AccessTokenProviderOptional.isPresent()
                &&
                (this.oauth2AccessDetails == null || !oauth2AccessDetails.equals(this.oauth2AccessTokenProviderOptional.get().getAccessDetails()));

        return oauthDetailsRefreshed;
    }

    /**
     * Extracts dynamic properties which typically represent the Java Mail
     * properties from the {@link ProcessContext} returning them as instance of
     * {@link Properties}
     */
    private Properties buildJavaMailProperties(ProcessContext context) {
        Properties javaMailProperties = new Properties();
        for (Entry<PropertyDescriptor, String> propertyDescriptorEntry : context.getProperties().entrySet()) {
            if (propertyDescriptorEntry.getKey().isDynamic()
                    && !propertyDescriptorEntry.getKey().getName().equals("mail.imap.timeout")
                    && !propertyDescriptorEntry.getKey().getName().equals("mail.pop3.timeout")) {
                javaMailProperties.setProperty(propertyDescriptorEntry.getKey().getName(),
                        propertyDescriptorEntry.getValue());
            }
        }
        String protocol = this.getProtocol(context);

        String propertyName = protocol.equals("pop3") ? "mail.pop3.timeout" : "mail.imap.timeout";
        final String timeoutInMillis = String.valueOf(context.getProperty(CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS));
        javaMailProperties.setProperty(propertyName, timeoutInMillis);

        oauth2AccessTokenProviderOptional.ifPresent(oauth2AccessTokenProvider -> javaMailProperties.put("mail." + protocol + ".auth.mechanisms", "XOAUTH2"));

        return javaMailProperties;
    }

    /**
     * Fills the internal message queue if such queue is empty. This is due to
     * the fact that per single session there may be multiple messages retrieved
     * from the email server (see FETCH_SIZE).
     */
    private synchronized void fillMessageQueueIfNecessary() {
        if (this.messageQueue.isEmpty()) {
            Object[] messages;
            try {
                messages = this.messageReceiver.receive();
            } catch (MessagingException e) {
                String errorMsg = "Failed to receive messages from Email server: [" + e.getClass().getName()
                        + " - " + e.getMessage();
                this.getLogger().error(errorMsg);
                throw new ProcessException(errorMsg, e);
            }

            if (messages != null) {
                for (Object message : messages) {
                    this.messageQueue.offer((Message) message);
                }
            }
        }
    }

    /**
     * Disposes the message by converting it to a {@link FlowFile} transferring
     * it to the REL_SUCCESS relationship.
     */
    private void transfer(Message emailMessage, ProcessContext context, ProcessSession processSession) {
        long start = System.nanoTime();
        FlowFile flowFile = processSession.create();

        flowFile = processSession.append(flowFile, out -> {
            try {
                emailMessage.writeTo(out);
            } catch (MessagingException e) {
                throw new IOException(e);
            }
        });

        long executionDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        String fromAddressesString = "";
        try {
            Address[] fromAddresses = emailMessage.getFrom();
            if (fromAddresses != null) {
                fromAddressesString = Arrays.asList(fromAddresses).toString();
            }
        } catch (MessagingException e) {
            this.logger.warn("Failed to retrieve 'From' attribute from Message.");
        }

        processSession.getProvenanceReporter().receive(flowFile, this.displayUrl, "Received message from " + fromAddressesString, executionDuration);
        this.getLogger().info("Successfully received {} from {} in {} millis", flowFile, fromAddressesString, executionDuration);
        processSession.transfer(flowFile, REL_SUCCESS);

    }

    /**
     * Receives message from the internal queue filling up the queue if
     * necessary.
     */
    private Message receiveMessage() {
        Message emailMessage = null;
        try {
            this.fillMessageQueueIfNecessary();
            emailMessage = this.messageQueue.poll(1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.logger.debug("Current thread is interrupted");
        }
        return emailMessage;
    }

    /**
     * Will flush the remaining messages when this processor is stopped.
     */
    private void flushRemainingMessages(ProcessContext processContext) {
        Message emailMessage;
        try {
            while ((emailMessage = this.messageQueue.poll(1, TimeUnit.MILLISECONDS)) != null) {
                this.transfer(emailMessage, processContext, this.processSession);
                this.processSession.commitAsync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            this.logger.debug("Current thread is interrupted");
        }
    }
}
