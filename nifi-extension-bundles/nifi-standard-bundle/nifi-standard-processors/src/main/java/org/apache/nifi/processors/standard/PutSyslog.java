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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.event.transport.EventSender;
import org.apache.nifi.event.transport.configuration.LineEnding;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.StringNettyEventSenderFactory;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.syslog.parsers.SyslogParser;
import org.apache.nifi.util.StopWatch;

import javax.net.ssl.SSLContext;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@TriggerWhenEmpty
@Tags({"syslog", "put", "udp", "tcp", "logs"})
@CapabilityDescription("Sends Syslog messages to a given host and port over TCP or UDP. Messages are constructed from the \"Message ___\" properties of the processor " +
        "which can use expression language to generate messages from incoming FlowFiles. The properties are used to construct messages of the form: " +
        "(<PRIORITY>)(VERSION )(TIMESTAMP) (HOSTNAME) (BODY) where version is optional.  The constructed messages are checked against regular expressions for " +
        "RFC5424 and RFC3164 formatted messages. The timestamp can be an RFC5424 timestamp with a format of \"yyyy-MM-dd'T'HH:mm:ss.SZ\" or \"yyyy-MM-dd'T'HH:mm:ss.S+hh:mm\", " +
        "or it can be an RFC3164 timestamp with a format of \"MMM d HH:mm:ss\". If a message is constructed that does not form a valid Syslog message according to the " +
        "above description, then it is routed to the invalid relationship. Valid messages are sent to the Syslog server and successes are routed to the success relationship, " +
        "failures routed to the failure relationship.")
@SeeAlso({ListenSyslog.class, ParseSyslog.class})
public class PutSyslog extends AbstractSyslogProcessor {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The IP address or hostname of the Syslog server.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAX_SOCKET_SEND_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Size of Socket Send Buffer")
            .description("The maximum size of the socket send buffer that should be used. This is a suggestion to the Operating System " +
                    "to indicate how big the socket buffer should be. If this value is set too low, the buffer may fill up before " +
                    "the data can be read, and incoming data will be dropped.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
            .Builder().name("Batch Size")
            .description("The number of incoming FlowFiles to process in a single execution of this processor.")
            .required(true)
            .defaultValue("25")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    public static final PropertyDescriptor IDLE_EXPIRATION = new PropertyDescriptor
            .Builder().name("Idle Connection Expiration")
            .description("The amount of time a connection should be held open without being used before closing the connection.")
            .required(true)
            .defaultValue("5 seconds")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
    public static final PropertyDescriptor MSG_PRIORITY = new PropertyDescriptor
            .Builder().name("Message Priority")
            .description("The priority for the Syslog messages, excluding < >.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MSG_VERSION = new PropertyDescriptor
            .Builder().name("Message Version")
            .description("The version for the Syslog messages.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MSG_TIMESTAMP = new PropertyDescriptor
            .Builder().name("Message Timestamp")
            .description("The timestamp for the Syslog messages. The timestamp can be an RFC5424 timestamp with a format of " +
                    "\"yyyy-MM-dd'T'HH:mm:ss.SZ\" or \"yyyy-MM-dd'T'HH:mm:ss.S+hh:mm\", \" or it can be an RFC3164 timestamp " +
                    "with a format of \"MMM d HH:mm:ss\".")
            .required(true)
            .defaultValue("${now():format('MMM d HH:mm:ss')}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MSG_HOSTNAME = new PropertyDescriptor
            .Builder().name("Message Hostname")
            .description("The hostname for the Syslog messages.")
            .required(true)
            .defaultValue("${hostname(true)}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MSG_BODY = new PropertyDescriptor
            .Builder().name("Message Body")
            .description("The body for the Syslog messages.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, syslog " +
                    "messages will be sent over a secure connection.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .dependsOn(PROTOCOL, TCP_VALUE)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            HOSTNAME,
            PROTOCOL,
            PORT,
            MAX_SOCKET_SEND_BUFFER_SIZE,
            SSL_CONTEXT_SERVICE,
            IDLE_EXPIRATION,
            TIMEOUT,
            BATCH_SIZE,
            CHARSET,
            MSG_PRIORITY,
            MSG_VERSION,
            MSG_TIMESTAMP,
            MSG_HOSTNAME,
            MSG_BODY
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to Syslog are sent out this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to Syslog are sent out this relationship.")
            .build();
    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("FlowFiles that do not form a valid Syslog message are sent out this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_INVALID
    );

    private EventSender<String> eventSender;
    private String transitUri;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String protocol = context.getProperty(PROTOCOL).getValue();
        final PropertyValue sslContextServiceProperty = context.getProperty(SSL_CONTEXT_SERVICE);

        if (UDP_VALUE.getValue().equals(protocol) && sslContextServiceProperty.isSet()) {
            results.add(new ValidationResult.Builder()
                    .explanation("SSL can not be used with UDP")
                    .valid(false).subject(SSL_CONTEXT_SERVICE.getDisplayName()).build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws InterruptedException {
        eventSender = getEventSender(context);
        final String protocol = context.getProperty(PROTOCOL).getValue();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        transitUri = String.format("%s://%s:%s", protocol, hostname, port);
    }

    @OnStopped
    public void onStopped() throws Exception {
        if (eventSender != null) {
            eventSender.close();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            context.yield();
        } else {
            for (final FlowFile flowFile : flowFiles) {
                final StopWatch timer = new StopWatch(true);
                final String syslogMessage = getSyslogMessage(context, flowFile);
                if (isValid(syslogMessage)) {
                    try {
                        eventSender.sendEvent(syslogMessage);
                        timer.stop();

                        final long duration = timer.getDuration(TimeUnit.MILLISECONDS);
                        session.getProvenanceReporter().send(flowFile, transitUri, duration, true);

                        getLogger().debug("Send Completed {}", flowFile);
                        session.transfer(flowFile, REL_SUCCESS);
                    } catch (final Exception e) {
                        getLogger().error("Send Failed {}", flowFile, e);
                        session.transfer(flowFile, REL_FAILURE);
                    }
                } else {
                    getLogger().debug("Syslog Message Invalid {}", flowFile);
                    session.transfer(flowFile, REL_INVALID);
                }
            }
        }
    }

    protected EventSender<String> getEventSender(final ProcessContext context) {
        final TransportProtocol protocol = TransportProtocol.valueOf(context.getProperty(PROTOCOL).getValue());
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions().getValue());

        final LineEnding lineEnding = TransportProtocol.TCP.equals(protocol) ? LineEnding.UNIX : LineEnding.NONE;
        final StringNettyEventSenderFactory factory = new StringNettyEventSenderFactory(getLogger(), hostname, port, protocol, charset, lineEnding);
        factory.setThreadNamePrefix(String.format("%s[%s]", PutSyslog.class.getSimpleName(), getIdentifier()));
        factory.setWorkerThreads(context.getMaxConcurrentTasks());
        factory.setMaxConnections(context.getMaxConcurrentTasks());

        final int timeout = context.getProperty(TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        factory.setTimeout(Duration.ofMillis(timeout));

        final PropertyValue sslContextServiceProperty = context.getProperty(SSL_CONTEXT_SERVICE);
        if (sslContextServiceProperty.isSet()) {
            final SSLContextProvider sslContextProvider = sslContextServiceProperty.asControllerService(SSLContextProvider.class);
            final SSLContext sslContext = sslContextProvider.createContext();
            factory.setSslContext(sslContext);
        }

        return factory.getEventSender();
    }

    private String getSyslogMessage(final ProcessContext context, final FlowFile flowFile) {
        final String priority = context.getProperty(MSG_PRIORITY).evaluateAttributeExpressions(flowFile).getValue();
        final String version = context.getProperty(MSG_VERSION).evaluateAttributeExpressions(flowFile).getValue();
        final String timestamp = context.getProperty(MSG_TIMESTAMP).evaluateAttributeExpressions(flowFile).getValue();
        final String hostname = context.getProperty(MSG_HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
        final String body = context.getProperty(MSG_BODY).evaluateAttributeExpressions(flowFile).getValue();

        final StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("<").append(priority).append(">");
        if (version != null) {
            messageBuilder.append(version).append(StringUtils.SPACE);
        }
        messageBuilder.append(timestamp).append(StringUtils.SPACE).append(hostname).append(StringUtils.SPACE).append(body);
        return messageBuilder.toString();
    }

    private boolean isValid(final String message) {
        for (final Pattern pattern : SyslogParser.MESSAGE_PATTERNS) {
            final Matcher matcher = pattern.matcher(message);
            if (matcher.matches()) {
                return true;
            }
        }
        return false;
    }
}
