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
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.ByteArrayMessageNettyEventServerFactory;
import org.apache.nifi.event.transport.netty.FilteringStrategy;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.syslog.events.SyslogEvent;
import org.apache.nifi.syslog.parsers.SyslogParser;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processor.util.listen.ListenerProperties.NETWORK_INTF_NAME;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"syslog", "listen", "udp", "tcp", "logs"})
@CapabilityDescription("Listens for Syslog messages being sent to a given port over TCP or UDP. Incoming messages are checked against regular " +
        "expressions for RFC5424 and RFC3164 formatted messages. The format of each message is: (<PRIORITY>)(VERSION )(TIMESTAMP) (HOSTNAME) (BODY) " +
        "where version is optional. The timestamp can be an RFC5424 timestamp with a format of \"yyyy-MM-dd'T'HH:mm:ss.SZ\" or \"yyyy-MM-dd'T'HH:mm:ss.S+hh:mm\", " +
        "or it can be an RFC3164 timestamp with a format of \"MMM d HH:mm:ss\". If an incoming messages matches one of these patterns, the message will be " +
        "parsed and the individual pieces will be placed in FlowFile attributes, with the original message in the content of the FlowFile. If an incoming " +
        "message does not match one of these patterns it will not be parsed and the syslog.valid attribute will be set to false with the original message " +
        "in the content of the FlowFile. Valid messages will be transferred on the success relationship, and invalid messages will be transferred on the " +
        "invalid relationship.")
@WritesAttributes({ @WritesAttribute(attribute = "syslog.priority", description = "The priority of the Syslog message."),
                    @WritesAttribute(attribute = "syslog.severity", description = "The severity of the Syslog message derived from the priority."),
                    @WritesAttribute(attribute = "syslog.facility", description = "The facility of the Syslog message derived from the priority."),
                    @WritesAttribute(attribute = "syslog.version", description = "The optional version from the Syslog message."),
                    @WritesAttribute(attribute = "syslog.timestamp", description = "The timestamp of the Syslog message."),
                    @WritesAttribute(attribute = "syslog.hostname", description = "The hostname or IP address of the Syslog message."),
                    @WritesAttribute(attribute = "syslog.sender", description = "The hostname of the Syslog server that sent the message."),
                    @WritesAttribute(attribute = "syslog.body", description = "The body of the Syslog message, everything after the hostname."),
                    @WritesAttribute(attribute = "syslog.valid", description = "An indicator of whether this message matched the expected formats. " +
                            "If this value is false, the other attributes will be empty and only the original message will be available in the content."),
                    @WritesAttribute(attribute = "syslog.protocol", description = "The protocol over which the Syslog message was received."),
                    @WritesAttribute(attribute = "syslog.port", description = "The port over which the Syslog message was received."),
                    @WritesAttribute(attribute = "mime.type", description = "The mime.type of the FlowFile which will be text/plain for Syslog messages.")})
@SeeAlso({PutSyslog.class, ParseSyslog.class})
public class ListenSyslog extends AbstractSyslogProcessor {

    public static final PropertyDescriptor MAX_MESSAGE_QUEUE_SIZE = new PropertyDescriptor.Builder()
        .name("Max Size of Message Queue")
        .displayName("Max Size of Message Queue")
        .description("The maximum size of the internal queue used to buffer messages being transferred from the underlying channel to the processor. " +
                    "Setting this value higher allows more messages to be buffered in memory during surges of incoming messages, but increases the total " +
                    "memory used by the processor.")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("10000")
        .required(true)
        .build();

    public static final PropertyDescriptor RECV_BUFFER_SIZE = new PropertyDescriptor.Builder()
        .name("Receive Buffer Size")
        .displayName("Receive Buffer Size")
        .description("The size of each buffer used to receive Syslog messages. Adjust this value appropriately based on the expected size of the " +
                    "incoming Syslog messages. When UDP is selected each buffer will hold one Syslog message. When TCP is selected messages are read " +
                    "from an incoming connection until the buffer is full, or the connection is closed. ")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("65507 B")
        .required(true)
        .build();
    public static final PropertyDescriptor MAX_SOCKET_BUFFER_SIZE = new PropertyDescriptor.Builder()
        .name("Max Size of Socket Buffer")
        .displayName("Max Size of Socket Buffer")
        .description("The maximum size of the socket buffer that should be used. This is a suggestion to the Operating System " +
                    "to indicate how big the socket buffer should be. If this value is set too low, the buffer may fill up before " +
                    "the data can be read, and incoming data will be dropped.")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .defaultValue("1 MB")
        .required(true)
        .dependsOn(PROTOCOL, TCP_VALUE)
        .build();
    public static final PropertyDescriptor MAX_CONNECTIONS = new PropertyDescriptor.Builder()
        .name("Max Number of TCP Connections")
        .displayName("Max Number of TCP Connections")
        .description("The maximum number of concurrent connections to accept Syslog messages in TCP mode.")
        .addValidator(StandardValidators.createLongValidator(1, 65535, true))
        .defaultValue("2")
        .required(true)
        .dependsOn(PROTOCOL, TCP_VALUE)
        .build();
    public static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Max Batch Size")
        .displayName("Max Batch Size")
        .description(
                "The maximum number of Syslog events to add to a single FlowFile. If multiple events are available, they will be concatenated along with "
                + "the <Message Delimiter> up to this configured maximum number of messages")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("1")
        .required(true)
        .build();
    public static final PropertyDescriptor MESSAGE_DELIMITER = new PropertyDescriptor.Builder()
        .name("Message Delimiter")
        .displayName("Message Delimiter")
        .description("Specifies the delimiter to place between Syslog messages when multiple messages are bundled together (see <Max Batch Size> property).")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("\\n")
        .required(true)
        .build();
    public static final PropertyDescriptor PARSE_MESSAGES = new PropertyDescriptor.Builder()
        .name("Parse Messages")
        .displayName("Parse Messages")
        .description("Indicates if the processor should parse the Syslog messages. If set to false, each outgoing FlowFile will only " +
                    "contain the sender, protocol, and port, and no additional attributes.")
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("SSL Context Service")
        .displayName("SSL Context Service")
        .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, syslog " +
                    "messages will be received over a secure connection.")
        .required(false)
        .identifiesControllerService(SSLContextProvider.class)
        .dependsOn(PROTOCOL, TCP_VALUE)
        .build();
    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
        .name("Client Auth")
        .displayName("Client Auth")
        .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
        .required(false)
        .allowableValues(ClientAuth.values())
        .defaultValue(ClientAuth.REQUIRED.name())
        .dependsOn(SSL_CONTEXT_SERVICE)
        .build();
    public static final PropertyDescriptor SOCKET_KEEP_ALIVE = new PropertyDescriptor.Builder()
            .name("socket-keep-alive")
            .displayName("Socket Keep Alive")
            .description("Whether or not to have TCP socket keep alive turned on. Timing details depend on operating system properties.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .defaultValue(Boolean.FALSE.toString())
            .dependsOn(PROTOCOL, TCP_VALUE)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PROTOCOL,
            PORT,
            NETWORK_INTF_NAME,
            SOCKET_KEEP_ALIVE,
            SSL_CONTEXT_SERVICE,
            CLIENT_AUTH,
            RECV_BUFFER_SIZE,
            MAX_MESSAGE_QUEUE_SIZE,
            MAX_SOCKET_BUFFER_SIZE,
            MAX_CONNECTIONS,
            MAX_BATCH_SIZE,
            MESSAGE_DELIMITER,
            PARSE_MESSAGES,
            CHARSET
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Syslog messages that match one of the expected formats will be sent out this relationship as a FlowFile per message.")
        .build();
    public static final Relationship REL_INVALID = new Relationship.Builder()
        .name("invalid")
        .description("Syslog messages that do not match one of the expected formats will be sent out this relationship as a FlowFile per message.")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_INVALID
    );

    protected static final String RECEIVED_COUNTER = "Messages Received";
    protected static final String SUCCESS_COUNTER = "FlowFiles Transferred to Success";
    private static final String DEFAULT_MIME_TYPE = "text/plain";

    private volatile EventServer eventServer;
    private volatile SyslogParser parser;
    private volatile BlockingQueue<ByteArrayMessage> syslogEvents = new LinkedBlockingQueue<>();
    private volatile byte[] messageDemarcatorBytes; //it is only the array reference that is volatile - not the contents.

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        // if we are changing the protocol, the events that we may have queued up are no longer valid, as they
        // were received using a different protocol and may be from a completely different source
        if (PROTOCOL.equals(descriptor)) {
            syslogEvents.clear();
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        if (validationContext.getProperty(MAX_BATCH_SIZE).asInteger() > 1 && validationContext.getProperty(PARSE_MESSAGES).asBoolean()) {
            results.add(new ValidationResult.Builder().subject("Parse Messages").input("true").valid(false)
                .explanation("Cannot set Parse Messages to 'true' if Batch Size is greater than 1").build());
        }

        return results;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final int receiveBufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxSocketBufferSize = context.getProperty(MAX_SOCKET_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final int maxMessageQueueSize = context.getProperty(MAX_MESSAGE_QUEUE_SIZE).asInteger();
        final String protocol = context.getProperty(PROTOCOL).getValue();
        final TransportProtocol transportProtocol = TransportProtocol.valueOf(protocol);
        final String networkInterfaceName = context.getProperty(NETWORK_INTF_NAME).evaluateAttributeExpressions().getValue();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions().getValue());
        final String msgDemarcator = context.getProperty(MESSAGE_DELIMITER).getValue().replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        messageDemarcatorBytes = msgDemarcator.getBytes(charset);
        parser = new SyslogParser(charset);
        syslogEvents = new LinkedBlockingQueue<>(maxMessageQueueSize);

        final InetAddress address = getListenAddress(networkInterfaceName);
        final ByteArrayMessageNettyEventServerFactory factory = new ByteArrayMessageNettyEventServerFactory(getLogger(),
                address, port, transportProtocol, messageDemarcatorBytes, receiveBufferSize, syslogEvents, FilteringStrategy.EMPTY);
        factory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        factory.setThreadNamePrefix(String.format("%s[%s]", ListenSyslog.class.getSimpleName(), getIdentifier()));
        final int maxConnections = context.getProperty(MAX_CONNECTIONS).asLong().intValue();
        factory.setWorkerThreads(maxConnections);
        factory.setSocketReceiveBuffer(maxSocketBufferSize);

        final Boolean socketKeepAlive = context.getProperty(SOCKET_KEEP_ALIVE).asBoolean();
        factory.setSocketKeepAlive(socketKeepAlive);

        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        if (sslContextProvider != null && TCP_VALUE.getValue().equals(protocol)) {
            final SSLContext sslContext = sslContextProvider.createContext();
            ClientAuth clientAuth = ClientAuth.REQUIRED;
            final PropertyValue clientAuthProperty = context.getProperty(CLIENT_AUTH);
            if (clientAuthProperty.isSet()) {
                clientAuth = ClientAuth.valueOf(clientAuthProperty.getValue());
            }
            factory.setSslContext(sslContext);
            factory.setClientAuth(clientAuth);
        }
        eventServer = factory.getEventServer();
    }

    public int getListeningPort() {
        return eventServer == null ? 0 : eventServer.getListeningPort();
    }

    @OnStopped
    public void shutdownEventServer() {
        if (eventServer != null) {
            eventServer.shutdown();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // poll the queue with a small timeout to avoid unnecessarily yielding below
        ByteArrayMessage rawSyslogEvent = getMessage(session);

        // if nothing in the queue just return, we don't want to yield here because yielding could adversely
        // impact performance, and we already have a long poll in getMessage so there will be some built in
        // throttling even when no data is available
        if (rawSyslogEvent == null) {
            return;
        }

        final boolean parseMessages = context.getProperty(PARSE_MESSAGES).asBoolean();
        final Map<String, FlowFile> flowFilePerSender = new HashMap<>();

        final Map<String, String> defaultAttributes = getDefaultAttributes(context);
        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
        for (int i = 0; i < maxBatchSize; i++) {
            SyslogEvent event = null;

            // If this is our first iteration, we have already polled our queues. Otherwise, poll on each iteration.
            if (i > 0) {
                rawSyslogEvent = getMessage(session);
                if (rawSyslogEvent == null) {
                    break;
                }
            }

            final String sender = rawSyslogEvent.getSender();
            FlowFile flowFile = flowFilePerSender.computeIfAbsent(sender, k -> session.create());
            flowFile = session.putAllAttributes(flowFile, defaultAttributes);
            flowFile = session.putAttribute(flowFile, SyslogAttributes.SYSLOG_SENDER.key(), sender);

            if (parseMessages) {
                event = parseSyslogEvent(rawSyslogEvent);

                // If the event is invalid, route it to 'invalid' and then stop.
                // We create a separate FlowFile for this case instead of using 'flowFile',
                // because the 'flowFile' object may already have data written to it.
                if (event == null || !event.isValid()) {
                    FlowFile invalidFlowFile = session.create();
                    invalidFlowFile = session.putAllAttributes(invalidFlowFile, defaultAttributes);
                    invalidFlowFile = session.putAttribute(invalidFlowFile, SyslogAttributes.SYSLOG_SENDER.key(), sender);

                    final byte[] messageBytes = rawSyslogEvent.getMessage();
                    invalidFlowFile = session.write(invalidFlowFile, outputStream -> outputStream.write(messageBytes));

                    session.transfer(invalidFlowFile, REL_INVALID);
                    break;
                }

                flowFile = session.putAllAttributes(flowFile, getEventAttributes(event));
            }

            // figure out if we should write the bytes from the raw event or parsed event
            final boolean writeDemarcator = (i > 0);
            final byte[] messageBytes = (event == null) ? rawSyslogEvent.getMessage() : event.getRawMessage();
            flowFile = session.append(flowFile, outputStream -> {
               if (writeDemarcator) {
                   outputStream.write(messageDemarcatorBytes);
               }
               outputStream.write(messageBytes);
            });

            flowFilePerSender.put(sender, flowFile);
        }

        for (final Map.Entry<String, FlowFile> entry : flowFilePerSender.entrySet()) {
            final String sender = entry.getKey();
            final FlowFile flowFile = entry.getValue();

            if (flowFile.getSize() == 0L) {
                session.remove(flowFile);
                getLogger().debug("Removing empty {} from Sender [{}]", flowFile, sender);
                continue;
            }

            session.transfer(flowFile, REL_SUCCESS);
            session.adjustCounter(SUCCESS_COUNTER, 1L, false);

            final String transitUri = getTransitUri(flowFile);
            session.getProvenanceReporter().receive(flowFile, transitUri);
        }
    }

    private InetAddress getListenAddress(final String networkInterfaceName) throws SocketException {
        InetAddress listenAddress = null;
        if (StringUtils.isNotEmpty(networkInterfaceName)) {
            final NetworkInterface networkInterface = NetworkInterface.getByName(networkInterfaceName);
            listenAddress = networkInterface.getInetAddresses().nextElement();
        }
        return listenAddress;
    }

    private SyslogEvent parseSyslogEvent(final ByteArrayMessage rawSyslogEvent) {
        final String sender = rawSyslogEvent.getSender();
        final byte[] message = rawSyslogEvent.getMessage();
        SyslogEvent syslogEvent = null;
        try {
            syslogEvent = parser.parseEvent(message, rawSyslogEvent.getSender());
        } catch (final RuntimeException e) {
            getLogger().warn("Syslog Parsing Failed Length [{}] Sender [{}]: {}", message.length, sender, e.getMessage());
        }
        return syslogEvent;
    }

    private ByteArrayMessage getMessage(final ProcessSession session) {
        ByteArrayMessage rawSyslogEvent = null;
        try {
            rawSyslogEvent = syslogEvents.poll(20, TimeUnit.MILLISECONDS);
            if (rawSyslogEvent != null) {
                session.adjustCounter(RECEIVED_COUNTER, 1L, false);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return rawSyslogEvent;
    }

    private String getTransitUri(final FlowFile flowFile) {
        final String protocol = flowFile.getAttribute(SyslogAttributes.SYSLOG_PROTOCOL.key());
        final String sender = flowFile.getAttribute(SyslogAttributes.SYSLOG_SENDER.key());
        final String port = flowFile.getAttribute(SyslogAttributes.SYSLOG_PORT.key());
        return String.format("%s://%s:%s", protocol.toLowerCase(), sender, port);
    }

    private Map<String, String> getDefaultAttributes(final ProcessContext context) {
        final String port = String.valueOf(getListeningPort());
        final String protocol = context.getProperty(PROTOCOL).getValue();

        final Map<String, String> defaultAttributes = new HashMap<>();
        defaultAttributes.put(SyslogAttributes.SYSLOG_PROTOCOL.key(), protocol);
        defaultAttributes.put(SyslogAttributes.SYSLOG_PORT.key(), port);
        defaultAttributes.put(CoreAttributes.MIME_TYPE.key(), DEFAULT_MIME_TYPE);
        return defaultAttributes;
    }

    private Map<String, String> getEventAttributes(final SyslogEvent event) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(SyslogAttributes.SYSLOG_PRIORITY.key(), event.getPriority());
        attributes.put(SyslogAttributes.SYSLOG_SEVERITY.key(), event.getSeverity());
        attributes.put(SyslogAttributes.SYSLOG_FACILITY.key(), event.getFacility());
        attributes.put(SyslogAttributes.SYSLOG_VERSION.key(), event.getVersion());
        attributes.put(SyslogAttributes.SYSLOG_TIMESTAMP.key(), event.getTimeStamp());
        attributes.put(SyslogAttributes.SYSLOG_HOSTNAME.key(), event.getHostName());
        attributes.put(SyslogAttributes.SYSLOG_BODY.key(), event.getMsgBody());
        attributes.put(SyslogAttributes.SYSLOG_VALID.key(), String.valueOf(event.isValid()));
        return attributes;
    }
}
