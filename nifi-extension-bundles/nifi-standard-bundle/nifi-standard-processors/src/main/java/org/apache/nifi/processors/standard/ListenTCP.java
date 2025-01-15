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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.event.transport.EventException;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.SslSessionStatus;
import org.apache.nifi.event.transport.configuration.BufferAllocator;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.ByteArrayMessageNettyEventServerFactory;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.listen.EventBatcher;
import org.apache.nifi.processor.util.listen.FlowFileEventBatch;
import org.apache.nifi.processor.util.listen.ListenerProperties;
import org.apache.nifi.processor.util.listen.queue.TrackingLinkedBlockingQueue;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextProvider;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"listen", "tcp", "tls", "ssl"})
@CapabilityDescription("Listens for incoming TCP connections and reads data from each connection using a line separator " +
        "as the message demarcator. The default behavior is for each message to produce a single FlowFile, however this can " +
        "be controlled by increasing the Batch Size to a larger value for higher throughput. The Receive Buffer Size must be " +
        "set as large as the largest messages expected to be received, meaning if every 100kb there is a line separator, then " +
        "the Receive Buffer Size must be greater than 100kb. " +
        "The processor can be configured to use an SSL Context Service to only allow secure connections. " +
        "When connected clients present certificates for mutual TLS authentication, the Distinguished Names of the client certificate's " +
        "issuer and subject are added to the outgoing FlowFiles as attributes. " +
        "The processor does not perform authorization based on Distinguished Name values, but since these values " +
        "are attached to the outgoing FlowFiles, authorization can be implemented based on these attributes.")
@WritesAttributes({
        @WritesAttribute(attribute = "tcp.sender", description = "The sending host of the messages."),
        @WritesAttribute(attribute = "tcp.port", description = "The sending port the messages were received."),
        @WritesAttribute(attribute = "client.certificate.issuer.dn", description = "For connections using mutual TLS, the Distinguished Name of the " +
                "Certificate Authority that issued the client's certificate " +
                "is attached to the FlowFile."),
        @WritesAttribute(attribute = "client.certificate.subject.dn", description = "For connections using mutual TLS, the Distinguished Name of the " +
                "client certificate's owner (subject) is attached to the FlowFile.")
})
public class ListenTCP extends AbstractProcessor {
    private static final String CLIENT_CERTIFICATE_SUBJECT_DN_ATTRIBUTE = "client.certificate.subject.dn";
    private static final String CLIENT_CERTIFICATE_ISSUER_DN_ATTRIBUTE = "client.certificate.issuer.dn";

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. If this property is set, " +
                    "messages will be received over a secure connection.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("Client Auth")
            .description("The client authentication policy to use for the SSL Context. Only used if an SSL Context Service is provided.")
            .required(true)
            .allowableValues(ClientAuth.values())
            .defaultValue(ClientAuth.REQUIRED.name())
            .dependsOn(SSL_CONTEXT_SERVICE)
            .build();

    protected static final PropertyDescriptor POOL_RECV_BUFFERS = new PropertyDescriptor.Builder()
            .name("pool-receive-buffers")
            .displayName("Pool Receive Buffers")
            .description("Enable or disable pooling of buffers that the processor uses for handling bytes received on socket connections. The framework allocates buffers as needed during processing.")
            .required(true)
            .defaultValue("True")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor IDLE_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("idle-timeout")
            .displayName("Idle Connection Timeout")
            .description("The amount of time a client's connection will remain open if no data is received. The default of 0 seconds will leave connections open until they are closed by the client.")
            .required(true)
            .defaultValue("0 seconds")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ListenerProperties.NETWORK_INTF_NAME,
            ListenerProperties.PORT,
            ListenerProperties.RECV_BUFFER_SIZE,
            ListenerProperties.MAX_MESSAGE_QUEUE_SIZE,
            ListenerProperties.MAX_SOCKET_BUFFER_SIZE,
            ListenerProperties.CHARSET,
            ListenerProperties.WORKER_THREADS,
            ListenerProperties.MAX_BATCH_SIZE,
            ListenerProperties.MESSAGE_DELIMITER,
            IDLE_CONNECTION_TIMEOUT,
            POOL_RECV_BUFFERS,
            SSL_CONTEXT_SERVICE,
            CLIENT_AUTH
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Messages received successfully will be sent out this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private static final long TRACKING_LOG_INTERVAL = 60000;
    private final AtomicLong nextTrackingLog = new AtomicLong();
    private int eventsCapacity;

    protected volatile int port;
    protected volatile TrackingLinkedBlockingQueue<ByteArrayMessage> events;
    protected volatile BlockingQueue<ByteArrayMessage> errorEvents;
    protected volatile EventServer eventServer;
    protected volatile byte[] messageDemarcatorBytes;
    protected volatile EventBatcher<ByteArrayMessage> eventBatcher;

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.removeProperty("max-receiving-threads");
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        int workerThreads = context.getProperty(ListenerProperties.WORKER_THREADS).asInteger();
        int bufferSize = context.getProperty(ListenerProperties.RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        int socketBufferSize = context.getProperty(ListenerProperties.MAX_SOCKET_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        Duration idleTimeout = Duration.ofSeconds(context.getProperty(IDLE_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.SECONDS));
        final String networkInterface = context.getProperty(ListenerProperties.NETWORK_INTF_NAME).evaluateAttributeExpressions().getValue();
        final InetAddress address = NetworkUtils.getInterfaceAddress(networkInterface);
        final Charset charset = Charset.forName(context.getProperty(ListenerProperties.CHARSET).getValue());
        port = context.getProperty(ListenerProperties.PORT).evaluateAttributeExpressions().asInteger();
        eventsCapacity = context.getProperty(ListenerProperties.MAX_MESSAGE_QUEUE_SIZE).asInteger();
        events = new TrackingLinkedBlockingQueue<>(eventsCapacity);
        errorEvents = new LinkedBlockingQueue<>();
        final String msgDemarcator = getMessageDemarcator(context);
        messageDemarcatorBytes = msgDemarcator.getBytes(charset);
        final NettyEventServerFactory eventFactory = new ByteArrayMessageNettyEventServerFactory(getLogger(), address, port, TransportProtocol.TCP, messageDemarcatorBytes, bufferSize, events);

        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        if (sslContextProvider != null) {
            final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
            ClientAuth clientAuth = ClientAuth.valueOf(clientAuthValue);
            SSLContext sslContext = sslContextProvider.createContext();
            eventFactory.setSslContext(sslContext);
            eventFactory.setClientAuth(clientAuth);
        }

        final boolean poolReceiveBuffers = context.getProperty(POOL_RECV_BUFFERS).asBoolean();
        final BufferAllocator bufferAllocator = poolReceiveBuffers ? BufferAllocator.POOLED : BufferAllocator.UNPOOLED;
        eventFactory.setBufferAllocator(bufferAllocator);
        eventFactory.setIdleTimeout(idleTimeout);
        eventFactory.setSocketReceiveBuffer(socketBufferSize);
        eventFactory.setWorkerThreads(workerThreads);
        eventFactory.setThreadNamePrefix(String.format("%s[%s]", getClass().getSimpleName(), getIdentifier()));

        try {
            eventServer = eventFactory.getEventServer();
        } catch (EventException e) {
            getLogger().error("Failed to bind to [{}:{}]", address, port, e);
        }
    }

    public int getListeningPort() {
        return eventServer == null ? 0 : eventServer.getListeningPort();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        processTrackingLog();
        final int batchSize = context.getProperty(ListenerProperties.MAX_BATCH_SIZE).asInteger();
        Map<String, FlowFileEventBatch<ByteArrayMessage>> batches = getEventBatcher().getBatches(session, batchSize, messageDemarcatorBytes);
        processEvents(session, batches);
    }

    private void processEvents(final ProcessSession session, final Map<String, FlowFileEventBatch<ByteArrayMessage>> batches) {
        for (Map.Entry<String, FlowFileEventBatch<ByteArrayMessage>> entry : batches.entrySet()) {
            FlowFile flowFile = entry.getValue().getFlowFile();
            final List<ByteArrayMessage> events = entry.getValue().getEvents();

            if (flowFile.getSize() == 0L || events.isEmpty()) {
                session.remove(flowFile);
                getLogger().debug("No data written to FlowFile from batch {}; removing FlowFile", entry.getKey());
                continue;
            }

            final Map<String, String> attributes = getAttributes(entry.getValue());
            addClientCertificateAttributes(attributes, events.getFirst());
            flowFile = session.putAllAttributes(flowFile, attributes);

            getLogger().debug("Transferring {} to success", flowFile);
            session.transfer(flowFile, REL_SUCCESS);
            session.adjustCounter("FlowFiles Transferred to Success", 1L, false);

            final String transitUri = getTransitUri(entry.getValue());
            session.getProvenanceReporter().receive(flowFile, transitUri);
        }
    }

    @OnStopped
    public void stopped() {
        if (eventServer != null) {
            eventServer.shutdown();
        }
        eventBatcher = null;
    }

    protected Map<String, String> getAttributes(final FlowFileEventBatch<ByteArrayMessage> batch) {
        final List<ByteArrayMessage> events = batch.getEvents();
        final String sender = events.getFirst().getSender();
        final Map<String, String> attributes = new HashMap<>(3);
        attributes.put("tcp.sender", sender);
        attributes.put("tcp.port", String.valueOf(port));
        return attributes;
    }

    protected String getTransitUri(final FlowFileEventBatch<ByteArrayMessage> batch) {
        final List<ByteArrayMessage> events = batch.getEvents();
        final String sender = events.getFirst().getSender();
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        return String.format("tcp://%s:%d", senderHost, port);
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private String getMessageDemarcator(final ProcessContext context) {
        return context.getProperty(ListenerProperties.MESSAGE_DELIMITER)
                .getValue()
                .replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
    }

    private EventBatcher<ByteArrayMessage> getEventBatcher() {
        if (eventBatcher == null) {
            eventBatcher = new EventBatcher<>(getLogger(), events, errorEvents) {
                @Override
                protected String getBatchKey(ByteArrayMessage event) {
                    return event.getSender();
                }
            };
        }
        return eventBatcher;
    }

    private void addClientCertificateAttributes(final Map<String, String> attributes, final ByteArrayMessage event) {
        final SslSessionStatus sslSessionStatus = event.getSslSessionStatus();
        if (sslSessionStatus != null) {
            attributes.put(CLIENT_CERTIFICATE_SUBJECT_DN_ATTRIBUTE, sslSessionStatus.getSubject().getName());
            attributes.put(CLIENT_CERTIFICATE_ISSUER_DN_ATTRIBUTE, sslSessionStatus.getIssuer().getName());
        }
    }

    private void processTrackingLog() {
        final long now = Instant.now().toEpochMilli();
        if (now > nextTrackingLog.get()) {
            getLogger().debug("Event Queue Capacity [{}] Remaining [{}] Size [{}] Largest Size [{}]",
                    eventsCapacity,
                    events.remainingCapacity(),
                    events.size(),
                    events.getLargestSize()
            );
            final long nextTrackingLogScheduled = now + TRACKING_LOG_INTERVAL;
            nextTrackingLog.getAndSet(nextTrackingLogScheduled);
        }
    }
}