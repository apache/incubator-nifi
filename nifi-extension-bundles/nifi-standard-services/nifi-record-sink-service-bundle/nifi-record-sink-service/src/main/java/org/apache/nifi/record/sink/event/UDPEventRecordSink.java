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
package org.apache.nifi.record.sink.event;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.event.transport.EventSender;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.ByteArrayNettyEventSenderFactory;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Record Sink Service implementation writes Records and sends a serialized Record to a UDP destination
 */
@Tags({"UDP", "event", "record", "sink"})
@CapabilityDescription("Format and send Records as UDP Datagram Packets to a configurable destination")
public class UDPEventRecordSink extends AbstractControllerService implements RecordSinkService {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("hostname")
            .displayName("Hostname")
            .description("Destination hostname or IP address")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("port")
            .displayName("Port")
            .description("Destination port number")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SENDER_THREADS = new PropertyDescriptor.Builder()
            .name("sender-threads")
            .displayName("Sender Threads")
            .description("Number of worker threads allocated for handling socket communication")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("2")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        HOSTNAME,
        PORT,
        RECORD_WRITER_FACTORY,
        SENDER_THREADS
    );

    private static final String TRANSIT_URI_ATTRIBUTE_KEY = "record.sink.url";

    private static final String TRANSIT_URI_FORMAT = "udp://%s:%d";

    private RecordSetWriterFactory writerFactory;

    private EventSender<byte[]> eventSender;

    private String transitUri;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        writerFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        eventSender = getEventSender(context);
    }

    @OnDisabled
    public void onDisabled() throws Exception {
        if (eventSender == null) {
            getLogger().debug("Event Sender not configured");
        } else {
            eventSender.close();
        }
    }

    /**
     * Send Records to Event Sender serializes each Record as a Record Set of one to a byte array for transmission
     *
     * @param recordSet Set of Records to be transmitted
     * @param attributes FlowFile attributes
     * @param sendZeroResults Whether to transmit empty record sets
     * @return Write Result indicating records transmitted
     * @throws IOException Thrown on transmission failures
     */
    @Override
    public WriteResult sendData(final RecordSet recordSet, final Map<String, String> attributes, boolean sendZeroResults) throws IOException {
        final Map<String, String> writeAttributes = new LinkedHashMap<>(attributes);
        writeAttributes.put(TRANSIT_URI_ATTRIBUTE_KEY, transitUri);
        int recordCount = 0;

        try (
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                final RecordSetWriter writer = writerFactory.createWriter(getLogger(), recordSet.getSchema(), outputStream, writeAttributes)
        ) {
            Record record;
            while ((record = recordSet.next()) != null) {
                final WriteResult writeResult = writer.write(record);
                writer.flush();
                sendRecord(outputStream);
                recordCount += writeResult.getRecordCount();
            }
        } catch (final SchemaNotFoundException e) {
            throw new IOException("Record Schema not found", e);
        } catch (final IOException | RuntimeException e) {
            throw new IOException(String.format("Record [%d] Destination [%s] Transmission failed", recordCount, transitUri), e);
        }

        return WriteResult.of(recordCount, writeAttributes);
    }

    /**
     * Send record and reset stream for subsequent records
     *
     * @param outputStream Byte Array Output Stream containing serialized record
     */
    private void sendRecord(final ByteArrayOutputStream outputStream) {
        final byte[] bytes = outputStream.toByteArray();
        eventSender.sendEvent(bytes);
        outputStream.reset();
    }

    private EventSender<byte[]> getEventSender(final ConfigurationContext context) {
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        transitUri = String.format(TRANSIT_URI_FORMAT, hostname, port);

        final ByteArrayNettyEventSenderFactory factory = new ByteArrayNettyEventSenderFactory(getLogger(), hostname, port, TransportProtocol.UDP);
        factory.setShutdownQuietPeriod(Duration.ZERO);
        factory.setShutdownTimeout(Duration.ZERO);
        factory.setThreadNamePrefix(String.format("%s[%s]", getClass().getSimpleName(), getIdentifier()));

        final int senderThreads = context.getProperty(SENDER_THREADS).evaluateAttributeExpressions().asInteger();
        factory.setWorkerThreads(senderThreads);

        return factory.getEventSender();
    }
}
