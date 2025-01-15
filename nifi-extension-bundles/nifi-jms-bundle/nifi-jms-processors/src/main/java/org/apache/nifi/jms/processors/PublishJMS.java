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
package org.apache.nifi.jms.processors;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProvider;
import org.apache.nifi.jms.processors.ioconcept.reader.FlowFileReader;
import org.apache.nifi.jms.processors.ioconcept.reader.FlowFileReaderCallback;
import org.apache.nifi.jms.processors.ioconcept.reader.StateTrackingFlowFileReader;
import org.apache.nifi.jms.processors.ioconcept.reader.record.RecordSupplier;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.stream.io.StreamUtils;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

import jakarta.jms.Destination;
import jakarta.jms.Message;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.nifi.jms.processors.ioconcept.reader.record.ProvenanceEventTemplates.PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE;
import static org.apache.nifi.jms.processors.ioconcept.reader.record.ProvenanceEventTemplates.PROVENANCE_EVENT_DETAILS_ON_RECORDSET_RECOVER;
import static org.apache.nifi.jms.processors.ioconcept.reader.record.ProvenanceEventTemplates.PROVENANCE_EVENT_DETAILS_ON_RECORDSET_SUCCESS;

/**
 * An implementation of JMS Message publishing {@link Processor} which upon each
 * invocation of {@link #onTrigger(ProcessContext, ProcessSession)} method will
 * construct a {@link Message} from the contents of the {@link FlowFile} sending
 * it to the {@link Destination} identified by the
 * {@link AbstractJMSProcessor#DESTINATION} property while transferring the
 * incoming {@link FlowFile} to 'success' {@link Relationship}. If message can
 * not be constructed and/or sent the incoming {@link FlowFile} will be
 * transitioned to 'failure' {@link Relationship}
 */
@Tags({ "jms", "put", "message", "send", "publish" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Creates a JMS Message from the contents of a FlowFile and sends it to a "
        + "JMS Destination (queue or topic) as JMS BytesMessage or TextMessage. "
        + "FlowFile attributes will be added as JMS headers and/or properties to the outgoing JMS message.")
@ReadsAttributes({
        @ReadsAttribute(attribute = JmsHeaders.DELIVERY_MODE, description = "This attribute becomes the JMSDeliveryMode message header. Must be an integer."),
        @ReadsAttribute(attribute = JmsHeaders.EXPIRATION, description = "This attribute becomes the JMSExpiration message header. Must be a long."),
        @ReadsAttribute(attribute = JmsHeaders.PRIORITY, description = "This attribute becomes the JMSPriority message header. Must be an integer."),
        @ReadsAttribute(attribute = JmsHeaders.REDELIVERED, description = "This attribute becomes the JMSRedelivered message header."),
        @ReadsAttribute(attribute = JmsHeaders.TIMESTAMP, description = "This attribute becomes the JMSTimestamp message header. Must be a long."),
        @ReadsAttribute(attribute = JmsHeaders.CORRELATION_ID, description = "This attribute becomes the JMSCorrelationID message header."),
        @ReadsAttribute(attribute = JmsHeaders.TYPE, description = "This attribute becomes the JMSType message header. Must be an integer."),
        @ReadsAttribute(attribute = JmsHeaders.REPLY_TO, description = "This attribute becomes the JMSReplyTo message header. Must be an integer."),
        @ReadsAttribute(attribute = JmsHeaders.DESTINATION, description = "This attribute becomes the JMSDestination message header. Must be an integer."),
        @ReadsAttribute(attribute = "other attributes", description = "All other attributes that do not start with " + JmsHeaders.PREFIX + " are added as message properties."),
        @ReadsAttribute(attribute = "other attributes .type", description = "When an attribute will be added as a message property, a second attribute of the same name but with an extra"
        + " `.type` at the end will cause the message property to be sent using that strong type. For example, attribute `delay` with value `12000` and another attribute"
        + " `delay.type` with value `integer` will cause a JMS message property `delay` to be sent as an Integer rather than a String. Supported types are boolean, byte,"
        + " short, integer, long, float, double, and string (which is the default).")
})
@DynamicProperty(name = "The name of a Connection Factory configuration property.", value = "The value of a given Connection Factory configuration property.",
        description = "Additional configuration property for the Connection Factory. It can be used when the Connection Factory is being configured via the 'JNDI *' or the 'JMS *'" +
                "properties of the processor. For more information, see the Additional Details page.",
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT)
@SeeAlso(value = { ConsumeJMS.class, JMSConnectionFactoryProvider.class })
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Client Library Location can reference resources over HTTP"
                )
        }
)
public class PublishJMS extends AbstractJMSProcessor<JMSPublisher> {

    static final PropertyDescriptor MESSAGE_BODY = new PropertyDescriptor.Builder()
            .name("message-body-type")
            .displayName("Message Body Type")
            .description("The type of JMS message body to construct.")
            .required(true)
            .defaultValue(BYTES_MESSAGE)
            .allowableValues(BYTES_MESSAGE, TEXT_MESSAGE)
            .build();
    static final PropertyDescriptor ALLOW_ILLEGAL_HEADER_CHARS = new PropertyDescriptor.Builder()
            .name("allow-illegal-chars-in-jms-header-names")
            .displayName("Allow Illegal Characters in Header Names")
            .description("Specifies whether illegal characters in header names should be sent to the JMS broker. " +
                    "Usually hyphens and full-stops.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor ATTRIBUTES_AS_HEADERS_REGEX = new PropertyDescriptor.Builder()
            .name("attributes-to-send-as-jms-headers-regex")
            .displayName("Attributes to Send as JMS Headers (Regex)")
            .description("Specifies the Regular Expression that determines the names of FlowFile attributes that" +
                    " should be sent as JMS Headers")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .defaultValue(".*")
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BASE_RECORD_READER)
            .description("The Record Reader to use for parsing the incoming FlowFile into Records.")
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(BASE_RECORD_WRITER)
            .description("The Record Writer to use for serializing Records before publishing them as an JMS Message.")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are sent to the JMS destination are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be sent to JMS destination are routed to this relationship")
            .build();

    private static final List<PropertyDescriptor> COMMON_PROPERTY_DESCRIPTORS = Stream.concat(
            JNDI_JMS_CF_PROPERTIES.stream(),
            JMS_CF_PROPERTIES.stream()
    ).toList();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            Stream.of(
                    CF_SERVICE,
                    DESTINATION,
                    DESTINATION_TYPE,
                    USER,
                    PASSWORD,
                    CLIENT_ID,
                    MESSAGE_BODY,
                    CHARSET,
                    ALLOW_ILLEGAL_HEADER_CHARS,
                    ATTRIBUTES_AS_HEADERS_REGEX,
                    MAX_BATCH_SIZE,
                    RECORD_READER,
                    RECORD_WRITER),
            COMMON_PROPERTY_DESCRIPTORS.stream()
    ).toList();

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    volatile Boolean allowIllegalChars;
    volatile Pattern attributeHeaderPattern;
    volatile RecordReaderFactory readerFactory;
    volatile RecordSetWriterFactory writerFactory;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        allowIllegalChars = context.getProperty(ALLOW_ILLEGAL_HEADER_CHARS).asBoolean();

        final String attributeHeaderRegex = context.getProperty(ATTRIBUTES_AS_HEADERS_REGEX).getValue();
        attributeHeaderPattern = Pattern.compile(attributeHeaderRegex);

        readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
    }

    /**
     * Will construct JMS {@link Message} by extracting its body from the
     * incoming {@link FlowFile}. {@link FlowFile} attributes that represent
     * standard JMS headers will be extracted from the {@link FlowFile} and set
     * as JMS headers on the newly constructed message. For the list of
     * available message headers please see {@link JmsHeaders}. <br>
     * <br>
     * Upon success the incoming {@link FlowFile} is transferred to the 'success'
     * {@link Relationship} and upon failure FlowFile is penalized and
     * transferred to the 'failure' {@link Relationship}
     */
    @Override
    protected void rendezvousWithJms(ProcessContext context, ProcessSession processSession, JMSPublisher publisher) throws ProcessException {
        final List<FlowFile> flowFiles = processSession.get(context.getProperty(MAX_BATCH_SIZE).asInteger());
        if (flowFiles.isEmpty()) {
            return;
        }

        flowFiles.forEach(flowFile -> {
            try {
                final String destinationName = context.getProperty(DESTINATION).evaluateAttributeExpressions(flowFile).getValue();
                final String charset = context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue();

                final Map<String, String> attributesToSend = new HashMap<>();
                // REGEX Attributes
                for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
                    final String key = entry.getKey();
                    if (attributeHeaderPattern.matcher(key).matches()) {
                        if (allowIllegalChars || key.endsWith(".type") || (!key.contains("-") && !key.contains("."))) {
                            attributesToSend.put(key, flowFile.getAttribute(key));
                        }
                    }
                }

                if (context.getProperty(RECORD_READER).isSet()) {
                    final FlowFileReader flowFileReader = new StateTrackingFlowFileReader(
                            getIdentifier(),
                            new RecordSupplier(readerFactory, writerFactory),
                            getLogger()
                    );

                    flowFileReader.read(
                            processSession,
                            flowFile,
                            content -> publisher.publish(destinationName, content, attributesToSend),
                            new FlowFileReaderCallback() {
                                @Override
                                public void onSuccess(FlowFile flowFile, int processedRecords, boolean isRecover, long transmissionMillis) {
                                    final String eventTemplate = isRecover ? PROVENANCE_EVENT_DETAILS_ON_RECORDSET_RECOVER : PROVENANCE_EVENT_DETAILS_ON_RECORDSET_SUCCESS;
                                    processSession.getProvenanceReporter().send(
                                            flowFile,
                                            destinationName,
                                            String.format(eventTemplate, processedRecords),
                                            transmissionMillis);

                                    processSession.transfer(flowFile, REL_SUCCESS);
                                }

                                @Override
                                public void onFailure(FlowFile flowFile, int processedRecords, long transmissionMillis, Exception e) {
                                    processSession.getProvenanceReporter().send(
                                            flowFile,
                                            destinationName,
                                            String.format(PROVENANCE_EVENT_DETAILS_ON_RECORDSET_FAILURE, processedRecords),
                                            transmissionMillis);

                                    handleException(context, processSession, publisher, flowFile, e);
                                }
                            }
                    );
                } else {
                    processStandardFlowFile(context, processSession, publisher, flowFile, destinationName, charset, attributesToSend);
                    processSession.transfer(flowFile, REL_SUCCESS);
                    processSession.getProvenanceReporter().send(flowFile, destinationName);
                }
            } catch (Exception e) {
                handleException(context, processSession, publisher, flowFile, e);
            }
        });
    }

    private void handleException(ProcessContext context, ProcessSession processSession, JMSPublisher publisher, FlowFile flowFile, Exception e) {
        processSession.transfer(flowFile, REL_FAILURE);
        this.getLogger().error("Failed while sending message to JMS via {}", publisher, e);
        context.yield();
        publisher.setValid(false);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     *
     */
    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * Will create an instance of {@link JMSPublisher}
     */
    @Override
    protected JMSPublisher finishBuildingJmsWorker(CachingConnectionFactory connectionFactory, JmsTemplate jmsTemplate, ProcessContext processContext) {
        return new JMSPublisher(connectionFactory, jmsTemplate, this.getLogger());
    }

    private void processStandardFlowFile(ProcessContext context, ProcessSession processSession, JMSPublisher publisher, FlowFile flowFile,
                                         String destinationName, String charset, Map<String, String> attributesToSend) {
        publishMessage(context, processSession, publisher, flowFile, destinationName, charset, attributesToSend);
    }

    private void publishMessage(ProcessContext context, ProcessSession processSession, JMSPublisher publisher, FlowFile flowFile,
                                String destinationName, String charset, Map<String, String> attributesToSend) {
        switch (context.getProperty(MESSAGE_BODY).getValue()) {
            case TEXT_MESSAGE:
                try {
                    publisher.publish(destinationName, this.extractTextMessageBody(flowFile, processSession, charset), attributesToSend);
                } catch (Exception e) {
                    publisher.setValid(false);
                    throw e;
                }
                break;
            case BYTES_MESSAGE:
            default:
                try {
                    publisher.publish(destinationName, this.extractMessageBody(flowFile, processSession), attributesToSend);
                } catch (Exception e) {
                    publisher.setValid(false);
                    throw e;
                }
                break;
        }
    }

    /**
     * Extracts contents of the {@link FlowFile} as byte array.
     */
    private byte[] extractMessageBody(FlowFile flowFile, ProcessSession session) {
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, messageContent, true));
        return messageContent;
    }

    private String extractTextMessageBody(FlowFile flowFile, ProcessSession session, String charset) {
        final StringWriter writer = new StringWriter();
        session.read(flowFile, in -> IOUtils.copy(in, writer, Charset.forName(charset)));
        return writer.toString();
    }

}
