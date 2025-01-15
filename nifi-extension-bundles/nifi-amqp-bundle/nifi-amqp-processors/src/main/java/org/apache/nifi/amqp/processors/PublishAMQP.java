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
package org.apache.nifi.amqp.processors;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Tags({ "amqp", "rabbit", "put", "message", "send", "publish" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Creates an AMQP Message from the contents of a FlowFile and sends the message to an AMQP Exchange. "
    + "In a typical AMQP exchange model, the message that is sent to the AMQP Exchange will be routed based on the 'Routing Key' "
    + "to its final destination in the queue (the binding). If due to some misconfiguration the binding between the Exchange, Routing Key "
    + "and Queue is not set up, the message will have no final destination and will return (i.e., the data will not make it to the queue). If "
    + "that happens you will see a log in both app-log and bulletin stating to that effect, and the FlowFile will be routed to the 'failure' relationship.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@ReadsAttributes({
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_APPID_ATTRIBUTE, description = "The App ID field to set on the AMQP Message"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_CONTENT_ENCODING_ATTRIBUTE, description = "The Content Encoding to set on the AMQP Message"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_CONTENT_TYPE_ATTRIBUTE, description = "The Content Type to set on the AMQP Message"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE, description = "The headers to set on the AMQP Message, if 'Header Source' is set to use it. "
        + "See additional details of the processor."),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_DELIVERY_MODE_ATTRIBUTE, description = "The numeric indicator for the Message's Delivery Mode"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_PRIORITY_ATTRIBUTE, description = "The Message priority"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_CORRELATION_ID_ATTRIBUTE, description = "The Message's Correlation ID"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_REPLY_TO_ATTRIBUTE, description = "The value of the Message's Reply-To field"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_EXPIRATION_ATTRIBUTE, description = "The Message Expiration"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_MESSAGE_ID_ATTRIBUTE, description = "The unique ID of the Message"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_TIMESTAMP_ATTRIBUTE, description = "The timestamp of the Message, as the number of milliseconds since epoch"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_TYPE_ATTRIBUTE, description = "The type of message"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_USER_ID_ATTRIBUTE, description = "The ID of the user"),
    @ReadsAttribute(attribute = AbstractAMQPProcessor.AMQP_CLUSTER_ID_ATTRIBUTE, description = "The ID of the AMQP Cluster"),
})
public class PublishAMQP extends AbstractAMQPProcessor<AMQPPublisher> {

    public static final PropertyDescriptor EXCHANGE = new PropertyDescriptor.Builder()
            .name("Exchange Name")
            .description("The name of the AMQP Exchange the messages will be sent to. Usually provided by the AMQP administrator (e.g., 'amq.direct'). "
                    + "It is an optional property. If kept empty the messages will be sent to a default AMQP exchange.")
            .required(true)
            .defaultValue("")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor ROUTING_KEY = new PropertyDescriptor.Builder()
            .name("Routing Key")
            .description("The name of the Routing Key that will be used by AMQP to route messages from the exchange to a destination queue(s). "
                    + "Usually provided by the administrator (e.g., 'myKey')In the event when messages are sent to a default exchange this property "
                    + "corresponds to a destination queue name, otherwise a binding from the Exchange to a Queue via Routing Key must be set "
                    + "(usually by the AMQP administrator)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor HEADERS_SOURCE = new PropertyDescriptor.Builder()
            .name("Headers Source")
            .description("The source of the headers which will be applied to the published message.")
            .required(true)
            .allowableValues(InputHeaderSource.class)
            .defaultValue(InputHeaderSource.AMQP_HEADERS_ATTRIBUTE)
            .build();
    public static final PropertyDescriptor HEADERS_PATTERN = new PropertyDescriptor.Builder()
            .name("Headers Pattern")
            .description("Regular expression that will be evaluated against the FlowFile attributes to select the matching attributes and put as AMQP headers. "
                + "Attribute name will be used as header key.")
            .required(true)
            .dependsOn(HEADERS_SOURCE, InputHeaderSource.FLOWFILE_ATTRIBUTES)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor HEADER_SEPARATOR = new PropertyDescriptor.Builder()
            .name("header.separator")
            .displayName("Header Separator")
            .description("The character that is used to split key-value for headers. The value must only one character. "
                    + "Otherwise you will get an error message")
            .defaultValue(",")
            .dependsOn(HEADERS_SOURCE, InputHeaderSource.AMQP_HEADERS_ATTRIBUTE)
            .addValidator(StandardValidators.SINGLE_CHAR_VALIDATOR)
            .required(false)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are sent to the AMQP destination are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be routed to the AMQP destination are routed to this relationship")
            .build();

    private final static List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            Stream.of(
                    EXCHANGE,
                    ROUTING_KEY,
                    HEADERS_SOURCE,
                    HEADERS_PATTERN,
                    HEADER_SEPARATOR
            ),
            getCommonPropertyDescriptors().stream()
    ).toList();

    private final static Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    /**
     * Will construct AMQP message by extracting its body from the incoming {@link FlowFile}. AMQP Properties will be extracted from the
     * {@link FlowFile} and converted to {@link BasicProperties} to be sent along with the message. Upon success the incoming {@link FlowFile} is
     * transferred to 'success' {@link Relationship} and upon failure FlowFile is penalized and transferred to the 'failure' {@link Relationship}
     * <br>
     * <p>
     * NOTE: Attributes extracted from {@link FlowFile} are considered candidates for AMQP properties if their names are prefixed with
     * "amqp$" (e.g., amqp$contentType=text/xml). For "amqp$headers" it depends on the value of
     * {@link PublishAMQP#HEADERS_SOURCE}, if the value is {@link InputHeaderSource#FLOWFILE_ATTRIBUTES} then message headers are created from this attribute value,
     * otherwise this attribute will be ignored.
     */
    @Override
    protected void processResource(final Connection connection, final AMQPPublisher publisher, ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String routingKey = context.getProperty(ROUTING_KEY).evaluateAttributeExpressions(flowFile).getValue();
        if (routingKey == null) {
            throw new IllegalArgumentException("Failed to determine 'routing key' with provided value '"
                + context.getProperty(ROUTING_KEY) + "' after evaluating it as expression against incoming FlowFile.");
        }
        InputHeaderSource selectedHeaderSource = context.getProperty(HEADERS_SOURCE).asAllowableValue(InputHeaderSource.class);
        Character headerSeparator = null;
        Pattern pattern = null;
        if (context.getProperty(HEADERS_PATTERN).isSet()) {
            pattern = Pattern.compile(context.getProperty(HEADERS_PATTERN).evaluateAttributeExpressions().getValue());
        }
        if (context.getProperty(HEADER_SEPARATOR).isSet()) {
            headerSeparator = context.getProperty(HEADER_SEPARATOR).getValue().charAt(0);
        }

        final BasicProperties amqpProperties = extractAmqpPropertiesFromFlowFile(flowFile, selectedHeaderSource, headerSeparator, pattern);

        final String exchange = context.getProperty(EXCHANGE).evaluateAttributeExpressions(flowFile).getValue();
        final byte[] messageContent = extractMessage(flowFile, session);

        try {
            publisher.publish(messageContent, amqpProperties, routingKey, exchange);
        } catch (AMQPRollbackException e) {
            session.rollback();
            throw e;
        } catch (AMQPException e) {
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            throw e;
        }

        session.transfer(flowFile, REL_SUCCESS);
        session.getProvenanceReporter().send(flowFile, connection.toString() + "/E:" + exchange + "/RK:" + routingKey);
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected AMQPPublisher createAMQPWorker(final ProcessContext context, final Connection connection) {
        return new AMQPPublisher(connection, getLogger());
    }

    /**
     * Extracts contents of the {@link FlowFile} as byte array.
     */
    private byte[] extractMessage(final FlowFile flowFile, ProcessSession session) {
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, messageContent, true));
        return messageContent;
    }


    /**
     * Reads an attribute from flowFile and pass it to the consumer function
     *
     * @param flowFile        FlowFile for reading the attribute
     * @param attributeKey    Name of the attribute
     * @param updater         Consumer function which will use the attribute value
     */
    private void readAmqpAttribute(final FlowFile flowFile, final String attributeKey, final Consumer<String> updater) {
        final String attributeValue = flowFile.getAttribute(attributeKey);
        if (attributeValue == null) {
            return;
        }

        try {
            updater.accept(attributeValue);
        } catch (final Exception e) {
            getLogger().warn("Failed to update AMQP Message Property [{}]", attributeKey, e);
        }
    }

    /**
     * Extracts AMQP properties from the {@link FlowFile} attributes. Attributes
     * extracted from {@link FlowFile} are considered candidates for AMQP
     * properties if their names are prefixed with "amqp$" (e.g., amqp$contentType=text/xml).
     */
    private BasicProperties extractAmqpPropertiesFromFlowFile(final FlowFile flowFile, final InputHeaderSource selectedHeaderSource, final Character separator, final Pattern pattern) {
        final AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();

        readAmqpAttribute(flowFile, AMQP_CONTENT_TYPE_ATTRIBUTE, builder::contentType);
        readAmqpAttribute(flowFile, AMQP_CONTENT_ENCODING_ATTRIBUTE, builder::contentEncoding);
        readAmqpAttribute(flowFile, AMQP_DELIVERY_MODE_ATTRIBUTE, mode -> builder.deliveryMode(Integer.parseInt(mode)));
        readAmqpAttribute(flowFile, AMQP_PRIORITY_ATTRIBUTE, pri -> builder.priority(Integer.parseInt(pri)));
        readAmqpAttribute(flowFile, AMQP_CORRELATION_ID_ATTRIBUTE, builder::correlationId);
        readAmqpAttribute(flowFile, AMQP_REPLY_TO_ATTRIBUTE, builder::replyTo);
        readAmqpAttribute(flowFile, AMQP_EXPIRATION_ATTRIBUTE, builder::expiration);
        readAmqpAttribute(flowFile, AMQP_MESSAGE_ID_ATTRIBUTE, builder::messageId);
        readAmqpAttribute(flowFile, AMQP_TIMESTAMP_ATTRIBUTE, ts -> builder.timestamp(new Date(Long.parseLong(ts))));
        readAmqpAttribute(flowFile, AMQP_TYPE_ATTRIBUTE, builder::type);
        readAmqpAttribute(flowFile, AMQP_USER_ID_ATTRIBUTE, builder::userId);
        readAmqpAttribute(flowFile, AMQP_APPID_ATTRIBUTE, builder::appId);
        readAmqpAttribute(flowFile, AMQP_CLUSTER_ID_ATTRIBUTE, builder::clusterId);

        Map<String, Object> headers = prepareAMQPHeaders(flowFile, selectedHeaderSource, separator, pattern);
        builder.headers(headers);

        return builder.build();
    }

    /**
     * Extract AMQP headers from incoming {@link FlowFile} based on selected headers source value.
     *
     * @param flowFile used to extract headers
     * @return {@link Map}
     */
    private Map<String, Object> prepareAMQPHeaders(final FlowFile flowFile, final InputHeaderSource selectedHeaderSource, final Character headerSeparator, final Pattern pattern) {
        final Map<String, Object> headers = new HashMap<>();
        if (InputHeaderSource.FLOWFILE_ATTRIBUTES.equals(selectedHeaderSource)) {
                headers.putAll(getMatchedAttributes(flowFile.getAttributes(), pattern));
        } else if (InputHeaderSource.AMQP_HEADERS_ATTRIBUTE.equals(selectedHeaderSource)) {
            readAmqpAttribute(flowFile, AMQP_HEADERS_ATTRIBUTE, value -> headers.putAll(validateAMQPHeaderProperty(value, headerSeparator)));
        }
        return headers;
    }

    /**
     * Matches the pattern to keys of input attributes and output the amqp headers map
     * @param attributes flowFile attributes to scan for match
     * @return Map with entries matching the pattern
     */
    private Map<String, String> getMatchedAttributes(final Map<String, String> attributes, final Pattern pattern) {
        final Map<String, String> headers = new HashMap<>();
        for (Map.Entry<String, String> attributeEntry : attributes.entrySet()) {
            if (pattern.matcher(attributeEntry.getKey()).matches()) {
                headers.put(attributeEntry.getKey(), attributeEntry.getValue());
            }
        }
        return headers;
    }

    /**
     * Will validate if provided amqpPropValue can be converted to a {@link Map}.
     * Should be passed in the format: amqp$headers=key=value
     * @param splitValue is used to split for property
     * @param amqpPropValue the value of the property
     * @return {@link Map} if valid otherwise null
     */
    private Map<String, Object> validateAMQPHeaderProperty(final String amqpPropValue, final Character splitValue) {
        final String[] strEntries = amqpPropValue.split(Pattern.quote(String.valueOf(splitValue)));
        final Map<String, Object> headers = new HashMap<>();
        for (String strEntry : strEntries) {
            final String[] kv = strEntry.split("=", -1); // without using limit, trailing delimiter would be ignored
            if (kv.length == 2) {
                headers.put(kv[0].trim(), kv[1].trim());
            } else if (kv.length == 1) {
                headers.put(kv[0].trim(), null);
            } else {
                getLogger().warn("Malformed key value pair in AMQP header property ({}): {}", amqpPropValue, strEntry);
            }
        }
        return headers;
    }
    public enum InputHeaderSource implements DescribedValue {

        FLOWFILE_ATTRIBUTES("FlowFile Attributes", "Select FlowFile Attributes based on regular expression pattern for event headers. Key of the matching attribute will be used as header key"),
        AMQP_HEADERS_ATTRIBUTE("AMQP Headers Attribute", "Prepare headers from '" + AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE + "' attribute string");
        private final String name;
        private final String description;

        InputHeaderSource(String displayName, String description) {
            this.name = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return name;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }
}
