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
package org.apache.nifi.processors.zendesk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.common.zendesk.validation.JsonPointerPropertyNameValidator;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.common.zendesk.ZendeskProperties.APPLICATION_JSON;
import static org.apache.nifi.common.zendesk.ZendeskProperties.WEB_CLIENT_SERVICE_PROVIDER;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_CREDENTIAL;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_AUTHENTICATION_TYPE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_CREATE_TICKETS_RESOURCE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_CREATE_TICKET_RESOURCE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_SUBDOMAIN;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKET_COMMENT_BODY;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKET_PRIORITY;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKET_SUBJECT;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_TICKET_TYPE;
import static org.apache.nifi.common.zendesk.ZendeskProperties.ZENDESK_USER;
import static org.apache.nifi.common.zendesk.util.ZendeskRecordPathUtils.addDynamicField;
import static org.apache.nifi.common.zendesk.util.ZendeskRecordPathUtils.addField;
import static org.apache.nifi.common.zendesk.util.ZendeskUtils.createRequestObject;
import static org.apache.nifi.common.zendesk.util.ZendeskUtils.getDynamicProperties;
import static org.apache.nifi.common.zendesk.util.ZendeskUtils.getResponseBody;
import static org.apache.nifi.flowfile.attributes.CoreAttributes.MIME_TYPE;
import static org.apache.nifi.processors.zendesk.AbstractZendesk.RECORD_COUNT_ATTRIBUTE_NAME;
import static org.apache.nifi.processors.zendesk.PutZendeskTicket.ERROR_CODE_ATTRIBUTE_NAME;
import static org.apache.nifi.processors.zendesk.PutZendeskTicket.ERROR_MESSAGE_ATTRIBUTE_NAME;
import static org.apache.nifi.web.client.api.HttpResponseStatus.CREATED;
import static org.apache.nifi.web.client.api.HttpResponseStatus.OK;

@Tags({"zendesk, ticket"})
@CapabilityDescription("Create Zendesk tickets using the Zendesk API.")
@DynamicProperty(
        name = "The path in the request object to add. The value needs be a valid JsonPointer.",
        value = "The path in the incoming record to get the value from.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Additional property to be added to the Zendesk request object.")
@WritesAttributes({
        @WritesAttribute(attribute = RECORD_COUNT_ATTRIBUTE_NAME, description = "The number of records processed."),
        @WritesAttribute(attribute = ERROR_CODE_ATTRIBUTE_NAME, description = "The error code of from the response."),
        @WritesAttribute(attribute = ERROR_MESSAGE_ATTRIBUTE_NAME, description = "The error message of from the response.")})
public class PutZendeskTicket extends AbstractZendesk {

    static final String ZENDESK_RECORD_READER_NAME = "zendesk-record-reader";
    static final String ERROR_CODE_ATTRIBUTE_NAME = "error.code";
    static final String ERROR_MESSAGE_ATTRIBUTE_NAME = "error.message";

    private static final ObjectMapper mapper = new ObjectMapper();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name(ZENDESK_RECORD_READER_NAME)
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    static final PropertyDescriptor TICKET_COMMENT_BODY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ZENDESK_TICKET_COMMENT_BODY)
            .dependsOn(RECORD_READER)
            .build();

    static final PropertyDescriptor TICKET_SUBJECT = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ZENDESK_TICKET_SUBJECT)
            .dependsOn(RECORD_READER)
            .build();

    static final PropertyDescriptor TICKET_PRIORITY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ZENDESK_TICKET_PRIORITY)
            .dependsOn(RECORD_READER)
            .build();

    static final PropertyDescriptor TICKET_TYPE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ZENDESK_TICKET_TYPE)
            .dependsOn(RECORD_READER)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            WEB_CLIENT_SERVICE_PROVIDER,
            ZENDESK_SUBDOMAIN,
            ZENDESK_USER,
            ZENDESK_AUTHENTICATION_TYPE,
            ZENDESK_AUTHENTICATION_CREDENTIAL,
            RECORD_READER,
            TICKET_COMMENT_BODY,
            TICKET_SUBJECT,
            TICKET_PRIORITY,
            TICKET_TYPE
    );

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the operation failed and retrying the operation will also fail, such as an invalid data or schema.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(new JsonPointerPropertyNameValidator())
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        long startNanos = System.nanoTime();
        HttpResponseEntity response;
        URI uri;
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        if (readerFactory == null) {
            try (final InputStream inputStream = session.read(flowFile)) {
                if (inputStream.available() == 0) {
                    inputStream.close();
                    getLogger().error("The incoming FlowFile's content is empty");
                    session.transfer(session.penalize(flowFile), REL_FAILURE);
                    return;
                }
                final HttpUriBuilder uriBuilder = uriBuilder(ZENDESK_CREATE_TICKET_RESOURCE);
                uri = uriBuilder.build();
                response = zendeskClient.performPostRequest(uri, inputStream);
            } catch (IOException e) {
                getLogger().error("Could not read the incoming FlowFile", e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }
        } else {
            final String commentBody = context.getProperty(TICKET_COMMENT_BODY).evaluateAttributeExpressions().getValue();
            final String subject = context.getProperty(TICKET_SUBJECT).evaluateAttributeExpressions().getValue();
            final String priority = context.getProperty(TICKET_PRIORITY).evaluateAttributeExpressions().getValue();
            final String type = context.getProperty(TICKET_TYPE).evaluateAttributeExpressions().getValue();
            final Map<String, String> dynamicProperties = getDynamicProperties(context, context.getProperties(), flowFile.getAttributes());
            List<ObjectNode> zendeskTickets = new ArrayList<>();

            try (final InputStream in = session.read(flowFile); final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
                Record record;

                while ((record = reader.nextRecord()) != null) {
                    ObjectNode baseTicketNode = mapper.createObjectNode();

                    addField("/comment/body", commentBody, baseTicketNode, record);
                    addField("/subject", subject, baseTicketNode, record);
                    addField("/priority", priority, baseTicketNode, record);
                    addField("/type", type, baseTicketNode, record);

                    for (Map.Entry<String, String> dynamicProperty : dynamicProperties.entrySet()) {
                        addDynamicField(dynamicProperty.getKey(), dynamicProperty.getValue(), baseTicketNode, record);
                    }
                    zendeskTickets.add(baseTicketNode);
                }

            } catch (IOException | SchemaNotFoundException | MalformedRecordException e) {
                getLogger().error("Error occurred while creating Zendesk tickets", e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }

            if (zendeskTickets.isEmpty()) {
                getLogger().info("No records found in the incoming FlowFile");
                flowFile = session.putAttribute(flowFile, RECORD_COUNT_ATTRIBUTE_NAME, "0");
                session.transfer(flowFile, REL_SUCCESS);
                return;
            }

            try {
                final InputStream inputStream = createRequestObject(zendeskTickets);
                uri = createUri(zendeskTickets.size());
                response = zendeskClient.performPostRequest(uri, inputStream);
                flowFile = session.putAttribute(flowFile, RECORD_COUNT_ATTRIBUTE_NAME, String.valueOf(zendeskTickets.size()));
            } catch (IOException e) {
                getLogger().error("Failed to post request to Zendesk", e);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }
        }

        handleResponse(session, flowFile, response, uri, startNanos);
    }

    private void handleResponse(ProcessSession session, FlowFile flowFile, HttpResponseEntity response, URI uri, long startNanos) {
        if (response.statusCode() == CREATED.getCode() || response.statusCode() == OK.getCode()) {
            flowFile = session.putAttribute(flowFile, MIME_TYPE.key(), APPLICATION_JSON);
            session.transfer(flowFile, REL_SUCCESS);
            long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, uri.toString(), transferMillis);
        } else {
            String errorMessage = getResponseBody(response);
            getLogger().error("Zendesk ticket creation returned with error, HTTP status={}, response={}", response.statusCode(), errorMessage);
            flowFile = session.putAttribute(flowFile, ERROR_CODE_ATTRIBUTE_NAME, String.valueOf(response.statusCode()));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE_NAME, errorMessage);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }
    }

    private URI createUri(int numberOfTickets) {
        final String resource = numberOfTickets > 1 ? ZENDESK_CREATE_TICKETS_RESOURCE : ZENDESK_CREATE_TICKET_RESOURCE;
        return uriBuilder(resource).build();
    }
}
