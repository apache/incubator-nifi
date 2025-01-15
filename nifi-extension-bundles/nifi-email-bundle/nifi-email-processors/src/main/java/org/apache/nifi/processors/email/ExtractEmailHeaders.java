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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import jakarta.mail.Address;
import jakarta.mail.BodyPart;
import jakarta.mail.Header;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.Session;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimePart;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

@SupportsBatching
@SideEffectFree
@Tags({"split", "email"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Using the flowfile content as source of data, extract header from an RFC compliant  email file adding the relevant attributes to the flowfile. " +
        "This processor does not perform extensive RFC validation but still requires a bare minimum compliance with RFC 2822")
@WritesAttributes({
        @WritesAttribute(attribute = "email.headers.bcc.*", description = "Each individual BCC recipient (if available)"),
        @WritesAttribute(attribute = "email.headers.cc.*", description = "Each individual CC recipient (if available)"),
        @WritesAttribute(attribute = "email.headers.from.*", description = "Each individual mailbox contained in the From  of the Email (array as per RFC-2822)"),
        @WritesAttribute(attribute = "email.headers.message-id", description = "The value of the Message-ID header (if available)"),
        @WritesAttribute(attribute = "email.headers.received_date", description = "The Received-Date of the message (if available)"),
        @WritesAttribute(attribute = "email.headers.sent_date", description = "Date the message was sent"),
        @WritesAttribute(attribute = "email.headers.subject", description = "Subject of the message (if available)"),
        @WritesAttribute(attribute = "email.headers.to.*", description = "Each individual TO recipient (if available)"),
        @WritesAttribute(attribute = "email.attachment_count", description = "Number of attachments of the message" )})

public class ExtractEmailHeaders extends AbstractProcessor {
    public static final String EMAIL_HEADER_BCC = "email.headers.bcc";
    public static final String EMAIL_HEADER_CC = "email.headers.cc";
    public static final String EMAIL_HEADER_FROM = "email.headers.from";
    public static final String EMAIL_HEADER_MESSAGE_ID = "email.headers.message-id";
    public static final String EMAIL_HEADER_RECV_DATE = "email.headers.received_date";
    public static final String EMAIL_HEADER_SENT_DATE = "email.headers.sent_date";
    public static final String EMAIL_HEADER_SUBJECT = "email.headers.subject";
    public static final String EMAIL_HEADER_TO = "email.headers.to";
    public static final String EMAIL_ATTACHMENT_COUNT = "email.attachment_count";

    public static final PropertyDescriptor CAPTURED_HEADERS = new PropertyDescriptor.Builder()
            .name("CAPTURED_HEADERS")
            .displayName("Additional Header List")
            .description("COLON separated list of additional headers to be extracted from the flowfile content." +
                    "NOTE the header key is case insensitive and will be matched as lower-case." +
                    " Values will respect email contents.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("x-mailer")
            .build();

    private static final AllowableValue STRICT_ADDRESSING = new AllowableValue("true", "Strict Address Parsing",
        "Strict email address format will be enforced. FlowFiles will be transfered to the failure relationship if the email address is invalid.");
    private static final AllowableValue NONSTRICT_ADDRESSING = new AllowableValue("false", "Non-Strict Address Parsing",
        "Accept emails, even if the address is poorly formed and doesn't strictly comply with RFC Validation.");
    public static final PropertyDescriptor STRICT_PARSING = new PropertyDescriptor.Builder()
            .name("STRICT_ADDRESS_PARSING")
            .displayName("Email Address Parsing")
            .description("If \"strict\", strict address format parsing rules are applied to mailbox and mailbox list fields, " +
                    "such as \"to\" and \"from\" headers, and FlowFiles with poorly formed addresses will be routed " +
                    "to the failure relationship, similar to messages that fail RFC compliant format validation. " +
                    "If \"non-strict\", the processor will extract the contents of mailbox list headers as comma-separated " +
                    "values without attempting to parse each value as well-formed Internet mailbox addresses. " +
                    "This is optional and defaults to " + STRICT_ADDRESSING.getDisplayName())
            .required(false)
            .defaultValue(STRICT_ADDRESSING.getValue())
            .allowableValues(STRICT_ADDRESSING, NONSTRICT_ADDRESSING)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Extraction was successful")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Flowfiles that could not be parsed as a RFC-2822 compliant message")
            .build();

    private static final String ATTACHMENT_DISPOSITION = "attachment";

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CAPTURED_HEADERS,
            STRICT_PARSING
    );

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final ComponentLog logger = getLogger();

        final List<FlowFile> invalidFlowFilesList = new ArrayList<>();
        final List<FlowFile> processedFlowFilesList = new ArrayList<>();

        final FlowFile originalFlowFile = session.get();
        if (originalFlowFile == null) {
            return;
        }

        final String requireStrictAddresses = context.getProperty(STRICT_PARSING).getValue();
        final List<String> capturedHeadersList = Arrays.asList(context.getProperty(CAPTURED_HEADERS).getValue().toLowerCase().split(":"));

        final Map<String, String> attributes = new HashMap<>();
        session.read(originalFlowFile, rawIn -> {
            try (final InputStream in = new BufferedInputStream(rawIn)) {
                Properties props = new Properties();
                props.put("mail.mime.address.strict", requireStrictAddresses);
                Session mailSession = Session.getInstance(props);
                MimeMessage originalMessage = new MimeMessage(mailSession, in);
                // RFC-2822 determines that a message must have a "From:" header
                // if a message lacks the field, it is flagged as invalid
                Address[] from = originalMessage.getFrom();
                if (from == null) {
                    throw new MessagingException("Message failed RFC-2822 validation: No Sender");
                }
                if (!capturedHeadersList.isEmpty()) {
                    Enumeration headers = originalMessage.getAllHeaders();
                    while (headers.hasMoreElements()) {
                        final Header header = (Header) headers.nextElement();
                        final String headerValue = header.getValue();
                        if (headerValue != null && !headerValue.isBlank()
                                && capturedHeadersList.contains(header.getName().toLowerCase())) {
                            attributes.put("email.headers." + header.getName().toLowerCase(), header.getValue());
                        }
                    }
                }

                putAddressListInAttributes(attributes, EMAIL_HEADER_TO, originalMessage.getRecipients(Message.RecipientType.TO));
                putAddressListInAttributes(attributes, EMAIL_HEADER_CC, originalMessage.getRecipients(Message.RecipientType.CC));
                putAddressListInAttributes(attributes, EMAIL_HEADER_BCC, originalMessage.getRecipients(Message.RecipientType.BCC));
                putAddressListInAttributes(attributes, EMAIL_HEADER_FROM, originalMessage.getFrom()); // RFC-2822 specifies "From" as mailbox-list

                final String messageId = originalMessage.getMessageID();
                if (messageId != null && !messageId.isEmpty()) {
                    attributes.put(EMAIL_HEADER_MESSAGE_ID, originalMessage.getMessageID());
                }
                if (originalMessage.getReceivedDate() != null) {
                    attributes.put(EMAIL_HEADER_RECV_DATE, originalMessage.getReceivedDate().toString());
                }
                if (originalMessage.getSentDate() != null) {
                    attributes.put(EMAIL_HEADER_SENT_DATE, originalMessage.getSentDate().toString());
                }
                final String subject = originalMessage.getSubject();
                if (subject != null && !subject.isEmpty()) {
                    attributes.put(EMAIL_HEADER_SUBJECT, subject);
                }

                final AtomicInteger attachmentsCounter = new AtomicInteger();
                countAttachments(attachmentsCounter, originalMessage, 0);
                attributes.put(EMAIL_ATTACHMENT_COUNT, Integer.toString(attachmentsCounter.get()));
            } catch (Exception e) {
                // Message is invalid or triggered an error during parsing
                attributes.clear();
                logger.error("Could not parse the flowfile {} as an email, treating as failure", originalFlowFile, e);
                invalidFlowFilesList.add(originalFlowFile);
            }
        });

        if (!attributes.isEmpty()) {
            FlowFile updatedFlowFile = session.putAllAttributes(originalFlowFile, attributes);
            logger.info("Extracted {} headers into {} file", attributes.size(), updatedFlowFile);
            processedFlowFilesList.add(updatedFlowFile);
        }

        session.transfer(processedFlowFilesList, REL_SUCCESS);
        session.transfer(invalidFlowFilesList, REL_FAILURE);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private static void putAddressListInAttributes(
            Map<String, String> attributes,
            final String attributePrefix,
            Address[] addresses) {
        if (addresses != null) {
            for (int count = 0; count < addresses.length; count++) {
                attributes.put(attributePrefix + "." + count, addresses[count].toString());
            }
        }
    }

    private void countAttachments(final AtomicInteger counter, final MimePart parentPart, final int depth) throws MessagingException, IOException {
        final String disposition = parentPart.getDisposition();

        final Object parentContent = parentPart.getContent();
        if (parentContent instanceof Multipart multipart) {
            final int count = multipart.getCount();

            final int partDepth = depth + 1;
            for (int i = 0; i < count; i++) {
                final BodyPart bodyPart = multipart.getBodyPart(i);
                if (bodyPart instanceof MimeBodyPart mimeBodyPart) {
                    countAttachments(counter, mimeBodyPart, partDepth);
                }
            }
        } else if (ATTACHMENT_DISPOSITION.equalsIgnoreCase(disposition) || depth > 0) {
            counter.getAndIncrement();
        }
    }
}
