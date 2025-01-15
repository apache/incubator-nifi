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
package org.apache.nifi.record.sink;

import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Tags({"email", "smtp", "record", "sink", "send", "write"})
@CapabilityDescription("Provides a RecordSinkService that can be used to send records in email using the specified writer for formatting.")
public class EmailRecordSink extends AbstractControllerService implements RecordSinkService {

    private static final String RFC822 = "Comma separated sequence of addresses following RFC822 syntax.";

    public static final PropertyDescriptor FROM = new PropertyDescriptor.Builder()
            .name("from")
            .displayName("From")
            .description("Specifies the Email address to use as the sender. " + RFC822)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor TO = new PropertyDescriptor.Builder()
            .name("to")
            .displayName("To")
            .description("The recipients to include in the To-Line of the email. " + RFC822)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CC = new PropertyDescriptor.Builder()
            .name("cc")
            .displayName("CC")
            .description("The recipients to include in the CC-Line of the email. " + RFC822)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor BCC = new PropertyDescriptor.Builder()
            .name("bcc")
            .displayName("BCC")
            .description("The recipients to include in the BCC-Line of the email. " + RFC822)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SUBJECT = new PropertyDescriptor.Builder()
            .name("subject")
            .displayName("Subject")
            .description("The email subject")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("Message from NiFi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SMTP_HOSTNAME = new PropertyDescriptor.Builder()
            .name("smtp-hostname")
            .displayName("SMTP Hostname")
            .description("The hostname of the SMTP Server that is used to send Email Notifications")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor SMTP_PORT = new PropertyDescriptor.Builder()
            .name("smtp-port")
            .displayName("SMTP Port")
            .description("The Port used for SMTP communications")
            .required(true)
            .defaultValue("25")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor SMTP_AUTH = new PropertyDescriptor.Builder()
            .name("smtp-auth")
            .displayName("SMTP Auth")
            .description("Flag indicating whether authentication should be used")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor SMTP_USERNAME = new PropertyDescriptor.Builder()
            .name("smtp-username")
            .displayName("SMTP Username")
            .description("Username for the SMTP account")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .dependsOn(SMTP_AUTH, "true")
            .build();
    public static final PropertyDescriptor SMTP_PASSWORD = new PropertyDescriptor.Builder()
            .name("smtp-password")
            .displayName("SMTP Password")
            .description("Password for the SMTP account")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .dependsOn(SMTP_AUTH, "true")
            .build();
    public static final PropertyDescriptor SMTP_STARTTLS = new PropertyDescriptor.Builder()
            .name("smtp-starttls")
            .displayName("SMTP STARTTLS")
            .description("Flag indicating whether STARTTLS should be enabled. "
                    + "If the server does not support STARTTLS, the connection continues without the use of TLS")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor SMTP_SSL = new PropertyDescriptor.Builder()
            .name("smtp-ssl")
            .displayName("SMTP SSL")
            .description("Flag indicating whether SSL should be enabled")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor HEADER_XMAILER = new PropertyDescriptor.Builder()
            .name("smtp-xmailer-header")
            .displayName("SMTP X-Mailer Header")
            .description("X-Mailer used in the header of the outgoing email")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("NiFi")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FROM,
            TO,
            CC,
            BCC,
            SUBJECT,
            SMTP_HOSTNAME,
            SMTP_PORT,
            SMTP_AUTH,
            SMTP_USERNAME,
            SMTP_PASSWORD,
            SMTP_STARTTLS,
            SMTP_SSL,
            HEADER_XMAILER,
            RecordSinkService.RECORD_WRITER_FACTORY
    );

    private volatile RecordSetWriterFactory writerFactory;

    /**
     * Mapping of the mail properties to the NiFi PropertyDescriptors that will be evaluated at runtime
     */
    private static final Map<String, PropertyDescriptor> propertyToContext = new HashMap<>();

    static {
        propertyToContext.put("mail.smtp.host", SMTP_HOSTNAME);
        propertyToContext.put("mail.smtp.port", SMTP_PORT);
        propertyToContext.put("mail.smtps.port", SMTP_PORT);
        propertyToContext.put("mail.smtp.socketFactory.port", SMTP_PORT);
        propertyToContext.put("mail.smtp.ssl.enable", SMTP_SSL);
        propertyToContext.put("mail.smtp.auth", SMTP_AUTH);
        propertyToContext.put("mail.smtp.starttls.enable", SMTP_STARTTLS);
        propertyToContext.put("mail.smtp.user", SMTP_USERNAME);
        propertyToContext.put("mail.smtp.password", SMTP_PASSWORD);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> errors = new ArrayList<>(super.customValidate(context));

        final String to = context.getProperty(TO).getValue();
        final String cc = context.getProperty(CC).getValue();
        final String bcc = context.getProperty(BCC).getValue();

        if (to == null && cc == null && bcc == null) {
            errors.add(new ValidationResult.Builder().subject("To, CC, BCC").valid(false).explanation("Must specify at least one To/CC/BCC address").build());
        }

        return errors;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        writerFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
    }

    @Override
    public WriteResult sendData(final RecordSet recordSet, final Map<String, String> attributes, final boolean sendZeroResults) throws IOException {
        WriteResult writeResult;
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), recordSet.getSchema(), out, attributes)) {
                writer.beginRecordSet();
                Record r;
                while ((r = recordSet.next()) != null) {
                    writer.write(r);
                    writer.flush();
                }
                writeResult = writer.finishRecordSet();
                writer.flush();
                sendMessage(getConfigurationContext(), out.toString());
            }
        } catch (SchemaNotFoundException e) {
            final String errorMessage = String.format("RecordSetWriter could not be created because the schema was not found. The schema name for the RecordSet to write is %s",
                    recordSet.getSchema().getSchemaName());
            throw new ProcessException(errorMessage, e);
        }
        return writeResult;
    }

    /**
     * Wrapper for static method {@link Transport#send(Message)} to add testability of this class.
     *
     * @param msg the message to send
     * @throws MessagingException on error
     */
    protected void send(final Message msg) throws MessagingException {
        Transport.send(msg);
    }

    private void sendMessage(final ConfigurationContext context, final String messageText) {
        final Properties properties = getMailProperties(context);
        final Session mailSession = createMailSession(properties);
        final Message message = new MimeMessage(mailSession);

        try {
            message.setFrom(InternetAddress.parse(context.getProperty(FROM).evaluateAttributeExpressions().getValue())[0]);

            final InternetAddress[] toAddresses = toInternetAddresses(context, TO);
            message.setRecipients(Message.RecipientType.TO, toAddresses);

            final InternetAddress[] ccAddresses = toInternetAddresses(context, CC);
            message.setRecipients(Message.RecipientType.CC, ccAddresses);

            final InternetAddress[] bccAddresses = toInternetAddresses(context, BCC);
            message.setRecipients(Message.RecipientType.BCC, bccAddresses);

            message.setHeader("X-Mailer", context.getProperty(HEADER_XMAILER).evaluateAttributeExpressions().getValue());
            message.setSubject(context.getProperty(SUBJECT).evaluateAttributeExpressions().getValue());

            message.setContent(messageText, "text/plain");
            message.setSentDate(new Date());

            send(message);
        } catch (final ProcessException | MessagingException e) {
            final String errorMessage = String.format("Send Failed using SMTP Host [%s] Port [%s]", properties.get("mail.smtp.host"), properties.get("mail.smtp.port"));
            throw new RuntimeException(errorMessage, e);
        }
    }

    /**
     * Uses the mapping of jakarta.mail properties to NiFi PropertyDescriptors to build the required Properties object to be used for sending this email
     *
     * @param context context
     * @return mail properties
     */
    private Properties getMailProperties(final ConfigurationContext context) {
        final Properties properties = new Properties();

        for (Map.Entry<String, PropertyDescriptor> entry : propertyToContext.entrySet()) {
            // Evaluate the property descriptor against the env/sys variable registry
            String property = entry.getKey();
            String propValue = context.getProperty(entry.getValue()).evaluateAttributeExpressions().getValue();

            // Nullable values are not allowed, so filter out
            if (StringUtils.isNotBlank(propValue)) {
                properties.setProperty(property, propValue);
            }
        }

        return properties;
    }

    /**
     * Based on the input properties, determine whether an authenticate or unauthenticated session should be used. If authenticated, creates a Password Authenticator for use in sending the email.
     *
     * @param properties mail properties
     * @return session
     */
    private Session createMailSession(final Properties properties) {
        final boolean auth = Boolean.parseBoolean(properties.getProperty("mail.smtp.auth"));
        return auth ? Session.getInstance(properties, new Authenticator() {
            @Override
            public PasswordAuthentication getPasswordAuthentication() {
                final String username = properties.getProperty("mail.smtp.user");
                final String password = properties.getProperty("mail.smtp.password");
                return new PasswordAuthentication(username, password);
            }
        }) : Session.getInstance(properties); // without auth
    }

    /**
     * @param context            the current context
     * @param propertyDescriptor the property to evaluate
     * @return an InternetAddress[] parsed from the supplied property
     * @throws AddressException if the property cannot be parsed to a valid InternetAddress[]
     */
    private InternetAddress[] toInternetAddresses(final ConfigurationContext context, PropertyDescriptor propertyDescriptor) throws AddressException {
        InternetAddress[] parse;
        final String value = context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue();
        if (value == null || value.isEmpty()) {
            if (propertyDescriptor.isRequired()) {
                final String exceptionMsg = "Required property '" + propertyDescriptor.getDisplayName() + "' evaluates to an empty string.";
                throw new AddressException(exceptionMsg);
            } else {
                parse = new InternetAddress[0];
            }
        } else {
            try {
                parse = InternetAddress.parse(value);
            } catch (AddressException e) {
                final String exceptionMsg = "Unable to parse a valid address for property '" + propertyDescriptor.getDisplayName() + "' with value '" + value + "'";
                throw new AddressException(exceptionMsg);
            }
        }
        return parse;
    }
}
