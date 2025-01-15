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

import jakarta.activation.DataHandler;
import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.Message.RecipientType;
import jakarta.mail.MessagingException;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.internet.MimeUtility;
import jakarta.mail.internet.PreencodedMimeBodyPart;
import jakarta.mail.util.ByteArrayDataSource;
import org.apache.commons.codec.binary.Base64;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SupportsBatching
@Tags({"email", "put", "notify", "smtp"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sends an e-mail to configured recipients for each incoming FlowFile")
@SupportsSensitiveDynamicProperties
@DynamicProperty(name = "mail.propertyName",
        value = "Value for a specific property to be set in the JavaMail Session object",
        description = "Dynamic property names that will be passed to the Mail session. " +
                "Possible properties can be found in: https://javaee.github.io/javamail/docs/api/com/sun/mail/smtp/package-summary.html.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The entirety of the FlowFile's content (as a String object) "
        + "will be read into memory in case the property to use the flow file content as the email body is set to true.")
public class PutEmail extends AbstractProcessor {

    private static final Pattern MAIL_PROPERTY_PATTERN = Pattern.compile("^mail\\.smtps?\\.([a-z0-9\\.]+)$");

    public static final PropertyDescriptor SMTP_HOSTNAME = new PropertyDescriptor.Builder()
            .name("SMTP Hostname")
            .description("The hostname of the SMTP host")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SMTP_PORT = new PropertyDescriptor.Builder()
            .name("SMTP Port")
            .description("The Port used for SMTP communications")
            .required(true)
            .defaultValue("25")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

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

    public static final PropertyDescriptor SMTP_USERNAME = new PropertyDescriptor.Builder()
            .name("SMTP Username")
            .description("Username for the SMTP account")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor SMTP_PASSWORD = new PropertyDescriptor.Builder()
            .name("SMTP Password")
            .description("Password for the SMTP account")
            .dependsOn(AUTHORIZATION_MODE, PASSWORD_BASED_AUTHORIZATION_MODE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor SMTP_AUTH = new PropertyDescriptor.Builder()
            .name("SMTP Auth")
            .description("Flag indicating whether authentication should be used")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor SMTP_TLS = new PropertyDescriptor.Builder()
            .name("SMTP TLS")
            .displayName("SMTP STARTTLS")
            .description("Flag indicating whether Opportunistic TLS should be enabled using STARTTLS command")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor SMTP_SOCKET_FACTORY = new PropertyDescriptor.Builder()
            .name("SMTP Socket Factory")
            .description("Socket Factory to use for SMTP Connection")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("javax.net.ssl.SSLSocketFactory")
            .build();
    public static final PropertyDescriptor HEADER_XMAILER = new PropertyDescriptor.Builder()
            .name("SMTP X-Mailer Header")
            .description("X-Mailer used in the header of the outgoing email")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("NiFi")
            .build();
    public static final PropertyDescriptor ATTRIBUTE_NAME_REGEX = new PropertyDescriptor.Builder()
            .name("attribute-name-regex")
            .displayName("Attributes to Send as Headers (Regex)")
            .description("A Regular Expression that is matched against all FlowFile attribute names. "
                    + "Any attribute whose name matches the regex will be added to the Email messages as a Header. "
                    + "If not specified, no FlowFile attributes will be added as headers.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("Content Type")
            .description("Mime Type used to interpret the contents of the email, such as text/plain or text/html")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("text/plain")
            .build();
    public static final PropertyDescriptor FROM = new PropertyDescriptor.Builder()
            .name("From")
            .description("Specifies the Email address to use as the sender. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor TO = new PropertyDescriptor.Builder()
            .name("To")
            .description("The recipients to include in the To-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CC = new PropertyDescriptor.Builder()
            .name("CC")
            .description("The recipients to include in the CC-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor BCC = new PropertyDescriptor.Builder()
            .name("BCC")
            .description("The recipients to include in the BCC-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor REPLY_TO = new PropertyDescriptor.Builder()
        .name("Reply-To")
        .description("The recipients that will receive the reply instead of the from (see RFC2822 §3.6.2)."
            + "This feature is useful, for example, when the email is sent by a no-reply account. This field is optional."
            + "Comma separated sequence of addresses following RFC822 syntax.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor SUBJECT = new PropertyDescriptor.Builder()
            .name("Subject")
            .description("The email subject")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("Message from NiFi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor MESSAGE = new PropertyDescriptor.Builder()
            .name("Message")
            .description("The body of the email message")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ATTACH_FILE = new PropertyDescriptor.Builder()
            .name("Attach File")
            .description("Specifies whether or not the FlowFile content should be attached to the email")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor CONTENT_AS_MESSAGE = new PropertyDescriptor.Builder()
            .name("email-ff-content-as-message")
            .displayName("Flow file content as message")
            .description("Specifies whether or not the FlowFile content should be the message of the email. If true, the 'Message' property is ignored.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor INCLUDE_ALL_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include All Attributes In Message")
            .description("Specifies whether or not all FlowFile attributes should be recorded in the body of the email message")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor INPUT_CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("input-character-set")
            .displayName("Input Character Set")
            .description("Specifies the character set of the FlowFile contents "
                    + "for reading input FlowFile contents to generate the message body "
                    + "or as an attachment to the message. "
                    + "If not set, UTF-8 will be the default value.")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(StandardCharsets.UTF_8.name())
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SMTP_HOSTNAME,
            SMTP_PORT,
            AUTHORIZATION_MODE,
            OAUTH2_ACCESS_TOKEN_PROVIDER,
            SMTP_USERNAME,
            SMTP_PASSWORD,
            SMTP_AUTH,
            SMTP_TLS,
            SMTP_SOCKET_FACTORY,
            HEADER_XMAILER,
            ATTRIBUTE_NAME_REGEX,
            CONTENT_TYPE,
            FROM,
            TO,
            CC,
            BCC,
            REPLY_TO,
            SUBJECT,
            MESSAGE,
            CONTENT_AS_MESSAGE,
            INPUT_CHARACTER_SET,
            ATTACH_FILE,
            INCLUDE_ALL_ATTRIBUTES
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully sent will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that fail to send will be routed to this relationship")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    /**
     * Mapping of the mail properties to the NiFi PropertyDescriptors that will be evaluated at runtime
     */
    private static final Map<String, PropertyDescriptor> propertyToContext = Map.of(
    "mail.smtp.host", SMTP_HOSTNAME,
    "mail.smtp.port", SMTP_PORT,
    "mail.smtp.socketFactory.port", SMTP_PORT,
    "mail.smtp.socketFactory.class", SMTP_SOCKET_FACTORY,
    "mail.smtp.auth", SMTP_AUTH,
    "mail.smtp.starttls.enable", SMTP_TLS,
    "mail.smtp.user", SMTP_USERNAME,
    "mail.smtp.password", SMTP_PASSWORD
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("SMTP property passed to the Mail Session")
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(new DynamicMailPropertyValidator())
                .build();
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

    private volatile Pattern attributeNamePattern = null;

    private volatile Optional<OAuth2AccessTokenProvider> oauth2AccessTokenProviderOptional;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String attributeNameRegex = context.getProperty(ATTRIBUTE_NAME_REGEX).getValue();
        this.attributeNamePattern = attributeNameRegex == null ? null : Pattern.compile(attributeNameRegex);

        if (context.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).isSet()) {
            OAuth2AccessTokenProvider oauth2AccessTokenProvider = context.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);

            oauth2AccessTokenProvider.getAccessDetails();

            oauth2AccessTokenProviderOptional = Optional.of(oauth2AccessTokenProvider);
        } else {
            oauth2AccessTokenProviderOptional = Optional.empty();
        }
    }

    private void setMessageHeader(final String header, final String value, final Message message) throws MessagingException {
        final ComponentLog logger = getLogger();
        try {
            message.setHeader(header, MimeUtility.encodeText(value));
        } catch (UnsupportedEncodingException e) {
            logger.warn("Unable to add header {} with value {} due to encoding exception", header, value);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Properties properties = this.getMailPropertiesFromFlowFile(context, flowFile);
        final Session mailSession = this.createMailSession(properties);
        final MimeMessage message = new MimeMessage(mailSession);

        try {
            message.addFrom(toInetAddresses(context, flowFile, FROM));
            message.setRecipients(RecipientType.TO, toInetAddresses(context, flowFile, TO));
            message.setRecipients(RecipientType.CC, toInetAddresses(context, flowFile, CC));
            message.setRecipients(RecipientType.BCC, toInetAddresses(context, flowFile, BCC));
            message.setReplyTo(toInetAddresses(context, flowFile, REPLY_TO));

            if (attributeNamePattern != null) {
                for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
                    if (attributeNamePattern.matcher(entry.getKey()).matches()) {
                        this.setMessageHeader(entry.getKey(), entry.getValue(), message);
                    }
                }
            }
            this.setMessageHeader("X-Mailer", context.getProperty(HEADER_XMAILER).evaluateAttributeExpressions(flowFile).getValue(), message);

            message.setSubject(context.getProperty(SUBJECT).evaluateAttributeExpressions(flowFile).getValue(), StandardCharsets.UTF_8.name());

            final String messageText = getMessage(flowFile, context, session);

            final String contentType = context.getProperty(CONTENT_TYPE).evaluateAttributeExpressions(flowFile).getValue();
            final Charset charset = getCharset(context);

            message.setContent(messageText, contentType + String.format("; charset=\"%s\"", MimeUtility.mimeCharset(charset.name())));

            message.setSentDate(new Date());

            if (context.getProperty(ATTACH_FILE).asBoolean()) {
                final String encoding = getEncoding(context);
                final MimeBodyPart mimeText = new PreencodedMimeBodyPart(encoding);
                final byte[] messageBytes = messageText.getBytes(charset);
                final byte[] encodedMessageBytes = "base64".equals(encoding) ? Base64.encodeBase64(messageBytes) : messageBytes;
                final DataHandler messageDataHandler = new DataHandler(
                        new ByteArrayDataSource(
                                encodedMessageBytes,
                                contentType + String.format("; charset=\"%s\"", MimeUtility.mimeCharset(charset.name()))
                        )
                );
                mimeText.setDataHandler(messageDataHandler);
                mimeText.setHeader("Content-Transfer-Encoding", MimeUtility.getEncoding(mimeText.getDataHandler()));
                final MimeBodyPart mimeFile = new MimeBodyPart();
                session.read(flowFile, stream -> {
                    try {
                        final String mimeTypeAttribute = flowFile.getAttribute("mime.type");
                        String mimeType = "application/octet-stream";
                        if (mimeTypeAttribute != null && !mimeTypeAttribute.isEmpty()) {
                            mimeType = mimeTypeAttribute;
                        }
                        mimeFile.setDataHandler(new DataHandler(new ByteArrayDataSource(stream, mimeType)));
                    } catch (final Exception e) {
                        throw new IOException(e);
                    }
                });

                mimeFile.setFileName(MimeUtility.encodeText(flowFile.getAttribute(CoreAttributes.FILENAME.key()), charset.name(), null));
                mimeFile.setHeader("Content-Transfer-Encoding", MimeUtility.getEncoding(mimeFile.getDataHandler()));
                final MimeMultipart multipart = new MimeMultipart();
                multipart.addBodyPart(mimeText);
                multipart.addBodyPart(mimeFile);

                message.setContent(multipart);
            } else {
                // message is not a Multipart, need to set Content-Transfer-Encoding header at the message level
                message.setHeader("Content-Transfer-Encoding", MimeUtility.getEncoding(message.getDataHandler()));
            }


            message.saveChanges();

            send(message);

            session.getProvenanceReporter().send(flowFile, "mailto:" + message.getAllRecipients()[0].toString());
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().debug("Sent email as a result of receiving {}", flowFile);
        } catch (final ProcessException | MessagingException | IOException e) {
            context.yield();
            getLogger().error("Failed to send email for {}: {}; routing to failure", flowFile, e.getMessage(), e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private String getMessage(final FlowFile flowFile, final ProcessContext context, final ProcessSession session) {
        String messageText = "";

        if (context.getProperty(CONTENT_AS_MESSAGE).evaluateAttributeExpressions(flowFile).asBoolean()) {
            // reading all the content of the input flow file
            final byte[] byteBuffer = new byte[(int) flowFile.getSize()];
            session.read(flowFile, in -> StreamUtils.fillBuffer(in, byteBuffer, false));

            final Charset charset = getCharset(context);
            messageText = new String(byteBuffer, 0, byteBuffer.length, charset);
        } else if (context.getProperty(MESSAGE).isSet()) {
            messageText = context.getProperty(MESSAGE).evaluateAttributeExpressions(flowFile).getValue();
        }

        if (context.getProperty(INCLUDE_ALL_ATTRIBUTES).asBoolean()) {
            return formatAttributes(flowFile, messageText);
        }

        return messageText;
    }

    /**
     * Based on the input properties, determine whether an authenticate or unauthenticated session should be used. If authenticated, creates a Password Authenticator for use in sending the email.
     *
     * @param properties mail properties
     * @return session
     */
    private Session createMailSession(final Properties properties) {
        final boolean auth = Boolean.parseBoolean(properties.getProperty("mail.smtp.auth"));

        /*
         * Conditionally create a password authenticator if the 'auth' parameter is set.
         */
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
     * Uses the mapping of javax.mail properties to NiFi PropertyDescriptors to build the required Properties object to be used for sending this email
     *
     * @param context context
     * @param flowFile flowFile
     * @return mail properties
     */
    private Properties getMailPropertiesFromFlowFile(final ProcessContext context, final FlowFile flowFile) {
        final Properties properties = new Properties();

        for (final Entry<String, PropertyDescriptor> entry : propertyToContext.entrySet()) {
            // Evaluate the property descriptor against the flow file
            final String flowFileValue = context.getProperty(entry.getValue()).evaluateAttributeExpressions(flowFile).getValue();
            final String property = entry.getKey();

            // Nullable values are not allowed, so filter out
            if (null != flowFileValue) {
                properties.setProperty(property, flowFileValue);
            }
        }

        oauth2AccessTokenProviderOptional.ifPresent(oAuth2AccessTokenProvider -> {
            String accessToken = oAuth2AccessTokenProvider.getAccessDetails().getAccessToken();

            properties.setProperty("mail.smtp.password", accessToken);
            properties.put("mail.smtp.auth.mechanisms", "XOAUTH2");
        });

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                final String mailPropertyValue = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();
                // Nullable values are not allowed, so filter out
                if (null != mailPropertyValue) {
                    properties.setProperty(descriptor.getName(), mailPropertyValue);
                }
            }
        }

        return properties;

    }

    public static final String BODY_SEPARATOR = "\n\n--------------------------------------------------\n";

    private static String formatAttributes(final FlowFile flowFile, final String messagePrepend) {
        final StringBuilder message = new StringBuilder(messagePrepend);
        message.append(BODY_SEPARATOR);
        message.append("\nStandard FlowFile Metadata:");
        message.append(String.format("\n\t%1$s = '%2$s'", "id", flowFile.getAttribute(CoreAttributes.UUID.key())));
        message.append(String.format("\n\t%1$s = '%2$s'", "entryDate", new Date(flowFile.getEntryDate())));
        message.append(String.format("\n\t%1$s = '%2$s'", "fileSize", flowFile.getSize()));
        message.append("\nFlowFile Attributes:");
        for (final Entry<String, String> attribute : flowFile.getAttributes().entrySet()) {
            message.append(String.format("\n\t%1$s = '%2$s'", attribute.getKey(), attribute.getValue()));
        }
        message.append("\n");
        return message.toString();
    }

    /**
     * @param context the current context
     * @param flowFile the current flow file
     * @param propertyDescriptor the property to evaluate
     * @return an InternetAddress[] parsed from the supplied property
     * @throws AddressException if the property cannot be parsed to a valid InternetAddress[]
     */
    private InternetAddress[] toInetAddresses(final ProcessContext context, final FlowFile flowFile,
                                              PropertyDescriptor propertyDescriptor) throws AddressException {
        InternetAddress[] parse;
        final String value = context.getProperty(propertyDescriptor).evaluateAttributeExpressions(flowFile).getValue();
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

    /**
     * Wrapper for static method {@link Transport#send(Message)} to add testability of this class.
     *
     * @param msg the message to send
     * @throws MessagingException on error
     */
    protected void send(final Message msg) throws MessagingException {
        Transport.send(msg);
    }

    private static class DynamicMailPropertyValidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            final Matcher matcher = MAIL_PROPERTY_PATTERN.matcher(subject);
            if (!matcher.matches()) {
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(false)
                        .explanation(String.format("[%s] does not start with mail.smtp", subject))
                        .build();
            }

            if (propertyToContext.containsKey(subject)) {
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(false)
                        .explanation(String.format("[%s] overwrites standard properties", subject))
                        .build();
            }

            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(true)
                    .explanation("Valid mail.smtp property found")
                    .build();
        }
    }

    /**
     * Utility function to get a charset from the {@code INPUT_CHARACTER_SET} property
     * @param context the ProcessContext
     * @return the Charset
     */
    private Charset getCharset(final ProcessContext context) {
        return Charset.forName(context.getProperty(INPUT_CHARACTER_SET).getValue());
    }

    /**
     * Utility function to get the correct encoding from the {@code INPUT_CHARACTER_SET} property
     * @param context the ProcessContext
     * @return the encoding
     */
    private String getEncoding(final ProcessContext context) {
        final Charset charset = Charset.forName(context.getProperty(INPUT_CHARACTER_SET).getValue());
        if (StandardCharsets.US_ASCII.equals(charset)) {
            return "7bit";
        }
        // Every other charset in StandardCharsets use 8 bits or more. Using base64 encoding by default
        return "base64";
    }
}
