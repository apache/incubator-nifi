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
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.ftp.FtpServer;
import org.apache.nifi.processors.standard.ftp.NifiFtpServer;
import org.apache.nifi.ssl.SSLContextProvider;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"ingest", "FTP", "FTPS", "listen"})
@CapabilityDescription("Starts an FTP server that listens on the specified port and transforms incoming files into FlowFiles. "
        + "The URI of the service will be ftp://{hostname}:{port}. The default port is 2221.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file received via the FTP/FTPS connection."),
        @WritesAttribute(attribute = "path", description = "The path pointing to the file's target directory. "
            + "E.g.: file.txt is uploaded to /Folder1/SubFolder, then the value of the path attribute will be \"/Folder1/SubFolder/\" "
            + "(note that it ends with a separator character).")
})
public class ListenFTP extends AbstractSessionFactoryProcessor {

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("Specifies the SSL Context Service that can be used to create secure connections. "
                    + "If an SSL Context Service is selected, then a keystore file must also be specified in the SSL Context Service. "
                    + "Without a keystore file, the processor cannot be started successfully."
                    + "Specifying a truststore file is optional. If a truststore file is specified, client authentication is required "
                    + "(the client needs to send a certificate to the server)."
                    + "Regardless of the selected TLS protocol, the highest available protocol is used for the connection. "
                    + "For example if NiFi is running on Java 11 and TLSv1.2 is selected in the controller service as the "
                    + "preferred TLS Protocol, TLSv1.3 will be used (regardless of TLSv1.2 being selected) because Java 11 "
                    + "supports TLSv1.3.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    public static final PropertyDescriptor BIND_ADDRESS = new PropertyDescriptor.Builder()
            .name("bind-address")
            .displayName("Bind Address")
            .description("The address the FTP server should be bound to. If not set (or set to 0.0.0.0), "
                    + "the server binds to all available addresses (i.e. all network interfaces of the host machine).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("listening-port")
            .displayName("Listening Port")
            .description("The Port to listen on for incoming connections. On Linux, root privileges are required to use port numbers below 1024.")
            .required(true)
            .defaultValue("2221")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .displayName("Username")
            .description("The name of the user that is allowed to log in to the FTP server. "
                    + "If a username is provided, a password must also be provided. "
                    + "If no username is specified, anonymous connections will be permitted.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("If the Username is set, then a password must also be specified. "
                    + "The password provided by the client trying to log in to the FTP server will be checked against this password.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .sensitive(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BIND_ADDRESS,
            PORT,
            USERNAME,
            PASSWORD,
            SSL_CONTEXT_SERVICE
    );

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for successfully received files.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            RELATIONSHIP_SUCCESS
    );

    private volatile FtpServer ftpServer;
    private volatile CountDownLatch sessionFactorySetSignal;
    private final AtomicReference<ProcessSessionFactory> sessionFactory = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void startFtpServer(ProcessContext context) {
        if (ftpServer == null) {
            sessionFactory.set(null);

            String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
            String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
            String bindAddress = context.getProperty(BIND_ADDRESS).evaluateAttributeExpressions().getValue();
            int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
            final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);

            try {
                sessionFactorySetSignal = new CountDownLatch(1);
                ftpServer = new NifiFtpServer.Builder()
                        .sessionFactory(sessionFactory)
                        .sessionFactorySetSignal(sessionFactorySetSignal)
                        .relationshipSuccess(RELATIONSHIP_SUCCESS)
                        .bindAddress(bindAddress)
                        .port(port)
                        .username(username)
                        .password(password)
                        .sslContextProvider(sslContextProvider)
                        .build();
                ftpServer.start();
            } catch (ProcessException processException) {
                getLogger().error(processException.getMessage(), processException);
                stopFtpServer();
                throw processException;
            }
        } else {
            getLogger().warn("Ftp server already started.");
        }
    }

    @OnStopped
    public void stopFtpServer() {
        if (ftpServer != null && !ftpServer.isStopped()) {
            ftpServer.stop();
        }
        ftpServer = null;
        sessionFactory.set(null);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        if (this.sessionFactory.compareAndSet(null, sessionFactory)) {
            sessionFactorySetSignal.countDown();
        }
        context.yield();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> results = new ArrayList<>(3);

        validateUsernameAndPassword(context, results);
        validateBindAddress(context, results);

        return results;
    }

    private void validateUsernameAndPassword(ValidationContext context, Collection<ValidationResult> validationResults) {
        String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

        if ((username == null) && (password != null)) {
            validationResults.add(usernameOrPasswordIsNull(USERNAME));
        } else if ((username != null) && (password == null)) {
            validationResults.add(usernameOrPasswordIsNull(PASSWORD));
        }
    }

    private void validateBindAddress(ValidationContext context, Collection<ValidationResult> validationResults) {
        String bindAddress = context.getProperty(BIND_ADDRESS).evaluateAttributeExpressions().getValue();
        try {
            InetAddress.getByName(bindAddress);
        } catch (UnknownHostException e) {
            String explanation = String.format("'%s' is unknown", BIND_ADDRESS.getDisplayName());
            validationResults.add(createValidationResult(BIND_ADDRESS.getDisplayName(), explanation));
        }
    }

    private ValidationResult usernameOrPasswordIsNull(PropertyDescriptor nullProperty) {
        String explanation = String.format("'%s' and '%s' should either both be provided or none of them", USERNAME.getDisplayName(), PASSWORD.getDisplayName());
        return createValidationResult(nullProperty.getDisplayName(), explanation);
    }

    private ValidationResult createValidationResult(String subject, String explanation) {
        return new ValidationResult.Builder().subject(subject).valid(false).explanation(explanation).build();
    }

}
