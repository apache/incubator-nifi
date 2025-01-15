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

package org.apache.nifi.snowflake.service;

import net.snowflake.ingest.SimpleIngestManager;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.snowflake.SnowflakeIngestManagerProviderService;
import org.apache.nifi.processors.snowflake.util.SnowflakeProperties;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.snowflake.service.util.AccountIdentifierFormat;
import org.apache.nifi.snowflake.service.util.AccountIdentifierFormatParameters;
import org.apache.nifi.snowflake.service.util.ConnectionUrlFormat;

import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.util.List;

@Tags({"snowflake", "jdbc", "database", "connection"})
@CapabilityDescription("Provides a Snowflake Ingest Manager for Snowflake pipe processors")
public class StandardSnowflakeIngestManagerProviderService extends AbstractControllerService
        implements SnowflakeIngestManagerProviderService {

    public static final PropertyDescriptor ACCOUNT_IDENTIFIER_FORMAT = new PropertyDescriptor.Builder()
            .name("account-identifier-format")
            .displayName("Account Identifier Format")
            .description("The format of the account identifier.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .allowableValues(AccountIdentifierFormat.class)
            .defaultValue(AccountIdentifierFormat.ACCOUNT_NAME)
            .build();

    public static final PropertyDescriptor HOST_URL = new PropertyDescriptor.Builder()
            .name("host-url")
            .displayName("Snowflake URL")
            .description("Example host url: [account-locator].[cloud-region].[cloud]" + ConnectionUrlFormat.SNOWFLAKE_HOST_SUFFIX)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.FULL_URL)
            .build();

    public static final PropertyDescriptor ACCOUNT_LOCATOR = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ACCOUNT_LOCATOR)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor CLOUD_REGION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.CLOUD_REGION)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor CLOUD_TYPE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.CLOUD_TYPE)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_LOCATOR)
            .build();

    public static final PropertyDescriptor ORGANIZATION_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ORGANIZATION_NAME)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_NAME)
            .build();

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.ACCOUNT_NAME)
            .dependsOn(ACCOUNT_IDENTIFIER_FORMAT, AccountIdentifierFormat.ACCOUNT_NAME)
            .build();

    public static final PropertyDescriptor USER_NAME = new PropertyDescriptor.Builder()
            .name("user-name")
            .displayName("User Name")
            .description("The Snowflake user name.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    public static final PropertyDescriptor PRIVATE_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("private-key-service")
            .displayName("Private Key Service")
            .description("Specifies the Controller Service that will provide the private key. The public key needs to be added to the user account in the Snowflake account beforehand.")
            .identifiesControllerService(PrivateKeyService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.DATABASE)
            .required(true)
            .build();

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.SCHEMA)
            .required(true)
            .build();

    public static final PropertyDescriptor PIPE = new PropertyDescriptor.Builder()
            .name("pipe")
            .displayName("Pipe")
            .description("The Snowflake pipe to ingest from.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ACCOUNT_IDENTIFIER_FORMAT,
            HOST_URL,
            ACCOUNT_LOCATOR,
            CLOUD_REGION,
            CLOUD_TYPE,
            ORGANIZATION_NAME,
            ACCOUNT_NAME,
            USER_NAME,
            PRIVATE_KEY_SERVICE,
            DATABASE,
            SCHEMA,
            PIPE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private volatile String fullyQualifiedPipeName;
    private volatile SimpleIngestManager ingestManager;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String user = context.getProperty(USER_NAME).evaluateAttributeExpressions().getValue();
        final String database = context.getProperty(DATABASE).evaluateAttributeExpressions().getValue();
        final String schema = context.getProperty(SCHEMA).evaluateAttributeExpressions().getValue();
        final String pipe = context.getProperty(PIPE).evaluateAttributeExpressions().getValue();
        fullyQualifiedPipeName = database + "." + schema + "." + pipe;
        final PrivateKeyService privateKeyService = context.getProperty(PRIVATE_KEY_SERVICE)
                .asControllerService(PrivateKeyService.class);
        final PrivateKey privateKey = privateKeyService.getPrivateKey();

        final AccountIdentifierFormat accountIdentifierFormat = context.getProperty(ACCOUNT_IDENTIFIER_FORMAT)
                .asAllowableValue(AccountIdentifierFormat.class);
        final AccountIdentifierFormatParameters parameters = getAccountIdentifierFormatParameters(context);
        final String account = accountIdentifierFormat.getAccount(parameters);
        final String host = accountIdentifierFormat.getHostname(parameters);
        try {
            ingestManager = new SimpleIngestManager(account, user, fullyQualifiedPipeName, privateKey, "https", host, 443);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new InitializationException("Failed create Snowflake ingest manager", e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        if (ingestManager != null) {
            ingestManager.close();
            ingestManager = null;
        }
    }

    @Override
    public String getPipeName() {
        return fullyQualifiedPipeName;
    }

    @Override
    public SimpleIngestManager getIngestManager() {
        return ingestManager;
    }

    private AccountIdentifierFormatParameters getAccountIdentifierFormatParameters(ConfigurationContext context) {
        return new AccountIdentifierFormatParameters(
                context.getProperty(HOST_URL).evaluateAttributeExpressions().getValue(),
                context.getProperty(ORGANIZATION_NAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(ACCOUNT_NAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(ACCOUNT_LOCATOR).evaluateAttributeExpressions().getValue(),
                context.getProperty(CLOUD_REGION).evaluateAttributeExpressions().getValue(),
                context.getProperty(CLOUD_TYPE).evaluateAttributeExpressions().getValue()
        );
    }
}
