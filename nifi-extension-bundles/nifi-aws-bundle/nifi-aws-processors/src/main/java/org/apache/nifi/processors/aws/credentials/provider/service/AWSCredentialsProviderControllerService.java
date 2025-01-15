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
package org.apache.nifi.processors.aws.credentials.provider.service;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.Signer;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AccessKeyPairCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AnonymousCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AssumeRoleCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.ExplicitDefaultCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.FileCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.ImplicitDefaultCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.NamedProfileCredentialsStrategy;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.ssl.SSLContextProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import static org.apache.nifi.processors.aws.signer.AwsSignerType.AWS_V4_SIGNER;
import static org.apache.nifi.processors.aws.signer.AwsSignerType.CUSTOM_SIGNER;
import static org.apache.nifi.processors.aws.signer.AwsSignerType.DEFAULT_SIGNER;

/**
 * Implementation of AWSCredentialsProviderService interface
 *
 * @see AWSCredentialsProviderService
 */
@CapabilityDescription("Defines credentials for Amazon Web Services processors. " +
        "Uses default credentials without configuration. " +
        "Default credentials support EC2 instance profile/role, default user profile, environment variables, etc. " +
        "Additional options include access key / secret key pairs, credentials file, named profile, and assume role credentials.")
@Tags({ "aws", "credentials", "provider" })
@Restricted(
    restrictions = {
        @Restriction(
            requiredPermission = RequiredPermission.ACCESS_ENVIRONMENT_CREDENTIALS,
            explanation = "The default configuration can read environment variables and system properties for credentials"
        )
    }
)
public class AWSCredentialsProviderControllerService extends AbstractControllerService implements AWSCredentialsProviderService {

    // Obsolete property names
    private static final String OBSOLETE_PROXY_HOST = "assume-role-proxy-host";
    private static final String OBSOLETE_PROXY_PORT = "assume-role-proxy-port";

    public static final PropertyDescriptor USE_DEFAULT_CREDENTIALS = new PropertyDescriptor.Builder()
        .name("default-credentials")
        .displayName("Use Default Credentials")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .sensitive(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .description("If true, uses the Default Credential chain, including EC2 instance profiles or roles, " +
            "environment variables, default user credentials, etc.")
        .build();

    public static final PropertyDescriptor PROFILE_NAME = new PropertyDescriptor.Builder()
        .name("profile-name")
        .displayName("Profile Name")
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(false)
        .description("The AWS profile name for credentials from the profile configuration file.")
        .build();

    public static final PropertyDescriptor CREDENTIALS_FILE = new PropertyDescriptor.Builder()
        .name("Credentials File")
        .displayName("Credentials File")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
        .description("Path to a file containing AWS access key and secret key in properties file format.")
        .build();

    public static final PropertyDescriptor ACCESS_KEY_ID = new PropertyDescriptor.Builder()
        .name("Access Key")
        .displayName("Access Key ID")
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(true)
        .build();

    public static final PropertyDescriptor SECRET_KEY = new PropertyDescriptor.Builder()
        .name("Secret Key")
        .displayName("Secret Access Key")
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(true)
        .build();

    public static final PropertyDescriptor USE_ANONYMOUS_CREDENTIALS = new PropertyDescriptor.Builder()
        .name("anonymous-credentials")
        .displayName("Use Anonymous Credentials")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .sensitive(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .description("If true, uses Anonymous credentials")
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_ARN = new PropertyDescriptor.Builder()
        .name("Assume Role ARN")
        .displayName("Assume Role ARN")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(false)
        .description("The AWS Role ARN for cross account access. This is used in conjunction with Assume Role Session Name and other Assume Role properties.")
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_NAME = new PropertyDescriptor.Builder()
        .name("Assume Role Session Name")
        .displayName("Assume Role Session Name")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(false)
        .description("The AWS Role Session Name for cross account access. This is used in conjunction with Assume Role ARN.")
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_STS_REGION = new PropertyDescriptor.Builder()
        .name("assume-role-sts-region")
        .displayName("Assume Role STS Region")
        .description("The AWS Security Token Service (STS) region")
        .dependsOn(ASSUME_ROLE_ARN)
        .allowableValues(getAvailableRegions())
        .defaultValue(createAllowableValue(Region.US_WEST_2).getValue())
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_EXTERNAL_ID = new PropertyDescriptor.Builder()
        .name("assume-role-external-id")
        .displayName("Assume Role External ID")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(false)
        .description("External ID for cross-account access. This is used in conjunction with Assume Role ARN.")
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("assume-role-ssl-context-service")
        .displayName("Assume Role SSL Context Service")
        .description("SSL Context Service used when connecting to the STS Endpoint.")
        .identifiesControllerService(SSLContextProvider.class)
        .required(false)
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE = new PropertyDescriptor.Builder()
        .name("assume-role-proxy-configuration-service")
        .displayName("Assume Role Proxy Configuration Service")
        .identifiesControllerService(ProxyConfigurationService.class)
        .required(false)
        .description("Proxy configuration for cross-account access, if needed within your environment. This will configure a proxy to request for temporary access keys into another AWS account.")
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_STS_ENDPOINT = new PropertyDescriptor.Builder()
        .name("assume-role-sts-endpoint")
        .displayName("Assume Role STS Endpoint Override")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(false)
        .description("The default AWS Security Token Service (STS) endpoint (\"sts.amazonaws.com\") works for " +
            "all accounts that are not for China (Beijing) region or GovCloud. You only need to set " +
            "this property to \"sts.cn-north-1.amazonaws.com.cn\" when you are requesting session credentials " +
            "for services in China(Beijing) region or to \"sts.us-gov-west-1.amazonaws.com\" for GovCloud.")
        .dependsOn(ASSUME_ROLE_ARN)
        .build();


    public static final PropertyDescriptor ASSUME_ROLE_STS_SIGNER_OVERRIDE = new PropertyDescriptor.Builder()
        .name("assume-role-sts-signer-override")
        .displayName("Assume Role STS Signer Override")
        .description("The AWS STS library uses Signature Version 4 by default. This property allows you to plug in your own custom signer implementation.")
        .required(false)
        .allowableValues(EnumSet.of(DEFAULT_SIGNER, AWS_V4_SIGNER, CUSTOM_SIGNER))
        .defaultValue(DEFAULT_SIGNER.getValue())
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor MAX_SESSION_TIME = new PropertyDescriptor.Builder()
        .name("Session Time")
        .displayName("Assume Role Session Time")
        .description("Session time for role based session (between 900 and 3600 seconds). This is used in conjunction with Assume Role ARN.")
        .defaultValue("3600")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .sensitive(false)
        .dependsOn(ASSUME_ROLE_ARN)
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_STS_CUSTOM_SIGNER_CLASS_NAME = new PropertyDescriptor.Builder()
        .name("custom-signer-class-name")
        .displayName("Custom Signer Class Name")
        .description(String.format("Fully qualified class name of the custom signer class. The signer must implement %s interface.", Signer.class.getName()))
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .dependsOn(ASSUME_ROLE_STS_SIGNER_OVERRIDE, CUSTOM_SIGNER)
        .build();

    public static final PropertyDescriptor ASSUME_ROLE_STS_CUSTOM_SIGNER_MODULE_LOCATION = new PropertyDescriptor.Builder()
        .name("custom-signer-module-location")
        .displayName("Custom Signer Module Location")
        .description("Comma-separated list of paths to files and/or directories which contain the custom signer's JAR file and its dependencies (if any).")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY)
        .dependsOn(ASSUME_ROLE_STS_SIGNER_OVERRIDE, CUSTOM_SIGNER)
        .dynamicallyModifiesClasspath(true)
        .build();


    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        USE_DEFAULT_CREDENTIALS,
        ACCESS_KEY_ID,
        SECRET_KEY,
        CREDENTIALS_FILE,
        PROFILE_NAME,
        USE_ANONYMOUS_CREDENTIALS,
        ASSUME_ROLE_ARN,
        ASSUME_ROLE_NAME,
        MAX_SESSION_TIME,
        ASSUME_ROLE_EXTERNAL_ID,
        ASSUME_ROLE_SSL_CONTEXT_SERVICE,
        ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE,
        ASSUME_ROLE_STS_REGION,
        ASSUME_ROLE_STS_ENDPOINT,
        ASSUME_ROLE_STS_SIGNER_OVERRIDE,
        ASSUME_ROLE_STS_CUSTOM_SIGNER_CLASS_NAME,
        ASSUME_ROLE_STS_CUSTOM_SIGNER_MODULE_LOCATION
    );

    private volatile ConfigurationContext context;
    private volatile AWSCredentialsProvider credentialsProvider;

    private final List<CredentialsStrategy> strategies = List.of(
        // Primary Credential Strategies
        new ExplicitDefaultCredentialsStrategy(),
        new AccessKeyPairCredentialsStrategy(),
        new FileCredentialsStrategy(),
        new NamedProfileCredentialsStrategy(),
        new AnonymousCredentialsStrategy(),

        // Implicit Default is the catch-all primary strategy
        new ImplicitDefaultCredentialsStrategy(),

        // Derived Credential Strategies
        new AssumeRoleCredentialsStrategy());


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        ProxyServiceMigration.migrateProxyProperties(config, ASSUME_ROLE_PROXY_CONFIGURATION_SERVICE, OBSOLETE_PROXY_HOST, OBSOLETE_PROXY_PORT, null, null);
    }

    @Override
    public AWSCredentialsProvider getCredentialsProvider() throws ProcessException {
        return credentialsProvider;
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider() {
        // Avoiding instantiation until actually used, in case v1-related configuration is not compatible with v2 clients
        final CredentialsStrategy primaryStrategy = selectPrimaryStrategy(context);
        final AwsCredentialsProvider primaryCredentialsProvider = primaryStrategy.getAwsCredentialsProvider(context);
        AwsCredentialsProvider derivedCredentialsProvider = null;

        for (final CredentialsStrategy strategy : strategies) {
            if (strategy.canCreateDerivedCredential(context)) {
                derivedCredentialsProvider = strategy.getDerivedAwsCredentialsProvider(context, primaryCredentialsProvider);
                break;
            }
        }

        return derivedCredentialsProvider == null ? primaryCredentialsProvider : derivedCredentialsProvider;
    }

    private CredentialsStrategy selectPrimaryStrategy(final PropertyContext propertyContext) {
        for (final CredentialsStrategy strategy : strategies) {
            if (strategy.canCreatePrimaryCredential(propertyContext)) {
                return strategy;
            }
        }
        return null;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final CredentialsStrategy selectedStrategy = selectPrimaryStrategy(validationContext);
        final ArrayList<ValidationResult> validationFailureResults = new ArrayList<>();

        for (CredentialsStrategy strategy : strategies) {
            final Collection<ValidationResult> strategyValidationFailures = strategy.validate(validationContext,
                selectedStrategy);
            if (strategyValidationFailures != null) {
                validationFailureResults.addAll(strategyValidationFailures);
            }
        }

        return validationFailureResults;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        this.context = context;

        credentialsProvider = createCredentialsProvider(context);
        getLogger().debug("Using credentials provider: {}", credentialsProvider.getClass());
    }

    private AWSCredentialsProvider createCredentialsProvider(final PropertyContext propertyContext) {
        final CredentialsStrategy primaryStrategy = selectPrimaryStrategy(propertyContext);
        AWSCredentialsProvider primaryCredentialsProvider = primaryStrategy.getCredentialsProvider(propertyContext);
        AWSCredentialsProvider derivedCredentialsProvider = null;

        for (CredentialsStrategy strategy : strategies) {
            if (strategy.canCreateDerivedCredential(propertyContext)) {
                derivedCredentialsProvider = strategy.getDerivedCredentialsProvider(propertyContext, primaryCredentialsProvider);
                break;
            }
        }

        if (derivedCredentialsProvider != null) {
            return derivedCredentialsProvider;
        } else {
            return primaryCredentialsProvider;
        }
    }


    public static AllowableValue[] getAvailableRegions() {
        final List<AllowableValue> values = new ArrayList<>();
        for (final Region region : Region.regions()) {
            if (region.isGlobalRegion()) {
                continue;
            }
            values.add(createAllowableValue(region));
        }
        return values.toArray(new AllowableValue[0]);
    }

    public static AllowableValue createAllowableValue(final Region region) {
        return new AllowableValue(region.id(), region.metadata().description(), "AWS Region Code : " + region.id());
    }

    @Override
    public String toString() {
        return "AWSCredentialsProviderService[id=" + getIdentifier() + "]";
    }
}