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
package org.apache.nifi.parameter.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.conn.ssl.SdkTLSSocketFactory;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.AWSSecretsManagerException;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.ListSecretsRequest;
import com.amazonaws.services.secretsmanager.model.ListSecretsResult;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.amazonaws.services.secretsmanager.model.SecretListEntry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.AbstractParameterProvider;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.VerifiableParameterProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.ssl.SSLContextProvider;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Reads secrets from AWS Secrets Manager to provide parameter values.  Secrets must be created similar to the following AWS cli command: <br/><br/>
 * <code>aws secretsmanager create-secret --name "[Context]" --secret-string '{ "[Param]": "[secretValue]", "[Param2]": "[secretValue2]" }'</code> <br/><br/>
 */
@Tags({"aws", "secretsmanager", "secrets", "manager"})
@CapabilityDescription("Fetches parameters from AWS SecretsManager.  Each secret becomes a Parameter group, which can map to a Parameter Context, with " +
        "key/value pairs in the secret mapping to Parameters in the group.")
public class AwsSecretsManagerParameterProvider extends AbstractParameterProvider implements VerifiableParameterProvider {
    enum ListingStrategy implements DescribedValue {
        ENUMERATION("Enumerate Secret Names", "Requires a set of secret names to fetch. AWS actions required: GetSecretValue."),

        PATTERN("Match Pattern", "Requires a regular expression pattern to match secret names. AWS actions required: ListSecrets and GetSecretValue.");

        private final String displayName;
        private final String description;

        ListingStrategy(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return this.displayName;
        }

        @Override
        public String getDescription() {
            return this.description;
        }
    }
    public static final PropertyDescriptor SECRET_LISTING_STRATEGY = new PropertyDescriptor.Builder()
            .name("secret-listing-strategy")
            .displayName("Secret Listing Strategy")
            .description("Strategy to use for listing secrets.")
            .required(true)
            .allowableValues(ListingStrategy.class)
            .defaultValue(ListingStrategy.PATTERN)
            .build();

    public static final PropertyDescriptor SECRET_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("secret-name-pattern")
            .displayName("Secret Name Pattern")
            .description("A Regular Expression matching on Secret Name that identifies Secrets whose parameters should be fetched. " +
                    "Any secrets whose names do not match this pattern will not be fetched.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .dependsOn(SECRET_LISTING_STRATEGY, ListingStrategy.PATTERN)
            .required(true)
            .defaultValue(".*")
            .build();

    public static final PropertyDescriptor SECRET_NAMES = new PropertyDescriptor.Builder()
            .name("secret-names")
            .displayName("Secret Names")
            .description("Comma-separated list of secret names to fetch.")
            .dependsOn(SECRET_LISTING_STRATEGY, ListingStrategy.ENUMERATION)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    /**
     * AWS credentials provider service
     *
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("aws-credentials-provider-service")
            .displayName("AWS Credentials Provider Service")
            .description("Service used to obtain an Amazon Web Services Credentials Provider")
            .required(true)
            .identifiesControllerService(AWSCredentialsProviderService.class)
            .build();

    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .name("aws-region")
            .displayName("Region")
            .required(true)
            .allowableValues(getAvailableRegions())
            .defaultValue(createAllowableValue(Regions.DEFAULT_REGION).getValue())
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("aws-communications-timeout")
            .displayName("Communications Timeout")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 secs")
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("aws-ssl-context-service")
            .displayName("SSL Context Service")
            .description("Specifies an optional SSL Context Service that, if provided, will be used to create connections")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    private static final String DEFAULT_USER_AGENT = "NiFi";
    private static final Protocol DEFAULT_PROTOCOL = Protocol.HTTPS;
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SECRET_LISTING_STRATEGY,
            SECRET_NAME_PATTERN,
            SECRET_NAMES,
            REGION,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            TIMEOUT,
            SSL_CONTEXT_SERVICE
    );

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) {
        AWSSecretsManager secretsManager = this.configureClient(context);

        final List<ParameterGroup> groups = new ArrayList<>();

        // Fetch either by pattern or by enumerated list. See description of SECRET_LISTING_STRATEGY for more details.
        final ListingStrategy listingStrategy = context.getProperty(SECRET_LISTING_STRATEGY).asAllowableValue(ListingStrategy.class);
        final Set<String> fetchSecretNames = new HashSet<>();
        if (listingStrategy == ListingStrategy.ENUMERATION) {
            final String secretNames = context.getProperty(SECRET_NAMES).getValue();
            fetchSecretNames.addAll(Arrays.asList(secretNames.split(",")));
        } else {
            final Pattern secretNamePattern = Pattern.compile(context.getProperty(SECRET_NAME_PATTERN).getValue());
            final ListSecretsRequest listSecretsRequest = new ListSecretsRequest();

            ListSecretsResult listSecretsResult = secretsManager.listSecrets(listSecretsRequest);
            while (!listSecretsResult.getSecretList().isEmpty()) {
                for (final SecretListEntry entry : listSecretsResult.getSecretList()) {
                    final String secretName = entry.getName();
                    if (!secretNamePattern.matcher(secretName).matches()) {
                        getLogger().debug("Secret [{}] does not match the secret name pattern {}", secretName, secretNamePattern);
                        continue;
                    }
                    fetchSecretNames.add(secretName);
                }
                final String nextToken = listSecretsResult.getNextToken();
                if (nextToken == null) {
                    break;
                }
                listSecretsRequest.setNextToken(nextToken);
                listSecretsResult = secretsManager.listSecrets(listSecretsRequest);
            }
        }

        for (final String secretName : fetchSecretNames) {
            final List<ParameterGroup> secretParameterGroups = fetchSecret(secretsManager, secretName);
            groups.addAll(secretParameterGroups);
        }
        return groups;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            final List<ParameterGroup> parameterGroups = fetchParameters(context);
            int parameterCount = 0;
            for (final ParameterGroup group : parameterGroups) {
                parameterCount += group.getParameters().size();
            }
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .verificationStepName("Fetch Parameters")
                    .explanation(String.format("Fetched secret keys [%d] as parameters, across groups [%d]",
                            parameterCount, parameterGroups.size()))
                    .build());
        } catch (final Exception e) {
            verificationLogger.error("Failed to fetch parameters", e);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .verificationStepName("Fetch Parameters")
                    .explanation("Failed to fetch parameters: " + e.getMessage())
                    .build());
        }
        return results;
    }

    private List<ParameterGroup> fetchSecret(final AWSSecretsManager secretsManager, final String secretName) {
        final List<ParameterGroup> groups = new ArrayList<>();

        final List<Parameter> parameters = new ArrayList<>();

        final GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretName);

        try {
            final GetSecretValueResult getSecretValueResult = secretsManager.getSecretValue(getSecretValueRequest);

            if (getSecretValueResult.getSecretString() == null) {
                getLogger().debug("Secret [{}] is not configured", secretName);
                return groups;
            }

            final ObjectNode secretObject = parseSecret(getSecretValueResult.getSecretString());
            if (secretObject == null) {
                getLogger().debug("Secret [{}] is not in the expected JSON key/value format", secretName);
                return groups;
            }

            for (final Iterator<Map.Entry<String, JsonNode>> it = secretObject.fields(); it.hasNext(); ) {
                final Map.Entry<String, JsonNode> field = it.next();
                final String parameterName = field.getKey();
                final String parameterValue = field.getValue().textValue();
                if (parameterValue == null) {
                    getLogger().debug("Secret [{}] Parameter [{}] has no value", secretName, parameterName);
                    continue;
                }

                parameters.add(createParameter(parameterName, parameterValue));
            }

            groups.add(new ParameterGroup(secretName, parameters));

            return groups;
        } catch (final ResourceNotFoundException e) {
            throw new IllegalStateException(String.format("Secret %s not found", secretName), e);
        } catch (final AWSSecretsManagerException e) {
            throw new IllegalStateException("Error retrieving secret " + secretName, e);
        }
    }

    private Parameter createParameter(final String parameterName, final String parameterValue) {
        return new Parameter.Builder()
                .name(parameterName)
                .value(parameterValue)
                .provided(true)
                .build();
    }

    protected ClientConfiguration createConfiguration(final ConfigurationContext context) {
        final ClientConfiguration config = new ClientConfiguration();
        config.setMaxErrorRetry(0);
        config.setUserAgentPrefix(DEFAULT_USER_AGENT);
        config.setProtocol(DEFAULT_PROTOCOL);
        final int commsTimeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        config.setConnectionTimeout(commsTimeout);
        config.setSocketTimeout(commsTimeout);

        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        if (sslContextProvider != null) {
            final SSLContext sslContext = sslContextProvider.createContext();
            SdkTLSSocketFactory sdkTLSSocketFactory = new SdkTLSSocketFactory(sslContext, new DefaultHostnameVerifier());
            config.getApacheHttpClientConfig().setSslSocketFactory(sdkTLSSocketFactory);
        }

        return config;
    }

    private ObjectNode parseSecret(final String secretString) {
        try {
            final JsonNode root = objectMapper.readTree(secretString);
            if (root instanceof ObjectNode) {
                return (ObjectNode) root;
            }
            return null;
        } catch (final JsonProcessingException e) {
            getLogger().debug("Error parsing JSON", e);
            return null;
        }
    }

    AWSSecretsManager configureClient(final ConfigurationContext context) {
        return AWSSecretsManagerClientBuilder.standard()
                .withRegion(context.getProperty(REGION).getValue())
                .withClientConfiguration(createConfiguration(context))
                .withCredentials(getCredentialsProvider(context))
                .build();
    }

    /**
     * Get credentials provider using the {@link AWSCredentialsProviderService}
     * @param context the configuration context
     * @return AWSCredentialsProvider the credential provider
     * @see  <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    protected AWSCredentialsProvider getCredentialsProvider(final ConfigurationContext context) {

        final AWSCredentialsProviderService awsCredentialsProviderService =
                context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService(AWSCredentialsProviderService.class);

        return awsCredentialsProviderService.getCredentialsProvider();

    }

    private static AllowableValue createAllowableValue(final Regions region) {
        return new AllowableValue(region.getName(), region.getDescription(), "AWS Region Code : " + region.getName());
    }

    private static AllowableValue[] getAvailableRegions() {
        final List<AllowableValue> values = new ArrayList<>();
        for (final Regions region : Regions.values()) {
            values.add(createAllowableValue(region));
        }
        return values.toArray(new AllowableValue[0]);
    }
}