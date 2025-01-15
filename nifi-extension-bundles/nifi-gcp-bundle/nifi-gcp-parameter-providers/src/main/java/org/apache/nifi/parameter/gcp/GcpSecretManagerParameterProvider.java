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
package org.apache.nifi.parameter.gcp;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.ProjectName;
import com.google.cloud.secretmanager.v1.Secret;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient.ListSecretsPage;
import com.google.cloud.secretmanager.v1.SecretManagerServiceSettings;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.AbstractParameterProvider;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.VerifiableParameterProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Reads secrets from GCP Secret Manager to provide parameter values.  Secrets must be created similar to the following GCP cli command: <br/><br/>
 * <code>gcp secretsmanager create-secret --name "[Context]" --secret-string '{ "[Param]": "[secretValue]", "[Param2]": "[secretValue2]" }'</code> <br/><br/>
 *
 */

@Tags({"gcp", "secret", "manager"})
@CapabilityDescription("Fetches parameters from GCP Secret Manager.  Each secret becomes a Parameter, which can be mapped to a Parameter Group " +
        "by adding a GCP label named 'group-name'.")
public class GcpSecretManagerParameterProvider extends AbstractParameterProvider implements VerifiableParameterProvider {
    private static final Logger logger = LoggerFactory.getLogger(GcpSecretManagerParameterProvider.class);

    public static final PropertyDescriptor GROUP_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("group-name-pattern")
            .displayName("Group Name Pattern")
            .description("A Regular Expression matching on the 'group-name' label value that identifies Secrets whose parameters should be fetched. " +
                    "Any secrets without a 'group-name' label value that matches this Regex will not be fetched.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(true)
            .defaultValue(".*")
            .build();

    public static final PropertyDescriptor PROJECT_ID = new PropertyDescriptor
            .Builder().name("gcp-project-id")
            .displayName("Project ID")
            .description("Google Cloud Project ID")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    /**
     * Links to the {@link GCPCredentialsService} which provides credentials for this particular processor.
     */
    public static final PropertyDescriptor GCP_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("gcp-credentials-provider-service")
            .displayName("GCP Credentials Provider Service")
            .description("The Controller Service used to obtain Google Cloud Platform credentials.")
            .required(true)
            .identifiesControllerService(GCPCredentialsService.class)
            .build();

    private static final String GROUP_NAME_LABEL = "group-name";
    private static final String SECRETS_PATH = "secrets/";
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GROUP_NAME_PATTERN,
            PROJECT_ID,
            GCP_CREDENTIALS_PROVIDER_SERVICE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) throws IOException {
        final Map<String, ParameterGroup> providedParameterGroups = new HashMap<>();
        final SecretManagerServiceClient secretsManager = this.configureClient(context);
        final ProjectName projectName = ProjectName.of(context.getProperty(PROJECT_ID).getValue());

        final SecretManagerServiceClient.ListSecretsPagedResponse pagedResponse = secretsManager.listSecrets(projectName);
        ListSecretsPage page = pagedResponse.getPage();
        do {
            for (final Secret secret : page.getValues()) {
                final String contextName = secret.getLabelsOrDefault(GROUP_NAME_LABEL, null);
                if (contextName == null) {
                    getLogger().debug("Secret [{}] does not have the {} label, and will be skipped", secret.getName(), GROUP_NAME_LABEL);
                    continue;
                }
                final String secretName = StringUtils.substringAfter(secret.getName(), SECRETS_PATH);

                fetchSecret(secretsManager, context, secretName, contextName, providedParameterGroups);
            }
            if (page.hasNextPage()) {
                page = page.getNextPage();
            }
        } while (page.hasNextPage());
        return new ArrayList<>(providedParameterGroups.values());
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            final List<ParameterGroup> parameterGroups = fetchParameters(context);
            int count = 0;
            for (final ParameterGroup group : parameterGroups) {
                count += group.getParameters().size();
            }
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .verificationStepName("Fetch Parameters")
                    .explanation(String.format("Fetched secret keys [%d] as parameters within groups [%d]",
                            count, parameterGroups.size()))
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

    private void fetchSecret(final SecretManagerServiceClient secretsManager, final ConfigurationContext context, final String secretName,
                                                     final String groupName, final Map<String, ParameterGroup> providedParameterGroups) {
        final Pattern groupNamePattern = Pattern.compile(context.getProperty(GROUP_NAME_PATTERN).getValue());
        final String projectId = context.getProperty(PROJECT_ID).getValue();

        if (!groupNamePattern.matcher(groupName).matches()) {
            logger.debug("Secret [{}] label [{}] does not match the group name pattern {}", secretName, groupName, groupNamePattern);
            return;
        }

        final SecretVersionName secretVersionName = SecretVersionName.of(projectId, secretName, "latest");

        // Access the secret version.
        final AccessSecretVersionResponse response = secretsManager.accessSecretVersion(secretVersionName);
        final String parameterValue = response.getPayload().getData().toStringUtf8();

        final Parameter parameter = createParameter(secretName, parameterValue);

        if (parameter != null) {
            final ParameterGroup group = providedParameterGroups
                    .computeIfAbsent(groupName, key -> new ParameterGroup(groupName, new ArrayList<>()));
            final List<Parameter> updatedParameters = new ArrayList<>(group.getParameters());
            updatedParameters.add(parameter);
            providedParameterGroups.put(groupName, new ParameterGroup(groupName, updatedParameters));
        }
    }

    private Parameter createParameter(final String parameterName, final String parameterValue) {
        return new Parameter.Builder()
            .name(parameterName)
            .value(parameterValue)
            .provided(true)
            .build();
    }

    SecretManagerServiceClient configureClient(final ConfigurationContext context) throws IOException {
        final GCPCredentialsService credentialsService = context.getProperty(GCP_CREDENTIALS_PROVIDER_SERVICE).asControllerService(GCPCredentialsService.class);

        final SecretManagerServiceClient client = SecretManagerServiceClient.create(SecretManagerServiceSettings
                .newBuilder().setCredentialsProvider(() -> credentialsService.getGoogleCredentials())
                .build());

        return client;
    }
}
