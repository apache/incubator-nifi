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
package org.apache.nifi.parameter.azure;

import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.azure.security.keyvault.secrets.models.SecretProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.AbstractParameterProvider;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.VerifiableParameterProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.AzureCredentialsService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Reads secrets from Azure Key Vault Secrets to provide parameter values.  Secrets must be created similar to the following Azure cli command: <br/><br/>
 * <code>az keyvault secret set --vault-name &lt;your-unique-keyvault-name> --name &lt;parameter-name> --value &lt;parameter-value>
 * --tags group-name=&lt;group-name></code> <br/><br/>
 * @see <a href="https://learn.microsoft.com/en-us/azure/key-vault/secrets/quick-create-cli">Azure Key Vault Secrets</a>
 */
@Tags({"azure", "keyvault", "key", "vault", "secrets"})
@CapabilityDescription("Fetches parameters from Azure Key Vault Secrets.  Each secret becomes a Parameter, which map to a Parameter Group by" +
        "adding a secret tag named 'group-name'.")
public class AzureKeyVaultSecretsParameterProvider extends AbstractParameterProvider implements VerifiableParameterProvider {
    public static final PropertyDescriptor AZURE_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("azure-credentials-service")
            .displayName("Azure Credentials Service")
            .description("Controller service used to obtain Azure credentials to be used with Key Vault client.")
            .required(true)
            .identifiesControllerService(AzureCredentialsService.class)
            .build();
    public static final PropertyDescriptor KEY_VAULT_URI = new PropertyDescriptor.Builder()
            .name("key-vault-uri")
            .displayName("Key Vault URI")
            .description("Vault URI of the Key Vault that contains the secrets")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor GROUP_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("group-name-pattern")
            .displayName("Group Name Pattern")
            .description("A Regular Expression matching on the 'group-name' tag value that identifies Secrets whose parameters should be fetched. " +
                    "Any secrets without a 'group-name' tag value that matches this Regex will not be fetched.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(true)
            .defaultValue(".*")
            .build();

    static final String GROUP_NAME_TAG = "group-name";

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            AZURE_CREDENTIALS_SERVICE,
            KEY_VAULT_URI,
            GROUP_NAME_PATTERN
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) {
        final SecretClient secretClient = configureSecretClient(context);
        final List<KeyVaultSecret> secrets = getAllSecrets(secretClient);
        return getParameterGroupsFromSecrets(context, secrets);
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

    private List<KeyVaultSecret> getAllSecrets(final SecretClient secretClient) {
        final List<KeyVaultSecret> secrets = new ArrayList<>();

        for (final SecretProperties secretProperties : secretClient.listPropertiesOfSecrets()) {
            if (secretProperties.isEnabled()) {
                KeyVaultSecret secretWithValue = secretClient.getSecret(secretProperties.getName(), secretProperties.getVersion());
                secrets.add(secretWithValue);
            }
        }

        return secrets;
    }

    private List<ParameterGroup> getParameterGroupsFromSecrets(final ConfigurationContext context, final List<KeyVaultSecret> secrets) {
        final Map<String, List<Parameter>> nameToParametersMap = new HashMap<>();
        for (final KeyVaultSecret secret: secrets) {
            final String parameterName = secret.getName();
            final String parameterValue = secret.getValue();

            final Map<String, String> tags = secret.getProperties().getTags();
            if (tags == null) {
                getLogger().debug("Secret with parameter name [{}] not recognized as a valid parameter since it does not have tags");
                continue;
            }
            final String parameterGroupName = tags.get(GROUP_NAME_TAG);
            if (parameterGroupName == null) {
                getLogger().debug("Secret with parameter name [{}] not recognized as a valid parameter since it does not have the [{}] tag", parameterName, GROUP_NAME_TAG);
                continue;
            }

            final Pattern groupNamePattern = Pattern.compile(context.getProperty(GROUP_NAME_PATTERN).getValue());

            if (!groupNamePattern.matcher(parameterGroupName).matches()) {
                getLogger().debug("Secret [{}] with tag [{}] does not match the group name pattern {}", parameterName,
                        parameterGroupName, groupNamePattern);
                continue;
            }

            nameToParametersMap
                    .computeIfAbsent(parameterGroupName, groupName -> new ArrayList<>())
                    .add(createParameter(parameterName, parameterValue));

        }

        return createParameterGroupFromMap(nameToParametersMap);
    }

    private List<ParameterGroup> createParameterGroupFromMap(final Map<String, List<Parameter>> nameToParametersMap) {
        final List<ParameterGroup> parameterGroups = new ArrayList<>();
        for (final Map.Entry<String, List<Parameter>> entry : nameToParametersMap.entrySet()) {
            final String parameterGroupName = entry.getKey();
            final List<Parameter> parameters = entry.getValue();
            parameterGroups.add(new ParameterGroup(parameterGroupName, parameters));
        }

        return parameterGroups;
    }

    private Parameter createParameter(final String parameterName, final String parameterValue) {
        return new Parameter.Builder()
            .name(parameterName)
            .value(parameterValue)
            .provided(true)
            .build();
    }

    SecretClient configureSecretClient(final ConfigurationContext context) {
        final AzureCredentialsService credentialsService =
                context.getProperty(AZURE_CREDENTIALS_SERVICE).asControllerService(AzureCredentialsService.class);
        final String vaultUrl = context.getProperty(KEY_VAULT_URI).getValue();
        return new SecretClientBuilder()
                .credential(credentialsService.getCredentials())
                .vaultUrl(vaultUrl)
                .buildClient();
    }
}
