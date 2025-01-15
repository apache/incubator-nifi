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
package org.apache.nifi.vault.hashicorp;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.parameter.AbstractParameterProvider;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterGroup;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.VerifiableParameterProvider;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@CapabilityDescription("Provides parameters from HashiCorp Vault Key/Value Version 1 and Version 2 Secrets.  Each Secret represents a parameter group, " +
        "which will map to a Parameter Context.  The keys and values in the Secret map to Parameters.")
@Tags({"hashicorp", "vault", "secret"})
public class HashiCorpVaultParameterProvider extends AbstractParameterProvider implements ParameterProvider, VerifiableParameterProvider {

    static final AllowableValue KV_1 = new AllowableValue("KV_1", "KV_1", "Key/Value Secret Engine Version 1.");
    static final AllowableValue KV_2 = new AllowableValue("KV_2", "KV_2", "Key/Value Secret Engine Version 2. "
            + "If multiple versions of the secret exist, latest will be used.");

    public static final PropertyDescriptor VAULT_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("vault-client-service")
            .displayName("HashiCorp Vault Client Service")
            .description("The service used to interact with HashiCorp Vault")
            .identifiesControllerService(HashiCorpVaultClientService.class)
            .addValidator(Validator.VALID)
            .required(true)
            .build();
    public static final PropertyDescriptor KV_PATH = new PropertyDescriptor.Builder()
            .name("kv-path")
            .displayName("Key/Value Path")
            .description("The HashiCorp Vault path to the Key/Value Secrets Engine")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .defaultValue("kv")
            .build();
    public static final PropertyDescriptor KV_VERSION = new PropertyDescriptor.Builder()
            .name("kv-version")
            .displayName("Key/Value Version")
            .description("The version of the Key/Value Secrets Engine")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .allowableValues(KV_1, KV_2)
            .defaultValue(KV_1)
            .build();
    public static final PropertyDescriptor SECRET_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("secret-name-pattern")
            .displayName("Secret Name Pattern")
            .description("A Regular Expression indicating which Secrets to include as parameter groups to map to Parameter Contexts by name.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(true)
            .defaultValue(".*")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            VAULT_CLIENT_SERVICE,
            KV_PATH,
            KV_VERSION,
            SECRET_NAME_PATTERN
    );

    private HashiCorpVaultCommunicationService vaultCommunicationService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) {
        if (vaultCommunicationService == null) {
            vaultCommunicationService = getVaultCommunicationService(context);
        }

        final List<ParameterGroup> parameterGroups = getParameterGroups(vaultCommunicationService, context);
        return parameterGroups;
    }

    private List<ParameterGroup> getParameterGroups(final HashiCorpVaultCommunicationService vaultCommunicationService,
                                                            final ConfigurationContext context) {
        final String kvPath = context.getProperty(KV_PATH).getValue();
        final String kvVersion = context.getProperty(KV_VERSION).getValue();
        final String secretIncludeRegex = context.getProperty(SECRET_NAME_PATTERN).getValue();
        final List<String> allSecretNames = vaultCommunicationService.listKeyValueSecrets(kvPath, kvVersion);
        final List<String> secretNames = allSecretNames.stream()
                .filter(name -> name.matches(secretIncludeRegex))
                .collect(Collectors.toList());

        final List<ParameterGroup> parameterGroups = new ArrayList<>();
        for (final String secretName : secretNames) {
            final Map<String, String> keyValues = vaultCommunicationService.readKeyValueSecretMap(kvPath, secretName, kvVersion);
            final List<Parameter> parameters = new ArrayList<>();
            keyValues.forEach( (key, value) -> {
                parameters.add(new Parameter.Builder()
                        .name(key)
                        .value(value)
                        .provided(true)
                        .build());
            });
            parameterGroups.add(new ParameterGroup(secretName, parameters));
        }
        final long parameterCount = parameterGroups.stream()
                .flatMap(group -> group.getParameters().stream())
                .count();
        final List<String> parameterGroupNames = parameterGroups.stream()
                .map(group -> group.getGroupName())
                .distinct()
                .collect(Collectors.toList());
        getLogger().info("Fetched parameter groups {}, containing a total of {} parameters", parameterGroupNames, parameterCount);
        return parameterGroups;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (VAULT_CLIENT_SERVICE.equals(descriptor)) {
            vaultCommunicationService = null;
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        try {
            final HashiCorpVaultCommunicationService vaultCommunicationService = getVaultCommunicationService(context);
            final List<ParameterGroup> parameterGroups = getParameterGroups(vaultCommunicationService, context);
            final int groupCount = parameterGroups.size();
            final long parameterCount = parameterGroups.stream()
                    .flatMap(group -> group.getParameters().stream())
                    .count();
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .verificationStepName("Fetch Secrets as Parameter Groups")
                    .explanation(String.format("Successfully fetched %s secrets matching the filter as Parameter Groups, " +
                            "containing a total of %s Parameters.", groupCount, parameterCount))
                    .build());
        } catch (final Exception e) {
            verificationLogger.error("Failed to fetch secrets as Parameter Groups", e);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .verificationStepName("Fetch Secrets as Parameter Groups")
                    .explanation(String.format("Failed to fetch secrets as Parameter Groups: " + e.getMessage()))
                    .build());
        }

        return results;
    }

    HashiCorpVaultCommunicationService getVaultCommunicationService(final ConfigurationContext context) {
        final HashiCorpVaultClientService clientService = context.getProperty(VAULT_CLIENT_SERVICE)
                .asControllerService(HashiCorpVaultClientService.class);
        return clientService.getHashiCorpVaultCommunicationService();
    }
}
