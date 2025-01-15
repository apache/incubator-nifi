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
package org.apache.nifi.box.controllerservices;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxAPIException;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxConfig;
import com.box.sdk.BoxDeveloperEditionAPIConnection;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;


@CapabilityDescription("Provides Box client objects through which Box API calls can be used.")
@Tags({"box", "client", "provider"})
public class JsonConfigBasedBoxClientService extends AbstractControllerService implements BoxClientService, VerifiableControllerService {
    public static final PropertyDescriptor ACCOUNT_ID = new PropertyDescriptor.Builder()
        .name("box-account-id")
        .displayName("Account ID")
        .description("The ID of the Box account who owns the accessed resource. Same as 'User Id' under 'App Info' in the App 'General Settings'.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor APP_CONFIG_FILE = new PropertyDescriptor.Builder()
        .name("app-config-file")
        .displayName("App Config File")
        .description("Full path of an App config JSON file. See Additional Details for more information.")
        .required(false)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor APP_CONFIG_JSON = new PropertyDescriptor.Builder()
        .name("app-config-json")
        .displayName("App Config JSON")
        .description("The raw JSON containing an App config. See Additional Details for more information.")
        .required(false)
        .sensitive(true)
        .addValidator(JsonValidator.INSTANCE)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP, ProxySpec.HTTP_AUTH};

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        ACCOUNT_ID,
        APP_CONFIG_FILE,
        APP_CONFIG_JSON,
        ProxyConfiguration.createProxyConfigPropertyDescriptor(PROXY_SPECS)
    );

    private volatile BoxAPIConnection boxAPIConnection;

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext configurationContext, final ComponentLog componentLog, final Map<String, String> map) {

        final List<ConfigVerificationResult> results = new ArrayList<>();
        try {
            createBoxApiConnection(configurationContext);
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName("Authentication")
                            .outcome(SUCCESSFUL)
                            .explanation("JSON App config verified")
                            .build()
            );
        } catch (final Exception e) {
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName("Authentication")
                            .outcome(FAILED)
                            .explanation("JSON App config failed to verify: " + e.getMessage())
                            .build()
            );
        }


        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        boxAPIConnection = createBoxApiConnection(context);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        if (validationContext.getProperty(APP_CONFIG_FILE).isSet() && validationContext.getProperty(APP_CONFIG_JSON).isSet()) {
            validationResults.add(new ValidationResult.Builder()
                .subject("App configuration")
                .valid(false)
                .explanation(String.format("'%s' and '%s' cannot be configured at the same time",
                    APP_CONFIG_FILE.getDisplayName(),
                    APP_CONFIG_JSON.getDisplayName())
                )
                .build());
        }

        if (!validationContext.getProperty(APP_CONFIG_FILE).isSet() && !validationContext.getProperty(APP_CONFIG_JSON).isSet()) {
            validationResults.add(new ValidationResult.Builder()
                .subject("App configuration")
                .valid(false)
                .explanation(String.format("either '%s' or '%s' must be configured",
                    APP_CONFIG_FILE.getDisplayName(),
                    APP_CONFIG_JSON.getDisplayName())
                )
                .build());
        }

        return validationResults;
    }

    @Override
    public BoxAPIConnection getBoxApiConnection() {
        return boxAPIConnection;
    }

    private BoxAPIConnection createBoxApiConnection(ConfigurationContext context) {

        final String accountId = context.getProperty(ACCOUNT_ID).evaluateAttributeExpressions().getValue();
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context);

        final BoxConfig boxConfig;
        if (context.getProperty(APP_CONFIG_FILE).isSet()) {
            String appConfigFile = context.getProperty(APP_CONFIG_FILE).evaluateAttributeExpressions().getValue();
            try (
                Reader reader = new FileReader(appConfigFile)
            ) {
                boxConfig = BoxConfig.readFrom(reader);
            } catch (FileNotFoundException e) {
                throw new ProcessException("Couldn't find Box config file", e);
            } catch (IOException e) {
                throw new ProcessException("Couldn't read Box config file", e);
            }
        } else {
            final String appConfig = context.getProperty(APP_CONFIG_JSON).evaluateAttributeExpressions().getValue();
            boxConfig = BoxConfig.readFrom(appConfig);
        }

        final BoxAPIConnection api;
        try {
            api = BoxDeveloperEditionAPIConnection.getAppEnterpriseConnection(boxConfig);
        } catch (final BoxAPIResponseException e) {
            if (boxConfig.getEnterpriseId().equals("0")) {
                throw new BoxAPIException("Box API integration is not enabled for account, the account's enterprise ID cannot be 0", e);
            } else {
                throw e;
            }
        }

        api.asUser(accountId);

        if (!Proxy.Type.DIRECT.equals(proxyConfiguration.getProxyType())) {
            api.setProxy(proxyConfiguration.createProxy());

            if (proxyConfiguration.hasCredential()) {
                api.setProxyBasicAuthentication(proxyConfiguration.getProxyUserName(), proxyConfiguration.getProxyUserPassword());
            }
        }
        return api;
    }
}
