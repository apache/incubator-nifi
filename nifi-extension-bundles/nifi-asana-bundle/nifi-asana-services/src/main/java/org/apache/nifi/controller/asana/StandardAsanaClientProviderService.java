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
package org.apache.nifi.controller.asana;

import com.asana.Client;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;

import static org.apache.nifi.controller.asana.StandardAsanaClient.ASANA_CLIENT_OPTION_BASE_URL;

@CapabilityDescription("Common service to authenticate with Asana, and to work on a specified workspace.")
@Tags({"asana", "service", "authentication"})
public class StandardAsanaClientProviderService extends AbstractControllerService implements AsanaClientProviderService {

    protected static final String ASANA_API_URL = "asana-api-url";
    protected static final String ASANA_PERSONAL_ACCESS_TOKEN = "asana-personal-access-token";
    protected static final String ASANA_WORKSPACE_NAME = "asana-workspace-name";

    protected static final PropertyDescriptor PROP_ASANA_API_BASE_URL = new PropertyDescriptor.Builder()
            .name(ASANA_API_URL)
            .displayName("API URL")
            .description("Base URL of Asana API. Leave it as default, unless you have your own Asana instance "
                    + "serving on a different URL. (typical for on-premise installations)")
            .required(true)
            .defaultValue(Client.DEFAULTS.get(ASANA_CLIENT_OPTION_BASE_URL).toString())
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_PERSONAL_ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name(ASANA_PERSONAL_ACCESS_TOKEN)
            .displayName("Personal Access Token")
            .description("Similarly to entering your username/password into a website, when you access "
                    + "your Asana data via the API you need to authenticate. Personal Access Token (PAT) "
                    + "is an authentication mechanism for accessing the API. You can generate a PAT from "
                    + "the Asana developer console. Refer to Asana Authentication Quick Start for detailed "
                    + "instructions on getting started.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    protected static final PropertyDescriptor PROP_ASANA_WORKSPACE_NAME = new PropertyDescriptor.Builder()
            .name(ASANA_WORKSPACE_NAME)
            .displayName("Workspace")
            .description("Specify which Asana workspace to use. Case sensitive. "
                    + "A workspace is the highest-level organizational unit in Asana. All projects and tasks "
                    + "have an associated workspace. An organization is a special kind of workspace that "
                    + "represents a company. In an organization, you can group your projects into teams.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PROP_ASANA_API_BASE_URL,
            PROP_ASANA_PERSONAL_ACCESS_TOKEN,
            PROP_ASANA_WORKSPACE_NAME
    );

    private volatile String personalAccessToken;
    private volatile String workspaceName;
    private volatile String baseUrl;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public synchronized void onEnabled(final ConfigurationContext context) {
        personalAccessToken = context.getProperty(PROP_ASANA_PERSONAL_ACCESS_TOKEN).getValue();
        workspaceName = context.getProperty(PROP_ASANA_WORKSPACE_NAME).getValue();
        baseUrl = context.getProperty(PROP_ASANA_API_BASE_URL).getValue();
    }

    @Override
    public synchronized AsanaClient createClient() {
        return new StandardAsanaClient(personalAccessToken, workspaceName, baseUrl);
    }
}
