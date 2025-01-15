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
package org.apache.nifi.services.dropbox;

import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialDetails;
import org.apache.nifi.dropbox.credentials.service.DropboxCredentialService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

@CapabilityDescription("Defines credentials for Dropbox processors.")
@Tags({"dropbox", "credentials", "provider"})
public class StandardDropboxCredentialService extends AbstractControllerService implements DropboxCredentialService {

    public static final PropertyDescriptor APP_KEY = new PropertyDescriptor.Builder()
            .name("app-key")
            .displayName("App Key")
            .description("App Key of the user's Dropbox app." +
                    " See Additional Details for more information.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor APP_SECRET = new PropertyDescriptor.Builder()
            .name("app-secret")
            .displayName("App Secret")
            .description("App Secret of the user's Dropbox app." +
                    " See Additional Details for more information.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("access-token")
            .displayName("Access Token")
            .description("Access Token of the user's Dropbox app." +
                    " See Additional Details for more information about Access Token generation.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor REFRESH_TOKEN = new PropertyDescriptor.Builder()
            .name("refresh-token")
            .displayName("Refresh Token")
            .description("Refresh Token of the user's Dropbox app." +
                    " See Additional Details for more information about Refresh Token generation.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            APP_KEY,
            APP_SECRET,
            ACCESS_TOKEN,
            REFRESH_TOKEN
    );

    private DropboxCredentialDetails credential;

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final String appKey = context.getProperty(APP_KEY).evaluateAttributeExpressions().getValue();
        final String appSecret = context.getProperty(APP_SECRET).evaluateAttributeExpressions().getValue();
        final String accessToken = context.getProperty(ACCESS_TOKEN).evaluateAttributeExpressions().getValue();
        final String refreshToken = context.getProperty(REFRESH_TOKEN).evaluateAttributeExpressions().getValue();

        this.credential = new DropboxCredentialDetails(appKey, appSecret, accessToken, refreshToken);
    }

    @Override
    public DropboxCredentialDetails getDropboxCredential() {
        return credential;
    }
}
