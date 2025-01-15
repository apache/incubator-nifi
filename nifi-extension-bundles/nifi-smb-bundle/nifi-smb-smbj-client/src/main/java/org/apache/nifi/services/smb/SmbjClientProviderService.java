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
package org.apache.nifi.services.smb;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.Share;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.PORT_VALIDATOR;
import static org.apache.nifi.smb.common.SmbProperties.ENABLE_DFS;
import static org.apache.nifi.smb.common.SmbProperties.SMB_DIALECT;
import static org.apache.nifi.smb.common.SmbProperties.TIMEOUT;
import static org.apache.nifi.smb.common.SmbProperties.USE_ENCRYPTION;
import static org.apache.nifi.smb.common.SmbUtils.buildSmbClient;

@Tags({"samba, smb, cifs, files"})
@CapabilityDescription("Provides access to SMB Sessions with shared authentication credentials.")
public class SmbjClientProviderService extends AbstractControllerService implements SmbClientProviderService {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .displayName("Hostname")
            .name("hostname")
            .description("The network host of the SMB file server.")
            .required(true)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .displayName("Domain")
            .name("domain")
            .description(
                    "The domain used for authentication. Optional, in most cases username and password is sufficient.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .displayName("Username")
            .name("username")
            .description(
                    "The username used for authentication.")
            .required(false)
            .defaultValue("Guest")
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .displayName("Password")
            .name("password")
            .description("The password used for authentication.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .displayName("Port")
            .name("port")
            .description("Port to use for connection.")
            .required(true)
            .addValidator(PORT_VALIDATOR)
            .defaultValue("445")
            .build();

    public static final PropertyDescriptor SHARE = new PropertyDescriptor.Builder()
            .displayName("Share")
            .name("share")
            .description("The network share to which files should be listed from. This is the \"first folder\"" +
                    "after the hostname: smb://hostname:port/[share]/dir1/dir2")
            .required(true)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            HOSTNAME,
            PORT,
            SHARE,
            USERNAME,
            PASSWORD,
            DOMAIN,
            SMB_DIALECT,
            USE_ENCRYPTION,
            ENABLE_DFS,
            TIMEOUT
    );

    private SMBClient smbClient;
    private AuthenticationContext authenticationContext;
    private String hostname;
    private int port;
    private String shareName;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.hostname = context.getProperty(HOSTNAME).getValue();
        this.port = context.getProperty(PORT).asInteger();
        this.shareName = context.getProperty(SHARE).getValue();
        this.smbClient = buildSmbClient(context);
        createAuthenticationContext(context);
    }

    @OnDisabled
    public void onDisabled() {
        smbClient.close();
        smbClient = null;
        hostname = null;
        port = 0;
        shareName = null;
    }

    @Override
    public URI getServiceLocation() {
        return URI.create(String.format("smb://%s:%d/%s", hostname, port, shareName));
    }

    @Override
    public SmbClientService getClient() throws IOException {
        final Connection connection = smbClient.connect(hostname, port);

        final Session session;
        final Share share;

        try {
            session = connection.authenticate(authenticationContext);
        } catch (Exception e) {
            throw new IOException("Could not create session for share " + getServiceLocation(), e);
        }

        try {
            share = session.connectShare(shareName);
        } catch (Exception e) {
            closeSession(session);
            throw new IOException("Could not connect to share " + getServiceLocation(), e);
        }

        if (!(share instanceof DiskShare)) {
            closeSession(session);
            throw new IllegalArgumentException("DiskShare not found. Share " + share.getClass().getSimpleName() + " found on " + getServiceLocation());
        }

        return new SmbjClientService(session, (DiskShare) share, getServiceLocation());
    }

    private void closeSession(final Session session) {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            getLogger().error("Could not close session to {}", getServiceLocation(), e);
        }
    }

    private void createAuthenticationContext(final ConfigurationContext context) {
        if (context.getProperty(USERNAME).isSet()) {
            final String userName = context.getProperty(USERNAME).getValue();
            final String password =
                    context.getProperty(PASSWORD).isSet() ? context.getProperty(PASSWORD).getValue() : "";
            final String domainOrNull =
                    context.getProperty(DOMAIN).isSet() ? context.getProperty(DOMAIN).getValue() : null;
            authenticationContext = new AuthenticationContext(userName, password.toCharArray(), domainOrNull);
        } else {
            authenticationContext = AuthenticationContext.anonymous();
        }
    }

}
