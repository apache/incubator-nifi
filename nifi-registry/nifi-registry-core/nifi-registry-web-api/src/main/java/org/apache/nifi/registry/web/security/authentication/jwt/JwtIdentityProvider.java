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
package org.apache.nifi.registry.web.security.authentication.jwt;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.authentication.AuthenticationRequest;
import org.apache.nifi.registry.security.authentication.AuthenticationResponse;
import org.apache.nifi.registry.security.authentication.BearerAuthIdentityProvider;
import org.apache.nifi.registry.security.authentication.IdentityProvider;
import org.apache.nifi.registry.security.authentication.IdentityProviderConfigurationContext;
import org.apache.nifi.registry.security.authentication.exception.IdentityAccessException;
import org.apache.nifi.registry.security.authentication.exception.InvalidCredentialsException;
import org.apache.nifi.registry.security.authorization.Authorizer;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.exception.SecurityProviderDestructionException;
import org.apache.nifi.registry.web.security.authentication.exception.InvalidAuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
public class JwtIdentityProvider extends BearerAuthIdentityProvider implements IdentityProvider {

    private static final Logger logger = LoggerFactory.getLogger(JwtIdentityProvider.class);

    private static final String issuer = JwtIdentityProvider.class.getSimpleName();

    private static final long expiration = TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS);

    private final JwtService jwtService;

    @Autowired
    public JwtIdentityProvider(JwtService jwtService, NiFiRegistryProperties nifiProperties, Authorizer authorizer) {
        this.jwtService = jwtService;
    }

    @Override
    public AuthenticationResponse authenticate(AuthenticationRequest authenticationRequest) throws InvalidCredentialsException, IdentityAccessException {

        if (authenticationRequest == null) {
            logger.info("Cannot authenticate null authenticationRequest, returning null.");
            return null;
        }

        final Object credentials = authenticationRequest.getCredentials();
        if (credentials == null) {
            logger.info("JWT not found in authenticationRequest credentials, returning null.");
            return null;
        }

        try {
            String jwtAuthToken = credentials.toString();
            final Jws<Claims> jws = jwtService.parseAndValidateToken(jwtAuthToken);

            final String jwtPrincipal = jwtService.getUserIdentityFromToken(jws);
            final Set<String> groups = jwtService.getUserGroupsFromToken(jws);

            return new AuthenticationResponse(jwtPrincipal, jwtPrincipal, expiration, issuer, groups);
        } catch (JwtException e) {
            throw new InvalidAuthenticationException(e.getMessage(), e);
        }
    }

    @Override
    public void onConfigured(IdentityProviderConfigurationContext configurationContext) throws SecurityProviderCreationException { }

    @Override
    public void preDestruction() throws SecurityProviderDestructionException { }

}
