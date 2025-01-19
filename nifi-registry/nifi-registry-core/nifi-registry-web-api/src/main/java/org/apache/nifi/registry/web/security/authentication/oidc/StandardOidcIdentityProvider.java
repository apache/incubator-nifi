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
package org.apache.nifi.registry.web.security.authentication.oidc;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.openid.connect.sdk.claims.AccessTokenHash;
import com.nimbusds.openid.connect.sdk.validators.AccessTokenValidator;
import com.nimbusds.openid.connect.sdk.validators.InvalidHashException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.authentication.exception.IdentityAccessException;
import org.apache.nifi.registry.util.FormatUtils;
import org.apache.nifi.registry.web.security.authentication.jwt.JwtService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jose.util.ResourceRetriever;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.Request;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.ClientSecretPost;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponseParser;
import com.nimbusds.openid.connect.sdk.UserInfoErrorResponse;
import com.nimbusds.openid.connect.sdk.UserInfoRequest;
import com.nimbusds.openid.connect.sdk.UserInfoResponse;
import com.nimbusds.openid.connect.sdk.UserInfoSuccessResponse;
import com.nimbusds.openid.connect.sdk.claims.IDTokenClaimsSet;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import com.nimbusds.openid.connect.sdk.token.OIDCTokens;
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator;
import net.minidev.json.JSONObject;

/**
 * OidcProvider for managing the OpenId Connect Authorization flow.
 */
@Component
public class StandardOidcIdentityProvider implements OidcIdentityProvider {

    private static final Logger logger = LoggerFactory.getLogger(StandardOidcIdentityProvider.class);
    private final String EMAIL_CLAIM = "email";

    private NiFiRegistryProperties properties;
    private JwtService jwtService;
    private OIDCProviderMetadata oidcProviderMetadata;
    private int oidcConnectTimeout;
    private int oidcReadTimeout;
    private IDTokenValidator tokenValidator;
    private ClientID clientId;
    private Secret clientSecret;

    /**
     * Creates a new StandardOidcIdentityProvider.
     *
     * @param jwtService jwt service
     * @param properties properties
     */
    @Autowired
    public StandardOidcIdentityProvider(final JwtService jwtService, final NiFiRegistryProperties properties) {
        this.properties = properties;
        this.jwtService = jwtService;
    }

    /**
     * Loads OIDC configuration values from {@link NiFiRegistryProperties}, connects to external OIDC provider, and retrieves
     * and validates provider metadata.
     */
    @Override
    public void initializeProvider() {
        // attempt to process the oidc configuration if configured
        if (!properties.isOidcEnabled()) {
            logger.warn("The OIDC provider is not configured or enabled");
            return;
        }

        validateOIDCConfiguration();

        try {
            // retrieve the oidc provider metadata
            oidcProviderMetadata = retrieveOidcProviderMetadata(properties.getOidcDiscoveryUrl());
        } catch (IOException | ParseException e) {
            throw new RuntimeException("Unable to retrieve OpenId Connect Provider metadata from: " + properties.getOidcDiscoveryUrl(), e);
        }

        validateOIDCProviderMetadata();
    }

    /**
     * Validates the retrieved OIDC provider metadata.
     */
    private void validateOIDCProviderMetadata() {
        // ensure the authorization endpoint is present
        if (oidcProviderMetadata.getAuthorizationEndpointURI() == null) {
            throw new RuntimeException("OpenId Connect Provider metadata does not contain an Authorization Endpoint.");
        }

        // ensure the token endpoint is present
        if (oidcProviderMetadata.getTokenEndpointURI() == null) {
            throw new RuntimeException("OpenId Connect Provider metadata does not contain a Token Endpoint.");
        }

        // ensure the oidc provider supports basic or post client auth
        List<ClientAuthenticationMethod> clientAuthenticationMethods = oidcProviderMetadata.getTokenEndpointAuthMethods();
        logger.info("OpenId Connect: Available clientAuthenticationMethods {} ", clientAuthenticationMethods);
        if (clientAuthenticationMethods == null || clientAuthenticationMethods.isEmpty()) {
            clientAuthenticationMethods = new ArrayList<>();
            clientAuthenticationMethods.add(ClientAuthenticationMethod.CLIENT_SECRET_BASIC);
            oidcProviderMetadata.setTokenEndpointAuthMethods(clientAuthenticationMethods);
            logger.warn("OpenId Connect: ClientAuthenticationMethods is null, Setting clientAuthenticationMethods as CLIENT_SECRET_BASIC");
        } else if (!clientAuthenticationMethods.contains(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
                && !clientAuthenticationMethods.contains(ClientAuthenticationMethod.CLIENT_SECRET_POST)) {
            throw new RuntimeException(String.format("OpenId Connect Provider does not support %s or %s",
                    ClientAuthenticationMethod.CLIENT_SECRET_BASIC.getValue(),
                    ClientAuthenticationMethod.CLIENT_SECRET_POST.getValue()));
        }

        // extract the supported json web signature algorithms
        final List<JWSAlgorithm> allowedAlgorithms = oidcProviderMetadata.getIDTokenJWSAlgs();
        if (allowedAlgorithms == null || allowedAlgorithms.isEmpty()) {
            throw new RuntimeException("The OpenId Connect Provider does not support any JWS algorithms.");
        }

        try {
            // get the preferred json web signature algorithm
            final String rawPreferredJwsAlgorithm = properties.getOidcPreferredJwsAlgorithm();

            final JWSAlgorithm preferredJwsAlgorithm;
            if (StringUtils.isBlank(rawPreferredJwsAlgorithm)) {
                preferredJwsAlgorithm = JWSAlgorithm.RS256;
            } else {
                if ("none".equalsIgnoreCase(rawPreferredJwsAlgorithm)) {
                    preferredJwsAlgorithm = null;
                } else {
                    preferredJwsAlgorithm = JWSAlgorithm.parse(rawPreferredJwsAlgorithm);
                }
            }

            if (preferredJwsAlgorithm == null) {
                tokenValidator = new IDTokenValidator(oidcProviderMetadata.getIssuer(), clientId);
            } else if (JWSAlgorithm.HS256.equals(preferredJwsAlgorithm) || JWSAlgorithm.HS384.equals(preferredJwsAlgorithm) || JWSAlgorithm.HS512.equals(preferredJwsAlgorithm)) {
                tokenValidator = new IDTokenValidator(oidcProviderMetadata.getIssuer(), clientId, preferredJwsAlgorithm, clientSecret);
            } else {
                final ResourceRetriever retriever = new DefaultResourceRetriever(oidcConnectTimeout, oidcReadTimeout);
                tokenValidator = new IDTokenValidator(oidcProviderMetadata.getIssuer(), clientId, preferredJwsAlgorithm, oidcProviderMetadata.getJWKSetURI().toURL(), retriever);
            }
        } catch (final Exception e) {
            throw new RuntimeException("Unable to create the ID token validator for the configured OpenId Connect Provider: " + e.getMessage(), e);
        }
    }

    /**
     * Loads the initial configuration values relating to the OIDC provider from the class {@link NiFiRegistryProperties} and populates the individual fields.
     */
    private void validateOIDCConfiguration() {
        if (properties.isLoginIdentityProviderEnabled()) {
            throw new RuntimeException("OpenId Connect support cannot be enabled if the Login Identity Provider or Apache Knox SSO is configured.");
        }

        // oidc connect timeout
        final String rawConnectTimeout = properties.getOidcConnectTimeout();
        try {
            oidcConnectTimeout = (int) FormatUtils.getPreciseTimeDuration(rawConnectTimeout, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Failed to parse value of property '{}' as a valid time period. Value was '{}'. Ignoring this value and using the default value of '{}'",
                    NiFiRegistryProperties.SECURITY_USER_OIDC_CONNECT_TIMEOUT, rawConnectTimeout, NiFiRegistryProperties.DEFAULT_SECURITY_USER_OIDC_CONNECT_TIMEOUT);
            oidcConnectTimeout = (int) FormatUtils.getPreciseTimeDuration(NiFiRegistryProperties.DEFAULT_SECURITY_USER_OIDC_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        // oidc read timeout
        final String rawReadTimeout = properties.getOidcReadTimeout();
        try {
            oidcReadTimeout = (int) FormatUtils.getPreciseTimeDuration(rawReadTimeout, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Failed to parse value of property '{}' as a valid time period. Value was '{}'. Ignoring this value and using the default value of '{}'",
                    NiFiRegistryProperties.SECURITY_USER_OIDC_READ_TIMEOUT, rawReadTimeout, NiFiRegistryProperties.DEFAULT_SECURITY_USER_OIDC_READ_TIMEOUT);
            oidcReadTimeout = (int) FormatUtils.getPreciseTimeDuration(NiFiRegistryProperties.DEFAULT_SECURITY_USER_OIDC_READ_TIMEOUT, TimeUnit.MILLISECONDS);
        }

        // client id
        final String rawClientId = properties.getOidcClientId();
        if (StringUtils.isBlank(rawClientId)) {
            throw new RuntimeException("Client ID is required when configuring an OIDC Provider.");
        }
        clientId = new ClientID(rawClientId);

        // client secret
        final String rawClientSecret = properties.getOidcClientSecret();
        if (StringUtils.isBlank(rawClientSecret)) {
            throw new RuntimeException("Client secret is required when configuring an OIDC Provider.");
        }
        clientSecret = new Secret(rawClientSecret);
    }

    /**
     * Returns the retrieved OIDC provider metadata from the external provider.
     *
     * @param discoveryUri the remote OIDC provider endpoint for service discovery
     * @return the provider metadata
     * @throws IOException    if there is a problem connecting to the remote endpoint
     * @throws ParseException if there is a problem parsing the response
     */
    private OIDCProviderMetadata retrieveOidcProviderMetadata(final String discoveryUri) throws IOException, ParseException {
        final URL url = URI.create(discoveryUri).toURL();
        final HTTPRequest httpRequest = new HTTPRequest(HTTPRequest.Method.GET, url);
        httpRequest.setConnectTimeout(oidcConnectTimeout);
        httpRequest.setReadTimeout(oidcReadTimeout);

        final HTTPResponse httpResponse = httpRequest.send();

        if (httpResponse.getStatusCode() != 200) {
            throw new IOException("Unable to download OpenId Connect Provider metadata from " + url + ": Status code " + httpResponse.getStatusCode());
        }

        final JSONObject jsonObject = httpResponse.getBodyAsJSONObject();
        return OIDCProviderMetadata.parse(jsonObject);
    }

    @Override
    public boolean isOidcEnabled() {
        return properties.isOidcEnabled();
    }

    @Override
    public URI getAuthorizationEndpoint() {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        return oidcProviderMetadata.getAuthorizationEndpointURI();
    }

    @Override
    public URI getEndSessionEndpoint() {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }
        return oidcProviderMetadata.getEndSessionEndpointURI();
    }

    @Override
    public URI getRevocationEndpoint() {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }
        return oidcProviderMetadata.getRevocationEndpointURI();
    }

    @Override
    public Scope getScope() {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        Scope scope = new Scope("openid", EMAIL_CLAIM);

        for (String additionalScope : properties.getOidcAdditionalScopes()) {
            // Scope automatically prevents duplicated entries
            scope.add(additionalScope);
        }

        return scope;
    }

    @Override
    public ClientID getClientId() {
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        return clientId;
    }

    @Override
    public String exchangeAuthorizationCodeForLoginAuthenticationToken(final AuthorizationGrant authorizationGrant) throws IOException {
        // Check if OIDC is enabled
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        // Build ClientAuthentication
        final ClientAuthentication clientAuthentication = createClientAuthentication();

        try {
            // Build the token request
            final HTTPRequest tokenHttpRequest = createTokenHTTPRequest(authorizationGrant, clientAuthentication);
            final TokenResponse response =  authorizeClient(tokenHttpRequest);
            return convertOIDCTokenToNiFiToken((OIDCTokenResponse) response);
        } catch (final ParseException | JOSEException | BadJOSEException | java.text.ParseException e) {
            throw new RuntimeException("Unable to parse the response from the Token request: " + e.getMessage());
        }
    }

    @Override
    public String exchangeAuthorizationCodeForAccessToken(final AuthorizationGrant authorizationGrant) throws Exception {
        // Check if OIDC is enabled
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        // Build ClientAuthentication
        final ClientAuthentication clientAuthentication = createClientAuthentication();

        try {
            // Build the token request
            final HTTPRequest tokenHttpRequest = createTokenHTTPRequest(authorizationGrant, clientAuthentication);
            final TokenResponse response = authorizeClient(tokenHttpRequest);
            return getAccessTokenString((OIDCTokenResponse) response);
        } catch (final ParseException | JOSEException | BadJOSEException | java.text.ParseException e) {
            throw new RuntimeException("Unable to parse the response from the Token request: " + e.getMessage());
        }
    }

    @Override
    public String exchangeAuthorizationCodeForIdToken(final AuthorizationGrant authorizationGrant) {
        // Check if OIDC is enabled
        if (!isOidcEnabled()) {
            throw new IllegalStateException(OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED);
        }

        // Build ClientAuthentication
        final ClientAuthentication clientAuthentication = createClientAuthentication();

        try {
            // Build the token request
            final HTTPRequest tokenHttpRequest = createTokenHTTPRequest(authorizationGrant, clientAuthentication);
            final TokenResponse response = authorizeClient(tokenHttpRequest);
            return getIdTokenString((OIDCTokenResponse) response);
        } catch (final RuntimeException | JOSEException | BadJOSEException | ParseException | IOException | java.text.ParseException e) {
            throw new RuntimeException("Unable to parse the response from the Token request: " + e.getMessage(), e);
        }
    }

    private TokenResponse authorizeClient(HTTPRequest tokenHttpRequest) throws ParseException, IOException, BadJOSEException, JOSEException, java.text.ParseException {
        // Get the token response
        final TokenResponse response = OIDCTokenResponseParser.parse(tokenHttpRequest.send());

        // Handle success
        if (response.indicatesSuccess()) {
            return response;
        } else {
            // If the response was not successful
            final TokenErrorResponse errorResponse = (TokenErrorResponse) response;
            throw new RuntimeException("An error occurred while invoking the Token endpoint: " +
                    errorResponse.getErrorObject().getDescription());
        }
    }

    private String convertOIDCTokenToNiFiToken(OIDCTokenResponse response) throws BadJOSEException, JOSEException, java.text.ParseException, IOException {
        final OIDCTokenResponse oidcTokenResponse = response;
        final OIDCTokens oidcTokens = oidcTokenResponse.getOIDCTokens();
        final JWT oidcJwt = oidcTokens.getIDToken();

        // validate the token - no nonce required for authorization code flow
        final IDTokenClaimsSet claimsSet = tokenValidator.validate(oidcJwt, null);

        // attempt to extract the configured claim to access the user's identity; default is 'email'
        String identityClaim = properties.getOidcClaimIdentifyingUser();
        String identity = claimsSet.getStringClaim(identityClaim);

        // Attempt to extract groups from the configured claim; default is 'groups'
        final String groupsClaim = properties.getOidcClaimGroups();
        final List<String> groups = claimsSet.getStringListClaim(groupsClaim);

        // If default identity not available, attempt secondary identity extraction
        if (StringUtils.isBlank(identity)) {
            // Provide clear message to admin that desired claim is missing and present available claims
            List<String> availableClaims = getAvailableClaims(oidcJwt.getJWTClaimsSet());
            logger.warn("Failed to obtain the identity of the user with the claim '{}'. The available claims on the OIDC response are: {}. Will attempt to obtain the identity from secondary sources",
                    identityClaim, availableClaims);

            // If the desired user claim was not "email" and "email" is present, use that
            if (!identityClaim.equalsIgnoreCase(EMAIL_CLAIM) && availableClaims.contains(EMAIL_CLAIM)) {
                identity = claimsSet.getStringClaim(EMAIL_CLAIM);
                logger.info("The 'email' claim was present. Using that claim to avoid extra remote call");
            } else {
                identity = retrieveIdentityFromUserInfoEndpoint(oidcTokens);
                logger.info("Retrieved identity from UserInfo endpoint");
            }
        }

        // extract expiration details from the claims set
        final Calendar now = Calendar.getInstance();
        final Date expiration = claimsSet.getExpirationTime();
        final long expiresIn = expiration.getTime() - now.getTimeInMillis();
        final String issuer = claimsSet.getIssuer().getValue();

        // convert into a nifi jwt for retrieval later
        return jwtService.generateSignedToken(identity, identity, issuer, issuer, expiresIn, groups);
    }

    private String retrieveIdentityFromUserInfoEndpoint(OIDCTokens oidcTokens) throws IOException {
        // explicitly try to get the identity from the UserInfo endpoint with the configured claim
        // extract the bearer access token
        final BearerAccessToken bearerAccessToken = oidcTokens.getBearerAccessToken();
        if (bearerAccessToken == null) {
            throw new IllegalStateException("No access token found in the ID tokens");
        }

        // invoke the UserInfo endpoint
        HTTPRequest userInfoRequest = createUserInfoRequest(bearerAccessToken);
        return lookupIdentityInUserInfo(userInfoRequest);
    }

    private HTTPRequest createTokenHTTPRequest(AuthorizationGrant authorizationGrant, ClientAuthentication clientAuthentication) {
        final TokenRequest request = new TokenRequest(oidcProviderMetadata.getTokenEndpointURI(), clientAuthentication, authorizationGrant);
        return formHTTPRequest(request);
    }

    private HTTPRequest createUserInfoRequest(BearerAccessToken bearerAccessToken) {
        final UserInfoRequest request = new UserInfoRequest(oidcProviderMetadata.getUserInfoEndpointURI(), bearerAccessToken);
        return formHTTPRequest(request);
    }

    private HTTPRequest formHTTPRequest(Request request) {
        final HTTPRequest httpRequest = request.toHTTPRequest();
        httpRequest.setConnectTimeout(oidcConnectTimeout);
        httpRequest.setReadTimeout(oidcReadTimeout);
        return httpRequest;
    }

    private ClientAuthentication createClientAuthentication() {
        final ClientAuthentication clientAuthentication;
        List<ClientAuthenticationMethod> authMethods = oidcProviderMetadata.getTokenEndpointAuthMethods();
        if (authMethods != null && authMethods.contains(ClientAuthenticationMethod.CLIENT_SECRET_POST)) {
            clientAuthentication = new ClientSecretPost(clientId, clientSecret);
        } else {
            clientAuthentication = new ClientSecretBasic(clientId, clientSecret);
        }
        return clientAuthentication;
    }

    private static List<String> getAvailableClaims(JWTClaimsSet claimSet) {
        // Get the claims available in the ID token response
        List<String> presentClaims = claimSet.getClaims().entrySet().stream()
                // Check claim values are not empty
                .filter(e -> StringUtils.isNotBlank(e.getValue().toString()))
                // If not empty, put claim name in a map
                .map(Map.Entry::getKey)
                .sorted()
                .collect(Collectors.toList());
        return presentClaims;
    }

    private String lookupIdentityInUserInfo(final HTTPRequest userInfoHttpRequest) throws IOException {
        try {
            // send the user request
            final UserInfoResponse response = UserInfoResponse.parse(userInfoHttpRequest.send());

            // interpret the details
            if (response.indicatesSuccess()) {
                final UserInfoSuccessResponse successResponse = (UserInfoSuccessResponse) response;

                final JWTClaimsSet claimsSet;
                if (successResponse.getUserInfo() != null) {
                    claimsSet = successResponse.getUserInfo().toJWTClaimsSet();
                } else {
                    claimsSet = successResponse.getUserInfoJWT().getJWTClaimsSet();
                }

                final String identity = claimsSet.getStringClaim(properties.getOidcClaimIdentifyingUser());

                // ensure we were able to get the user's identity
                if (StringUtils.isBlank(identity)) {
                    throw new IllegalStateException("Unable to extract identity from the UserInfo token using the claim '" +
                            properties.getOidcClaimIdentifyingUser() + "'.");
                } else {
                    return identity;
                }
            } else {
                final UserInfoErrorResponse errorResponse = (UserInfoErrorResponse) response;
                throw new IdentityAccessException("An error occurred while invoking the UserInfo endpoint: " + errorResponse.getErrorObject().getDescription());
            }
        } catch (final ParseException | java.text.ParseException e) {
            throw new IdentityAccessException("Unable to parse the response from the UserInfo token request: " + e.getMessage());
        }
    }

    private String getAccessTokenString(final OIDCTokenResponse response) throws Exception {
        final OIDCTokens oidcTokens = response.getOIDCTokens();

        // Validate the Access Token
        validateAccessToken(oidcTokens);

        // Return the Access Token String
        return oidcTokens.getAccessToken().getValue();
    }

    private String getIdTokenString(OIDCTokenResponse response) throws BadJOSEException, JOSEException {
        final OIDCTokens oidcTokens = response.getOIDCTokens();

        // Validate the Token - no nonce required for authorization code flow
        validateIdToken(oidcTokens.getIDToken());

        // Return the ID Token string
        return oidcTokens.getIDTokenString();
    }

    private void validateAccessToken(OIDCTokens oidcTokens) throws Exception {
        // Get the Access Token to validate
        final AccessToken accessToken = oidcTokens.getAccessToken();

        // Get the preferredJwsAlgorithm for validation
        final JWSAlgorithm jwsAlgorithm = extractJwsAlgorithm();

        // Get the accessTokenHash for validation
        final String atHashString = oidcTokens
                .getIDToken()
                .getJWTClaimsSet()
                .getStringClaim("at_hash");

        // Compute the Access Token hash
        final AccessTokenHash atHash = new AccessTokenHash(atHashString);

        try {
            // Validate the Token
            AccessTokenValidator.validate(accessToken, jwsAlgorithm, atHash);
        } catch (InvalidHashException e) {
            throw new Exception("Unable to validate the Access Token: " + e.getMessage());
        }
    }

    private IDTokenClaimsSet validateIdToken(JWT oidcJwt) throws BadJOSEException, JOSEException {
        try {
            return tokenValidator.validate(oidcJwt, null);
        } catch (BadJOSEException e) {
            throw new BadJOSEException("Unable to validate the ID Token: " + e.getMessage());
        }
    }

    private JWSAlgorithm extractJwsAlgorithm() {

        final String rawPreferredJwsAlgorithm = properties.getOidcPreferredJwsAlgorithm();

        final JWSAlgorithm preferredJwsAlgorithm;
        if (StringUtils.isBlank(rawPreferredJwsAlgorithm)) {
            preferredJwsAlgorithm = JWSAlgorithm.RS256;
        } else {
            if ("none".equalsIgnoreCase(rawPreferredJwsAlgorithm)) {
                preferredJwsAlgorithm = null;
            } else {
                preferredJwsAlgorithm = JWSAlgorithm.parse(rawPreferredJwsAlgorithm);
            }
        }
        return preferredJwsAlgorithm;
    }
}
