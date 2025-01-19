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
package org.apache.nifi.registry.web.api;

import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.openid.connect.sdk.AuthenticationErrorResponse;
import com.nimbusds.openid.connect.sdk.AuthenticationResponseParser;
import com.nimbusds.openid.connect.sdk.AuthenticationSuccessResponse;
import io.jsonwebtoken.JwtException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.nifi.registry.authorization.CurrentUser;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.exception.AdministrationException;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.authentication.AuthenticationRequest;
import org.apache.nifi.registry.security.authentication.AuthenticationResponse;
import org.apache.nifi.registry.security.authentication.BasicAuthIdentityProvider;
import org.apache.nifi.registry.security.authentication.IdentityProvider;
import org.apache.nifi.registry.security.authentication.IdentityProviderUsage;
import org.apache.nifi.registry.security.authentication.exception.IdentityAccessException;
import org.apache.nifi.registry.security.authentication.exception.InvalidCredentialsException;
import org.apache.nifi.registry.security.authorization.user.NiFiUser;
import org.apache.nifi.registry.security.authorization.user.NiFiUserUtils;
import org.apache.nifi.registry.util.FormatUtils;
import org.apache.nifi.registry.web.exception.UnauthorizedException;
import org.apache.nifi.registry.web.security.authentication.jwt.JwtService;
import org.apache.nifi.registry.web.security.authentication.kerberos.KerberosSpnegoIdentityProvider;
import org.apache.nifi.registry.web.security.authentication.oidc.OidcService;
import org.apache.nifi.registry.web.security.authentication.x509.X509IdentityProvider;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
@Path("/access")
@Tag(name = "Access")
public class AccessResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(AccessResource.class);

    private static final String OIDC_REQUEST_IDENTIFIER = "oidc-request-identifier";
    private static final String REVOKE_ACCESS_TOKEN_LOGOUT = "oidc_access_token_logout";
    private static final String ID_TOKEN_LOGOUT = "oidc_id_token_logout";
    private static final String STANDARD_LOGOUT = "oidc_standard_logout";

    private NiFiRegistryProperties properties;
    private JwtService jwtService;
    private OidcService oidcService;
    private X509IdentityProvider x509IdentityProvider;
    private KerberosSpnegoIdentityProvider kerberosSpnegoIdentityProvider;
    private IdentityProvider identityProvider;

    @Context
    protected UriInfo uriInfo;

    @Autowired
    public AccessResource(
            NiFiRegistryProperties properties,
            JwtService jwtService,
            X509IdentityProvider x509IdentityProvider,
            OidcService oidcService,
            @Nullable KerberosSpnegoIdentityProvider kerberosSpnegoIdentityProvider,
            @Nullable IdentityProvider identityProvider,
            ServiceFacade serviceFacade,
            EventService eventService) {
        super(serviceFacade, eventService);
        this.properties = properties;
        this.jwtService = jwtService;
        this.x509IdentityProvider = x509IdentityProvider;
        this.oidcService = oidcService;
        this.kerberosSpnegoIdentityProvider = kerberosSpnegoIdentityProvider;
        this.identityProvider = identityProvider;
    }

    /**
     * Gets the current client's identity and authorized permissions.
     *
     * @param httpServletRequest the servlet request
     * @return An object describing the current client identity, as determined by the server, and it's permissions.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(
            summary = "Get access status",
            description = "Returns the current client's authenticated identity and permissions to top-level resources",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = CurrentUser.class))),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry might be running unsecured.")
            }
    )
    public Response getAccessStatus(@Context HttpServletRequest httpServletRequest) {

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            // Not expected to happen unless the nifi registry server has been seriously misconfigured.
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        final CurrentUser currentUser = serviceFacade.getCurrentUser();
        currentUser.setLoginSupported(isBasicLoginSupported(httpServletRequest));
        currentUser.setOIDCLoginSupported(isOIDCLoginSupported(httpServletRequest));

        return generateOkResponse(currentUser).build();
    }

    /**
     * Creates a token for accessing the REST API.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token")
    @Operation(
            summary = "Create token trying all providers",
            description = "Creates a token for accessing the REST API via auto-detected method of verifying client identity claim credentials. " +
                    "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'.",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login with username/password."),
                    @ApiResponse(responseCode = "500", description = HttpStatusMessages.MESSAGE_500)
            }
    )
    public Response createAccessTokenByTryingAllProviders(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Access tokens are only issued over HTTPS");
        }

        List<IdentityProvider> identityProviderWaterfall = generateIdentityProviderWaterfall();

        String token = null;
        for (IdentityProvider provider : identityProviderWaterfall) {

            AuthenticationRequest authenticationRequest = provider.extractCredentials(httpServletRequest);
            if (authenticationRequest == null) {
                continue;
            }
            try {
                token = createAccessToken(provider, authenticationRequest);
                break;
            } catch (final InvalidCredentialsException ice) {
                logger.debug("{}: the supplied client credentials are invalid.", provider.getClass().getSimpleName());
                logger.debug("", ice);
            }

        }

        if (StringUtils.isEmpty(token)) {
            List<IdentityProviderUsage.AuthType> acceptableAuthTypes = identityProviderWaterfall.stream()
                    .map(IdentityProvider::getUsageInstructions)
                    .map(IdentityProviderUsage::getAuthType)
                    .filter(Objects::nonNull)
                    .distinct()
                    .collect(Collectors.toList());

            throw new UnauthorizedException("Client credentials are missing or invalid according to all configured identity providers.")
                    .withAuthenticateChallenge(acceptableAuthTypes);
        }

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateCreatedResponse(uri, token).build();
    }

    /**
     * Creates a token for accessing the REST API.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token/login")
    @Operation(
            summary = "Create token using basic auth",
            description = "Creates a token for accessing the REST API via username/password. The user credentials must be passed in standard HTTP Basic Auth format. " +
                    "That is: 'Authorization: Basic <credentials>', where <credentials> is the base64 encoded value of '<username>:<password>'. " +
                    "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'.",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login with username/password."),
                    @ApiResponse(responseCode = "500", description = HttpStatusMessages.MESSAGE_500)
            }
    )
    public Response createAccessTokenUsingBasicAuthCredentials(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Access tokens are only issued over HTTPS");
        }

        // if not configured with custom identity provider, or if provider doesn't support HTTP Basic Auth, don't consider credentials
        if (identityProvider == null) {
            logger.debug("An Identity Provider must be configured to use this endpoint. Please consult the administration guide.");
            throw new IllegalStateException("Username/Password login not supported by this NiFi. Contact System Administrator.");
        }
        if (!(identityProvider instanceof BasicAuthIdentityProvider)) {
            logger.debug("An Identity Provider is configured, but it does not support HTTP Basic Auth authentication. The configured Identity Provider must extend {}",
                    BasicAuthIdentityProvider.class);
            throw new IllegalStateException("Username/Password login not supported by this NiFi. Contact System Administrator.");
        }

        // generate JWT for response
        AuthenticationRequest authenticationRequest = identityProvider.extractCredentials(httpServletRequest);

        if (authenticationRequest == null) {
            throw new UnauthorizedException("The client credentials are missing from the request.")
                    .withAuthenticateChallenge(IdentityProviderUsage.AuthType.OTHER);
        }

        final String token;
        try {
            token = createAccessToken(identityProvider, authenticationRequest);
        } catch (final InvalidCredentialsException ice) {
            throw new UnauthorizedException("The supplied client credentials are not valid.", ice)
                    .withAuthenticateChallenge(IdentityProviderUsage.AuthType.OTHER);
        }

        // form the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateCreatedResponse(uri, token).build();
    }

    @DELETE
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/logout")
    @Operation(
            summary = "Performs a logout for other providers that have been issued a JWT.",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", description = "User was logged out successfully."),
                    @ApiResponse(responseCode = "401", description = "Authentication token provided was empty or not in the correct JWT format."),
                    @ApiResponse(responseCode = "500", description = "Client failed to log out."),
            }
    )
    public Response logout(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        final String userIdentity = NiFiUserUtils.getNiFiUserIdentity();

        if (userIdentity != null && !userIdentity.isEmpty()) {
            try {
                logger.info("Logging out user {}", userIdentity);
                jwtService.deleteKey(userIdentity);
                return generateOkResponse().build();
            } catch (final JwtException e) {
                logger.error("Logout of user {} failed", userIdentity, e);
                return Response.serverError().build();
            }
        } else {
            return Response.status(401, "Authentication token provided was empty or not in the correct JWT format.").build();
        }
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/logout/complete")
    @Operation(
            summary = "Completes the logout sequence.",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "200", description = "User was logged out successfully."),
                    @ApiResponse(responseCode = "401", description = "Authentication token provided was empty or not in the correct JWT format."),
                    @ApiResponse(responseCode = "500", description = "Client failed to log out."),
            }
    )
    public void logoutComplete(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws IOException {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User logout is only supported when running over HTTPS.");
        }

        // redirect to NiFi Registry page after logout completes
        httpServletResponse.sendRedirect(getNiFiRegistryUri());
    }

    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token/kerberos")
    @Operation(
            summary = "Create token using kerberos",
            description = "Creates a token for accessing the REST API via Kerberos Service Tickets or SPNEGO Tokens (which includes Kerberos Service Tickets). " +
                    "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'.",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login Kerberos credentials."),
                    @ApiResponse(responseCode = "500", description = HttpStatusMessages.MESSAGE_500)
            }
    )
    public Response createAccessTokenUsingKerberosTicket(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Access tokens are only issued over HTTPS");
        }

        // if not configured with custom identity provider, don't consider credentials
        if (!properties.isKerberosSpnegoSupportEnabled() || kerberosSpnegoIdentityProvider == null) {
            throw new IllegalStateException("Kerberos service ticket login not supported by this NiFi Registry");
        }

        AuthenticationRequest authenticationRequest = kerberosSpnegoIdentityProvider.extractCredentials(httpServletRequest);

        if (authenticationRequest == null) {
            throw new UnauthorizedException("The client credentials are missing from the request.")
                    .withAuthenticateChallenge(kerberosSpnegoIdentityProvider.getUsageInstructions().getAuthType());
        }

        final String token;
        try {
            token = createAccessToken(kerberosSpnegoIdentityProvider, authenticationRequest);
        } catch (final InvalidCredentialsException ice) {
            throw new UnauthorizedException("The supplied client credentials are not valid.", ice)
                    .withAuthenticateChallenge(kerberosSpnegoIdentityProvider.getUsageInstructions().getAuthType());
        }

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateCreatedResponse(uri, token).build();

    }

    /**
     * Creates a token for accessing the REST API using a custom identity provider configured using NiFi Registry extensions.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token/identity-provider")
    @Operation(
            summary = "Create token using identity provider",
            description = "Creates a token for accessing the REST API via a custom identity provider. " +
                    "The user credentials must be passed in a format understood by the custom identity provider, e.g., a third-party auth token in an HTTP header. " +
                    "The exact format of the user credentials expected by the custom identity provider can be discovered by 'GET /access/token/identity-provider/usage'. " +
                    "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. The token can be used in the Authorization header " +
                    "in the format 'Authorization: Bearer <token>'.",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = HttpStatusMessages.MESSAGE_401),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login with customized credentials."),
                    @ApiResponse(responseCode = "500", description = HttpStatusMessages.MESSAGE_500)
            }
    )
    public Response createAccessTokenUsingIdentityProviderCredentials(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Access tokens are only issued over HTTPS");
        }

        // if not configured with custom identity provider, don't consider credentials
        if (identityProvider == null) {
            throw new IllegalStateException("Custom login not supported by this NiFi Registry");
        }

        AuthenticationRequest authenticationRequest = identityProvider.extractCredentials(httpServletRequest);

        if (authenticationRequest == null) {
            throw new UnauthorizedException("The client credentials are missing from the request.")
                    .withAuthenticateChallenge(identityProvider.getUsageInstructions().getAuthType());
        }

        final String token;
        try {
            token = createAccessToken(identityProvider, authenticationRequest);
        } catch (InvalidCredentialsException ice) {
            throw new UnauthorizedException("The supplied client credentials are not valid.", ice)
                    .withAuthenticateChallenge(identityProvider.getUsageInstructions().getAuthType());
        }

        // build the response
        final URI uri = URI.create(generateResourceUri("access", "token"));
        return generateCreatedResponse(uri, token).build();

    }

    /**
     * Creates a token for accessing the REST API using a custom identity provider configured using NiFi Registry extensions.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token/identity-provider/usage")
    @Operation(
            summary = "Get identity provider usage",
            description = "Provides a description of how the currently configured identity provider expects credentials to be passed to POST /access/token/identity-provider",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login with customized credentials."),
                    @ApiResponse(responseCode = "500", description = HttpStatusMessages.MESSAGE_500)
            }
    )
    public Response getIdentityProviderUsageInstructions(@Context HttpServletRequest httpServletRequest) {

        // if not configuration for login, don't consider credentials
        if (identityProvider == null) {
            throw new IllegalStateException("Custom login not supported by this NiFi Registry");
        }

        Class ipClazz = identityProvider.getClass();
        String identityProviderName = StringUtils.isNotEmpty(ipClazz.getSimpleName()) ? ipClazz.getSimpleName() : ipClazz.getName();

        try {
            String usageInstructions = "Usage Instructions for '" + identityProviderName + "': ";
            usageInstructions += identityProvider.getUsageInstructions().getText();
            return generateOkResponse(usageInstructions).build();

        } catch (Exception e) {
            // If, for any reason, this identity provider does not support getUsageInstructions(), e.g., returns null or throws NotImplementedException.
            return Response.status(Response.Status.NOT_IMPLEMENTED)
                    .entity("The currently configured identity provider, '" + identityProvider.getClass().getName() + "' does not provide usage instructions.")
                    .build();
        }

    }

    /**
     * Creates a token for accessing the REST API using a custom identity provider configured using NiFi Registry extensions.
     *
     * @param httpServletRequest the servlet request
     * @return A JWT (string)
     */
    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token/identity-provider/test")
    @Operation(
            summary = "Test identity provider",
            description = "Tests the format of the credentials against this identity provider without preforming authentication on the credentials to validate them. " +
                    "The user credentials should be passed in a format understood by the custom identity provider as defined by 'GET /access/token/identity-provider/usage'.",
            responses = {
                    @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = HttpStatusMessages.MESSAGE_400),
                    @ApiResponse(responseCode = "401", description = "The format of the credentials were not recognized by the currently configured identity provider."),
                    @ApiResponse(responseCode = "409", description = HttpStatusMessages.MESSAGE_409 + " The NiFi Registry may not be configured to support login with customized credentials."),
                    @ApiResponse(responseCode = "500", description = HttpStatusMessages.MESSAGE_500)
            }
    )
    public Response testIdentityProviderRecognizesCredentialsFormat(@Context HttpServletRequest httpServletRequest) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("Access tokens are only issued over HTTPS");
        }

        // if not configured with custom identity provider, don't consider credentials
        if (identityProvider == null) {
            throw new IllegalStateException("Custom login not supported by this NiFi Registry");
        }

        final Class ipClazz = identityProvider.getClass();
        final String identityProviderName = StringUtils.isNotEmpty(ipClazz.getSimpleName()) ? ipClazz.getSimpleName() : ipClazz.getName();

        // attempt to extract client credentials without authenticating them
        AuthenticationRequest authenticationRequest = identityProvider.extractCredentials(httpServletRequest);

        if (authenticationRequest == null) {
            throw new UnauthorizedException("The format of the credentials were not recognized by the currently configured identity provider " +
                    "'" + identityProviderName + "'. " + identityProvider.getUsageInstructions().getText())
                    .withAuthenticateChallenge(identityProvider.getUsageInstructions().getAuthType());
        }


        final String successMessage = identityProviderName + " recognized the format of the credentials in the HTTP request.";
        return generateOkResponse(successMessage).build();

    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/oidc/request")
    @Operation(
            summary = "Initiates a request to authenticate through the configured OpenId Connect provider.",
            description = NON_GUARANTEED_ENDPOINT
    )
    public void oidcRequest(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            //forwardToMessagePage(httpServletRequest, httpServletResponse, "User authentication/authorization is only supported when running over HTTPS.");
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            //forwardToMessagePage(httpServletRequest, httpServletResponse, "OpenId Connect is not configured.");
            throw new IllegalStateException("OpenId Connect is not configured.");
        }

        final URI authorizationURI = oidcRequestAuthorizationCode(httpServletResponse, getOidcCallback());

        // generate the response
        httpServletResponse.sendRedirect(authorizationURI.toString());
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/oidc/callback")
    @Operation(
            summary = "Redirect/callback URI for processing the result of the OpenId Connect login sequence.",
            description = NON_GUARANTEED_ENDPOINT
    )
    public void oidcCallback(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            //forwardToMessagePage(httpServletRequest, httpServletResponse, "User authentication/authorization is only supported when running over HTTPS.");
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            //forwardToMessagePage(httpServletRequest, httpServletResponse, "OpenId Connect is not configured.");
            throw new IllegalStateException("OpenId Connect is not configured.");
        }

        final String oidcRequestIdentifier = getOidcRequestIdentifier(httpServletRequest);
        if (oidcRequestIdentifier == null) {
            throw new IllegalStateException("The login request identifier was not found in the request. Unable to continue.");
        }


        final com.nimbusds.openid.connect.sdk.AuthenticationResponse oidcResponse =
                parseAuthenticationResponse(getRequestUri(), httpServletResponse, true);

        if (oidcResponse.indicatesSuccess()) {
            final AuthenticationSuccessResponse successfulOidcResponse = (AuthenticationSuccessResponse) oidcResponse;

            validateOIDCState(oidcRequestIdentifier, successfulOidcResponse, httpServletResponse, true);

            try {
                // exchange authorization code for id token
                final AuthorizationCode authorizationCode = successfulOidcResponse.getAuthorizationCode();
                final AuthorizationGrant authorizationGrant = new AuthorizationCodeGrant(authorizationCode, URI.create(getOidcCallback()));
                oidcService.exchangeAuthorizationCodeForLoginAuthenticationToken(oidcRequestIdentifier, authorizationGrant);
            } catch (final Exception e) {
                logger.error("Unable to exchange authorization for ID token", e);

                // remove the oidc request cookie
                removeOidcRequestCookie(httpServletResponse);

                // forward to the error page
                throw new IllegalStateException("Unable to exchange authorization for ID token: " + e.getMessage());
            }

            // redirect to the name page
            httpServletResponse.sendRedirect(getNiFiRegistryUri());
        } else {
            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            // report the unsuccessful login
            final AuthenticationErrorResponse errorOidcResponse = (AuthenticationErrorResponse) oidcResponse;
            throw new IllegalStateException("Unsuccessful login attempt: " + errorOidcResponse.getErrorObject().getDescription());
        }
    }

    @POST
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/oidc/exchange")
    @Operation(
            summary = "Retrieves a JWT following a successful login sequence using the configured OpenId Connect provider.",
            description = NON_GUARANTEED_ENDPOINT
    )
    public Response oidcExchange(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        // only consider user specific access over https
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        // ensure oidc is enabled
        if (!oidcService.isOidcEnabled()) {
            throw new IllegalStateException("OpenId Connect is not configured.");
        }

        final String oidcRequestIdentifier = getOidcRequestIdentifier(httpServletRequest);
        if (oidcRequestIdentifier == null) {
            throw new IllegalArgumentException("The login request identifier was not found in the request. Unable to continue.");
        }

        // remove the oidc request cookie
        removeOidcRequestCookie(httpServletResponse);

        // get the jwt
        final String jwt = oidcService.getJwt(oidcRequestIdentifier);
        if (jwt == null) {
            throw new IllegalArgumentException("A JWT for this login request identifier could not be found. Unable to continue.");
        }

        // generate the response
        return generateOkResponse(jwt).build();
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/oidc/logout")
    @Operation(
            summary = "Performs a logout in the OpenId Provider.",
            description = NON_GUARANTEED_ENDPOINT
    )
    public void oidcLogout(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        if (!oidcService.isOidcEnabled()) {
            throw new IllegalStateException("OpenId Connect is not configured.");
        }

        // Checks if OIDC service supports logout using either by invoking the revocation endpoint (for OAuth 2.0 providers)
        // or the end session endpoint (for OIDC providers). If either is supported, send a request to get an authorization
        // code that can be eventually exchanged for a token that is required as a parameter for the logout request.
        final String logoutMethod = determineLogoutMethod();
        switch (logoutMethod) {
            case REVOKE_ACCESS_TOKEN_LOGOUT:
            case ID_TOKEN_LOGOUT:
                final URI authorizationURI = oidcRequestAuthorizationCode(httpServletResponse, getOidcLogoutCallback());
                httpServletResponse.sendRedirect(authorizationURI.toString());
                break;
            default:
                // If the above logout methods are not supported, last ditch effort to logout by providing the client_id,
                // to the end session endpoint if it exists. This is a way to logout defined in the OIDC specs, but the
                // id_token_hint logout method is recommended. This option is not available when using the POST request
                // to the revocation endpoint (OAuth 2.0 providers).
                final URI endSessionEndpoint = oidcService.getEndSessionEndpoint();
                if (endSessionEndpoint != null) {
                    final String postLogoutRedirectUri = getNiFiRegistryUri();
                    final URI logoutUri = UriBuilder.fromUri(endSessionEndpoint)
                            .queryParam("post_logout_redirect_uri", postLogoutRedirectUri)
                            .queryParam("client_id", oidcService.getClientId())
                            .build();
                    httpServletResponse.sendRedirect(logoutUri.toString());
                } else {
                    throw new IllegalStateException("Unable to initiate logout: Logout method unrecognized");
                }
                break;
        }
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/oidc/logout/callback")
    @Operation(
            summary = "Redirect/callback URI for processing the result of the OpenId Connect logout sequence.",
            description = NON_GUARANTEED_ENDPOINT
    )
    public void oidcLogoutCallback(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User logout is only supported when running over HTTPS.");
        }

        if (!oidcService.isOidcEnabled()) {
            throw new IllegalStateException("OpenId Connect is not configured.");
        }

        final String oidcRequestIdentifier = getOidcRequestIdentifier(httpServletRequest);
        if (oidcRequestIdentifier == null) {
            throw new IllegalStateException("The OIDC request identifier was not found in the request. Unable to continue.");
        }

        final com.nimbusds.openid.connect.sdk.AuthenticationResponse oidcResponse =
                parseAuthenticationResponse(getRequestUri(), httpServletResponse, false);

        if (oidcResponse.indicatesSuccess()) {
            final AuthenticationSuccessResponse successfulOidcResponse = (AuthenticationSuccessResponse) oidcResponse;

            validateOIDCState(oidcRequestIdentifier, successfulOidcResponse, httpServletResponse, false);

            try {
                // exchange authorization code for id token
                final AuthorizationCode authorizationCode = successfulOidcResponse.getAuthorizationCode();
                final AuthorizationGrant authorizationGrant = new AuthorizationCodeGrant(authorizationCode, URI.create(getOidcLogoutCallback()));

                final String logoutMethod = determineLogoutMethod();
                switch (logoutMethod) {
                    case REVOKE_ACCESS_TOKEN_LOGOUT:
                        final String accessToken;
                        try {
                            accessToken = oidcService.exchangeAuthorizationCodeForAccessToken(authorizationGrant);
                        } catch (final Exception e) {
                            final String errorMsg = "Unable to exchange authorization for the access token: " + e.getMessage();
                            logger.error(errorMsg, e);

                            throw new IllegalStateException(errorMsg);
                        }

                        final URI revokeEndpoint = oidcService.getRevocationEndpoint();
                        try {
                            revokeEndpointRequest(httpServletResponse, accessToken, revokeEndpoint);
                        } catch (final IOException e) {
                            final String errorMsg = "There was an error logging out of the OpenID Connect Provider: " + e.getMessage();
                            logger.error(errorMsg, e);

                            throw new IllegalStateException(errorMsg);
                        }
                        break;
                    case ID_TOKEN_LOGOUT:
                        final String idToken;
                        try {
                            idToken = oidcService.exchangeAuthorizationCodeForIdToken(authorizationGrant);
                        } catch (final Exception e) {
                            final String errorMsg = "Unable to exchange authorization for the ID token: " + e.getMessage();
                            logger.error(errorMsg, e);

                            throw new IllegalStateException(errorMsg);
                        }

                        final URI endSessionEndpoint = oidcService.getEndSessionEndpoint();
                        final String postLogoutRedirectUri = getNiFiRegistryUri();
                        final URI logoutUri = UriBuilder.fromUri(endSessionEndpoint)
                                .queryParam("id_token_hint", idToken)
                                .queryParam("post_logout_redirect_uri", postLogoutRedirectUri)
                                .build();
                        httpServletResponse.sendRedirect(logoutUri.toString());
                        break;
                    default:
                        // there should be no other way to logout at this point, return error
                        throw new IllegalStateException("Unable to complete logout: Logout method unrecognized");
                }
            } catch (final Exception e) {
                logger.error(e.getMessage(), e);

                removeOidcRequestCookie(httpServletResponse);

                throw e;
            }
        } else {
            removeOidcRequestCookie(httpServletResponse);

            // report the unsuccessful logout
            final AuthenticationErrorResponse errorOidcResponse = (AuthenticationErrorResponse) oidcResponse;
            throw new IllegalStateException("Unsuccessful logout attempt: " + errorOidcResponse.getErrorObject().getDescription());
        }
    }

    /**
     * Gets the value of a cookie matching the specified name. If no cookie with that name exists, null is returned.
     *
     * @param cookies the cookies
     * @param name the name of the cookie
     * @return the value of the corresponding cookie, or null if the cookie does not exist
     */
    private String getCookieValue(final Cookie[] cookies, final String name) {
        if (cookies != null) {
            for (final Cookie cookie : cookies) {
                if (name.equals(cookie.getName())) {
                    return cookie.getValue();
                }
            }
        }

        return null;
    }

    public void setOidcService(OidcService oidcService) {
        this.oidcService = oidcService;
    }

    private String getOidcCallback() {
        return generateResourceUri("access", "oidc", "callback");
    }

    private String getOidcLogoutCallback() {
        return generateResourceUri("access", "oidc", "logout", "callback");
    }

    private void removeOidcRequestCookie(final HttpServletResponse httpServletResponse) {
        final Cookie cookie = new Cookie(OIDC_REQUEST_IDENTIFIER, null);
        cookie.setPath("/");
        cookie.setHttpOnly(true);
        cookie.setMaxAge(0);
        cookie.setSecure(true);
        httpServletResponse.addCookie(cookie);
    }

    protected URI getRequestUri() {
        return uriInfo.getRequestUri();
    }

    private String createAccessToken(IdentityProvider identityProvider, AuthenticationRequest authenticationRequest)
            throws InvalidCredentialsException, AdministrationException {

        final AuthenticationResponse authenticationResponse;

        try {
            authenticationResponse = identityProvider.authenticate(authenticationRequest);
            final String token = jwtService.generateSignedToken(authenticationResponse);
            return token;
        } catch (final IdentityAccessException | JwtException e) {
            throw new AdministrationException(e.getMessage());
        }

    }

    /**
     * A helper function that generates a prioritized list of IdentityProviders to use to
     * attempt client authentication.
     * <p>
     * Note: This is currently a hard-coded list order consisting of:
     * <p>
     * - X509IdentityProvider (if available)
     * - KerberosProvider (if available)
     * - User-defined IdentityProvider (if available)
     * <p>
     * However, in the future it could be entirely user-configurable
     *
     * @return a list of providers to use in order to authenticate the client.
     */
    private List<IdentityProvider> generateIdentityProviderWaterfall() {
        List<IdentityProvider> identityProviderWaterfall = new ArrayList<>();

        // if configured with an X509IdentityProvider, add it to the list of providers to try
        if (x509IdentityProvider != null) {
            identityProviderWaterfall.add(x509IdentityProvider);
        }

        // if configured with an KerberosSpnegoIdentityProvider, add it to the end of the list of providers to try
        if (kerberosSpnegoIdentityProvider != null) {
            identityProviderWaterfall.add(kerberosSpnegoIdentityProvider);
        }

        // if configured with custom identity provider, add it to the end of the list of providers to try
        if (identityProvider != null) {
            identityProviderWaterfall.add(identityProvider);
        }

        return identityProviderWaterfall;
    }

    private boolean isBasicLoginSupported(HttpServletRequest request) {
        return request.isSecure() && identityProvider != null;
    }

    private boolean isOIDCLoginSupported(HttpServletRequest request) {
        return request.isSecure() && oidcService != null && oidcService.isOidcEnabled();
    }

    private String determineLogoutMethod() {
        if (oidcService.getEndSessionEndpoint() != null) {
            return ID_TOKEN_LOGOUT;
        } else if (oidcService.getRevocationEndpoint() != null) {
            return REVOKE_ACCESS_TOKEN_LOGOUT;
        } else {
            return STANDARD_LOGOUT;
        }
    }

    /**
     * Generates the request Authorization URI for the OpenID Connect Provider. Returns an authorization
     * URI using the provided callback URI.
     *
     * @param httpServletResponse the servlet response
     * @param callback the OIDC callback URI
     * @return the authorization URI
     */
    private URI oidcRequestAuthorizationCode(@Context final HttpServletResponse httpServletResponse, final String callback) {
        final String oidcRequestIdentifier = UUID.randomUUID().toString();
        // generate a cookie to associate this login sequence
        final Cookie cookie = new Cookie(OIDC_REQUEST_IDENTIFIER, oidcRequestIdentifier);
        cookie.setPath("/");
        cookie.setHttpOnly(true);
        cookie.setMaxAge(60);
        cookie.setSecure(true);
        httpServletResponse.addCookie(cookie);

        // get the state for this request
        final State state = oidcService.createState(oidcRequestIdentifier);

        // build the authorization uri
        final URI authorizationUri = UriBuilder.fromUri(oidcService.getAuthorizationEndpoint())
                .queryParam("client_id", oidcService.getClientId())
                .queryParam("response_type", "code")
                .queryParam("scope", oidcService.getScope().toString())
                .queryParam("state", state.getValue())
                .queryParam("redirect_uri", callback)
                .build();
        return authorizationUri;
    }

    private String getOidcRequestIdentifier(final HttpServletRequest httpServletRequest) {
        return getCookieValue(httpServletRequest.getCookies(), OIDC_REQUEST_IDENTIFIER);
    }

    private com.nimbusds.openid.connect.sdk.AuthenticationResponse parseAuthenticationResponse(final URI requestUri,
                                                                                               final HttpServletResponse httpServletResponse,
                                                                                               final boolean isLogin) {
        final com.nimbusds.openid.connect.sdk.AuthenticationResponse oidcResponse;
        try {
            oidcResponse = AuthenticationResponseParser.parse(requestUri);
        } catch (final ParseException e) {
            final String loginOrLogoutString = isLogin ? "login" : "logout";
            logger.error("Unable to parse the redirect URI from the OpenId Connect Provider. Unable to continue {} process.", loginOrLogoutString);

            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            throw new IllegalStateException(String.format("Unable to parse the redirect URI from the OpenId Connect Provider. Unable to continue %s process.", loginOrLogoutString));
        }
        return oidcResponse;
    }

    private void validateOIDCState(final String oidcRequestIdentifier,
                                   final AuthenticationSuccessResponse successfulOidcResponse,
                                   final HttpServletResponse httpServletResponse,
                                   final boolean isLogin) {
        // confirm state
        final State state = successfulOidcResponse.getState();
        if (state == null || !oidcService.isStateValid(oidcRequestIdentifier, state)) {
            final String loginOrLogoutMessage = isLogin ? "login" : "logout";
            logger.error("The state value returned by the OpenId Connect Provider does not match the stored state. Unable to continue {} process.", loginOrLogoutMessage);

            // remove the oidc request cookie
            removeOidcRequestCookie(httpServletResponse);

            throw new IllegalStateException(String.format("Proposed state does not match the stored state. Unable to continue %s process.", loginOrLogoutMessage));
        }
    }

    /**
     * Sends a POST request to the revoke endpoint to log out of the ID Provider.
     *
     * @param httpServletResponse the servlet response
     * @param accessToken the OpenID Connect Provider access token
     * @param revokeEndpoint the name of the cookie
     * @throws IOException exceptional case for communication error with the OpenId Connect Provider
     */
    private void revokeEndpointRequest(@Context HttpServletResponse httpServletResponse, String accessToken, URI revokeEndpoint) throws IOException, NoSuchAlgorithmException {
        final CloseableHttpClient httpClient = getHttpClient();
        HttpPost httpPost = new HttpPost(revokeEndpoint);

        List<NameValuePair> params = new ArrayList<>();
        // Append a query param with the access token
        params.add(new BasicNameValuePair("token", accessToken));
        httpPost.setEntity(new UrlEncodedFormEntity(params));

        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
            if (response.getStatusLine().getStatusCode() == HTTPResponse.SC_OK) {
                // redirect to NiFi Registry page after logout completes
                logger.debug("You are logged out of the OpenId Connect Provider.");
                final String postLogoutRedirectUri = getNiFiRegistryUri();
                httpServletResponse.sendRedirect(postLogoutRedirectUri);
            } else {
                logger.error("There was an error logging out of the OpenId Connect Provider. Response status: {}", response.getStatusLine().getStatusCode());
            }
        } finally {
            httpClient.close();
        }
    }

    private CloseableHttpClient getHttpClient() throws NoSuchAlgorithmException {
        final String rawConnectTimeout = properties.getOidcConnectTimeout();
        final String rawReadTimeout = properties.getOidcReadTimeout();
        final int oidcConnectTimeout = (int) FormatUtils.getPreciseTimeDuration(rawConnectTimeout, TimeUnit.MILLISECONDS);
        final int oidcReadTimeout = (int) FormatUtils.getPreciseTimeDuration(rawReadTimeout, TimeUnit.MILLISECONDS);

        final RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(oidcConnectTimeout)
                .setConnectionRequestTimeout(oidcReadTimeout)
                .setSocketTimeout(oidcReadTimeout)
                .build();

        final HttpClientBuilder builder = HttpClientBuilder
                .create()
                .setDefaultRequestConfig(config)
                .setSSLContext(SSLContext.getDefault());

        return builder.build();
    }
}
