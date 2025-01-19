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
package org.apache.nifi.web.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.admin.service.AdministrationException;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.exception.AuthenticationNotSupportedException;
import org.apache.nifi.authentication.exception.IdentityAccessException;
import org.apache.nifi.authentication.exception.InvalidLoginCredentialsException;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.authorization.util.IdentityMappingUtil;
import org.apache.nifi.web.security.LogoutException;
import org.apache.nifi.web.security.cookie.ApplicationCookieName;
import org.apache.nifi.web.security.jwt.provider.BearerTokenProvider;
import org.apache.nifi.web.security.jwt.revocation.JwtLogoutListener;
import org.apache.nifi.web.security.logout.LogoutRequest;
import org.apache.nifi.web.security.logout.LogoutRequestManager;
import org.apache.nifi.web.security.token.LoginAuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.server.resource.web.BearerTokenResolver;
import org.springframework.stereotype.Controller;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

/**
 * RESTful endpoint for managing access.
 */
@Controller
@Path("/access")
@Tag(name = "Access")
public class AccessResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(AccessResource.class);
    protected static final String AUTHENTICATION_NOT_ENABLED_MSG = "User authentication/authorization is only supported when running over HTTPS.";

    private LoginIdentityProvider loginIdentityProvider;
    private JwtLogoutListener jwtLogoutListener;
    private BearerTokenProvider bearerTokenProvider;
    private BearerTokenResolver bearerTokenResolver;
    private LogoutRequestManager logoutRequestManager;

    /**
     * Creates a token for accessing the REST API via username/password stored as a cookie in the browser.
     *
     * @param httpServletRequest the servlet request
     * @param username the username
     * @param password the password
     * @return A JWT (string) in a cookie and as the body
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/token")
    @Operation(
            summary = "Creates a token for accessing the REST API via username/password",
            description = "The token returned is formatted as a JSON Web Token (JWT). The token is base64 encoded and comprised of three parts. The header, " +
                    "the body, and the signature. The expiration of the token is a contained within the body. It is stored in the browser as a cookie, but also returned in" +
                    "the response body to be stored/used by third party client scripts.",
            responses = {
                    @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(implementation = String.class))),
                    @ApiResponse(responseCode = "400", description = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(responseCode = "403", description = "Client is not authorized to make this request."),
                    @ApiResponse(responseCode = "409", description = "The request was valid but NiFi was not in the appropriate state to process it."),
                    @ApiResponse(responseCode = "500", description = "Unable to create access token because an unexpected error occurred.")
            }
    )
    public Response createAccessToken(
            @Context final HttpServletRequest httpServletRequest,
            @Context final HttpServletResponse httpServletResponse,
            @FormParam("username") final String username,
            @FormParam("password") final String password) {

        // only support access tokens when communicating over HTTPS
        if (!httpServletRequest.isSecure()) {
            throw new AuthenticationNotSupportedException("Access tokens are only issued over HTTPS.");
        }

        // if not configuration for login, don't consider credentials
        if (loginIdentityProvider == null) {
            throw new IllegalStateException("Username/Password login not supported by this NiFi.");
        }

        final LoginAuthenticationToken loginAuthenticationToken;

        // ensure we have login credentials
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            throw new IllegalArgumentException("The username and password must be specified.");
        }

        try {
            // attempt to authenticate
            final AuthenticationResponse authenticationResponse = loginIdentityProvider.authenticate(new LoginCredentials(username, password));
            final String rawIdentity = authenticationResponse.getIdentity();
            final String mappedIdentity = IdentityMappingUtil.mapIdentity(rawIdentity, IdentityMappingUtil.getIdentityMappings(properties));
            final long expirationDuration = authenticationResponse.getExpiration();
            final Instant expiration = Instant.now().plusMillis(expirationDuration);

            // create the authentication token
            loginAuthenticationToken = new LoginAuthenticationToken(mappedIdentity, expiration, Collections.emptySet());
        } catch (final InvalidLoginCredentialsException ilce) {
            throw new IllegalArgumentException("The supplied username and password are not valid.", ilce);
        } catch (final IdentityAccessException iae) {
            throw new AdministrationException(iae.getMessage(), iae);
        }

        final String bearerToken = bearerTokenProvider.getBearerToken(loginAuthenticationToken);
        final URI uri = URI.create(generateResourceUri("access", "token"));
        setBearerToken(httpServletResponse, bearerToken);
        return generateCreatedResponse(uri, bearerToken).build();
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
    public Response logOut(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException(AUTHENTICATION_NOT_ENABLED_MSG);
        }

        final String mappedUserIdentity = NiFiUserUtils.getNiFiUserIdentity();
        if (StringUtils.isBlank(mappedUserIdentity)) {
            return Response.status(Response.Status.UNAUTHORIZED)
                    .entity("Authentication token provided was empty or not in the correct JWT format.").build();
        }

        try {
            final String requestIdentifier = UUID.randomUUID().toString();
            logger.info("Logout Request [{}] Identity [{}] started", requestIdentifier, mappedUserIdentity);
            applicationCookieService.removeCookie(getCookieResourceUri(), httpServletResponse, ApplicationCookieName.AUTHORIZATION_BEARER);

            final String bearerToken = bearerTokenResolver.resolve(httpServletRequest);
            jwtLogoutListener.logout(bearerToken);

            // create a LogoutRequest and tell the LogoutRequestManager about it for later retrieval
            final LogoutRequest logoutRequest = new LogoutRequest(requestIdentifier, mappedUserIdentity);
            logoutRequestManager.start(logoutRequest);

            applicationCookieService.addCookie(getCookieResourceUri(), httpServletResponse, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER, logoutRequest.getRequestIdentifier());
            return generateOkResponse().build();
        } catch (final LogoutException e) {
            logger.error("Logout Failed Identity [{}]", mappedUserIdentity, e);
            return Response.serverError().build();
        }
    }

    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.WILDCARD)
    @Path("/logout/complete")
    @Operation(
            summary = "Completes the logout sequence by removing the cached Logout Request and Cookie if they existed and redirects to /nifi/login.",
            description = NON_GUARANTEED_ENDPOINT,
            responses = {
                    @ApiResponse(responseCode = "302", description = "User was logged out successfully."),
                    @ApiResponse(responseCode = "401", description = "Authentication token provided was empty or not in the correct JWT format."),
                    @ApiResponse(responseCode = "500", description = "Client failed to log out."),
            }
    )
    public void logOutComplete(@Context HttpServletRequest httpServletRequest, @Context HttpServletResponse httpServletResponse) throws Exception {
        if (!httpServletRequest.isSecure()) {
            throw new IllegalStateException("User authentication/authorization is only supported when running over HTTPS.");
        }

        // complete the logout request by removing the cookie and cached request, if they were present
        completeLogoutRequest(httpServletResponse);

        // redirect to logout landing page
        httpServletResponse.sendRedirect(getNiFiLogoutCompleteUri());
    }

    private LogoutRequest completeLogoutRequest(final HttpServletResponse httpServletResponse) {
        LogoutRequest logoutRequest = null;

        final Optional<String> cookieValue = getLogoutRequestIdentifier();
        if (cookieValue.isPresent()) {
            final String logoutRequestIdentifier = cookieValue.get();
            logoutRequest = logoutRequestManager.complete(logoutRequestIdentifier);
            logger.info("Logout Request [{}] Completed [{}]", logoutRequestIdentifier, logoutRequest.getMappedUserIdentity());
        } else {
            logger.warn("Logout Request Cookie [{}] not found", ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER.getCookieName());
        }

        removeLogoutRequestCookie(httpServletResponse);
        return logoutRequest;
    }

    private String getNiFiLogoutCompleteUri() {
        return getNiFiUri() + "logout-complete";
    }

    /**
     * Send Set-Cookie header to remove Logout Request Identifier cookie from client
     *
     * @param httpServletResponse HTTP Servlet Response
     */
    private void removeLogoutRequestCookie(final HttpServletResponse httpServletResponse) {
        applicationCookieService.removeCookie(getCookieResourceUri(), httpServletResponse, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER);
    }

    /**
     * Get Logout Request Identifier from current HTTP Request Cookie header
     *
     * @return Optional Logout Request Identifier
     */
    private Optional<String> getLogoutRequestIdentifier() {
        return applicationCookieService.getCookieValue(httpServletRequest, ApplicationCookieName.LOGOUT_REQUEST_IDENTIFIER);
    }

    @Autowired(required = false)
    public void setLoginIdentityProvider(LoginIdentityProvider loginIdentityProvider) {
        this.loginIdentityProvider = loginIdentityProvider;
    }

    @Autowired
    public void setBearerTokenProvider(final BearerTokenProvider bearerTokenProvider) {
        this.bearerTokenProvider = bearerTokenProvider;
    }

    @Autowired
    public void setBearerTokenResolver(final BearerTokenResolver bearerTokenResolver) {
        this.bearerTokenResolver = bearerTokenResolver;
    }

    @Autowired
    public void setJwtLogoutListener(final JwtLogoutListener jwtLogoutListener) {
        this.jwtLogoutListener = jwtLogoutListener;
    }

    @Autowired
    public void setLogoutRequestManager(LogoutRequestManager logoutRequestManager) {
        this.logoutRequestManager = logoutRequestManager;
    }
}
