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
package org.apache.nifi.registry.security.authentication;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * Authentication response for a user login attempt.
 */
public class AuthenticationResponse implements Serializable {

    private final String identity;
    private final String username;
    private final long expiration;
    private final String issuer;
    private final Set<String> groups;

    /**
     * Creates an authentication response. The username and how long the authentication is valid in milliseconds
     *
     * @param identity The user identity
     * @param username The username
     * @param expiration The expiration in milliseconds
     * @param issuer The issuer of the token
     */
    public AuthenticationResponse(final String identity, final String username, final long expiration, final String issuer) {
        this(identity, username, expiration, issuer, Collections.emptySet());
    }

    /**
     * Creates an authentication response. The username and how long the authentication is valid in milliseconds
     *
     * @param identity The user identity
     * @param username The username
     * @param expiration The expiration in milliseconds
     * @param issuer The issuer of the token
     * @param groups The user groups
     */
    public AuthenticationResponse(final String identity, final String username, final long expiration, final String issuer, final Set<String> groups) {
        this.identity = identity;
        this.username = username;
        this.expiration = expiration;
        this.issuer = issuer;
        this.groups = groups;
    }

    public String getIdentity() {
        return identity;
    }

    public String getUsername() {
        return username;
    }

    public String getIssuer() {
        return issuer;
    }

    /**
     * Returns the expiration of a given authentication in milliseconds.
     *
     * @return The expiration in milliseconds
     */
    public long getExpiration() {
        return expiration;
    }

    public Set<String> getGroups() {
        return groups;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AuthenticationResponse that = (AuthenticationResponse) o;

        if (expiration != that.expiration) return false;
        if (identity != null ? !identity.equals(that.identity) : that.identity != null) return false;
        if (username != null ? !username.equals(that.username) : that.username != null) return false;
        return issuer != null ? issuer.equals(that.issuer) : that.issuer == null;
    }

    @Override
    public int hashCode() {
        int result = identity != null ? identity.hashCode() : 0;
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (int) (expiration ^ (expiration >>> 32));
        result = 31 * result + (issuer != null ? issuer.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "AuthenticationResponse{" +
                "identity='" + identity + '\'' +
                ", username='" + username + '\'' +
                ", expiration=" + expiration +
                ", issuer='" + issuer + '\'' +
                '}';
    }
}
