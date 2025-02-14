/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.atlassian.bitbucket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.client.GitCommit;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.StandardHttpContentType;
import org.apache.nifi.web.client.api.StandardMultipartFormDataStreamBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Implementation of {@link GitRepositoryClient} for BitBucket.
 */
public class BitBucketRepositoryClient implements GitRepositoryClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(BitBucketRepositoryClient.class);

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String BASIC = "Basic";
    private static final String BEARER = "Bearer";

    private final ObjectMapper objectMapper = JsonMapper.builder().build();

    private final String apiUrl;
    private final String apiVersion;
    private final String clientId;
    private final String workspace;
    private final String repoName;
    private final String repoPath;
    private WebClientServiceProvider webClient;
    private BitBucketToken<String> authToken;

    private final boolean canRead;
    private final boolean canWrite;

    private BitBucketRepositoryClient(final Builder builder) throws FlowRegistryException {
        webClient = Objects.requireNonNull(builder.webClient, "Web Client is required");
        workspace = Objects.requireNonNull(builder.workspace, "Workspace is required");
        repoName = Objects.requireNonNull(builder.repoName, "Repository Name is required");

        apiUrl = Objects.requireNonNull(builder.apiUrl, "API Instance is required");
        apiVersion = Objects.requireNonNull(builder.apiVersion, "API Version is required");

        final BitBucketAuthenticationType authenticationType = Objects.requireNonNull(builder.authenticationType, "Authentication type is required");

        switch (authenticationType) {
        case ACCESS_TOKEN -> {
            Objects.requireNonNull(builder.accessToken, "Access Token is required");
            authToken = new AccessToken(builder.accessToken);
        }
        case BASIC_AUTH -> {
            Objects.requireNonNull(builder.username, "Username is required");
            Objects.requireNonNull(builder.appPassword, "App Password URL is required");
            authToken = new BasicAuthToken(builder.username, builder.appPassword);
        }
        case OAUTH2 -> {
            Objects.requireNonNull(builder.oauthService, "OAuth 2.0 Token Provider is required");
            authToken = new OAuthToken(builder.oauthService);
        }
        }

        clientId = Objects.requireNonNull(builder.clientId, "Client ID is required");
        repoPath = builder.repoPath;

        final String permission = checkRepoPermissions();

        switch (permission) {
        case "admin", "write" -> {
            canRead = true;
            canWrite = true;
        }
        case "read" -> {
            canRead = true;
            canWrite = false;
        }
        case "none" -> {
            canRead = false;
            canWrite = false;
        }
        default -> {
            canRead = false;
            canWrite = false;
        }
        }

        LOGGER.info("Created {} for clientId = [{}], repository [{}]", getClass().getSimpleName(), clientId, repoName);
    }

    @Override
    public boolean hasReadPermission() {
        return canRead;
    }

    @Override
    public boolean hasWritePermission() {
        return canWrite;
    }

    /**
     * @return the name of the workspace
     */
    public String getWorkspace() {
        return workspace;
    }

    /**
     * @return the name of the repository
     */
    public String getRepoName() {
        return repoName;
    }

    @Override
    public Set<String> getBranches() throws FlowRegistryException {
        LOGGER.debug("Getting branches for repository [{}]", repoName);

        // https://api.bitbucket.org/2.0/repositories/{workspace}/{repoName}/refs/branches
        URI uri = getUriBuilder().addPathSegment("refs").addPathSegment("branches").build();
        HttpResponseEntity response = this.webClient.getWebClientService().get().uri(uri).header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue()).retrieve();

        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            throw new FlowRegistryException(String.format("Error while listing branches for repository [%s]: %s", repoName, getErrorMessage(response)));
        }

        JsonNode jsonResponse;
        try {
            jsonResponse = this.objectMapper.readTree(response.body());
        } catch (IOException e) {
            throw new FlowRegistryException("Could not parse response from BitBucket API", e);
        }
        Iterator<JsonNode> branches = jsonResponse.get("values").elements();

        Set<String> result = new HashSet<>();
        while (branches.hasNext()) {
            JsonNode branch = branches.next();
            result.add(branch.get("name").asText());
        }

        return result;
    }

    @Override
    public Set<String> getTopLevelDirectoryNames(final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath("");
        LOGGER.debug("Getting top-level directories for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, repoName);

        final Iterator<JsonNode> files = getFiles(branch, resolvedPath);

        final Set<String> result = new HashSet<>();
        while (files.hasNext()) {
            JsonNode file = files.next();
            if (file.get("type").asText().equals("commit_directory")) {
                final Path fullPath = Paths.get(file.get("path").asText());
                result.add(fullPath.getFileName().toString());
            }
        }

        return result;
    }

    @Override
    public Set<String> getFileNames(final String directory, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(directory);
        LOGGER.debug("Getting filenames for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, repoName);

        final Iterator<JsonNode> files = getFiles(branch, resolvedPath);

        final Set<String> result = new HashSet<>();
        while (files.hasNext()) {
            JsonNode file = files.next();
            if (file.get("type").asText().equals("commit_file")) {
                final Path fullPath = Paths.get(file.get("path").asText());
                result.add(fullPath.getFileName().toString());
            }
        }

        return result;
    }

    @Override
    public List<GitCommit> getCommits(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        LOGGER.debug("Getting commits for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, repoName);

        Iterator<JsonNode> commits = getListCommits(branch, resolvedPath);

        final List<GitCommit> result = new ArrayList<>();
        while (commits.hasNext()) {
            JsonNode commit = commits.next();
            result.add(toGitCommit(commit));
        }

        return result;
    }

    @Override
    public InputStream getContentFromBranch(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        LOGGER.debug("Getting content for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, repoName);
        final Optional<String> lastCommit = getLatestCommit(branch, resolvedPath);

        if (lastCommit.isEmpty()) {
            throw new FlowRegistryException(String.format("Could not find committed files at %s on branch %s response from BitBucket API", resolvedPath, branch));
        }
        return getContentFromCommit(path, lastCommit.get());
    }

    @Override
    public InputStream getContentFromCommit(final String path, final String commitSha) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        LOGGER.debug("Getting content for path [{}] from commit [{}] in repository [{}]", resolvedPath, commitSha, repoName);

        // https://api.bitbucket.org/2.0/repositories/{workspace}/{repoName}/src/{commit}/{path}
        final URI uri = getUriBuilder().addPathSegment("src").addPathSegment(commitSha).addPathSegment(resolvedPath).build();
        final HttpResponseEntity response = this.webClient.getWebClientService().get().uri(uri).header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue()).retrieve();

        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            throw new FlowRegistryException(
                    String.format("Error while retrieving content for repository [%s] at path %s: %s", repoName, resolvedPath, getErrorMessage(response)));
        }

        return response.body();
    }

    @Override
    public Optional<String> getContentSha(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        LOGGER.debug("Getting content SHA for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, repoName);
        return getLatestCommit(branch, resolvedPath);
    }

    @Override
    public String createContent(final GitCreateContentRequest request) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(request.getPath());
        final String branch = request.getBranch();
        LOGGER.debug("Creating content at path [{}] on branch [{}] in repository [{}] ", resolvedPath, branch, repoName);

        final StandardMultipartFormDataStreamBuilder multipartBuilder = new StandardMultipartFormDataStreamBuilder();
        multipartBuilder.addPart(resolvedPath, StandardHttpContentType.APPLICATION_JSON, request.getContent().getBytes(StandardCharsets.UTF_8));
        multipartBuilder.addPart("message", StandardHttpContentType.TEXT_PLAIN, request.getMessage().getBytes(StandardCharsets.UTF_8));
        multipartBuilder.addPart("branch", StandardHttpContentType.TEXT_PLAIN, branch.getBytes(StandardCharsets.UTF_8));

        // https://api.bitbucket.org/2.0/repositories/{workspace}/{repoName}/src
        final URI uri = getUriBuilder().addPathSegment("src").build();
        final HttpResponseEntity response = this.webClient.getWebClientService()
                .post()
                .uri(uri)
                .body(multipartBuilder.build(), OptionalLong.empty())
                .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                .header(CONTENT_TYPE_HEADER, multipartBuilder.getHttpContentType().getContentType())
                .retrieve();

        if (response.statusCode() != HttpURLConnection.HTTP_CREATED) {
            throw new FlowRegistryException(
                    String.format("Error while committing content for repository [%s] on branch %s at path %s: %s", repoName, branch, resolvedPath, getErrorMessage(response)));
        }

        final Optional<String> lastCommit = getLatestCommit(branch, resolvedPath);

        if (lastCommit.isEmpty()) {
            throw new FlowRegistryException(String.format("Could not find commit for the file %s we just tried to commit on branch %s", resolvedPath, branch));
        }

        return lastCommit.get();
    }

    @Override
    public InputStream deleteContent(final String filePath, final String commitMessage, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(filePath);
        LOGGER.debug("Deleting content at path [{}] on branch [{}] in repository [{}] ", resolvedPath, branch, repoName);

        final InputStream fileToBeDeleted = getContentFromBranch(filePath, branch);

        final StandardMultipartFormDataStreamBuilder multipartBuilder = new StandardMultipartFormDataStreamBuilder();
        multipartBuilder.addPart("files", StandardHttpContentType.TEXT_PLAIN, resolvedPath.getBytes(StandardCharsets.UTF_8));
        multipartBuilder.addPart("message", StandardHttpContentType.TEXT_PLAIN, commitMessage.getBytes(StandardCharsets.UTF_8));
        multipartBuilder.addPart("branch", StandardHttpContentType.TEXT_PLAIN, branch.getBytes(StandardCharsets.UTF_8));

        // https://api.bitbucket.org/2.0/repositories/{workspace}/{repoName}/src
        final URI uri = getUriBuilder().addPathSegment("src").build();
        final HttpResponseEntity response = this.webClient.getWebClientService()
                .post()
                .uri(uri)
                .body(multipartBuilder.build(), OptionalLong.empty())
                .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                .header(CONTENT_TYPE_HEADER, multipartBuilder.getHttpContentType().getContentType())
                .retrieve();

        if (response.statusCode() != HttpURLConnection.HTTP_CREATED) {
            throw new FlowRegistryException(
                    String.format("Error while deleting content for repository [%s] on branch %s at path %s: %s", repoName, branch, resolvedPath, getErrorMessage(response)));
        }

        return fileToBeDeleted;
    }

    private Iterator<JsonNode> getFiles(final String branch, final String resolvedPath) throws FlowRegistryException {
        final Optional<String> lastCommit = getLatestCommit(branch, resolvedPath);

        if (lastCommit.isEmpty()) {
            throw new FlowRegistryException(String.format("Could not find committed files at %s on branch %s response from BitBucket API", resolvedPath, branch));
        }

        // retrieve source data
        // https://api.bitbucket.org/2.0/repositories/{workspace}/{repoName}/src/{commit}/{path}
        final URI uri = getUriBuilder().addPathSegment("src").addPathSegment(lastCommit.get()).addPathSegment(resolvedPath).build();
        final HttpResponseEntity response = this.webClient.getWebClientService().get().uri(uri).header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue()).retrieve();

        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            throw new FlowRegistryException(
                    String.format("Error while listing content for repository [%s] on branch %s at path %s: %s", repoName, branch, resolvedPath, getErrorMessage(response)));
        }

        final JsonNode jsonResponse;
        try {
            jsonResponse = this.objectMapper.readTree(response.body());
        } catch (IOException e) {
            throw new FlowRegistryException("Could not parse response from BitBucket API", e);
        }
        return jsonResponse.get("values").elements();
    }

    private Iterator<JsonNode> getListCommits(final String branch, final String path) throws FlowRegistryException {
        // retrieve latest commit for that branch
        // https://api.bitbucket.org/2.0/repositories/{workspace}/{repoName}/commits/{branch}
        final URI uri = getUriBuilder().addPathSegment("commits").addPathSegment(branch).addQueryParameter("path", path).build();
        final HttpResponseEntity response = this.webClient.getWebClientService().get().uri(uri).header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue()).retrieve();

        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            throw new FlowRegistryException(String.format("Error while listing commits for repository [%s] on branch %s: %s", repoName, branch, getErrorMessage(response)));
        }

        final JsonNode jsonResponse;
        try {
            jsonResponse = this.objectMapper.readTree(response.body());
        } catch (IOException e) {
            throw new FlowRegistryException("Could not parse response from BitBucket API", e);
        }
        return jsonResponse.get("values").elements();
    }

    private Optional<String> getLatestCommit(final String branch, final String path) throws FlowRegistryException {
        Iterator<JsonNode> commits = getListCommits(branch, path);
        if (commits.hasNext()) {
            return Optional.of(commits.next().get("hash").asText());
        } else {
            return Optional.empty();
        }
    }

    private String checkRepoPermissions() throws FlowRegistryException {
        LOGGER.debug("Retrieving information about current user");

        // 'https://api.bitbucket.org/2.0/user/permissions/repositories?q=repository.name="{repoName}"
        URI uri = this.webClient.getHttpUriBuilder()
                .scheme("https")
                .host(apiUrl)
                .addPathSegment(apiVersion)
                .addPathSegment("user")
                .addPathSegment("permissions")
                .addPathSegment("repositories")
                .addQueryParameter("q", "repository.name=\"" + repoName + "\"")
                .build();
        HttpResponseEntity response = this.webClient.getWebClientService().get().uri(uri).header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue()).retrieve();

        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            throw new FlowRegistryException(String.format("Error while retrieving permission metadata for specified repo - %s", getErrorMessage(response)));
        }

        JsonNode jsonResponse;
        try {
            jsonResponse = this.objectMapper.readTree(response.body());
        } catch (IOException e) {
            throw new FlowRegistryException("Could not parse response from BitBucket API", e);
        }
        Iterator<JsonNode> repoPermissions = jsonResponse.get("values").elements();

        if (repoPermissions.hasNext()) {
            return repoPermissions.next().get("permission").asText();
        } else {
            return "none";
        }
    }

    private GitCommit toGitCommit(final JsonNode commit) {
        return new GitCommit(
                commit.get("hash").asText(),
                commit.get("author").get("raw").asText(),
                commit.get("message").asText(),
                Instant.parse(commit.get("date").asText()));
    }

    private String getErrorMessage(HttpResponseEntity response) throws FlowRegistryException {
        final JsonNode jsonResponse;
        try {
            jsonResponse = this.objectMapper.readTree(response.body());
        } catch (IOException e) {
            throw new FlowRegistryException("Could not parse response from BitBucket API", e);
        }
        return String.format("[%s] - %s", jsonResponse.get("type").asText(), jsonResponse.get("error").get("message").asText());
    }

    private String getResolvedPath(final String path) {
        return repoPath == null ? path : repoPath + "/" + path;
    }

    private HttpUriBuilder getUriBuilder() {
        return this.webClient.getHttpUriBuilder()
                .scheme("https")
                .host(apiUrl)
                .addPathSegment(apiVersion)
                .addPathSegment("repositories")
                .addPathSegment(workspace)
                .addPathSegment(repoName);
    }

    private interface BitBucketToken<T> {
        T getAuthzHeaderValue();
    }

    private class BasicAuthToken implements BitBucketToken<String> {
        private String token;

        public BasicAuthToken(final String username, final String appPassword) {
            final String basicCreds = username + ":" + appPassword;
            final byte[] basicCredsBytes = basicCreds.getBytes(StandardCharsets.UTF_8);

            final Base64.Encoder encoder = Base64.getEncoder();
            token = encoder.encodeToString(basicCredsBytes);
        }

        @Override
        public String getAuthzHeaderValue() {
            return BASIC + " " + token;
        }
    }

    private class AccessToken implements BitBucketToken<String> {
        private String token;

        public AccessToken(final String token) {
            this.token = token;
        }

        @Override
        public String getAuthzHeaderValue() {
            return BEARER + " " + token;
        }
    }

    private class OAuthToken implements BitBucketToken<String> {
        private OAuth2AccessTokenProvider oauthService;

        public OAuthToken(final OAuth2AccessTokenProvider oauthService) {
            this.oauthService = oauthService;
        }

        @Override
        public String getAuthzHeaderValue() {
            return BEARER + " " + oauthService.getAccessDetails().getAccessToken();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String clientId;
        private String apiUrl;
        private String apiVersion;
        private BitBucketAuthenticationType authenticationType;
        private String accessToken;
        private String username;
        private String appPassword;
        private OAuth2AccessTokenProvider oauthService;
        private WebClientServiceProvider webClient;
        private String workspace;
        private String repoName;
        private String repoPath;

        public Builder clientId(final String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder apiUrl(final String apiUrl) {
            this.apiUrl = apiUrl;
            return this;
        }

        public Builder apiVersion(final String apiVersion) {
            this.apiVersion = apiVersion;
            return this;
        }

        public Builder authenticationType(final BitBucketAuthenticationType authenticationType) {
            this.authenticationType = authenticationType;
            return this;
        }

        public Builder accessToken(final String accessToken) {
            this.accessToken = accessToken;
            return this;
        }

        public Builder username(final String username) {
            this.username = username;
            return this;
        }

        public Builder appPassword(final String appPassword) {
            this.appPassword = appPassword;
            return this;
        }

        public Builder oauthService(final OAuth2AccessTokenProvider oauthService) {
            this.oauthService = oauthService;
            return this;
        }

        public Builder webClient(final WebClientServiceProvider webClient) {
            this.webClient = webClient;
            return this;
        }

        public Builder workspace(final String workspace) {
            this.workspace = workspace;
            return this;
        }

        public Builder repoName(final String repoName) {
            this.repoName = repoName;
            return this;
        }

        public Builder repoPath(final String repoPath) {
            this.repoPath = repoPath;
            return this;
        }

        public BitBucketRepositoryClient build() throws FlowRegistryException {
            return new BitBucketRepositoryClient(this);
        }
    }
}
