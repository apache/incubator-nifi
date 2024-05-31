/*
 *
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
 *
 */

package org.apache.nifi.github;

import org.apache.nifi.registry.flow.FlowRegistryException;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHContent;
import org.kohsuke.github.GHContentUpdateResponse;
import org.kohsuke.github.GHMyself;
import org.kohsuke.github.GHPermissionType;
import org.kohsuke.github.GHRef;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.extras.authorization.JWTTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Client to encapsulate access to a GitHub Repository through the Hub4j GitHub client.
 */
public class GitHubRepositoryClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(GitHubRepositoryClient.class);

    private static final String BRANCH_REF_PATTERN = "refs/heads/%s";
    private static final int COMMIT_PAGE_SIZE = 50;

    private final String repoOwner;
    private final String repoName;
    private final String repoPath;

    private final GitHub gitHub;
    private final GHRepository repository;
    private final GitHubAuthenticationType authenticationType;
    private final boolean canRead;
    private final boolean canWrite;

    private GitHubRepositoryClient(final Builder builder) throws IOException, FlowRegistryException {
        final String apiUrl = Objects.requireNonNull(builder.apiUrl, "API URL is required");
        final GitHubBuilder gitHubBuilder = new GitHubBuilder().withEndpoint(apiUrl);

        repoPath = builder.repoPath;
        repoOwner = Objects.requireNonNull(builder.repoOwner, "Repository Owner is required");
        repoName = Objects.requireNonNull(builder.repoName, "Repository Name is required");
        authenticationType = Objects.requireNonNull(builder.authenticationType, "Authentication Type is required");

        switch (authenticationType) {
            case PERSONAL_ACCESS_TOKEN -> gitHubBuilder.withOAuthToken(builder.personalAccessToken);
            case APP_INSTALLATION_TOKEN -> gitHubBuilder.withAppInstallationToken(builder.appInstallationToken);
            case APP_INSTALLATION_ID_AND_PRIVATE_KEY -> {
                try {
                    JWTTokenProvider jwtTokenProvider = new JWTTokenProvider(builder().appId, builder().privateKey);
                    String token = jwtTokenProvider.getEncodedAuthorization();
                    gitHubBuilder.withJwtToken(token);
                } catch (Exception e) {
                    throw new FlowRegistryException("Failed to generate JWT from App ID and Private Key", e);
                }
            }
        }

        gitHub = gitHubBuilder.build();

        final String fullRepoName = repoOwner + "/" + repoName;
        try {
            repository = gitHub.getRepository(fullRepoName);
        } catch (final FileNotFoundException fnf) {
            throw new FlowRegistryException("Repository [" + fullRepoName + "] not found");
        }

        // if anonymous then we assume the client has read permissions, otherwise the call to getRepository above would have failed
        // if not anonymous then we get the identity of the current user and then ask for the permissions the current user has on the repo
        if (gitHub.isAnonymous()) {
            canRead = true;
            canWrite = false;
        } else {
            final GHMyself currentUser = gitHub.getMyself();
            canRead = repository.hasPermission(currentUser, GHPermissionType.READ);
            canWrite = repository.hasPermission(currentUser, GHPermissionType.WRITE);
        }
    }

    /**
     * @return the repo owner
     */
    public String getRepoOwner() {
        return repoOwner;
    }

    /**
     * @return the repo name
     */
    public String getRepoName() {
        return repoName;
    }

    /**
     * @return the authentication type this client is configured with
     */
    public GitHubAuthenticationType getAuthenticationType() {
        return authenticationType;
    }

    /**
     * @return true if the repository is readable by configured credentials
     */
    public boolean getCanRead() {
        return canRead;
    }

    /**
     * @return true if the repository is writable by the configured credentials
     */
    public boolean getCanWrite() {
        return canWrite;
    }

    /**
     * Creates the content specified by the given builder.
     *
     * @param request the request for the content to create
     * @return the update response
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public String createContent(final GitHubCreateContentRequest request) throws IOException, FlowRegistryException {
        final String branch = request.getBranch();
        final String resolvedPath = getResolvedPath(request.getPath());
        LOGGER.debug("Creating content at path [{}] on branch [{}] in repo [{}] ", resolvedPath, branch, repository.getName());
        return execute(() -> {
            try {
                final GHContentUpdateResponse response = repository.createContent()
                        .branch(branch)
                        .path(resolvedPath)
                        .content(request.getContent())
                        .message(request.getMessage())
                        .sha(request.getExistingContentSha())
                        .commit();
                return response.getCommit().getSha();
            } catch (final FileNotFoundException fnf) {
                throwPathOrBranchNotFound(fnf, resolvedPath, branch);
                return null;
            }
        });
    }

    /**
     * Gets the names of all the branches in the repo.
     *
     * @return the set of all branches in the repo
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public Set<String> getBranches() throws IOException, FlowRegistryException {
        LOGGER.debug("Getting branches for repo [{}]", repository.getName());
        return execute(() -> repository.getBranches().keySet());
    }

    /**
     * Gets an InputStream to read the latest content of the given path from the given branch.
     * The returned stream already contains the contents of the requested file.
     *
     * @param path the path to the content
     * @param branch the branch
     * @return an input stream containing the contents of the path
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public InputStream getContentFromBranch(final String path, final String branch) throws IOException, FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        final String branchRef = BRANCH_REF_PATTERN.formatted(branch);
        LOGGER.debug("Getting content for [{}] from branch [{}] in repo [{}] ", resolvedPath, branch, repository.getName());

        return execute(() -> {
            try {
                final GHContent ghContent = repository.getFileContent(resolvedPath, branchRef);
                return ghContent.read();
            } catch (final FileNotFoundException fnf) {
                throwPathOrBranchNotFound(fnf, resolvedPath, branchRef);
                return null;
            }
        });
    }

    /**
     * Gets the content of the given path from the given commit.
     * The returned stream already contains the contents of the requested file.
     *
     * @param path the path to the content
     * @param commitSha the commit SHA
     * @return an input stream containing the contents of the path
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public InputStream getContentFromCommit(final String path, final String commitSha) throws IOException, FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        LOGGER.debug("Getting content for [{}] from commit [{}] in repo [{}] ", resolvedPath, commitSha, repository.getName());

        return execute(() -> {
            try {
                final GHContent ghContent = repository.getFileContent(resolvedPath, commitSha);
                return ghContent.read();
            } catch (final FileNotFoundException fnf) {
                throw new FlowRegistryException("Path [" + resolvedPath + "] or Commit [" + commitSha + "] not found", fnf);
            }
        });
    }

    /**
     * Gets the commits for a given path on a given branch.
     *
     * @param path the path
     * @param branch the branch
     * @return the list of commits for the given path
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public List<GHCommit> getCommits(final String path, final String branch) throws IOException, FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        final String branchRef = BRANCH_REF_PATTERN.formatted(branch);
        LOGGER.debug("Getting commits for [{}] from branch [{}] in repo [{}]", resolvedPath, branch, repository.getName());

        return execute(() -> {
            try {
                final GHRef branchGhRef = repository.getRef(branchRef);
                return repository.queryCommits()
                        .path(resolvedPath)
                        .from(branchGhRef.getObject().getSha())
                        .pageSize(COMMIT_PAGE_SIZE)
                        .list()
                        .toList();
            } catch (final FileNotFoundException fnf) {
                throwPathOrBranchNotFound(fnf, resolvedPath, branchRef);
                return null;
            }
        });
    }

    /**
     * Gets the top-level directory names, which are the directories at the root of the repo, or within the prefix if specified.
     *
     * @param branch the branch
     * @return the set of directory names
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public Set<String> getTopLevelDirectoryNames(final String branch) throws IOException, FlowRegistryException {
        return getDirectoryItems("", branch, GHContent::isDirectory);
    }

    /**
     * Gets the names of the directories contained within the given directory.
     *
     * @param directory the directory to list
     * @param branch the branch
     * @return the set of directory names
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public Set<String> getDirectoryNames(final String directory, final String branch) throws IOException, FlowRegistryException {
        return getDirectoryItems(directory, branch, GHContent::isDirectory);
    }

    /**
     * Gets the names of the directories container within the given directory.
     *
     * @param directory the directory to list
     * @param branch the branch
     * @return the set of file names
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public Set<String> getFileNames(final String directory, final String branch) throws IOException, FlowRegistryException {
        return getDirectoryItems(directory, branch, GHContent::isFile);
    }

    /**
     * Get the names of the items in the given directory on the given branch, filtered by the provided filter.
     *
     * @param directory the directory
     * @param branch the branch
     * @param filter the filter to determine which items get included
     * @return the set of item names
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    private Set<String> getDirectoryItems(final String directory, final String branch, final Predicate<GHContent> filter) throws IOException, FlowRegistryException {
        final String resolvedDirectory = getResolvedPath(directory);
        final String branchRef = BRANCH_REF_PATTERN.formatted(branch);
        LOGGER.debug("Getting directory items for [{}] from branch [{}] in repo [{}] ", resolvedDirectory, branch, repository.getName());

        return execute(() -> {
            try {
                return repository.getDirectoryContent(resolvedDirectory, branchRef).stream()
                        .filter(filter)
                        .map(GHContent::getName)
                        .collect(Collectors.toSet());
            } catch (final FileNotFoundException fnf) {
                throwPathOrBranchNotFound(fnf, resolvedDirectory, branchRef);
                return null;
            }
        });
    }

    /**
     * Gets the current SHA for the given path from the given branch.
     *
     * @param path the path to the content
     * @param branch the branch
     * @return current sha for the given file, or empty optional
     *
     * @throws IOException if an I/O error happens calling GitHub
     */
    public Optional<String> getContentSha(final String path, final String branch) throws IOException, FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        final String branchRef = BRANCH_REF_PATTERN.formatted(branch);
        LOGGER.debug("Getting content SHA for [{}] from branch [{}] in repo [{}] ", resolvedPath, branch, repository.getName());

        return execute(() -> {
            try {
                final GHContent ghContent = repository.getFileContent(resolvedPath, branchRef);
                return Optional.of(ghContent.getSha());
            } catch (final FileNotFoundException e) {
                LOGGER.warn("Unable to get content SHA for [{}] from branch [{}] because content does not exist", resolvedPath, branch);
                return Optional.empty();
            }
        });
    }

    /**
     * Deletes the contents for the given file on the given branch.
     *
     * @param filePath the file path to delete
     * @param commitMessage the commit message for the delete commit
     * @param branch the branch to delete from
     * @return the deleted content
     *
     * @throws IOException if an I/O error happens calling GitHub
     * @throws FlowRegistryException if a non I/O error happens calling GitHub
     */
    public GHContent deleteContent(final String filePath, final String commitMessage, final String branch) throws FlowRegistryException, IOException {
        final String resolvedPath = getResolvedPath(filePath);
        LOGGER.debug("Deleting file [{}] in repo [{}] on branch [{}]", resolvedPath, repository.getName(), branch);
        return execute(() -> {
            try {
                GHContent ghContent = repository.getFileContent(resolvedPath);
                ghContent.delete(commitMessage, branch);
                return ghContent;
            } catch (final FileNotFoundException fnf) {
                throwPathOrBranchNotFound(fnf, resolvedPath, branch);
                return null;
            }
        });
    }

    private String getResolvedPath(final String path) {
        return repoPath == null ? path : repoPath + "/" + path;
    }

    private void throwPathOrBranchNotFound(final FileNotFoundException fileNotFoundException, final String path, final String branch) throws FlowRegistryException {
        throw new FlowRegistryException("Path [" + path + "] or Branch [" + branch + "] not found", fileNotFoundException);
    }

    private <T> T execute(final GHRequest<T> action) throws FlowRegistryException, IOException {
        try {
            return action.execute();
        } catch (final FlowRegistryException | IOException e) {
            throw e;
        } catch (final Exception e) {
            throw new FlowRegistryException(e.getMessage(), e);
        }
    }

    /**
     * Functional interface for making a request to GitHub which may throw IOException.
     *
     * @param <T> the result of the request
     */
    private interface GHRequest<T> {

        T execute() throws IOException, FlowRegistryException;

    }

    /**
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for the repository client.
     */
    public static class Builder {

        private String apiUrl;
        private GitHubAuthenticationType authenticationType;
        private String personalAccessToken;
        private String appInstallationToken;
        private String repoOwner;
        private String repoName;
        private String repoPath;
        private String privateKey;
        private String appId;

        public Builder apiUrl(final String apiUrl) {
            this.apiUrl = apiUrl;
            return this;
        }

        public Builder authenticationType(final GitHubAuthenticationType authenticationType) {
            this.authenticationType = authenticationType;
            return this;
        }

        public Builder personalAccessToken(final String personalAccessToken) {
            this.personalAccessToken = personalAccessToken;
            return this;
        }

        public Builder appInstallationToken(final String appInstallationToken) {
            this.appInstallationToken = appInstallationToken;
            return this;
        }

        public Builder repoOwner(final String repoOwner) {
            this.repoOwner = repoOwner;
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
        public Builder appId(final String appId){
            this.appId = appId;
            return this;
        }

        public Builder privateKey(final String privateKey){
            this.privateKey = privateKey;
            return this;
        }

        public GitHubRepositoryClient build() throws IOException, FlowRegistryException {
            return new GitHubRepositoryClient(this);
        }

    }
}
