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

package org.apache.nifi.registry.flow.git;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.flow.AbstractFlowRegistryClient;
import org.apache.nifi.registry.flow.AuthorizationException;
import org.apache.nifi.registry.flow.BucketLocation;
import org.apache.nifi.registry.flow.FlowAlreadyExistsException;
import org.apache.nifi.registry.flow.FlowLocation;
import org.apache.nifi.registry.flow.FlowRegistryBranch;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryClientInitializationContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.FlowRegistryPermissions;
import org.apache.nifi.registry.flow.FlowVersionLocation;
import org.apache.nifi.registry.flow.RegisterAction;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.git.client.GitCommit;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.apache.nifi.registry.flow.git.serialize.FlowSnapshotSerializer;
import org.apache.nifi.registry.flow.git.serialize.JacksonFlowSnapshotSerializer;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Base class for git-based flow registry clients.
 */
public abstract class AbstractGitFlowRegistryClient extends AbstractFlowRegistryClient {

    public static final PropertyDescriptor REPOSITORY_BRANCH = new PropertyDescriptor.Builder()
            .name("Default Branch")
            .description("The default branch to use for this client")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("main")
            .required(true)
            .build();

    public static final PropertyDescriptor REPOSITORY_PATH = new PropertyDescriptor.Builder()
            .name("Repository Path")
            .description("The path in the repository that this client will use to store all data. " +
                    "If left blank, then the root of the repository will be used.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor DIRECTORY_FILTER_EXCLUDE = new PropertyDescriptor.Builder()
            .name("Directory Filter Exclusion")
            .description("Directories whose names match the given regular expression will be ignored "
                    + "when listing buckets.")
            .defaultValue("[.].*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(true)
            .build();

    static final String DEFAULT_BUCKET_NAME = "default";
    static final String DEFAULT_BUCKET_KEEP_FILE_PATH = DEFAULT_BUCKET_NAME + "/.keep";
    static final String DEFAULT_BUCKET_KEEP_FILE_CONTENT = "Do Not Delete";
    static final String DEFAULT_BUCKET_KEEP_FILE_MESSAGE = "Creating default bucket";

    static final String REGISTER_FLOW_MESSAGE_PREFIX = "Registering Flow";
    static final String REGISTER_FLOW_MESSAGE_FORMAT = REGISTER_FLOW_MESSAGE_PREFIX + " [%s]";
    static final String DEREGISTER_FLOW_MESSAGE_FORMAT = "Deregistering Flow [%s]";
    static final String DEFAULT_FLOW_SNAPSHOT_MESSAGE_FORMAT = "Saving Flow Snapshot %s";
    static final String SNAPSHOT_FILE_EXTENSION = ".json";
    static final String SNAPSHOT_FILE_PATH_FORMAT = "%s/%s" + SNAPSHOT_FILE_EXTENSION;
    static final String FLOW_CONTENTS_GROUP_ID = "flow-contents-group";

    private volatile FlowSnapshotSerializer flowSnapshotSerializer;
    private volatile GitRepositoryClient repositoryClient;
    private volatile Pattern directoryExclusionPattern;
    private final AtomicBoolean clientInitialized = new AtomicBoolean(false);

    private volatile List<PropertyDescriptor> propertyDescriptors;

    @Override
    public void initialize(final FlowRegistryClientInitializationContext context) {
        super.initialize(context);

        final List<PropertyDescriptor> combinedPropertyDescriptors = new ArrayList<>(createPropertyDescriptors());
        combinedPropertyDescriptors.add(REPOSITORY_BRANCH);
        combinedPropertyDescriptors.add(REPOSITORY_PATH);
        combinedPropertyDescriptors.add(DIRECTORY_FILTER_EXCLUDE);
        propertyDescriptors = Collections.unmodifiableList(combinedPropertyDescriptors);

        flowSnapshotSerializer = createFlowSnapshotSerializer();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final String repoPath = validationContext.getProperty(REPOSITORY_PATH).getValue();
        if (repoPath != null && (repoPath.startsWith("/") || repoPath.endsWith("/"))) {
            results.add(new ValidationResult.Builder()
                    .subject(REPOSITORY_PATH.getDisplayName())
                    .valid(false)
                    .explanation("Path can not start or end with /")
                    .build());
        }

        return results;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        synchronized (this) {
            invalidateClient();
        }
    }

    @Override
    public boolean isBranchingSupported(final FlowRegistryClientConfigurationContext context) {
        return true;
    }

    @Override
    public Set<FlowRegistryBranch> getBranches(final FlowRegistryClientConfigurationContext context) throws FlowRegistryException, IOException {
        final GitRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        return repositoryClient.getBranches().stream()
                .map(branchName -> {
                    final FlowRegistryBranch flowRegistryBranch = new FlowRegistryBranch();
                    flowRegistryBranch.setName(branchName);
                    return flowRegistryBranch;
                }).collect(Collectors.toSet());
    }

    @Override
    public FlowRegistryBranch getDefaultBranch(final FlowRegistryClientConfigurationContext context) {
        final FlowRegistryBranch defaultBranch = new FlowRegistryBranch();
        defaultBranch.setName(context.getProperty(REPOSITORY_BRANCH).getValue());
        return defaultBranch;
    }

    @Override
    public Set<FlowRegistryBucket> getBuckets(final FlowRegistryClientConfigurationContext context, final String branch) throws IOException, FlowRegistryException {
        final GitRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final Set<FlowRegistryBucket> buckets = repositoryClient.getTopLevelDirectoryNames(branch).stream()
                .filter(bucketName -> !directoryExclusionPattern.matcher(bucketName).matches())
                .map(bucketName -> createFlowRegistryBucket(repositoryClient, bucketName))
                .collect(Collectors.toSet());

        // if the repository has no top-level directories, then return a default bucket entry, this won't exist in the repository until the first time a flow is saved to it
        return buckets.isEmpty() ? Set.of(createFlowRegistryBucket(repositoryClient, DEFAULT_BUCKET_NAME)) : buckets;
    }

    @Override
    public FlowRegistryBucket getBucket(final FlowRegistryClientConfigurationContext context, final BucketLocation bucketLocation) throws FlowRegistryException, IOException {
        final GitRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);
        return createFlowRegistryBucket(repositoryClient, bucketLocation.getBucketId());
    }

    @Override
    public RegisteredFlow registerFlow(final FlowRegistryClientConfigurationContext context, final RegisteredFlow flow) throws FlowRegistryException, IOException {
        final GitRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyWritePermissions(repositoryClient);

        final String branch = flow.getBranch();
        final FlowLocation flowLocation = new FlowLocation(branch, flow.getBucketIdentifier(), flow.getIdentifier());
        final String filePath = getSnapshotFilePath(flowLocation);
        final String commitMessage = REGISTER_FLOW_MESSAGE_FORMAT.formatted(flow.getIdentifier());

        final Optional<String> existingFileSha = repositoryClient.getContentSha(filePath, branch);
        if (existingFileSha.isPresent()) {
            throw new FlowAlreadyExistsException("Another flow is already registered at [" + filePath + "] on branch [" + branch + "]");
        }

        // Clear values we don't want in the json stored in Git
        final String originalBucketId = flow.getBucketIdentifier();
        flow.setBucketIdentifier(null);
        flow.setBucketName(null);
        flow.setBranch(null);

        final RegisteredFlowSnapshot flowSnapshot = new RegisteredFlowSnapshot();
        flowSnapshot.setFlow(flow);

        final GitCreateContentRequest request = GitCreateContentRequest.builder()
                .branch(branch)
                .path(filePath)
                .content(flowSnapshotSerializer.serialize(flowSnapshot))
                .message(commitMessage)
                .build();

        repositoryClient.createContent(request);

        // Re-populate fields before returning
        flow.setBucketName(originalBucketId);
        flow.setBucketIdentifier(originalBucketId);
        flow.setBranch(branch);

        return flow;
    }

    @Override
    public RegisteredFlow deregisterFlow(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        final GitRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyWritePermissions(repositoryClient);

        final String branch = flowLocation.getBranch();
        final String filePath = getSnapshotFilePath(flowLocation);
        final String commitMessage = DEREGISTER_FLOW_MESSAGE_FORMAT.formatted(flowLocation.getFlowId());
        try (final InputStream deletedSnapshotContent = repositoryClient.deleteContent(filePath, commitMessage, branch)) {
            final RegisteredFlowSnapshot deletedSnapshot = getSnapshot(deletedSnapshotContent);
            populateFlowAndSnapshotMetadata(deletedSnapshot, flowLocation);
            updateBucketReferences(repositoryClient, deletedSnapshot, flowLocation.getBucketId());
            return deletedSnapshot.getFlow();
        }
    }

    @Override
    public RegisteredFlow getFlow(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        final GitRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final String branch = flowLocation.getBranch();
        final String filePath = getSnapshotFilePath(flowLocation);

        final RegisteredFlowSnapshot existingSnapshot = getSnapshot(repositoryClient, filePath, branch);
        populateFlowAndSnapshotMetadata(existingSnapshot, flowLocation);
        updateBucketReferences(repositoryClient, existingSnapshot, flowLocation.getBucketId());

        final RegisteredFlow registeredFlow = existingSnapshot.getFlow();
        registeredFlow.setBranch(branch);
        return registeredFlow;
    }

    @Override
    public Set<RegisteredFlow> getFlows(final FlowRegistryClientConfigurationContext context, final BucketLocation bucketLocation) throws IOException, FlowRegistryException {
        final GitRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final String branch = bucketLocation.getBranch();
        final String bucketId = bucketLocation.getBucketId();

        return repositoryClient.getFileNames(bucketId, branch).stream()
                .filter(filename -> filename.endsWith(SNAPSHOT_FILE_EXTENSION))
                .map(filename -> mapToRegisteredFlow(bucketLocation, filename))
                .collect(Collectors.toSet());
    }

    @Override
    public RegisteredFlowSnapshot getFlowContents(final FlowRegistryClientConfigurationContext context, final FlowVersionLocation flowVersionLocation)
            throws FlowRegistryException, IOException {
        final GitRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final String version = flowVersionLocation.getVersion();
        final String filePath = getSnapshotFilePath(flowVersionLocation);

        final InputStream inputStream = repositoryClient.getContentFromCommit(filePath, version);
        final RegisteredFlowSnapshot flowSnapshot = getSnapshot(inputStream);
        populateFlowAndSnapshotMetadata(flowSnapshot, flowVersionLocation);

        // populate values that aren't store in Git
        flowSnapshot.getSnapshotMetadata().setVersion(version);
        flowSnapshot.getSnapshotMetadata().setBranch(flowVersionLocation.getBranch());
        flowSnapshot.getFlow().setBranch(flowVersionLocation.getBranch());

        // populate outgoing bucket references
        updateBucketReferences(repositoryClient, flowSnapshot, flowVersionLocation.getBucketId());

        // determine if the version is the "latest" version by comparing to the response of getLatestVersion
        final String latestVersion = getLatestVersion(context, flowVersionLocation).orElse(null);
        flowSnapshot.setLatest(version.equals(latestVersion));

        return flowSnapshot;
    }

    @Override
    public RegisteredFlowSnapshot registerFlowSnapshot(final FlowRegistryClientConfigurationContext context, final RegisteredFlowSnapshot flowSnapshot, final RegisterAction action)
            throws FlowRegistryException, IOException {
        final GitRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyWritePermissions(repositoryClient);

        final RegisteredFlowSnapshotMetadata snapshotMetadata = flowSnapshot.getSnapshotMetadata();
        final String branch = snapshotMetadata.getBranch();
        final FlowLocation flowLocation = new FlowLocation(snapshotMetadata.getBranch(), snapshotMetadata.getBucketIdentifier(), snapshotMetadata.getFlowIdentifier());
        final String filePath = getSnapshotFilePath(flowLocation);
        final String previousSha = repositoryClient.getContentSha(filePath, branch).orElse(null);

        final String snapshotComments = snapshotMetadata.getComments();
        final String commitMessage = StringUtils.isBlank(snapshotComments) ? DEFAULT_FLOW_SNAPSHOT_MESSAGE_FORMAT.formatted(flowLocation.getFlowId()) : snapshotComments;

        final RegisteredFlowSnapshot existingSnapshot = getSnapshot(repositoryClient, filePath, branch);
        populateFlowAndSnapshotMetadata(existingSnapshot, flowLocation);

        final RegisteredFlow existingFlow = existingSnapshot.getFlow();
        existingFlow.setBranch(null);
        flowSnapshot.setFlow(existingFlow);

        // Clear values we don't want stored in the json in Git
        flowSnapshot.setBucket(null);
        flowSnapshot.getSnapshotMetadata().setBucketIdentifier(null);
        flowSnapshot.getSnapshotMetadata().setBranch(null);
        flowSnapshot.getSnapshotMetadata().setVersion(null);
        flowSnapshot.getSnapshotMetadata().setComments(null);
        flowSnapshot.getSnapshotMetadata().setTimestamp(0);

        // replace the id of the top level group and all of its references with a constant value prior to serializing to avoid
        // unnecessary diffs when different instances of the same flow are imported and have different top-level PG ids
        final String originalFlowContentsGroupId = replaceGroupId(flowSnapshot.getFlowContents(), FLOW_CONTENTS_GROUP_ID);
        final Position originalFlowContentsPosition = replacePosition(flowSnapshot.getFlowContents(), new Position(0, 0));

        final GitCreateContentRequest createContentRequest = GitCreateContentRequest.builder()
                .branch(branch)
                .path(filePath)
                .content(flowSnapshotSerializer.serialize(flowSnapshot))
                .message(commitMessage)
                .existingContentSha(previousSha)
                .build();

        final String createContentCommitSha = repositoryClient.createContent(createContentRequest);

        final VersionedFlowCoordinates versionedFlowCoordinates = new VersionedFlowCoordinates();
        versionedFlowCoordinates.setRegistryId(getIdentifier());
        versionedFlowCoordinates.setBranch(flowLocation.getBranch());
        versionedFlowCoordinates.setBucketId(flowLocation.getBucketId());
        versionedFlowCoordinates.setFlowId(flowLocation.getFlowId());
        versionedFlowCoordinates.setVersion(createContentCommitSha);
        versionedFlowCoordinates.setStorageLocation(getStorageLocation(repositoryClient));

        flowSnapshot.getFlowContents().setVersionedFlowCoordinates(versionedFlowCoordinates);
        flowSnapshot.getFlow().setBranch(branch);
        flowSnapshot.getSnapshotMetadata().setBranch(branch);
        flowSnapshot.getSnapshotMetadata().setVersion(createContentCommitSha);
        flowSnapshot.setLatest(true);

        // populate outgoing bucket references
        updateBucketReferences(repositoryClient, flowSnapshot, flowLocation.getBucketId());

        // set back to the original id so that the returned snapshot is has the correct values from what was passed in
        replaceGroupId(flowSnapshot.getFlowContents(), originalFlowContentsGroupId);
        replacePosition(flowSnapshot.getFlowContents(), originalFlowContentsPosition);

        return flowSnapshot;
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation)
            throws FlowRegistryException, IOException {
        final GitRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final String branch = flowLocation.getBranch();
        final String filePath = getSnapshotFilePath(flowLocation);

        final Set<RegisteredFlowSnapshotMetadata> snapshotMetadataSet = new LinkedHashSet<>();
        for (final GitCommit commit : repositoryClient.getCommits(filePath, branch)) {
            final RegisteredFlowSnapshotMetadata snapshotMetadata = createSnapshotMetadata(commit, flowLocation);
            if (snapshotMetadata.getComments() != null && snapshotMetadata.getComments().startsWith(REGISTER_FLOW_MESSAGE_PREFIX)) {
                continue;
            }
            snapshotMetadataSet.add(snapshotMetadata);
        }
        return snapshotMetadataSet;
    }

    @Override
    public Optional<String> getLatestVersion(final FlowRegistryClientConfigurationContext context, final FlowLocation flowLocation) throws FlowRegistryException, IOException {
        final GitRepositoryClient repositoryClient = getRepositoryClient(context);
        verifyReadPermissions(repositoryClient);

        final String branch = flowLocation.getBranch();
        final String filePath = getSnapshotFilePath(flowLocation);

        final List<GitCommit> commits = repositoryClient.getCommits(filePath, branch);
        final String latestVersion = commits.isEmpty() ? null : commits.getFirst().id();
        return Optional.ofNullable(latestVersion);
    }

    @Override
    public String generateFlowId(final String flowName) {
        return flowName.trim()
                .replaceAll("\\s", "-") // replace whitespace with -
                .replaceAll("[^a-zA-Z0-9-]", "") // replace all other invalid chars with empty string
                .replaceAll("(-)\\1+", "$1"); // replace consecutive - with single -
    }

    private FlowRegistryBucket createFlowRegistryBucket(final GitRepositoryClient repositoryClient, final String name) {
        final FlowRegistryPermissions bucketPermissions = new FlowRegistryPermissions();
        bucketPermissions.setCanRead(repositoryClient.hasReadPermission());
        bucketPermissions.setCanWrite(repositoryClient.hasWritePermission());
        bucketPermissions.setCanDelete(repositoryClient.hasWritePermission());

        final FlowRegistryBucket bucket = new FlowRegistryBucket();
        bucket.setIdentifier(name);
        bucket.setName(name);
        bucket.setPermissions(bucketPermissions);
        return bucket;
    }

    private RegisteredFlowSnapshotMetadata createSnapshotMetadata(final GitCommit commit, final FlowLocation flowLocation) throws IOException {
        final RegisteredFlowSnapshotMetadata snapshotMetadata = new RegisteredFlowSnapshotMetadata();
        snapshotMetadata.setBranch(flowLocation.getBranch());
        snapshotMetadata.setBucketIdentifier(flowLocation.getBucketId());
        snapshotMetadata.setFlowIdentifier(flowLocation.getFlowId());
        snapshotMetadata.setVersion(commit.id());
        snapshotMetadata.setAuthor(commit.author());
        snapshotMetadata.setComments(commit.message());
        snapshotMetadata.setTimestamp(commit.commitDate().toEpochMilli());
        return snapshotMetadata;
    }

    private RegisteredFlow mapToRegisteredFlow(final BucketLocation bucketLocation, final String filename) {
        final String branch = bucketLocation.getBranch();
        final String bucketId = bucketLocation.getBucketId();
        final String flowId = filename.replace(SNAPSHOT_FILE_EXTENSION, "");

        final RegisteredFlow registeredFlow = new RegisteredFlow();
        registeredFlow.setIdentifier(flowId);
        registeredFlow.setName(flowId);
        registeredFlow.setBranch(branch);
        registeredFlow.setBucketIdentifier(bucketId);
        registeredFlow.setBucketName(bucketId);
        return registeredFlow;
    }

    private String getSnapshotFilePath(final FlowLocation flowLocation) {
        return SNAPSHOT_FILE_PATH_FORMAT.formatted(flowLocation.getBucketId(), flowLocation.getFlowId());
    }

    private RegisteredFlowSnapshot getSnapshot(final GitRepositoryClient repositoryClient, final String filePath, final String branch) throws IOException, FlowRegistryException {
        try (final InputStream contentInputStream = repositoryClient.getContentFromBranch(filePath, branch)) {
            return flowSnapshotSerializer.deserialize(contentInputStream);
        }
    }

    private RegisteredFlowSnapshot getSnapshot(final InputStream inputStream) throws IOException {
        try {
            return flowSnapshotSerializer.deserialize(inputStream);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (final IOException ignored) {
                    // Close quietly
                }
            }
        }
    }

    private Position replacePosition(final VersionedProcessGroup group, final Position newPosition) {
        final Position originalPosition = group.getPosition();
        group.setPosition(newPosition);
        return originalPosition;
    }

    private String replaceGroupId(final VersionedProcessGroup group, final String newGroupId) {
        final String originalGroupId = group.getIdentifier();
        group.setIdentifier(newGroupId);

        replaceGroupId(group.getProcessGroups(), newGroupId);
        replaceGroupId(group.getRemoteProcessGroups(), newGroupId);
        replaceGroupId(group.getProcessors(), newGroupId);
        replaceGroupId(group.getFunnels(), newGroupId);
        replaceGroupId(group.getLabels(), newGroupId);
        replaceGroupId(group.getInputPorts(), newGroupId);
        replaceGroupId(group.getOutputPorts(), newGroupId);
        replaceGroupId(group.getControllerServices(), newGroupId);
        replaceGroupId(group.getConnections(), newGroupId);

        if (group.getConnections() != null) {
            for (final VersionedConnection connection : group.getConnections()) {
                replaceGroupId(connection.getSource(), originalGroupId, newGroupId);
                replaceGroupId(connection.getDestination(), originalGroupId, newGroupId);
            }
        }

        return originalGroupId;
    }

    private <T extends VersionedComponent> void replaceGroupId(final Collection<T> components, final String newGroupIdentifier) {
        if (components == null) {
            return;
        }
        components.forEach(c -> c.setGroupIdentifier(newGroupIdentifier));
    }

    private void replaceGroupId(final ConnectableComponent connectableComponent, final String originalGroupId, final String newGroupId) {
        if (connectableComponent == null) {
            return;
        }

        if (originalGroupId.equals(connectableComponent.getGroupId())) {
            connectableComponent.setGroupId(newGroupId);
        }
    }

    private void updateBucketReferences(final GitRepositoryClient repositoryClient, final RegisteredFlowSnapshot flowSnapshot, final String bucketId) {
        final FlowRegistryBucket bucket = createFlowRegistryBucket(repositoryClient, bucketId);
        flowSnapshot.setBucket(bucket);

        final RegisteredFlow flow = flowSnapshot.getFlow();
        flow.setBucketName(bucketId);
        flow.setBucketIdentifier(bucketId);

        final RegisteredFlowSnapshotMetadata snapshotMetadata = flowSnapshot.getSnapshotMetadata();
        snapshotMetadata.setBucketIdentifier(bucketId);
    }

    // Ensures the snapshot has non-null flow and metadata fields, which would only be null if taking a flow from "Download Flow Definition" and adding directly to Git
    private void populateFlowAndSnapshotMetadata(final RegisteredFlowSnapshot flowSnapshot, final FlowLocation flowLocation) {
        if (flowSnapshot.getFlow() == null) {
            final RegisteredFlow registeredFlow = new RegisteredFlow();
            registeredFlow.setName(flowLocation.getFlowId());
            registeredFlow.setIdentifier(flowLocation.getFlowId());
            flowSnapshot.setFlow(registeredFlow);
        }
        if (flowSnapshot.getSnapshotMetadata() == null) {
            final RegisteredFlowSnapshotMetadata snapshotMetadata = new RegisteredFlowSnapshotMetadata();
            snapshotMetadata.setFlowIdentifier(flowLocation.getFlowId());
            flowSnapshot.setSnapshotMetadata(snapshotMetadata);
        }
    }

    private void verifyWritePermissions(final GitRepositoryClient repositoryClient) throws AuthorizationException {
        if (!repositoryClient.hasWritePermission()) {
            throw new AuthorizationException("Client does not have write access to the repository");
        }
    }

    private void verifyReadPermissions(final GitRepositoryClient repositoryClient) throws AuthorizationException {
        if (!repositoryClient.hasReadPermission()) {
            throw new AuthorizationException("Client does not have read access to the repository");
        }
    }

    protected synchronized GitRepositoryClient getRepositoryClient(final FlowRegistryClientConfigurationContext context) throws IOException, FlowRegistryException {
        if (!clientInitialized.get()) {
            getLogger().info("Initializing repository client");
            repositoryClient = createRepositoryClient(context);
            initializeDefaultBucket(context);
            directoryExclusionPattern = Pattern.compile(context.getProperty(DIRECTORY_FILTER_EXCLUDE).getValue());
            clientInitialized.set(true);
        }
        return repositoryClient;
    }

    protected void invalidateClient() {
        clientInitialized.set(false);
        if (repositoryClient != null) {
            try {
                repositoryClient.close();
            } catch (final Exception e) {
                getLogger().warn("Error closing repository client", e);
            }
        }
        repositoryClient = null;
    }

    // If the client has write permissions to the repo, then ensure the directory for the default bucket is present and if not create it,
    // otherwise the client can only be used to import flows from the repo and won't be able to set up the default bucket
    private void initializeDefaultBucket(final FlowRegistryClientConfigurationContext context) throws IOException, FlowRegistryException {
        if (!repositoryClient.hasWritePermission()) {
            getLogger().info("Repository client [{}] does not have write permissions to the repository, skipping setup of default bucket", getIdentifier());
            return;
        }

        final String branch = context.getProperty(REPOSITORY_BRANCH).getValue();
        final Set<String> bucketDirectoryNames = repositoryClient.getTopLevelDirectoryNames(branch);
        if (!bucketDirectoryNames.isEmpty()) {
            getLogger().debug("Found {} existing buckets, skipping setup of default bucket", bucketDirectoryNames.size());
            return;
        }

        final String storageLocation = getStorageLocation(repositoryClient);
        getLogger().info("Creating default bucket in repo [{}] on branch [{}]", storageLocation, branch);

        repositoryClient.createContent(
                GitCreateContentRequest.builder()
                        .branch(branch)
                        .path(DEFAULT_BUCKET_KEEP_FILE_PATH)
                        .content(DEFAULT_BUCKET_KEEP_FILE_CONTENT)
                        .message(DEFAULT_BUCKET_KEEP_FILE_MESSAGE)
                        .build()
        );
    }

    /**
     * Create the property descriptors for this client.
     *
     * @return the list of property descriptors
     */
    protected abstract List<PropertyDescriptor> createPropertyDescriptors();

    /**
     * Provide the storage location for this client.
     *
     * @param repositoryClient the repository client
     * @return the storage location value
     */
    protected abstract String getStorageLocation(final GitRepositoryClient repositoryClient);

    /**
     * Creates the repository client based on the current configuration context.
     *
     * @param context the configuration context
     * @return the repository client
     * @throws IOException if an I/O error occurs creating the client
     * @throws FlowRegistryException if a non-I/O error occurs creating the client
     */
    protected abstract GitRepositoryClient createRepositoryClient(final FlowRegistryClientConfigurationContext context) throws IOException, FlowRegistryException;

    // protected to allow for overriding from tests
    protected FlowSnapshotSerializer createFlowSnapshotSerializer() {
        return new JacksonFlowSnapshotSerializer();
    }
}
