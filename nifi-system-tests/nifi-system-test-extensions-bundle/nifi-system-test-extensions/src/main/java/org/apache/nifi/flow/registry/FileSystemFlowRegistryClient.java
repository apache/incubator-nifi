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

package org.apache.nifi.flow.registry;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.flow.AbstractFlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryPermissions;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.RegisteredFlowVersionInfo;
import org.apache.nifi.util.file.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FileSystemFlowRegistryClient extends AbstractFlowRegistryClient {
    private final ObjectMapper objectMapper = new ObjectMapper();

    {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
        .name("Directory")
        .displayName("Directory")
        .description("The root directory to store flows in")
        .required(true)
        .addValidator(StandardValidators.createDirectoryExistsValidator(false, false))
        .defaultValue("target/flow-registry-storage")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(DIRECTORY);
    }

    @Override
    public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String storageLocation) {
        try {
            final URL url = new URL(storageLocation);
            final File file = new java.io.File(url.toURI());
            final Path path = file.toPath();

            final String configuredDirectory = context.getProperty(DIRECTORY).getValue();
            final Path rootPath = Paths.get(configuredDirectory);

            // If this doesn't throw an Exception, the given storageLocation is relative to the root path
            rootPath.relativize(path);
        } catch (final Exception e) {
            return false;
        }

        return true;
    }

    @Override
    public Set<FlowRegistryBucket> getBuckets(final FlowRegistryClientConfigurationContext context) throws IOException {
        final File rootDir = getRootDirectory(context);
        final File[] children = rootDir.listFiles();
        if (children == null) {
            throw new IOException("Cannot get listing of directory " + rootDir.getAbsolutePath());
        }

        final Set<FlowRegistryBucket> buckets = Arrays.stream(children).map(this::toBucket).collect(Collectors.toSet());
        return buckets;
    }

    private FlowRegistryBucket toBucket(final File file) {
        final FlowRegistryBucket bucket = new FlowRegistryBucket();
        bucket.setIdentifier(file.getName());
        bucket.setName(bucket.getName());

        final FlowRegistryPermissions permissions = new FlowRegistryPermissions();
        permissions.setCanDelete(true);
        permissions.setCanRead(true);
        permissions.setCanWrite(true);

        bucket.setPermissions(permissions);
        return bucket;
    }

    private File getRootDirectory(final FlowRegistryClientConfigurationContext context) {
        final String rootDirectory = context.getProperty(DIRECTORY).getValue();
        if (rootDirectory == null) {
            throw new IllegalStateException("Registry Client cannot be used, as Directory property has not been set");
        }

        return new File(rootDirectory);
    }

    @Override
    public FlowRegistryBucket getBucket(final FlowRegistryClientConfigurationContext context, final String bucketId) {
        final File rootDir = getRootDirectory(context);
        final File bucketDir = new File(rootDir, bucketId);
        final FlowRegistryBucket bucket = toBucket(bucketDir);
        return bucket;
    }

    @Override
    public RegisteredFlow registerFlow(final FlowRegistryClientConfigurationContext context, final RegisteredFlow flow) throws IOException {
        final File rootDir = getRootDirectory(context);
        final String bucketId = flow.getBucketIdentifier();
        final File bucketDir = new File(rootDir, bucketId);
        final File flowDir = new File(bucketDir, flow.getIdentifier());
        Files.createDirectories(flowDir.toPath());

        return flow;
    }

    @Override
    public RegisteredFlow deregisterFlow(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws IOException {
        final File rootDir = getRootDirectory(context);
        final File bucketDir = new File(rootDir, bucketId);
        final File flowDir = new File(bucketDir, flowId);

        final File[] versionDirs = flowDir.listFiles();

        final RegisteredFlow flow = new RegisteredFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setBucketName(bucketId);
        flow.setIdentifier(flowId);
        flow.setLastModifiedTimestamp(flowDir.lastModified());
        flow.setVersionCount(versionDirs == null ? 0 : versionDirs.length);

        FileUtils.deleteFile(flowDir, true);
        return flow;
    }

    @Override
    public RegisteredFlow getFlow(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) {
        final File rootDir = getRootDirectory(context);
        final File bucketDir = new File(rootDir, bucketId);
        final File flowDir = new File(bucketDir, flowId);

        final File[] versionDirs = flowDir.listFiles();

        final RegisteredFlow flow = new RegisteredFlow();
        flow.setBucketIdentifier(bucketId);
        flow.setBucketName(bucketId);
        flow.setIdentifier(flowId);
        flow.setLastModifiedTimestamp(flowDir.lastModified());
        flow.setVersionCount(versionDirs == null ? 0 : versionDirs.length);

        return flow;
    }

    @Override
    public Set<RegisteredFlow> getFlows(final FlowRegistryClientConfigurationContext context, final String bucketId) throws IOException {
        final File rootDir = getRootDirectory(context);
        final File bucketDir = new File(rootDir, bucketId);
        final File[] flowDirs = bucketDir.listFiles();
        if (flowDirs == null) {
            throw new IOException("Could not get listing of directory " + bucketDir);
        }

        final Set<RegisteredFlow> registeredFlows = new HashSet<>();
        for (final File flowDir : flowDirs) {
            final RegisteredFlow flow = getFlow(context, bucketId, flowDir.getName());
            registeredFlows.add(flow);
        }

        return registeredFlows;
    }

    @Override
    public RegisteredFlowSnapshot getFlowContents(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId, final int version) throws IOException {
        final File rootDir = getRootDirectory(context);
        final File bucketDir = new File(rootDir, bucketId);
        final File flowDir = new File(bucketDir, flowId);
        final File versionDir = new File(flowDir, String.valueOf(version));
        final File snapshotFile = new File(versionDir, "snapshot.json");

        final Pattern intPattern = Pattern.compile("\\d+");
        final File[] versionFiles = flowDir.listFiles(file -> intPattern.matcher(file.getName()).matches());

        final JsonFactory factory = new JsonFactory(objectMapper);
        try (final JsonParser parser = factory.createParser(snapshotFile)) {
            final RegisteredFlowSnapshot snapshot = parser.readValueAs(RegisteredFlowSnapshot.class);
            populateBucket(snapshot, bucketId);
            populateFlow(snapshot, bucketId, flowId, version, versionFiles == null ? 0 : versionFiles.length);

            return snapshot;
        }
    }

    private void populateBucket(final RegisteredFlowSnapshot snapshot, final String bucketId) {
        final FlowRegistryBucket existingBucket = snapshot.getBucket();
        if (existingBucket != null) {
            return;
        }

        final FlowRegistryBucket bucket = new FlowRegistryBucket();
        bucket.setCreatedTimestamp(System.currentTimeMillis());
        bucket.setIdentifier(bucketId);
        bucket.setName(bucketId);
        bucket.setPermissions(createAllowAllPermissions());
        snapshot.setBucket(bucket);

        snapshot.getSnapshotMetadata().setBucketIdentifier(bucketId);
    }

    private void populateFlow(final RegisteredFlowSnapshot snapshot, final String bucketId, final String flowId, final int version, final int numVersions) {
        final RegisteredFlow existingFlow = snapshot.getFlow();
        if (existingFlow != null) {
            return;
        }

        final RegisteredFlow flow = new RegisteredFlow();
        flow.setCreatedTimestamp(System.currentTimeMillis());
        flow.setLastModifiedTimestamp(System.currentTimeMillis());
        flow.setBucketIdentifier(bucketId);
        flow.setBucketName(bucketId);
        flow.setIdentifier(flowId);
        flow.setName(flowId);
        flow.setPermissions(createAllowAllPermissions());
        flow.setVersionCount(numVersions);

        final RegisteredFlowVersionInfo versionInfo = new RegisteredFlowVersionInfo();
        versionInfo.setVersion(version);
        flow.setVersionInfo(versionInfo);

        snapshot.setFlow(flow);
        snapshot.getSnapshotMetadata().setFlowIdentifier(flowId);
    }

    @Override
    public RegisteredFlowSnapshot registerFlowSnapshot(final FlowRegistryClientConfigurationContext context, final RegisteredFlowSnapshot flowSnapshot) throws IOException {
        final File rootDir = getRootDirectory(context);
        final RegisteredFlowSnapshotMetadata metadata = flowSnapshot.getSnapshotMetadata();
        final String bucketId = metadata.getBucketIdentifier();
        final String flowId = metadata.getFlowIdentifier();
        final long version = metadata.getVersion();

        final File bucketDir = new File(rootDir, bucketId);
        final File flowDir = new File(bucketDir, flowId);
        final File versionDir = new File(flowDir, String.valueOf(version));

        // Create the directory for the version, if it doesn't exist.
        if (!versionDir.exists()) {
            Files.createDirectories(versionDir.toPath());
        }

        final File snapshotFile = new File(versionDir, "snapshot.json");

        final RegisteredFlowSnapshot fullyPopulated = fullyPopulate(flowSnapshot, flowDir);
        final JsonFactory factory = new JsonFactory(objectMapper);
        try (final JsonGenerator generator = factory.createGenerator(snapshotFile, JsonEncoding.UTF8)) {
            generator.writeObject(fullyPopulated);
        }

        return fullyPopulated;
    }

    private RegisteredFlowSnapshot fullyPopulate(final RegisteredFlowSnapshot requested, final File flowDir) {
        final RegisteredFlowSnapshot full = new RegisteredFlowSnapshot();
        full.setExternalControllerServices(requested.getExternalControllerServices());
        full.setFlowContents(requested.getFlowContents());
        full.setFlowEncodingVersion(requested.getFlowEncodingVersion());
        full.setParameterContexts(requested.getParameterContexts());
        full.setParameterProviders(requested.getParameterProviders());
        full.setSnapshotMetadata(requested.getSnapshotMetadata());

        // Populated the bucket
        final FlowRegistryBucket bucket;
        if (requested.getBucket() == null) {
            bucket = new FlowRegistryBucket();
            bucket.setCreatedTimestamp(System.currentTimeMillis());
            bucket.setDescription("Description");
            bucket.setIdentifier(requested.getSnapshotMetadata().getBucketIdentifier());
            bucket.setName(requested.getSnapshotMetadata().getBucketIdentifier());

            final FlowRegistryPermissions bucketPermissions = createAllowAllPermissions();
            bucket.setPermissions(bucketPermissions);
        } else {
            bucket = requested.getBucket();
        }
        full.setBucket(bucket);

        // Populate the flow
        final RegisteredFlow flow;
        if (requested.getFlow() == null) {
            flow = new RegisteredFlow();
            flow.setBucketIdentifier(requested.getSnapshotMetadata().getBucketIdentifier());
            flow.setBucketName(requested.getSnapshotMetadata().getBucketIdentifier());
            flow.setCreatedTimestamp(System.currentTimeMillis());
            flow.setDescription("Description");
            flow.setIdentifier(requested.getSnapshotMetadata().getFlowIdentifier());
            flow.setName(requested.getSnapshotMetadata().getFlowIdentifier());
            flow.setLastModifiedTimestamp(System.currentTimeMillis());
            flow.setPermissions(createAllowAllPermissions());

            final File[] flowVersionDirs = flowDir.listFiles();
            final int versionCount = flowVersionDirs == null ? 0 : flowVersionDirs.length;;
            flow.setVersionCount(versionCount);

            final RegisteredFlowVersionInfo versionInfo = new RegisteredFlowVersionInfo();
            versionInfo.setVersion(versionCount);
            flow.setVersionInfo(versionInfo);
        } else {
            flow = requested.getFlow();
        }
        full.setFlow(flow);

        return full;
    }

    private FlowRegistryPermissions createAllowAllPermissions() {
        final FlowRegistryPermissions permissions = new FlowRegistryPermissions();
        permissions.setCanWrite(true);
        permissions.setCanRead(true);
        permissions.setCanDelete(true);
        return permissions;
    }

    @Override
    public Set<RegisteredFlowSnapshotMetadata> getFlowVersions(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws IOException {
        final File rootDir = getRootDirectory(context);
        final File bucketDir = new File(rootDir, bucketId);
        final File flowDir = new File(bucketDir, flowId);
        final File[] versionDirs = flowDir.listFiles();
        if (versionDirs == null) {
            throw new IOException("Could not list directories of " + flowDir);
        }

        final Set<RegisteredFlowSnapshotMetadata> metadatas = new HashSet<>();
        for (final File versionDir : versionDirs) {
            final String versionName = versionDir.getName();

            final RegisteredFlowSnapshotMetadata metadata = new RegisteredFlowSnapshotMetadata();
            metadata.setVersion(Integer.parseInt(versionName));
            metadata.setTimestamp(versionDir.lastModified());
            metadata.setFlowIdentifier(flowId);
            metadata.setBucketIdentifier(bucketId);
            metadata.setAuthor("System Test Author");
            metadatas.add(metadata);
        }

        return metadatas;
    }

    @Override
    public int getLatestVersion(final FlowRegistryClientConfigurationContext context, final String bucketId, final String flowId) throws IOException {
        final File rootDir = getRootDirectory(context);
        final File bucketDir = new File(rootDir, bucketId);
        final File flowDir = new File(bucketDir, flowId);
        final File[] versionDirs = flowDir.listFiles();
        if (versionDirs == null) {
            throw new IOException("Cannot list directories of " + flowDir);
        }

        final OptionalInt greatestValue = Arrays.stream(versionDirs)
            .map(File::getName)
            .mapToInt(Integer::parseInt)
            .max();
        return greatestValue.orElse(-1);
    }
}
