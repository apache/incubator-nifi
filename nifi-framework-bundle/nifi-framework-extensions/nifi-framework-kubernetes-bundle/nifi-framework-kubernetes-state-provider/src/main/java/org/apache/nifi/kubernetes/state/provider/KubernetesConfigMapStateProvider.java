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
package org.apache.nifi.kubernetes.state.provider;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.kubernetes.client.ServiceAccountNamespaceProvider;
import org.apache.nifi.kubernetes.client.StandardKubernetesClientProvider;
import org.apache.nifi.logging.ComponentLog;

/**
 * State Provider implementation based on Kubernetes ConfigMaps with Base64 encoded keys to meet Kubernetes constraints
 */
public class KubernetesConfigMapStateProvider extends AbstractConfigurableComponent implements StateProvider {
    static final PropertyDescriptor CONFIG_MAP_NAME_PREFIX = new PropertyDescriptor.Builder()
        .name("ConfigMap Name Prefix")
        .description("Optional prefix that the Provider will prepend to Kubernetes ConfigMap names. The resulting ConfigMap name will contain nifi-component and the component identifier.")
        .addValidator(Validator.VALID)
        .required(false)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONFIG_MAP_NAME_PREFIX
    );

    private static final int MAX_UPDATE_ATTEMPTS = 5;

    private static final Scope[] SUPPORTED_SCOPES = {Scope.CLUSTER};

    private static final Charset KEY_CHARACTER_SET = StandardCharsets.UTF_8;

    private static final String CONFIG_MAP_NAME_FORMAT = "%snifi-component-%%s";

    private static final String CONFIG_MAP_NAME_PATTERN_FORMAT = "^%snifi-component-(.+)$";

    private static final String PREFIX_SEPARATOR = "-";

    private static final String EMPTY_PREFIX = "";

    private static final int COMPONENT_ID_GROUP = 1;

    /** Encode ConfigMap keys using URL Encoder without padding characters for compliance with Kubernetes naming */
    private static final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();

    private static final Base64.Decoder decoder = Base64.getUrlDecoder();

    private final AtomicBoolean enabled = new AtomicBoolean();

    private String configMapNameFormat;

    private Pattern configMapNamePattern;

    private KubernetesClient kubernetesClient;

    private String namespace;

    private String identifier;

    private ComponentLog logger;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * Get configured component identifier
     *
     * @return Component Identifier
     */
    @Override
    public String getIdentifier() {
        return identifier;
    }

    /**
     * Initialize Provider using configured properties
     *
     * @param context Initialization Context
     */
    @Override
    public void initialize(final StateProviderInitializationContext context) {
        this.identifier = context.getIdentifier();
        this.logger = context.getLogger();
        this.kubernetesClient = getKubernetesClient();
        this.namespace = new ServiceAccountNamespaceProvider().getNamespace();

        final PropertyValue configMapNamePrefixProperty = context.getProperty(CONFIG_MAP_NAME_PREFIX);
        final String prefixPropertyValue = configMapNamePrefixProperty.getValue();

        final String configMapNamePrefix = prefixPropertyValue == null || prefixPropertyValue.isBlank() ? EMPTY_PREFIX : prefixPropertyValue + PREFIX_SEPARATOR;
        configMapNameFormat = String.format(CONFIG_MAP_NAME_FORMAT, configMapNamePrefix);
        configMapNamePattern = Pattern.compile(String.format(CONFIG_MAP_NAME_PATTERN_FORMAT, configMapNamePrefix));
    }

    /**
     * Shutdown Provider
     */
    @Override
    public void shutdown() {
        kubernetesClient.close();
        logger.info("Provider shutdown");
    }

    /**
     * Set State as ConfigMap based on Component Identifier
     *
     * @param state State Map
     * @param componentId Component Identifier
     * @throws IOException Thrown on failure to set State Map
     */
    @Override
    public void setState(final Map<String, String> state, final String componentId) throws IOException {
        try {
            final ConfigMap configMap = createConfigMapBuilder(state, componentId).build();
            final Resource<ConfigMap> configMapResource = kubernetesClient.configMaps().resource(configMap);

            ConfigMap configMapCreated = null;

            // Attempt to create or update, up to 3 times. We expect that we will update more frequently than create
            // so we first attempt to update. If we get back a 404, then we create it.
            boolean create = false;
            for (int attempt = 0; attempt < MAX_UPDATE_ATTEMPTS; attempt++) {
                try {
                    if (create) {
                        configMapCreated = configMapResource.create();
                    } else {
                        configMapCreated = configMapResource.update();
                    }

                    break;
                } catch (final KubernetesClientException e) {
                    final int returnCode = e.getCode();
                    if (returnCode == 404) {
                        // A 404 return code indicates that we need to create the resource instead of update it.
                        // Now, we will attempt to create the resource instead of update it, so we'll reset the attempt counter.
                        attempt = 0;
                        create = true;
                        continue;
                    }

                    if (returnCode >= 500) {
                        // Server-side error. We should retry, up to some number of attempts.
                        if (attempt == MAX_UPDATE_ATTEMPTS - 1) {
                            throw e;
                        }
                    } else {
                        // There's an issue with the request. Throw the Exception.
                        throw e;
                    }
                } catch (final Exception e) {
                    if (attempt < MAX_UPDATE_ATTEMPTS - 1) {
                        logger.warn("Failed to update state for component with ID {}. Will attempt to update the resource again.", componentId, e);
                    } else {
                        logger.error("Failed to update state for component with ID {}", componentId, e);
                        throw e;
                    }
                }
            }

            if (configMapCreated == null) {
                throw new IOException("Exhausted maximum number of attempts (%s) to update state for component with ID %s but could not update it".formatted(MAX_UPDATE_ATTEMPTS, componentId));
            }

            final Optional<String> version = getVersion(configMapCreated);
            logger.debug("Set State Component ID [{}] Version [{}]", componentId, version);
        } catch (final KubernetesClientException e) {
            if (isNotFound(e.getCode())) {
                logger.debug("State not found for Component ID [{}]", componentId, e);
            } else {
                throw new IOException(String.format("Failed to update state for Component with ID [%s]", componentId), e);
            }
        } catch (final RuntimeException e) {
            throw new IOException(String.format("Failed to update state for Component with ID [%s]", componentId), e);
        }
    }

    /**
     * Get State Map for Component Identifier
     *
     * @param componentId Component Identifier of State to be retrieved
     * @return State Map
     * @throws IOException Thrown on failure to get State Map
     */
    @Override
    public StateMap getState(final String componentId) throws IOException {
        try {
            final ConfigMap configMap = configMapResource(componentId).get();
            final Map<String, String> data = configMap == null ? Collections.emptyMap() : getDecodedMap(configMap.getData());
            final Optional<String> version = configMap == null ? Optional.empty() : getVersion(configMap);
            return new StandardStateMap(data, version);
        } catch (final RuntimeException e) {
            throw new IOException(String.format("Get failed for Component ID [%s]", componentId), e);
        }
    }

    /**
     * Replace State ConfigMap with new State based on current resource version
     *
     * @param currentState Current State Map with version
     * @param state New State Map
     * @param componentId Component Identifier
     * @return Replace operation status
     */
    @Override
    public boolean replace(final StateMap currentState, final Map<String, String> state, final String componentId) throws IOException {
        final ConfigMapBuilder configMapBuilder = createConfigMapBuilder(state, componentId);
        final Optional<String> stateVersion = currentState.getStateVersion();
        if (stateVersion.isPresent()) {
            final String resourceVersion = stateVersion.get();
            configMapBuilder.editOrNewMetadata().withResourceVersion(resourceVersion).endMetadata();
        }
        final ConfigMap configMap = configMapBuilder.build();

        try {
            Resource<ConfigMap> configMapResource = kubernetesClient.configMaps().resource(configMap);
            final ConfigMap newConfigMap;
            if (stateVersion.isPresent()) {
                newConfigMap = configMapResource.update();
            } else {
                newConfigMap = configMapResource.create();
            }
            final Optional<String> version = getVersion(newConfigMap);
            logger.debug("Replaced State Component ID [{}] Version [{}]", componentId, version);
            return true;
        } catch (final KubernetesClientException e) {
            if (isNotFoundOrConflict(e.getCode())) {
                logger.debug("Replace State Failed Component ID [{}] Version [{}]", componentId, stateVersion, e);
                return false;
            } else {
                throw new IOException(String.format("Replace failed for Component ID [%s]", componentId), e);
            }
        } catch (final RuntimeException e) {
            throw new IOException(String.format("Replace failed for Component ID [%s]", componentId), e);
        }
    }

    /**
     * Clear state information for specified Component Identifier
     *
     * @param componentId the id of the component for which state is being cleared
     * @throws IOException Thrown on failure to clear state for Component Identifier
     */
    @Override
    public void clear(final String componentId) throws IOException {
        try {
            setState(Collections.emptyMap(), componentId);
        } catch (final RuntimeException e) {
            throw new IOException(String.format("Clear failed for Component ID [%s]", componentId), e);
        }
    }

    /**
     * Remove state information for specified Component Identifier
     *
     * @param componentId Identifier of component removed from the configuration
     * @throws IOException Thrown on failure to remove state for Component Identifier
     */
    @Override
    public void onComponentRemoved(final String componentId) throws IOException {
        try {
            final List<StatusDetails> deleteStatus = configMapResource(componentId).delete();
            logger.debug("Config Map [{}] deleted {}", componentId, deleteStatus);
        } catch (final RuntimeException e) {
            throw new IOException(String.format("Remove failed for Component ID [%s]", componentId), e);
        }
    }

    /**
     * Enable Provider
     */
    @Override
    public void enable() {
        enabled.getAndSet(true);
    }

    /**
     * Disable Provider
     */
    @Override
    public void disable() {
        enabled.getAndSet(false);
    }

    /**
     * Get Enabled status
     *
     * @return Enabled status
     */
    @Override
    public boolean isEnabled() {
        return enabled.get();
    }

    /**
     * Get Supported Scopes returns CLUSTER
     *
     * @return Supported Scopes including CLUSTER
     */
    @Override
    public Scope[] getSupportedScopes() {
        return SUPPORTED_SCOPES;
    }

    /**
     * Kubernetes ConfigMap Provider supported Component Enumeration
     *
     * @return Component Enumeration supported
     */
    @Override
    public boolean isComponentEnumerationSupported() {
        return true;
    }

    /**
     * Get Component Identifiers with stored state based on ConfigMap names matching standard pattern
     *
     * @return Component Identifiers with stored state or empty when none found
     */
    @Override
    public Collection<String> getStoredComponentIds() {
        final ConfigMapList configMapList = kubernetesClient.configMaps().inNamespace(namespace).list();
        return configMapList.getItems().stream()
                .map(ConfigMap::getMetadata)
                .map(ObjectMeta::getName)
                .map(configMapNamePattern::matcher)
                .filter(Matcher::matches)
                .map(matcher -> matcher.group(COMPONENT_ID_GROUP))
                .toList();
    }

    /**
     * Get Kubernetes Client using standard configuration
     *
     * @return Kubernetes Client
     */
    protected KubernetesClient getKubernetesClient() {
        return new StandardKubernetesClientProvider().getKubernetesClient();
    }

    private Resource<ConfigMap> configMapResource(final String componentId) {
        final String name = getConfigMapName(componentId);
        return kubernetesClient.configMaps().inNamespace(namespace).withName(name);
    }

    private ConfigMapBuilder createConfigMapBuilder(final Map<String, String> state, final String componentId) {
        final Map<String, String> encodedData = getEncodedMap(state);
        final String name = getConfigMapName(componentId);
        return new ConfigMapBuilder()
                .withNewMetadata()
                .withNamespace(namespace)
                .withName(name)
                .endMetadata()
                .withData(encodedData);
    }

    private String getConfigMapName(final String componentId) {
        return String.format(configMapNameFormat, componentId);
    }

    private Optional<String> getVersion(final ConfigMap configMap) {
        final ObjectMeta metadata = configMap.getMetadata();
        final String resourceVersion = metadata.getResourceVersion();
        return Optional.ofNullable(resourceVersion);
    }

    private Map<String, String> getEncodedMap(final Map<String, String> stateMap) {
        final Map<String, String> encodedMap = new LinkedHashMap<>();
        stateMap.forEach((key, value) -> {
            final byte[] keyBytes = key.getBytes(KEY_CHARACTER_SET);
            final String encodedKey = encoder.encodeToString(keyBytes);
            encodedMap.put(encodedKey, value);
        });
        return encodedMap;
    }

    private Map<String, String> getDecodedMap(final Map<String, String> configMap) {
        final Map<String, String> decodedMap = new LinkedHashMap<>();
        configMap.forEach((key, value) -> {
            final byte[] keyBytes = decoder.decode(key);
            final String decodedKey = new String(keyBytes, KEY_CHARACTER_SET);
            decodedMap.put(decodedKey, value);
        });
        return decodedMap;
    }

    private boolean isNotFound(final int code) {
        return HttpURLConnection.HTTP_NOT_FOUND == code;
    }

    private boolean isNotFoundOrConflict(final int code) {
        return isNotFound(code) || HttpURLConnection.HTTP_CONFLICT == code;
    }
}
