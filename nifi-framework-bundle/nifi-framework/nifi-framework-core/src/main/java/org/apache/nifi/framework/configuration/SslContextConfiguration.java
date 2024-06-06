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
package org.apache.nifi.framework.configuration;

import org.apache.nifi.framework.ssl.FrameworkSslContextHolder;
import org.apache.nifi.framework.ssl.SecurityStoreChangedPathListener;
import org.apache.nifi.framework.ssl.WatchServiceMonitorCommand;
import org.apache.nifi.security.ssl.KeyManagerListener;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.TrustManagerListener;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import java.security.KeyStore;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.util.NiFiProperties.SECURITY_AUTO_RELOAD_ENABLED;
import static org.apache.nifi.util.NiFiProperties.SECURITY_AUTO_RELOAD_INTERVAL;
import static org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE;
import static org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE_PASSWD;
import static org.apache.nifi.util.NiFiProperties.SECURITY_KEYSTORE_TYPE;
import static org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE;
import static org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE_PASSWD;
import static org.apache.nifi.util.NiFiProperties.SECURITY_TRUSTSTORE_TYPE;

/**
 * SSL Context Configuration class for Spring Application
 */
@Configuration
public class SslContextConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(SslContextConfiguration.class);

    private static final String EMPTY = "";

    private NiFiProperties properties;

    @Autowired
    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Bean
    public SSLContext sslContext() {
        return FrameworkSslContextHolder.getSslContext();
    }

    @Bean
    public X509ExtendedKeyManager keyManager() {
        return FrameworkSslContextHolder.getKeyManager();
    }

    @Bean
    public X509ExtendedTrustManager trustManager() {
        return FrameworkSslContextHolder.getTrustManager();
    }

    @Bean
    public WatchServiceMonitorCommand watchServiceMonitorCommand() {
        final WatchServiceMonitorCommand command;

        if (isReloadEnabled()) {
            final String reloadIntervalProperty = properties.getProperty(SECURITY_AUTO_RELOAD_INTERVAL);
            final long reloadIntervalSeconds = Math.round(FormatUtils.getPreciseTimeDuration(reloadIntervalProperty, TimeUnit.SECONDS));
            final Duration reloadDuration = Duration.ofSeconds(reloadIntervalSeconds);

            final X509ExtendedKeyManager keyManager = keyManager();
            final X509ExtendedTrustManager trustManager = trustManager();
            if (keyManager instanceof KeyManagerListener keyManagerListener && trustManager instanceof TrustManagerListener trustManagerListener) {
                final Set<Path> storeFileNames = getStoreFileNames();
                final SecurityStoreChangedPathListener changedPathListener = new SecurityStoreChangedPathListener(
                        storeFileNames,
                        keyManagerListener,
                        FrameworkSslContextHolder.getKeyManagerBuilder(),
                        trustManagerListener,
                        FrameworkSslContextHolder.getTrustManagerBuilder()
                );

                final WatchService watchService = storeWatchService();
                command = new WatchServiceMonitorCommand(watchService, changedPathListener);

                watchServiceScheduler().scheduleAtFixedRate(command, reloadDuration);
                logger.info("Scheduled Security Store Monitor with Duration [{}]", reloadDuration);
            } else {
                throw new IllegalStateException("Key Manager Listener or Trust Manager Listener not found");
            }
        } else {
            command = null;
        }
        return command;
    }

    @Bean
    public ThreadPoolTaskScheduler watchServiceScheduler() {
        final ThreadPoolTaskScheduler scheduler;

        if (isReloadEnabled()) {
            scheduler = new ThreadPoolTaskScheduler();
            scheduler.setThreadNamePrefix("Security Store Monitor ");
        } else {
            scheduler = null;
        }

        return scheduler;
    }

    @Bean
    public WatchService storeWatchService() {
        final WatchService watchService;

        final String keyStoreProperty = properties.getProperty(SECURITY_KEYSTORE);
        if (keyStoreProperty == null || keyStoreProperty.isBlank()) {
            watchService = null;
        } else if (isReloadEnabled()) {
            final Set<Path> storeDirectories = getStoreDirectories();
            final FileSystem fileSystem = FileSystems.getDefault();
            try {
                watchService = fileSystem.newWatchService();

                for (final Path storeDirectory : storeDirectories) {
                    storeDirectory.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
                }
            } catch (final IOException e) {
                throw new UncheckedIOException("Store Watch Service creation failed", e);
            }
        } else {
            watchService = null;
        }

        return watchService;
    }

    @Bean
    public KeyStore keyStore() {
        final KeyStore keyStore;

        final String keyStorePath = properties.getProperty(SECURITY_KEYSTORE);
        if (keyStorePath == null || keyStorePath.isBlank()) {
            keyStore = null;
        } else {
            final char[] keyStorePassword = properties.getProperty(SECURITY_KEYSTORE_PASSWD, EMPTY).toCharArray();
            final String keyStoreType = properties.getProperty(SECURITY_KEYSTORE_TYPE);

            try (InputStream inputStream = new FileInputStream(keyStorePath)) {
                keyStore = new StandardKeyStoreBuilder()
                        .inputStream(inputStream)
                        .password(keyStorePassword)
                        .type(keyStoreType)
                        .build();
            } catch (final IOException e) {
                throw new IllegalStateException("Failed to load Key Store [%s] Type [%s]".formatted(keyStorePath, keyStoreType), e);
            }
        }

        return keyStore;
    }

    @Bean
    public KeyStore trustStore() {
        final KeyStore trustStore;

        final String trustStorePath = properties.getProperty(SECURITY_TRUSTSTORE);
        if (trustStorePath == null || trustStorePath.isBlank()) {
            trustStore = null;
        } else {
            final char[] trustStorePassword = properties.getProperty(SECURITY_TRUSTSTORE_PASSWD, EMPTY).toCharArray();
            final String trustStoreType = properties.getProperty(SECURITY_TRUSTSTORE_TYPE);

            try (InputStream inputStream = new FileInputStream(trustStorePath)) {
                trustStore = new StandardKeyStoreBuilder()
                        .inputStream(inputStream)
                        .password(trustStorePassword)
                        .type(trustStoreType)
                        .build();
            } catch (final IOException e) {
                throw new IllegalStateException("Failed to load Trust Store [%s] Type [%s]".formatted(trustStorePath, trustStoreType), e);
            }
        }

        return trustStore;
    }

    private Set<Path> getStoreFileNames() {
        final Set<Path> storeFileNames = new HashSet<>();

        final Path keyStorePath = getKeyStorePath();
        addStoreFileName(keyStorePath, storeFileNames);

        final Path trustStorePath = getTrustStorePath();
        addStoreFileName(trustStorePath, storeFileNames);

        return storeFileNames;
    }

    private void addStoreFileName(final Path storePath, final Set<Path> storeFileNames) {
        storeFileNames.add(storePath.getFileName());

        if (Files.isSymbolicLink(storePath)) {
            try {
                final Path realStorePath = storePath.toRealPath();
                storeFileNames.add(realStorePath.getFileName());
            } catch (final IOException e) {
                throw new UncheckedIOException("Failed to resolve Store Path Link [%s]".formatted(storePath), e);
            }
        }
    }

    private Set<Path> getStoreDirectories() {
        final Set<Path> storeDirectories = new HashSet<>();

        final Path keyStorePath = getKeyStorePath();
        addStorePath(keyStorePath, storeDirectories);

        final Path trustStorePath = getTrustStorePath();
        addStorePath(trustStorePath, storeDirectories);

        return storeDirectories;
    }

    private void addStorePath(final Path storePath, final Set<Path> storeDirectories) {
        final Path storeDirectory = storePath.getParent();
        storeDirectories.add(storeDirectory);

        if (Files.isSymbolicLink(storePath)) {
            try {
                final Path realStorePath = storePath.toRealPath();
                final Path realStoreDirectory = realStorePath.getParent();
                storeDirectories.add(realStoreDirectory);
            } catch (final IOException e) {
                throw new UncheckedIOException("Failed to resolve Store Path Link [%s]".formatted(storePath), e);
            }
        }
    }

    private Path getKeyStorePath() {
        final String keyStoreProperty = properties.getProperty(SECURITY_KEYSTORE);
        if (keyStoreProperty == null || keyStoreProperty.isBlank()) {
            throw new IllegalStateException("Security Property [%s] not configured".formatted(SECURITY_KEYSTORE));
        }
        return Paths.get(keyStoreProperty).toAbsolutePath();
    }

    private Path getTrustStorePath() {
        final String trustStoreProperty = properties.getProperty(SECURITY_TRUSTSTORE);
        if (trustStoreProperty == null || trustStoreProperty.isBlank()) {
            throw new IllegalStateException("Security Property [%s] not configured".formatted(SECURITY_TRUSTSTORE));
        }

        return Paths.get(trustStoreProperty).toAbsolutePath();
    }

    private boolean isReloadEnabled() {
        final String reloadEnabledProperty = properties.getProperty(SECURITY_AUTO_RELOAD_ENABLED);
        return Boolean.parseBoolean(reloadEnabledProperty);
    }
}
