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
package org.apache.nifi.distributed.cache.client;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.protocol.ProtocolVersion;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.StandardVersionNegotiatorFactory;
import org.apache.nifi.remote.VersionNegotiatorFactory;
import org.apache.nifi.ssl.SSLContextProvider;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({"distributed", "cache", "state", "set", "cluster"})
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.server.SetCacheServer"})
@CapabilityDescription("Provides the ability to communicate with a SetCacheServer. This can be used in order to share a Set "
        + "between nodes in a NiFi cluster")
public class SetCacheClientService extends AbstractControllerService implements DistributedSetCacheClient {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Server Hostname")
            .description("The name of the server that is running the DistributedSetCacheServer service")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Server Port")
            .description("The port on the remote server that is to be used when communicating with the DistributedSetCacheServer service")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .defaultValue("4557")
            .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("If specified, indicates the SSL Context Service that is used to communicate with the "
                    + "remote server. If not specified, communications will not be encrypted")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();
    public static final PropertyDescriptor COMMUNICATIONS_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .description("Specifies how long to wait when communicating with the remote server before determining "
                    + "that there is a communications failure if data cannot be sent or received")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 secs")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        HOSTNAME,
        PORT,
        SSL_CONTEXT_SERVICE,
        COMMUNICATIONS_TIMEOUT
    );

    private volatile NettySetCacheClient cacheClient = null;

    /**
     * Creator of object used to broker the version of the distributed cache protocol with the service.
     */
    private volatile VersionNegotiatorFactory versionNegotiatorFactory = null;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
       return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        getLogger().debug("Enabling Set Cache Client Service [{}]", context.getName());
        this.versionNegotiatorFactory = new StandardVersionNegotiatorFactory(ProtocolVersion.V1.value());
        this.cacheClient = new NettySetCacheClient(
                context.getProperty(HOSTNAME).getValue(),
                context.getProperty(PORT).asInteger(),
                context.getProperty(COMMUNICATIONS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(),
                context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class),
                versionNegotiatorFactory,
                this.getIdentifier());
    }

    @OnDisabled
    public void onDisabled() throws IOException {
        getLogger().debug("Disabling Set Cache Client Service");
        this.cacheClient.close();
        this.versionNegotiatorFactory = null;
        this.cacheClient = null;
    }

    @OnStopped
    public void onStopped() throws IOException {
        if (isEnabled()) {
            onDisabled();
        }
    }

    @Override
    public <T> boolean addIfAbsent(T value, Serializer<T> serializer) throws IOException {
        final byte[] bytes = CacheClientSerde.serialize(value, serializer);
        return cacheClient.addIfAbsent(bytes);
    }

    @Override
    public <T> boolean contains(T value, Serializer<T> serializer) throws IOException {
        final byte[] bytes = CacheClientSerde.serialize(value, serializer);
        return cacheClient.contains(bytes);
    }

    @Override
    public <T> boolean remove(T value, Serializer<T> serializer) throws IOException {
        final byte[] bytes = CacheClientSerde.serialize(value, serializer);
        return cacheClient.remove(bytes);
    }

    @Override
    public void close() throws IOException {
        if (isEnabled()) {
            onDisabled();
        }
    }
}
