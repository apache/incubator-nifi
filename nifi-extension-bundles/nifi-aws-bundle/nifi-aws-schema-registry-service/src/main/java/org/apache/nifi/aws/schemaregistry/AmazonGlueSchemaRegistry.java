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
package org.apache.nifi.aws.schemaregistry;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.aws.schemaregistry.client.CachingSchemaRegistryClient;
import org.apache.nifi.aws.schemaregistry.client.GlueSchemaRegistryClient;
import org.apache.nifi.aws.schemaregistry.client.SchemaRegistryClient;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.ssl.SSLContextProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.Proxy;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"schema", "registry", "aws", "avro", "glue"})
@CapabilityDescription("Provides a Schema Registry that interacts with the AWS Glue Schema Registry so that those Schemas that are stored in the Glue Schema "
        + "Registry can be used in NiFi. When a Schema is looked up by name by this registry, it will find a Schema in the Glue Schema Registry with their names.")
public class AmazonGlueSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT,
            SchemaField.SCHEMA_TEXT_FORMAT, SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);

    static final PropertyDescriptor SCHEMA_REGISTRY_NAME = new PropertyDescriptor.Builder()
            .name("schema-registry-name")
            .displayName("Schema Registry Name")
            .description("The name of the Schema Registry")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .name("region")
            .displayName("Region")
            .description("The region of the cloud resources")
            .required(true)
            .allowableValues(getAvailableRegions())
            .defaultValue(createAllowableValue(Region.US_WEST_2).getValue())
            .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache Size")
            .description("Specifies how many Schemas should be cached from the Schema Registry")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .required(true)
            .build();

    static final PropertyDescriptor CACHE_EXPIRATION = new PropertyDescriptor.Builder()
            .name("cache-expiration")
            .displayName("Cache Expiration")
            .description("Specifies how long a Schema that is cached should remain in the cache. Once this time period elapses, a "
                    + "cached version of a schema will no longer be used, and the service will have to communicate with the "
                    + "Schema Registry again in order to obtain the schema.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 hour")
            .required(true)
            .build();

    static final PropertyDescriptor COMMUNICATIONS_TIMEOUT = new PropertyDescriptor.Builder()
            .name("communications-timeout")
            .displayName("Communications Timeout")
            .description("Specifies how long to wait to receive data from the Schema Registry before considering the communications a failure")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue("30 secs")
            .required(true)
            .build();

    public static final PropertyDescriptor AWS_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("aws-credentials-provider-service")
            .displayName("AWS Credentials Provider Service")
            .description("The Controller Service that is used to obtain AWS credentials provider")
            .required(false)
            .identifiesControllerService(AWSCredentialsProviderService.class)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("Specifies an optional SSL Context Service that, if provided, will be used to create connections")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH};

    private static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(PROXY_SPECS);

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SCHEMA_REGISTRY_NAME,
            REGION,
            COMMUNICATIONS_TIMEOUT,
            CACHE_SIZE,
            CACHE_EXPIRATION,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            PROXY_CONFIGURATION_SERVICE,
            SSL_CONTEXT_SERVICE
    );


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private volatile SchemaRegistryClient client;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final AWSCredentialsProviderService awsCredentialsProviderService = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(AWSCredentialsProviderService.class);
        final AwsCredentialsProvider credentialsProvider = awsCredentialsProviderService.getAwsCredentialsProvider();
        final String schemaRegistryName = context.getProperty(SCHEMA_REGISTRY_NAME).getValue();
        final String region = context.getProperty(REGION).getValue();
        final int cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final long cacheExpiration = context.getProperty(CACHE_EXPIRATION).asTimePeriod(TimeUnit.NANOSECONDS);

        final GlueClientBuilder glueClientBuilder = GlueClient
                .builder()
                .credentialsProvider(credentialsProvider)
                .httpClient(createSdkHttpClient(context))
                .region(Region.of(region));

        final GlueSchemaRegistryClient glueSchemaRegistryClient = new GlueSchemaRegistryClient(glueClientBuilder.build(), schemaRegistryName);

        client = new CachingSchemaRegistryClient(glueSchemaRegistryClient, cacheSize, cacheExpiration);
    }

    @Override
    public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final String schemaName = schemaIdentifier.getName().orElseThrow(
                () -> new SchemaNotFoundException("Cannot retrieve schema because Schema Name is not present")
        );

        final OptionalInt version = schemaIdentifier.getVersion();

        if (version.isPresent()) {
            return client.getSchema(schemaName, version.getAsInt());
        } else {
            return client.getSchema(schemaName);
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }

    private static AllowableValue createAllowableValue(final Region region) {
        final String description = region.metadata() != null ? region.metadata().description() : region.id();
        return new AllowableValue(region.id(), description, "AWS Region Code : " + region.id());
    }

    private static AllowableValue[] getAvailableRegions() {
        final List<AllowableValue> values = new ArrayList<>();
        for (final Region region : Region.regions()) {
            values.add(createAllowableValue(region));
        }
        values.sort(Comparator.comparing(AllowableValue::getDisplayName));
        return values.toArray(new AllowableValue[0]);
    }

    private SdkHttpClient createSdkHttpClient(final ConfigurationContext context) {
        final ApacheHttpClient.Builder builder = ApacheHttpClient.builder();

        final int communicationsTimeout = context.getProperty(COMMUNICATIONS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        builder.connectionTimeout(Duration.ofMillis(communicationsTimeout));
        builder.socketTimeout(Duration.ofMillis(communicationsTimeout));

        if (this.getSupportedPropertyDescriptors().contains(SSL_CONTEXT_SERVICE)) {
            final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
            if (sslContextProvider != null) {
                final X509TrustManager trustManager = sslContextProvider.createTrustManager();
                final TrustManager[] trustManagers = new TrustManager[]{trustManager};
                builder.tlsTrustManagersProvider(() -> trustManagers);

                final Optional<X509ExtendedKeyManager> keyManagerFound = sslContextProvider.createKeyManager();
                if (keyManagerFound.isPresent()) {
                    final X509ExtendedKeyManager keyManager = keyManagerFound.get();
                    final KeyManager[] keyManagers = new KeyManager[]{keyManager};
                    builder.tlsKeyManagersProvider(() -> keyManagers);
                }
            }
        }
        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context);

        if (Proxy.Type.HTTP.equals(proxyConfig.getProxyType())) {
            final software.amazon.awssdk.http.apache.ProxyConfiguration.Builder proxyConfigBuilder = software.amazon.awssdk.http.apache.ProxyConfiguration.builder()
                    .endpoint(URI.create(String.format("http://%s:%s", proxyConfig.getProxyServerHost(), proxyConfig.getProxyServerPort())));

            if (proxyConfig.hasCredential()) {
                proxyConfigBuilder.username(proxyConfig.getProxyUserName());
                proxyConfigBuilder.password(proxyConfig.getProxyUserPassword());
            }
            builder.proxyConfiguration(proxyConfigBuilder.build());
        }

        return builder.build();
    }
}
