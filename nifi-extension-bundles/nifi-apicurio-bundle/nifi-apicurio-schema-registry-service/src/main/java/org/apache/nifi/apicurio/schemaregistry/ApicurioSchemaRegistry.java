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
package org.apache.nifi.apicurio.schemaregistry;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.apicurio.schemaregistry.client.ApicurioSchemaRegistryClient;
import org.apache.nifi.apicurio.schemaregistry.client.CachingSchemaRegistryClient;
import org.apache.nifi.apicurio.schemaregistry.client.SchemaRegistryApiClient;
import org.apache.nifi.apicurio.schemaregistry.client.SchemaRegistryClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"schema", "registry", "apicurio", "avro"})
@CapabilityDescription("Provides a Schema Registry that interacts with the Apicurio Schema Registry so that those Schemas that are stored in the Apicurio Schema "
        + "Registry can be used in NiFi. When a Schema is looked up by name by this registry, it will find a Schema in the Apicurio Schema Registry with their artifact identifiers.")
public class ApicurioSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT,
            SchemaField.SCHEMA_TEXT_FORMAT, SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);

    static final PropertyDescriptor SCHEMA_REGISTRY_URL = new PropertyDescriptor.Builder()
            .name("Schema Registry URL")
            .displayName("Schema Registry URL")
            .description("The URL of the Schema Registry e.g. http://localhost:8080")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor SCHEMA_GROUP_ID = new PropertyDescriptor.Builder()
            .name("Schema Group ID")
            .displayName("Schema Group ID")
            .description("The artifact Group ID for the schemas")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("default")
            .required(true)
            .build();
    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Cache Size")
            .displayName("Cache Size")
            .description("Specifies how many Schemas should be cached from the Schema Registry. The cache size must be "
                    + "a non-negative integer. When it is set to 0, the cache is effectively disabled.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .required(true)
            .build();

    static final PropertyDescriptor CACHE_EXPIRATION = new PropertyDescriptor.Builder()
            .name("Cache Expiration")
            .displayName("Cache Expiration")
            .description("Specifies how long a Schema that is cached should remain in the cache. Once this time period elapses, a "
                    + "cached version of a schema will no longer be used, and the service will have to communicate with the "
                    + "Schema Registry again in order to obtain the schema.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 hour")
            .required(true)
            .build();
    public static final PropertyDescriptor WEB_CLIENT_PROVIDER = new PropertyDescriptor.Builder()
            .name("Web Client Service Provider")
            .displayName("Web Client Service Provider")
            .description("Controller service for HTTP client operations")
            .required(true)
            .identifiesControllerService(WebClientServiceProvider.class)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SCHEMA_REGISTRY_URL,
            SCHEMA_GROUP_ID,
            CACHE_SIZE,
            CACHE_EXPIRATION,
            WEB_CLIENT_PROVIDER
    );


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private volatile SchemaRegistryClient client;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final String schemaRegistryUrl = context.getProperty(SCHEMA_REGISTRY_URL).getValue();
        final String schemaGroupId = context.getProperty(SCHEMA_GROUP_ID).getValue();
        final int cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final long cacheExpiration = context.getProperty(CACHE_EXPIRATION).asTimePeriod(TimeUnit.NANOSECONDS);

        final WebClientServiceProvider webClientServiceProvider =
                context.getProperty(WEB_CLIENT_PROVIDER).asControllerService(WebClientServiceProvider.class);

        final SchemaRegistryApiClient apiClient = new SchemaRegistryApiClient(webClientServiceProvider, schemaRegistryUrl, schemaGroupId);
        final SchemaRegistryClient schemaRegistryClient = new ApicurioSchemaRegistryClient(apiClient);
        client = new CachingSchemaRegistryClient(schemaRegistryClient, cacheSize, cacheExpiration);
    }

    @Override
    public RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final String schemaId = schemaIdentifier.getName().orElseThrow(
                () -> new SchemaNotFoundException("Cannot retrieve schema because Schema Name is not present")
        );
        final OptionalInt version = schemaIdentifier.getVersion();

        return client.getSchema(schemaId, version);
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
