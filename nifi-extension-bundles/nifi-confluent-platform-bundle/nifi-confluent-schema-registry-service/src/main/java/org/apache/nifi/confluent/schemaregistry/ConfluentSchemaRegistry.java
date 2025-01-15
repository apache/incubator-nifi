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

package org.apache.nifi.confluent.schemaregistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.confluent.schemaregistry.client.AuthenticationType;
import org.apache.nifi.confluent.schemaregistry.client.CachingSchemaRegistryClient;
import org.apache.nifi.confluent.schemaregistry.client.RestSchemaRegistryClient;
import org.apache.nifi.confluent.schemaregistry.client.SchemaRegistryClient;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.ssl.SSLContextProvider;

@Tags({"schema", "registry", "confluent", "avro", "kafka"})
@CapabilityDescription("Provides a Schema Registry that interacts with the Confluent Schema Registry so that those Schemas that are stored in the Confluent Schema "
    + "Registry can be used in NiFi. The Confluent Schema Registry has a notion of a \"subject\" for schemas, which is their terminology for a schema name. When a Schema "
    + "is looked up by name by this registry, it will find a Schema in the Confluent Schema Registry with that subject.")
@DynamicProperty(name = "request.header.*", value = "String literal, may not be empty", description = "Properties that begin with 'request.header.' " +
        "are populated into a map and passed as http headers in REST requests to the Confluent Schema Registry")
public class ConfluentSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT,
        SchemaField.SCHEMA_TEXT_FORMAT, SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);

    private static final String REQUEST_HEADER_PREFIX = "request.header.";


    static final PropertyDescriptor SCHEMA_REGISTRY_URLS = new PropertyDescriptor.Builder()
        .name("url")
        .displayName("Schema Registry URLs")
        .description("A comma-separated list of URLs of the Schema Registry to interact with")
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .defaultValue("http://localhost:8081")
        .required(true)
        .addValidator(new MultipleURLValidator())
        .build();

    static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
        .name("ssl-context")
        .displayName("SSL Context Service")
        .description("Specifies the SSL Context Service to use for interacting with the Confluent Schema Registry")
        .identifiesControllerService(SSLContextProvider.class)
        .required(false)
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

    static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
        .name("timeout")
        .displayName("Communications Timeout")
        .description("Specifies how long to wait to receive data from the Schema Registry before considering the communications a failure")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .defaultValue("30 secs")
        .required(true)
        .build();

    static final PropertyDescriptor AUTHENTICATION_TYPE = new PropertyDescriptor.Builder()
            .name("authentication-type")
            .displayName("Authentication Type")
            .description("HTTP Client Authentication Type for Confluent Schema Registry")
            .required(false)
            .allowableValues(AuthenticationType.values())
            .defaultValue(AuthenticationType.NONE.toString())
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .displayName("Username")
            .description("Username for authentication to Confluent Schema Registry")
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x39\\x3b-\\x7e\\x80-\\xff]+$")))
            .required(false)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString())
            .build();

    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("Password for authentication to Confluent Schema Registry")
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x7e\\x80-\\xff]+$")))
            .required(false)
            .dependsOn(AUTHENTICATION_TYPE, AuthenticationType.BASIC.toString())
            .sensitive(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        SCHEMA_REGISTRY_URLS,
        SSL_CONTEXT,
        TIMEOUT,
        CACHE_SIZE,
        CACHE_EXPIRATION,
        AUTHENTICATION_TYPE,
        USERNAME,
        PASSWORD
    );

    private volatile SchemaRegistryClient client;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private static final Validator REQUEST_HEADER_VALIDATOR = (subject, value, context) -> new ValidationResult.Builder()
            .subject(subject)
            .input(value)
            .valid(subject.startsWith(REQUEST_HEADER_PREFIX)
                    && subject.length() > REQUEST_HEADER_PREFIX.length())
            .explanation("Dynamic property names must be of format 'request.header.*'")
            .build();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptionName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptionName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .addValidator(REQUEST_HEADER_VALIDATOR)
                .build();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final List<String> baseUrls = getBaseURLs(context);
        final int timeoutMillis = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        final SSLContext sslContext;
        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextProvider.class);
        if (sslContextProvider == null) {
            sslContext = null;
        } else {
            sslContext = sslContextProvider.createContext();
        }

        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();

        // generate a map of http headers where the key is the remainder of the property name after
        // the request header prefix
        final Map<String, String> httpHeaders =
                context.getProperties().entrySet()
                        .stream()
                        .filter(e -> e.getKey().getName().startsWith(REQUEST_HEADER_PREFIX))
                        .collect(Collectors.toMap(
                                map -> map.getKey().getName().substring(REQUEST_HEADER_PREFIX.length()),
                                Map.Entry::getValue)
                        );

        final SchemaRegistryClient restClient = new RestSchemaRegistryClient(baseUrls, timeoutMillis,
                sslContext, username, password, getLogger(), httpHeaders);

        final int cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final long cacheExpiration = context.getProperty(CACHE_EXPIRATION).asTimePeriod(TimeUnit.NANOSECONDS).longValue();

        client = new CachingSchemaRegistryClient(restClient, cacheSize, cacheExpiration);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final boolean sslContextSet = validationContext.getProperty(SSL_CONTEXT).isSet();
        if (sslContextSet) {
            final List<String> baseUrls = getBaseURLs(validationContext);
            final List<String> insecure = baseUrls.stream()
                .filter(url -> !url.startsWith("https"))
                .collect(Collectors.toList());

            if (!insecure.isEmpty()) {
                results.add(new ValidationResult.Builder()
                    .subject(SCHEMA_REGISTRY_URLS.getDisplayName())
                    .input(insecure.get(0))
                    .valid(false)
                    .explanation("When SSL Context is configured, all Schema Registry URL's must use HTTPS, not HTTP")
                    .build());
            }
        }

        final PropertyValue authenticationTypeProperty = validationContext.getProperty(AUTHENTICATION_TYPE);
        if (authenticationTypeProperty.isSet()) {
            final AuthenticationType authenticationType = AuthenticationType.valueOf(authenticationTypeProperty.getValue());
            if (AuthenticationType.BASIC.equals(authenticationType)) {
                final String username = validationContext.getProperty(USERNAME).getValue();
                if (StringUtils.isBlank(username)) {
                    results.add(new ValidationResult.Builder()
                            .subject(USERNAME.getDisplayName())
                            .valid(false)
                            .explanation("Username is required for Basic Authentication")
                            .build());
                }
                final String password = validationContext.getProperty(PASSWORD).getValue();
                if (StringUtils.isBlank(password)) {
                    results.add(new ValidationResult.Builder()
                            .subject(PASSWORD.getDisplayName())
                            .valid(false)
                            .explanation("Password is required for Basic Authentication")
                            .build());
                }
            }
        }

        return results;
    }

    private List<String> getBaseURLs(final PropertyContext context) {
        final String urls = context.getProperty(SCHEMA_REGISTRY_URLS).evaluateAttributeExpressions().getValue();
        final List<String> baseUrls = Stream.of(urls.split(","))
            .map(url -> url.trim())
            .collect(Collectors.toList());

        return baseUrls;
    }

    private RecordSchema retrieveSchemaByName(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final Optional<String> schemaName = schemaIdentifier.getName();
        if (!schemaName.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Name is not present");
        }

        final RecordSchema schema;
        if (schemaIdentifier.getVersion().isPresent()) {
            schema = client.getSchema(schemaName.get(), schemaIdentifier.getVersion().getAsInt());
        } else {
            schema = client.getSchema(schemaName.get());
        }

        return schema;
    }

    private RecordSchema retrieveSchemaById(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final OptionalLong schemaId = schemaIdentifier.getSchemaVersionId();
        if (!schemaId.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Id is not present");
        }

        final RecordSchema schema = client.getSchema((int) schemaId.getAsLong());
        return schema;
    }

    @Override
    public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        if (schemaIdentifier.getName().isPresent()) {
            return retrieveSchemaByName(schemaIdentifier);
        } else {
            return retrieveSchemaById(schemaIdentifier);
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
