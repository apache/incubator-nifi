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
package org.apache.nifi.kafka.service;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kafka.service.api.KafkaConnectionService;
import org.apache.nifi.kafka.service.api.common.ServiceConfiguration;
import org.apache.nifi.kafka.service.api.consumer.AutoOffsetReset;
import org.apache.nifi.kafka.service.api.consumer.KafkaConsumerService;
import org.apache.nifi.kafka.service.api.consumer.PollingContext;
import org.apache.nifi.kafka.service.api.producer.KafkaProducerService;
import org.apache.nifi.kafka.service.api.producer.ProducerConfiguration;
import org.apache.nifi.kafka.service.consumer.Kafka3ConsumerService;
import org.apache.nifi.kafka.service.consumer.pool.Subscription;
import org.apache.nifi.kafka.service.producer.Kafka3ProducerService;
import org.apache.nifi.kafka.shared.property.IsolationLevel;
import org.apache.nifi.kafka.shared.property.SaslMechanism;
import org.apache.nifi.kafka.shared.property.provider.KafkaPropertyProvider;
import org.apache.nifi.kafka.shared.property.provider.StandardKafkaPropertyProvider;
import org.apache.nifi.kafka.shared.transaction.TransactionIdSupplier;
import org.apache.nifi.kerberos.SelfContainedKerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;
import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.KERBEROS_SERVICE_NAME;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_LOCATION;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEYSTORE_TYPE;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_KEY_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_LOCATION;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.nifi.kafka.shared.property.KafkaClientProperty.SSL_TRUSTSTORE_TYPE;

@DynamicProperty(name = "The name of a Kafka configuration property.", value = "The value of a given Kafka configuration property.",
        description = "These properties will be added on the Kafka configuration after loading any provided configuration properties."
                + " In the event a dynamic property represents a property that was already set, its value will be ignored and WARN message logged."
                + " For the list of available Kafka properties please refer to: http://kafka.apache.org/documentation.html#configuration.",
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT)
@CapabilityDescription("Provides and manages connections to Kafka Brokers for producer or consumer operations.")
public class Kafka3ConnectionService extends AbstractControllerService implements KafkaConnectionService, VerifiableControllerService {

    public static final PropertyDescriptor BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name("bootstrap.servers")
            .displayName("Bootstrap Servers")
            .description("Comma-separated list of Kafka Bootstrap Servers in the format host:port. Corresponds to Kafka bootstrap.servers property")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor SECURITY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("security.protocol")
            .displayName("Security Protocol")
            .description("Security protocol used to communicate with brokers. Corresponds to Kafka Client security.protocol property")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SecurityProtocol.values())
            .defaultValue(SecurityProtocol.PLAINTEXT.name())
            .build();

    public static final PropertyDescriptor SASL_MECHANISM = new PropertyDescriptor.Builder()
            .name("sasl.mechanism")
            .displayName("SASL Mechanism")
            .description("SASL mechanism used for authentication. Corresponds to Kafka Client sasl.mechanism property")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SaslMechanism.getAvailableSaslMechanisms())
            .defaultValue(SaslMechanism.GSSAPI.getValue())
            .build();

    public static final PropertyDescriptor SASL_USERNAME = new PropertyDescriptor.Builder()
            .name("sasl.username")
            .displayName("SASL Username")
            .description("Username provided with configured password when using PLAIN or SCRAM SASL Mechanisms")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(
                    SASL_MECHANISM,
                    SaslMechanism.PLAIN.getValue(),
                    SaslMechanism.SCRAM_SHA_256.getValue(),
                    SaslMechanism.SCRAM_SHA_512.getValue()
            )
            .build();

    public static final PropertyDescriptor SASL_PASSWORD = new PropertyDescriptor.Builder()
            .name("sasl.password")
            .displayName("SASL Password")
            .description("Password provided with configured username when using PLAIN or SCRAM SASL Mechanisms")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(
                    SASL_MECHANISM,
                    SaslMechanism.PLAIN.getValue(),
                    SaslMechanism.SCRAM_SHA_256.getValue(),
                    SaslMechanism.SCRAM_SHA_512.getValue()
            )
            .build();

    public static final PropertyDescriptor SELF_CONTAINED_KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Service supporting user authentication with Kerberos")
            .identifiesControllerService(SelfContainedKerberosUserService.class)
            .required(false)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("Service supporting SSL communication with Kafka brokers")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor TRANSACTION_ISOLATION_LEVEL = new PropertyDescriptor.Builder()
            .name("isolation.level")
            .displayName("Transaction Isolation Level")
            .description("""
                    Specifies how the service should handle transaction isolation levels when communicating with Kafka.
                    The uncommited option means that messages will be received as soon as they are written to Kafka but will be pulled, even if the producer cancels the transactions.
                    The committed option configures the service to not receive any messages for which the producer's transaction was canceled, but this can result in some latency since the
                    consumer must wait for the producer to finish its entire transaction instead of pulling as the messages become available.
                    Corresponds to Kafka isolation.level property.
                    """)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(IsolationLevel.class)
            .defaultValue(IsolationLevel.READ_COMMITTED)
            .required(true)
            .build();

    public static final PropertyDescriptor MAX_POLL_RECORDS = new PropertyDescriptor.Builder()
            .name("max.poll.records")
            .displayName("Max Poll Records")
            .description("Maximum number of records Kafka should return in a single poll.")
            .required(true)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("default.api.timeout.ms")
            .displayName("Client Timeout")
            .description("Default timeout for Kafka client operations. Mapped to Kafka default.api.timeout.ms. The Kafka request.timeout.ms property is derived from half of the configured timeout")
            .defaultValue("60 sec")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor METADATA_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("max.block.ms")
            .displayName("Max Metadata Wait Time")
            .description("""
                    The amount of time publisher will wait to obtain metadata or wait for the buffer to flush during the 'send' call before failing the
                    entire 'send' call. Corresponds to Kafka max.block.ms property
                    """)
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("5 sec")
            .build();

    public static final PropertyDescriptor ACK_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("ack.wait.time")
            .displayName("Acknowledgment Wait Time")
            .description("""
                    After sending a message to Kafka, this indicates the amount of time that the service will wait for a response from Kafka.
                    If Kafka does not acknowledge the message within this time period, the service will throw an exception.
                    """)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue("5 sec")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOOTSTRAP_SERVERS,
            SECURITY_PROTOCOL,
            SASL_MECHANISM,
            SASL_USERNAME,
            SASL_PASSWORD,
            SELF_CONTAINED_KERBEROS_USER_SERVICE,
            KERBEROS_SERVICE_NAME,
            SSL_CONTEXT_SERVICE,
            TRANSACTION_ISOLATION_LEVEL,
            MAX_POLL_RECORDS,
            CLIENT_TIMEOUT,
            METADATA_WAIT_TIME,
            ACK_WAIT_TIME
    );

    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(2);
    private static final String CONNECTION_STEP = "Kafka Broker Connection";
    private static final String TOPIC_LISTING_STEP = "Kafka Topic Listing";

    private volatile Properties clientProperties;
    private volatile ServiceConfiguration serviceConfiguration;
    private volatile Properties consumerProperties;

    @OnEnabled
    public void onEnabled(final ConfigurationContext configurationContext) {
        clientProperties = getClientProperties(configurationContext);
        serviceConfiguration = getServiceConfiguration(configurationContext);
        consumerProperties = getConsumerProperties(configurationContext, clientProperties);
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public KafkaConsumerService getConsumerService(final PollingContext pollingContext) {
        Objects.requireNonNull(pollingContext, "Polling Context required");

        final Subscription subscription = createSubscription(pollingContext);

        final Properties properties = new Properties();
        properties.putAll(consumerProperties);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, subscription.getGroupId());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, subscription.getAutoOffsetReset().getValue());

        final ByteArrayDeserializer deserializer = new ByteArrayDeserializer();
        final Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties, deserializer, deserializer);

        final Optional<Pattern> topicPatternFound = subscription.getTopicPattern();
        if (topicPatternFound.isPresent()) {
            final Pattern topicPattern = topicPatternFound.get();
            consumer.subscribe(topicPattern);
        } else {
            final Collection<String> topics = subscription.getTopics();
            consumer.subscribe(topics);
        }

        return new Kafka3ConsumerService(getLogger(), consumer, subscription, pollingContext.getMaxUncommittedTime());
    }

    private Subscription createSubscription(final PollingContext pollingContext) {
        final String groupId = pollingContext.getGroupId();
        final Optional<Pattern> topicPatternFound = pollingContext.getTopicPattern();
        final AutoOffsetReset autoOffsetReset = pollingContext.getAutoOffsetReset();

        return topicPatternFound
            .map(pattern -> new Subscription(groupId, pattern, autoOffsetReset))
            .orElseGet(() -> new Subscription(groupId, pollingContext.getTopics(), autoOffsetReset));
    }

    @Override
    public KafkaProducerService getProducerService(final ProducerConfiguration producerConfiguration) {
        final Properties propertiesProducer = new Properties();
        propertiesProducer.putAll(clientProperties);
        if (producerConfiguration.getTransactionsEnabled()) {
            propertiesProducer.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    new TransactionIdSupplier(producerConfiguration.getTransactionIdPrefix()).get());
        }
        if (producerConfiguration.getDeliveryGuarantee() != null) {
            propertiesProducer.put(ProducerConfig.ACKS_CONFIG, producerConfiguration.getDeliveryGuarantee());
        }
        if (producerConfiguration.getCompressionCodec() != null) {
            propertiesProducer.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerConfiguration.getCompressionCodec());
        }
        final String partitionClass = producerConfiguration.getPartitionClass();
        if (partitionClass != null && partitionClass.startsWith("org.apache.kafka")) {
            propertiesProducer.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionClass);
        }

        return new Kafka3ProducerService(propertiesProducer, serviceConfiguration, producerConfiguration);
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext configurationContext, final ComponentLog verificationLogger, final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        final Properties clientProperties = getClientProperties(configurationContext);
        clientProperties.putAll(variables);
        try (final Admin admin = Admin.create(clientProperties)) {
            final ListTopicsResult listTopicsResult = admin.listTopics();

            final KafkaFuture<Collection<TopicListing>> requestedListings = listTopicsResult.listings();
            final Collection<TopicListing> topicListings = requestedListings.get(VERIFY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            final String topicListingExplanation = String.format("Topics Found [%d]", topicListings.size());
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(TOPIC_LISTING_STEP)
                            .outcome(SUCCESSFUL)
                            .explanation(topicListingExplanation)
                            .build()
            );
        } catch (final Exception e) {
            verificationLogger.error("Kafka Broker verification failed", e);
            results.add(
                    new ConfigVerificationResult.Builder()
                            .verificationStepName(CONNECTION_STEP)
                            .outcome(FAILED)
                            .explanation(e.getMessage())
                            .build()
            );
        }

        return results;
    }

    private Properties getConsumerProperties(final PropertyContext propertyContext, final Properties defaultProperties) {
        final Properties properties = new Properties();
        properties.putAll(defaultProperties);

        final IsolationLevel isolationLevel = propertyContext.getProperty(TRANSACTION_ISOLATION_LEVEL).asAllowableValue(IsolationLevel.class);
        properties.put(TRANSACTION_ISOLATION_LEVEL.getName(), isolationLevel.getValue());

        return properties;
    }

    private Properties getClientProperties(final PropertyContext propertyContext) {
        final Properties properties = new Properties();

        final KafkaPropertyProvider propertyProvider = new StandardKafkaPropertyProvider(ConsumerConfig.class);
        final Map<String, Object> propertiesProvider = propertyProvider.getProperties(propertyContext);
        propertiesProvider.forEach((key, value) -> properties.setProperty(key, value.toString()));

        final String configuredBootstrapServers = propertyContext.getProperty(BOOTSTRAP_SERVERS).getValue();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configuredBootstrapServers);

        setSslProperties(properties, propertyContext);

        final int defaultApiTimeoutMs = getDefaultApiTimeoutMs(propertyContext);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutMs);

        final int requestTimeoutMs = getRequestTimeoutMs(propertyContext);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);

        final long timePeriod = propertyContext.getProperty(METADATA_WAIT_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, timePeriod);

        return properties;
    }

    private void setSslProperties(final Properties properties, final PropertyContext context) {
        final PropertyValue sslContextServiceProperty = context.getProperty(SSL_CONTEXT_SERVICE);
        if (sslContextServiceProperty.isSet()) {
            final SSLContextService sslContextService = sslContextServiceProperty.asControllerService(SSLContextService.class);
            if (sslContextService.isKeyStoreConfigured()) {
                properties.put(SSL_KEYSTORE_LOCATION.getProperty(), sslContextService.getKeyStoreFile());
                properties.put(SSL_KEYSTORE_TYPE.getProperty(), sslContextService.getKeyStoreType());

                final String keyStorePassword = sslContextService.getKeyStorePassword();
                properties.put(SSL_KEYSTORE_PASSWORD.getProperty(), keyStorePassword);

                final String keyPassword = sslContextService.getKeyPassword();
                final String configuredKeyPassword = keyPassword == null ? keyStorePassword : keyPassword;
                properties.put(SSL_KEY_PASSWORD.getProperty(), configuredKeyPassword);
            }
            if (sslContextService.isTrustStoreConfigured()) {
                properties.put(SSL_TRUSTSTORE_LOCATION.getProperty(), sslContextService.getTrustStoreFile());
                properties.put(SSL_TRUSTSTORE_TYPE.getProperty(), sslContextService.getTrustStoreType());
                properties.put(SSL_TRUSTSTORE_PASSWORD.getProperty(), sslContextService.getTrustStorePassword());
            }
        }
    }

    private ServiceConfiguration getServiceConfiguration(final PropertyContext propertyContext) {
        final Duration maxAckWait = propertyContext.getProperty(ACK_WAIT_TIME).asDuration();
        return new ServiceConfiguration(maxAckWait);
    }

    private int getDefaultApiTimeoutMs(final PropertyContext propertyContext) {
        return propertyContext.getProperty(CLIENT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
    }

    private int getRequestTimeoutMs(final PropertyContext propertyContext) {
        final int defaultApiTimeoutMs = getDefaultApiTimeoutMs(propertyContext);
        return defaultApiTimeoutMs / 2;
    }
}
