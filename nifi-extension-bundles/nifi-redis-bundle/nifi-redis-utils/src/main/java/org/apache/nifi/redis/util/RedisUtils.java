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
package org.apache.nifi.redis.util;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.RedisType;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.lang.Nullable;
import redis.clients.jedis.JedisPoolConfig;

import javax.net.ssl.SSLContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RedisUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisUtils.class);

    // These properties are shared among the controller service(s) and processor(s) that use a RedisConnectionPool

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .identifiesControllerService(RedisConnectionPool.class)
            .required(true)
            .build();

    public static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("redis-cache-ttl")
            .displayName("TTL")
            .description("Indicates how long the data should exist in Redis. Setting '0 secs' would mean the data would exist forever")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .defaultValue("0 secs")
            .build();


    // These properties are shared between the connection pool controller service and the state provider, the name
    // is purposely set to be more human-readable since that will be referenced in state-management.xml

    public static final AllowableValue REDIS_MODE_STANDALONE = new AllowableValue(RedisType.STANDALONE.getDisplayName(), RedisType.STANDALONE.getDisplayName(), RedisType.STANDALONE.getDescription());
    public static final AllowableValue REDIS_MODE_SENTINEL = new AllowableValue(RedisType.SENTINEL.getDisplayName(), RedisType.SENTINEL.getDisplayName(), RedisType.SENTINEL.getDescription());
    public static final AllowableValue REDIS_MODE_CLUSTER = new AllowableValue(RedisType.CLUSTER.getDisplayName(), RedisType.CLUSTER.getDisplayName(), RedisType.CLUSTER.getDescription());

    public static final PropertyDescriptor REDIS_MODE = new PropertyDescriptor.Builder()
            .name("Redis Mode")
            .displayName("Redis Mode")
            .description("The type of Redis being communicated with - standalone, sentinel, or clustered.")
            .allowableValues(REDIS_MODE_STANDALONE, REDIS_MODE_SENTINEL, REDIS_MODE_CLUSTER)
            .defaultValue(REDIS_MODE_STANDALONE.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("Connection String")
            .displayName("Connection String")
            .description("The connection string for Redis. In a standalone instance this value will be of the form hostname:port. " +
                    "In a sentinel instance this value will be the comma-separated list of sentinels, such as host1:port1,host2:port2,host3:port3. " +
                    "In a clustered instance this value will be the comma-separated list of cluster masters, such as host1:port,host2:port,host3:port.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("Database Index")
            .displayName("Database Index")
            .description("The database index to be used by connections created from this connection pool. " +
                    "See the databases property in redis.conf, by default databases 0-15 will be available.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    public static final PropertyDescriptor COMMUNICATION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communication Timeout")
            .displayName("Communication Timeout")
            .description("The timeout to use when attempting to communicate with Redis.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor CLUSTER_MAX_REDIRECTS = new PropertyDescriptor.Builder()
            .name("Cluster Max Redirects")
            .displayName("Cluster Max Redirects")
            .description("The maximum number of redirects that can be performed when clustered.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .required(true)
            .build();

    public static final PropertyDescriptor SENTINEL_MASTER = new PropertyDescriptor.Builder()
            .name("Sentinel Master")
            .displayName("Sentinel Master")
            .description("The name of the sentinel master, require when Mode is set to Sentinel")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username used to authenticate to the Redis server.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .displayName("Password")
            .description("The password used to authenticate to the Redis server. See the 'requirepass' property in redis.conf.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor SENTINEL_USERNAME = new PropertyDescriptor.Builder()
            .name("Sentinel Username")
            .description("The username used to authenticate to the Redis sentinel server.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SENTINEL_PASSWORD = new PropertyDescriptor.Builder()
            .name("Sentinel Password")
            .displayName("Sentinel Password")
            .description("The password used to authenticate to the Redis Sentinel server. See the 'requirepass' and 'sentinel sentinel-pass' properties in sentinel.conf.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor POOL_MAX_TOTAL = new PropertyDescriptor.Builder()
            .name("Pool - Max Total")
            .displayName("Pool - Max Total")
            .description("The maximum number of connections that can be allocated by the pool (checked out to clients, or idle awaiting checkout). " +
                    "A negative value indicates that there is no limit.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("8")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MAX_IDLE = new PropertyDescriptor.Builder()
            .name("Pool - Max Idle")
            .displayName("Pool - Max Idle")
            .description("The maximum number of idle connections that can be held in the pool, or a negative value if there is no limit.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("8")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MIN_IDLE = new PropertyDescriptor.Builder()
            .name("Pool - Min Idle")
            .displayName("Pool - Min Idle")
            .description("The target for the minimum number of idle connections to maintain in the pool. If the configured value of Min Idle is " +
                    "greater than the configured value for Max Idle, then the value of Max Idle will be used instead.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("0")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_BLOCK_WHEN_EXHAUSTED = new PropertyDescriptor.Builder()
            .name("Pool - Block When Exhausted")
            .displayName("Pool - Block When Exhausted")
            .description("Whether or not clients should block and wait when trying to obtain a connection from the pool when the pool has no available connections. " +
                    "Setting this to false means an error will occur immediately when a client requests a connection and none are available.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("Pool - Max Wait Time")
            .displayName("Pool - Max Wait Time")
            .description("The amount of time to wait for an available connection when Block When Exhausted is set to true.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .name("Pool - Min Evictable Idle Time")
            .displayName("Pool - Min Evictable Idle Time")
            .description("The minimum amount of time an object may sit idle in the pool before it is eligible for eviction.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("60 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TIME_BETWEEN_EVICTION_RUNS = new PropertyDescriptor.Builder()
            .name("Pool - Time Between Eviction Runs")
            .displayName("Pool - Time Between Eviction Runs")
            .description("The amount of time between attempting to evict idle connections from the pool.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 seconds")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_NUM_TESTS_PER_EVICTION_RUN = new PropertyDescriptor.Builder()
            .name("Pool - Num Tests Per Eviction Run")
            .displayName("Pool - Num Tests Per Eviction Run")
            .description("The number of connections to tests per eviction attempt. A negative value indicates to test all connections.")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("-1")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_ON_CREATE = new PropertyDescriptor.Builder()
            .name("Pool - Test On Create")
            .displayName("Pool - Test On Create")
            .description("Whether or not connections should be tested upon creation.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_ON_BORROW = new PropertyDescriptor.Builder()
            .name("Pool - Test On Borrow")
            .displayName("Pool - Test On Borrow")
            .description("Whether or not connections should be tested upon borrowing from the pool.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_ON_RETURN = new PropertyDescriptor.Builder()
            .name("Pool - Test On Return")
            .displayName("Pool - Test On Return")
            .description("Whether or not connections should be tested upon returning to the pool.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor POOL_TEST_WHILE_IDLE = new PropertyDescriptor.Builder()
            .name("Pool - Test While Idle")
            .displayName("Pool - Test While Idle")
            .description("Whether or not connections should be tested while idle.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .displayName("SSL Context Service")
            .description("If specified, this service will be used to create an SSL Context that will be used "
                    + "to secure communications; if not specified, communications will not be secure")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    public static final List<PropertyDescriptor> REDIS_CONNECTION_PROPERTY_DESCRIPTORS = List.of(
        RedisUtils.REDIS_MODE,
        RedisUtils.CONNECTION_STRING,
        RedisUtils.DATABASE,
        RedisUtils.COMMUNICATION_TIMEOUT,
        RedisUtils.CLUSTER_MAX_REDIRECTS,
        RedisUtils.SENTINEL_MASTER,
        RedisUtils.USERNAME,
        RedisUtils.PASSWORD,
        RedisUtils.SENTINEL_USERNAME,
        RedisUtils.SENTINEL_PASSWORD,
        RedisUtils.SSL_CONTEXT_SERVICE,
        RedisUtils.POOL_MAX_TOTAL,
        RedisUtils.POOL_MAX_IDLE,
        RedisUtils.POOL_MIN_IDLE,
        RedisUtils.POOL_BLOCK_WHEN_EXHAUSTED,
        RedisUtils.POOL_MAX_WAIT_TIME,
        RedisUtils.POOL_MIN_EVICTABLE_IDLE_TIME,
        RedisUtils.POOL_TIME_BETWEEN_EVICTION_RUNS,
        RedisUtils.POOL_NUM_TESTS_PER_EVICTION_RUN,
        RedisUtils.POOL_TEST_ON_CREATE,
        RedisUtils.POOL_TEST_ON_BORROW,
        RedisUtils.POOL_TEST_ON_RETURN,
        RedisUtils.POOL_TEST_WHILE_IDLE
    );

    public static RedisConfig createRedisConfig(final PropertyContext context) {
        final RedisType redisType = RedisType.fromDisplayName(context.getProperty(RedisUtils.REDIS_MODE).getValue());
        final String connectString =   context.getProperty(RedisUtils.CONNECTION_STRING).evaluateAttributeExpressions().getValue();

        final RedisConfig redisConfig = new RedisConfig(redisType, connectString);
        redisConfig.setSentinelMaster(context.getProperty(RedisUtils.SENTINEL_MASTER).evaluateAttributeExpressions().getValue());
        redisConfig.setDbIndex(context.getProperty(RedisUtils.DATABASE).evaluateAttributeExpressions().asInteger());
        redisConfig.setUsername(context.getProperty(RedisUtils.USERNAME).evaluateAttributeExpressions().getValue());
        redisConfig.setPassword(context.getProperty(RedisUtils.PASSWORD).evaluateAttributeExpressions().getValue());
        redisConfig.setSentinelUsername(context.getProperty(RedisUtils.SENTINEL_USERNAME).evaluateAttributeExpressions().getValue());
        redisConfig.setSentinelPassword(context.getProperty(RedisUtils.SENTINEL_PASSWORD).evaluateAttributeExpressions().getValue());
        redisConfig.setTimeout(context.getProperty(RedisUtils.COMMUNICATION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
        redisConfig.setClusterMaxRedirects(context.getProperty(RedisUtils.CLUSTER_MAX_REDIRECTS).asInteger());
        redisConfig.setPoolMaxTotal(context.getProperty(RedisUtils.POOL_MAX_TOTAL).asInteger());
        redisConfig.setPoolMaxIdle(context.getProperty(RedisUtils.POOL_MAX_IDLE).asInteger());
        redisConfig.setPoolMinIdle(context.getProperty(RedisUtils.POOL_MIN_IDLE).asInteger());
        redisConfig.setBlockWhenExhausted(context.getProperty(RedisUtils.POOL_BLOCK_WHEN_EXHAUSTED).asBoolean());
        redisConfig.setMaxWaitTime(Duration.ofMillis(context.getProperty(RedisUtils.POOL_MAX_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS)));
        redisConfig.setMinEvictableIdleTime(Duration.ofMillis(context.getProperty(RedisUtils.POOL_MIN_EVICTABLE_IDLE_TIME).asTimePeriod(TimeUnit.MILLISECONDS)));
        redisConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(context.getProperty(RedisUtils.POOL_TIME_BETWEEN_EVICTION_RUNS).asTimePeriod(TimeUnit.MILLISECONDS)));
        redisConfig.setNumTestsPerEvictionRun(context.getProperty(RedisUtils.POOL_NUM_TESTS_PER_EVICTION_RUN).asInteger());
        redisConfig.setTestOnCreate(context.getProperty(RedisUtils.POOL_TEST_ON_CREATE).asBoolean());
        redisConfig.setTestOnBorrow(context.getProperty(RedisUtils.POOL_TEST_ON_BORROW).asBoolean());
        redisConfig.setTestOnReturn(context.getProperty(RedisUtils.POOL_TEST_ON_RETURN).asBoolean());
        redisConfig.setTestWhenIdle(context.getProperty(RedisUtils.POOL_TEST_WHILE_IDLE).asBoolean());
        return redisConfig;
    }

    public static JedisConnectionFactory createConnectionFactory(final PropertyContext context, final SSLContext sslContext) {
        return createConnectionFactory(createRedisConfig(context), sslContext);
    }

    public static JedisConnectionFactory createConnectionFactory(final RedisConfig redisConfig, final SSLContext sslContext) {
        final RedisType redisMode = redisConfig.getRedisMode();
        final String connectionString = redisConfig.getConnectionString();
        final Integer dbIndex = redisConfig.getDbIndex();
        final String username = redisConfig.getUsername();
        final String password = redisConfig.getPassword();
        final String sentinelUsername = redisConfig.getSentinelUsername();
        final String sentinelPassword = redisConfig.getSentinelPassword();
        final Integer timeout = redisConfig.getTimeout();
        final JedisPoolConfig poolConfig = createJedisPoolConfig(redisConfig);

        JedisClientConfiguration.JedisClientConfigurationBuilder builder = JedisClientConfiguration.builder()
                .connectTimeout(Duration.ofMillis(timeout))
                .readTimeout(Duration.ofMillis(timeout))
                .usePooling()
                .poolConfig(poolConfig)
                .and();

        if (sslContext != null) {
            builder = builder.useSsl()
                    .sslParameters(sslContext.getSupportedSSLParameters())
                    .sslSocketFactory(sslContext.getSocketFactory())
                    .and();
        }

        final JedisClientConfiguration jedisClientConfiguration = builder.build();
        JedisConnectionFactory connectionFactory;

        if (RedisType.STANDALONE == redisMode) {
            LOGGER.info("Connecting to Redis in standalone mode at {}", connectionString);
            final String[] hostAndPortSplit = connectionString.split("[:]");
            final String host = hostAndPortSplit[0].trim();
            final Integer port = Integer.parseInt(hostAndPortSplit[1].trim());
            final RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration(host, port);
            enrichRedisConfiguration(redisStandaloneConfiguration, dbIndex, username, password, sentinelUsername, sentinelPassword);

            connectionFactory = new JedisConnectionFactory(redisStandaloneConfiguration, jedisClientConfiguration);

        } else if (RedisType.SENTINEL == redisMode) {
            final String[] sentinels = connectionString.split("[,]");
            final String sentinelMaster = redisConfig.getSentinelMaster();
            final RedisSentinelConfiguration sentinelConfiguration = new RedisSentinelConfiguration(sentinelMaster, new HashSet<>(getTrimmedValues(sentinels)));
            enrichRedisConfiguration(sentinelConfiguration, dbIndex, username, password, sentinelUsername, sentinelPassword);

            LOGGER.info("Connecting to Redis in sentinel mode...");
            LOGGER.info("Redis master = {}", sentinelMaster);

            for (final String sentinel : sentinels) {
                LOGGER.info("Redis sentinel at {}", sentinel);
            }

            connectionFactory = new JedisConnectionFactory(sentinelConfiguration, jedisClientConfiguration);

        } else {
            final String[] clusterNodes = connectionString.split("[,]");
            final Integer maxRedirects = redisConfig.getClusterMaxRedirects();

            final RedisClusterConfiguration clusterConfiguration = new RedisClusterConfiguration(getTrimmedValues(clusterNodes));
            enrichRedisConfiguration(clusterConfiguration, dbIndex, username, password, sentinelUsername, sentinelPassword);
            clusterConfiguration.setMaxRedirects(maxRedirects);

            LOGGER.info("Connecting to Redis in clustered mode...");
            for (final String clusterNode : clusterNodes) {
                LOGGER.info("Redis cluster node at {}", clusterNode);
            }

            connectionFactory = new JedisConnectionFactory(clusterConfiguration, jedisClientConfiguration);
        }

        // need to call this to initialize the pool/connections
        connectionFactory.afterPropertiesSet();
        return connectionFactory;
    }

    private static List<String> getTrimmedValues(final String[] values) {
        final List<String> trimmedValues = new ArrayList<>();
        for (final String value : values) {
            trimmedValues.add(value.trim());
        }
        return trimmedValues;
    }

    private static void enrichRedisConfiguration(final RedisConfiguration redisConfiguration,
                                                 final Integer dbIndex,
                                                 @Nullable final String username,
                                                 @Nullable final String password,
                                                 @Nullable final String sentinelUsername,
                                                 @Nullable final String sentinelPassword) {
        if (redisConfiguration instanceof RedisConfiguration.WithDatabaseIndex) {
            ((RedisConfiguration.WithDatabaseIndex) redisConfiguration).setDatabase(dbIndex);
        }
        if (redisConfiguration instanceof RedisConfiguration.WithPassword) {
            ((RedisConfiguration.WithPassword) redisConfiguration).setUsername(username);
            ((RedisConfiguration.WithPassword) redisConfiguration).setPassword(RedisPassword.of(password));
        }
        if (redisConfiguration instanceof RedisConfiguration.SentinelConfiguration) {
            ((RedisConfiguration.SentinelConfiguration) redisConfiguration).setSentinelUsername(sentinelUsername);
            ((RedisConfiguration.SentinelConfiguration) redisConfiguration).setSentinelPassword(RedisPassword.of(sentinelPassword));
        }
    }

    private static JedisPoolConfig createJedisPoolConfig(final RedisConfig redisConfig) {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(redisConfig.getPoolMaxTotal());
        poolConfig.setMaxIdle(redisConfig.getPoolMaxIdle());
        poolConfig.setMinIdle(redisConfig.getPoolMinIdle());
        poolConfig.setBlockWhenExhausted(redisConfig.getBlockWhenExhausted());
        poolConfig.setMaxWait(redisConfig.getMaxWaitTime());
        poolConfig.setMinEvictableIdleTime(redisConfig.getMinEvictableIdleTime());
        poolConfig.setTimeBetweenEvictionRuns(redisConfig.getTimeBetweenEvictionRuns());
        poolConfig.setNumTestsPerEvictionRun(redisConfig.getNumTestsPerEvictionRun());
        poolConfig.setTestOnCreate(redisConfig.getTestOnCreate());
        poolConfig.setTestOnBorrow(redisConfig.getTestOnBorrow());
        poolConfig.setTestOnReturn(redisConfig.getTestOnReturn());
        poolConfig.setTestWhileIdle(redisConfig.getTestWhenIdle());
        return poolConfig;
    }

    public static List<ValidationResult> validate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final String redisMode = validationContext.getProperty(RedisUtils.REDIS_MODE).getValue();
        final String connectionString = validationContext.getProperty(RedisUtils.CONNECTION_STRING).evaluateAttributeExpressions().getValue();
        final Integer dbIndex = validationContext.getProperty(RedisUtils.DATABASE).evaluateAttributeExpressions().asInteger();

        if (StringUtils.isBlank(connectionString)) {
            results.add(new ValidationResult.Builder()
                    .subject(RedisUtils.CONNECTION_STRING.getDisplayName())
                    .valid(false)
                    .explanation("Connection String cannot be blank")
                    .build());
        } else if (RedisUtils.REDIS_MODE_STANDALONE.getValue().equals(redisMode)) {
            final String[] hostAndPort = connectionString.split("[:]");
            if (hostAndPort == null || hostAndPort.length != 2 || StringUtils.isBlank(hostAndPort[0]) || StringUtils.isBlank(hostAndPort[1]) || !isInteger(hostAndPort[1])) {
                results.add(new ValidationResult.Builder()
                        .subject(RedisUtils.CONNECTION_STRING.getDisplayName())
                        .input(connectionString)
                        .valid(false)
                        .explanation("Standalone Connection String must be in the form host:port")
                        .build());
            }
        } else {
            for (final String connection : connectionString.split("[,]")) {
                final String[] hostAndPort = connection.split("[:]");
                if (hostAndPort == null || hostAndPort.length != 2 || StringUtils.isBlank(hostAndPort[0]) || StringUtils.isBlank(hostAndPort[1]) || !isInteger(hostAndPort[1])) {
                    results.add(new ValidationResult.Builder()
                            .subject(RedisUtils.CONNECTION_STRING.getDisplayName())
                            .input(connection)
                            .valid(false)
                            .explanation("Connection String must be in the form host:port,host:port,host:port,etc.")
                            .build());
                }
            }
        }

        if (RedisUtils.REDIS_MODE_CLUSTER.getValue().equals(redisMode) && dbIndex > 0) {
            results.add(new ValidationResult.Builder()
                    .subject(RedisUtils.DATABASE.getDisplayName())
                    .valid(false)
                    .explanation("Database Index must be 0 when using clustered Redis")
                    .build());
        }

        if (RedisUtils.REDIS_MODE_SENTINEL.getValue().equals(redisMode)) {
            final String sentinelMaster = validationContext.getProperty(RedisUtils.SENTINEL_MASTER).evaluateAttributeExpressions().getValue();
            if (StringUtils.isEmpty(sentinelMaster)) {
                results.add(new ValidationResult.Builder()
                        .subject(RedisUtils.SENTINEL_MASTER.getDisplayName())
                        .valid(false)
                        .explanation("Sentinel Master must be provided when Mode is Sentinel")
                        .build());
            }
        }

        return results;
    }

    private static boolean isInteger(final String number) {
        try {
            Integer.parseInt(number);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static RedisTemplate<String, String> createRedisTemplateForStreams(final RedisConnectionFactory connectionFactory) {
        final StreamMessageListenerContainer.StreamMessageListenerContainerOptionsBuilder<String, MapRecord<String, String, byte[]>> builder =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                        .hashValueSerializer(RedisSerializer.byteArray());

        final StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, byte[]>> containerOptions = builder.build();
        return createRedisTemplateForStreams(connectionFactory, containerOptions);
    }

    public static RedisTemplate<String, String> createRedisTemplateForStreams(
            final RedisConnectionFactory connectionFactory,
            final StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, byte[]>> containerOptions) {
        final RedisTemplate<String, String> redisTemplate = new RedisTemplate<>();
        redisTemplate.setKeySerializer(containerOptions.getKeySerializer());
        redisTemplate.setValueSerializer(containerOptions.getKeySerializer());
        redisTemplate.setHashKeySerializer(containerOptions.getHashKeySerializer());
        redisTemplate.setHashValueSerializer(containerOptions.getHashValueSerializer());
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    public static RedisTemplate<String, String> createRedisTemplateForKeyValues(final RedisConnectionFactory connectionFactory) {
        final RedisTemplate<String, String> redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    public static RedisTemplate<String, byte[]> createRedisTemplateForPubSub(final RedisConnectionFactory connectionFactory) {
        final RedisTemplate<String, byte[]> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.setValueSerializer(RedisSerializer.byteArray());
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }
}
