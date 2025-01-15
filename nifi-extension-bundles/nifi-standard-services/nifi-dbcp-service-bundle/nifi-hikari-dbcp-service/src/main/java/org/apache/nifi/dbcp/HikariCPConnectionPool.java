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
package org.apache.nifi.dbcp;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;

import javax.security.auth.login.LoginException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;

/**
 * Implementation of Database Connection Pooling Service. HikariCP is used for connection pooling functionality.
 */
@RequiresInstanceClassLoading
@Tags({"dbcp", "hikari", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service based on HikariCP. Connections can be asked from pool and returned after usage.")
@SupportsSensitiveDynamicProperties
@DynamicProperty(name = "JDBC property name", value = "JDBC property value", expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
        description = "Specifies a property name and value to be set on the JDBC connection(s). "
                + "If Expression Language is used, evaluation will be performed upon the controller service being enabled. "
                + "Note that no flow file input (attributes, e.g.) is available for use in Expression Language constructs for these properties.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Database Driver Location can reference resources over HTTP"
                )
        }
)
public class HikariCPConnectionPool extends AbstractControllerService implements DBCPService, VerifiableControllerService {
    /**
     * Property Name Prefix for Sensitive Dynamic Properties
     */
    protected static final String SENSITIVE_PROPERTY_PREFIX = "SENSITIVE.";
    protected static final long INFINITE_MILLISECONDS = -1L;

    private static final String DEFAULT_TOTAL_CONNECTIONS = "10";
    private static final String DEFAULT_MAX_CONN_LIFETIME = "-1";

    private static final int DEFAULT_MIN_VALIDATION_TIMEOUT = 250;

    public static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
            .name("hikaricp-connection-url")
            .displayName("Database Connection URL")
            .description("A database connection URL used to connect to a database. May contain database system name, host, port, database name and some parameters."
                    + " The exact syntax of a database connection URL is specified by your DBMS.")
            .addValidator(new ConnectionUrlValidator())
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
            .name("hikaricp-driver-classname")
            .displayName("Database Driver Class Name")
            .description("The fully-qualified class name of the JDBC driver. Example: com.mysql.jdbc.Driver")
            .required(true)
            .addValidator(new DriverClassValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("hikaricp-driver-locations")
            .displayName("Database Driver Location(s)")
            .description("Comma-separated list of files/folders and/or URLs containing the driver JAR and its dependencies (if any). For example '/var/tmp/mariadb-java-client-1.1.7.jar'")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY, ResourceType.URL)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
            .name("hikaricp-username")
            .displayName("Database User")
            .description("Database user name")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("hikaricp-password")
            .displayName("Password")
            .description("The password for the database user")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("hikaricp-max-wait-time")
            .displayName("Max Wait Time")
            .description("The maximum amount of time that the pool will wait (when there are no available connections) "
                    + " for a connection to be returned before failing, or 0 <time units> to wait indefinitely. ")
            .defaultValue("500 millis")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("hikaricp-max-total-conns")
            .displayName("Max Total Connections")
            .description("This property controls the maximum size that the pool is allowed to reach, including both idle and in-use connections. Basically this value will determine the "
                    + "maximum number of actual connections to the database backend. A reasonable value for this is best determined by your execution environment. When the pool reaches "
                    + "this size, and no idle connections are available, the service will block for up to connectionTimeout milliseconds before timing out.")
            .defaultValue(DEFAULT_TOTAL_CONNECTIONS)
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor VALIDATION_QUERY = new PropertyDescriptor.Builder()
            .name("hikaricp-validation-query")
            .displayName("Validation Query")
            .description("Validation Query used to validate connections before returning them. "
                    + "When connection is invalid, it gets dropped and new valid connection will be returned. "
                    + "NOTE: Using validation might have some performance penalty.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
            .name("hikaricp-min-idle-conns")
            .displayName("Minimum Idle Connections")
            .description("This property controls the minimum number of idle connections that HikariCP tries to maintain in the pool. If the idle connections dip below this value and total "
                    + "connections in the pool are less than 'Max Total Connections', HikariCP will make a best effort to add additional connections quickly and efficiently. It is recommended "
                    + "that this property to be set equal to 'Max Total Connections'.")
            .defaultValue(DEFAULT_TOTAL_CONNECTIONS)
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = new PropertyDescriptor.Builder()
            .name("hikaricp-max-conn-lifetime")
            .displayName("Max Connection Lifetime")
            .description("The maximum lifetime of a connection. After this time is exceeded the " +
                    "connection will fail the next activation, passivation or validation test. A value of zero or less " +
                    "means the connection has an infinite lifetime.")
            .defaultValue(DEFAULT_MAX_CONN_LIFETIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("hikaricp-kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Specifies the Kerberos User Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        DATABASE_URL,
        DB_DRIVERNAME,
        DB_DRIVER_LOCATION,
        KERBEROS_USER_SERVICE,
        DB_USER,
        DB_PASSWORD,
        MAX_WAIT_TIME,
        MAX_TOTAL_CONNECTIONS,
        VALIDATION_QUERY,
        MIN_IDLE,
        MAX_CONN_LIFETIME
    );

    private volatile HikariDataSource dataSource;
    private volatile KerberosUser kerberosUser;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR);

        if (propertyDescriptorName.startsWith(SENSITIVE_PROPERTY_PREFIX)) {
            builder.sensitive(true).expressionLanguageSupported(ExpressionLanguageScope.NONE);
        } else {
            builder.expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT);
        }

        return builder.build();
    }

    /**
     * Configures connection pool by creating an instance of the
     * {@link HikariDataSource} based on configuration provided with
     * {@link ConfigurationContext}.
     * <p>
     * This operation makes no guarantees that the actual connection could be
     * made since the underlying system may still go off-line during normal
     * operation of the connection pool.
     *
     * @param context the configuration context
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        dataSource = new HikariDataSource();
        configureDataSource(context, dataSource);
    }

    private long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? INFINITE_MILLISECONDS : prop.asTimePeriod(TimeUnit.MILLISECONDS);
    }

    /**
     * Shutdown pool, close all open connections.
     * If a principal is authenticated with a KDC, that principal is logged out.
     * <p>
     * If a @{@link LoginException} occurs while attempting to log out the @{@link org.apache.nifi.security.krb.KerberosUser},
     * an attempt will still be made to shut down the pool and close open connections.
     *
     */
    @OnDisabled
    public void shutdown() {
        try {
            if (kerberosUser != null) {
                kerberosUser.logout();
            }
        } finally {
            kerberosUser = null;
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            } finally {
                dataSource = null;
            }
        }
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            final Connection con;
            if (kerberosUser != null) {
                KerberosAction<Connection> kerberosAction = new KerberosAction<>(kerberosUser, dataSource::getConnection, getLogger());
                con = kerberosAction.execute();
            } else {
                con = dataSource.getConnection();
            }
            return con;
        } catch (final SQLException e) {
            // If using Kerberos,  attempt to re-login
            if (kerberosUser != null) {
                getLogger().info("Error getting connection, performing Kerberos re-login");
                kerberosUser.login();
            }
            throw new ProcessException("Connection retrieval failed", e);
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        List<ConfigVerificationResult> results = new ArrayList<>();
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
        KerberosUser kerberosUser = null;
        try {
            if (kerberosUserService != null) {
                kerberosUser = kerberosUserService.createKerberosUser();
                if (kerberosUser != null) {
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Configure Kerberos User")
                            .outcome(SUCCESSFUL)
                            .explanation("Successfully configured Kerberos user")
                            .build());
                }
            }
        } catch (final Exception e) {
            verificationLogger.error("Failed to configure Kerberos user", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Configure Kerberos User")
                    .outcome(FAILED)
                    .explanation("Failed to configure Kerberos user: " + e.getMessage())
                    .build());
        }

        final HikariDataSource hikariDataSource = new HikariDataSource();
        try {
            configureDataSource(context, hikariDataSource);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Configure Data Source")
                    .outcome(SUCCESSFUL)
                    .explanation("Successfully configured data source")
                    .build());

            try (final Connection conn = getConnection(hikariDataSource, kerberosUser)) {
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Establish Connection")
                        .outcome(SUCCESSFUL)
                        .explanation("Successfully established Database Connection")
                        .build());
            } catch (final Exception e) {
                verificationLogger.error("Failed to establish Database Connection", e);
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Establish Connection")
                        .outcome(FAILED)
                        .explanation("Failed to establish Database Connection: " + e.getMessage())
                        .build());
            }
        } catch (final Exception e) {
            String message = "Failed to configure Data Source.";
            if (e.getCause() instanceof ClassNotFoundException) {
                message += String.format("  Ensure changes to the '%s' property are applied before verifying",
                        DB_DRIVER_LOCATION.getDisplayName());
            }
            verificationLogger.error(message, e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Configure Data Source")
                    .outcome(FAILED)
                    .explanation(message + ": " + e.getMessage())
                    .build());
        } finally {
            try {
                shutdown(dataSource, kerberosUser);
            } catch (final SQLException e) {
                verificationLogger.error("Failed to shut down data source", e);
            }
        }
        return results;
    }

    protected void configureDataSource(final ConfigurationContext context, final HikariDataSource dataSource) {
        final String driverName = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions().getValue();
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String passw = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        final String dburl = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        final long maxWaitMillis = extractMillisWithInfinite(context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions());
        final int minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        final long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());

        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

        if (kerberosUserService != null) {
            kerberosUser = kerberosUserService.createKerberosUser();
            if (kerberosUser != null) {
                kerberosUser.login();
            }
        }

        dataSource.setConnectionTimeout(maxWaitMillis);
        dataSource.setValidationTimeout(Math.max(maxWaitMillis, DEFAULT_MIN_VALIDATION_TIMEOUT));
        dataSource.setMaximumPoolSize(maxTotal);
        dataSource.setMinimumIdle(minIdle);
        dataSource.setMaxLifetime(maxConnLifetimeMillis);

        if (validationQuery != null && !validationQuery.isEmpty()) {
            dataSource.setConnectionTestQuery(validationQuery);
        }

        dataSource.setDriverClassName(driverName);
        dataSource.setJdbcUrl(dburl);
        dataSource.setUsername(user);
        dataSource.setPassword(passw);

        final List<PropertyDescriptor> dynamicProperties = context.getProperties()
                .keySet()
                .stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toList());

        Properties properties = dataSource.getDataSourceProperties();
        dynamicProperties.forEach((descriptor) -> {
            final PropertyValue propertyValue = context.getProperty(descriptor);
            if (descriptor.isSensitive()) {
                final String propertyName = StringUtils.substringAfter(descriptor.getName(), SENSITIVE_PROPERTY_PREFIX);
                properties.setProperty(propertyName, propertyValue.getValue());
            } else {
                properties.setProperty(descriptor.getName(), propertyValue.evaluateAttributeExpressions().getValue());
            }
        });
        dataSource.setDataSourceProperties(properties);
        dataSource.setPoolName(toString());
    }

    private Connection getConnection(final HikariDataSource dataSource, final KerberosUser kerberosUser) {
        try {
            final Connection con;
            if (kerberosUser != null) {
                KerberosAction<Connection> kerberosAction = new KerberosAction<>(kerberosUser, dataSource::getConnection, getLogger());
                con = kerberosAction.execute();
            } else {
                con = dataSource.getConnection();
            }
            return con;
        } catch (final SQLException e) {
            // If using Kerberos,  attempt to re-login
            if (kerberosUser != null) {
                try {
                    getLogger().info("Error getting connection, performing Kerberos re-login", e);
                    kerberosUser.login();
                } catch (KerberosLoginException le) {
                    throw new ProcessException("Unable to authenticate Kerberos principal", le);
                }
            }
            throw new ProcessException(e);
        }
    }

    private void shutdown(final HikariDataSource dataSource, final KerberosUser kerberosUser) throws SQLException {
        try {
            if (kerberosUser != null) {
                kerberosUser.logout();
            }
        } finally {
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }

        @Override
    public String toString() {
        return String.format("%s[id=%s]", getClass().getSimpleName(), getIdentifier());
    }

    HikariDataSource getDataSource() {
        return dataSource;
    }
}
