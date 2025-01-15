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

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.utils.DBCPProperties;
import org.apache.nifi.dbcp.utils.DataSourceConfiguration;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import javax.security.auth.login.LoginException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.dbcp.utils.DBCPProperties.DATABASE_URL;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVERNAME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_PASSWORD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_USER;
import static org.apache.nifi.dbcp.utils.DBCPProperties.EVICTION_RUN_PERIOD;
import static org.apache.nifi.dbcp.utils.DBCPProperties.KERBEROS_USER_SERVICE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_CONN_LIFETIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_TOTAL_CONNECTIONS;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MAX_WAIT_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.MIN_IDLE;
import static org.apache.nifi.dbcp.utils.DBCPProperties.SOFT_MIN_EVICTABLE_IDLE_TIME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.VALIDATION_QUERY;
import static org.apache.nifi.dbcp.utils.DBCPProperties.extractMillisWithInfinite;

/**
 * Implementation of Database Connection Pooling Service for Hadoop related JDBC Service.
 * Apache DBCP is used for connection pooling functionality.
 */
@RequiresInstanceClassLoading
@Tags({"dbcp", "jdbc", "database", "connection", "pooling", "store", "hadoop"})
@CapabilityDescription("Provides a Database Connection Pooling Service for Hadoop related JDBC services. This service requires that " +
        "the Database Driver Location(s) contains some version of a hadoop-common JAR, or a shaded JAR that shades hadoop-common.")
@DynamicProperty(name = "The name of a Hadoop configuration property.", value = "The value of the given Hadoop configuration property.",
        description = "These properties will be set on the Hadoop configuration after loading any provided configuration files.",
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT)
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.REFERENCE_REMOTE_RESOURCES,
                        explanation = "Database Driver Location can reference resources over HTTP"
                )
        }
)
public class HadoopDBCPConnectionPool extends AbstractDBCPConnectionPool {

    private static final String HADOOP_CONFIGURATION_CLASS = "org.apache.hadoop.conf.Configuration";
    private static final String HADOOP_UGI_CLASS = "org.apache.hadoop.security.UserGroupInformation";

    public static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(DBCPProperties.DB_DRIVER_LOCATION)
            .description("Comma-separated list of files/folders and/or URLs containing the driver JAR and its dependencies. " +
                    "For example '/var/tmp/phoenix-client.jar'. NOTE: It is required that the resources specified by this property provide " +
                    "the classes from hadoop-common, such as Configuration and UserGroupInformation.")
            .required(true)
            .build();

    public static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hadoop-config-resources")
            .displayName("Hadoop Configuration Resources")
            .description("A file, or comma separated list of files, which contain the Hadoop configuration (core-site.xml, etc.). Without this, Hadoop "
                    + "will search the classpath, or will revert to a default configuration. Note that to enable authentication with Kerberos, "
                    + "the appropriate properties must be set in the configuration files.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dynamicallyModifiesClasspath(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        DATABASE_URL,
        DB_DRIVERNAME,
        DB_DRIVER_LOCATION,
        HADOOP_CONFIGURATION_RESOURCES,
        KERBEROS_USER_SERVICE,
        DB_USER,
        DB_PASSWORD,
        MAX_WAIT_TIME,
        MAX_TOTAL_CONNECTIONS,
        VALIDATION_QUERY,
        MIN_IDLE,
        MAX_IDLE,
        MAX_CONN_LIFETIME,
        EVICTION_RUN_PERIOD,
        MIN_EVICTABLE_IDLE_TIME,
        SOFT_MIN_EVICTABLE_IDLE_TIME
    );

    private volatile Boolean foundHadoopDependencies;

    // Holder of cached Configuration information so validation does not reload the same config over and over
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>(null);

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.removeProperty("Kerberos Principal");
        config.removeProperty("Kerberos Password");
        config.removeProperty("Kerberos Keytab");
        config.removeProperty("kerberos-credentials-service");
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>();

        // Determine if we need to validate the presence of the required hadoop dependencies...
        if (foundHadoopDependencies == null) {
            final ClassLoader classLoader = this.getClass().getClassLoader();
            try {
                Class.forName(HADOOP_CONFIGURATION_CLASS, true, classLoader);
                Class.forName(HADOOP_UGI_CLASS, true, classLoader);
                foundHadoopDependencies = true;
            } catch (ClassNotFoundException cnf) {
                getLogger().debug(cnf.getMessage(), cnf);
                foundHadoopDependencies = false;
            }
        }

        // If hadoop classes are missing then we can't perform the rest of the validation, so short circuit and return...
        if (!foundHadoopDependencies) {
            problems.add(new ValidationResult.Builder()
                    .subject(DB_DRIVER_LOCATION.getDisplayName())
                    .valid(false)
                    .explanation("required Hadoop classes were not found in any of the specified resources, please ensure that hadoop-common is available")
                    .build());
            return problems;
        }

        final boolean confFileProvided = validationContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).isSet();
        if (confFileProvided) {
            final String configFiles = validationContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            ValidationResources resources = validationResourceHolder.get();

            // if no resources in the holder, or if the holder has different resources loaded,
            // then load the Configuration and set the new resources in the holder
            if (resources == null || !configFiles.equals(resources.getConfigResources())) {
                getLogger().debug("Reloading validation resources");
                resources = new ValidationResources(configFiles, getConfigurationFromFiles(configFiles));
                validationResourceHolder.set(resources);
            }
        }

        return problems;
    }

    protected Configuration getConfigurationFromFiles(final String configFiles) {
        final Configuration conf = new Configuration();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                conf.addResource(new Path(configFile.trim()));
            }
        }
        return conf;
    }

    /**
     * Configures connection pool by creating an instance of the
     * {@link BasicDataSource} based on configuration provided with
     * {@link ConfigurationContext}.
     * <p>
     * This operation makes no guarantees that the actual connection could be
     * made since the underlying system may still go off-line during normal
     * operation of the connection pool.
     *
     * @param context the configuration context
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        // Get Configuration instance from specified resources
        final String configFiles = context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
        final Configuration hadoopConfig = getConfigurationFromFiles(configFiles);

        // Add any dynamic properties to the HBase Configuration
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                hadoopConfig.set(descriptor.getName(), context.getProperty(descriptor).evaluateAttributeExpressions().getValue());
            }
        }
    }

    /**
     * Shutdown pool, close all open connections.
     * If a principal is authenticated with a KDC, that principal is logged out.
     * <p>
     * If a @{@link LoginException} occurs while attempting to log out the @{@link org.apache.nifi.security.krb.KerberosUser},
     * an attempt will still be made to shut down the pool and close open connections.
     *
     * @throws SQLException if there is an error while closing open connections
     */
    @OnDisabled
    public void shutdown() throws SQLException {
        validationResourceHolder.set(null);
        foundHadoopDependencies = null;
        kerberosUser = null;
        try {
            if (dataSource != null) {
                dataSource.close();
            }
        } finally {
            dataSource = null;
        }
    }

    @Override
    protected Driver getDriver(String driverName, String url) {
        final Class<?> clazz;

        try {
            clazz = Class.forName(driverName);
        } catch (final ClassNotFoundException e) {
            throw new ProcessException("Driver class " + driverName + " is not found", e);
        }

        try {
            return DriverManager.getDriver(url);
        } catch (final SQLException e) {
            // In case the driver is not registered by the implementation, we explicitly try to register it.
            try {
                final Driver driver = (Driver) clazz.getDeclaredConstructor().newInstance();
                DriverManager.registerDriver(driver);
                return DriverManager.getDriver(url);
            } catch (final SQLException e2) {
                throw new ProcessException("No suitable driver for the given Database Connection URL", e2);
            } catch (final Exception e2) {
                throw new ProcessException("Creating driver instance is failed", e2);
            }
        }
    }

    @Override
    protected DataSourceConfiguration getDataSourceConfiguration(ConfigurationContext context) {
        final String url = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();
        final String driverName = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions().getValue();
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        final Long maxWaitMillis = extractMillisWithInfinite(context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions());
        final Integer minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        final Integer maxIdle = context.getProperty(MAX_IDLE).evaluateAttributeExpressions().asInteger();
        final Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());
        final Long timeBetweenEvictionRunsMillis = extractMillisWithInfinite(context.getProperty(EVICTION_RUN_PERIOD).evaluateAttributeExpressions());
        final Long minEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());
        final Long softMinEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());

        return new DataSourceConfiguration.Builder(url, driverName, user, password)
                .validationQuery(validationQuery)
                .maxWaitMillis(maxWaitMillis)
                .maxTotal(maxTotal)
                .minIdle(minIdle)
                .maxIdle(maxIdle)
                .maxConnLifetimeMillis(maxConnLifetimeMillis)
                .timeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis)
                .minEvictableIdleTimeMillis(minEvictableIdleTimeMillis)
                .softMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis)
                .build();
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public String toString() {
        return "HadoopDBCPConnectionPool[id=" + getIdentifier() + "]";
    }
}
