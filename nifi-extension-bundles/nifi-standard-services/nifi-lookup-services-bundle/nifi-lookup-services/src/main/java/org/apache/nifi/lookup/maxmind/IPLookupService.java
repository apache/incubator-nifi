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

package org.apache.nifi.lookup.maxmind;

import com.maxmind.db.InvalidDatabaseException;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse.ConnectionType;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.IspResponse;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Subdivision;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.StopWatch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Tags({"lookup", "enrich", "ip", "geo", "ipgeo", "maxmind", "isp", "domain", "cellular", "anonymous", "tor"})
@CapabilityDescription("A lookup service that provides several types of enrichment information for IP addresses. The service is configured by providing a MaxMind "
    + "Database file and specifying which types of enrichment should be provided for an IP Address or Hostname. Each type of enrichment is a separate lookup, so configuring the "
    + "service to provide all of the available enrichment data may be slower than returning only a portion of the available enrichments. In order to use this service, a lookup "
    + "must be performed using key of 'ip' and a value that is a valid IP address or hostname. View the Usage of this component "
    + "and choose to view Additional Details for more information, such as the Schema that pertains to the information that is returned.")
public class IPLookupService extends AbstractControllerService implements RecordLookupService {

    private volatile String databaseFile = null;
    static final String IP_KEY = "ip";
    private static final Set<String> REQUIRED_KEYS = Stream.of(IP_KEY).collect(Collectors.toSet());

    private volatile DatabaseReader databaseReader = null;
    private volatile String databaseChecksum = null;
    private volatile long databaseLastRefreshAttempt = -1;

    private final Lock dbWriteLock = new ReentrantLock();

    static final long REFRESH_THRESHOLD_MS = 5 * 60 * 1000;

    static final PropertyDescriptor GEO_DATABASE_FILE = new PropertyDescriptor.Builder()
        .name("database-file")
        .displayName("MaxMind Database File")
        .description("Path to Maxmind IP Enrichment Database File")
        .required(true)
        .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();
    static final PropertyDescriptor LOOKUP_CITY = new PropertyDescriptor.Builder()
        .name("lookup-city")
        .displayName("Lookup Geo Enrichment")
        .description("Specifies whether or not information about the geographic information, such as cities, corresponding to the IP address should be returned")
        .allowableValues("true", "false")
        .defaultValue("true")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .build();
    static final PropertyDescriptor LOOKUP_ISP = new PropertyDescriptor.Builder()
        .name("lookup-isp")
        .displayName("Lookup ISP")
        .description("Specifies whether or not information about the Information Service Provider corresponding to the IP address should be returned")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    static final PropertyDescriptor LOOKUP_DOMAIN = new PropertyDescriptor.Builder()
        .name("lookup-domain")
        .displayName("Lookup Domain Name")
        .description("Specifies whether or not information about the Domain Name corresponding to the IP address should be returned. "
            + "If true, the lookup will contain second-level domain information, such as foo.com but will not contain bar.foo.com")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    static final PropertyDescriptor LOOKUP_CONNECTION_TYPE = new PropertyDescriptor.Builder()
        .name("lookup-connection-type")
        .displayName("Lookup Connection Type")
        .description("Specifies whether or not information about the Connection Type corresponding to the IP address should be returned. "
            + "If true, the lookup will contain a 'connectionType' field that (if populated) will contain a value of 'Dialup', 'Cable/DSL', 'Corporate', or 'Cellular'")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    static final PropertyDescriptor LOOKUP_ANONYMOUS_IP_INFO = new PropertyDescriptor.Builder()
        .name("lookup-anonymous-ip")
        .displayName("Lookup Anonymous IP Information")
        .description("Specifies whether or not information about whether or not the IP address belongs to an anonymous network should be returned.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        GEO_DATABASE_FILE,
        LOOKUP_CITY,
        LOOKUP_ISP,
        LOOKUP_DOMAIN,
        LOOKUP_CONNECTION_TYPE,
        LOOKUP_ANONYMOUS_IP_INFO
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws IOException {
        databaseFile = context.getProperty(GEO_DATABASE_FILE).evaluateAttributeExpressions().getValue();

        final File dbFile = new File(databaseFile);
        final String dbFileChecksum = getChecksum(dbFile);
        loadDatabase(dbFile, dbFileChecksum);

        // initialize the last refresh attempt to the time the service was enabled
        databaseLastRefreshAttempt = System.currentTimeMillis();
    }

    private String getChecksum(final File file) throws IOException {
        String fileChecksum;
        try (final InputStream in = new FileInputStream(file)) {
            fileChecksum = DigestUtils.md5Hex(in);
        }

        return fileChecksum;
    }

    @OnStopped
    public void closeReader() throws IOException {
        final DatabaseReader reader = databaseReader;
        if (reader != null) {
            reader.close();
        }

        databaseFile = null;
        databaseReader = null;
        databaseChecksum = null;
        databaseLastRefreshAttempt = -1;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

    @Override
    public Optional<Record> lookup(final Map<String, Object> coordinates) throws LookupFailureException {
        if (coordinates == null) {
            return Optional.empty();
        }

        // determine if we should attempt to refresh the database based on exceeding a certain amount of time since last refresh
        if (shouldAttemptDatabaseRefresh()) {
            try {
                refreshDatabase();
            } catch (IOException e) {
                throw new LookupFailureException("Failed to refresh database file: " + e.getMessage(), e);
            }
        }

        // If an external process changes the underlying file before we have a chance to reload the reader, then we'll get an
        // InvalidDatabaseException, so force a reload and then retry the lookup one time, if we still get an error then throw it
        try {
            final DatabaseReader databaseReader = this.databaseReader;
            return doLookup(databaseReader, coordinates);
        } catch (InvalidDatabaseException idbe) {
            if (dbWriteLock.tryLock()) {
                try {
                    getLogger().debug("Attempting to reload database after InvalidDatabaseException");
                    try {
                        final File dbFile = new File(databaseFile);
                        final String dbFileChecksum = getChecksum(dbFile);
                        loadDatabase(dbFile, dbFileChecksum);
                        databaseLastRefreshAttempt = System.currentTimeMillis();
                    } catch (IOException ioe) {
                        throw new LookupFailureException("Error reloading database due to: " + ioe.getMessage(), ioe);
                    }

                    getLogger().debug("Attempting to retry lookup after InvalidDatabaseException");
                    try {
                        final DatabaseReader databaseReader = this.databaseReader;
                        return doLookup(databaseReader, coordinates);
                    } catch (final Exception e) {
                        throw new LookupFailureException("Error performing look up: " + e.getMessage(), e);
                    }
                } finally {
                    dbWriteLock.unlock();
                }
            } else {
                throw new LookupFailureException("Failed to lookup a value for " + coordinates + " due to " + idbe.getMessage(), idbe);
            }
        }
    }

    private Optional<Record> doLookup(final DatabaseReader databaseReader, final Map<String, Object> coordinates) throws LookupFailureException, InvalidDatabaseException {
        if (coordinates.get(IP_KEY) == null) {
            return Optional.empty();
        }

        final String ipAddress = coordinates.get(IP_KEY).toString();

        final InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getByName(ipAddress);
        } catch (final IOException ioe) {
            getLogger().warn("Could not resolve the IP for value '{}'. This is usually caused by issue resolving the appropriate DNS record or providing the service with an invalid IP address",
                    coordinates, ioe);

            return Optional.empty();
        }

        final Record geoRecord;
        if (getProperty(LOOKUP_CITY).asBoolean()) {
            final CityResponse cityResponse;
            try {
                cityResponse = databaseReader.tryCity(inetAddress).orElse(null);
            } catch (final InvalidDatabaseException idbe) {
                throw idbe;
            } catch (final Exception e) {
                throw new LookupFailureException("Failed to lookup City information for IP Address " + inetAddress, e);
            }

            geoRecord = createRecord(cityResponse);
        } else {
            geoRecord = null;
        }

        final Record ispRecord;
        if (getProperty(LOOKUP_ISP).asBoolean()) {
            final IspResponse ispResponse;
            try {
                ispResponse = databaseReader.tryIsp(inetAddress).orElse(null);
            } catch (final InvalidDatabaseException idbe) {
                throw idbe;
            } catch (final Exception e) {
                throw new LookupFailureException("Failed to lookup ISP information for IP Address " + inetAddress, e);
            }

            ispRecord = createRecord(ispResponse);
        } else {
            ispRecord = null;
        }

        final String domainName;
        if (getProperty(LOOKUP_DOMAIN).asBoolean()) {
            final DomainResponse domainResponse;
            try {
                domainResponse = databaseReader.tryDomain(inetAddress).orElse(null);
            } catch (final InvalidDatabaseException idbe) {
                throw idbe;
            } catch (final Exception e) {
                throw new LookupFailureException("Failed to lookup Domain information for IP Address " + inetAddress, e);
            }

            domainName = domainResponse == null ? null : domainResponse.getDomain();
        } else {
            domainName = null;
        }

        final String connectionType;
        if (getProperty(LOOKUP_CONNECTION_TYPE).asBoolean()) {
            final ConnectionTypeResponse connectionTypeResponse;
            try {
                connectionTypeResponse = databaseReader.tryConnectionType(inetAddress).orElse(null);
            } catch (final InvalidDatabaseException idbe) {
                throw idbe;
            } catch (final Exception e) {
                throw new LookupFailureException("Failed to lookup Domain information for IP Address " + inetAddress, e);
            }

            if (connectionTypeResponse == null) {
                connectionType = null;
            } else {
                final ConnectionType type = connectionTypeResponse.getConnectionType();
                connectionType = type == null ? null : type.name();
            }
        } else {
            connectionType = null;
        }

        final Record anonymousIpRecord;
        if (getProperty(LOOKUP_ANONYMOUS_IP_INFO).asBoolean()) {
            final AnonymousIpResponse anonymousIpResponse;
            try {
                anonymousIpResponse = databaseReader.tryAnonymousIp(inetAddress).orElse(null);
            } catch (final InvalidDatabaseException idbe) {
                throw idbe;
            } catch (final Exception e) {
                throw new LookupFailureException("Failed to lookup Anonymous IP Information for IP Address " + inetAddress, e);
            }

            anonymousIpRecord = createRecord(anonymousIpResponse);
        } else {
            anonymousIpRecord = null;
        }

        if (geoRecord == null && ispRecord == null && domainName == null && connectionType == null && anonymousIpRecord == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(createContainerRecord(geoRecord, ispRecord, domainName, connectionType, anonymousIpRecord));
    }

    // returns true if the reader was never initialized or if the database hasn't been updated in longer than our threshold
    private boolean shouldAttemptDatabaseRefresh() {
        return System.currentTimeMillis() - databaseLastRefreshAttempt >= REFRESH_THRESHOLD_MS;
    }

    private void refreshDatabase() throws IOException {
        // since this is the only place the write lock is used, if something else has it then we know another thread is
        // already refreshing the database so we can just move on if we don't get the lock, no need to block
        if (dbWriteLock.tryLock()) {
            try {
                // now that we have the lock check again to make sure we still need to refresh
                if (shouldAttemptDatabaseRefresh()) {
                    final File dbFile = new File(databaseFile);
                    final String dbFileChecksum = getChecksum(dbFile);
                    if (!dbFileChecksum.equals(databaseChecksum)) {
                        loadDatabase(dbFile, dbFileChecksum);
                    } else {
                        getLogger().debug("Checksum hasn't changed, database will not be reloaded");
                    }

                    // update the timestamp even if we didn't refresh so that we'll wait a full threshold again
                    databaseLastRefreshAttempt = System.currentTimeMillis();
                } else {
                    getLogger().debug("Acquired write lock, but no longer need to reload the database");
                }
            } finally {
                dbWriteLock.unlock();
            }
        } else {
            getLogger().debug("Unable to acquire write lock, skipping reload of database");
        }
    }

    private void loadDatabase(final File dbFile, final String dbFileChecksum) throws IOException {
        final StopWatch stopWatch = new StopWatch(true);
        final DatabaseReader reader = createDatabaseReader(dbFile);
        stopWatch.stop();
        getLogger().info("Completed loading of Maxmind Database.  Elapsed time was {} milliseconds.", stopWatch.getDuration(TimeUnit.MILLISECONDS));
        databaseReader = reader;
        databaseChecksum = dbFileChecksum;
    }

    private Record createRecord(final CityResponse city) {
        if (city == null) {
            return null;
        }

        final Map<String, Object> values = new HashMap<>();
        values.put(CitySchema.CITY.getFieldName(), city.getCity().getName());

        final Location location = city.getLocation();
        values.put(CitySchema.ACCURACY.getFieldName(), location.getAccuracyRadius());
        values.put(CitySchema.METRO_CODE.getFieldName(), location.getMetroCode());
        values.put(CitySchema.TIMEZONE.getFieldName(), location.getTimeZone());
        values.put(CitySchema.LATITUDE.getFieldName(), location.getLatitude());
        values.put(CitySchema.LONGITUDE.getFieldName(), location.getLongitude());
        values.put(CitySchema.CONTINENT.getFieldName(), city.getContinent().getName());
        values.put(CitySchema.POSTALCODE.getFieldName(), city.getPostal().getCode());
        values.put(CitySchema.COUNTRY.getFieldName(), createRecord(city.getCountry()));

        final Object[] subdivisions = new Object[city.getSubdivisions().size()];
        int i = 0;
        for (final Subdivision subdivision : city.getSubdivisions()) {
            subdivisions[i++] = createRecord(subdivision);
        }
        values.put(CitySchema.SUBDIVISIONS.getFieldName(), subdivisions);

        return new MapRecord(CitySchema.GEO_SCHEMA, values);
    }

    private Record createRecord(final Subdivision subdivision) {
        if (subdivision == null) {
            return null;
        }

        final Map<String, Object> values = new HashMap<>(2);
        values.put(CitySchema.SUBDIVISION_NAME.getFieldName(), subdivision.getName());
        values.put(CitySchema.SUBDIVISION_ISO.getFieldName(), subdivision.getIsoCode());
        return new MapRecord(CitySchema.SUBDIVISION_SCHEMA, values);
    }

    private Record createRecord(final Country country) {
        if (country == null) {
            return null;
        }

        final Map<String, Object> values = new HashMap<>(2);
        values.put(CitySchema.COUNTRY_NAME.getFieldName(), country.getName());
        values.put(CitySchema.COUNTRY_ISO.getFieldName(), country.getIsoCode());
        return new MapRecord(CitySchema.COUNTRY_SCHEMA, values);
    }

    private Record createRecord(final IspResponse isp) {
        if (isp == null) {
            return null;
        }

        final Map<String, Object> values = new HashMap<>(4);
        values.put(IspSchema.ASN.getFieldName(), isp.getAutonomousSystemNumber());
        values.put(IspSchema.ASN_ORG.getFieldName(), isp.getAutonomousSystemOrganization());
        values.put(IspSchema.NAME.getFieldName(), isp.getIsp());
        values.put(IspSchema.ORG.getFieldName(), isp.getOrganization());

        return new MapRecord(IspSchema.ISP_SCHEMA, values);
    }

    private Record createRecord(final AnonymousIpResponse anonymousIp) {
        if (anonymousIp == null) {
            return null;
        }

        final Map<String, Object> values = new HashMap<>(5);
        values.put(AnonymousIpSchema.ANONYMOUS.getFieldName(), anonymousIp.isAnonymous());
        values.put(AnonymousIpSchema.ANONYMOUS_VPN.getFieldName(), anonymousIp.isAnonymousVpn());
        values.put(AnonymousIpSchema.HOSTING_PROVIDER.getFieldName(), anonymousIp.isHostingProvider());
        values.put(AnonymousIpSchema.PUBLIC_PROXY.getFieldName(), anonymousIp.isPublicProxy());
        values.put(AnonymousIpSchema.TOR_EXIT_NODE.getFieldName(), anonymousIp.isTorExitNode());

        return new MapRecord(AnonymousIpSchema.ANONYMOUS_IP_SCHEMA, values);
    }

    private Record createContainerRecord(final Record geoRecord, final Record ispRecord, final String domainName, final String connectionType, final Record anonymousIpRecord) {
        final Map<String, Object> values = new HashMap<>(4);
        values.put("geo", geoRecord);
        values.put("isp", ispRecord);
        values.put("domainName", domainName);
        values.put("connectionType", connectionType);
        values.put("anonymousIp", anonymousIpRecord);

        final Record containerRecord = new MapRecord(ContainerSchema.CONTAINER_SCHEMA, values);
        return containerRecord;
    }

    DatabaseReader createDatabaseReader(File dbFile) throws IOException {
        return new DatabaseReader.Builder(dbFile).build();
    }
}
