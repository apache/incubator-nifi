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
package org.apache.nifi.processors.splunk;

import com.splunk.JobExportArgs;
import com.splunk.SSLSecurityProtocol;
import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ClassloaderIsolationKeyProvider;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.ssl.SSLContextProvider;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"get", "splunk", "logs"})
@CapabilityDescription("Retrieves data from Splunk Enterprise.")
@WritesAttributes({
        @WritesAttribute(attribute = "splunk.query", description = "The query that performed to produce the FlowFile."),
        @WritesAttribute(attribute = "splunk.earliest.time", description = "The value of the earliest time that was used when performing the query."),
        @WritesAttribute(attribute = "splunk.latest.time", description = "The value of the latest time that was used when performing the query.")
})
@RequiresInstanceClassLoading(cloneAncestorResources = true)
@Stateful(scopes = Scope.CLUSTER, description = "If using one of the managed Time Range Strategies, this processor will " +
        "store the values of the latest and earliest times from the previous execution so that the next execution of the " +
        "can pick up where the last execution left off. The state will be cleared and start over if the query is changed.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class GetSplunk extends AbstractProcessor implements ClassloaderIsolationKeyProvider {

    public static final String HTTP_SCHEME = "http";
    public static final String HTTPS_SCHEME = "https";

    public static final PropertyDescriptor SCHEME = new PropertyDescriptor.Builder()
            .name("Scheme")
            .description("The scheme for connecting to Splunk.")
            .allowableValues(HTTPS_SCHEME, HTTP_SCHEME)
            .defaultValue(HTTPS_SCHEME)
            .required(true)
            .build();
    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The ip address or hostname of the Splunk server.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .required(true)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .description("The port of the Splunk server.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .defaultValue("8089")
            .build();
    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("Max wait time for connection to the Splunk server.")
            .required(false)
            .defaultValue("5 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Max wait time for response from the Splunk server.")
            .required(false)
            .defaultValue("15 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("Query")
            .description("The query to execute. Typically beginning with a <search> command followed by a search clause, " +
                    "such as <search source=\"tcp:7689\"> to search for messages received on TCP port 7689.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("search * | head 100")
            .required(true)
            .build();

    public static final AllowableValue EVENT_TIME_VALUE = new AllowableValue("Event Time", "Event Time",
            "Search based on the time of the event which may be different than when the event was indexed.");
    public static final AllowableValue INDEX_TIME_VALUE = new AllowableValue("Index Time", "Index Time",
            "Search based on the time the event was indexed in Splunk.");

    public static final PropertyDescriptor TIME_FIELD_STRATEGY = new PropertyDescriptor.Builder()
            .name("Time Field Strategy")
            .description("Indicates whether to search by the time attached to the event, or by the time the event was indexed in Splunk.")
            .allowableValues(EVENT_TIME_VALUE, INDEX_TIME_VALUE)
            .defaultValue(EVENT_TIME_VALUE.getValue())
            .required(true)
            .build();

    public static final AllowableValue MANAGED_BEGINNING_VALUE = new AllowableValue("Managed from Beginning", "Managed from Beginning",
            "The processor will manage the date ranges of the query starting from the beginning of time.");
    public static final AllowableValue MANAGED_CURRENT_VALUE = new AllowableValue("Managed from Current", "Managed from Current",
            "The processor will manage the date ranges of the query starting from the current time.");
    public static final AllowableValue PROVIDED_VALUE = new AllowableValue("Provided", "Provided",
            "The the time range provided through the Earliest Time and Latest Time properties will be used.");

    public static final PropertyDescriptor TIME_RANGE_STRATEGY = new PropertyDescriptor.Builder()
            .name("Time Range Strategy")
            .description("Indicates how to apply time ranges to each execution of the query. Selecting a managed option " +
                    "allows the processor to apply a time range from the last execution time to the current execution time. " +
                    "When using <Managed from Beginning>, an earliest time will not be applied on the first execution, and thus all " +
                    "records searched. When using <Managed from Current> the earliest time of the first execution will be the " +
                    "initial execution time. When using <Provided>, the time range will come from the Earliest Time and Latest Time " +
                    "properties, or no time range will be applied if these properties are left blank.")
            .allowableValues(MANAGED_BEGINNING_VALUE, MANAGED_CURRENT_VALUE, PROVIDED_VALUE)
            .defaultValue(PROVIDED_VALUE.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor EARLIEST_TIME = new PropertyDescriptor.Builder()
            .name("Earliest Time")
            .description("The value to use for the earliest time when querying. Only used with a Time Range Strategy of Provided. " +
                    "See Splunk's documentation on Search Time Modifiers for guidance in populating this field.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor LATEST_TIME = new PropertyDescriptor.Builder()
            .name("Latest Time")
            .description("The value to use for the latest time when querying. Only used with a Time Range Strategy of Provided. " +
                    "See Splunk's documentation on Search Time Modifiers for guidance in populating this field.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor TIME_ZONE = new PropertyDescriptor.Builder()
            .name("Time Zone")
            .description("The Time Zone to use for formatting dates when performing a search. Only used with Managed time strategies.")
            .allowableValues(TimeZone.getAvailableIDs())
            .defaultValue("UTC")
            .required(true)
            .build();
    public static final PropertyDescriptor APP = new PropertyDescriptor.Builder()
            .name("Application")
            .description("The Splunk Application to query.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor OWNER = new PropertyDescriptor.Builder()
            .name("Owner")
            .description("The owner to pass to Splunk.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor TOKEN = new PropertyDescriptor.Builder()
            .name("Token")
            .description("The token to pass to Splunk.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username to authenticate to Splunk.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password to authenticate to Splunk.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .build();

    public static final AllowableValue ATOM_VALUE = new AllowableValue(JobExportArgs.OutputMode.ATOM.name(), JobExportArgs.OutputMode.ATOM.name());
    public static final AllowableValue CSV_VALUE = new AllowableValue(JobExportArgs.OutputMode.CSV.name(), JobExportArgs.OutputMode.CSV.name());
    public static final AllowableValue JSON_VALUE = new AllowableValue(JobExportArgs.OutputMode.JSON.name(), JobExportArgs.OutputMode.JSON.name());
    public static final AllowableValue JSON_COLS_VALUE = new AllowableValue(JobExportArgs.OutputMode.JSON_COLS.name(), JobExportArgs.OutputMode.JSON_COLS.name());
    public static final AllowableValue JSON_ROWS_VALUE = new AllowableValue(JobExportArgs.OutputMode.JSON_ROWS.name(), JobExportArgs.OutputMode.JSON_ROWS.name());
    public static final AllowableValue RAW_VALUE = new AllowableValue(JobExportArgs.OutputMode.RAW.name(), JobExportArgs.OutputMode.RAW.name());
    public static final AllowableValue XML_VALUE = new AllowableValue(JobExportArgs.OutputMode.XML.name(), JobExportArgs.OutputMode.XML.name());

    public static final PropertyDescriptor OUTPUT_MODE = new PropertyDescriptor.Builder()
            .name("Output Mode")
            .description("The output mode for the results.")
            .allowableValues(ATOM_VALUE, CSV_VALUE, JSON_VALUE, JSON_COLS_VALUE, JSON_ROWS_VALUE, RAW_VALUE, XML_VALUE)
            .defaultValue(JSON_VALUE.getValue())
            .required(true)
            .build();

    public static final AllowableValue TLS_1_2_VALUE = new AllowableValue(SSLSecurityProtocol.TLSv1_2.name(), SSLSecurityProtocol.TLSv1_2.name());
    public static final AllowableValue TLS_1_1_VALUE = new AllowableValue(SSLSecurityProtocol.TLSv1_1.name(), SSLSecurityProtocol.TLSv1_1.name());
    public static final AllowableValue TLS_1_VALUE = new AllowableValue(SSLSecurityProtocol.TLSv1.name(), SSLSecurityProtocol.TLSv1.name());
    public static final AllowableValue SSL_3_VALUE = new AllowableValue(SSLSecurityProtocol.SSLv3.name(), SSLSecurityProtocol.SSLv3.name());

    public static final PropertyDescriptor SECURITY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("Security Protocol")
            .description("The security protocol to use for communicating with Splunk.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(TLS_1_2_VALUE, TLS_1_1_VALUE, TLS_1_VALUE, SSL_3_VALUE)
            .defaultValue(TLS_1_2_VALUE.getValue())
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL connections.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Results retrieved from Splunk are sent out this relationship.")
            .build();

    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    public static final String EARLIEST_TIME_KEY = "earliestTime";
    public static final String LATEST_TIME_KEY = "latestTime";

    public static final String QUERY_ATTR = "splunk.query";
    public static final String EARLIEST_TIME_ATTR = "splunk.earliest.time";
    public static final String LATEST_TIME_ATTR = "splunk.latest.time";

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        SCHEME,
        HOSTNAME,
        PORT,
        CONNECT_TIMEOUT,
        READ_TIMEOUT,
        QUERY,
        TIME_FIELD_STRATEGY,
        TIME_RANGE_STRATEGY,
        EARLIEST_TIME,
        LATEST_TIME,
        TIME_ZONE,
        APP,
        OWNER,
        TOKEN,
        USERNAME,
        PASSWORD,
        SECURITY_PROTOCOL,
        OUTPUT_MODE,
        SSL_CONTEXT_SERVICE
    );

    private volatile String transitUri;
    private volatile boolean resetState = false;
    private volatile Service splunkService;
    protected final AtomicBoolean isInitialized = new AtomicBoolean(false);

    @Override
    public final Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String scheme = validationContext.getProperty(SCHEME).getValue();
        final String secProtocol = validationContext.getProperty(SECURITY_PROTOCOL).getValue();

        if (HTTPS_SCHEME.equals(scheme) && StringUtils.isBlank(secProtocol)) {
            results.add(new ValidationResult.Builder()
                    .explanation("Security Protocol must be specified when using HTTPS")
                    .valid(false).subject("Security Protocol").build());
        }

        final String username = validationContext.getProperty(USERNAME).getValue();
        final String password = validationContext.getProperty(PASSWORD).getValue();

        if (!StringUtils.isBlank(username) && StringUtils.isBlank(password)) {
            results.add(new ValidationResult.Builder()
                    .explanation("Password must be specified when providing a Username")
                    .valid(false).subject("Password").build());
        }

        return results;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if ( ((oldValue != null && !oldValue.equals(newValue)))
                && (descriptor.equals(QUERY)
                || descriptor.equals(TIME_FIELD_STRATEGY)
                || descriptor.equals(TIME_RANGE_STRATEGY)
                || descriptor.equals(EARLIEST_TIME)
                || descriptor.equals(LATEST_TIME)
                || descriptor.equals(HOSTNAME))
                ) {
            getLogger().debug("A property that require resetting state was modified - {} oldValue {} newValue {}",
                    descriptor.getDisplayName(), oldValue, newValue);
            resetState = true;
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String scheme = context.getProperty(SCHEME).getValue();
        final String host = context.getProperty(HOSTNAME).getValue();
        final int port = context.getProperty(PORT).asInteger();
        transitUri = scheme + "://" + host + ":" + port;

        // if properties changed since last execution then remove any previous state
        if (resetState) {
            try {
                getLogger().debug("Clearing state based on property modifications");
                context.getStateManager().clear(Scope.CLUSTER);
            } catch (final IOException ioe) {
                getLogger().warn("Failed to clear state", ioe);
            }
            resetState = false;
        }
    }

    @OnStopped
    public void onStopped() {
        if (splunkService != null) {
            isInitialized.set(false);
            splunkService.logout();
            splunkService = null;
        }
    }

    @OnRemoved
    public void onRemoved(final ProcessContext context) {
        try {
            context.getStateManager().clear(Scope.CLUSTER);
        } catch (IOException e) {
           getLogger().error("Unable to clear processor state due to {}", e.getMessage(), e);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final OffsetDateTime currentTime = OffsetDateTime.now();

        synchronized (isInitialized) {
            if (!isInitialized.get()) {
                splunkService = createSplunkService(context);
                isInitialized.set(true);
            }
        }

        final String query = context.getProperty(QUERY).getValue();
        final String outputMode = context.getProperty(OUTPUT_MODE).getValue();
        final String timeRangeStrategy = context.getProperty(TIME_RANGE_STRATEGY).getValue();
        final String timeZone = context.getProperty(TIME_ZONE).getValue();
        final String timeFieldStrategy = context.getProperty(TIME_FIELD_STRATEGY).getValue();

        final JobExportArgs exportArgs = new JobExportArgs();
        exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL);
        exportArgs.setOutputMode(JobExportArgs.OutputMode.valueOf(outputMode));

        String earliestTime = null;
        String latestTime;

        if (PROVIDED_VALUE.getValue().equals(timeRangeStrategy)) {
            // for provided we just use the values of the properties
            earliestTime = context.getProperty(EARLIEST_TIME).getValue();
            latestTime = context.getProperty(LATEST_TIME).getValue();
        } else {
            try {
                // not provided so we need to check the previous state
                final TimeRange previousRange = loadState(session);
                final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT)
                        .withZone(TimeZone.getTimeZone(timeZone).toZoneId());

                if (previousRange == null) {
                    // no previous state so set the earliest time based on the strategy
                    if (MANAGED_CURRENT_VALUE.getValue().equals(timeRangeStrategy)) {
                        earliestTime = dateTimeFormatter.format(currentTime);
                    }

                    // no previous state so set the latest time to the current time
                    latestTime = dateTimeFormatter.format(currentTime);

                    // if its the first time through don't actually run, just save the state to get the
                    // initial time saved and next execution will be the first real execution
                    if (latestTime.equals(earliestTime)) {
                        saveState(session, new TimeRange(earliestTime, latestTime));
                        return;
                    }

                } else {
                    // we have previous state so set earliestTime to (latestTime + 1) of last range
                    try {
                        final String previousLastTime = previousRange.getLatestTime();
                        final OffsetDateTime previousLastDate = OffsetDateTime.parse(previousLastTime, dateTimeFormatter);

                        earliestTime = dateTimeFormatter.format(previousLastDate.plusSeconds(1));
                        latestTime = dateTimeFormatter.format(currentTime);
                    } catch (DateTimeParseException e) {
                       throw new ProcessException(e);
                    }
                }

            } catch (IOException e) {
                getLogger().error("Unable to load data from State Manager due to {}", e.getMessage(), e);
                context.yield();
                return;
            }
        }

        if (!StringUtils.isBlank(earliestTime)) {
            if (EVENT_TIME_VALUE.getValue().equalsIgnoreCase(timeFieldStrategy)) {
                exportArgs.setEarliestTime(earliestTime);
            } else {
                exportArgs.setIndexEarliest(earliestTime);
            }
        }

        if (!StringUtils.isBlank(latestTime)) {
            if (EVENT_TIME_VALUE.getValue().equalsIgnoreCase(timeFieldStrategy)) {
                exportArgs.setLatestTime(latestTime);
            } else {
                exportArgs.setIndexLatest(latestTime);
            }
        }

        if (EVENT_TIME_VALUE.getValue().equalsIgnoreCase(timeFieldStrategy)) {
            getLogger().debug("Using earliest_time of {} and latest_time of {}", earliestTime, latestTime);
        } else {
            getLogger().debug("Using index_earliest of {} and index_latest of {}", earliestTime, latestTime);
        }

        InputStream export;
        try {
            export = splunkService.export(query, exportArgs);
        //Catch Stale connection exception, reinitialize, and retry
        } catch (com.splunk.HttpException e) {
            getLogger().error("Splunk request status code:{} Retrying the request.", e.getStatus());
            splunkService.logout();
            splunkService = createSplunkService(context);
            export = splunkService.export(query, exportArgs);
        }

        final InputStream exportSearch = export;

        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, rawOut -> {
            try (BufferedOutputStream out = new BufferedOutputStream(rawOut)) {
                IOUtils.copyLarge(exportSearch, out);
            }
        });

        final Map<String, String> attributes = new HashMap<>(3);
        attributes.put(EARLIEST_TIME_ATTR, earliestTime);
        attributes.put(LATEST_TIME_ATTR, latestTime);
        attributes.put(QUERY_ATTR, query);
        flowFile = session.putAllAttributes(flowFile, attributes);

        session.getProvenanceReporter().receive(flowFile, transitUri);
        session.transfer(flowFile, REL_SUCCESS);
        getLogger().debug("Received {} from Splunk", flowFile);

        // save the time range for the next execution to pick up where we left off
        // if saving fails then roll back the session so we can try again next execution
        // only need to do this for the managed time strategies
        if (!PROVIDED_VALUE.getValue().equals(timeRangeStrategy)) {
            try {
                saveState(session, new TimeRange(earliestTime, latestTime));
            } catch (IOException e) {
                getLogger().error("Unable to load data from State Manager due to {}", e.getMessage(), e);
                session.rollback();
                context.yield();
            }
        }
    }

    protected Service createSplunkService(final ProcessContext context) {
        final ServiceArgs serviceArgs = new ServiceArgs();

        final String scheme = context.getProperty(SCHEME).getValue();
        serviceArgs.setScheme(scheme);

        final String host = context.getProperty(HOSTNAME).getValue();
        serviceArgs.setHost(host);

        final int port = context.getProperty(PORT).asInteger();
        serviceArgs.setPort(port);

        final int connect_timeout = context.getProperty(CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        serviceArgs.add("connectTimeout", connect_timeout);

        final int read_timeout = context.getProperty(READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        serviceArgs.add("readTimeout", read_timeout);

        final String app = context.getProperty(APP).getValue();
        if (!StringUtils.isBlank(app)) {
            serviceArgs.setApp(app);
        }

        final String owner = context.getProperty(OWNER).getValue();
        if (!StringUtils.isBlank(owner)) {
            serviceArgs.setOwner(owner);
        }

        final String token = context.getProperty(TOKEN).getValue();
        if (!StringUtils.isBlank(token)) {
            serviceArgs.setToken(token);
        }

        final String username = context.getProperty(USERNAME).getValue();
        if (!StringUtils.isBlank(username)) {
            serviceArgs.setUsername(username);
        }

        final String password = context.getProperty(PASSWORD).getValue();
        if (!StringUtils.isBlank(password)) {
            serviceArgs.setPassword(password);
        }

        final String secProtocol = context.getProperty(SECURITY_PROTOCOL).getValue();
        if (!StringUtils.isBlank(secProtocol) && HTTPS_SCHEME.equals(scheme)) {
            serviceArgs.setSSLSecurityProtocol(SSLSecurityProtocol.valueOf(secProtocol));
        }

        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        if (sslContextProvider != null) {
            Service.setSSLSocketFactory(sslContextProvider.createContext().getSocketFactory());
        }

        return Service.connect(serviceArgs);
    }

    private void saveState(final ProcessSession session, TimeRange timeRange) throws IOException {
        final String earliest = StringUtils.isBlank(timeRange.getEarliestTime()) ? "" : timeRange.getEarliestTime();
        final String latest = StringUtils.isBlank(timeRange.getLatestTime()) ? "" : timeRange.getLatestTime();

        Map<String, String> state = new HashMap<>(2);
        state.put(EARLIEST_TIME_KEY, earliest);
        state.put(LATEST_TIME_KEY, latest);

        getLogger().debug("Saving state with earliestTime of {} and latestTime of {}", earliest, latest);
        session.setState(state, Scope.CLUSTER);
    }

    private TimeRange loadState(final ProcessSession session) throws IOException {
        final StateMap stateMap = session.getState(Scope.CLUSTER);

        if (stateMap.getStateVersion().isEmpty()) {
            getLogger().debug("No previous state found");
            return null;
        }

        final String earliest = stateMap.get(EARLIEST_TIME_KEY);
        final String latest = stateMap.get(LATEST_TIME_KEY);
        getLogger().debug("Loaded state with earliestTime of {} and latestTime of {}", earliest, latest);

        if (StringUtils.isBlank(earliest) && StringUtils.isBlank(latest)) {
            return null;
        } else {
            return new TimeRange(earliest, latest);
        }
    }

    @Override
    public String getClassloaderIsolationKey(PropertyContext context) {
        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        if (sslContextProvider != null) {
            // Class loader isolation is only necessary when SSL is enabled, as Service.setSSLSocketFactory
            // changes the Socket Factory for all instances.
            return sslContextProvider.getIdentifier();
        } else {
            // This workaround ensures that instances don't unnecessarily use an isolated classloader.
            return getClass().getName();
        }
    }

    static class TimeRange {

        final String earliestTime;
        final String latestTime;

        public TimeRange(String earliestTime, String latestTime) {
            this.earliestTime = earliestTime;
            this.latestTime = latestTime;
        }

        public String getEarliestTime() {
            return earliestTime;
        }

        public String getLatestTime() {
            return latestTime;
        }

    }

}
