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

package org.apache.nifi.processors.airtable;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.processors.airtable.service.AirtableRestService.API_V0_BASE_URL;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.airtable.parse.AirtableRetrieveTableResult;
import org.apache.nifi.processors.airtable.parse.AirtableTableRetriever;
import org.apache.nifi.processors.airtable.service.AirtableGetRecordsParameters;
import org.apache.nifi.processors.airtable.service.AirtableRestService;
import org.apache.nifi.processors.airtable.service.RateLimitExceededException;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

@PrimaryNodeOnly
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@TriggerWhenEmpty
@Tags({"airtable", "query", "database"})
@CapabilityDescription("Query records from an Airtable table. Records are incrementally retrieved based on the last modified time of the records."
        + " Records can also be further filtered by setting the 'Custom Filter' property which supports the formulas provided by the Airtable API."
        + " This processor is intended to be run on the Primary Node only.")
@Stateful(scopes = Scope.CLUSTER, description = "The last successful query's time is stored in order to enable incremental loading."
        + " The initial query returns all the records in the table and each subsequent query filters the records by their last modified time."
        + " In other words, if a record is updated after the last successful query only the updated records will be returned in the next query."
        + " State is stored across the cluster, so this Processor can run only on the Primary Node and if a new Primary Node is selected,"
        + " the new node can pick up where the previous one left off without duplicating the data.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Sets the number of records in the FlowFile."),
        @WritesAttribute(attribute = "fragment.identifier", description = "If 'Max Records Per FlowFile' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Records Per FlowFile' is set then this is the total number of "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet."),
        @WritesAttribute(attribute = "fragment.index", description = "If 'Max Records Per FlowFile' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order "
                + "FlowFiles were produced"),
})
@DefaultSettings(yieldDuration = "15 sec")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class QueryAirtableTable extends AbstractProcessor {

    static final PropertyDescriptor API_URL = new PropertyDescriptor.Builder()
            .name("api-url")
            .displayName("API URL")
            .description("The URL for the Airtable REST API including the domain and the path to the API (e.g. https://api.airtable.com/v0).")
            .defaultValue(API_V0_BASE_URL)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    // API Keys are deprecated, Airtable now provides Personal Access Tokens instead.
    static final PropertyDescriptor PAT = new PropertyDescriptor.Builder()
            .name("pat")
            .displayName("Personal Access Token")
            .description("The Personal Access Token (PAT) to use in queries. Should be generated on Airtable's account page.")
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor BASE_ID = new PropertyDescriptor.Builder()
            .name("base-id")
            .displayName("Base ID")
            .description("The ID of the Airtable base to be queried.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_ID = new PropertyDescriptor.Builder()
            .name("table-id")
            .displayName("Table ID")
            .description("The name or the ID of the Airtable table to be queried.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("fields")
            .displayName("Fields")
            .description("Comma-separated list of fields to query from the table. Both the field's name and ID can be used.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor CUSTOM_FILTER = new PropertyDescriptor.Builder()
            .name("custom-filter")
            .displayName("Custom Filter")
            .description("Filter records by Airtable's formulas.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor QUERY_TIME_WINDOW_LAG = new PropertyDescriptor.Builder()
            .name("query-time-window-lag")
            .displayName("Query Time Window Lag")
            .description("The amount of lag to be applied to the query time window's end point. Set this property to avoid missing records when the clock of your local machines"
                    + " and Airtable servers' clock are not in sync. Must be greater than or equal to 1 second.")
            .defaultValue("3 s")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor WEB_CLIENT_SERVICE_PROVIDER = new PropertyDescriptor.Builder()
            .name("web-client-service-provider")
            .displayName("Web Client Service Provider")
            .description("Web Client Service Provider to use for Airtable REST API requests")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    static final PropertyDescriptor QUERY_PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("query-page-size")
            .displayName("Query Page Size")
            .description("Number of records to be fetched in a page. Should be between 1 and 100 inclusively.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.createLongValidator(1, 100, true))
            .build();

    static final PropertyDescriptor MAX_RECORDS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("max-records-per-flowfile")
            .displayName("Max Records Per FlowFile")
            .description("The maximum number of result records that will be included in a single FlowFile. This will allow you to break up very large"
                    + " result sets into multiple FlowFiles. If no value specified, then all records are returned in a single FlowFile.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful query.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            API_URL,
            PAT,
            BASE_ID,
            TABLE_ID,
            FIELDS,
            CUSTOM_FILTER,
            QUERY_TIME_WINDOW_LAG,
            WEB_CLIENT_SERVICE_PROVIDER,
            QUERY_PAGE_SIZE,
            MAX_RECORDS_PER_FLOWFILE
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS
    );

    private static final String LAST_QUERY_TIME_WINDOW_END = "last_query_time_window_end";

    private volatile AirtableRestService airtableRestService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String apiUrl = context.getProperty(API_URL).evaluateAttributeExpressions().getValue();
        final String pat = context.getProperty(PAT).getValue();
        final String baseId = context.getProperty(BASE_ID).evaluateAttributeExpressions().getValue();
        final String tableId = context.getProperty(TABLE_ID).evaluateAttributeExpressions().getValue();
        final WebClientServiceProvider webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
        airtableRestService = new AirtableRestService(webClientServiceProvider, apiUrl, pat, baseId, tableId);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final Integer maxRecordsPerFlowFile = context.getProperty(MAX_RECORDS_PER_FLOWFILE).evaluateAttributeExpressions().asInteger();
        final Long queryTimeWindowLagSeconds = context.getProperty(QUERY_TIME_WINDOW_LAG).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS);

        final StateMap state;
        try {
            state = session.getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to get cluster state", e);
        }

        final String lastRecordFetchDateTime = state.get(LAST_QUERY_TIME_WINDOW_END);
        final String currentRecordFetchDateTime = OffsetDateTime.now()
                .minusSeconds(queryTimeWindowLagSeconds)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        final AirtableGetRecordsParameters getRecordsParameters = buildGetRecordsParameters(context, lastRecordFetchDateTime, currentRecordFetchDateTime);
        final AirtableRetrieveTableResult retrieveTableResult;
        try {
            final AirtableTableRetriever tableRetriever = new AirtableTableRetriever(airtableRestService, getRecordsParameters, maxRecordsPerFlowFile);
            retrieveTableResult = tableRetriever.retrieveAll(session);
        } catch (IOException e) {
            throw new ProcessException("Failed to read Airtable records", e);
        } catch (RateLimitExceededException e) {
            context.yield();
            throw new ProcessException("Airtable REST API rate limit exceeded while reading records", e);
        }

        final Map<String, String> newState = new HashMap<>(state.toMap());
        newState.put(LAST_QUERY_TIME_WINDOW_END, currentRecordFetchDateTime);
        try {
            session.setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to update cluster state", e);
        }

        final List<FlowFile> flowFiles = retrieveTableResult.getFlowFiles();
        if (flowFiles.isEmpty()) {
            context.yield();
            return;
        }

        if (maxRecordsPerFlowFile != null) {
            addFragmentAttributesToFlowFiles(session, flowFiles);
        }
        transferFlowFiles(session, flowFiles, retrieveTableResult.getTotalRecordCount());
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.renameProperty("api-key", PAT.getName());
    }

    private AirtableGetRecordsParameters buildGetRecordsParameters(final ProcessContext context,
            final String lastRecordFetchTime,
            final String nowDateTimeString) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(nowDateTimeString);

        final String fieldsProperty = context.getProperty(FIELDS).evaluateAttributeExpressions().getValue();
        final String customFilter = context.getProperty(CUSTOM_FILTER).evaluateAttributeExpressions().getValue();
        final Integer pageSize = context.getProperty(QUERY_PAGE_SIZE).evaluateAttributeExpressions().asInteger();

        final AirtableGetRecordsParameters.Builder getRecordsParametersBuilder = new AirtableGetRecordsParameters.Builder();
        if (lastRecordFetchTime != null) {
            getRecordsParametersBuilder.modifiedAfter(lastRecordFetchTime);
        }
        getRecordsParametersBuilder.modifiedBefore(nowDateTimeString);
        if (fieldsProperty != null) {
            getRecordsParametersBuilder.fields(Arrays.stream(fieldsProperty.split(",")).map(String::trim).collect(Collectors.toList()));
        }
        getRecordsParametersBuilder.customFilter(customFilter);
        if (pageSize != null) {
            getRecordsParametersBuilder.pageSize(pageSize);
        }

        return getRecordsParametersBuilder.build();
    }

    private void addFragmentAttributesToFlowFiles(final ProcessSession session, final List<FlowFile> flowFiles) {
        final String fragmentIdentifier = UUID.randomUUID().toString();
        for (int i = 0; i < flowFiles.size(); i++) {
            final Map<String, String> fragmentAttributes = new HashMap<>();
            fragmentAttributes.put(FRAGMENT_ID.key(), fragmentIdentifier);
            fragmentAttributes.put(FRAGMENT_INDEX.key(), String.valueOf(i));
            fragmentAttributes.put(FRAGMENT_COUNT.key(), String.valueOf(flowFiles.size()));

            flowFiles.set(i, session.putAllAttributes(flowFiles.get(i), fragmentAttributes));
        }
    }

    private void transferFlowFiles(final ProcessSession session, final List<FlowFile> flowFiles, final int totalRecordCount) {
        final String transitUri = airtableRestService.createUriBuilder().build().toString();
        for (final FlowFile flowFile : flowFiles) {
            session.getProvenanceReporter().receive(flowFile, transitUri);
            session.transfer(flowFile, REL_SUCCESS);
        }
        session.adjustCounter("Records Processed", totalRecordCount, false);
        final String flowFilesAsString = flowFiles.stream().map(FlowFile::toString).collect(Collectors.joining(", ", "[", "]"));
        getLogger().debug("Transferred FlowFiles [{}] Records [{}]", flowFilesAsString, totalRecordCount);
    }
}
