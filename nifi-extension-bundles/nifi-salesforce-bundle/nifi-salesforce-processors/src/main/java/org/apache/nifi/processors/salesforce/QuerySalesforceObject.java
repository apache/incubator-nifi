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
package org.apache.nifi.processors.salesforce;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.component.salesforce.api.dto.SObjectDescription;
import org.apache.camel.component.salesforce.api.dto.SObjectField;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonParserFactory;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.json.SchemaApplicationStrategy;
import org.apache.nifi.json.StartingFieldStrategy;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.salesforce.rest.SalesforceConfiguration;
import org.apache.nifi.processors.salesforce.rest.SalesforceRestClient;
import org.apache.nifi.processors.salesforce.schema.SalesforceSchemaHolder;
import org.apache.nifi.processors.salesforce.schema.SalesforceToRecordSchemaConverter;
import org.apache.nifi.processors.salesforce.util.IncrementalContext;
import org.apache.nifi.processors.salesforce.util.SalesforceQueryBuilder;
import org.apache.nifi.processors.salesforce.validator.SalesforceAgeValidator;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.API_VERSION;
import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.READ_TIMEOUT;
import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.SALESFORCE_INSTANCE_URL;
import static org.apache.nifi.processors.salesforce.util.CommonSalesforceProperties.TOKEN_PROVIDER;

@TriggerSerially
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"salesforce", "sobject", "soql", "query"})
@CapabilityDescription("Retrieves records from a Salesforce sObject. Users can add arbitrary filter conditions by setting the 'Custom WHERE Condition' property."
        + " The processor can also run a custom query, although record processing is not supported in that case."
        + " Supports incremental retrieval: users can define a field in the 'Age Field' property that will be used to determine when the record was created."
        + " When this property is set the processor will retrieve new records. Incremental loading and record-based processing are only supported in property-based queries."
        + " It's also possible to define an initial cutoff value for the age, filtering out all older records"
        + " even for the first run. In case of 'Property Based Query' this processor should run on the Primary Node only."
        + " FlowFile attribute 'record.count' indicates how many records were retrieved and written to the output."
        + " The processor can accept an optional input FlowFile and reference the FlowFile attributes in the query."
        + " When 'Include Deleted Records' is true, the processor will include deleted records (soft-deletes) in the results by using the 'queryAll' API."
        + " The 'IsDeleted' field will be automatically included in the results when querying deleted records.")
@Stateful(scopes = Scope.CLUSTER, description = "When 'Age Field' is set, after performing a query the time of execution is stored. Subsequent queries will be augmented"
        + " with an additional condition so that only records that are newer than the stored execution time (adjusted with the optional value of 'Age Delay') will be retrieved."
        + " State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary Node is selected,"
        + " the new node can pick up where the previous node left off, without duplicating the data.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer."),
        @WritesAttribute(attribute = "record.count", description = "Sets the number of records in the FlowFile."),
        @WritesAttribute(attribute = "total.record.count", description = "Sets the total number of records in the FlowFile.")
})
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
@SeeAlso(PutSalesforceObject.class)
public class QuerySalesforceObject extends AbstractProcessor {

    static final AllowableValue PROPERTY_BASED_QUERY = new AllowableValue("property-based-query", "Property Based Query", "Provide query by properties.");
    static final AllowableValue CUSTOM_QUERY = new AllowableValue("custom-query", "Custom Query", "Provide custom SOQL query.");

    static final PropertyDescriptor QUERY_TYPE = new PropertyDescriptor.Builder()
            .name("query-type")
            .displayName("Query Type")
            .description("Choose to provide the query by parameters or a full custom query.")
            .required(true)
            .defaultValue(PROPERTY_BASED_QUERY.getValue())
            .allowableValues(PROPERTY_BASED_QUERY, CUSTOM_QUERY)
            .build();

    static final PropertyDescriptor CUSTOM_SOQL_QUERY = new PropertyDescriptor.Builder()
            .name("custom-soql-query")
            .displayName("Custom SOQL Query")
            .description("Specify the SOQL query to run.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(QUERY_TYPE, CUSTOM_QUERY)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor SOBJECT_NAME = new PropertyDescriptor.Builder()
            .name("sobject-name")
            .displayName("sObject Name")
            .description("The Salesforce sObject to be queried")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(QUERY_TYPE, PROPERTY_BASED_QUERY)
            .build();

    static final PropertyDescriptor FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("field-names")
            .displayName("Field Names")
            .description("Comma-separated list of field names requested from the sObject to be queried. When this field is left empty, all fields are queried.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(QUERY_TYPE, PROPERTY_BASED_QUERY)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Service used for writing records returned from the Salesforce REST API")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .dependsOn(QUERY_TYPE, PROPERTY_BASED_QUERY)
            .build();

    static final PropertyDescriptor CREATE_ZERO_RECORD_FILES = new PropertyDescriptor.Builder()
            .name("create-zero-record-files")
            .displayName("Create Zero Record FlowFiles")
            .description("Specifies whether or not to create a FlowFile when the Salesforce REST API does not return any records")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .dependsOn(QUERY_TYPE, PROPERTY_BASED_QUERY)
            .build();

    public static final PropertyDescriptor AGE_FIELD = new PropertyDescriptor.Builder()
            .name("age-field")
            .displayName("Age Field")
            .description("The name of a TIMESTAMP field that will be used to filter records using a bounded time window."
                    + "The processor will return only those records with a timestamp value newer than the timestamp recorded after the last processor run."
            )
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(QUERY_TYPE, PROPERTY_BASED_QUERY)
            .build();

    public static final PropertyDescriptor AGE_DELAY = new PropertyDescriptor.Builder()
            .name("age-delay")
            .displayName("Age Delay")
            .description("The ending timestamp of the time window will be adjusted earlier by the amount configured in this property." +
                    " For example, with a property value of 10 seconds, an ending timestamp of 12:30:45 would be changed to 12:30:35.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .dependsOn(AGE_FIELD)
            .dependsOn(QUERY_TYPE, PROPERTY_BASED_QUERY)
            .build();

    public static final PropertyDescriptor INITIAL_AGE_FILTER = new PropertyDescriptor.Builder()
            .name("initial-age-filter")
            .displayName("Initial Age Start Time")
            .description("This property specifies the start time that the processor applies when running the first query.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(AGE_FIELD)
            .dependsOn(QUERY_TYPE, PROPERTY_BASED_QUERY)
            .build();

    static final PropertyDescriptor CUSTOM_WHERE_CONDITION = new PropertyDescriptor.Builder()
            .name("custom-where-condition")
            .displayName("Custom WHERE Condition")
            .description("A custom expression to be added in the WHERE clause of the query")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(QUERY_TYPE, PROPERTY_BASED_QUERY)
            .build();

    static final PropertyDescriptor INCLUDE_DELETED_RECORDS = new PropertyDescriptor.Builder()
            .name("include-deleted-records")
            .displayName("Include Deleted Records")
            .description("If true, the processor will include deleted records (IsDeleted = true) in the query results. When enabled, the processor will use the 'queryAll' API.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .dependsOn(QUERY_TYPE, PROPERTY_BASED_QUERY)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful query.")
            .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The input flowfile gets sent to this relationship when the query succeeds.")
            .autoTerminateDefault(true)
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The input flowfile gets sent to this relationship when the query fails.")
            .autoTerminateDefault(true)
            .build();

    public static final String LAST_AGE_FILTER = "last_age_filter";
    private static final String STARTING_FIELD_NAME = "records";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String TIME_FORMAT = "HH:mm:ss.SSSZ";
    private static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private static final String NEXT_RECORDS_URL = "nextRecordsUrl";
    private static final String TOTAL_SIZE = "totalSize";
    private static final String RECORDS = "records";
    private static final BiPredicate<String, String> CAPTURE_PREDICATE = (fieldName, fieldValue) -> NEXT_RECORDS_URL.equals(fieldName);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFactory JSON_FACTORY = OBJECT_MAPPER.getFactory();
    private static final String TOTAL_RECORD_COUNT_ATTRIBUTE = "total.record.count";
    private static final int MAX_RECORD_COUNT = 2000;
    private static final JsonParserFactory jsonParserFactory = new JsonParserFactory();

    private volatile SalesforceToRecordSchemaConverter salesForceToRecordSchemaConverter;
    private volatile SalesforceRestClient salesforceRestService;
    private volatile boolean resetState = false;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        if (resetState) {
            clearState(context);
            resetState = false;
        }

        salesForceToRecordSchemaConverter = new SalesforceToRecordSchemaConverter(
                DATE_FORMAT,
                DATE_TIME_FORMAT,
                TIME_FORMAT
        );

        String salesforceVersion = context.getProperty(API_VERSION).getValue();
        String instanceUrl = context.getProperty(SALESFORCE_INSTANCE_URL).getValue();
        OAuth2AccessTokenProvider accessTokenProvider = context.getProperty(TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);

        SalesforceConfiguration salesforceConfiguration = SalesforceConfiguration.create(
                instanceUrl,
                salesforceVersion,
                () -> accessTokenProvider.getAccessDetails().getAccessToken(),
                context.getProperty(READ_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue()
        );

        salesforceRestService = new SalesforceRestClient(salesforceConfiguration);
    }

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            SALESFORCE_INSTANCE_URL,
            API_VERSION,
            QUERY_TYPE,
            CUSTOM_SOQL_QUERY,
            SOBJECT_NAME,
            FIELD_NAMES,
            RECORD_WRITER,
            AGE_FIELD,
            INITIAL_AGE_FILTER,
            AGE_DELAY,
            CUSTOM_WHERE_CONDITION,
            INCLUDE_DELETED_RECORDS,
            READ_TIMEOUT,
            CREATE_ZERO_RECORD_FILES,
            TOKEN_PROVIDER
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE,
            REL_ORIGINAL
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        return SalesforceAgeValidator.validate(validationContext, results);
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if ((oldValue != null && !oldValue.equals(newValue))
                && (descriptor.equals(SALESFORCE_INSTANCE_URL)
                || descriptor.equals(QUERY_TYPE)
                || descriptor.equals(SOBJECT_NAME)
                || descriptor.equals(AGE_FIELD)
                || descriptor.equals(INITIAL_AGE_FILTER)
                || descriptor.equals(CUSTOM_WHERE_CONDITION)
                || descriptor.equals(INCLUDE_DELETED_RECORDS))
        ) {
            getLogger().debug("A property that require resetting state was modified - {} oldValue {} newValue {}",
                    descriptor.getDisplayName(), oldValue, newValue);
            resetState = true;
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        boolean isCustomQuery = CUSTOM_QUERY.getValue().equals(context.getProperty(QUERY_TYPE).getValue());
        FlowFile flowFile = session.get();
        if (isCustomQuery) {
            processCustomQuery(context, session, flowFile);
        } else {
            processQuery(context, session, flowFile);
        }
    }

    private void processQuery(ProcessContext context, ProcessSession session, FlowFile originalFlowFile) {
        AtomicReference<String> nextRecordsUrl = new AtomicReference<>();
        String sObject = context.getProperty(SOBJECT_NAME).getValue();
        String fields = context.getProperty(FIELD_NAMES).getValue();
        String customWhereClause = context.getProperty(CUSTOM_WHERE_CONDITION).evaluateAttributeExpressions(originalFlowFile).getValue();
        RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        boolean createZeroRecordFlowFiles = context.getProperty(CREATE_ZERO_RECORD_FILES).asBoolean();
        boolean includeDeletedRecords = context.getProperty(INCLUDE_DELETED_RECORDS).asBoolean();

        StateMap state = getState(session);
        IncrementalContext incrementalContext = new IncrementalContext(context, state);
        SalesforceSchemaHolder salesForceSchemaHolder = getConvertedSalesforceSchema(sObject, fields, includeDeletedRecords);

        if (StringUtils.isBlank(fields)) {
            fields = salesForceSchemaHolder.getSalesforceObject().getFields()
                    .stream()
                    .map(SObjectField::getName)
                    .collect(Collectors.joining(","));
        }

        // Add IsDeleted to fields if Include Deleted Records is true
        if (includeDeletedRecords) {
            List<String> fieldList = Arrays.stream(fields.split("\\s*,\\s*")).collect(Collectors.toList());
            if (fieldList.stream().noneMatch(f -> f.equalsIgnoreCase("IsDeleted"))) {
                fields = fields + ", IsDeleted";
            }
        }

        String querySObject = new SalesforceQueryBuilder(incrementalContext)
                .buildQuery(sObject, fields, customWhereClause);

        AtomicBoolean isOriginalTransferred = new AtomicBoolean(false);
        List<FlowFile> outgoingFlowFiles = new ArrayList<>();
        Map<String, String> originalAttributes = Optional.ofNullable(originalFlowFile)
                .map(FlowFile::getAttributes)
                .orElseGet(HashMap::new);

        long startNanos = System.nanoTime();

        do {
            FlowFile outgoingFlowFile = createOutgoingFlowFile(session, originalFlowFile);
            outgoingFlowFiles.add(outgoingFlowFile);
            Map<String, String> attributes = new HashMap<>(originalAttributes);

            AtomicInteger recordCountHolder = new AtomicInteger();
            try {
                outgoingFlowFile = session.write(outgoingFlowFile, processRecordsCallback(session, nextRecordsUrl, writerFactory, state, incrementalContext,
                        salesForceSchemaHolder, querySObject, originalAttributes, attributes, recordCountHolder, includeDeletedRecords));
                int recordCount = recordCountHolder.get();

                if (createZeroRecordFlowFiles || recordCount != 0) {
                    outgoingFlowFile = session.putAllAttributes(outgoingFlowFile, attributes);

                    session.adjustCounter("Records Processed", recordCount, false);
                    getLogger().info("Successfully written {} records for {}", recordCount, outgoingFlowFile);
                } else {
                    outgoingFlowFiles.remove(outgoingFlowFile);
                    session.remove(outgoingFlowFile);
                }
            } catch (Exception e) {
                if (e.getCause() instanceof IOException) {
                    throw new ProcessException("Couldn't get Salesforce records", e);
                } else if (e.getCause() instanceof SchemaNotFoundException) {
                    handleError(session, originalFlowFile, isOriginalTransferred, outgoingFlowFiles, e, "Couldn't create record writer");
                } else if (e.getCause() instanceof MalformedRecordException) {
                    handleError(session, originalFlowFile, isOriginalTransferred, outgoingFlowFiles, e, "Couldn't read records from input");
                } else {
                    handleError(session, originalFlowFile, isOriginalTransferred, outgoingFlowFiles, e, "Couldn't get Salesforce records");
                }
                break;
            }
        } while (nextRecordsUrl.get() != null);

        transferFlowFiles(session, outgoingFlowFiles, originalFlowFile, isOriginalTransferred, startNanos, sObject);
    }

    private OutputStreamCallback processRecordsCallback(ProcessSession session, AtomicReference<String> nextRecordsUrl, RecordSetWriterFactory writerFactory,
                                                        StateMap state, IncrementalContext incrementalContext, SalesforceSchemaHolder salesForceSchemaHolder,
                                                        String querySObject, Map<String, String> originalAttributes, Map<String, String> attributes,
                                                        AtomicInteger recordCountHolder, boolean includeDeletedRecords) {
        return out -> {
            try {
                handleRecordSet(out, nextRecordsUrl, querySObject, writerFactory, salesForceSchemaHolder, originalAttributes, attributes, recordCountHolder, includeDeletedRecords);

                if (incrementalContext.getAgeFilterUpper() != null) {
                    Map<String, String> newState = new HashMap<>(state.toMap());
                    newState.put(LAST_AGE_FILTER, incrementalContext.getAgeFilterUpper());
                    updateState(session, newState);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void handleRecordSet(OutputStream out, AtomicReference<String> nextRecordsUrl, String querySObject, RecordSetWriterFactory writerFactory,
                                 SalesforceSchemaHolder salesForceSchemaHolder, Map<String, String> originalAttributes, Map<String, String> attributes,
                                 AtomicInteger recordCountHolder, boolean includeDeletedRecords) throws Exception {
        try (
                InputStream querySObjectResultInputStream = getResultInputStream(nextRecordsUrl.get(), querySObject, includeDeletedRecords);
                JsonTreeRowRecordReader jsonReader = createJsonReader(querySObjectResultInputStream, salesForceSchemaHolder.getRecordSchema());
                RecordSetWriter writer = createRecordSetWriter(writerFactory, originalAttributes, out, salesForceSchemaHolder.getRecordSchema())
        ) {
            writer.beginRecordSet();

            Record querySObjectRecord;
            while ((querySObjectRecord = jsonReader.nextRecord()) != null) {
                writer.write(querySObjectRecord);
            }

            WriteResult writeResult = writer.finishRecordSet();

            Map<String, String> capturedFields = jsonReader.getCapturedFields();
            nextRecordsUrl.set(capturedFields.getOrDefault(NEXT_RECORDS_URL, null));

            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
            attributes.putAll(writeResult.getAttributes());
            recordCountHolder.set(writeResult.getRecordCount());
        }
    }

    private JsonTreeRowRecordReader createJsonReader(InputStream querySObjectResultInputStream, RecordSchema recordSchema) throws IOException, MalformedRecordException {
        return new JsonTreeRowRecordReader(
                querySObjectResultInputStream,
                getLogger(),
                recordSchema,
                DATE_FORMAT,
                TIME_FORMAT,
                DATE_TIME_FORMAT,
                StartingFieldStrategy.NESTED_FIELD,
                STARTING_FIELD_NAME,
                SchemaApplicationStrategy.SELECTED_PART,
                CAPTURE_PREDICATE,
                jsonParserFactory
        );
    }

    private RecordSetWriter createRecordSetWriter(RecordSetWriterFactory writerFactory, Map<String, String> originalAttributes, OutputStream out,
                                                  RecordSchema recordSchema) throws IOException, SchemaNotFoundException {
        return writerFactory.createWriter(
                getLogger(),
                writerFactory.getSchema(
                        originalAttributes,
                        recordSchema
                ),
                out,
                originalAttributes
        );
    }

    private void processCustomQuery(ProcessContext context, ProcessSession session, FlowFile originalFlowFile) {
        String customQuery = context.getProperty(CUSTOM_SOQL_QUERY).evaluateAttributeExpressions(originalFlowFile).getValue();
        AtomicReference<String> nextRecordsUrl = new AtomicReference<>();
        AtomicReference<String> totalSize = new AtomicReference<>();
        AtomicBoolean isOriginalTransferred = new AtomicBoolean(false);
        List<FlowFile> outgoingFlowFiles = new ArrayList<>();
        long startNanos = System.nanoTime();
        boolean includeDeletedRecords = context.getProperty(INCLUDE_DELETED_RECORDS).asBoolean();
        do {
            try (InputStream response = getResultInputStream(nextRecordsUrl.get(), customQuery, includeDeletedRecords)) {
                FlowFile outgoingFlowFile = createOutgoingFlowFile(session, originalFlowFile);
                outgoingFlowFiles.add(outgoingFlowFile);
                outgoingFlowFile = session.write(outgoingFlowFile, parseCustomQueryResponse(response, nextRecordsUrl, totalSize));
                int recordCount = nextRecordsUrl.get() != null ? MAX_RECORD_COUNT : Integer.parseInt(totalSize.get()) % MAX_RECORD_COUNT;
                Map<String, String> attributes = new HashMap<>();
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                attributes.put(TOTAL_RECORD_COUNT_ATTRIBUTE, String.valueOf(recordCount));
                session.adjustCounter("Salesforce records processed", recordCount, false);
                outgoingFlowFile = session.putAllAttributes(outgoingFlowFile, attributes);
            } catch (IOException e) {
                throw new ProcessException("Couldn't get Salesforce records", e);
            } catch (Exception e) {
                handleError(session, originalFlowFile, isOriginalTransferred, outgoingFlowFiles, e, "Couldn't get Salesforce records");
                break;
            }
        } while (nextRecordsUrl.get() != null);

        transferFlowFiles(session, outgoingFlowFiles, originalFlowFile, isOriginalTransferred, startNanos, "custom");
    }

    private void transferFlowFiles(ProcessSession session, List<FlowFile> outgoingFlowFiles, FlowFile originalFlowFile, AtomicBoolean isOriginalTransferred,
                                   long startNanos, String urlDetail) {
        if (!outgoingFlowFiles.isEmpty()) {
            session.transfer(outgoingFlowFiles, REL_SUCCESS);
            long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            outgoingFlowFiles.forEach(ff ->
                    session.getProvenanceReporter().receive(ff, salesforceRestService.getVersionedBaseUrl() + "/" + urlDetail, transferMillis)
            );
        }
        if (originalFlowFile != null && !isOriginalTransferred.get()) {
            session.transfer(originalFlowFile, REL_ORIGINAL);
        }
    }

    private FlowFile createOutgoingFlowFile(ProcessSession session, FlowFile originalFlowFile) {
        if (originalFlowFile != null) {
            return session.create(originalFlowFile);
        } else {
            return session.create();
        }
    }

    private OutputStreamCallback parseCustomQueryResponse(InputStream in, AtomicReference<String> nextRecordsUrl, AtomicReference<String> totalSize) {
        nextRecordsUrl.set(null);
        return out -> {
            try (JsonParser jsonParser = JSON_FACTORY.createParser(in);
                 JsonGenerator jsonGenerator = JSON_FACTORY.createGenerator(out, JsonEncoding.UTF8)) {
                while (jsonParser.nextToken() != null) {
                    if (nextTokenIs(jsonParser, TOTAL_SIZE)) {
                        totalSize.set(jsonParser.getValueAsString());
                    } else if (nextTokenIs(jsonParser, NEXT_RECORDS_URL)) {
                        nextRecordsUrl.set(jsonParser.getValueAsString());
                    } else if (nextTokenIs(jsonParser, RECORDS)) {
                        jsonGenerator.copyCurrentStructure(jsonParser);
                    }
                }
            }
        };
    }

    private boolean nextTokenIs(JsonParser jsonParser, String value) throws IOException {
        return jsonParser.getCurrentToken() == JsonToken.FIELD_NAME && jsonParser.currentName()
                .equals(value) && jsonParser.nextToken() != null;
    }

    private InputStream getResultInputStream(String nextRecordsUrl, String querySObject, boolean includeDeletedRecords) {
        if (nextRecordsUrl == null) {
            if (includeDeletedRecords) {
                return salesforceRestService.queryAll(querySObject);
            } else {
                return salesforceRestService.query(querySObject);
            }
        }
        return salesforceRestService.getNextRecords(nextRecordsUrl);
    }

    private SalesforceSchemaHolder getConvertedSalesforceSchema(String sObject, String fields, boolean includeDeletedRecords) {
        try (InputStream describeSObjectResult = salesforceRestService.describeSObject(sObject)) {
            return convertSchema(describeSObjectResult, fields);
        } catch (IOException e) {
            throw new UncheckedIOException("Salesforce input stream close failed", e);
        }
    }

    private void handleError(ProcessSession session, FlowFile originalFlowFile, AtomicBoolean isOriginalTransferred, List<FlowFile> outgoingFlowFiles,
                             Exception e, String errorMessage) {
        if (originalFlowFile != null) {
            session.transfer(originalFlowFile, REL_FAILURE);
            isOriginalTransferred.set(true);
        }
        getLogger().error(errorMessage, e);
        session.remove(outgoingFlowFiles);
        outgoingFlowFiles.clear();
    }

    private StateMap getState(ProcessSession session) {
        StateMap state;
        try {
            state = session.getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("State retrieval failed", e);
        }
        return state;
    }

    private void updateState(ProcessSession session, Map<String, String> newState) {
        try {
            session.setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Last Age Filter state update failed", e);
        }
    }

    private void clearState(ProcessContext context) {
        try {
            getLogger().debug("Clearing state based on property modifications");
            context.getStateManager().clear(Scope.CLUSTER);
        } catch (final IOException e) {
            getLogger().warn("Failed to clear state", e);
        }
    }

    protected SalesforceSchemaHolder convertSchema(InputStream describeSObjectResult, String fieldsOfInterest) {
        try {
            SObjectDescription salesforceObject = salesForceToRecordSchemaConverter.getSalesforceObject(describeSObjectResult);
            RecordSchema recordSchema = salesForceToRecordSchemaConverter.convertSchema(salesforceObject, fieldsOfInterest);

            RecordSchema querySObjectResultSchema = new SimpleRecordSchema(Collections.singletonList(
                    new RecordField(STARTING_FIELD_NAME, RecordFieldType.ARRAY.getArrayDataType(
                            RecordFieldType.RECORD.getRecordDataType(recordSchema)
                    ))
            ));

            return new SalesforceSchemaHolder(querySObjectResultSchema, recordSchema, salesforceObject);
        } catch (IOException e) {
            throw new ProcessException("SObject to Record schema conversion failed", e);
        }
    }
}
