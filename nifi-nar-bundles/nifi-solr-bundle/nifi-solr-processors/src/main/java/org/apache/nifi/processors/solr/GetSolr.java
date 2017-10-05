/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.solr;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.util.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;

@Tags({"Apache", "Solr", "Get", "Pull", "Records"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Queries Solr and outputs the results as a FlowFile")
@Stateful(scopes = {Scope.LOCAL}, description = "Stores latest date of Date Field or greatest value of field _version_ so that the same data will not be fetched multiple times.")
public class GetSolr extends SolrProcessor {

    public static final String STATE_MANAGER_FILTER = "stateManager_filter";
    public static final String STATE_MANAGER_CURSOR_MARK = "stateManager_cursorMark";

    public static final AllowableValue MODE_XML = new AllowableValue("XML");
    public static final AllowableValue MODE_REC = new AllowableValue("Records");

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor
            .Builder().name("Return Type")
            .description("Write Solr documents to FlowFiles as XML or using a Record Writer")
            .required(true)
            .allowableValues(MODE_XML, MODE_REC)
            .defaultValue(MODE_REC.getValue())
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor
            .Builder().name("Record Writer")
            .description("The Record Writer to use in order to write Solr documents to FlowFiles. Must be set if \"Records\" is used as return type.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(false)
            .required(false)
            .build();

    public static final PropertyDescriptor SOLR_QUERY = new PropertyDescriptor
            .Builder().name("Solr Query")
            .description("A query to execute against Solr")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATE_FIELD = new PropertyDescriptor
            .Builder().name("Date Field")
            .description("The name of a date field in Solr used to filter results")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ID_FIELD = new PropertyDescriptor
            .Builder().name("Id Field")
            .description("The name of the unique field in the Solr collection")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RETURN_FIELDS = new PropertyDescriptor
            .Builder().name("Return Fields")
            .description("Comma-separated list of field names to return")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
            .Builder().name("Batch Size")
            .description("Number of rows per Solr query")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The results of querying Solr")
            .build();

    private final AtomicBoolean clearState = new AtomicBoolean(false);
    private final AtomicBoolean dateFieldNotInSpecifiedFieldsList = new AtomicBoolean(false);

    private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
    static {
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SOLR_TYPE);
        descriptors.add(SOLR_LOCATION);
        descriptors.add(COLLECTION);
        descriptors.add(RETURN_TYPE);
        descriptors.add(RECORD_WRITER);
        descriptors.add(SOLR_QUERY);
        descriptors.add(DATE_FIELD);
        descriptors.add(ID_FIELD);
        descriptors.add(RETURN_FIELDS);
        descriptors.add(BATCH_SIZE);
        descriptors.add(JAAS_CLIENT_APP_NAME);
        descriptors.add(BASIC_USERNAME);
        descriptors.add(BASIC_PASSWORD);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(SOLR_SOCKET_TIMEOUT);
        descriptors.add(SOLR_CONNECTION_TIMEOUT);
        descriptors.add(SOLR_MAX_CONNECTIONS);
        descriptors.add(SOLR_MAX_CONNECTIONS_PER_HOST);
        descriptors.add(ZK_CLIENT_TIMEOUT);
        descriptors.add(ZK_CONNECTION_TIMEOUT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        clearState.set(true);
    }

    @OnScheduled
    public void onScheduled2(final ProcessContext context) throws IOException {
        if (clearState.getAndSet(false)) {
            context.getStateManager().clear(Scope.LOCAL);
            final Map<String,String> newStateMap = new HashMap<String,String>();

            newStateMap.put(STATE_MANAGER_CURSOR_MARK, "*");
            newStateMap.put(STATE_MANAGER_FILTER, "*");

            context.getStateManager().setState(newStateMap, Scope.LOCAL);
        }
    }

    @Override
    protected final Collection<ValidationResult> customValidate(ValidationContext context) {
        final Collection<ValidationResult> problems = new ArrayList<>();

        if (context.getProperty(RETURN_TYPE).evaluateAttributeExpressions().getValue().equals(MODE_REC.getValue()) &&
                !context.getProperty(RECORD_WRITER).isSet()) {
            problems.add(new ValidationResult.Builder()
                    .explanation("for parsing records a record writer has to be configured")
                    .valid(false)
                    .subject("Record writer check")
                    .build());
        }
        problems.addAll(super.customValidate(context));
        return problems;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        final AtomicBoolean continuePaging = new AtomicBoolean(true);
        final SolrQuery solrQuery = new SolrQuery();

        try {
            final String dateField = context.getProperty(DATE_FIELD).getValue();

            solrQuery.setQuery("*:*");
            final String query = context.getProperty(SOLR_QUERY).getValue();
            if (!StringUtils.isBlank(query) && !query.equals("*:*")) {
                solrQuery.addFilterQuery(query);
            }
            final StringBuilder automatedFilterQuery = (new StringBuilder())
                    .append(dateField)
                    .append(":[")
                    .append(context.getStateManager().getState(Scope.LOCAL).get(STATE_MANAGER_FILTER))
                    .append(" TO *]");
            solrQuery.addFilterQuery(automatedFilterQuery.toString());

            final List<String> fieldList = new ArrayList<String>();
            final String returnFields = context.getProperty(RETURN_FIELDS).getValue();
            if (!StringUtils.isBlank(returnFields)) {
                fieldList.addAll(Arrays.asList(returnFields.trim().split("[,]")));
                if (!fieldList.contains(dateField)) {
                    fieldList.add(dateField);
                    dateFieldNotInSpecifiedFieldsList.set(true);
                }
                for (String returnField : fieldList) {
                    solrQuery.addField(returnField.trim());
                }
            }

            solrQuery.setParam(CursorMarkParams.CURSOR_MARK_PARAM, context.getStateManager().getState(Scope.LOCAL).get(STATE_MANAGER_CURSOR_MARK));
            solrQuery.setRows(context.getProperty(BATCH_SIZE).asInteger());

            final StringBuilder sortClause = (new StringBuilder())
                    .append(dateField)
                    .append(" asc,")
                    .append(context.getProperty(ID_FIELD).getValue())
                    .append(" asc");
            solrQuery.setParam("sort", sortClause.toString());

            while (continuePaging.get()) {
                final QueryRequest req = new QueryRequest(solrQuery);
                if (isBasicAuthEnabled()) {
                    req.setBasicAuthCredentials(getUsername(), getPassword());
                }

                logger.debug(solrQuery.toQueryString());
                final QueryResponse response = req.process(getSolrClient());
                final SolrDocumentList documentList = response.getResults();

                if (response.getResults().size() > 0) {
                    final SolrDocument lastSolrDocument = documentList.get(response.getResults().size()-1);
                    final String latestDateValue = df.format(lastSolrDocument.get(dateField));

                    solrQuery.setParam(CursorMarkParams.CURSOR_MARK_PARAM, response.getNextCursorMark());
                    final Map<String,String> updateStateManager = new HashMap<String,String>();
                    updateStateManager.putAll(context.getStateManager().getState(Scope.LOCAL).toMap());
                    updateStateManager.put(STATE_MANAGER_CURSOR_MARK, response.getNextCursorMark());
                    updateStateManager.put(STATE_MANAGER_FILTER, latestDateValue);
                    context.getStateManager().setState(updateStateManager, Scope.LOCAL);

                    FlowFile flowFile = session.create();
                    flowFile = session.putAttribute(flowFile, "solrQuery", solrQuery.toString());

                    if (context.getProperty(RETURN_TYPE).getValue().equals(MODE_XML.getValue())){
                        if (dateFieldNotInSpecifiedFieldsList.get()) {
                            for (SolrDocument doc : response.getResults()) {
                                doc.removeFields(dateField);
                            }
                        }
                        flowFile = session.write(flowFile, new QueryResponseOutputStreamCallback(response));
                        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/xml");

                    } else {
                        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
                        final RecordSchema schema = writerFactory.getSchema(null, null);
                        final RecordSet recordSet = solrDocumentsToRecordSet(response.getResults(), schema);
                        final StringBuffer mimeType = new StringBuffer();
                        flowFile = session.write(flowFile, new OutputStreamCallback() {
                            @Override
                            public void process(final OutputStream out) throws IOException {
                                try {
                                    final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, out);
                                    writer.write(recordSet);
                                    writer.flush();
                                    mimeType.append(writer.getMimeType());
                                } catch (SchemaNotFoundException e) {
                                    throw new ProcessException("Could not parse Solr response", e);
                                }
                            }
                        });
                        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeType.toString());
                    }
                    session.transfer(flowFile, REL_SUCCESS);
                }
                continuePaging.set(response.getResults().size() == Integer.parseInt(context.getProperty(BATCH_SIZE).getValue()));
            }
        } catch(SolrServerException | SchemaNotFoundException | IOException e){
            context.yield();
            session.rollback();
            logger.error("Failed to execute query {} due to {}", new Object[]{solrQuery.toString(), e}, e);
            throw new ProcessException(e);
        } catch( final Throwable t){
            context.yield();
            session.rollback();
            logger.error("Failed to execute query {} due to {}", new Object[]{solrQuery.toString(), t}, t);
            throw t;
        }
    }

    /**
     * Writes each SolrDocument to a record.
     */
    private RecordSet solrDocumentsToRecordSet(final List<SolrDocument> docs, final RecordSchema schema) {
        final List<Record> lr = new ArrayList<Record>();

        for (SolrDocument doc : docs) {
            final Map<String, Object> recordValues = new LinkedHashMap<>();
            for (RecordField field : schema.getFields()){
                final Object fieldValue = doc.getFieldValue(field.getFieldName());
                if (fieldValue != null) {
                    if (field.getDataType().getFieldType().equals(RecordFieldType.ARRAY)){
                        recordValues.put(field.getFieldName(), ((List<Object>) fieldValue).toArray());
                    } else {
                        recordValues.put(field.getFieldName(), fieldValue);
                    }
                }
            }
            lr.add(new MapRecord(schema, recordValues));
        }
        return new ListRecordSet(schema, lr);
    }

    /**
     * Writes each SolrDocument in XML format to the OutputStream.
     */
    private class QueryResponseOutputStreamCallback implements OutputStreamCallback {
        private QueryResponse response;

        public QueryResponseOutputStreamCallback(QueryResponse response) {
            this.response = response;
        }

        @Override
        public void process(OutputStream out) throws IOException {
            for (SolrDocument doc : response.getResults()) {
                String xml = ClientUtils.toXML(toSolrInputDocument(doc));
                IOUtils.write(xml, out, StandardCharsets.UTF_8);
            }
        }

        public SolrInputDocument toSolrInputDocument(SolrDocument d) {
            SolrInputDocument doc = new SolrInputDocument();

            for (String name : d.getFieldNames()) {
                doc.addField(name, d.getFieldValue(name));
            }

            return doc;
        }
    }
}
