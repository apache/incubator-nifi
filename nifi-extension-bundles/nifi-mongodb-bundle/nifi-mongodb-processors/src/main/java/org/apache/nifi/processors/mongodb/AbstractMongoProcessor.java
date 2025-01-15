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
package org.apache.nifi.processors.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class AbstractMongoProcessor extends AbstractProcessor {
    public static final String ATTRIBUTE_MONGODB_UPDATE_MODE = "mongodb.update.mode";

    protected static final String JSON_TYPE_EXTENDED = "Extended";
    protected static final String JSON_TYPE_STANDARD   = "Standard";
    protected static final AllowableValue JSON_EXTENDED = new AllowableValue(JSON_TYPE_EXTENDED, "Extended JSON",
            "Use MongoDB's \"extended JSON\". This is the JSON generated with toJson() on a MongoDB Document from the Java driver");
    protected static final AllowableValue JSON_STANDARD = new AllowableValue(JSON_TYPE_STANDARD, "Standard JSON",
            "Generate a JSON document that conforms to typical JSON conventions instead of Mongo-specific conventions.");

    static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("mongo-client-service")
        .displayName("Client Service")
        .description("If configured, this property will use the assigned client service for connection pooling.")
        .required(false)
        .identifiesControllerService(MongoDBClientService.class)
        .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
        .name("Mongo Database Name")
        .displayName("Mongo Database Name")
        .description("The name of the database to use")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
        .name("Mongo Collection Name")
        .description("The name of the collection to use")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected static final PropertyDescriptor JSON_TYPE = new PropertyDescriptor.Builder()
            .allowableValues(JSON_EXTENDED, JSON_STANDARD)
            .defaultValue(JSON_TYPE_EXTENDED)
            .displayName("JSON Type")
            .name("json-type")
            .description("By default, MongoDB's Java driver returns \"extended JSON\". Some of the features of this variant of JSON" +
                    " may cause problems for other JSON parsers that expect only standard JSON types and conventions. This configuration setting " +
                    " controls whether to use extended JSON or provide a clean view that conforms to standard JSON.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    static final PropertyDescriptor RESULTS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("results-per-flowfile")
            .displayName("Results Per FlowFile")
            .description("How many results to put into a flowfile at once. The whole body will be treated as a JSON array of results.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("The number of elements returned from the server in one batch.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    static final PropertyDescriptor QUERY_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("mongo-query-attribute")
            .displayName("Query Output Attribute")
            .description("If set, the query will be written to a specified attribute on the output flowfiles.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .required(false)
            .build();
    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("mongo-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor DATE_FORMAT = new PropertyDescriptor.Builder()
        .name("mongo-date-format")
        .displayName("Date Format")
        .description("The date format string to use for formatting Date fields that are returned from Mongo. It is only " +
                "applied when the JSON output format is set to Standard JSON.")
        .defaultValue("yyyy-MM-dd'T'HH:mm:ss'Z'")
        .addValidator((subject, input, context) -> {
            ValidationResult.Builder result = new ValidationResult.Builder()
                .subject(subject)
                .input(input);
            try {
                new SimpleDateFormat(input).format(new Date());
                result.valid(true);
            } catch (Exception ex) {
                result.valid(false)
                    .explanation(ex.getMessage());
            }

            return result.build();
        })
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CLIENT_SERVICE,
            DATABASE_NAME,
            COLLECTION_NAME
    );

    public enum UpdateMethod implements DescribedValue {
        UPDATE_ONE("one", "Update One", "Updates only the first document that matches the query."),
        UPDATE_MANY("many", "Update Many", "Updates every document that matches the query."),
        UPDATE_FF_ATTRIBUTE("flowfile-attribute", "Use '" + ATTRIBUTE_MONGODB_UPDATE_MODE + "' FlowFile attribute.",
            "Use the value of the '" + ATTRIBUTE_MONGODB_UPDATE_MODE + "' attribute of the incoming FlowFile. Acceptable values are 'one' and 'many'.");
        private final String value;
        private final String displayName;
        private final String description;

        UpdateMethod(final String value, final String displayName, final String description) {
            this.value = value;
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    protected ObjectMapper objectMapper;
    protected MongoClient mongoClient;
    protected MongoDBClientService clientService;

    protected static List<PropertyDescriptor> getCommonPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public final void createClient(ProcessContext context) {
        clientService = context.getProperty(CLIENT_SERVICE).asControllerService(MongoDBClientService.class);
    }

    protected MongoClientSettings.Builder getClientSettings(final String uri, final SSLContext sslContext) {
        final MongoClientSettings.Builder builder = MongoClientSettings.builder();
        builder.applyConnectionString(new ConnectionString(uri));
        if (sslContext != null) {
            builder.applyToSslSettings(sslBuilder ->
                    sslBuilder.enabled(true).context(sslContext)
            );
        }
        return builder;
    }

    @OnStopped
    public final void closeClient() {
        if (mongoClient != null) {
            getLogger().info("Closing MongoClient");
            mongoClient.close();
            mongoClient = null;
        }
    }

    protected MongoDatabase getDatabase(final ProcessContext context, final FlowFile flowFile) {
        final String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();

        return clientService != null ? clientService.getDatabase(databaseName) : mongoClient.getDatabase(databaseName);
    }

    protected MongoCollection<Document> getCollection(final ProcessContext context, final FlowFile flowFile) {
        final String collectionName = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(collectionName)) {
            throw new ProcessException("Collection name was empty after expression language evaluation.");
        }
        return getDatabase(context, flowFile).getCollection(collectionName);
    }

    protected String getURI(final ProcessContext context) {
        return clientService.getURI();
    }

    protected void writeBatch(String payload, FlowFile parent, ProcessContext context, ProcessSession session,
                              Map<String, String> extraAttributes, Relationship rel) throws UnsupportedEncodingException {
        String charset = context.getProperty(CHARSET).evaluateAttributeExpressions(parent).getValue();

        FlowFile flowFile = parent != null ? session.create(parent) : session.create();
        flowFile = session.importFrom(new ByteArrayInputStream(payload.getBytes(charset)), flowFile);
        flowFile = session.putAllAttributes(flowFile, extraAttributes);
        if (parent == null) {
            session.getProvenanceReporter().receive(flowFile, getURI(context));
        }
        session.transfer(flowFile, rel);
    }

    protected synchronized void configureMapper(String setting, String dateFormat) {
        objectMapper = new ObjectMapper();

        if (setting.equals(JSON_TYPE_STANDARD)) {
            objectMapper.registerModule(ObjectIdSerializer.getModule());
            DateFormat df = new SimpleDateFormat(dateFormat);
            objectMapper.setDateFormat(df);
        }
    }

    /**
     * Checks if given update mode option matches for the incoming flow file
     * @param updateMethodToMatch the value against which processor's mode is compared
     * @param configuredUpdateMethod the value coming from running processor
     * @param flowFile incoming flow file to extract processor mode
     * @return true if the incoming files update mode matches with updateMethodToMatch
     */
    protected boolean updateModeMatches(
        UpdateMethod updateMethodToMatch, UpdateMethod configuredUpdateMethod, FlowFile flowFile) {

        return updateMethodToMatch == configuredUpdateMethod
            || (UpdateMethod.UPDATE_FF_ATTRIBUTE == configuredUpdateMethod
                    && updateMethodToMatch.getValue().equalsIgnoreCase(flowFile.getAttribute(ATTRIBUTE_MONGODB_UPDATE_MODE)));
    }

}
