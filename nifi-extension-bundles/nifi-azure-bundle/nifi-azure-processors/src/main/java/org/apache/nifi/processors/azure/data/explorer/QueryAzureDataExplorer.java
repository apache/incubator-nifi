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
package org.apache.nifi.processors.azure.data.explorer;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.services.azure.data.explorer.KustoQueryResponse;
import org.apache.nifi.services.azure.data.explorer.KustoQueryService;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Azure", "Data", "Explorer", "ADX", "Kusto"})
@CapabilityDescription("Query Azure Data Explorer and stream JSON results to output FlowFiles")
@WritesAttributes({
        @WritesAttribute(attribute = QueryAzureDataExplorer.QUERY_ERROR_MESSAGE, description = "Azure Data Explorer query error message on failures"),
        @WritesAttribute(attribute = QueryAzureDataExplorer.QUERY_EXECUTED, description = "Azure Data Explorer query executed"),
        @WritesAttribute(attribute = "mime.type", description = "Content Type set to application/json")
})
public class QueryAzureDataExplorer extends AbstractProcessor {
    public static final String QUERY_ERROR_MESSAGE = "query.error.message";

    public static final String QUERY_EXECUTED = "query.executed";

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing results of a successful Query")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles containing original input associated with a failed Query")
            .build();

    public static final PropertyDescriptor KUSTO_QUERY_SERVICE = new PropertyDescriptor.Builder()
            .name("Kusto Query Service")
            .displayName("Kusto Query Service")
            .description("Azure Data Explorer Kusto Query Service")
            .required(true)
            .identifiesControllerService(KustoQueryService.class)
            .build();

    public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Database Name")
            .displayName("Database Name")
            .description("Azure Data Explorer Database Name for querying")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("Query")
            .displayName("Query")
            .description("Query to be run against Azure Data Explorer")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    protected static final String APPLICATION_JSON = "application/json";

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS,
            FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            KUSTO_QUERY_SERVICE,
            DATABASE_NAME,
            QUERY
    );

    private volatile KustoQueryService service;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        service = context.getProperty(KUSTO_QUERY_SERVICE).asControllerService(KustoQueryService.class);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile).getValue();
        final String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();

        try {
            flowFile = session.putAttribute(flowFile, QUERY_EXECUTED, query);

            final KustoQueryResponse kustoQueryResponse = executeQuery(databaseName, query);
            if (kustoQueryResponse.isError()) {
                getLogger().error("Query failed: {}", kustoQueryResponse.getErrorMessage());
                flowFile = session.putAttribute(flowFile, QUERY_ERROR_MESSAGE, kustoQueryResponse.getErrorMessage());
                session.transfer(flowFile, FAILURE);
            } else {
                try (final InputStream responseStream = kustoQueryResponse.getResponseStream()) {
                    flowFile = session.importFrom(responseStream, flowFile);
                    flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
                    session.transfer(flowFile, SUCCESS);
                }
            }
        } catch (final Exception e) {
            getLogger().error("Query failed", e);
            flowFile = session.putAttribute(flowFile, QUERY_ERROR_MESSAGE, e.getMessage());
            session.transfer(flowFile, FAILURE);
        }
    }

    protected KustoQueryResponse executeQuery(String databaseName, String adxQuery) {
        return service.executeQuery(databaseName, adxQuery);
    }
}
