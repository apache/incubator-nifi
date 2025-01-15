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

package org.apache.nifi.processors.workday;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.URL_VALIDATOR;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

@Tags({"Workday", "report"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("A processor which can interact with a configurable Workday Report. The processor can forward the content without modification, or you can transform it by"
    + " providing the specific Record Reader and Record Writer services based on your needs. You can also remove fields by defining schema in the Record Writer. "
    + "Supported Workday report formats are: csv, simplexml, json")
@SideEffectFree
@SupportsBatching
@WritesAttributes({
    @WritesAttribute(attribute = GetWorkdayReport.GET_WORKDAY_REPORT_JAVA_EXCEPTION_CLASS, description = "The Java exception class raised when the processor fails"),
    @WritesAttribute(attribute = GetWorkdayReport.GET_WORKDAY_REPORT_JAVA_EXCEPTION_MESSAGE, description = "The Java exception message raised when the processor fails"),
    @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Source / Record Writer"),
    @WritesAttribute(attribute = GetWorkdayReport.RECORD_COUNT, description = "The number of records in an outgoing FlowFile. This is only populated on the 'success' relationship "
        + "when Record Reader and Writer is set.")})
public class GetWorkdayReport extends AbstractProcessor {

    protected static final String STATUS_CODE = "getworkdayreport.status.code";
    protected static final String REQUEST_URL = "getworkdayreport.request.url";
    protected static final String REQUEST_DURATION = "getworkdayreport.request.duration";
    protected static final String TRANSACTION_ID = "getworkdayreport.tx.id";
    protected static final String GET_WORKDAY_REPORT_JAVA_EXCEPTION_CLASS = "getworkdayreport.java.exception.class";
    protected static final String GET_WORKDAY_REPORT_JAVA_EXCEPTION_MESSAGE = "getworkdayreport.java.exception.message";
    protected static final String RECORD_COUNT = "record.count";
    protected static final String BASIC_PREFIX = "Basic ";
    protected static final String HEADER_AUTHORIZATION = "Authorization";
    protected static final String HEADER_CONTENT_TYPE = "Content-Type";
    protected static final String USERNAME_PASSWORD_SEPARATOR = ":";

    protected static final PropertyDescriptor REPORT_URL = new PropertyDescriptor.Builder()
        .name("Workday Report URL")
        .displayName("Workday Report URL")
        .description("HTTP remote URL of Workday report including a scheme of http or https, as well as a hostname or IP address with optional port and path elements.")
        .required(true)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .addValidator(URL_VALIDATOR)
        .build();

    protected static final PropertyDescriptor WORKDAY_USERNAME = new PropertyDescriptor.Builder()
        .name("Workday Username")
        .displayName("Workday Username")
        .description("The username provided for authentication of Workday requests. Encoded using Base64 for HTTP Basic Authentication as described in RFC 7617.")
        .required(true)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x39\\x3b-\\x7e\\x80-\\xff]+$")))
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();

    protected static final PropertyDescriptor WORKDAY_PASSWORD = new PropertyDescriptor.Builder()
        .name("Workday Password")
        .displayName("Workday Password")
        .description("The password provided for authentication of Workday requests. Encoded using Base64 for HTTP Basic Authentication as described in RFC 7617.")
        .required(true)
        .sensitive(true)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x7e\\x80-\\xff]+$")))
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .build();

    protected static final PropertyDescriptor WEB_CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("Web Client Service Provider")
        .description("Web client which is used to communicate with the Workday API.")
        .required(true)
        .identifiesControllerService(WebClientServiceProvider.class)
        .build();

    protected static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(false)
        .build();

    protected static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("The Record Writer to use for serializing Records to an output FlowFile.")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .dependsOn(RECORD_READER_FACTORY)
        .required(true)
        .build();

    protected static final Relationship ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("Request FlowFiles transferred when receiving HTTP responses with a status code between 200 and 299.")
        .autoTerminateDefault(true)
        .build();

    protected static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Request FlowFiles transferred when receiving socket communication errors.")
        .build();

    protected static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Response FlowFiles transferred when receiving HTTP responses with a status code between 200 and 299.")
        .build();

    protected static final Set<Relationship> RELATIONSHIPS = Set.of(
            ORIGINAL,
            SUCCESS,
            FAILURE
    );

    protected static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            REPORT_URL,
            WORKDAY_USERNAME,
            WORKDAY_PASSWORD,
            WEB_CLIENT_SERVICE,
            RECORD_READER_FACTORY,
            RECORD_WRITER_FACTORY
    );

    private final AtomicReference<WebClientService> webClientReference = new AtomicReference<>();
    private final AtomicReference<RecordReaderFactory> recordReaderFactoryReference = new AtomicReference<>();
    private final AtomicReference<RecordSetWriterFactory> recordSetWriterFactoryReference = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void setUpClient(final ProcessContext context)  {
        WebClientServiceProvider standardWebClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE).asControllerService(WebClientServiceProvider.class);
        RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
        RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        WebClientService webClientService = standardWebClientServiceProvider.getWebClientService();
        webClientReference.set(webClientService);
        recordReaderFactoryReference.set(recordReaderFactory);
        recordSetWriterFactoryReference.set(recordSetWriterFactory);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (skipExecution(context, flowFile)) {
            return;
        }

        FlowFile responseFlowFile = null;

        try {
            WebClientService webClientService = webClientReference.get();
            URI uri = new URI(context.getProperty(REPORT_URL).evaluateAttributeExpressions(flowFile).getValue().trim());
            long startNanos = System.nanoTime();
            String authorization = createAuthorizationHeader(context, flowFile);

            try (HttpResponseEntity httpResponseEntity = webClientService.get()
                .uri(uri)
                .header(HEADER_AUTHORIZATION, authorization)
                .retrieve()) {
                responseFlowFile = createResponseFlowFile(flowFile, session, httpResponseEntity);
                long elapsedTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                Map<String, String> commonAttributes = createCommonAttributes(uri, httpResponseEntity, elapsedTime);

                if (flowFile != null) {
                    flowFile = session.putAllAttributes(flowFile, setMimeType(commonAttributes, httpResponseEntity));
                }
                if (responseFlowFile != null) {
                    responseFlowFile = session.putAllAttributes(responseFlowFile, commonAttributes);
                    if (flowFile == null) {
                        session.getProvenanceReporter().receive(responseFlowFile, uri.toString(), elapsedTime);
                    } else {
                        session.getProvenanceReporter().fetch(responseFlowFile, uri.toString(), elapsedTime);
                    }
                }

                route(flowFile, responseFlowFile, session, context, httpResponseEntity.statusCode());
            }
        } catch (Exception e) {
            if (flowFile == null) {
                getLogger().error("Request Processing failed", e);
                context.yield();
            } else {
                getLogger().error("Request Processing failed: {}", flowFile, e);
                session.penalize(flowFile);
                flowFile = session.putAttribute(flowFile, GET_WORKDAY_REPORT_JAVA_EXCEPTION_CLASS, e.getClass().getSimpleName());
                flowFile = session.putAttribute(flowFile, GET_WORKDAY_REPORT_JAVA_EXCEPTION_MESSAGE, e.getMessage());
                session.transfer(flowFile, FAILURE);
            }

            if (responseFlowFile != null) {
                session.remove(responseFlowFile);
            }
        }
    }

    /*
     *  If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
     *  However, if we have no FlowFile and we have connections coming from other Processors, then
     *  we know that we should run only if we have a FlowFile.
     */
    private boolean skipExecution(ProcessContext context, FlowFile flowfile) {
        return context.hasIncomingConnection() && flowfile == null && context.hasNonLoopConnection();
    }

    private FlowFile createResponseFlowFile(FlowFile flowfile, ProcessSession session, HttpResponseEntity httpResponseEntity)
        throws IOException, SchemaNotFoundException, MalformedRecordException {
        FlowFile responseFlowFile = null;
        try {
            if (isSuccess(httpResponseEntity.statusCode())) {
                responseFlowFile = flowfile == null ? session.create() : session.create(flowfile);
                InputStream responseBodyStream = httpResponseEntity.body();
                if (recordReaderFactoryReference.get() != null) {
                    TransformResult transformResult = transformRecords(session, flowfile, responseFlowFile, responseBodyStream);
                    Map<String, String> attributes = new HashMap<>();
                    attributes.put(RECORD_COUNT, String.valueOf(transformResult.getNumberOfRecords()));
                    attributes.put(CoreAttributes.MIME_TYPE.key(), transformResult.getMimeType());
                    responseFlowFile = session.putAllAttributes(responseFlowFile, attributes);
                } else {
                    responseFlowFile = session.importFrom(responseBodyStream, responseFlowFile);
                    Optional<String> mimeType = httpResponseEntity.headers().getFirstHeader(HEADER_CONTENT_TYPE);
                    if (mimeType.isPresent()) {
                        responseFlowFile = session.putAttribute(responseFlowFile, CoreAttributes.MIME_TYPE.key(), mimeType.get());
                    }
                }
            }
        } catch (Exception e) {
            session.remove(responseFlowFile);
            throw e;
        }
        return responseFlowFile;
    }

    private String createAuthorizationHeader(ProcessContext context, FlowFile flowfile) {
        String userName = context.getProperty(WORKDAY_USERNAME).evaluateAttributeExpressions(flowfile).getValue();
        String password = context.getProperty(WORKDAY_PASSWORD).evaluateAttributeExpressions(flowfile).getValue();
        String base64Credential = Base64.getEncoder().encodeToString((userName + USERNAME_PASSWORD_SEPARATOR + password).getBytes(StandardCharsets.UTF_8));
        return BASIC_PREFIX + base64Credential;
    }

    private TransformResult transformRecords(ProcessSession session, FlowFile flowfile, FlowFile responseFlowFile, InputStream responseBodyStream)
        throws IOException, SchemaNotFoundException, MalformedRecordException {
        int numberOfRecords = 0;
        String mimeType;
        try (RecordReader reader = recordReaderFactoryReference.get().createRecordReader(flowfile, new BufferedInputStream(responseBodyStream), getLogger())) {
            RecordSchema schema = recordSetWriterFactoryReference.get().getSchema(flowfile == null ? Collections.emptyMap() : flowfile.getAttributes(), reader.getSchema());
            try (OutputStream responseStream = session.write(responseFlowFile);
                RecordSetWriter recordSetWriter = recordSetWriterFactoryReference.get().createWriter(getLogger(), schema, responseStream, responseFlowFile)) {
                mimeType = recordSetWriter.getMimeType();
                recordSetWriter.beginRecordSet();
                Record currentRecord;
                // as the report can be changed independently from the flow, it's safer to ignore field types and unknown fields in the Record Reading process
                while ((currentRecord = reader.nextRecord(false, true)) != null) {
                    recordSetWriter.write(currentRecord);
                    numberOfRecords++;
                }
            }
        }
        return new TransformResult(numberOfRecords, mimeType);
    }

    private void route(FlowFile request, FlowFile response, ProcessSession session, ProcessContext context, int statusCode) {
        if (!isSuccess(statusCode) && request == null) {
            context.yield();
        }

        if (isSuccess(statusCode)) {
            if (request != null) {
                session.transfer(request, ORIGINAL);
            }
            if (response != null) {
                session.transfer(response, SUCCESS);
            }
        } else {
            if (request != null) {
                session.transfer(request, FAILURE);
            }
        }
    }

    private boolean isSuccess(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private Map<String, String> createCommonAttributes(URI uri, HttpResponseEntity httpResponseEntity, long elapsedTime) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(STATUS_CODE, String.valueOf(httpResponseEntity.statusCode()));
        attributes.put(REQUEST_URL, uri.toString());
        attributes.put(REQUEST_DURATION, Long.toString(elapsedTime));
        attributes.put(TRANSACTION_ID, UUID.randomUUID().toString());
        return attributes;
    }

    private Map<String, String> setMimeType(Map<String, String> commonAttributes, HttpResponseEntity httpResponseEntity) {
        Map<String, String> attributes = commonAttributes;
        Optional<String> contentType = httpResponseEntity.headers().getFirstHeader(HEADER_CONTENT_TYPE);
        if (contentType.isPresent()) {
            attributes = new HashMap<>(commonAttributes);
            attributes.put(CoreAttributes.MIME_TYPE.key(), contentType.get());
        }
        return attributes;
    }

    private static class TransformResult {
        private final int numberOfRecords;
        private final String mimeType;

        private TransformResult(int numberOfRecords, String mimeType) {
            this.numberOfRecords = numberOfRecords;
            this.mimeType = mimeType;
        }

        private int getNumberOfRecords() {
            return numberOfRecords;
        }

        private String getMimeType() {
            return mimeType;
        }
    }
}
