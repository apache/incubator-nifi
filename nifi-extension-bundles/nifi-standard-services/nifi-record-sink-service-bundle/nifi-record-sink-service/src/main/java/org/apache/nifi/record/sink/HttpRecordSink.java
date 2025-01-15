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

package org.apache.nifi.record.sink;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

@Tags({"http", "post", "record", "sink"})
@CapabilityDescription("Format and send Records to a configured uri using HTTP post. The Record Writer formats the records which are sent as the body of the HTTP post request. " +
        "JsonRecordSetWriter is often used with this processor because many HTTP posts require a JSON body.")
public class HttpRecordSink extends AbstractControllerService implements RecordSinkService {
    protected static final String HEADER_AUTHORIZATION = "Authorization";
    protected static final String HEADER_CONTENT_TYPE = "Content-Type";

     public static final PropertyDescriptor API_URL = new PropertyDescriptor.Builder()
            .name("API URL")
            .description("The URL which receives the HTTP requests.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Batch Size")
            .description("Specifies the maximum number of records to send in the body of each HTTP request. Zero means the batch size is not limited, "
                    + "and all records are sent together in a single HTTP request.")
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    public static final PropertyDescriptor WEB_SERVICE_CLIENT_PROVIDER = new PropertyDescriptor.Builder()
            .name("Web Service Client Provider")
            .description("Controller service to provide the HTTP client for sending the HTTP requests.")
            .required(true)
            .identifiesControllerService(WebClientServiceProvider.class)
            .build();

    public static final PropertyDescriptor OAUTH2_ACCESS_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("OAuth2 Access Token Provider")
            .description("OAuth2 service that provides the access tokens for the HTTP requests.")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            API_URL,
            MAX_BATCH_SIZE,
            RECORD_WRITER_FACTORY,
            WEB_SERVICE_CLIENT_PROVIDER,
            OAUTH2_ACCESS_TOKEN_PROVIDER
    );

    private String apiUrl;
    private int maxBatchSize;
    private volatile RecordSetWriterFactory writerFactory;
    private WebClientServiceProvider webClientServiceProvider;
    private volatile Optional<OAuth2AccessTokenProvider> oauth2AccessTokenProviderOptional;
    Map<String, String> dynamicHttpHeaders;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * Returns a PropertyDescriptor for the given name. This is for the user to be able to define their own properties
     * which will sent as HTTP headers on the HTTP request
     *
     * @param propertyDescriptorName used to lookup if any property descriptors exist for that name
     * @return a PropertyDescriptor object corresponding to the specified dynamic property name
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (hasProhibitedName(propertyDescriptorName, HEADER_CONTENT_TYPE)) {
            // Content-Type is case-sensitive for overriding our default Content-Type header, so prevent any other combination of upper/lower case letters
            return getInvalidDynamicPropertyDescriptor(propertyDescriptorName, "is not allowed. Only exact case of Content-Type is allowed.");
        }

        if (hasProhibitedName(propertyDescriptorName, HEADER_AUTHORIZATION)) {
            // Authorization is case-sensitive for overriding our default Authorization header, so prevent any other combination of upper/lower case letters
            return getInvalidDynamicPropertyDescriptor(propertyDescriptorName, "is not allowed. Only exact case of Authorization is allowed.");
        }

        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .dynamic(true)
                .build();
    }

    private static boolean hasProhibitedName(String userInput, String correctName) {
        // Do not allow : in any case be
        // cause it is not the correct name
        // 'correctName' header is case-sensitive for overriding our default 'correctName' header, so prevent any other combination of upper/lower case letters
        return (correctName + ":").equalsIgnoreCase(userInput)
                || (correctName.equalsIgnoreCase(userInput) && !correctName.equals(userInput));
    }

    private static PropertyDescriptor getInvalidDynamicPropertyDescriptor(String propertyDescriptorName, String explanation) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator((subject, input, context) -> new ValidationResult.Builder()
                        .explanation(explanation)
                        .valid(false)
                        .subject(subject)
                        .build())
                .dynamic(true)
                .build();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        apiUrl = context.getProperty(API_URL).evaluateAttributeExpressions().getValue();
        maxBatchSize = context.getProperty(MAX_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        writerFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        webClientServiceProvider = context
                .getProperty(WEB_SERVICE_CLIENT_PROVIDER).asControllerService(WebClientServiceProvider.class);

        if (context.getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).isSet()) {
            OAuth2AccessTokenProvider oauth2AccessTokenProvider = context
                    .getProperty(OAUTH2_ACCESS_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class);
            oauth2AccessTokenProvider.getAccessDetails();
            oauth2AccessTokenProviderOptional = Optional.of(oauth2AccessTokenProvider);
        } else {
            oauth2AccessTokenProviderOptional = Optional.empty();
        }

        // Dynamic properties are sent as http headers on the post request.
        dynamicHttpHeaders = context.getProperties().keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toMap(
                        PropertyDescriptor::getName,
                        p -> context.getProperty(p).evaluateAttributeExpressions().getValue()));
    }

    @Override
    public WriteResult sendData(RecordSet recordSet, Map<String, String> attributes, boolean sendZeroResults) throws IOException {
        WriteResult writeResult;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final RecordSetWriter writer = writerFactory.createWriter(getLogger(), recordSet.getSchema(), baos, attributes)) {
                writeResult = sendRecords(recordSet, writer, baos, maxBatchSize);
        } catch (SchemaNotFoundException e) {
            final String errorMessage = String.format("RecordSetWriter could not be created because the schema was not found. The schema name for the RecordSet to write is %s",
                    recordSet.getSchema().getSchemaName());
            throw new IOException(errorMessage, e);
        }

        return writeResult;
    }

    private WriteResult sendRecords(final RecordSet recordSet, final RecordSetWriter writer, final ByteArrayOutputStream baos, int maxBatchSize) throws IOException {
        WriteResult writeResult = WriteResult.EMPTY;
        Record r = recordSet.next();
        if (r != null) {
            int batchCount = 0;
            do {
                if (maxBatchSize != 1 && batchCount == 0) {
                    // If maxBatchSize is one, then do NOT write record set begin or end markers because
                    // each single record is sent alone without being in an array.
                    writer.beginRecordSet();
                }

                writeResult = writer.write(r);
                batchCount++;

                r = recordSet.next();

                // If this is last record, then send current group of records.
                // OR if we have processed maxBatchSize records, then send current group of records.
                // Unless batchCount is 0, which means to send all records together in one batch at the end.
                if (r == null || (maxBatchSize > 0 && batchCount >= maxBatchSize)) {
                    if (maxBatchSize != 1) {
                        writeResult = writer.finishRecordSet();
                    }
                    writer.flush();
                    sendHttpRequest(baos.toByteArray(), writer.getMimeType());
                    baos.reset();
                    batchCount = 0;
                }
            } while (r != null);
        }
        return writeResult;
    }

    public void sendHttpRequest(final byte[] body, String mimeType) throws IOException {
        final URI apiUri = URI.create(apiUrl);
        final HttpUriBuilder uriBuilder = webClientServiceProvider.getHttpUriBuilder()
                .scheme(apiUri.getScheme())
                .host(apiUri.getHost())
                .encodedPath(apiUri.getPath());
        if (apiUri.getPort() != -1) {
            uriBuilder.port(apiUri.getPort());
        }
        final URI uri = uriBuilder.build();

        HttpRequestBodySpec requestBodySpec = webClientServiceProvider.getWebClientService()
                .post()
                .uri(uri);

        dynamicHttpHeaders.forEach(requestBodySpec::header);

        if (StringUtils.isNotBlank(mimeType) && !dynamicHttpHeaders.containsKey(HEADER_CONTENT_TYPE)) {
            requestBodySpec.header(HEADER_CONTENT_TYPE, mimeType);
        }

        if (!dynamicHttpHeaders.containsKey(HEADER_AUTHORIZATION)) {
            oauth2AccessTokenProviderOptional.ifPresent(oauth2AccessTokenProvider ->
                    requestBodySpec.header(HEADER_AUTHORIZATION, "Bearer " + oauth2AccessTokenProvider.getAccessDetails().getAccessToken()));
        }

        final InputStream requestBodyInputStream = new ByteArrayInputStream(body);

        try (final HttpResponseEntity response = requestBodySpec
                .body(requestBodyInputStream, OptionalLong.of(requestBodyInputStream.available()))
                .retrieve()) {
            final int statusCode = response.statusCode();
            if (!(statusCode >= 200 && statusCode < 300)) {
                throw new IOException(String.format("HTTP request failed with status code: %s for url: %s and returned response body: %s",
                        statusCode, uri.toString(), response.body() == null ? "none" : IOUtils.toString(response.body(), StandardCharsets.UTF_8)));
            }
        } catch (final IOException ioe) {
            throw ioe;
        } catch (final Exception e) {
            throw new IOException(String.format("HttpRecordSink HTTP request transmission failed for url: %s", uri.toString()), e);
        }
    }
}
