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
package org.apache.nifi.services.slack;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Tags({"slack", "record", "sink"})
@CapabilityDescription("Format and send Records to a configured Channel using the Slack Post Message API. " +
        "The service requires a Slack App with a Bot User configured for access to a Slack workspace. " +
        "The Bot User OAuth Bearer Token is required for posting messages to Slack.")
public class SlackRecordSink extends AbstractControllerService implements RecordSinkService {

    private static final String SLACK_API_URL = "https://slack.com/api";

    public static final PropertyDescriptor API_URL = new PropertyDescriptor.Builder()
            .name("api-url")
            .displayName("API URL")
            .description("Slack Web API URL for posting text messages to channels." +
                    " It only needs to be changed if Slack changes its API URL.")
            .required(true)
            .defaultValue(SLACK_API_URL)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("access-token")
            .displayName("Access Token")
            .description("Bot OAuth Token used for authenticating and authorizing the Slack request sent by NiFi.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHANNEL_ID = new PropertyDescriptor.Builder()
            .name("channel-id")
            .displayName("Channel ID")
            .description("Slack channel, private group, or IM channel to send the message to. Use Channel ID instead of the name.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INPUT_CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("input-character-set")
            .displayName("Input Character Set")
            .description("Specifies the character set of the records used to generate the Slack message.")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(StandardCharsets.UTF_8.name())
            .build();

    public static final PropertyDescriptor WEB_SERVICE_CLIENT_PROVIDER = new PropertyDescriptor.Builder()
            .name("web-service-client-provider")
            .displayName("Web Service Client Provider")
            .description("Controller service to provide HTTP client for communicating with Slack API")
            .required(true)
            .identifiesControllerService(WebClientServiceProvider.class)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        API_URL,
        ACCESS_TOKEN,
        CHANNEL_ID,
        INPUT_CHARACTER_SET,
        RECORD_WRITER_FACTORY,
        WEB_SERVICE_CLIENT_PROVIDER
    );

    private volatile RecordSetWriterFactory writerFactory;
    private SlackRestService service;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        writerFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        final WebClientServiceProvider webClientServiceProvider = context
                .getProperty(WEB_SERVICE_CLIENT_PROVIDER)
                .asControllerService(WebClientServiceProvider.class);
        final String accessToken = context.getProperty(ACCESS_TOKEN).getValue();
        final String apiUrl = context.getProperty(API_URL).getValue();
        final String charset = context.getProperty(INPUT_CHARACTER_SET).getValue();
        service = new SlackRestService(webClientServiceProvider, accessToken, apiUrl, charset, getLogger());
    }

    @Override
    public WriteResult sendData(final RecordSet recordSet, final Map<String, String> attributes, final boolean sendZeroResults) throws IOException {
        WriteResult writeResult;
        final String channel = getConfigurationContext().getProperty(CHANNEL_ID).getValue();
        int recordCount = 0;
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), recordSet.getSchema(), out, attributes)) {
                writer.beginRecordSet();
                Record record = recordSet.next();
                while (record != null) {
                    writer.write(record);
                    writer.flush();
                    record = recordSet.next();
                    recordCount++;
                }
                writeResult = writer.finishRecordSet();
                writer.flush();
            } catch (final SchemaNotFoundException e) {
                final String errorMessage = String.format("RecordSetWriter could not be created because the schema was not found. The schema name for the RecordSet to write is %s",
                        recordSet.getSchema().getSchemaName());
                throw new ProcessException(errorMessage, e);
            }
            if (recordCount > 0 || sendZeroResults) {
                try {
                    final String message = out.toString();
                    service.sendMessageToChannel(message, channel);
                } catch (final SlackRestServiceException e) {
                    throw new IOException("Failed to send messages to Slack", e);
                }
            }
        }
        return writeResult;
    }
}
