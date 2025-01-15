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
package org.apache.nifi.processors.azure.storage.queue;

import com.azure.core.util.Context;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.models.QueueStorageException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SeeAlso({GetAzureQueueStorage_v12.class})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "azure", "microsoft", "cloud", "storage", "queue", "enqueue" })
@CapabilityDescription("Writes the content of the incoming FlowFiles to the configured Azure Queue Storage.")
public class PutAzureQueueStorage_v12 extends AbstractAzureQueueStorage_v12 {
    public static final PropertyDescriptor MESSAGE_TIME_TO_LIVE = new PropertyDescriptor.Builder()
            .name("Message Time To Live")
            .displayName("Message Time To Live")
            .description("Maximum time to allow the message to be in the queue")
            .required(true)
            .defaultValue("7 days")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor VISIBILITY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Visibility Timeout")
            .displayName("Visibility Timeout")
            .description("The length of time during which the message will be invisible after it is read. " +
                    "If the processing unit fails to delete the message after it is read, then the message will reappear in the queue.")
            .required(true)
            .defaultValue("30 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP, ProxySpec.SOCKS};
    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            QUEUE_NAME,
            ENDPOINT_SUFFIX,
            STORAGE_CREDENTIALS_SERVICE,
            MESSAGE_TIME_TO_LIVE,
            VISIBILITY_TIMEOUT,
            REQUEST_TIMEOUT,
            ProxyConfiguration.createProxyConfigPropertyDescriptor(PROXY_SPECS)
    );

    // 7 days is the maximum timeout as per https://learn.microsoft.com/en-us/rest/api/storageservices/get-messages
    private static final Duration MAX_VISIBILITY_TIMEOUT = Duration.ofDays(7);

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = (List<ValidationResult>) super.customValidate(validationContext);
        final Duration visibilityTimeout = Duration.ofSeconds(
                validationContext.getProperty(VISIBILITY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS)
        );

        if (visibilityTimeout.getSeconds() <= 0) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(VISIBILITY_TIMEOUT.getDisplayName())
                    .explanation(VISIBILITY_TIMEOUT.getDisplayName() + " should be greater than 0 secs")
                    .build());
        }

        if (MAX_VISIBILITY_TIMEOUT.compareTo(visibilityTimeout) < 0) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject(VISIBILITY_TIMEOUT.getDisplayName())
                    .explanation(VISIBILITY_TIMEOUT.getDisplayName() + " should not be greater than 7 days")
                    .build());
        }

        final int ttl = validationContext.getProperty(MESSAGE_TIME_TO_LIVE).asTimePeriod(TimeUnit.SECONDS).intValue();
        if (ttl <= 0) {
            results.add(new ValidationResult.Builder()
                    .subject(MESSAGE_TIME_TO_LIVE.getDisplayName())
                    .valid(false)
                    .explanation(MESSAGE_TIME_TO_LIVE.getDisplayName() + " should be any positive number")
                    .build());
        }

        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);
        final String flowFileContent = baos.toString();

        final int visibilityTimeoutInSecs = context.getProperty(VISIBILITY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final int ttl = context.getProperty(MESSAGE_TIME_TO_LIVE).asTimePeriod(TimeUnit.SECONDS).intValue();
        final int requestTimeoutInSecs = context.getProperty(REQUEST_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();

        final QueueClient queueClient = createQueueClient(context, flowFile);
        try {
            queueClient.sendMessageWithResponse(
                    flowFileContent,
                    Duration.ofSeconds(visibilityTimeoutInSecs),
                    Duration.ofSeconds(ttl),
                    Duration.ofSeconds(requestTimeoutInSecs),
                    Context.NONE
            );
        } catch (final QueueStorageException e) {
            getLogger().error("Failed to write message to Azure Queue Storage", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
        final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        session.getProvenanceReporter().send(flowFile, queueClient.getQueueUrl().toString(), transmissionMillis);
    }
}
