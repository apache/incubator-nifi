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
package org.apache.nifi.processors.email;

import java.util.List;
import java.util.stream.Stream;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.springframework.integration.mail.ImapMailReceiver;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Consumes messages from Email Server using IMAP protocol. "
        + "The raw-bytes of each received email message are written as contents of the FlowFile")
@Tags({ "Email", "Imap", "Get", "Ingest", "Ingress", "Message", "Consume" })
public class ConsumeIMAP extends AbstractEmailProcessor<ImapMailReceiver> {

    public static final PropertyDescriptor SHOULD_MARK_READ = new PropertyDescriptor.Builder()
            .name("Mark Messages as Read")
            .description("Specify if messages should be marked as read after retrieval.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor USE_SSL = new PropertyDescriptor.Builder()
            .name("Use SSL")
            .description("Specifies if IMAP connection must be obtained via SSL encrypted connection (i.e., IMAPS)")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                    SHOULD_MARK_READ,
                    USE_SSL
            )
    ).toList();

    @Override
    protected ImapMailReceiver buildMessageReceiver(ProcessContext processContext) {
        ImapMailReceiver receiver = new ImapMailReceiver(this.buildUrl(processContext));
        boolean shouldMarkAsRead = processContext.getProperty(SHOULD_MARK_READ).asBoolean();
        receiver.setShouldMarkMessagesAsRead(shouldMarkAsRead);
        receiver.setShouldDeleteMessages(processContext.getProperty(AbstractEmailProcessor.SHOULD_DELETE_MESSAGES).asBoolean());
        return receiver;
    }

    @Override
    protected String getProtocol(ProcessContext processContext) {
        return processContext.getProperty(USE_SSL).asBoolean() ? "imaps" : "imap";
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }
}
