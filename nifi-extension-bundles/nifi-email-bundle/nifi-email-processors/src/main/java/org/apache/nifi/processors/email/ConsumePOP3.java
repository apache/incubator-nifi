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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.springframework.integration.mail.Pop3MailReceiver;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Consumes messages from Email Server using POP3 protocol. "
        + "The raw-bytes of each received email message are written as contents of the FlowFile")
@Tags({ "Email", "POP3", "Get", "Ingest", "Ingress", "Message", "Consume" })
public class ConsumePOP3 extends AbstractEmailProcessor<Pop3MailReceiver> {

    @Override
    protected String getProtocol(ProcessContext processContext) {
        return "pop3";
    }

    @Override
    protected Pop3MailReceiver buildMessageReceiver(ProcessContext context) {
        final Pop3MailReceiver receiver = new Pop3MailReceiver(this.buildUrl(context));
        receiver.setShouldDeleteMessages(context.getProperty(AbstractEmailProcessor.SHOULD_DELETE_MESSAGES).asBoolean());
        return receiver;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return getCommonPropertyDescriptors();
    }
}
