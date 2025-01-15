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

package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.syslog.events.SyslogEvent;
import org.apache.nifi.syslog.parsers.SyslogParser;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"logs", "syslog", "attributes", "system", "event", "message"})
@CapabilityDescription("Attempts to parses the contents of a Syslog message in accordance to RFC5424 and RFC3164 " +
        "formats and adds attributes to the FlowFile for each of the parts of the Syslog message." +
        "Note: Be mindfull that RFC3164 is informational and a wide range of different implementations are present in" +
        " the wild. If messages fail parsing, considering using RFC5424 or using a generic parsing processors such as " +
        "ExtractGrok.")
@WritesAttributes({@WritesAttribute(attribute = "syslog.priority", description = "The priority of the Syslog message."),
    @WritesAttribute(attribute = "syslog.severity", description = "The severity of the Syslog message derived from the priority."),
    @WritesAttribute(attribute = "syslog.facility", description = "The facility of the Syslog message derived from the priority."),
    @WritesAttribute(attribute = "syslog.version", description = "The optional version from the Syslog message."),
    @WritesAttribute(attribute = "syslog.timestamp", description = "The timestamp of the Syslog message."),
    @WritesAttribute(attribute = "syslog.hostname", description = "The hostname or IP address of the Syslog message."),
    @WritesAttribute(attribute = "syslog.sender", description = "The hostname of the Syslog server that sent the message."),
    @WritesAttribute(attribute = "syslog.body", description = "The body of the Syslog message, everything after the hostname.")})
@SeeAlso({ListenSyslog.class, PutSyslog.class})
public class ParseSyslog extends AbstractProcessor {

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("Specifies which character set of the Syslog messages")
        .required(true)
        .defaultValue("UTF-8")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        CHARSET
    );

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Any FlowFile that could not be parsed as a Syslog message will be transferred to this Relationship without any attributes being added")
        .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Any FlowFile that is successfully parsed as a Syslog message will be to this Relationship.")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_FAILURE,
            REL_SUCCESS
    );

    private SyslogParser parser;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String charsetName = context.getProperty(CHARSET).getValue();

        // If the parser already exists and uses the same charset, it does not need to be re-initialized
        if (parser == null || !parser.getCharsetName().equals(charsetName)) {
            parser = new SyslogParser(Charset.forName(charsetName));
        }

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));

        final SyslogEvent event;
        try {
            event = parser.parseEvent(buffer, null);
        } catch (final ProcessException pe) {
            getLogger().error("Routing to failure since failed to parse {} as a Syslog message", flowFile, pe);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (event == null || !event.isValid()) {
            getLogger().error("Failed to parse {} as a Syslog message: it does not conform to any of the RFC formats supported; routing to failure", flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final Map<String, String> attributes = new HashMap<>(8);
        attributes.put(SyslogAttributes.SYSLOG_PRIORITY.key(), event.getPriority());
        attributes.put(SyslogAttributes.SYSLOG_SEVERITY.key(), event.getSeverity());
        attributes.put(SyslogAttributes.SYSLOG_FACILITY.key(), event.getFacility());
        attributes.put(SyslogAttributes.SYSLOG_VERSION.key(), event.getVersion());
        attributes.put(SyslogAttributes.SYSLOG_TIMESTAMP.key(), event.getTimeStamp());
        attributes.put(SyslogAttributes.SYSLOG_HOSTNAME.key(), event.getHostName());
        attributes.put(SyslogAttributes.SYSLOG_BODY.key(), event.getMsgBody());

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);
    }

}
