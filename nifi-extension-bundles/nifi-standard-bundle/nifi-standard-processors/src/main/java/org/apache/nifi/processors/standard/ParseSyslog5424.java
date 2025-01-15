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
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.syslog.events.Syslog5424Event;
import org.apache.nifi.syslog.keyproviders.SyslogPrefixedKeyProvider;
import org.apache.nifi.syslog.parsers.StrictSyslog5424Parser;
import org.apache.nifi.syslog.utils.NifiStructuredDataPolicy;
import org.apache.nifi.syslog.utils.NilHandlingPolicy;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"logs", "syslog", "syslog5424", "attributes", "system", "event", "message"})
@CapabilityDescription("Attempts to parse the contents of a well formed Syslog message in accordance to RFC5424 " +
        "format and adds attributes to the FlowFile for each of the parts of the Syslog message, including Structured Data." +
        "Structured Data will be written to attributes as one attribute per item id + parameter " +
        "see https://tools.ietf.org/html/rfc5424." +
        "Note: ParseSyslog5424 follows the specification more closely than ParseSyslog.  If your Syslog producer " +
        "does not follow the spec closely, with regards to using '-' for missing header entries for example, those logs " +
        "will fail with this parser, where they would not fail with ParseSyslog.")
@WritesAttributes({@WritesAttribute(attribute = "syslog.priority", description = "The priority of the Syslog message."),
    @WritesAttribute(attribute = "syslog.severity", description = "The severity of the Syslog message derived from the priority."),
    @WritesAttribute(attribute = "syslog.facility", description = "The facility of the Syslog message derived from the priority."),
    @WritesAttribute(attribute = "syslog.version", description = "The optional version from the Syslog message."),
    @WritesAttribute(attribute = "syslog.timestamp", description = "The timestamp of the Syslog message."),
    @WritesAttribute(attribute = "syslog.hostname", description = "The hostname or IP address of the Syslog message."),
    @WritesAttribute(attribute = "syslog.appname", description = "The appname of the Syslog message."),
    @WritesAttribute(attribute = "syslog.procid", description = "The procid of the Syslog message."),
    @WritesAttribute(attribute = "syslog.messageid", description = "The messageid the Syslog message."),
    @WritesAttribute(attribute = "syslog.structuredData", description = "Multiple entries per structuredData of the Syslog message."),
    @WritesAttribute(attribute = "syslog.sender", description = "The hostname of the Syslog server that sent the message."),
    @WritesAttribute(attribute = "syslog.body", description = "The body of the Syslog message, everything after the hostname.")})
@SeeAlso({ListenSyslog.class, ParseSyslog.class, PutSyslog.class})
public class ParseSyslog5424 extends AbstractProcessor {

    public static final AllowableValue OMIT = new AllowableValue(NilHandlingPolicy.OMIT.name(), NilHandlingPolicy.OMIT.name(), "The missing field will not have an attribute added.");
    public static final AllowableValue NULL = new AllowableValue(NilHandlingPolicy.NULL.name(), NilHandlingPolicy.NULL.name(), "The missing field will have an empty attribute added.");
    public static final AllowableValue DASH = new AllowableValue(NilHandlingPolicy.DASH.name(), NilHandlingPolicy.DASH.name(), "The missing field will have an attribute added with the value of '-'.");

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("Specifies which character set of the Syslog messages")
        .required(true)
        .defaultValue("UTF-8")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .build();

    public static final PropertyDescriptor NIL_POLICY = new PropertyDescriptor.Builder()
        .name("nil_policy")
        .displayName("NIL Policy")
        .description("Defines how NIL values are handled for header fields.")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .allowableValues(OMIT, NULL, DASH)
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .defaultValue(NULL.getValue())
        .build();

    public static final PropertyDescriptor INCLUDE_BODY_IN_ATTRIBUTES = new PropertyDescriptor.Builder()
        .name("include_policy")
        .displayName("Include Message Body in Attributes")
        .description("If true, then the Syslog Message body will be included in the attributes.")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .allowableValues("true", "false")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .defaultValue("true")
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CHARSET,
            NIL_POLICY,
            INCLUDE_BODY_IN_ATTRIBUTES
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

    private volatile StrictSyslog5424Parser parser;

    private volatile Charset charset;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        charset = Charset.forName(context.getProperty(CHARSET).getValue());
        final String nilPolicyString = context.getProperty(NIL_POLICY).getValue();
        parser = new StrictSyslog5424Parser(NilHandlingPolicy.valueOf(nilPolicyString), NifiStructuredDataPolicy.FLATTEN, new SyslogPrefixedKeyProvider());
    }
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        boolean includeBody = true;

        if (context.getProperty(INCLUDE_BODY_IN_ATTRIBUTES).isSet()) {
           includeBody = context.getProperty(INCLUDE_BODY_IN_ATTRIBUTES).asBoolean();
        }

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));
        final String line = new String(buffer, charset).trim();

        final Syslog5424Event syslogEvent;
        try {
            syslogEvent = parser.parseEvent(line);
        } catch (final ProcessException pe) {
            getLogger().error("Failed to parse {} as a Syslog 5424  message; routing to failure", flowFile, pe);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (syslogEvent == null || !syslogEvent.isValid()) {
            getLogger().error("Failed to parse {} as a Syslog message: it does not conform to any of the RFC formats supported; routing to failure", flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        Map<String, String> attributeMap = convertMap(syslogEvent.getFieldMap());
        if (!includeBody) {
            attributeMap.remove(SyslogAttributes.SYSLOG_BODY.key());
        }
        flowFile = session.putAllAttributes(flowFile, attributeMap);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private static Map<String, String> convertMap(Map<String, Object> map) {
        Map<String, String> returnMap = new HashMap<>();
        map.forEach((key, value) -> returnMap.put(key, (String) value));
        return returnMap;
    }
}
