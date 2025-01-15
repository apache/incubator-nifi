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

import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.file.transfer.PutFileTransfer;
import org.apache.nifi.processors.standard.util.FTPTransfer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"remote", "copy", "egress", "put", "ftp", "archive", "files"})
@CapabilityDescription("Sends FlowFiles to an FTP Server")
@SeeAlso(GetFTP.class)
@DynamicProperties({
    @DynamicProperty(name = "pre.cmd._____", value = "Not used", description = "The command specified in the key will be executed before doing a put.  You may add these optional properties "
            + " to send any commands to the FTP server before the file is actually transferred (before the put command)."
            + " This option is only available for the PutFTP processor, as only FTP has this functionality. This is"
            + " essentially the same as sending quote commands to an FTP server from the command line.  While this is the same as sending a quote command, it is very important that"
            + " you leave off the ."),
    @DynamicProperty(name = "post.cmd._____", value = "Not used", description = "The command specified in the key will be executed after doing a put.  You may add these optional properties "
            + " to send any commands to the FTP server before the file is actually transferred (before the put command)."
            + " This option is only available for the PutFTP processor, as only FTP has this functionality. This is"
            + " essentially the same as sending quote commands to an FTP server from the command line.  While this is the same as sending a quote command, it is very important that"
            + " you leave off the .")})
public class PutFTP extends PutFileTransfer<FTPTransfer> {

    private static final Pattern PRE_SEND_CMD_PATTERN = Pattern.compile("^pre\\.cmd\\.(\\d+)$");
    private static final Pattern POST_SEND_CMD_PATTERN = Pattern.compile("^post\\.cmd\\.(\\d+)$");

    private final AtomicReference<List<PropertyDescriptor>> preSendDescriptorRef = new AtomicReference<>();
    private final AtomicReference<List<PropertyDescriptor>> postSendDescriptorRef = new AtomicReference<>();

    // PutFileTransfer.onTrigger() uses FlowFile attributes
    public static final PropertyDescriptor REMOTE_PATH = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(FTPTransfer.REMOTE_PATH)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FTPTransfer.HOSTNAME,
            FTPTransfer.PORT,
            FTPTransfer.USERNAME,
            FTPTransfer.PASSWORD,
            REMOTE_PATH,
            FTPTransfer.CREATE_DIRECTORY,
            FTPTransfer.BATCH_SIZE,
            FTPTransfer.CONNECTION_TIMEOUT,
            FTPTransfer.DATA_TIMEOUT,
            FTPTransfer.CONFLICT_RESOLUTION,
            FTPTransfer.DOT_RENAME,
            FTPTransfer.TEMP_FILENAME,
            FTPTransfer.TRANSFER_MODE,
            FTPTransfer.CONNECTION_MODE,
            FTPTransfer.REJECT_ZERO_BYTE,
            FTPTransfer.LAST_MODIFIED_TIME,
            FTPTransfer.PERMISSIONS,
            FTPTransfer.USE_COMPRESSION,
            FTPTransfer.PROXY_CONFIGURATION_SERVICE,
            FTPTransfer.BUFFER_SIZE,
            FTPTransfer.UTF8_ENCODING
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        FTPTransfer.migrateProxyProperties(config);
    }

    @Override
    protected void beforePut(final FlowFile flowFile, final ProcessContext context, final FTPTransfer transfer) throws IOException {
        transfer.sendCommands(getCommands(preSendDescriptorRef.get(), context, flowFile), flowFile);
    }

    @Override
    protected void afterPut(final FlowFile flowFile, final ProcessContext context, final FTPTransfer transfer) throws IOException {
        transfer.sendCommands(getCommands(postSendDescriptorRef.get(), context, flowFile), flowFile);
    }

    @Override
    protected FTPTransfer getFileTransfer(final ProcessContext context) {
        return new FTPTransfer(context, getLogger());
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public void determinePrePostSendProperties(final ProcessContext context) {
        final Map<Integer, PropertyDescriptor> preDescriptors = new TreeMap<>();
        final Map<Integer, PropertyDescriptor> postDescriptors = new TreeMap<>();

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            final String name = descriptor.getName();
            final Matcher preMatcher = PRE_SEND_CMD_PATTERN.matcher(name);
            if (preMatcher.matches()) {
                final int index = Integer.parseInt(preMatcher.group(1));
                preDescriptors.put(index, descriptor);
            } else {
                final Matcher postMatcher = POST_SEND_CMD_PATTERN.matcher(name);
                if (postMatcher.matches()) {
                    final int index = Integer.parseInt(postMatcher.group(1));
                    postDescriptors.put(index, descriptor);
                }
            }
        }

        final List<PropertyDescriptor> preDescriptorList = new ArrayList<>(preDescriptors.values());
        final List<PropertyDescriptor> postDescriptorList = new ArrayList<>(postDescriptors.values());
        this.preSendDescriptorRef.set(preDescriptorList);
        this.postSendDescriptorRef.set(postDescriptorList);
    }

    private List<String> getCommands(final List<PropertyDescriptor> descriptors, final ProcessContext context, final FlowFile flowFile) {
        final List<String> cmds = new ArrayList<>();
        for (final PropertyDescriptor descriptor : descriptors) {
            cmds.add(context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue());
        }

        return cmds;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        FTPTransfer.validateProxySpec(validationContext, results);
        return results;
    }
}
