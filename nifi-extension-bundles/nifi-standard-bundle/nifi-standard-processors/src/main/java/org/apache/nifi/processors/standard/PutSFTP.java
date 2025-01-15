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
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processor.util.file.transfer.PutFileTransfer;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"remote", "copy", "egress", "put", "sftp", "archive", "files"})
@CapabilityDescription("Sends FlowFiles to an SFTP Server")
@SeeAlso(GetSFTP.class)
public class PutSFTP extends PutFileTransfer<SFTPTransfer> {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FileTransfer.HOSTNAME,
            SFTPTransfer.PORT,
            FileTransfer.USERNAME,
            FileTransfer.PASSWORD,
            SFTPTransfer.PRIVATE_KEY_PATH,
            SFTPTransfer.PRIVATE_KEY_PASSPHRASE,
            FileTransfer.REMOTE_PATH,
            FileTransfer.CREATE_DIRECTORY,
            SFTPTransfer.DISABLE_DIRECTORY_LISTING,
            FileTransfer.BATCH_SIZE,
            FileTransfer.CONNECTION_TIMEOUT,
            FileTransfer.DATA_TIMEOUT,
            FileTransfer.CONFLICT_RESOLUTION,
            FileTransfer.REJECT_ZERO_BYTE,
            FileTransfer.DOT_RENAME,
            FileTransfer.TEMP_FILENAME,
            SFTPTransfer.HOST_KEY_FILE,
            FileTransfer.LAST_MODIFIED_TIME,
            FileTransfer.PERMISSIONS,
            FileTransfer.REMOTE_OWNER,
            FileTransfer.REMOTE_GROUP,
            SFTPTransfer.STRICT_HOST_KEY_CHECKING,
            SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT,
            FileTransfer.USE_COMPRESSION,
            SFTPTransfer.PROXY_CONFIGURATION_SERVICE,
            SFTPTransfer.CIPHERS_ALLOWED,
            SFTPTransfer.KEY_ALGORITHMS_ALLOWED,
            SFTPTransfer.KEY_EXCHANGE_ALGORITHMS_ALLOWED,
            SFTPTransfer.MESSAGE_AUTHENTICATION_CODES_ALLOWED
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
    protected SFTPTransfer getFileTransfer(final ProcessContext context) {
        return new SFTPTransfer(context, getLogger());
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();
        SFTPTransfer.validateProxySpec(validationContext, results);
        return results;
    }
}
