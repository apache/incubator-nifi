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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.MultiProcessorUseCase;
import org.apache.nifi.annotation.documentation.ProcessorConfiguration;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.file.transfer.FetchFileTransfer;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// Note that we do not use @SupportsBatching annotation. This processor cannot support batching because it must ensure that session commits happen before remote files are deleted.
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sftp", "get", "retrieve", "files", "fetch", "remote", "ingest", "source", "input"})
@CapabilityDescription("Fetches the content of a file from a remote SFTP server and overwrites the contents of an incoming FlowFile with the content of the remote file.")
@SeeAlso({GetSFTP.class, PutSFTP.class, GetFTP.class, PutFTP.class})
@WritesAttributes({
    @WritesAttribute(attribute = "sftp.remote.host", description = "The hostname or IP address from which the file was pulled"),
    @WritesAttribute(attribute = "sftp.remote.port", description = "The port that was used to communicate with the remote SFTP server"),
    @WritesAttribute(attribute = "sftp.remote.filename", description = "The name of the remote file that was pulled"),
    @WritesAttribute(attribute = "filename", description = "The filename is updated to point to the filename fo the remote file"),
    @WritesAttribute(attribute = "path", description = "If the Remote File contains a directory name, that directory name will be added to the FlowFile using the 'path' attribute"),
    @WritesAttribute(attribute = "fetch.failure.reason", description = "The name of the failure relationship applied when routing to any failure relationship")
})
@MultiProcessorUseCase(
    description = "Retrieve all files in a directory of an SFTP Server",
    keywords = {"sftp", "secure", "file", "transform", "state", "retrieve", "fetch", "all", "stream"},
    configurations = {
        @ProcessorConfiguration(
            processorClass = ListSFTP.class,
            configuration = """
                The "Hostname" property should be set to the fully qualified hostname of the FTP Server. It's a good idea to parameterize \
                    this property by setting it to something like `#{SFTP_SERVER}`.
                The "Remote Path" property must be set to the directory on the FTP Server where the files reside. If the flow being built is to be reused elsewhere, \
                    it's a good idea to parameterize this property by setting it to something like `#{SFTP_REMOTE_PATH}`.
                Configure the "Username" property to the appropriate username for logging into the FTP Server. It's usually a good idea to parameterize this property \
                    by setting it to something like `#{SFTP_USERNAME}`.
                Configure the "Password" property to the appropriate password for the provided username. It's usually a good idea to parameterize this property \
                    by setting it to something like `#{SFTP_PASSWORD}`.

                The 'success' Relationship of this Processor is then connected to FetchSFTP.
                """
        ),
        @ProcessorConfiguration(
            processorClass = FetchSFTP.class,
            configuration = """
                "Hostname" = "${sftp.remote.host}"
                "Remote File" = "${path}/${filename}"
                "Username" = "${sftp.listing.user}"
                "Password" = "#{SFTP_PASSWORD}"
                """
        )
    }
)
public class FetchSFTP extends FetchFileTransfer {

    private static final PropertyDescriptor PORT =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(UNDEFAULTED_PORT).defaultValue("22").build();

    private static final PropertyDescriptor DISABLE_DIRECTORY_LISTING = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SFTPTransfer.DISABLE_DIRECTORY_LISTING)
            .description(String.format("Control how '%s' is created when '%s' is '%s' and '%s' is enabled. %s",
                    MOVE_DESTINATION_DIR.getDisplayName(),
                    COMPLETION_STRATEGY.getDisplayName(),
                    COMPLETION_MOVE.getDisplayName(),
                    MOVE_CREATE_DIRECTORY.getDisplayName(),
                    SFTPTransfer.DISABLE_DIRECTORY_LISTING.getDescription())).build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            HOSTNAME,
            PORT,
            USERNAME,
            SFTPTransfer.PASSWORD,
            SFTPTransfer.PRIVATE_KEY_PATH,
            SFTPTransfer.PRIVATE_KEY_PASSPHRASE,
            REMOTE_FILENAME,
            COMPLETION_STRATEGY,
            MOVE_DESTINATION_DIR,
            MOVE_CREATE_DIRECTORY,
            DISABLE_DIRECTORY_LISTING,
            SFTPTransfer.CONNECTION_TIMEOUT,
            SFTPTransfer.DATA_TIMEOUT,
            SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT,
            SFTPTransfer.HOST_KEY_FILE,
            SFTPTransfer.STRICT_HOST_KEY_CHECKING,
            SFTPTransfer.USE_COMPRESSION,
            SFTPTransfer.PROXY_CONFIGURATION_SERVICE,
            FILE_NOT_FOUND_LOG_LEVEL,
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
    protected FileTransfer createFileTransfer(final ProcessContext context) {
        return new SFTPTransfer(context, getLogger());
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();
        SFTPTransfer.validateProxySpec(validationContext, results);
        return results;
    }
}
