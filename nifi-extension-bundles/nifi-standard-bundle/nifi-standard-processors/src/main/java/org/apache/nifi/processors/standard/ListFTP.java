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
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processor.util.file.transfer.ListFileTransfer;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.Collection;
import java.util.List;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"list", "ftp", "remote", "ingest", "source", "input", "files"})
@CapabilityDescription("Performs a listing of the files residing on an FTP server. For each file that is found on the remote server, a new FlowFile will be created with the filename attribute "
    + "set to the name of the file on the remote server. This can then be used in conjunction with FetchFTP in order to fetch those files.")
@SeeAlso({FetchFTP.class, GetFTP.class, PutFTP.class})
@WritesAttributes({
    @WritesAttribute(attribute = "ftp.remote.host", description = "The hostname of the FTP Server"),
    @WritesAttribute(attribute = "ftp.remote.port", description = "The port that was connected to on the FTP Server"),
    @WritesAttribute(attribute = "ftp.listing.user", description = "The username of the user that performed the FTP Listing"),
    @WritesAttribute(attribute = ListFile.FILE_OWNER_ATTRIBUTE, description = "The numeric owner id of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_GROUP_ATTRIBUTE, description = "The numeric group id of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_PERMISSIONS_ATTRIBUTE, description = "The read/write/execute permissions of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_SIZE_ATTRIBUTE, description = "The number of bytes in the source file"),
    @WritesAttribute(attribute = ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description = "The timestamp of when the file in the filesystem was" +
            "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
    @WritesAttribute(attribute = "filename", description = "The name of the file on the FTP Server"),
    @WritesAttribute(attribute = "path", description = "The fully qualified name of the directory on the FTP Server from which the file was pulled"),
})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
    + "This allows the Processor to list only files that have been added or modified after "
    + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if "
    + "a new Primary Node is selected, the new node will not duplicate the data that was listed by the previous Primary Node.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListFTP extends ListFileTransfer {

    private static final PropertyDescriptor PORT =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(UNDEFAULTED_PORT).defaultValue("21").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FILE_TRANSFER_LISTING_STRATEGY,
            HOSTNAME,
            PORT,
            USERNAME,
            FTPTransfer.PASSWORD,
            REMOTE_PATH,
            RECORD_WRITER,
            FTPTransfer.RECURSIVE_SEARCH,
            FTPTransfer.FOLLOW_SYMLINK,
            FTPTransfer.FILE_FILTER_REGEX,
            FTPTransfer.PATH_FILTER_REGEX,
            FTPTransfer.IGNORE_DOTTED_FILES,
            FTPTransfer.REMOTE_POLL_BATCH_SIZE,
            FTPTransfer.CONNECTION_TIMEOUT,
            FTPTransfer.DATA_TIMEOUT,
            FTPTransfer.CONNECTION_MODE,
            FTPTransfer.TRANSFER_MODE,
            FTPTransfer.PROXY_CONFIGURATION_SERVICE,
            FTPTransfer.BUFFER_SIZE,
            TARGET_SYSTEM_TIMESTAMP_PRECISION,
            ListedEntityTracker.TRACKING_STATE_CACHE,
            ListedEntityTracker.TRACKING_TIME_WINDOW,
            ListedEntityTracker.INITIAL_LISTING_TARGET,
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
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        return new FTPTransfer(context, getLogger());
    }

    @Override
    protected String getProtocolName() {
        return "ftp";
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        // Use cluster scope so that component can be run on Primary Node Only and can still
        // pick up where it left off, even if the Primary Node changes.
        return Scope.CLUSTER;
    }

    @Override
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
        FTPTransfer.validateProxySpec(validationContext, results);
    }
}
