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
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processor.util.file.transfer.GetFileTransfer;
import org.apache.nifi.processors.standard.util.FTPTransfer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"FTP", "get", "retrieve", "files", "fetch", "remote", "ingest", "source", "input"})
@CapabilityDescription("Fetches files from an FTP Server and creates FlowFiles from them")
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on the remote server"),
    @WritesAttribute(attribute = "path", description = "The path is set to the path of the file's directory on the remote server. "
            + "For example, if the <Remote Path> property is set to /tmp, files picked up from /tmp will have the path attribute set "
            + "to /tmp. If the <Search Recursively> property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path "
            + "attribute will be set to /tmp/abc/1/2/3"),
    @WritesAttribute(attribute = "file.lastModifiedTime", description = "The date and time that the source file was last modified"),
    @WritesAttribute(attribute = "file.lastAccessTime", description = "The date and time that the file was last accessed. May not work on "
            + "all file systems"),
    @WritesAttribute(attribute = "file.owner", description = "The numeric owner id of the source file"),
    @WritesAttribute(attribute = "file.group", description = "The numeric group id of the source file"),
    @WritesAttribute(attribute = "file.permissions", description = "The read/write/execute permissions of the source file"),
    @WritesAttribute(attribute = "absolute.path", description = "The full/absolute path from where a file was picked up. The current 'path' "
            + "attribute is still populated, but may be a relative path")})
@SeeAlso(PutFTP.class)
public class GetFTP extends GetFileTransfer {

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FTPTransfer.HOSTNAME,
            FTPTransfer.PORT,
            FTPTransfer.USERNAME,
            FTPTransfer.PASSWORD,
            FTPTransfer.CONNECTION_MODE,
            FTPTransfer.TRANSFER_MODE,
            FTPTransfer.REMOTE_PATH,
            FTPTransfer.FILE_FILTER_REGEX,
            FTPTransfer.PATH_FILTER_REGEX,
            FTPTransfer.POLLING_INTERVAL,
            FTPTransfer.RECURSIVE_SEARCH,
            FTPTransfer.FOLLOW_SYMLINK,
            FTPTransfer.IGNORE_DOTTED_FILES,
            FTPTransfer.DELETE_ORIGINAL,
            FTPTransfer.CONNECTION_TIMEOUT,
            FTPTransfer.DATA_TIMEOUT,
            FTPTransfer.MAX_SELECTS,
            FTPTransfer.REMOTE_POLL_BATCH_SIZE,
            FTPTransfer.USE_NATURAL_ORDERING,
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
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        return new FTPTransfer(context, getLogger());
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        FTPTransfer.validateProxySpec(validationContext, results);
        return results;
    }
}
