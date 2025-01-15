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
package org.apache.nifi.processors.smb;


import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2CreateOptions;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskEntry;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.smb.common.SmbProperties.ENABLE_DFS;
import static org.apache.nifi.smb.common.SmbProperties.SMB_DIALECT;
import static org.apache.nifi.smb.common.SmbProperties.TIMEOUT;
import static org.apache.nifi.smb.common.SmbProperties.USE_ENCRYPTION;
import static org.apache.nifi.smb.common.SmbUtils.buildSmbClient;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"samba, smb, cifs, files, put"})
@CapabilityDescription("Writes the contents of a FlowFile to a samba network location. " +
    "Use this processor instead of a cifs mounts if share access control is important." +
    "Configure the Hostname, Share and Directory accordingly: \\\\[Hostname]\\[Share]\\[path\\to\\Directory]")
@SeeAlso({GetSmbFile.class, ListSmb.class, FetchSmb.class})
@ReadsAttributes({@ReadsAttribute(attribute = "filename", description = "The filename to use when writing the FlowFile to the network folder.")})
public class PutSmbFile extends AbstractProcessor {
    public static final String SHARE_ACCESS_NONE = "none";
    public static final String SHARE_ACCESS_READ = "read";
    public static final String SHARE_ACCESS_READDELETE = "read, delete";
    public static final String SHARE_ACCESS_READWRITEDELETE = "read, write, delete";

    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The network host to which files should be written.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SHARE = new PropertyDescriptor.Builder()
            .name("Share")
            .description("The network share to which files should be written. This is the \"first folder\"" +
            "after the hostname: \\\\hostname\\[share]\\dir1\\dir2")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The network folder to which files should be written. This is the remaining relative " +
            "path after the share: \\\\hostname\\share\\[dir1\\dir2]. You may use expression language.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .name("Domain")
            .description("The domain used for authentication. Optional, in most cases username and password is sufficient.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username used for authentication. If no username is set then anonymous authentication is attempted.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password used for authentication. Required if Username is set.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor CREATE_DIRS = new PropertyDescriptor.Builder()
            .name("Create Missing Directories")
            .description("If true, then missing destination directories will be created. If false, flowfiles are penalized and sent to failure.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor SHARE_ACCESS = new PropertyDescriptor.Builder()
            .name("Share Access Strategy")
            .description("Indicates which shared access are granted on the file during the write. " +
                "None is the most restrictive, but the safest setting to prevent corruption.")
            .required(true)
            .defaultValue(SHARE_ACCESS_NONE)
            .allowableValues(SHARE_ACCESS_NONE, SHARE_ACCESS_READ, SHARE_ACCESS_READDELETE, SHARE_ACCESS_READWRITEDELETE)
            .build();
    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(REPLACE_RESOLUTION)
            .allowableValues(REPLACE_RESOLUTION, IGNORE_RESOLUTION, FAIL_RESOLUTION)
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of files to put in each iteration")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();
    public static final PropertyDescriptor RENAME_SUFFIX = new PropertyDescriptor.Builder()
            .name("Temporary Suffix")
            .description("A temporary suffix which will be apended to the filename while it's transfering. After the transfer is complete, the suffix will be removed.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to the output network path are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output network path for some reason are transferred to this relationship")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            HOSTNAME,
            SHARE,
            DIRECTORY,
            DOMAIN,
            USERNAME,
            PASSWORD,
            CREATE_DIRS,
            SHARE_ACCESS,
            CONFLICT_RESOLUTION,
            BATCH_SIZE,
            RENAME_SUFFIX,
            SMB_DIALECT,
            USE_ENCRYPTION,
            ENABLE_DFS,
            TIMEOUT);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private SMBClient smbClient = null; // this gets synchronized when the `connect` method is called
    private Set<SMB2ShareAccess> sharedAccess;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        smbClient = initSmbClient(context);

        switch (context.getProperty(SHARE_ACCESS).getValue()) {
            case SHARE_ACCESS_NONE:
                sharedAccess = Collections.<SMB2ShareAccess>emptySet();
                break;
            case SHARE_ACCESS_READ:
                sharedAccess = EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ);
                break;
            case SHARE_ACCESS_READDELETE:
                sharedAccess = EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ, SMB2ShareAccess.FILE_SHARE_DELETE);
                break;
            case SHARE_ACCESS_READWRITEDELETE:
                sharedAccess = EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ, SMB2ShareAccess.FILE_SHARE_WRITE, SMB2ShareAccess.FILE_SHARE_DELETE);
                break;
        }
    }

    @OnStopped
    public void onStopped() {
        if (smbClient != null) {
            smbClient.close();
            smbClient = null;
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = new ArrayList<>();
        if (validationContext.getProperty(USERNAME).isSet() && !validationContext.getProperty(PASSWORD).isSet()) {
            set.add(new ValidationResult.Builder().explanation("Password must be set if username is supplied.").build());
        }
        return set;
    }

    SMBClient initSmbClient(final ProcessContext context) {
        return buildSmbClient(context);
    }

    private void createMissingDirectoriesRecursevly(ComponentLog logger, DiskShare share, String pathToCreate) {
        List<String> paths = new ArrayList<>();

        java.io.File file = new java.io.File(pathToCreate);
        paths.add(file.getPath());

        while (file.getParent() != null) {
            String parent = file.getParent();
            paths.add(parent);
            file = new java.io.File(parent);
        }

        Collections.reverse(paths);
        for (String path : paths) {
            if (!share.folderExists(path)) {
                logger.debug("Creating folder {}", path);
                share.mkdir(path);
            } else {
                logger.debug("Folder already exists {}. Moving on", path);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        List<FlowFile> flowFiles = session.get(batchSize);
        if ( flowFiles.isEmpty() ) {
            return;
        }
        final ComponentLog logger = getLogger();
        logger.debug("Processing next {} flowfiles", flowFiles.size());

        final String hostname = context.getProperty(HOSTNAME).getValue();
        final String shareName = context.getProperty(SHARE).getValue();

        final String domain = context.getProperty(DOMAIN).getValue();
        final String username = context.getProperty(USERNAME).getValue();
        String password = context.getProperty(PASSWORD).getValue();

        AuthenticationContext ac = null;
        if (username != null && password != null) {
            ac = new AuthenticationContext(
                username,
                password.toCharArray(),
                domain);
        } else {
            ac = AuthenticationContext.anonymous();
        }

        try (Connection connection = smbClient.connect(hostname);
            Session smbSession = connection.authenticate(ac);
            DiskShare share = (DiskShare) smbSession.connectShare(shareName)) {

            for (FlowFile flowFile : flowFiles) {
                final long processingStartTime = System.nanoTime();

                final String destinationDirectory = context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue();
                final String destinationFilename = flowFile.getAttribute(CoreAttributes.FILENAME.key());

                String destinationFullPath;

                // build destination path for the flowfile
                if (destinationDirectory == null || destinationDirectory.trim().isEmpty()) {
                    destinationFullPath = destinationFilename;
                } else {
                    destinationFullPath = new java.io.File(destinationDirectory, destinationFilename).getPath();
                }

                // handle missing directory
                final String destinationFileParentDirectory = new java.io.File(destinationFullPath).getParent();
                final Boolean createMissingDirectories = context.getProperty(CREATE_DIRS).asBoolean();
                if (!createMissingDirectories && !share.folderExists(destinationFileParentDirectory)) {
                    flowFile = session.penalize(flowFile);
                    logger.warn("Penalizing {} and routing to failure as configured because the destination directory ({}) doesn't exist", flowFile, destinationFileParentDirectory);
                    session.transfer(flowFile, REL_FAILURE);
                    continue;
                } else if (!share.folderExists(destinationFileParentDirectory)) {
                    createMissingDirectoriesRecursevly(logger, share, destinationFileParentDirectory);
                }

                // handle conflict resolution
                final String conflictResolution = context.getProperty(CONFLICT_RESOLUTION).getValue();
                if (share.fileExists(destinationFullPath)) {
                    if (conflictResolution.equals(IGNORE_RESOLUTION)) {
                        session.transfer(flowFile, REL_SUCCESS);
                        logger.info("Transferring {} to success as configured because file with same name already exists", flowFile);
                        continue;
                    } else if (conflictResolution.equals(FAIL_RESOLUTION)) {
                        flowFile = session.penalize(flowFile);
                        logger.warn("Penalizing {} and routing to failure as configured because file with the same name already exists", flowFile);
                        session.transfer(flowFile, REL_FAILURE);
                        continue;
                    }
                }

                // handle temporary suffix
                final String renameSuffixValue = context.getProperty(RENAME_SUFFIX).getValue();
                final Boolean renameSuffix = renameSuffixValue != null && !renameSuffixValue.trim().isEmpty();
                String finalDestinationFullPath = destinationFullPath;
                if (renameSuffix) {
                    finalDestinationFullPath += renameSuffixValue;
                }

                // handle the transfer
                try (
                    File shareDestinationFile = share.openFile(
                        finalDestinationFullPath,
                        EnumSet.of(AccessMask.GENERIC_WRITE),
                        EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL),
                        sharedAccess,
                        SMB2CreateDisposition.FILE_OVERWRITE_IF,
                        EnumSet.of(SMB2CreateOptions.FILE_WRITE_THROUGH));
                    OutputStream shareDestinationFileOutputStream = shareDestinationFile.getOutputStream()) {
                    session.exportTo(flowFile, shareDestinationFileOutputStream);
                } catch (Exception e) {
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                    logger.error("Cannot transfer the file. Penalizing {} and routing to 'failure'", flowFile, e);
                    continue;
                }

                // handle the rename
                if (renameSuffix) {
                    try (DiskEntry fileDiskEntry = share.open(
                        finalDestinationFullPath,
                        EnumSet.of(AccessMask.DELETE, AccessMask.GENERIC_WRITE),
                        EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL),
                        sharedAccess,
                        SMB2CreateDisposition.FILE_OPEN,
                        EnumSet.of(SMB2CreateOptions.FILE_WRITE_THROUGH))) {

                        // normalize path slashes for the network share
                        destinationFullPath = destinationFullPath.replace("/", "\\");

                        // rename the file on the share and replace it in case it exists
                        fileDiskEntry.rename(destinationFullPath, true);
                    } catch (Exception e) {
                        flowFile = session.penalize(flowFile);
                        session.transfer(flowFile, REL_FAILURE);
                        logger.error("Cannot rename the file. Penalizing {} and routing to 'failure'", flowFile, e);
                        continue;
                    }
                }

                // handle the success
                final URI provenanceUri = new URI("smb", hostname, "/" + destinationFullPath.replace('\\', '/'), null);
                final long processingTimeInNano = System.nanoTime() - processingStartTime;
                final long processingTimeInMilli = TimeUnit.MILLISECONDS.convert(processingTimeInNano, TimeUnit.NANOSECONDS);
                session.getProvenanceReporter().send(flowFile, provenanceUri.toString(), processingTimeInMilli);
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (Exception e) {
            session.transfer(flowFiles, REL_FAILURE);
            logger.error("Could not establish smb connection", e);
            smbClient.getServerList().unregister(hostname);
        }
    }
}
