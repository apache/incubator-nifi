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
import com.hierynomus.mserref.NtStatus;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileBasicInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2CreateOptions;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
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

import java.io.InputStream;
import java.net.URI;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static org.apache.nifi.smb.common.SmbProperties.ENABLE_DFS;
import static org.apache.nifi.smb.common.SmbProperties.SMB_DIALECT;
import static org.apache.nifi.smb.common.SmbProperties.TIMEOUT;
import static org.apache.nifi.smb.common.SmbProperties.USE_ENCRYPTION;
import static org.apache.nifi.smb.common.SmbUtils.buildSmbClient;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"samba, smb, cifs, files, get"})
@CapabilityDescription("Reads file from a samba network location to FlowFiles. " +
    "Use this processor instead of a cifs mounts if share access control is important. " +
    "Configure the Hostname, Share and Directory accordingly: \\\\[Hostname]\\[Share]\\[path\\to\\Directory]")
@SeeAlso({PutSmbFile.class, ListSmb.class, FetchSmb.class})
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on the network share"),
        @WritesAttribute(attribute = "path", description = "The path is set to the relative path of the file's network share name. For example, "
                + "if the input is set to \\\\hostname\\share\\tmp, files picked up from \\tmp will have the path attribute set to tmp"),
        @WritesAttribute(attribute = "file.creationTime", description = "The date and time that the file was created. May not work on all file systems"),
        @WritesAttribute(attribute = "file.lastModifiedTime", description = "The date and time that the file was last modified. May not work on all "
                + "file systems"),
        @WritesAttribute(attribute = "file.lastAccessTime", description = "The date and time that the file was last accessed. May not work on all "
                + "file systems"),
        @WritesAttribute(attribute = "absolute.path", description = "The full path from where a file was picked up. This includes "
                + "the hostname and the share name")})
public class GetSmbFile extends AbstractProcessor {
    public static final String SHARE_ACCESS_NONE = "none";
    public static final String SHARE_ACCESS_READ = "read";
    public static final String SHARE_ACCESS_READDELETE = "read, delete";
    public static final String SHARE_ACCESS_READWRITEDELETE = "read, write, delete";

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
            "path after the share: \\\\hostname\\share\\[dir1\\dir2].")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
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
    public static final PropertyDescriptor SHARE_ACCESS = new PropertyDescriptor.Builder()
            .name("Share Access Strategy")
            .description("Indicates which shared access are granted on the file during the read. " +
                "None is the most restrictive, but the safest setting to prevent corruption.")
            .required(true)
            .defaultValue(SHARE_ACCESS_NONE)
            .allowableValues(SHARE_ACCESS_NONE, SHARE_ACCESS_READ, SHARE_ACCESS_READDELETE, SHARE_ACCESS_READWRITEDELETE)
            .build();
    public static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether or not to pull files from subdirectories")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor KEEP_SOURCE_FILE = new PropertyDescriptor.Builder()
            .name("Keep Source File")
            .description("If true, the file is not deleted after it has been copied to the Content Repository; "
                    + "this causes the file to be picked up continually and is useful for testing purposes.  "
                    + "If not keeping original NiFi will need write permissions on the directory it is pulling "
                    + "from otherwise it will ignore the file.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor PATH_FILTER = new PropertyDescriptor.Builder()
            .name("Path Filter")
            .description("When " + RECURSE.getName() + " is true, then only subdirectories whose path matches the given regular expression will be scanned")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor IGNORE_HIDDEN_FILES = new PropertyDescriptor.Builder()
            .name("Ignore Hidden Files")
            .description("Indicates whether or not hidden files should be ignored")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();
    public static final PropertyDescriptor POLLING_INTERVAL = new PropertyDescriptor.Builder()
            .name("Polling Interval")
            .description("Indicates how long to wait before performing a directory listing")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of files to pull in each iteration")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_SIZE_ATTRIBUTE = "file.size";

    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All files are routed to success").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        HOSTNAME,
        SHARE,
        DIRECTORY,
        DOMAIN,
        USERNAME,
        PASSWORD,
        SHARE_ACCESS,
        FILE_FILTER,
        PATH_FILTER,
        BATCH_SIZE,
        KEEP_SOURCE_FILE,
        RECURSE,
        POLLING_INTERVAL,
        IGNORE_HIDDEN_FILES,
        SMB_DIALECT,
        USE_ENCRYPTION,
        ENABLE_DFS,
        TIMEOUT
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS
    );


    private final BlockingQueue<String> fileQueue = new LinkedBlockingQueue<>();
    private final Set<String> inProcess = new HashSet<>();    // guarded by queueLock
    private final Set<String> recentlyProcessed = new HashSet<>();    // guarded by queueLock
    private final Lock queueLock = new ReentrantLock();

    private final Lock listingLock = new ReentrantLock();

    private final AtomicLong queueLastUpdated = new AtomicLong(0L);

    private SMBClient smbClient = null; // this gets synchronized when the `connect` method is called

    private Pattern filePattern;
    private Pattern pathPattern;
    private boolean ignoreHidden;
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

        initiateFilterFile(context);
        fileQueue.clear();

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

    private void initiateFilterFile(final ProcessContext context) {
        final String filePatternStr = context.getProperty(FILE_FILTER).getValue();
        filePattern = filePatternStr == null ? null : Pattern.compile(filePatternStr);
        final String pathPatternStr = context.getProperty(PATH_FILTER).getValue();
        pathPattern = pathPatternStr == null ? null : Pattern.compile(pathPatternStr);
        ignoreHidden = context.getProperty(IGNORE_HIDDEN_FILES).asBoolean().booleanValue();
    }

    private boolean filterFile(final String directory, final String filename, final long fileAttributes) {
        if (pathPattern != null && !pathPattern.matcher(directory).matches()) {
            return false;
        }
        if (filePattern != null && !filePattern.matcher(filename).matches()) {
            return false;
        }
        if (ignoreHidden && (fileAttributes & FileAttributes.FILE_ATTRIBUTE_HIDDEN.getValue()) != 0) {
            return false;
        }
        return true;
    }

    private Set<String> performListing(final DiskShare diskShare, final String directory, final String filter, final boolean recurseSubdirectories) {
        final Set<String> queue = new HashSet<>();
        if (!diskShare.folderExists(directory)) {
            return queue;
        }

        final List<FileIdBothDirectoryInformation> children = diskShare.list(directory);
        if (children == null) {
            return queue;
        }

        for (final FileIdBothDirectoryInformation child : children) {
            final String filename = child.getFileName();
            if (filename.equals(".") || filename.equals("..")) {
                continue;
            }
            String fullPath;
            if (directory.isEmpty()) {
                fullPath = filename;
            } else {
                fullPath = directory + "\\" + filename;
            }
            final long fileAttributes = child.getFileAttributes();
            if ((fileAttributes & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue()) != 0) {
                if (recurseSubdirectories) {
                    queue.addAll(performListing(diskShare, fullPath, filter, true));
                }
            } else if (filterFile(directory, filename, fileAttributes)) {
                queue.add(fullPath);
            }
        }

        return queue;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final ComponentLog logger = getLogger();

        final String hostname = context.getProperty(HOSTNAME).getValue();
        final String shareName = context.getProperty(SHARE).getValue();

        final String domain = context.getProperty(DOMAIN).getValue();
        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();

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
                String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
                if (directory == null) {
                    directory = "";
                }
                final boolean keepingSourceFile = context.getProperty(KEEP_SOURCE_FILE).asBoolean();
                final String filter = context.getProperty(FILE_FILTER).getValue();

                if (fileQueue.size() < 100) {
                    final long pollingMillis = context.getProperty(POLLING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
                    if ((queueLastUpdated.get() < System.currentTimeMillis() - pollingMillis) && listingLock.tryLock()) {
                        try {
                            final Set<String> listing = performListing(share, directory, filter, context.getProperty(RECURSE).asBoolean().booleanValue());

                            queueLock.lock();
                            try {
                                listing.removeAll(inProcess);
                                if (!keepingSourceFile) {
                                    listing.removeAll(recentlyProcessed);
                                }

                                fileQueue.clear();
                                fileQueue.addAll(listing);

                                queueLastUpdated.set(System.currentTimeMillis());
                                recentlyProcessed.clear();

                                if (listing.isEmpty()) {
                                    context.yield();
                                }
                            } finally {
                                queueLock.unlock();
                            }
                        } finally {
                            listingLock.unlock();
                        }
                    }
                }

                final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
                final List<String> files = new ArrayList<>(batchSize);
                queueLock.lock();
                try {
                    fileQueue.drainTo(files, batchSize);
                    if (files.isEmpty()) {
                        return;
                    } else {
                        inProcess.addAll(files);
                    }
                } finally {
                    queueLock.unlock();
                }

                final ListIterator<String> itr = files.listIterator();
                FlowFile flowFile = null;

                try {
                    while (itr.hasNext()) {
                        final String file = itr.next();
                        final String[] fileSplits = file.split("\\\\");
                        final String filename = fileSplits[fileSplits.length - 1];
                        final String filePath = String.join("\\", Arrays.copyOf(fileSplits, fileSplits.length - 1));
                        final URI uri = new URI("smb", hostname, "/" + file.replace('\\', '/'), null);

                        flowFile = session.create();
                        final long importStart = System.nanoTime();

                        try (File f = share.openFile(
                                file,
                                EnumSet.of(AccessMask.GENERIC_READ),
                                EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL),
                                sharedAccess,
                                SMB2CreateDisposition.FILE_OPEN,
                            EnumSet.of(SMB2CreateOptions.FILE_SEQUENTIAL_ONLY));
                            InputStream is = f.getInputStream()) {

                            flowFile = session.importFrom(is, flowFile);

                            final long importNanos = System.nanoTime() - importStart;
                            final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);
                            final FileAllInformation fileInfo = f.getFileInformation();
                            final FileBasicInformation fileBasicInfo = fileInfo.getBasicInformation();
                            final long fileSize = fileInfo.getStandardInformation().getEndOfFile();

                            final Map<String, String> attributes = new HashMap<>();
                            attributes.put(CoreAttributes.FILENAME.key(), filename);
                            attributes.put(CoreAttributes.PATH.key(), filePath);
                            attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), "\\\\" + hostname + "\\" + shareName + "\\" + file);
                            attributes.put(FILE_CREATION_TIME_ATTRIBUTE, dateFormatter.format(fileBasicInfo.getCreationTime().toInstant().atZone(ZoneId.systemDefault())));
                            attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, dateFormatter.format(fileBasicInfo.getLastAccessTime().toInstant().atZone(ZoneId.systemDefault())));
                            attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, dateFormatter.format(fileBasicInfo.getLastWriteTime().toInstant().atZone(ZoneId.systemDefault())));
                            attributes.put(FILE_SIZE_ATTRIBUTE, String.valueOf(fileSize));
                            attributes.put(HOSTNAME.getName(), hostname);
                            attributes.put(SHARE.getName(), shareName);

                            flowFile = session.putAllAttributes(flowFile, attributes);
                            session.getProvenanceReporter().receive(flowFile, uri.toString(), importMillis);

                            session.transfer(flowFile, REL_SUCCESS);
                        } catch (SMBApiException e) {
                            // do not fail whole batch if a single file cannot be accessed
                            if (e.getStatus() == NtStatus.STATUS_SHARING_VIOLATION) {
                                logger.info("Could not acquire sharing access for file {}", file);
                                if (flowFile != null) {
                                    session.remove(flowFile);
                                }
                                continue;
                            } else {
                                throw e;
                            }
                        }

                        try {
                            if (!keepingSourceFile) {
                                share.rm(file);
                            }
                        } catch (SMBApiException e) {
                            logger.error("Could not remove file {}", file);
                        }

                        if (!isScheduled()) {  // if processor stopped, put the rest of the files back on the queue.
                            queueLock.lock();
                            try {
                                while (itr.hasNext()) {
                                    final String nextFile = itr.next();
                                    fileQueue.add(nextFile);
                                    inProcess.remove(nextFile);
                                }
                            } finally {
                                queueLock.unlock();
                            }
                        }
                    }

                    session.commitAsync();
                } catch (final Exception e) {
                    logger.error("Failed to retrieve files due to {}", e);

                    // anything that we've not already processed needs to be put back on the queue
                    if (flowFile != null) {
                        session.remove(flowFile);
                    }
                } finally {
                    queueLock.lock();
                    try {
                        inProcess.removeAll(files);
                        recentlyProcessed.addAll(files);
                    } finally {
                        queueLock.unlock();
                    }
                }
        } catch (Exception e) {
            logger.error("Could not establish smb connection", e);
            context.yield();
            smbClient.getServerList().unregister(hostname);
        }
    }
}
