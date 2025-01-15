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
package org.apache.nifi.processors.hadoop;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.util.GSSExceptionRollbackYieldSessionHandler;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"hadoop", "HCFS", "HDFS", "delete", "remove", "filesystem"})
@CapabilityDescription("Deletes one or more files or directories from HDFS. The path can be provided as an attribute from an incoming FlowFile, "
        + "or a statically set path that is periodically removed. If this processor has an incoming connection, it"
        + "will ignore running on a periodic basis and instead rely on incoming FlowFiles to trigger a delete. "
        + "Note that you may use a wildcard character to match multiple files or directories. If there are"
        + " no incoming connections no flowfiles will be transfered to any output relationships.  If there is an incoming"
        + " flowfile then provided there are no detected failures it will be transferred to success otherwise it will be sent to false. If"
        + " knowledge of globbed files deleted is necessary use ListHDFS first to produce a specific list of files to delete. ")
@Restricted(restrictions = {
        @Restriction(
                requiredPermission = RequiredPermission.WRITE_DISTRIBUTED_FILESYSTEM,
                explanation = "Provides operator the ability to delete any file that NiFi has access to in HDFS or the local filesystem.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "hdfs.filename", description = "HDFS file to be deleted. "
                + "If multiple files are deleted, then only the last filename is set."),
        @WritesAttribute(attribute = "hdfs.path", description = "HDFS Path specified in the delete request. "
                + "If multiple paths are deleted, then only the last path is set."),
        @WritesAttribute(attribute = "hadoop.file.url", description = "The hadoop url for the file to be deleted."),
        @WritesAttribute(attribute = "hdfs.error.message", description = "HDFS error message related to the hdfs.error.code")
})
@SeeAlso({ListHDFS.class, PutHDFS.class})
public class DeleteHDFS extends AbstractHadoopProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("When an incoming flowfile is used then if there are no errors invoking delete the flowfile will route here.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("When an incoming flowfile is used and there is a failure while deleting then the flowfile will route here.")
            .build();

    public static final PropertyDescriptor FILE_OR_DIRECTORY = new PropertyDescriptor.Builder()
            .name("file_or_directory")
            .displayName("Path")
            .description("The HDFS file or directory to delete. A wildcard expression may be used to only delete certain files")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor RECURSIVE = new PropertyDescriptor.Builder()
            .name("recursive")
            .displayName("Recursive")
            .description("Remove contents of a non-empty directory recursively")
            .allowableValues("true", "false")
            .required(true)
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected final Pattern GLOB_PATTERN = Pattern.compile("\\[|\\]|\\*|\\?|\\^|\\{|\\}|\\\\c");
    protected final Matcher GLOB_MATCHER = GLOB_PATTERN.matcher("");

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Stream.concat(
            getCommonPropertyDescriptors().stream(),
            Stream.of(
                FILE_OR_DIRECTORY,
                RECURSIVE
            )
    ).toList();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile originalFlowFile = session.get();

        // If this processor has an incoming connection, then do not run unless a
        // FlowFile is actually sent through
        if (originalFlowFile == null && context.hasIncomingConnection()) {
            return;
        }

        // We need a FlowFile to report provenance correctly.
        final FlowFile finalFlowFile = originalFlowFile != null ? originalFlowFile : session.create();
        final String fileOrDirectoryName = getPath(context, session, finalFlowFile);
        final FileSystem fileSystem = getFileSystem();
        final UserGroupInformation ugi = getUserGroupInformation();

        ugi.doAs((PrivilegedAction<Object>) () -> {
            FlowFile flowFile = finalFlowFile;
            try {
                // Check if the user has supplied a file or directory pattern
                List<Path> pathList = Lists.newArrayList();
                if (GLOB_MATCHER.reset(fileOrDirectoryName).find()) {
                    FileStatus[] fileStatuses = fileSystem.globStatus(new Path(fileOrDirectoryName));
                    if (fileStatuses != null) {
                        for (FileStatus fileStatus : fileStatuses) {
                            pathList.add(fileStatus.getPath());
                        }
                    }
                } else {
                    pathList.add(new Path(fileOrDirectoryName));
                }

                int failedPath = 0;
                for (Path path : pathList) {
                    if (fileSystem.exists(path)) {
                        try {
                            Map<String, String> attributes = Maps.newHashMapWithExpectedSize(2);
                            attributes.put(getAttributePrefix() + ".filename", path.getName());
                            attributes.put(getAttributePrefix() + ".path", path.getParent().toString());
                            flowFile = session.putAllAttributes(flowFile, attributes);

                            fileSystem.delete(path, isRecursive(context, session));
                            getLogger().debug("For flowfile {} Deleted file at path {} with name {}", originalFlowFile, path.getParent(), path.getName());
                            final Path qualifiedPath = path.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
                            flowFile = session.putAttribute(flowFile, HADOOP_FILE_URL_ATTRIBUTE, qualifiedPath.toString());
                            session.getProvenanceReporter().invokeRemoteProcess(flowFile, qualifiedPath.toString());
                        } catch (IOException ioe) {
                            if (handleAuthErrors(ioe, session, context, new GSSExceptionRollbackYieldSessionHandler())) {
                                return null;
                            } else {
                                // One possible scenario is that the IOException is permissions based, however it would be impractical to check every possible
                                // external HDFS authorization tool (Ranger, Sentry, etc). Local ACLs could be checked but the operation would be expensive.
                                getLogger().warn("Failed to delete file or directory", ioe);

                                Map<String, String> attributes = Maps.newHashMapWithExpectedSize(1);
                                // The error message is helpful in understanding at a flowfile level what caused the IOException (which ACL is denying the operation, e.g.)
                                attributes.put(getAttributePrefix() + ".error.message", ioe.getMessage());

                                session.transfer(session.putAllAttributes(session.clone(flowFile), attributes), getFailureRelationship());
                                failedPath++;
                            }
                        }
                    }
                }

                if (failedPath == 0) {
                    session.transfer(flowFile, getSuccessRelationship());
                } else {
                    // If any path has been failed to be deleted, remove the FlowFile as it's been cloned and sent to failure.
                    session.remove(flowFile);
                }
            } catch (IOException e) {
                if (handleAuthErrors(e, session, context, new GSSExceptionRollbackYieldSessionHandler())) {
                    return null;
                } else {
                    getLogger().error("Error processing delete for flowfile {} due to {}", flowFile, e.getMessage(), e);
                    session.transfer(flowFile, getFailureRelationship());
                }
            }

            return null;
        });

    }

    protected Relationship getSuccessRelationship() {
        return REL_SUCCESS;
    }

    protected Relationship getFailureRelationship() {
        return REL_FAILURE;
    }

    protected boolean isRecursive(final ProcessContext context, final ProcessSession session) {
        return context.getProperty(RECURSIVE).asBoolean();
    }

    protected String getPath(final ProcessContext context, final ProcessSession session, final FlowFile finalFlowFile) {
        return getNormalizedPath(context, FILE_OR_DIRECTORY, finalFlowFile).toString();
    }

    protected String getAttributePrefix() {
        return "hdfs";
    }
}