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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.TailFile.TailFileState.StateKeys;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import static org.apache.nifi.expression.ExpressionLanguageScope.ENVIRONMENT;
import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;
import static org.apache.nifi.processor.util.StandardValidators.DATA_SIZE_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.REGULAR_EXPRESSION_VALIDATOR;

// note: it is important that this Processor is not marked as @SupportsBatching because the session commits must complete before persisting state locally; otherwise, data loss may occur
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"tail", "file", "log", "text", "source"})
@CapabilityDescription("\"Tails\" a file, or a list of files, ingesting data from the file as it is written to the file. The file is expected to be textual. Data is ingested only when a "
        + "new line is encountered (carriage return or new-line character or combination). If the file to tail is periodically \"rolled over\", as is generally the case "
        + "with log files, an optional Rolling Filename Pattern can be used to retrieve data from files that have rolled over, even if the rollover occurred while NiFi "
        + "was not running (provided that the data still exists upon restart of NiFi). It is generally advisable to set the Run Schedule to a few seconds, rather than running "
        + "with the default value of 0 secs, as this Processor will consume a lot of resources if scheduled very aggressively. At this time, this Processor does not support "
        + "ingesting files that have been compressed when 'rolled over'.")
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER}, description = "Stores state about where in the Tailed File it left off so that on restart it does not have to duplicate data. "
        + "State is stored either local or clustered depend on the <File Location> property.")
@WritesAttribute(attribute = "tailfile.original.path", description = "Path of the original file the flow file comes from.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.READ_FILESYSTEM,
                        explanation = "Provides operator the ability to read from any file that NiFi has access to.")
        }
)
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "30 sec")
public class TailFile extends AbstractProcessor {

    static final String MAP_PREFIX = "file.";
    private static final byte[] NEW_LINE_BYTES = "\n".getBytes(StandardCharsets.UTF_8);

    static final AllowableValue LOCATION_LOCAL = new AllowableValue("Local", "Local",
            "State is stored locally. Each node in a cluster will tail a different file.");
    static final AllowableValue LOCATION_REMOTE = new AllowableValue("Remote", "Remote",
            "State is located on a remote resource. This Processor will store state across the cluster so that "
            + "it can be run on Primary Node Only and a new Primary Node can pick up where the last one left off.");

    static final AllowableValue MODE_SINGLEFILE = new AllowableValue("Single file", "Single file",
            "In this mode, only the one file indicated in the 'Files to tail' property will be watched by the processor."
            + " In this mode, the file may not exist when starting the processor.");
    static final AllowableValue MODE_MULTIFILE = new AllowableValue("Multiple files", "Multiple files",
            "In this mode, the 'Files to tail' property accepts a regular expression and the processor will look"
            + " for files in 'Base directory' to list the files to tail by the processor.");

    static final AllowableValue START_BEGINNING_OF_TIME = new AllowableValue("Beginning of Time", "Beginning of Time",
            "Start with the oldest data that matches the Rolling Filename Pattern and then begin reading from the File to Tail");
    static final AllowableValue START_CURRENT_FILE = new AllowableValue("Beginning of File", "Beginning of File",
            "Start with the beginning of the File to Tail. Do not ingest any data that has already been rolled over");
    static final AllowableValue START_CURRENT_TIME = new AllowableValue("Current Time", "Current Time",
            "Start with the data at the end of the File to Tail. Do not ingest any data thas has already been rolled over or any "
            + "data in the File to Tail that has already been written.");

    static final PropertyDescriptor BASE_DIRECTORY = new Builder()
            .name("tail-base-directory")
            .displayName("Base directory")
            .description("Base directory used to look for files to tail. This property is required when using Multifile mode.")
            .expressionLanguageSupported(ENVIRONMENT)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor MODE = new Builder()
            .name("tail-mode")
            .displayName("Tailing mode")
            .description("Mode to use: single file will tail only one file, multiple file will look for a list of file. In Multiple mode"
                    + " the Base directory is required.")
            .expressionLanguageSupported(NONE)
            .required(true)
            .allowableValues(MODE_SINGLEFILE, MODE_MULTIFILE)
            .defaultValue(MODE_SINGLEFILE.getValue())
            .build();

    static final PropertyDescriptor FILENAME = new Builder()
            .displayName("File(s) to Tail")
            .name("File to Tail")
            .description("Path of the file to tail in case of single file mode. If using multifile mode, regular expression to find files "
                    + "to tail in the base directory. In case recursivity is set to true, the regular expression will be used to match the "
                    + "path starting from the base directory (see additional details for examples).")
            .expressionLanguageSupported(ENVIRONMENT)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .required(true)
            .build();

    static final PropertyDescriptor ROLLING_FILENAME_PATTERN = new Builder()
            .name("Rolling Filename Pattern")
            .description("If the file to tail \"rolls over\" as would be the case with log files, this filename pattern will be used to "
                    + "identify files that have rolled over so that if NiFi is restarted, and the file has rolled over, it will be able to pick up where it left off. "
                    + "This pattern supports wildcard characters * and ?, it also supports the notation ${filename} to specify a pattern based on the name of the file "
                    + "(without extension), and will assume that the files that have rolled over live in the same directory as the file being tailed. "
                    + "The same glob pattern will be used for all files.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(NONE)
            .required(false)
            .build();

    static final PropertyDescriptor POST_ROLLOVER_TAIL_PERIOD = new Builder()
        .name("Post-Rollover Tail Period")
        .description("When a file is rolled over, the processor will continue tailing the rolled over file until it has not been modified for this amount of time. " +
            "This allows for another process to rollover a file, and then flush out any buffered data. Note that when this value is set, and the tailed file rolls over, " +
            "the new file will not be tailed until the old file has not been modified for the configured amount of time. Additionally, when using this capability, in order to avoid data " +
            "duplication, this period must be set longer than the Processor's Run Schedule, and the Processor must not be stopped after the file being tailed has been " +
            "rolled over and before the data has been fully consumed. Otherwise, the data may be duplicated, as the entire file may be written out as the contents of a single FlowFile.")
        .required(false)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .defaultValue("0 sec")
        .build();

    static final PropertyDescriptor STATE_LOCATION = new Builder()
            .displayName("State Location")
            .name("File Location") //retained name of property for backward compatibility of configs
            .description("Specifies where the state is located either local or cluster so that state can be stored "
                    + "appropriately in order to ensure that all data is consumed without duplicating data upon restart of NiFi")
            .required(true)
            .allowableValues(LOCATION_LOCAL, LOCATION_REMOTE)
            .defaultValue(LOCATION_LOCAL.getValue())
            .build();

    static final PropertyDescriptor START_POSITION = new Builder()
            .name("Initial Start Position")
            .description("When the Processor first begins to tail data, this property specifies where the Processor should begin reading data. Once data has been ingested from a file, "
                    + "the Processor will continue from the last point from which it has received data.")
            .allowableValues(START_BEGINNING_OF_TIME, START_CURRENT_FILE, START_CURRENT_TIME)
            .defaultValue(START_CURRENT_FILE.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor RECURSIVE = new Builder()
            .name("tailfile-recursive-lookup")
            .displayName("Recursive lookup")
            .description("When using Multiple files mode, this property defines if files must be listed recursively or not"
                    + " in the base directory.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    static final PropertyDescriptor LOOKUP_FREQUENCY = new Builder()
            .name("tailfile-lookup-frequency")
            .displayName("Lookup frequency")
            .description("Only used in Multiple files mode. It specifies the minimum "
                    + "duration the processor will wait before listing again the files to tail.")
            .required(false)
            .defaultValue("10 minutes")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAXIMUM_AGE = new Builder()
            .name("tailfile-maximum-age")
            .displayName("Maximum age")
            .description("Only used in Multiple files mode. It specifies the necessary "
                    + "minimum duration to consider that no new messages will be appended in a file regarding its last "
                    + "modification date. This should not be set too low to avoid duplication of data in case new messages "
                    + "are appended at a lower frequency.")
            .required(false)
            .defaultValue("24 hours")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor REREAD_ON_NUL = new Builder()
            .name("reread-on-nul")
            .displayName("Reread when NUL encountered")
            .description("If this option is set to 'true', when a NUL character is read, the processor will yield and try to read the same part again later. "
                + "(Note: Yielding may delay the processing of other files tailed by this processor, not just the one with the NUL character.) "
                + "The purpose of this flag is to allow users to handle cases where reading a file may return temporary NUL values. "
                + "NFS for example may send file contents out of order. In this case the missing parts are temporarily replaced by NUL values. "
                + "CAUTION! If the file contains legitimate NUL values, setting this flag causes this processor to get stuck indefinitely. "
                + "For this reason users should refrain from using this feature if they can help it and try to avoid having the target file on a file system where reads are unreliable.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor LINE_START_PATTERN = new Builder()
        .name("Line Start Pattern")
        .displayName("Line Start Pattern")
        .description("A Regular Expression to match against the start of a log line. If specified, any line that matches the expression, and any following lines, will be buffered until another line" +
            " matches the Expression. In doing this, we can avoid splitting apart multi-line messages in the file. This assumes that the data is in UTF-8 format.")
        .required(false)
        .addValidator(REGULAR_EXPRESSION_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .dependsOn(MODE, MODE_SINGLEFILE)
        .build();

    static final PropertyDescriptor MAX_BUFFER_LENGTH = new Builder()
        .name("Max Buffer Size")
        .displayName("Max Buffer Size")
        .description("When using the Line Start Pattern, there may be situations in which the data in the file being tailed never matches the Regular Expression. This would result in the processor " +
            "buffering all data from the tailed file, which can quickly exhaust the heap. To avoid this, the Processor will buffer only up to this amount of data before flushing the buffer, even if" +
            " it means ingesting partial data from the file.")
        .required(true)
        .addValidator(DATA_SIZE_VALIDATOR)
        .expressionLanguageSupported(NONE)
        .defaultValue("64 KB")
        .dependsOn(LINE_START_PATTERN)
        .build();

    static final PropertyDescriptor PRE_ALLOCATED_BUFFER_SIZE = new Builder()
            .name("pre-allocated-buffer-size")
            .displayName("Pre-Allocated Buffer Size")
            .description("Sets the amount of memory that is pre-allocated for each tailed file.")
            .required(true)
            .addValidator(DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(NONE)
            .defaultValue("65536 B")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            MODE,
            FILENAME,
            ROLLING_FILENAME_PATTERN,
            POST_ROLLOVER_TAIL_PERIOD,
            BASE_DIRECTORY,
            START_POSITION,
            STATE_LOCATION,
            RECURSIVE,
            LOOKUP_FREQUENCY,
            MAXIMUM_AGE,
            REREAD_ON_NUL,
            LINE_START_PATTERN,
            PRE_ALLOCATED_BUFFER_SIZE,
            MAX_BUFFER_LENGTH
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are routed to this Relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
        REL_SUCCESS
    );

    private volatile Map<String, TailFileObject> states = new HashMap<>();
    private final AtomicLong lastLookup = new AtomicLong(0L);
    private final AtomicBoolean isMultiChanging = new AtomicBoolean(false);
    private volatile boolean requireStateLookup = true;

    private final ByteArrayOutputStream linesBuffer = new ByteArrayOutputStream();
    private volatile Pattern lineStartPattern;
    private volatile long maxBufferBytes;
    private volatile int preAllocatedBufferSize;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (isConfigurationRestored() && FILENAME.equals(descriptor)) {
            states = new HashMap<>();
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        if (context.getProperty(MODE).getValue().equals(MODE_MULTIFILE.getValue())) {
            String path = context.getProperty(BASE_DIRECTORY).evaluateAttributeExpressions().getValue();
            if (path == null) {
                results.add(new ValidationResult.Builder()
                    .subject(BASE_DIRECTORY.getName())
                    .valid(false)
                    .explanation("Base directory property cannot be empty in Multifile mode.")
                    .build());
            } else if (!new File(path).isDirectory()) {
                results.add(new ValidationResult.Builder()
                    .subject(BASE_DIRECTORY.getName())
                    .valid(false)
                    .explanation(path + " is not a directory.")
                    .build());
            }
        }

        return results;
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange() {
        this.requireStateLookup = true;
    }

    private List<String> lookup(final ProcessContext context) {
        // set last lookup to now
        lastLookup.set(new Date().getTime());

        // maxAge
        long maxAge = context.getProperty(MAXIMUM_AGE).getValue() == null ? Long.MAX_VALUE : context.getProperty(MAXIMUM_AGE).asTimePeriod(TimeUnit.MILLISECONDS);

        // get list of files to tail
        List<String> filesToTail = new ArrayList<>();

        if (context.getProperty(MODE).getValue().equals(MODE_MULTIFILE.getValue())) {
            filesToTail.addAll(getFilesToTail(context.getProperty(BASE_DIRECTORY).evaluateAttributeExpressions().getValue(),
                context.getProperty(FILENAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(RECURSIVE).asBoolean(),
                maxAge));
        } else {
            filesToTail.add(context.getProperty(FILENAME).evaluateAttributeExpressions().getValue());
        }
        return filesToTail;
    }

    @OnScheduled
    public void compileRegex(final ProcessContext context) {
        final String regex = context.getProperty(LINE_START_PATTERN).getValue();
        lineStartPattern = (regex == null) ? null : Pattern.compile(regex);

        this.maxBufferBytes = context.getProperty(MAX_BUFFER_LENGTH).asDataSize(DataUnit.B).longValue();
        this.preAllocatedBufferSize = context.getProperty(PRE_ALLOCATED_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
    }

    @OnScheduled
    public void recoverState(final ProcessContext context) throws IOException {
        // set isMultiChanging
        isMultiChanging.set(context.getProperty(MODE).getValue().equals(MODE_MULTIFILE.getValue()));

        List<String> filesToTail = lookup(context);

        final Scope scope = getStateScope(context);
        final StateMap stateMap = context.getStateManager().getState(scope);

        if (stateMap.getStateVersion().isEmpty() || stateMap.toMap().isEmpty()) {
            //state has been cleared or never stored so recover as 'empty state'
            initStates(filesToTail, Collections.emptyMap(), true);
            recoverState(filesToTail, Collections.emptyMap());
            return;
        }

        final Map<String, String> statesMap = stateMap.toMap();

        initStates(filesToTail, statesMap, false);
        recoverState(filesToTail, statesMap);
    }

    private void initStates(final List<String> filesToTail, final Map<String, String> statesMap, final boolean isCleared) {
        int fileIndex = 0;

        if (isCleared) {
            states.clear();
        } else {
            // we have to deal with the case where NiFi has been restarted. In this
            // case 'states' object is empty but the statesMap is not. So we have to
            // put back the files we already know about in 'states' object before
            // doing the recovery
            if (states.isEmpty() && !statesMap.isEmpty()) {
                for (Entry<String, String> entry : statesMap.entrySet()) {
                    final String key = entry.getKey();
                    final String value = entry.getValue();
                    if (key.endsWith(TailFileState.StateKeys.FILENAME) && filesToTail.contains(value)) {
                        int index = Integer.parseInt(key.split("\\.")[1]);
                        states.put(value, new TailFileObject(index, statesMap, preAllocatedBufferSize));
                    }
                }
            }

            // first, we remove the files that are no longer present
            final List<String> toBeRemoved = new ArrayList<>();
            for (Entry<String, TailFileObject> entry : states.entrySet()) {
                final String filePath = entry.getKey();
                final TailFileObject tailFileObject = entry.getValue();
                if (!filesToTail.contains(filePath)) {
                    toBeRemoved.add(filePath);
                    cleanReader(tailFileObject);
                }
            }
            toBeRemoved.forEach(states.keySet()::remove);

            // then we need to get the highest ID used so far to be sure
            // we don't mix different files in case we add new files to tail
            for (TailFileObject tfo : states.values()) {
                if (fileIndex <= tfo.getFilenameIndex()) {
                    fileIndex = tfo.getFilenameIndex() + 1;
                }
            }

        }

        for (String filename : filesToTail) {
            if (isCleared || !states.containsKey(filename)) {
                final TailFileState tailFileState = new TailFileState(filename, null, null, 0L, 0L, 0L, null, ByteBuffer.allocate(preAllocatedBufferSize));
                states.put(filename, new TailFileObject(fileIndex, tailFileState));

                fileIndex++;
            }
        }
    }

    private void recoverState(final List<String> filesToTail, final Map<String, String> map) throws IOException {
        for (String file : filesToTail) {
            recoverState(map, file);
        }
    }

    /**
     * Method to list the files to tail according to the given base directory
     * and using the user-provided regular expression
     * @param baseDir base directory to recursively look into
     * @param fileRegex expression regular used to match files to tail
     * @param isRecursive true if looking for file recursively, false otherwise
     * @return List of files to tail
     */
    private List<String> getFilesToTail(final String baseDir, String fileRegex, boolean isRecursive, long maxAge) {
        final Collection<File> files = FileUtils.listFiles(new File(baseDir), null, isRecursive);
        final List<String> result = new ArrayList<>();

        final String baseDirNoTrailingSeparator = baseDir.endsWith(File.separator) ? baseDir.substring(0, baseDir.length() - 1) : baseDir;
        final String fullRegex;
        if (File.separator.equals("/")) {
            // handle unix-style paths
            fullRegex = baseDirNoTrailingSeparator + File.separator + fileRegex;
        } else {
            // handle windows-style paths, need to quote backslash characters
            fullRegex = baseDirNoTrailingSeparator + Pattern.quote(File.separator) + fileRegex;
        }
        final Pattern p = Pattern.compile(fullRegex);

        for (File file : files) {
            final String path = file.getPath();
            if (p.matcher(path).matches()) {
                if (isMultiChanging.get()) {
                    if ((new Date().getTime() - file.lastModified()) < maxAge) {
                        result.add(path);
                    }
                } else {
                    result.add(path);
                }
            }
        }

        return result;
    }

    /**
     * Updates member variables to reflect the "expected recovery checksum" and
     * seek to the appropriate location in the tailed file, updating our
     * checksum, so that we are ready to proceed with the
     * {@link #onTrigger(ProcessContext, ProcessSession)} call.
     *
     * @param stateValues the values that were recovered from state that was
     * previously stored. This Map should be populated with the keys defined in
     * {@link TailFileState.StateKeys}.
     * @param filePath the file of the file for which state must be recovered
     * @throws IOException if unable to seek to the appropriate location in the
     * tailed file.
     */
    private void recoverState(final Map<String, String> stateValues, final String filePath) throws IOException {
        final TailFileObject tailFileObject = states.get(filePath);
        final String prefix = MAP_PREFIX + tailFileObject.getFilenameIndex() + '.';

        if (!stateValues.containsKey(prefix + TailFileState.StateKeys.FILENAME)
                || !stateValues.containsKey(prefix + TailFileState.StateKeys.POSITION)
                || !stateValues.containsKey(prefix + TailFileState.StateKeys.TIMESTAMP)
                || !stateValues.containsKey(prefix + TailFileState.StateKeys.LENGTH)) {
            resetState(filePath);
            return;
        }

        final String checksumValue = stateValues.get(prefix + TailFileState.StateKeys.CHECKSUM);
        final boolean checksumPresent = (checksumValue != null);
        final String storedStateFilename = stateValues.get(prefix + TailFileState.StateKeys.FILENAME);
        final long position = Long.parseLong(stateValues.get(prefix + TailFileState.StateKeys.POSITION));
        final long timestamp = Long.parseLong(stateValues.get(prefix + TailFileState.StateKeys.TIMESTAMP));
        final long length = Long.parseLong(stateValues.get(prefix + TailFileState.StateKeys.LENGTH));

        FileChannel reader = null;
        File tailFile = null;

        if (checksumPresent && filePath.equals(storedStateFilename)) {
            tailFileObject.setExpectedRecoveryChecksum(Long.parseLong(checksumValue));

            // We have an expected checksum and the currently configured filename is the same as the state file.
            // We need to check if the existing file is the same as the one referred to in the state file based on
            // the checksum.
            final Checksum checksum = new CRC32();
            final File existingTailFile = new File(storedStateFilename);
            if (existingTailFile.length() >= position) {
                try (final InputStream tailFileIs = Files.newInputStream(existingTailFile.toPath());
                     final CheckedInputStream in = new CheckedInputStream(tailFileIs, checksum)) {

                    try {
                        StreamUtils.copy(in, new NullOutputStream(), tailFileObject.getState().getPosition());
                    } catch (final EOFException eof) {
                        // If we hit EOFException, then the file is smaller than we expected. Assume rollover.
                        getLogger().debug("When recovering state, file being tailed has less data than was stored in the state. "
                                + "Assuming rollover. Will begin tailing current file from beginning.");
                    }

                    final long checksumResult = in.getChecksum().getValue();
                    if (checksumResult == tailFileObject.getExpectedRecoveryChecksum()) {
                        // Checksums match. This means that we want to resume reading from where we left off.
                        // So we will populate the reader object so that it will be used in onTrigger. If the
                        // checksums do not match, then we will leave the reader object null, so that the next
                        // call to onTrigger will result in a new Reader being created and starting at the
                        // beginning of the file.
                        getLogger().debug("When recovering state, checksum of tailed file matches the stored checksum. Will resume where left off.");
                        tailFile = existingTailFile;
                        reader = FileChannel.open(tailFile.toPath(), StandardOpenOption.READ);
                        getLogger().debug("Created FileChannel {} for {} in recoverState", reader, tailFile);

                        reader.position(position);
                    } else {
                        // we don't seek the reader to the position, so our reader will start at beginning of file.
                        getLogger().debug("When recovering state, checksum of tailed file does not match the stored checksum. Will begin tailing current file from beginning.");
                    }
                }
            } else {
                // fewer bytes than our position, so we know we weren't already reading from this file. Keep reader at a position of 0.
                getLogger().debug("When recovering state, existing file to tail is only {} bytes but position flag is {}; "
                        + "this indicates that the file has rotated. Will begin tailing current file from beginning.", existingTailFile.length(), position);
            }

            tailFileObject.setState(new TailFileState(filePath, tailFile, reader, position, timestamp, length, checksum, ByteBuffer.allocate(preAllocatedBufferSize)));
        } else {
            resetState(filePath);
        }

        getLogger().debug("Recovered state {}", tailFileObject.getState());
    }

    private void resetState(final String filePath) {
        states.get(filePath).setExpectedRecoveryChecksum(null);
        states.get(filePath).setState(new TailFileState(filePath, null, null, 0L, 0L, 0L, null, ByteBuffer.allocate(preAllocatedBufferSize)));
    }

    @OnStopped
    public void cleanup(final ProcessContext context) {
        for (TailFileObject tfo : states.values()) {
            cleanReader(tfo);
            final TailFileState state = tfo.getState();
            tfo.setState(new TailFileState(state.getFilename(), state.getFile(), null, state.getPosition() - linesBuffer.size(),
                state.getTimestamp(), state.getLength(), state.getChecksum(), state.getBuffer(), state.isTailingPostRollover()));
            persistState(tfo, null, context);
        }

        linesBuffer.reset();
    }

    private void cleanReader(TailFileObject tfo) {
        if (tfo.getState() == null) {
            return;
        }

        final FileChannel reader = tfo.getState().getReader();
        if (reader == null) {
            return;
        }

        try {
            reader.close();
            getLogger().debug("Closed FileChannel {}", reader);
        } catch (final IOException ioe) {
            getLogger().warn("Failed to close file handle during cleanup");
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (isMultiChanging.get()) {
            long timeSinceLastLookup = new Date().getTime() - lastLookup.get();
            if (timeSinceLastLookup > context.getProperty(LOOKUP_FREQUENCY).asTimePeriod(TimeUnit.MILLISECONDS)) {
                try {
                    final List<String> filesToTail = lookup(context);
                    final Scope scope = getStateScope(context);
                    final StateMap stateMap = session.getState(scope);
                    initStates(filesToTail, stateMap.toMap(), false);
                } catch (IOException e) {
                    getLogger().error("Exception raised while attempting to recover state about where the tailing last left off", e);
                    context.yield();
                    return;
                }
            }
        }

        if (requireStateLookup) {
            try {
                recoverState(context);
            } catch (IOException e) {
                getLogger().error("Exception raised while attempting to recover state about where the tailing last left off", e);
                context.yield();
                return;
            }

            requireStateLookup = false;
        }

        if (states.isEmpty()) {
            context.yield();
            return;
        }

        for (final String tailFile : states.keySet()) {
            try {
                processTailFile(context, session, tailFile);
            } catch (NulCharacterEncounteredException e) {
                getLogger().warn("NUL character encountered in {} and '{}' is set to 'true', yielding.", tailFile, REREAD_ON_NUL.getDisplayName());
                context.yield();
                return;
            }
        }

        // If a Line Start Pattern is being used and data is buffered, the Position that has been stored in the state will
        // not be accurate. To address this, we call cleanup(), which will handle updating the state to the correct values for us.
        if (lineStartPattern != null && linesBuffer.size() > 0) {
            cleanup(context);
        }

        // The states map is up-to-date, to actualize session state map remove references
        // which do not have a counterpart in states
        try {
            final Scope scope = getStateScope(context);
            StateMap sessionStateMap = session.getState(scope);
            Map<String, String> sessionStates = new HashMap<>(sessionStateMap.toMap());
            List<String> keysToRemove = collectKeysToBeRemoved(sessionStates);
            keysToRemove.forEach(sessionStates.keySet()::remove);
            getLogger().debug("Removed {} references to nonexistent files from session's state map",
                    keysToRemove.size());
            session.setState(sessionStates, scope);
        } catch (IOException e) {
            getLogger().error("Exception raised while attempting to cleanup session's state map", e);
            context.yield();
        }
    }

    private List<String> collectKeysToBeRemoved(Map<String, String> sessionStates) {
        List<String> keysToRemove = new ArrayList<>();
        List<String> filesToRemove = sessionStates.entrySet().stream()
                .filter(entry -> entry.getKey().endsWith(StateKeys.FILENAME)
                        && !states.containsKey(entry.getValue()))
                .map(Entry::getKey)
                .toList();

        for (String key : filesToRemove) {
            final String prefix = StringUtils.substringBefore(key, StateKeys.FILENAME);
            keysToRemove.add(prefix + StateKeys.FILENAME);
            keysToRemove.add(prefix + StateKeys.LENGTH);
            keysToRemove.add(prefix + StateKeys.POSITION);
            keysToRemove.add(prefix + StateKeys.CHECKSUM);
            keysToRemove.add(prefix + StateKeys.TIMESTAMP);
            keysToRemove.add(prefix + StateKeys.TAILING_POST_ROLLOVER);
        }

        return keysToRemove;
    }

    private void processTailFile(final ProcessContext context, final ProcessSession session, final String tailFile) {
        // If user changes the file that is being tailed, we need to consume the already-rolled-over data according
        // to the Initial Start Position property
        boolean rolloverOccurred;
        TailFileObject tfo = states.get(tailFile);

        if (tfo.isTailFileChanged()) {
            rolloverOccurred = false;
            final String recoverPosition = context.getProperty(START_POSITION).getValue();

            if (START_BEGINNING_OF_TIME.getValue().equals(recoverPosition)) {
                recoverRolledFiles(context, session, tailFile, tfo.getExpectedRecoveryChecksum(), tfo.getState().getTimestamp(), tfo.getState().getPosition());
            } else if (START_CURRENT_FILE.getValue().equals(recoverPosition)) {
                cleanup(context);
                tfo.setState(new TailFileState(tailFile, null, null, 0L, 0L, 0L, null, tfo.getState().getBuffer()));
            } else {
                final String filename = tailFile;
                final File file = new File(filename);

                try {
                    final FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
                    getLogger().debug("Created FileChannel {} for {}", fileChannel, file);

                    final Checksum checksum = new CRC32();
                    final long position = file.length();
                    final long timestamp = file.lastModified() + 1;

                    try (final InputStream fis = new FileInputStream(file);
                            final CheckedInputStream in = new CheckedInputStream(fis, checksum)) {
                        StreamUtils.copy(in, new NullOutputStream(), position);
                    }

                    fileChannel.position(position);
                    cleanup(context);
                    tfo.setState(new TailFileState(filename, file, fileChannel, position, timestamp, file.length(), checksum, tfo.getState().getBuffer()));
                } catch (final IOException ioe) {
                    getLogger().error("Attempted to position Reader at current position in file {} but failed to do so", file, ioe);
                    context.yield();
                    return;
                }
            }

            tfo.setTailFileChanged(false);
        } else {
            // Recover any data that may have rolled over since the last time that this processor ran.
            // If expectedRecoveryChecksum != null, that indicates that this is the first iteration since processor was started, so use whatever checksum value
            // was present when the state was last persisted. In this case, we must then null out the value so that the next iteration won't keep using the "recovered"
            // value. If the value is null, then we know that either the processor has already recovered that data, or there was no state persisted. In either case,
            // use whatever checksum value is currently in the state.
            Long expectedChecksumValue = tfo.getExpectedRecoveryChecksum();
            if (expectedChecksumValue == null) {
                expectedChecksumValue = tfo.getState().getChecksum() == null ? null : tfo.getState().getChecksum().getValue();
            }

            rolloverOccurred = recoverRolledFiles(context, session, tailFile, expectedChecksumValue, tfo.getState().getTimestamp(), tfo.getState().getPosition());
            if (rolloverOccurred) {
                final boolean tailAfterRollover = context.getProperty(POST_ROLLOVER_TAIL_PERIOD).asTimePeriod(TimeUnit.MILLISECONDS) > 0;
                if (tailAfterRollover) {
                    getLogger().debug("File {} was rolled over and the Rollover Tail Period is set, so will not consume from new file during this iteration.", tailFile);
                    return;
                }
            }

            tfo.setExpectedRecoveryChecksum(null);
        }

        // initialize local variables from state object; this is done so that we can easily change the values throughout
        // the onTrigger method and then create a new state object after we finish processing the files.
        TailFileState state = tfo.getState();
        File file = state.getFile();
        FileChannel reader = state.getReader();
        Checksum checksum = state.getChecksum();
        if (checksum == null) {
            checksum = new CRC32();
        }
        long position = state.getPosition();
        long timestamp = state.getTimestamp();
        long length = state.getLength();

        // Create a reader if necessary.
        if (file == null || reader == null) {
            file = new File(tailFile);
            reader = createReader(file, position);
            if (reader == null) {
                context.yield();
                return;
            }
        }

        final long startNanos = System.nanoTime();

        // Check if file has rotated
        // We determine that the file has rotated if any of the following conditions are met:
        // 1. 'rolloverOccured' == true, which indicates that we have found a new file matching the rollover pattern.
        // 2. The file was modified after the timestamp in our state, AND the file is smaller than we expected. This satisfies
        //    the case where we are tailing File A, and that file is then renamed (say to B) and a new file named A is created
        //    and is written to. In such a case, File A may have a file size smaller than we have in our state, so we know that
        //    it rolled over.
        // 3. The File Channel that we have indicates that the size of the file is different than file.length() indicates, AND
        //    the File Channel also indicates that we have read all data in the file. This case may also occur in the same scenario
        //    as #2, above. In this case, the File Channel is pointing to File A, but the 'file' object is pointing to File B. They
        //    both have the same name but are different files. As a result, once we have consumed all data from the File Channel,
        //    we want to roll over and consume data from the new file.
        boolean rotated = rolloverOccurred;
        if (!rotated) {
            final long fileLength = file.length();
            if (length > fileLength) {
                getLogger().debug("Rotated = true because TailFileState Length = {}, File Length = {}", length, fileLength);
                rotated = true;
            } else {
                try {
                    final long readerSize = reader.size();
                    final long readerPosition = reader.position();

                    if (readerSize == readerPosition && readerSize != fileLength) {
                        getLogger().debug("Rotated = true because readerSize={}, readerPosition={}, fileLength={}", readerSize, readerPosition, fileLength);
                        rotated = true;
                    }
                } catch (final IOException e) {
                    getLogger().warn("Failed to determined the size or position of the File Channel when "
                        + "determining if the file has rolled over. Will assume that the file being tailed has not rolled over", e);
                }
            }
        }

        if (rotated) {
            // Since file has rotated, we close the reader, create a new one, and then reset our state.
            try {
                reader.close();
                getLogger().debug("Closed FileChannel {}", reader);
            } catch (final IOException ioe) {
                getLogger().warn("Failed to close reader for {}", file, ioe);
            }

            reader = createReader(file, 0L);
            position = 0L;
            checksum.reset();
        }

        if (file.length() == position || !file.exists()) {
            // no data to consume so rather than continually running, yield to allow other processors to use the thread.
            getLogger().debug("No data to consume; created no FlowFiles");
            tfo.setState(new TailFileState(tailFile, file, reader, position, timestamp, length, checksum, state.getBuffer()));
            persistState(tfo, session, context);
            context.yield();
            return;
        }

        // If there is data to consume, read as much as we can.
        final TailFileState currentState = state;
        final Checksum chksum = checksum;
        // data has been written to file. Stream it to a new FlowFile.
        FlowFile flowFile = session.create();

        final FileChannel fileReader = reader;
        final AtomicLong positionHolder = new AtomicLong(position);
        final boolean reReadOnNul = context.getProperty(REREAD_ON_NUL).asBoolean();

        AtomicReference<NulCharacterEncounteredException> abort = new AtomicReference<>();
        flowFile = session.write(flowFile, rawOut -> {
            try (final OutputStream out = new BufferedOutputStream(rawOut)) {
                positionHolder.set(readLines(fileReader, currentState.getBuffer(), out, chksum, reReadOnNul));
            } catch (NulCharacterEncounteredException e) {
                positionHolder.set(e.getRePos());
                abort.set(e);
            }
        });

        // If there ended up being no data, just remove the FlowFile
        if (flowFile.getSize() == 0) {
            session.remove(flowFile);
            getLogger().debug("No data to consume; removed created FlowFile");
        } else {
            // determine filename for FlowFile by using <base filename of log file>.<initial offset>-<final offset>.<extension>
            final String tailFilename = file.getName();
            final String baseName = StringUtils.substringBeforeLast(tailFilename, ".");
            final String flowFileName;
            if (baseName.length() < tailFilename.length()) {
                flowFileName = baseName + "." + position + "-" + positionHolder.get() + "." + StringUtils.substringAfterLast(tailFilename, ".");
            } else {
                flowFileName = baseName + "." + position + "-" + positionHolder.get();
            }

            flowFile = session.putAllAttributes(flowFile, Map.of(
                    CoreAttributes.FILENAME.key(), flowFileName,
                    CoreAttributes.MIME_TYPE.key(), "text/plain",
                    "tailfile.original.path", tailFile
            ));
            session.getProvenanceReporter().receive(flowFile, file.toURI().toString(), "FlowFile contains bytes " + position + " through " + positionHolder.get() + " of source file",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().debug("Created {} and routed to success", flowFile);
        }

        if (flowFile.getSize() > 0 || linesBuffer.size() > 0) {
            position = positionHolder.get();

            // Set timestamp to the latest of when the file was modified and the current timestamp stored in the state.
            // We do this because when we read a file that has been rolled over, we set the state to 1 millisecond later than the last mod date
            // in order to avoid ingesting that file again. If we then read from this file during the same second (or millisecond, depending on the
            // operating system file last mod precision), then we could set the timestamp to a smaller value, which could result in reading in the
            // rotated file a second time.
            timestamp = Math.max(state.getTimestamp(), file.lastModified());
            length = file.length();
        }

        // Create a new state object to represent our current position, timestamp, etc.
        tfo.setState(new TailFileState(tailFile, file, reader, position, timestamp, length, checksum, state.getBuffer()));

        persistState(tfo, session, context);

        if (abort.get() != null) {
            final long newPosition = positionHolder.get();
            try {
                reader.position(newPosition);
            } catch (IOException ex) {
                getLogger().warn("Couldn't reposition the reader for {} due to {}", file, ex, ex);
                try {
                    reader.close();
                } catch (IOException ex2) {
                    getLogger().warn("Failed to close reader for {} due to {}", file, ex2, ex2);
                }
            }

            throw abort.get();
        }
    }

    private long readLines(final FileChannel reader, final ByteBuffer buffer, final OutputStream out, final Checksum checksum, Boolean reReadOnNul) throws IOException {
        return readLines(reader, buffer, out, checksum, reReadOnNul, false);
    }

    /**
     * Read new lines from the given FileChannel, copying it to the given Output
     * Stream. The Checksum is used in order to later determine whether or not
     * data has been consumed.
     *
     * @param reader The FileChannel to read data from
     * @param buffer the buffer to use for copying data
     * @param out the OutputStream to copy the data to
     * @param checksum the Checksum object to use in order to calculate checksum
     * for recovery purposes
     * @param reReadOnNul If set to 'true', ASCII NUL characters will be treated as
     * temporary values and a NulCharacterEncounteredException is thrown.
     * This allows the caller to re-attempt a read from the same position.
     * If set to 'false' these characters will be treated as regular content.
     * @param readFully If set to 'true' the last chunk of bytes after the last whole line
     * will be also written to the OutputStream
     *
     * @return The new position after the lines have been read
     * @throws java.io.IOException if an I/O error occurs.
     */
    private long readLines(final FileChannel reader, final ByteBuffer buffer, final OutputStream out, final Checksum checksum,
                           Boolean reReadOnNul, final boolean readFully) throws IOException {
        getLogger().debug("Reading lines starting at position {}", reader.position());

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            long pos = reader.position();
            long rePos = pos; // position to re-read

            int num;
            int linesRead = 0;
            boolean seenCR = false;
            buffer.clear();

            while ((num = reader.read(buffer)) != -1) {
                buffer.flip();

                for (int i = 0; i < num; i++) {
                    byte ch = buffer.get(i);

                    switch (ch) {
                        case '\n': {
                            baos.write(ch);
                            seenCR = false;
                            flushByteArrayOutputStream(baos, out, checksum, false);
                            rePos = pos + i + 1;
                            linesRead++;
                            break;
                        }
                        case '\r': {
                            baos.write(ch);
                            seenCR = true;
                            break;
                        }
                        case '\0': {
                            if (reReadOnNul) {
                                throw new NulCharacterEncounteredException(rePos);
                            }
                        }
                        default: {
                            if (seenCR) {
                                seenCR = false;
                                flushByteArrayOutputStream(baos, out, checksum, false);
                                linesRead++;
                                baos.write(ch);
                                rePos = pos + i;
                            } else {
                                baos.write(ch);
                            }
                        }
                    }
                }

                pos = reader.position();
            }

            if (readFully) {
                flushByteArrayOutputStream(baos, out, checksum, true);
                rePos = reader.position();
            }

            if (rePos < reader.position()) {
                getLogger().debug("Read {} lines; repositioning reader from {} to {}", linesRead, pos, rePos);
                reader.position(rePos); // Ensure we can re-read if necessary
            }

            return rePos;
        }
    }

    private void flushByteArrayOutputStream(final ByteArrayOutputStream baos, final OutputStream out, final Checksum checksum, final boolean ignoreRegex) throws IOException {
        final byte[] baosBuffer = baos.toByteArray();
        baos.reset();

        // If the regular expression is being ignored, we need to flush anything that is buffered.
        // This happens, for example, when a file has been rolled over. At that point, we want to flush whatever we have,
        // even if the regex hasn't been matched.
        if (ignoreRegex) {
            flushLinesBuffer(out, checksum);
        }

        if (lineStartPattern == null) {
            out.write(baosBuffer);

            checksum.update(baosBuffer, 0, baosBuffer.length);
            if (getLogger().isTraceEnabled()) {
                getLogger().trace("Checksum updated to {}", checksum.getValue());
            }

            return;
        }

        final String bufferAsString = new String(baosBuffer, StandardCharsets.UTF_8);
        final String[] lines = bufferAsString.split("\n");
        for (final String line : lines) {
            final boolean startsWithRegex = lineStartPattern.matcher(line).lookingAt();

            if (startsWithRegex || linesBuffer.size() >= maxBufferBytes) {
                // We found a line that matches our start regex. Flush what we have buffered and reset our buffer.
                flushLinesBuffer(out, checksum);
            }

            // This line does not match our start regex. Buffer this line until we encounter a line that does.
            linesBuffer.write(line.getBytes(StandardCharsets.UTF_8));
            linesBuffer.write(NEW_LINE_BYTES);
        }
    }

    private void flushLinesBuffer(final OutputStream out, final Checksum checksum) throws IOException {
        linesBuffer.writeTo(out);

        checksum.update(linesBuffer.toByteArray(), 0, linesBuffer.size());
        if (getLogger().isTraceEnabled()) {
            getLogger().trace("Checksum updated to {}", checksum.getValue());
        }

        linesBuffer.reset();
    }

    /**
     * Returns a list of all Files that match the following criteria:
     *
     * <ul>
     * <li>Filename matches the Rolling Filename Pattern</li>
     * <li>Filename does not match the actual file being tailed</li>
     * <li>The Last Modified Time on the file is equal to or later than the
     * given minimum timestamp</li>
     * </ul>
     *
     * <p>
     * The List that is returned will be ordered by file timestamp, providing
     * the oldest file first.
     * </p>
     *
     * @param context the ProcessContext to use in order to determine Processor
     * configuration
     * @param minTimestamp any file with a Last Modified Time before this
     * timestamp will not be returned
     * @return a list of all Files that have rolled over
     * @throws IOException if unable to perform the listing of files
     */
    private List<File> getRolledOffFiles(final ProcessContext context, final long minTimestamp, final String tailFilePath) throws IOException {
        final File tailFile = new File(tailFilePath);
        File directory = tailFile.getParentFile();
        if (directory == null) {
            directory = new File(".");
        }

        String rollingPattern = context.getProperty(ROLLING_FILENAME_PATTERN).getValue();
        if (rollingPattern == null) {
            return Collections.emptyList();
        } else {
            rollingPattern = rollingPattern.replace("${filename}", StringUtils.substringBeforeLast(tailFile.getName(), "."));
        }

        final List<File> rolledOffFiles = new ArrayList<>();
        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory.toPath(), rollingPattern)) {
            for (final Path path : dirStream) {
                final File file = path.toFile();
                final long lastMod = file.lastModified();

                if (lastMod >= minTimestamp && !file.equals(tailFile)) {
                    rolledOffFiles.add(file);
                } else {
                    getLogger().debug("Found rolled off file {} but its last modified timestamp is before the cutoff (Last Mod = {}, Cutoff = {}) so will not consume it",
                            file, lastMod, minTimestamp);
                }
            }
        }

        // Sort files based on last modified timestamp. If same timestamp, use filename as a secondary sort, as often
        // files that are rolled over are given a naming scheme that is lexicographically sort in the same order as the
        // timestamp, such as yyyy-MM-dd-HH-mm-ss
        rolledOffFiles.sort(Comparator.comparingLong(File::lastModified).thenComparing(File::getName));

        return rolledOffFiles;
    }

    private Scope getStateScope(final ProcessContext context) {
        final String location = context.getProperty(STATE_LOCATION).getValue();
        if (LOCATION_REMOTE.getValue().equalsIgnoreCase(location)) {
            return Scope.CLUSTER;
        }

        return Scope.LOCAL;
    }

    private void persistState(final TailFileObject tfo, final ProcessSession session, final ProcessContext context) {
        persistState(tfo.getState().toStateMap(tfo.getFilenameIndex()), session, context);
    }

    private void persistState(final Map<String, String> state, final ProcessSession session, final ProcessContext context) {
        try {
            final Scope scope = getStateScope(context);
            final StateMap oldState = session == null ? context.getStateManager().getState(scope) : session.getState(scope);

            Map<String, String> updatedState = new HashMap<>(oldState.toMap());
            updatedState.putAll(state);

            if (session == null) {
                context.getStateManager().setState(updatedState, scope);
            } else {
                session.setState(updatedState, scope);
            }
        } catch (final IOException e) {
            getLogger().warn("Failed to store state due to {}; some data may be duplicated on restart of NiFi", e);
        }
    }

    private FileChannel createReader(final File file, final long position) {
        final FileChannel reader;

        try {
            reader = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        } catch (final IOException ioe) {
            getLogger().warn("Unable to open file {}; will attempt to access file again after the configured Yield Duration has elapsed", file, ioe);
            return null;
        }

        getLogger().debug("Created FileChannel {} for {}", reader, file);

        try {
            reader.position(position);
        } catch (final IOException ioe) {
            getLogger().error("Failed to read from {}", file, ioe);

            try {
                reader.close();
                getLogger().debug("Closed FileChannel {}", reader);
            } catch (final IOException ignored) {
            }

            return null;
        }

        return reader;
    }

    // for testing purposes
    Map<String, TailFileObject> getState() {
        return states;
    }

    /**
     * Finds any files that have rolled over and have not yet been ingested by
     * this Processor. Each of these files that is found will be ingested as its
     * own FlowFile. If a file is found that has been partially ingested, the
     * rest of the file will be ingested as a single FlowFile but the data that
     * already has been ingested will not be ingested again.
     *
     * @param context the ProcessContext to use in order to obtain Processor
     * configuration.
     * @param session the ProcessSession to use in order to interact with
     * FlowFile creation and content.
     * @param expectedChecksum the checksum value that is expected for the
     * oldest file from offset 0 through &lt;position&gt;.
     * @param timestamp the latest Last Modified Timestamp that has been
     * consumed. Any data that was written before this data will not be
     * ingested.
     * @param position the byte offset in the file being tailed, where tailing
     * last left off.
     *
     * @return <code>true</code> if the file being tailed has rolled over,
     * <code>false</code> otherwise
     */
    private boolean recoverRolledFiles(final ProcessContext context, final ProcessSession session, final String tailFile, final Long expectedChecksum, final long timestamp, final long position) {
        try {
            // Find all files that match our rollover pattern, if any, and order them based on their timestamp and filename.
            // Ignore any file that has a timestamp earlier than the state that we have persisted. If we were reading from
            // a file when we stopped running, then that file that we were reading from should be the first file in this list,
            // assuming that the file still exists on the file system.
            final List<File> rolledOffFiles = getRolledOffFiles(context, timestamp, tailFile);
            return recoverRolledFiles(context, session, tailFile, rolledOffFiles, expectedChecksum, position);
        } catch (final IOException e) {
            getLogger().error("Failed to recover files that have rolled over", e);
            return false;
        }
    }

    /**
     * Finds any files that have rolled over and have not yet been ingested by
     * this Processor. Each of these files that is found will be ingested as its
     * own FlowFile. If a file is found that has been partially ingested, the
     * rest of the file will be ingested as a single FlowFile but the data that
     * already has been ingested will not be ingested again.
     *
     * @param context the ProcessContext to use in order to obtain Processor
     * configuration.
     * @param session the ProcessSession to use in order to interact with
     * FlowFile creation and content.
     * @param expectedChecksum the checksum value that is expected for the
     * oldest file from offset 0 through &lt;position&gt;.
     * @param position the byte offset in the file being tailed, where tailing
     * last left off.
     *
     * @return <code>true</code> if the file being tailed has rolled over, false
     * otherwise
     */
    private boolean recoverRolledFiles(final ProcessContext context, final ProcessSession session, final String tailFile, final List<File> rolledOffFiles, final Long expectedChecksum,
            final long position) {
        try {
            getLogger().debug("Recovering Rolled Off Files; total number of files rolled off = {}", rolledOffFiles.size());
            TailFileObject tfo = states.get(tailFile);

            final long postRolloverTailMillis = context.getProperty(POST_ROLLOVER_TAIL_PERIOD).asTimePeriod(TimeUnit.MILLISECONDS);
            final boolean tailingPostRollover = tfo.getState().isTailingPostRollover();
            final boolean shouldTailPostRollover = postRolloverTailMillis > 0;

            // For first file that we find, it may or may not be the file that we were last reading from.
            // As a result, we have to read up to the position we stored, while calculating the checksum. If the checksums match,
            // then we know we've already processed this file. If the checksums do not match, then we have not
            // processed this file and we need to seek back to position 0 and ingest the entire file.
            // For all other files that have been rolled over, we need to just ingest the entire file.
            boolean rolloverOccurred = !rolledOffFiles.isEmpty();

            final boolean tailFirstFile;
            if (rolloverOccurred) {
                final File firstFile = rolledOffFiles.getFirst();
                final long millisSinceModified = getCurrentTimeMs() - firstFile.lastModified();
                final boolean fileGrew = firstFile.length() >= position;
                final boolean tailRolledFile = postRolloverTailMillis == 0 || millisSinceModified < postRolloverTailMillis;
                tailFirstFile = fileGrew && tailRolledFile && expectedChecksum != null;
            } else {
                tailFirstFile = false;
            }

            if (tailFirstFile) {
                final File firstFile = rolledOffFiles.getFirst();

                final boolean consumed;
                if (shouldTailPostRollover) {
                    // User has configured to continue tailing file after it has been rolled over, until it's no longer being modified.
                    // Consume any newly added lines from the rolled over file, but do not consume the last line, if it doesn't have a newline.
                    // Keep the state indicating that we are currently tailing a file post-rollover.
                    consumed = tailRolledFile(context, session, tailFile, expectedChecksum, position, tfo, firstFile, false, true);
                } else {
                    // User has not configured to continue tailing file after it has been rolled over. If any data was written to the rolled file before
                    // rolling it over, consume that data, up to the end of the file, including the last line, even if it doesn't have a newline.
                    consumed = tailRolledFile(context, session, tailFile, expectedChecksum, position, tfo, firstFile, true, false);
                }

                if (consumed) {
                    rolledOffFiles.removeFirst();
                }
            } else if (tailingPostRollover && shouldTailPostRollover) {
                // This condition is encountered when we are tailing a file post-rollover, and we've now reached the point where the rolled file
                // has not changed.
                final List<File> allRolledFiles = getRolledOffFiles(context, 0L, tailFile);
                allRolledFiles.sort(Comparator.comparing(File::lastModified).reversed());
                final File newestFile = allRolledFiles.getFirst();

                // If we don't notice that the file has been modified, per the checks above, then we want to keep checking until the last modified
                // date has eclipsed the configured value for the Post-Rollover Tail Period. Until then, return false. Once that occurs, we will
                // consume the rest of the data, including the last line, even if it doesn't have a line ending.
                final long millisSinceModified = getCurrentTimeMs() - newestFile.lastModified();
                if (millisSinceModified < postRolloverTailMillis) {
                    getLogger().debug("Rolled over file {} (size={}, lastModified={}) was modified {} millis ago, which isn't long enough to consume file fully without taking line endings into " +
                        "account. Will do nothing for now.", newestFile, newestFile.length(), newestFile.lastModified(), millisSinceModified);
                    return true;
                }

                // The file has been rolled over and is no longer being written to. Consume all the way to the end of the file, including the last line,
                // even if it does not have a newline after it.
                final boolean consumed = tailRolledFile(context, session, tailFile, expectedChecksum, position, tfo, newestFile, true, false);
                if (consumed) {
                    getLogger().debug("Consumed the final data from {}", newestFile);
                    rolledOffFiles.remove(newestFile);
                } else {
                    getLogger().debug("No more data to consume from {} (size={}, lastModified={})", newestFile, newestFile.length(), newestFile.lastModified());
                }
            }

            // For each file that we found that matches our Rollover Pattern, and has a last modified date later than the timestamp
            // that we recovered from the state file, we need to consume the entire file. The only exception to this is the file that
            // we were reading when we last stopped, as it may already have been partially consumed. That is taken care of in the
            // above block of code.
            for (final File file : rolledOffFiles) {
                tfo.setState(consumeFileFully(file, context, session, tfo));
            }

            return rolloverOccurred;
        } catch (final IOException e) {
            getLogger().error("Failed to recover files that have rolled over", e);
            return false;
        }
    }

    private boolean tailRolledFile(final ProcessContext context, final ProcessSession session, final String tailFile, final Long expectedChecksum,
                                final long position, final TailFileObject tfo, final File fileToTail, final boolean readFully, final boolean tailingPostRollover) throws IOException {

        final Boolean reReadOnNul = context.getProperty(REREAD_ON_NUL).asBoolean();
        final long startNanos = System.nanoTime();

        try (final FileInputStream fis = new FileInputStream(fileToTail);
             final CheckedInputStream in = new CheckedInputStream(fis, new CRC32())) {
            StreamUtils.copy(in, new NullOutputStream(), position);

            final long checksumResult = in.getChecksum().getValue();
            if (checksumResult != expectedChecksum) {
                getLogger().debug("Checksum for {} did not match expected checksum. Checksum for file was {} but expected {}. Will consume entire file",
                        fileToTail, checksumResult, expectedChecksum);

                return false;
            }

            getLogger().debug("Checksum for {} matched expected checksum. Will skip first {} bytes", fileToTail, position);

            // This is the same file that we were reading when we shutdown. Start reading from this point on.
            FlowFile flowFile = session.create();

            final TailFileState currentState = tfo.getState();
            final Checksum checksum = currentState.getChecksum() == null ? new CRC32() : currentState.getChecksum();
            final ByteBuffer buffer = currentState.getBuffer() == null ? ByteBuffer.allocate(preAllocatedBufferSize) : currentState.getBuffer();
            final FileChannel channel = fis.getChannel();
            final long timestamp = fileToTail.lastModified();

            final AtomicReference<NulCharacterEncounteredException> abort = new AtomicReference<>();

            flowFile = session.write(flowFile, out -> {
                try {
                    readLines(channel, buffer, out, checksum, reReadOnNul, readFully);
                } catch (final NulCharacterEncounteredException ncee) {
                    abort.set(ncee);

                    // Log the fact that we encountered a NUL character and yield. But we don't re-throw the Exception because
                    // we want to continue on with the same logic of transferring non-zero flowfiles, removing 0-byte flowfiles,
                    // and maintaining our state.
                    getLogger().info("Encountered NUL character when tailing file {}; will yield", tailFile);
                    context.yield();
                }
            });

            if (flowFile.getSize() == 0L) {
                session.remove(flowFile);
            } else {
                flowFile = session.putAllAttributes(flowFile, Map.of(
                        CoreAttributes.FILENAME.key(), fileToTail.getName(),
                        CoreAttributes.MIME_TYPE.key(), "text/plain",
                        "tailfile.original.path", tailFile
                ));

                session.getProvenanceReporter().receive(flowFile, fileToTail.toURI().toString(), "FlowFile contains bytes 0 through " + position + " of source file",
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
                session.transfer(flowFile, REL_SUCCESS);
                getLogger().debug("Created {} from rolled over file {} and routed to success", flowFile, fileToTail);
            }

            // We need to update the state to account for the fact that we just brought data in.
            // If we are going to tail a rolled over file for some amount of time, then we need to keep the state pointing to the
            // same file, just using an updated position/timestamp/checksum/length. This way, the next iteration will compare against these
            // updated values.
            // But if we are not going to tail the rolled over file for any period of time, we can essentially reset the state.
            final long postRolloverTailMillis = context.getProperty(POST_ROLLOVER_TAIL_PERIOD).asTimePeriod(TimeUnit.MILLISECONDS);
            final long millisSinceUpdate = getCurrentTimeMs() - timestamp;
            if (tailingPostRollover && postRolloverTailMillis > 0) {
                getLogger().debug("File {} has been rolled over, but it was updated {} millis ago, which is less than the configured {} ({} ms), so will continue tailing",
                    fileToTail, millisSinceUpdate, POST_ROLLOVER_TAIL_PERIOD.getDisplayName(), postRolloverTailMillis);

                final long length = currentState.getLength() + flowFile.getSize();
                final long updatedPosition = position + flowFile.getSize();
                final TailFileState updatedState = new TailFileState(currentState.getFilename(), currentState.getFile(), channel, updatedPosition, timestamp, length, checksum,
                    buffer, tailingPostRollover);

                tfo.setState(updatedState);
            } else {
                final NulCharacterEncounteredException ncee = abort.get();
                if (ncee != null) {
                    throw ncee;
                }

                // use a timestamp of lastModified() + 1 so that we do not ingest this file again.
                getLogger().debug("Completed tailing of file {}; will cleanup state", tailFile);
                cleanup(context);
                tfo.setState(new TailFileState(tailFile, null, null, 0L, fileToTail.lastModified() + 1L, fileToTail.length(), null, tfo.getState().getBuffer(), tailingPostRollover));
            }

            persistState(tfo, session, context);
            return true;
        }
    }

    /**
     * Creates a new FlowFile that contains the entire contents of the given
     * file and transfers that FlowFile to success. This method will commit the
     * given session and emit an appropriate Provenance Event.
     *
     * @param file the file to ingest
     * @param context the ProcessContext
     * @param session the ProcessSession
     * @param tfo the current state
     *
     * @return the new, updated state that reflects that the given file has been
     * ingested.
     */
    private TailFileState consumeFileFully(final File file, final ProcessContext context, final ProcessSession session, TailFileObject tfo) throws IOException {
        FlowFile flowFile = session.create();

        try (final InputStream fis = new FileInputStream(file)) {
            flowFile = session.write(flowFile, out -> {
                flushLinesBuffer(out, new CRC32());
                StreamUtils.copy(fis, out);
            });
        }

        if (flowFile.getSize() == 0L) {
            session.remove(flowFile);
        } else {
            flowFile = session.putAllAttributes(flowFile, Map.of(
                    CoreAttributes.FILENAME.key(), file.getName(),
                    CoreAttributes.MIME_TYPE.key(), "text/plain",
                    "tailfile.original.path", tfo.getState().getFilename())
            );
            session.getProvenanceReporter().receive(flowFile, file.toURI().toString());
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().debug("Created {} from {} and routed to success", flowFile, file);

            // use a timestamp of lastModified() + 1 so that we do not ingest this file again.
            cleanup(context);
            tfo.setState(new TailFileState(context.getProperty(FILENAME).evaluateAttributeExpressions().getValue(), null, null, 0L, file.lastModified() + 1L, file.length(), null,
                    tfo.getState().getBuffer()));

            persistState(tfo, session, context);
        }

        return tfo.getState();
    }

    public long getCurrentTimeMs() {
        return System.currentTimeMillis();
    }

    static class TailFileObject {

        private TailFileState state;
        private Long expectedRecoveryChecksum;
        private final int filenameIndex;
        private boolean tailFileChanged = true;

        public TailFileObject(final int index, final TailFileState fileState) {
            this.filenameIndex = index;
            this.state = fileState;
        }

        public TailFileObject(int index, Map<String, String> statesMap, int preAllocatedBufferSize) {
            this.filenameIndex = index;
            this.tailFileChanged = false;
            final String prefix = MAP_PREFIX + index + '.';
            final String filename = statesMap.get(prefix + TailFileState.StateKeys.FILENAME);
            final long position = Long.parseLong(statesMap.get(prefix + TailFileState.StateKeys.POSITION));
            final long timestamp = Long.parseLong(statesMap.get(prefix + TailFileState.StateKeys.TIMESTAMP));
            final long length = Long.parseLong(statesMap.get(prefix + TailFileState.StateKeys.LENGTH));
            final boolean tailingPostRollover = Boolean.parseBoolean(prefix + StateKeys.TAILING_POST_ROLLOVER);
            this.state = new TailFileState(filename, new File(filename), null, position, timestamp, length, null, ByteBuffer.allocate(preAllocatedBufferSize), tailingPostRollover);
        }

        public int getFilenameIndex() {
            return filenameIndex;
        }

        public TailFileState getState() {
            return state;
        }

        public void setState(TailFileState state) {
            this.state = state;
        }

        public Long getExpectedRecoveryChecksum() {
            return expectedRecoveryChecksum;
        }

        public void setExpectedRecoveryChecksum(Long expectedRecoveryChecksum) {
            this.expectedRecoveryChecksum = expectedRecoveryChecksum;
        }

        public boolean isTailFileChanged() {
            return tailFileChanged;
        }

        public void setTailFileChanged(boolean tailFileChanged) {
            this.tailFileChanged = tailFileChanged;
        }

    }

    /**
     * A simple Java class to hold information about our state so that we can
     * maintain this state across multiple invocations of the Processor
     */
    static class TailFileState {

        private final String filename; // hold onto filename and not just File because we want to match that against the user-defined filename to recover from
        private final File file;
        private final FileChannel reader;
        private final long position;
        private final long timestamp;
        private final long length;
        private final Checksum checksum;
        private final ByteBuffer buffer;
        private final boolean tailingPostRollover;

        static class StateKeys {
            public static final String FILENAME = "filename";
            public static final String POSITION = "position";
            public static final String TIMESTAMP = "timestamp";
            public static final String CHECKSUM = "checksum";
            public static final String LENGTH = "length";
            public static final String TAILING_POST_ROLLOVER = "tailingPostRollover";
        }

        public TailFileState(final String filename, final File file, final FileChannel reader, final long position, final long timestamp,
                             final long length, final Checksum checksum, final ByteBuffer buffer) {
            this(filename, file, reader, position, timestamp, length, checksum, buffer, false);
        }

        public TailFileState(final String filename, final File file, final FileChannel reader, final long position, final long timestamp,
                             final long length, final Checksum checksum, final ByteBuffer buffer, final boolean tailingPostRollover) {
            this.filename = filename;
            this.file = file;
            this.reader = reader;
            this.position = position;
            this.length = length;
            this.timestamp = timestamp; // many operating systems will use only second-level precision for last-modified times so cut off milliseconds
            this.checksum = checksum;
            this.buffer = buffer;
            this.tailingPostRollover = tailingPostRollover;
        }

        public String getFilename() {
            return filename;
        }

        public File getFile() {
            return file;
        }

        public FileChannel getReader() {
            return reader;
        }

        public long getPosition() {
            return position;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getLength() {
            return length;
        }

        public Checksum getChecksum() {
            return checksum;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public boolean isTailingPostRollover() {
            return tailingPostRollover;
        }

        @Override
        public String toString() {
            return "TailFileState[filename=" + filename + ", position=" + position + ", timestamp=" + timestamp + ", checksum=" + (checksum == null ? "null" : checksum.getValue()) +
                ", tailingPostRollover=" + tailingPostRollover + "]";
        }

        public Map<String, String> toStateMap(int index) {
            final String prefix = MAP_PREFIX + index + '.';
            final Map<String, String> map = HashMap.newHashMap(4);
            map.put(prefix + StateKeys.FILENAME, filename);
            map.put(prefix + StateKeys.POSITION, String.valueOf(position));
            map.put(prefix + StateKeys.LENGTH, String.valueOf(length));
            map.put(prefix + StateKeys.TIMESTAMP, String.valueOf(timestamp));
            map.put(prefix + StateKeys.CHECKSUM, checksum == null ? null : String.valueOf(checksum.getValue()));
            map.put(prefix + StateKeys.TAILING_POST_ROLLOVER, String.valueOf(tailingPostRollover));
            return map;
        }
    }

    static class NulCharacterEncounteredException extends RuntimeException {
        private final long rePos;

        public NulCharacterEncounteredException(long rePos) {
            this.rePos = rePos;
        }

        public long getRePos() {
            return rePos;
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }
}
