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

import com.google.common.base.Throwables;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ClassloaderIsolationKeyProvider;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosUser;
import org.ietf.jgss.GSSException;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * This is a base class that is helpful when building processors interacting with HDFS.
 * <p/>
 * As of Apache NiFi 1.5.0, the Relogin Period property is no longer used in the configuration of a Hadoop processor.
 * Due to changes made to {@link SecurityUtil#loginKerberos(Configuration, String, String)}, which is used by this
 * class to authenticate a principal with Kerberos, Hadoop components no longer
 * attempt relogins explicitly.  For more information, please read the documentation for
 * {@link SecurityUtil#loginKerberos(Configuration, String, String)}.
 *
 * @see SecurityUtil#loginKerberos(Configuration, String, String)
 */
@RequiresInstanceClassLoading(cloneAncestorResources = true)
public abstract class AbstractHadoopProcessor extends AbstractProcessor implements ClassloaderIsolationKeyProvider {
    private static final String DENY_LFS_ACCESS = "NIFI_HDFS_DENY_LOCAL_FILE_SYSTEM_ACCESS";

    private static final String DENY_LFS_EXPLANATION = String.format("LFS Access Denied according to Environment Variable [%s]", DENY_LFS_ACCESS);

    private static final Pattern LOCAL_FILE_SYSTEM_URI = Pattern.compile("^file:.*");

    private static final String NORMALIZE_ERROR_WITH_PROPERTY = "The filesystem component of the URI configured in the '{}' property ({}) does not match " +
            "the filesystem URI from the Hadoop configuration file ({}) and will be ignored.";

    private static final String NORMALIZE_ERROR_WITHOUT_PROPERTY = "The filesystem component of the URI configured ({}) does not match the filesystem URI from " +
            "the Hadoop configuration file ({}) and will be ignored.";

    // properties
    public static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("Hadoop Configuration Resources")
            .description("A file or comma separated list of files which contains the Hadoop file system configuration. Without this, Hadoop "
                    + "will search the classpath for a 'core-site.xml' and 'hdfs-site.xml' file or will revert to a default configuration. "
                    + "To use swebhdfs, see 'Additional Details' section of PutHDFS's documentation.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The HDFS directory from which files should be read")
            .required(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder()
            .name("Compression codec")
            .required(true)
            .allowableValues(CompressionType.allowableValues())
            .defaultValue(CompressionType.NONE.toString())
            .build();

    public static final PropertyDescriptor ADDITIONAL_CLASSPATH_RESOURCES = new PropertyDescriptor.Builder()
            .name("Additional Classpath Resources")
            .description("A comma-separated list of paths to files and/or directories that will be added to the classpath and used for loading native libraries. " +
                    "When specifying a directory, all files with in the directory will be added to the classpath, but further sub-directories will not be included.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Specifies the Kerberos User Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();


    public static final String ABSOLUTE_HDFS_PATH_ATTRIBUTE = "absolute.hdfs.path";
    public static final String HADOOP_FILE_URL_ATTRIBUTE = "hadoop.file.url";

    protected static final String TARGET_HDFS_DIR_CREATED_ATTRIBUTE = "target.dir.created";

    private static final Object RESOURCES_LOCK = new Object();
    private static final HdfsResources EMPTY_HDFS_RESOURCES = new HdfsResources(null, null, null, null);

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            HADOOP_CONFIGURATION_RESOURCES,
            KERBEROS_USER_SERVICE,
            ADDITIONAL_CLASSPATH_RESOURCES
    );

    // variables shared by all threads of this processor
    // Hadoop Configuration, Filesystem, and UserGroupInformation (optional)
    final AtomicReference<HdfsResources> hdfsResources = new AtomicReference<>();

    // Holder of cached Configuration information so validation does not reload the same config over and over
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

    protected static List<PropertyDescriptor> getCommonPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        hdfsResources.set(EMPTY_HDFS_RESOURCES);
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.removeProperty("Kerberos Principal");
        config.removeProperty("Kerberos Password");
        config.removeProperty("Kerberos Keytab");
        config.removeProperty("kerberos-credentials-service");
        config.removeProperty("Kerberos Relogin Period");
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public String getClassloaderIsolationKey(final PropertyContext context) {
        class ClassloaderIsolationKeyBuilder {
            private static final String SEPARATOR = "__";

            private final StringBuilder sb = new StringBuilder();

            void add(final String value) {
                if (value != null) {
                    if (sb.length() > 0) {
                        sb.append(SEPARATOR);
                    }
                    sb.append(value);
                }
            }

            String build() {
                return sb.length() > 0 ? sb.toString() : null;
            }
        }

        final ClassloaderIsolationKeyBuilder builder = new ClassloaderIsolationKeyBuilder();

        builder.add(context.getProperty(HADOOP_CONFIGURATION_RESOURCES).getValue());
        builder.add(context.getProperty(ADDITIONAL_CLASSPATH_RESOURCES).getValue());

        try {
            final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
            if (kerberosUserService != null) {
                final KerberosUser kerberosUser = kerberosUserService.createKerberosUser();
                builder.add(kerberosUser.getPrincipal());
            }
        } catch (IllegalStateException e) {
            // the Kerberos controller service is disabled, therefore this part of the isolation key cannot be determined yet
        }

        return builder.build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        final List<String> locations = getConfigLocations(validationContext);

        if (locations.isEmpty()) {
            return results;
        }

        try {
            final Configuration conf = getHadoopConfigurationForValidation(locations);
            results.addAll(validateFileSystem(conf));
        } catch (final IOException e) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject("Hadoop Configuration Resources")
                    .explanation("Could not load Hadoop Configuration resources due to: " + e)
                    .build());
        }

        return results;
    }

    protected Collection<ValidationResult> validateFileSystem(final Configuration configuration) {
        final List<ValidationResult> results = new ArrayList<>();

        if (isFileSystemAccessDenied(FileSystem.getDefaultUri(configuration))) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject("Hadoop File System")
                    .explanation(DENY_LFS_EXPLANATION)
                    .build());
        }

        return results;
    }

    protected Configuration getHadoopConfigurationForValidation(final List<String> locations) throws IOException {
        ValidationResources resources = validationResourceHolder.get();

        // if no resources in the holder, or if the holder has different resources loaded,
        // then load the Configuration and set the new resources in the holder
        if (resources == null || !locations.equals(resources.getConfigLocations())) {
            getLogger().debug("Reloading validation resources");
            final Configuration config = new ExtendedConfiguration(getLogger());
            config.setClassLoader(Thread.currentThread().getContextClassLoader());
            resources = new ValidationResources(locations, getConfigurationFromResources(config, locations));
            validationResourceHolder.set(resources);
        }

        return resources.getConfiguration();
    }

    /**
     * If your subclass also has an @OnScheduled annotated method and you need hdfsResources in that method, then be sure to call super.abstractOnScheduled(context)
     */
    @OnScheduled
    public final void abstractOnScheduled(ProcessContext context) throws IOException {
        try {
            // This value will be null when called from ListHDFS, because it overrides all of the default
            // properties this processor sets. TODO: re-work ListHDFS to utilize Kerberos
            HdfsResources resources = hdfsResources.get();
            if (resources.getConfiguration() == null) {
                resources = resetHDFSResources(getConfigLocations(context), context);
                hdfsResources.set(resources);
            }
        } catch (Exception ex) {
            getLogger().error("HDFS Configuration failed", ex);
            hdfsResources.set(EMPTY_HDFS_RESOURCES);
            throw ex;
        }
    }

    protected List<String> getConfigLocations(PropertyContext context) {
            final ResourceReferences configResources = context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().asResources();
            final List<String> locations = configResources.asLocations();
            return locations;
    }

    @OnStopped
    public final void abstractOnStopped() {
        final HdfsResources resources = hdfsResources.get();
        if (resources != null) {
            // Attempt to close the FileSystem
            final FileSystem fileSystem = resources.getFileSystem();
            HDFSResourceHelper.closeFileSystem(fileSystem);
        }

        // Clear out the reference to the resources
        hdfsResources.set(EMPTY_HDFS_RESOURCES);
    }

    private static Configuration getConfigurationFromResources(final Configuration config, final List<String> locations) throws IOException {
        boolean foundResources = !locations.isEmpty();

        if (foundResources) {
            for (String resource : locations) {
                config.addResource(new Path(resource.trim()));
            }
        } else {
            // check that at least 1 non-default resource is available on the classpath
            String configStr = config.toString();
            for (String resource : configStr.substring(configStr.indexOf(":") + 1).split(",")) {
                if (!resource.contains("default") && config.getResource(resource.trim()) != null) {
                    foundResources = true;
                    break;
                }
            }
        }

        if (!foundResources) {
            throw new IOException("Could not find any of the " + HADOOP_CONFIGURATION_RESOURCES.getName() + " on the classpath");
        }
        return config;
    }

    /*
     * Reset Hadoop Configuration and FileSystem based on the supplied configuration resources.
     */
    HdfsResources resetHDFSResources(final List<String> resourceLocations, final ProcessContext context) throws IOException {
        Configuration config = new ExtendedConfiguration(getLogger());
        config.setClassLoader(Thread.currentThread().getContextClassLoader());

        getConfigurationFromResources(config, resourceLocations);

        // give sub-classes a chance to process configuration
        preProcessConfiguration(config, context);

        // first check for timeout on HDFS connection, because FileSystem has a hard coded 15 minute timeout
        checkHdfsUriForTimeout(config);

        // disable caching of Configuration and FileSystem objects, else we cannot reconfigure the processor without a complete
        // restart
        String disableCacheName = String.format("fs.%s.impl.disable.cache", FileSystem.getDefaultUri(config).getScheme());
        config.set(disableCacheName, "true");

        // If kerberos is enabled, create the file system as the kerberos principal
        // -- use RESOURCE_LOCK to guarantee UserGroupInformation is accessed by only a single thread at at time
        FileSystem fs;
        UserGroupInformation ugi;
        KerberosUser kerberosUser;
        synchronized (RESOURCES_LOCK) {
            if (SecurityUtil.isSecurityEnabled(config)) {
                kerberosUser = getKerberosUser(context);
                ugi = SecurityUtil.getUgiForKerberosUser(config, kerberosUser);
            } else {
                config.set("ipc.client.fallback-to-simple-auth-allowed", "true");
                config.set("hadoop.security.authentication", "simple");
                ugi = SecurityUtil.loginSimple(config);
                kerberosUser = null;
            }
            fs = getFileSystemAsUser(config, ugi);
        }
        getLogger().debug("resetHDFSResources UGI [{}], KerberosUser [{}]", ugi, kerberosUser);

        final Path workingDir = fs.getWorkingDirectory();
        getLogger().info("Initialized a new HDFS File System with working dir: {} default block size: {} default replication: {} config: {}",
                workingDir, fs.getDefaultBlockSize(workingDir), fs.getDefaultReplication(workingDir), config.toString());

        return new HdfsResources(config, fs, ugi, kerberosUser);
    }

    private KerberosUser getKerberosUser(final ProcessContext context) {
        // Check Kerberos User Service first, if present then get the KerberosUser from the service
        // The customValidate method ensures that KerberosUserService can't be set at the same time as the credentials service or explicit properties
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
        if (kerberosUserService != null) {
            return kerberosUserService.createKerberosUser();
        } else {
            throw new IllegalStateException("Unable to authenticate with Kerberos, no keytab or password was provided");
        }
    }

    /**
     * This method will be called after the Configuration has been created, but before the FileSystem is created,
     * allowing sub-classes to take further action on the Configuration before creating the FileSystem.
     *
     * @param config the Configuration that will be used to create the FileSystem
     * @param context the context that can be used to retrieve additional values
     */
    protected void preProcessConfiguration(final Configuration config, final ProcessContext context) {

    }

    /**
     * This exists in order to allow unit tests to override it so that they don't take several minutes waiting for UDP packets to be received
     *
     * @param config
     *            the configuration to use
     * @return the FileSystem that is created for the given Configuration
     * @throws IOException
     *             if unable to create the FileSystem
     */
    protected FileSystem getFileSystem(final Configuration config) throws IOException {
        return FileSystem.get(config);
    }

    protected FileSystem getFileSystemAsUser(final Configuration config, UserGroupInformation ugi) throws IOException {
        try {
            return ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(config));
        } catch (InterruptedException e) {
            throw new IOException("Unable to create file system: " + e.getMessage());
        }
    }

    /*
     * Drastically reduce the timeout of a socket connection from the default in FileSystem.get()
     */
    protected void checkHdfsUriForTimeout(Configuration config) throws IOException {
        URI hdfsUri = FileSystem.getDefaultUri(config);
        String address = hdfsUri.getAuthority();
        int port = hdfsUri.getPort();
        if (address == null || address.isEmpty() || port < 0) {
            return;
        }
        InetSocketAddress namenode = NetUtils.createSocketAddr(address, port);
        SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(config);
        Socket socket = null;
        try {
            socket = socketFactory.createSocket();
            NetUtils.connect(socket, namenode, 1000); // 1 second timeout
        } finally {
            IOUtils.closeQuietly(socket);
        }
    }

    /**
     * Returns the configured CompressionCodec, or null if none is configured.
     *
     * @param context
     *            the ProcessContext
     * @param configuration
     *            the Hadoop Configuration
     * @return CompressionCodec or null
     */
    protected org.apache.hadoop.io.compress.CompressionCodec getCompressionCodec(ProcessContext context, Configuration configuration) {
        org.apache.hadoop.io.compress.CompressionCodec codec = null;
        if (context.getProperty(COMPRESSION_CODEC).isSet()) {
            String compressionClassname = CompressionType.valueOf(context.getProperty(COMPRESSION_CODEC).getValue()).toString();
            CompressionCodecFactory ccf = new CompressionCodecFactory(configuration);
            codec = ccf.getCodecByClassName(compressionClassname);
        }

        return codec;
    }

    /**
     * Returns the relative path of the child that does not include the filename or the root path.
     *
     * @param root
     *            the path to relativize from
     * @param child
     *            the path to relativize
     * @return the relative path
     */
    public static String getPathDifference(final Path root, final Path child) {
        final int depthDiff = child.depth() - root.depth();
        if (depthDiff <= 1) {
            return "";
        }
        String lastRoot = root.getName();
        Path childsParent = child.getParent();
        final StringBuilder builder = new StringBuilder();
        builder.append(childsParent.getName());
        for (int i = (depthDiff - 3); i >= 0; i--) {
            childsParent = childsParent.getParent();
            String name = childsParent.getName();
            if (name.equals(lastRoot) && childsParent.toString().endsWith(root.toString())) {
                break;
            }
            builder.insert(0, Path.SEPARATOR).insert(0, name);
        }
        return builder.toString();
    }

    protected Configuration getConfiguration() {
        return hdfsResources.get().getConfiguration();
    }

    protected FileSystem getFileSystem() {
        return hdfsResources.get().getFileSystem();
    }

    protected UserGroupInformation getUserGroupInformation() {
        getLogger().trace("getting UGI instance");
        // if there is a KerberosUser associated with UGI, call checkTGTAndRelogin to ensure UGI's underlying Subject has a valid ticket
        SecurityUtil.checkTGTAndRelogin(getLogger(), hdfsResources.get().getKerberosUser());
        return hdfsResources.get().getUserGroupInformation();
    }

    boolean isLocalFileSystemAccessDenied() {
        return Boolean.parseBoolean(System.getenv(DENY_LFS_ACCESS));
    }

    protected boolean isFileSystemAccessDenied(final URI fileSystemUri) {
        boolean accessDenied;

        if (isLocalFileSystemAccessDenied()) {
            accessDenied = LOCAL_FILE_SYSTEM_URI.matcher(fileSystemUri.toString()).matches();
        } else {
            accessDenied = false;
        }

        return accessDenied;
    }

    static protected class ValidationResources {
        private final List<String> configLocations;
        private final Configuration configuration;

        public ValidationResources(final List<String> configLocations, final Configuration configuration) {
            this.configLocations = configLocations;
            this.configuration = configuration;
        }

        public List<String> getConfigLocations() {
            return configLocations;
        }

        public Configuration getConfiguration() {
            return configuration;
        }
    }

    protected Path getNormalizedPath(ProcessContext context, PropertyDescriptor property) {
        return getNormalizedPath(context, property, null);
    }

    protected Path getNormalizedPath(final String rawPath) {
       return getNormalizedPath(rawPath, Optional.empty());
    }

    protected Path getNormalizedPath(final ProcessContext context, final PropertyDescriptor property, final FlowFile flowFile) {
        final String propertyValue = context.getProperty(property).evaluateAttributeExpressions(flowFile).getValue();
        return getNormalizedPath(propertyValue, Optional.of(property.getDisplayName()));
    }

    private Path getNormalizedPath(final String rawPath, final Optional<String> propertyName) {
        final URI uri = new Path(rawPath).toUri();
        final URI fileSystemUri = getFileSystem().getUri();
        final String path;

        if (uri.getScheme() != null) {
            if (!uri.getScheme().equals(fileSystemUri.getScheme()) || (uri.getAuthority() != null && !uri.getAuthority().equals(fileSystemUri.getAuthority()))) {
                if (propertyName.isPresent()) {
                    getLogger().warn(NORMALIZE_ERROR_WITH_PROPERTY, propertyName, uri, fileSystemUri);
                } else {
                    getLogger().warn(NORMALIZE_ERROR_WITHOUT_PROPERTY, uri, fileSystemUri);
                }
            }

            path = uri.getPath();
        } else {
            path = rawPath;
        }

        return new Path(path.replaceAll("/+", "/"));
    }

    /**
     * Returns an optional with the first throwable in the causal chain that is assignable to the provided cause type,
     * and satisfies the provided cause predicate, {@link Optional#empty()} otherwise.
     * @param t The throwable to inspect for the cause.
     * @return Throwable Cause
     */
    protected <T extends Throwable> Optional<T> findCause(Throwable t, Class<T> expectedCauseType, Predicate<T> causePredicate) {
        Stream<Throwable> causalChain = Throwables.getCausalChain(t).stream();
        return causalChain
                .filter(expectedCauseType::isInstance)
                .map(expectedCauseType::cast)
                .filter(causePredicate)
                .findFirst();
    }

    protected boolean handleAuthErrors(Throwable t, ProcessSession session, ProcessContext context, BiConsumer<ProcessSession, ProcessContext> sessionHandler) {
        Optional<GSSException> causeOptional = findCause(t, GSSException.class, gsse -> GSSException.NO_CRED == gsse.getMajor());
        if (causeOptional.isPresent()) {

            getLogger().error("An error occurred while connecting to HDFS. Rolling back session and, and resetting HDFS resources", causeOptional.get());
            try {
                hdfsResources.set(resetHDFSResources(getConfigLocations(context), context));
            } catch (IOException ioe) {
                getLogger().error("An error occurred resetting HDFS resources, you may need to restart the processor.");
            }
            sessionHandler.accept(session, context);
            return true;
        }
        return false;
    }
}