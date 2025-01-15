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

package org.apache.nifi.graph;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.graph.gremlin.SimpleEntry;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Tags({"graph", "gremlin"})
@CapabilityDescription("This service interacts with a tinkerpop-compliant graph service, providing both script submission and bytecode submission capabilities. " +
        "Script submission is the default, with the script command being sent to the gremlin server as text. This should only be used for simple interactions with a tinkerpop-compliant server " +
        "such as counts or other operations that do not require the injection of custom classed. " +
        "Bytecode submission allows much more flexibility. When providing a jar, custom serializers can be used and pre-compiled graph logic can be utilized by groovy scripts" +
        "provided by processors such as the ExecuteGraphQueryRecord.")
@RequiresInstanceClassLoading
public class TinkerpopClientService extends AbstractControllerService implements GraphClientService {
    public static final String NOT_SUPPORTED = "NOT_SUPPORTED";
    private static final AllowableValue BYTECODE_SUBMISSION = new AllowableValue("bytecode-submission", "ByteCode Submission",
            "Groovy scripts are compiled within NiFi, with the GraphTraversalSource injected as a variable 'g'. Effectively allowing " +
                    "your logic to directly manipulates the graph without string serialization overheard."
            );

    private static final AllowableValue SCRIPT_SUBMISSION = new AllowableValue("script-submission", "Script Submission",
            "Script is sent to the gremlin server as a submission. Similar to a rest request. "
    );

    private static final AllowableValue YAML_SETTINGS = new AllowableValue("yaml-settings", "Yaml Settings",
            "Connection to the gremlin server will be specified via a YAML file (very flexible)");

    private static final AllowableValue SERVICE_SETTINGS = new AllowableValue("service-settings", "Service-Defined Settings",
            "Connection to the gremlin server will be specified via values on this controller (simpler). " +
                    "Only recommended for testing and development with a simple grpah instance. ");

    public static final PropertyDescriptor SUBMISSION_TYPE = new PropertyDescriptor.Builder()
            .name("submission-type")
            .displayName("Script Submission Type")
            .description("A selection that toggles for between script submission or as bytecode submission")
            .allowableValues(SCRIPT_SUBMISSION, BYTECODE_SUBMISSION)
            .defaultValue("script-submission")
            .required(true)
            .build();

    public static final PropertyDescriptor CONNECTION_SETTINGS = new PropertyDescriptor.Builder()
            .name("connection-settings")
            .displayName("Settings Specification")
            .description("Selecting \"Service-Defined Settings\" connects using the setting on this service. Selecting \"Yaml Settings\" uses the specified YAML file for connection settings. ")
            .allowableValues(SERVICE_SETTINGS, YAML_SETTINGS)
            .defaultValue("service-settings")
            .required(true)
            .build();

    public static final PropertyDescriptor CONTACT_POINTS = new PropertyDescriptor.Builder()
            .name("tinkerpop-contact-points")
            .displayName("Contact Points")
            .description("A comma-separated list of hostnames or IP addresses where an Gremlin-enabled server can be found.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(CONNECTION_SETTINGS, SERVICE_SETTINGS)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("tinkerpop-port")
            .displayName("Port")
            .description("The port where Gremlin Server is running on each host listed as a contact point.")
            .required(true)
            .defaultValue("8182")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(CONNECTION_SETTINGS, SERVICE_SETTINGS)
            .build();

    public static final PropertyDescriptor PATH = new PropertyDescriptor.Builder()
            .name("tinkerpop-path")
            .displayName("Path")
            .description("The URL path where Gremlin Server is running on each host listed as a contact point.")
            .required(true)
            .defaultValue("/gremlin")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(CONNECTION_SETTINGS, SERVICE_SETTINGS)
            .build();

    public static final PropertyDescriptor TRAVERSAL_SOURCE_NAME = new PropertyDescriptor.Builder()
            .name("gremlin-traversal-source-name")
            .displayName("Traversal Source Name")
            .description("An optional property that lets you set the name of the remote traversal instance. " +
                    "This can be really important when working with databases like JanusGraph that support " +
                    "multiple backend traversal configurations simultaneously.")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor REMOTE_OBJECTS_FILE = new PropertyDescriptor.Builder()
            .name("remote-objects-file")
            .displayName("Remote Objects File")
            .description("The remote-objects file YAML used for connecting to the gremlin server.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(CONNECTION_SETTINGS, YAML_SETTINGS)
            .build();

    public static final PropertyDescriptor USER_NAME = new PropertyDescriptor.Builder()
            .name("user-name")
            .displayName("Username")
            .description("The username used to authenticate with the gremlin server." +
                    " Note: when using a remote.yaml file, this username value (if set) will overload any " +
                    "username set in the YAML file.")
            .required(false)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("The password used to authenticate with the gremlin server." +
                    " Note: when using a remote.yaml file, this password setting (if set) will override any " +
                    "password set in the YAML file")
            .required(false)
            .sensitive(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor EXTRA_RESOURCE = new PropertyDescriptor.Builder()
            .name("extension")
            .displayName("Extension JARs")
            .description("A comma-separated list of Java JAR files to be loaded. This should contain any Serializers or other " +
                    "classes specified in the YAML file. Additionally, any custom classes required for the groovy script to " +
                    "work in the bytecode submission setting should also be contained in these JAR files.")
            .dependsOn(CONNECTION_SETTINGS, YAML_SETTINGS)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY, ResourceType.URL)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor EXTENSION_CLASSES = new PropertyDescriptor.Builder()
            .name("extension-classes")
            .displayName("Extension Classes")
            .addValidator(Validator.VALID)
            .description("A comma-separated list of fully qualified Java class names that correspond to classes to implement. This " +
                    "is useful for services such as JanusGraph that need specific serialization classes. " +
                    "This configuration property has no effect unless a value for the Extension JAR field is " +
                    "also provided.")
            .dependsOn(EXTRA_RESOURCE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(CONNECTION_SETTINGS, YAML_SETTINGS)
            .required(false)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SUBMISSION_TYPE,
            CONNECTION_SETTINGS,
            REMOTE_OBJECTS_FILE,
            EXTRA_RESOURCE,
            EXTENSION_CLASSES,
            CONTACT_POINTS,
            PORT,
            PATH,
            TRAVERSAL_SOURCE_NAME,
            USER_NAME,
            PASSWORD,
            SSL_CONTEXT_SERVICE
    );

    private GroovyShell groovyShell;
    private Map<String, Script> compiledCode;
    protected Cluster cluster;
    private String traversalSourceName;
    private GraphTraversalSource traversalSource;
    private boolean scriptSubmission = true;
    boolean usesSSL;
    protected String transitUrl;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        loadClasses(context);
        GroovyClassLoader loader = new GroovyClassLoader(this.getClass().getClassLoader());
        groovyShell = new GroovyShell(loader);
        compiledCode = new ConcurrentHashMap<>();

        if (context.getProperty(TRAVERSAL_SOURCE_NAME).isSet()) {
            traversalSourceName = context.getProperty(TRAVERSAL_SOURCE_NAME).evaluateAttributeExpressions()
                    .getValue();
        }

        scriptSubmission = context.getProperty(SUBMISSION_TYPE).getValue().equals(SCRIPT_SUBMISSION.getValue());

        cluster = buildCluster(context);
    }

    @OnDisabled
    public void shutdown() {
        try {
            compiledCode = null;
            if (traversalSource != null) {
                traversalSource.close();
            }
        } catch (Exception e) {
            throw new ProcessException(e);
        } finally {
            if (cluster != null) {
                cluster.close();
            }
            cluster = null;
            traversalSource = null;
        }
    }

    @Override
    public Map<String, String> executeQuery(String s, Map<String, Object> map, GraphQueryResultCallback graphQueryResultCallback) {
        try {
            if (scriptSubmission) {
                return scriptSubmission(s, map, graphQueryResultCallback);
            } else {
                return bytecodeSubmission(s, map, graphQueryResultCallback);
            }
        } catch (Exception ex) {
            throw new ProcessException(ex);
        }
    }

    @Override
    public String getTransitUrl() {
        return this.transitUrl;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Collection<ValidationResult> customValidate(ValidationContext context) {
        Collection<ValidationResult> results = new ArrayList<>();
        boolean jarsIsSet = !StringUtils.isEmpty(context.getProperty(EXTRA_RESOURCE).getValue());
        boolean clzIsSet = !StringUtils.isEmpty(context.getProperty(EXTENSION_CLASSES).getValue());

        if (jarsIsSet && clzIsSet) {
            try {
                final ClassLoader loader = ClassLoaderUtils
                        .getCustomClassLoader(context.getProperty(EXTRA_RESOURCE).getValue(), this.getClass().getClassLoader(), null);
                String[] classes = context.getProperty(EXTENSION_CLASSES).evaluateAttributeExpressions().getValue().split(",[\\s]*");
                for (String clz : classes) {
                    Class.forName(clz, true, loader);
                }
            } catch (Exception ex) {
                results.add(new ValidationResult.Builder().subject(EXTENSION_CLASSES.getDisplayName()).valid(false).explanation(ex.toString()).build());
            }
        }

        if (context.getProperty(USER_NAME).isSet() && !context.getProperty(PASSWORD).isSet()) {
            results.add(new ValidationResult.Builder()
                    .explanation("When specifying a username, the password must also be set").valid(false).build()
            );
        }
        if (context.getProperty(PASSWORD).isSet() && !context.getProperty(USER_NAME).isSet()) {
            results.add(new ValidationResult.Builder()
                    .explanation("When specifying a password, the password must also be set").valid(false).build()
            );
        }
        return results;
    }

    protected Cluster.Builder setupSSL(ConfigurationContext context, Cluster.Builder builder) {
        if (context.getProperty(SSL_CONTEXT_SERVICE).isSet()) {
            final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
            ApplicationProtocolConfig applicationProtocolConfig = new ApplicationProtocolConfig(ApplicationProtocolConfig.Protocol.NONE,
                    ApplicationProtocolConfig.SelectorFailureBehavior.FATAL_ALERT, ApplicationProtocolConfig.SelectedListenerFailureBehavior.FATAL_ALERT);
            JdkSslContext jdkSslContext = new JdkSslContext(sslContextProvider.createContext(), true, null,
                    IdentityCipherSuiteFilter.INSTANCE, applicationProtocolConfig, ClientAuth.NONE, null, false);

            builder
                    .enableSsl(true)
                    .sslContext(jdkSslContext);
            usesSSL = true;
        }

        return builder;
    }


    public void loadClasses(ConfigurationContext context) {
        String path = context.getProperty(EXTRA_RESOURCE).getValue();
        String classList = context.getProperty(EXTENSION_CLASSES).getValue();
        if (path != null && classList != null && !path.isEmpty() && !classList.isEmpty()) {
            try {
                ClassLoader loader = ClassLoaderUtils.getCustomClassLoader(path, this.getClass().getClassLoader(), null);
                String[] classes = context.getProperty(EXTENSION_CLASSES).evaluateAttributeExpressions().getValue().split(",[\\s]*");
                for (String cls : classes) {
                    Class<?> clz = Class.forName(cls.trim(), true, loader);
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug(clz.getName());
                    }
                }
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        }
    }


    protected Cluster buildCluster(ConfigurationContext context) {

        Cluster.Builder builder = Cluster.build();
        List<String> hosts = new ArrayList<>();
        if (!context.getProperty(REMOTE_OBJECTS_FILE).isSet()) {
            String contactProp = context.getProperty(CONTACT_POINTS).evaluateAttributeExpressions().getValue();
            int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
            String path = context.getProperty(PATH).evaluateAttributeExpressions().getValue();
            String[] contactPoints = contactProp.split(",[\\s]*");
            for (String contactPoint : contactPoints) {
                builder.addContactPoint(contactPoint.trim());
                hosts.add(contactPoint.trim());
            }
            builder.port(port);
            if (path != null && !path.isEmpty()) {
                builder.path(path);
            }
        } else {
            //ToDo: there is a bug in getting the hostname from the builder, therefore when doing
            // bytecode submission, the transitUrl ends up being effectively useless. Need to extract it
            // from the yaml to get this to work as expected.
            File yamlFile = new File(context.getProperty(REMOTE_OBJECTS_FILE).evaluateAttributeExpressions().getValue());
            try {
                builder = Cluster.build(yamlFile);
            } catch (Exception ex) {
                throw new ProcessException(ex);
            }
        }
        builder = setupSSL(context, builder);

        if (context.getProperty(USER_NAME).isSet() && context.getProperty(PASSWORD).isSet()) {
            String username = context.getProperty(USER_NAME).evaluateAttributeExpressions().getValue();
            String password = context.getProperty(PASSWORD).getValue();
            builder.credentials(username, password);
        }

        Cluster cluster = builder.create();

        transitUrl = String.format("gremlin%s://%s:%s%s", usesSSL ? "+ssl" : "",
                String.join(",", hosts), cluster.getPort(), cluster.getPath());

        return cluster;
    }

    protected Map<String, String> scriptSubmission(String query, Map<String, Object> parameters, GraphQueryResultCallback handler) {
        try {
            Client client = cluster.connect();
            Iterator<Result> iterator = client.submit(query, parameters).iterator();
            long count = 0;
            while (iterator.hasNext()) {
                Result result = iterator.next();
                Object obj = result.getObject();
                if (obj instanceof Map) {
                    handler.process((Map) obj, iterator.hasNext());
                } else {
                    handler.process(new HashMap<>() {{
                        put("result", obj);
                    }}, iterator.hasNext());
                }
                count++;
            }

            Map<String, String> resultAttributes = new HashMap<>();
            resultAttributes.put(NODES_CREATED, NOT_SUPPORTED);
            resultAttributes.put(RELATIONS_CREATED, NOT_SUPPORTED);
            resultAttributes.put(LABELS_ADDED, NOT_SUPPORTED);
            resultAttributes.put(NODES_DELETED, NOT_SUPPORTED);
            resultAttributes.put(RELATIONS_DELETED, NOT_SUPPORTED);
            resultAttributes.put(PROPERTIES_SET, NOT_SUPPORTED);
            resultAttributes.put(ROWS_RETURNED, String.valueOf(count));

            return resultAttributes;

        } catch (Exception ex) {
            throw new ProcessException(ex);
        }
    }

    protected Map<String, String> bytecodeSubmission(String s, Map<String, Object> map, GraphQueryResultCallback graphQueryResultCallback) {
        String hash = DigestUtils.sha256Hex(s);
        Script compiled;

        if (this.traversalSource == null) {
            this.traversalSource = createTraversal();
        }
        int rowsReturned = 0;

        if (compiledCode.containsKey(hash)) {
            compiled = compiledCode.get(hash);
        } else {
            compiled = groovyShell.parse(s);
            compiledCode.put(s, compiled);
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("{}", map);
        }

        Binding bindings = new Binding();
        map.forEach(bindings::setProperty);
        bindings.setProperty("g", traversalSource);
        bindings.setProperty("log", getLogger());
        try {
            compiled.setBinding(bindings);
            Object result = compiled.run();
            if (result instanceof Map) {
                Map<String, Object> resultMap = (Map<String, Object>) result;
                if (!resultMap.isEmpty()) {
                    Iterator outerResultSet = resultMap.entrySet().iterator();
                    while (outerResultSet.hasNext()) {
                        Map.Entry<String, Object> innerResultSet = (Map.Entry<String, Object>) outerResultSet.next();
                        if (innerResultSet.getValue() instanceof Map) {
                            Iterator resultSet = ((Map) innerResultSet.getValue()).entrySet().iterator();
                            while (resultSet.hasNext()) {
                                Map.Entry<String, Object> tempResult = (Map.Entry<String, Object>) resultSet.next();
                                Map<String, Object> tempRetObject = new HashMap<>();
                                tempRetObject.put(tempResult.getKey(), tempResult.getValue());
                                SimpleEntry<String, Object> returnObject = new SimpleEntry<>(tempResult.getKey(), tempRetObject);
                                Map<String, Object> resultReturnMap = new HashMap<>();
                                resultReturnMap.put(innerResultSet.getKey(), returnObject);
                                if (getLogger().isDebugEnabled()) {
                                    getLogger().debug("{}", resultReturnMap);
                                }
                                graphQueryResultCallback.process(resultReturnMap, resultSet.hasNext());
                            }
                        } else {
                            Map<String, Object> resultReturnMap = new HashMap<>();
                            resultReturnMap.put(innerResultSet.getKey(), innerResultSet.getValue());
                            graphQueryResultCallback.process(resultReturnMap, false);
                        }
                        rowsReturned++;
                    }

                }
            }
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        Map<String, String> resultAttributes = new HashMap<>();
        resultAttributes.put(ROWS_RETURNED, String.valueOf(rowsReturned));

        return resultAttributes;
    }

    protected GraphTraversalSource createTraversal() {
        GraphTraversalSource traversal;
        try {
            if (StringUtils.isEmpty(traversalSourceName)) {
                traversal = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster));
            } else {
                traversal = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster, traversalSourceName));
            }
        } catch (Exception e) {
            throw new ProcessException(e);
        }


        return traversal;
    }
}
