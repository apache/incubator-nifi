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
package org.apache.nifi.processors.groovyx;

import groovy.lang.GroovyShell;
import groovy.lang.Script;
import java.io.File;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.groovyx.flow.GroovyProcessSessionWrap;
import org.apache.nifi.processors.groovyx.sql.OSql;
import org.apache.nifi.processors.groovyx.util.Files;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.codehaus.groovy.runtime.ResourceGroovyMethods;
import org.codehaus.groovy.runtime.StackTraceUtils;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"script", "groovy", "groovyx"})
@CapabilityDescription(
        "Experimental Extended Groovy script processor. The script is responsible for "
        + "handling the incoming flow file (transfer to SUCCESS or remove, e.g.) as well as any flow files created by "
        + "the script. If the handling is incomplete or incorrect, the session will be rolled back.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
        }
)
@SeeAlso(classNames = {"org.apache.nifi.processors.script.ExecuteScript"})
@DynamicProperty(name = "A script engine property to update",
        value = "The value to set it to",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Updates a script engine property specified by the Dynamic Property's key with the value specified by the Dynamic Property's value. "
                + "Use `CTL.` to access any controller services, `SQL.` to access any DBCPServices, `RecordReader.` to access RecordReaderFactory instances, or "
                + "`RecordWriter.` to access any RecordSetWriterFactory instances.")
public class ExecuteGroovyScript extends AbstractProcessor {
    public static final String GROOVY_CLASSPATH = "${groovy.classes.path}";

    private static final String PRELOADS = "import org.apache.nifi.components.*;" + "import org.apache.nifi.flowfile.FlowFile;" + "import org.apache.nifi.processor.*;"
            + "import org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult;" + "import org.apache.nifi.processor.exception.*;" + "import org.apache.nifi.processor.io.*;"
            + "import org.apache.nifi.processor.util.*;" + "import org.apache.nifi.processors.script.*;" + "import org.apache.nifi.logging.ComponentLog;";

    public static final PropertyDescriptor SCRIPT_FILE = new PropertyDescriptor.Builder()
            .name("groovyx-script-file")
            .displayName("Script File")
            .required(false)
            .description("Path to script file to execute. Only one of Script File or Script Body may be used")
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SCRIPT_BODY = new PropertyDescriptor.Builder()
            .name("groovyx-script-body")
            .displayName("Script Body")
            .required(false)
            .description("Body of script to execute. Only one of Script File or Script Body may be used")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static String[] VALID_FAIL_STRATEGY = {"rollback", "transfer to failure"};
    public static final PropertyDescriptor FAIL_STRATEGY = new PropertyDescriptor.Builder()
            .name("groovyx-failure-strategy")
            .displayName("Failure strategy")
            .description("What to do with unhandled exceptions. If you want to manage exception by code then keep the default value `rollback`."
                    + " If `transfer to failure` selected and unhandled exception occurred then all flowFiles received from incoming queues in this session"
                    + " will be transferred to `failure` relationship with additional attributes set: ERROR_MESSAGE and ERROR_STACKTRACE."
                    + " If `rollback` selected and unhandled exception occurred then all flowFiles received from incoming queues will be penalized and returned."
                    + " If the processor has no incoming connections then this parameter has no effect."
                )
            .required(true).expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(VALID_FAIL_STRATEGY)
            .defaultValue(VALID_FAIL_STRATEGY[0])
            .build();

    public static final PropertyDescriptor ADD_CLASSPATH = new PropertyDescriptor.Builder()
            .name("groovyx-additional-classpath")
            .displayName("Additional classpath")
            .required(false)
            .description("Classpath list separated by semicolon or comma. You can use masks like `*`, `*.jar` in file name.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("FlowFiles that were successfully processed").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles that failed to be processed").build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SCRIPT_FILE,
            SCRIPT_BODY,
            FAIL_STRATEGY,
            ADD_CLASSPATH
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    //parameters evaluated on Start or on Validate
    File scriptFile = null;  //SCRIPT_FILE
    String scriptBody = null; //SCRIPT_BODY
    String addClasspath = null; //ADD_CLASSPATH
    String groovyClasspath = null; //evaluated from GROOVY_CLASSPATH = ${groovy.classes.path} global property
    //compiled script
    volatile GroovyShell shell = null; //new GroovyShell();
    volatile Class<Script> compiled = null;  //compiled script
    volatile long scriptLastModified = 0;  //last scriptFile modification to check if recompile required

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    private File asFile(String f) {
        if (f == null || f.isEmpty()) {
            return null;
        }
        return new File(f);
    }

    private void callScriptStatic(String method, final ProcessContext context) throws IllegalAccessException, java.lang.reflect.InvocationTargetException {
        if (compiled != null) {
            Method m = null;
            try {
                m = compiled.getDeclaredMethod(method, ProcessContext.class);
            } catch (NoSuchMethodException e) {
                // The method will not be invoked if it does not exist
            }
            if (m == null) {
                try {
                    m = compiled.getDeclaredMethod(method, Object.class);
                } catch (NoSuchMethodException e) {
                    // The method will not be invoked if it does not exist
                }
            }
            if (m != null) {
                m.invoke(null, context);
            }
        }
    }

    /**
     * Let's do validation by script compile at this point.
     *
     * @param context provides a mechanism for obtaining externally managed values, such as property values and supplies convenience methods for operating on those values
     * @return Collection of ValidationResult objects that will be added to any other validation findings - may be null
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        this.scriptFile = asFile(context.getProperty(SCRIPT_FILE).evaluateAttributeExpressions().getValue());  //SCRIPT_FILE
        this.scriptBody = context.getProperty(SCRIPT_BODY).getValue(); //SCRIPT_BODY
        this.addClasspath = context.getProperty(ADD_CLASSPATH).evaluateAttributeExpressions().getValue(); //ADD_CLASSPATH
        this.groovyClasspath = context.newPropertyValue(GROOVY_CLASSPATH).evaluateAttributeExpressions().getValue(); //evaluated from ${groovy.classes.path} global property

        final Collection<ValidationResult> results = new HashSet<>();
        try {
            getGroovyScript();
        } catch (Throwable t) {
            results.add(new ValidationResult.Builder().subject("GroovyScript").input(this.scriptFile != null ? this.scriptFile.toString() : null).valid(false).explanation(t.toString()).build());
        }
        return results;
    }

    /**
     * Hook method allowing subclasses to eagerly react to a configuration
     * change for the given property descriptor. As an alternative to using this
     * method a processor may simply get the latest value whenever it needs it
     * and if necessary lazily evaluate it.
     *
     * @param descriptor of the modified property
     * @param oldValue   non-null property value (previous)
     * @param newValue   the new property value or if null indicates the property was removed
     */
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        // Only re-create the shell if necessary, this helps if loading native libraries
        if (ExecuteGroovyScript.ADD_CLASSPATH.equals(descriptor)) {
            this.shell = null;
        }
        this.compiled = null;
        this.scriptLastModified = 0;
    }

    /**
     * Performs setup operations when the processor is scheduled to run. This includes evaluating the processor's
     * properties, as well as reloading the script (from file or the "Script Body" property)
     *
     * @param context the context in which to perform the setup operations
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.scriptFile = asFile(context.getProperty(SCRIPT_FILE).evaluateAttributeExpressions().getValue());  //SCRIPT_FILE
        this.scriptBody = context.getProperty(SCRIPT_BODY).getValue(); //SCRIPT_BODY
        this.addClasspath = context.getProperty(ADD_CLASSPATH).evaluateAttributeExpressions().getValue(); //ADD_CLASSPATH
        this.groovyClasspath = context.newPropertyValue(GROOVY_CLASSPATH).evaluateAttributeExpressions().getValue(); //evaluated from ${groovy.classes.path} global property
        try {
            //compile if needed
            getGroovyScript();
        } catch (Throwable t) {
            getLogger().error("Load script failed", t);
            throw new ProcessException("Load script failed: " + t, t);
        }
        try {
            callScriptStatic("onStart", context);
        } catch (Throwable t) {
            getLogger().error("onStart failed", t);
            throw new ProcessException("onStart failed: " + t, t);
        }
    }
    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        try {
            callScriptStatic("onUnscheduled", context);
        } catch (Throwable t) {
            throw new ProcessException("onUnscheduled failed: " + t, t);
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        try {
            callScriptStatic("onStop", context);
        } catch (Throwable t) {
            throw new ProcessException("Failed to finalize groovy script:\n" + t, t);
        }
        //reset of compiled script not needed here because we did it onPropertyModified
    }

    // used in validation and processing
    @SuppressWarnings("unchecked")
    Script getGroovyScript() throws Throwable {
        GroovyMethods.init();
        if (scriptBody != null && scriptFile != null) {
            throw new ProcessException("Only one parameter accepted: `" + SCRIPT_BODY.getDisplayName() + "` or `" + SCRIPT_FILE.getDisplayName() + "`");
        }
        if (scriptBody == null && scriptFile == null) {
            throw new ProcessException("At least one parameter required: `" + SCRIPT_BODY.getDisplayName() + "` or `" + SCRIPT_FILE.getDisplayName() + "`");
        }

        if (shell == null) {
            CompilerConfiguration conf = new CompilerConfiguration();
            conf.setDebug(true);
            shell = new GroovyShell(conf);
            if (addClasspath != null && !addClasspath.isEmpty()) {
                for (File fcp : Files.listPathsFiles(addClasspath)) {
                    if (!fcp.exists()) {
                        throw new ProcessException("Path not found `" + fcp + "` for `" + ADD_CLASSPATH.getDisplayName() + "`");
                    }
                    shell.getClassLoader().addClasspath(fcp.toString());
                }
            }
            //try to add classpath with groovy classes
            if (groovyClasspath != null && !groovyClasspath.isEmpty()) {
                shell.getClassLoader().addClasspath(groovyClasspath);
            }
        }
        Script script = null;
        if (compiled != null && scriptFile != null && scriptLastModified != scriptFile.lastModified() && System.currentTimeMillis() - scriptFile.lastModified() > 3000) {
            //force recompile if script file has been changed
            compiled = null;
        }
        if (compiled == null) {
            String scriptName;
            String scriptText;
            if (scriptFile != null) {
                scriptName = scriptFile.getName();
                scriptLastModified = scriptFile.lastModified();
                scriptText = ResourceGroovyMethods.getText(scriptFile, "UTF-8");
            } else {
                scriptName = "Script" + Long.toHexString(scriptBody.hashCode()) + ".groovy";
                scriptText = scriptBody;
            }
            script = shell.parse(PRELOADS + scriptText, scriptName);
            compiled = (Class<Script>) script.getClass();
        }
        if (script == null) {
            script = compiled.getDeclaredConstructor().newInstance();
        }
        Thread.currentThread().setContextClassLoader(shell.getClassLoader());
        return script;
    }

    /**
     * init SQL variables from DBCP services
     */
    private void onInitSQL(Map<String, Object> SQL) throws SQLException {
        for (Map.Entry<String, Object> e : SQL.entrySet()) {
            DBCPService s = (DBCPService) e.getValue();
            OSql sql = new OSql(s.getConnection(Collections.emptyMap()));
            //try to set autocommit to false
            try {
                if (sql.getConnection().getAutoCommit()) {
                    try {
                        sql.getConnection().setAutoCommit(false);
                    } catch (SQLFeatureNotSupportedException sfnse) {
                        getLogger().debug("setAutoCommit(false) not supported by this driver");
                    }
                }
            } catch (Throwable ei) {
                getLogger().warn("Failed to set autocommit=false for `{}`", e.getKey(), ei);
            }
            e.setValue(sql);
        }
    }

    /**
     * before commit SQL services
     */
    private void onCommitSQL(Map<String, Object> SQL) throws SQLException {
        for (Map.Entry<String, Object> e : SQL.entrySet()) {
            OSql sql = (OSql) e.getValue();
            if (!sql.getConnection().getAutoCommit()) {
                sql.commit();
            }
        }
    }

    /**
     * finalize SQL services. no exceptions should be thrown.
     */
    private void onFinitSQL(Map<String, Object> SQL) {
        for (Map.Entry<String, Object> e : SQL.entrySet()) {
            OSql sql = (OSql) e.getValue();
            try {
                if (!sql.getConnection().getAutoCommit()) {
                    try {
                        sql.getConnection().setAutoCommit(true); //default autocommit value in nifi
                    } catch (SQLFeatureNotSupportedException sfnse) {
                        getLogger().debug("setAutoCommit(true) not supported by this driver");
                    }
                }
            } catch (Throwable ei) {
                getLogger().warn("Failed to set autocommit=true for `{}`", e.getKey(), ei);
            }
            try {
                sql.close();
                sql = null;
            } catch (Throwable ei) {
                // Nothing to do
            }
        }
    }

    /**
     * exception SQL services
     */
    private void onFailSQL(Map<String, Object> SQL) {
        for (Map.Entry<String, Object> e : SQL.entrySet()) {
            OSql sql = (OSql) e.getValue();
            try {
                if (!sql.getConnection().getAutoCommit()) {
                    sql.rollback();
                }
            } catch (Throwable ei) {
                //the rollback error is usually not important, rather it is the DML error that is really important
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession _session) throws ProcessException {
        boolean toFailureOnError = VALID_FAIL_STRATEGY[1].equals(context.getProperty(FAIL_STRATEGY).getValue());
        //create wrapped session to control list of newly created and files got from this session.
        //so transfer original input to failure will be possible
        GroovyProcessSessionWrap session = new GroovyProcessSessionWrap(_session, toFailureOnError);

        Map<String, Object> CTL = new AccessMap("CTL");
        Map<String, Object> SQL = new AccessMap("SQL");
        Map<String, Object> RECORD_READER = new AccessMap("RecordReader");
        Map<String, Object> RECORD_SET_WRITER = new AccessMap("RecordSetWriter");

        try {
            Script script = getGroovyScript(); //compilation must be moved to validation
            Map bindings = script.getBinding().getVariables();

            bindings.clear();

            // Find the user-added properties and bind them for the script
            for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
                if (property.getKey().isDynamic()) {
                    if (property.getKey().getName().startsWith("CTL.")) {
                        //get controller service
                        ControllerService ctl = context.getProperty(property.getKey()).asControllerService(ControllerService.class);
                        CTL.put(property.getKey().getName().substring(4), ctl);
                    } else if (property.getKey().getName().startsWith("SQL.")) {
                        DBCPService dbcp = context.getProperty(property.getKey()).asControllerService(DBCPService.class);
                        SQL.put(property.getKey().getName().substring(4), dbcp);
                    } else if (property.getKey().getName().startsWith("RecordReader.")) {
                        // Get RecordReaderFactory controller service
                        RecordReaderFactory recordReader = context.getProperty(property.getKey()).asControllerService(RecordReaderFactory.class);
                        RECORD_READER.put(property.getKey().getName().substring(13), recordReader);
                    } else if (property.getKey().getName().startsWith("RecordWriter.")) {
                        // Get RecordWriterFactory controller service
                        RecordSetWriterFactory recordWriter = context.getProperty(property.getKey()).asControllerService(RecordSetWriterFactory.class);
                        RECORD_SET_WRITER.put(property.getKey().getName().substring(13), recordWriter);
                    } else {
                        // Add the dynamic property bound to its full PropertyValue to the script engine
                        if (property.getValue() != null) {
                            bindings.put(property.getKey().getName(), context.getProperty(property.getKey()));
                        }
                    }
                }
            }
            onInitSQL(SQL);

            bindings.put("session", session);
            bindings.put("context", context);
            bindings.put("log", getLogger());
            bindings.put("REL_SUCCESS", REL_SUCCESS);
            bindings.put("REL_FAILURE", REL_FAILURE);
            bindings.put("CTL", CTL);
            bindings.put("SQL", SQL);
            bindings.put("RecordReader", RECORD_READER);
            bindings.put("RecordWriter", RECORD_SET_WRITER);

            script.run();
            bindings.clear();

            onCommitSQL(SQL);
            session.commitAsync();
        } catch (Throwable t) {
            getLogger().error(t.toString(), t);
            onFailSQL(SQL);
            if (toFailureOnError) {
                //transfer all received to failure with two new attributes: ERROR_MESSAGE and ERROR_STACKTRACE.
                session.revertReceivedTo(REL_FAILURE, StackTraceUtils.deepSanitize(t));
            } else {
                session.rollback(true);
            }
        } finally {
            onFinitSQL(SQL);
        }

    }

    /**
     * Returns a PropertyDescriptor for the given name. This is for the user to be able to define their own properties
     * which will be available as variables in the script
     *
     * @param propertyDescriptorName used to lookup if any property descriptors exist for that name
     * @return a PropertyDescriptor object corresponding to the specified dynamic property name
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (propertyDescriptorName.startsWith("CTL.")) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .required(false)
                    .description("Controller service accessible from code as `" + propertyDescriptorName + "`")
                    .dynamic(true)
                    .identifiesControllerService(ControllerService.class)
                    .build();
        }
        if (propertyDescriptorName.startsWith("SQL.")) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .required(false)
                    .description("The `groovy.sql.Sql` object created from DBCP Controller service and accessible from code as `" + propertyDescriptorName + "`")
                    .dynamic(true)
                    .identifiesControllerService(DBCPService.class)
                    .build();
        }
        if (propertyDescriptorName.startsWith("RecordReader.")) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .displayName(propertyDescriptorName)
                    .required(false)
                    .description("RecordReaderFactory controller service accessible from code as `" + propertyDescriptorName + "`")
                    .dynamic(true)
                    .identifiesControllerService(RecordReaderFactory.class)
                    .build();
        }
        if (propertyDescriptorName.startsWith("RecordWriter.")) {
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .displayName(propertyDescriptorName)
                    .required(false)
                    .description("RecordSetWriterFactory controller service accessible from code as `" + propertyDescriptorName + "`")
                    .dynamic(true)
                    .identifiesControllerService(RecordSetWriterFactory.class)
                    .build();
        }
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .dynamic(true)
                .build();
    }

    /** simple HashMap with exception on access of non-existent key */
    private static class AccessMap extends HashMap<String, Object> {
        private String parentKey;
        AccessMap(String parentKey) {
            this.parentKey = parentKey;
        }
        @Override
        public Object get(Object key) {
            if (!containsKey(key)) {
                throw new RuntimeException("The `" + parentKey + "." + key + "` not defined in processor properties");
            }
            return super.get(key);
        }
    }
}