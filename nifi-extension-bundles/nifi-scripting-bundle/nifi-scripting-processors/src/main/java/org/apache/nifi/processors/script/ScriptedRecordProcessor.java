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
package org.apache.nifi.processors.script;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.search.Searchable;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

abstract class ScriptedRecordProcessor extends AbstractProcessor implements Searchable {
    protected static final Set<String> SCRIPT_OPTIONS = ScriptingComponentUtils.getAvailableEngines();

    protected volatile String scriptToRun = null;
    protected final AtomicReference<CompiledScript> compiledScriptRef = new AtomicReference<>();
    private final ScriptingComponentHelper scriptingComponentHelper = new ScriptingComponentHelper();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .displayName("Record Reader")
            .description("The Record Reader to use parsing the incoming FlowFile into Records")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Record Writer")
            .displayName("Record Writer")
            .description("The Record Writer to use for serializing Records after they have been transformed")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    static final PropertyDescriptor LANGUAGE = new PropertyDescriptor.Builder()
            .name("Script Engine")
            .displayName("Script Language")
            .description("The Language to use for the script")
            .allowableValues(SCRIPT_OPTIONS)
            .defaultValue("Groovy")
            .required(true)
            .build();

    protected static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            RECORD_READER,
            RECORD_WRITER,
            LANGUAGE,
            ScriptingComponentUtils.SCRIPT_BODY,
            ScriptingComponentUtils.SCRIPT_FILE,
            ScriptingComponentUtils.MODULES);


    @OnScheduled
    public void setup(final ProcessContext context) throws IOException {
        if (!scriptingComponentHelper.isInitialized.get()) {
            scriptingComponentHelper.createResources(false);
        }

        scriptingComponentHelper.setupVariables(context);
        scriptToRun = scriptingComponentHelper.getScriptBody();

        if (scriptToRun == null && scriptingComponentHelper.getScriptPath() != null) {
            try (final FileInputStream scriptStream = new FileInputStream(scriptingComponentHelper.getScriptPath())) {
                scriptToRun = IOUtils.toString(scriptStream, Charset.defaultCharset());
            }
        }

        // Create a script runner for each possible task
        final int maxTasks = context.getMaxConcurrentTasks();
        scriptingComponentHelper.setupScriptRunners(maxTasks, scriptToRun, getLogger());

        // Always compile when first run
        compiledScriptRef.set(null);
    }

    protected ScriptEvaluator createEvaluator(final ScriptEngine scriptEngine, final FlowFile flowFile) throws ScriptException {
        return new InterpretedScriptEvaluator(scriptEngine, scriptToRun, flowFile, getLogger());
    }

    private CompiledScript getOrCompileScript(final Compilable scriptEngine, final String scriptToRun) throws ScriptException {
        final CompiledScript existing = compiledScriptRef.get();
        if (existing != null) {
            return existing;
        }

        final CompiledScript compiled = scriptEngine.compile(scriptToRun);
        final boolean updated = compiledScriptRef.compareAndSet(null, compiled);
        if (updated) {
            return compiled;
        }

        return compiledScriptRef.get();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return scriptingComponentHelper.customValidate(validationContext);
    }

    @Override
    public Collection<SearchResult> search(final SearchContext context) {
        return ScriptingComponentUtils.search(context, getLogger());
    }

    protected static Bindings setupBindings(final ScriptEngine scriptEngine) {
        Bindings bindings = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);
        if (bindings == null) {
            bindings = new SimpleBindings();
        }

        scriptEngine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

        return bindings;
    }

    protected ScriptRunner pollScriptRunner() {
        final ScriptRunner scriptRunner = scriptingComponentHelper.scriptRunnerQ.poll();

        // This shouldn't happen. But just in case.
        if (scriptRunner == null) {
            throw new ProcessException("Could not acquire script runner!");
        }

        return scriptRunner;
    }

    protected void offerScriptRunner(ScriptRunner scriptRunner) {
        scriptingComponentHelper.scriptRunnerQ.offer(scriptRunner);
    }
}
