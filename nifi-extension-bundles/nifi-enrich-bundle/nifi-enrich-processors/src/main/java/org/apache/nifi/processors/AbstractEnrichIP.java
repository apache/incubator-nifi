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
package org.apache.nifi.processors;

import com.maxmind.geoip2.DatabaseReader;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.file.monitor.LastModifiedMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractEnrichIP extends AbstractProcessor {

    public static final PropertyDescriptor GEO_DATABASE_FILE = new PropertyDescriptor.Builder()
            // Name has been left untouched so that we don't cause a breaking change
            // but ideally this should be renamed to MaxMind Database File or something similar
            .name("Geo Database File")
            .displayName("MaxMind Database File")
            .description("Path to Maxmind IP Enrichment Database File")
            .required(true)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE, ResourceType.DIRECTORY)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor IP_ADDRESS_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("IP Address Attribute")
            .displayName("IP Address Attribute")
            .required(true)
            .description("The name of an attribute whose value is a dotted decimal IP address for which enrichment should occur")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .name("Log Level")
            .displayName("Log Level")
            .required(true)
            .description("The Log Level to use when an IP is not found in the database. Accepted values: INFO, DEBUG, WARN, ERROR.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(MessageLogLevel.WARN.toString())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_FOUND = new Relationship.Builder()
            .name("found")
            .description("Where to route flow files after successfully enriching attributes with data provided by database")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("Where to route flow files after unsuccessfully enriching attributes because no data was found")
            .build();

    enum MessageLogLevel {
        DEBUG, INFO, WARN, ERROR
    }

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_FOUND,
            REL_NOT_FOUND
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GEO_DATABASE_FILE,
            IP_ADDRESS_ATTRIBUTE,
            LOG_LEVEL
        );

    final AtomicReference<DatabaseReader> databaseReaderRef = new AtomicReference<>(null);
    private volatile SynchronousFileWatcher watcher;

    private final ReadWriteLock dbReadWriteLock = new ReentrantReadWriteLock();

    private volatile File dbFile;

    private volatile boolean needsReload = true;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        dbFile = context.getProperty(GEO_DATABASE_FILE).evaluateAttributeExpressions().asResource().asFile();
        this.watcher = new SynchronousFileWatcher(Paths.get(dbFile.toURI()), new LastModifiedMonitor(), 30000L);
        loadDatabaseFile();
    }

    protected void loadDatabaseFile() throws IOException {
        final StopWatch stopWatch = new StopWatch(true);
        final DatabaseReader reader = new DatabaseReader.Builder(dbFile).build();
        stopWatch.stop();
        getLogger().info("Completed loading of Maxmind Database.  Elapsed time was {} milliseconds.", stopWatch.getDuration(TimeUnit.MILLISECONDS));
        databaseReaderRef.set(reader);
    }

    @OnStopped
    public void closeReader() throws IOException {
        final DatabaseReader reader = databaseReaderRef.get();
        if (reader != null) {
            reader.close();
        }
    }

    protected SynchronousFileWatcher getWatcher() {
        return watcher;
    }

    protected Lock getDbWriteLock() {
        return dbReadWriteLock.writeLock();
    }

    protected Lock getDbReadLock() {
        return dbReadWriteLock.readLock();
    }

    protected boolean isNeedsReload() {
        return needsReload;
    }

    protected void setNeedsReload(final boolean needsReload) {
        this.needsReload = needsReload;
    }
}
