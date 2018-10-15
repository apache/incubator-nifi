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
package org.apache.nifi.processors.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SupportsBatching
@Tags({"cassandra", "cql", "put", "insert", "update", "set"})
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Execute provided Cassandra Query Language (CQL) statement on a Cassandra 1.x, 2.x, or 3.0.x cluster. "
        + "The content of an incoming FlowFile is expected to be the CQL command to execute. The CQL command may use "
        + "the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes with the "
        + "naming convention cql.args.N.type and cql.args.N.value, where N is a positive integer. The cql.args.N.type "
        + "is expected to be a lowercase string indicating the Cassandra type.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "cql.args.N.type",
                description = "Incoming FlowFiles are expected to be parameterized CQL statements. The type of each "
                        + "parameter is specified as a lowercase string corresponding to the Cassandra data type (text, "
                        + "int, boolean, e.g.). In the case of collections, the primitive type(s) of the elements in the "
                        + "collection should be comma-delimited, follow the collection type, and be enclosed in angle brackets "
                        + "(< and >), for example set<text> or map<timestamp, int>."),
        @ReadsAttribute(attribute = "cql.args.N.value",
                description = "Incoming FlowFiles are expected to be parameterized CQL statements. The value of the "
                        + "parameters are specified as cql.args.1.value, cql.args.2.value, cql.args.3.value, and so on. The "
                        + " type of the cql.args.1.value parameter is specified by the cql.args.1.type attribute.")
})
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutCassandraQL extends AbstractCassandraProcessor {

    public static final PropertyDescriptor STATEMENT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .displayName("Max Wait Time")
            .description("The maximum amount of time allowed for a running CQL select query. Must be of format "
                    + "<duration> <TimeUnit> where <duration> is a non-negative integer and TimeUnit is a supported "
                    + "Time Unit, such as: nanos, millis, secs, mins, hrs, days. A value of zero means there is no limit. ")
            .defaultValue("0 seconds")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor STATEMENT_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("putcql-stmt-cache-size")
            .displayName("Statement Cache Size")
            .description("The maximum number of CQL Prepared Statements to cache. This can improve performance if many incoming flow files have the same CQL statement "
                    + "with different values for the parameters. If this property is set to zero, the cache is effectively disabled.")
            .defaultValue("0")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;

    private final static Set<Relationship> relationships;

    /**
     * LRU cache for the compiled patterns. The size of the cache is determined by the value of the Statement Cache Size property
     */
    @VisibleForTesting
    private ConcurrentMap<String, PreparedStatement> statementCache;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(STATEMENT_TIMEOUT);
        _propertyDescriptors.add(STATEMENT_CACHE_SIZE);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_RETRY);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        ComponentLog log = getLogger();

        // Initialize the prepared statement cache
        int statementCacheSize = context.getProperty(STATEMENT_CACHE_SIZE).evaluateAttributeExpressions().asInteger();
        statementCache =  CacheBuilder.newBuilder()
                .maximumSize(statementCacheSize)
                .<String, PreparedStatement>build()
                .asMap();

        try {
            connectToCassandra(context);

        } catch (final NoHostAvailableException nhae) {
            log.error("No host in the Cassandra cluster can be contacted successfully to execute this statement", nhae);
            // Log up to 10 error messages. Otherwise if a 1000-node cluster was specified but there was no connectivity,
            // a thousand error messages would be logged. However we would like information from Cassandra itself, so
            // cap the error limit at 10, format the messages, and don't include the stack trace (it is displayed by the
            // logger message above).
            log.error(nhae.getCustomMessage(10, true, false));
            throw new ProcessException(nhae);
        } catch (final AuthenticationException ae) {
            log.error("Invalid username/password combination", ae);
            throw new ProcessException(ae);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        ComponentLog logger = getLogger();
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        final long statementTimeout = context.getProperty(STATEMENT_TIMEOUT).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);
        final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());

        // The documentation for the driver recommends the session remain open the entire time the processor is running
        // and states that it is thread-safe. This is why connectionSession is not in a try-with-resources.
        final Session connectionSession = cassandraSession.get();

        String cql = getCQL(session, flowFile, charset);
        try {
            PreparedStatement statement = statementCache.get(cql);
            if(statement == null) {
                statement = connectionSession.prepare(cql);
                statementCache.put(cql, statement);
            }
            BoundStatement boundStatement = statement.bind();

            buildBoundStatement(flowFile, boundStatement);

            try {
                ResultSetFuture future = connectionSession.executeAsync(boundStatement);
                if (statementTimeout > 0) {
                    future.getUninterruptibly(statementTimeout, TimeUnit.MILLISECONDS);
                } else {
                    future.getUninterruptibly();
                }
                // Emit a Provenance SEND event
                final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                // This isn't a real URI but since Cassandra is distributed we just use the cluster name
                String transitUri = "cassandra://" + connectionSession.getCluster().getMetadata().getClusterName();
                session.getProvenanceReporter().send(flowFile, transitUri, transmissionMillis, true);
                session.transfer(flowFile, REL_SUCCESS);

            } catch (final TimeoutException e) {
                throw new ProcessException(e);
            }


        } catch (final NoHostAvailableException nhae) {
            getLogger().error("No host in the Cassandra cluster can be contacted successfully to execute this statement", nhae);
            // Log up to 10 error messages. Otherwise if a 1000-node cluster was specified but there was no connectivity,
            // a thousand error messages would be logged. However we would like information from Cassandra itself, so
            // cap the error limit at 10, format the messages, and don't include the stack trace (it is displayed by the
            // logger message above).
            getLogger().error(nhae.getCustomMessage(10, true, false));
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_RETRY);

        } catch (final QueryExecutionException qee) {
            logger.error("Cannot execute the statement with the requested consistency level successfully", qee);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_RETRY);

        } catch (final QueryValidationException qve) {
            logger.error("The CQL statement {} is invalid due to syntax error, authorization issue, or another "
                            + "validation problem; routing {} to failure",
                    new Object[]{cql, flowFile}, qve);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);

        } catch (final ProcessException e) {
            logger.error("Unable to execute CQL select statement {} for {} due to {}; routing to failure",
                    new Object[]{cql, flowFile, e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Determines the CQL statement that should be executed for the given FlowFile
     *
     * @param session  the session that can be used to access the given FlowFile
     * @param flowFile the FlowFile whose CQL statement should be executed
     * @return the CQL that is associated with the given FlowFile
     */

    private String getCQL(final ProcessSession session, final FlowFile flowFile, final Charset charset) {
        // Read the CQL from the FlowFile's content
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });

        // Create the PreparedStatement string to use for this FlowFile.
        return new String(buffer, charset);
    }

    @OnStopped
    public void stop() {
        super.stop();
        statementCache.clear();
    }
}
