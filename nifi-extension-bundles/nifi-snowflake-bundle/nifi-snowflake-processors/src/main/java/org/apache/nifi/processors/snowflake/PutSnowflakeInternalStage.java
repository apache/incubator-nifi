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

package org.apache.nifi.processors.snowflake;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.snowflake.util.SnowflakeInternalStageType;
import org.apache.nifi.processors.snowflake.util.SnowflakeInternalStageTypeParameters;
import org.apache.nifi.processors.snowflake.util.SnowflakeProperties;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.nifi.processors.snowflake.util.SnowflakeAttributes.ATTRIBUTE_STAGED_FILE_PATH;

@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = ATTRIBUTE_STAGED_FILE_PATH,
                description = "Staged file path")
})
@Tags({"snowflake", "jdbc", "database", "connection", "snowpipe"})
@CapabilityDescription("Puts files into a Snowflake internal stage. The internal stage must be created in the Snowflake account beforehand."
        + " This processor can be connected to a StartSnowflakeIngest processor to ingest the file in the internal stage")
@SeeAlso({StartSnowflakeIngest.class, GetSnowflakeIngestStatus.class})
public class PutSnowflakeInternalStage extends AbstractProcessor {

    public static final PropertyDescriptor SNOWFLAKE_CONNECTION_PROVIDER = new PropertyDescriptor.Builder()
            .name("snowflake-connection-provider")
            .displayName("Snowflake Connection Provider")
            .description("Specifies the Controller Service to use for creating SQL connections to Snowflake.")
            .identifiesControllerService(SnowflakeConnectionProviderService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor INTERNAL_STAGE_TYPE = new PropertyDescriptor.Builder()
            .name("internal-stage-type")
            .displayName("Internal Stage Type")
            .description("The type of internal stage to use")
            .allowableValues(SnowflakeInternalStageType.class)
            .required(true)
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.DATABASE)
            .dependsOn(INTERNAL_STAGE_TYPE, SnowflakeInternalStageType.NAMED, SnowflakeInternalStageType.TABLE)
            .build();

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SnowflakeProperties.SCHEMA)
            .dependsOn(INTERNAL_STAGE_TYPE, SnowflakeInternalStageType.NAMED, SnowflakeInternalStageType.TABLE)
            .build();

    public static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("table")
            .displayName("Table")
            .description("The name of the table in the Snowflake account.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(INTERNAL_STAGE_TYPE, SnowflakeInternalStageType.TABLE)
            .build();

    public static final PropertyDescriptor INTERNAL_STAGE = new PropertyDescriptor.Builder()
            .name("internal-stage")
            .displayName("Stage")
            .description("The name of the internal stage in the Snowflake account to put files into.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(INTERNAL_STAGE_TYPE, SnowflakeInternalStageType.NAMED)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles of successful PUT operation")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("For FlowFiles of failed PUT operation")
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SNOWFLAKE_CONNECTION_PROVIDER,
            INTERNAL_STAGE_TYPE,
            DATABASE,
            SCHEMA,
            TABLE,
            INTERNAL_STAGE
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

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
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final SnowflakeInternalStageType internalStageType = context.getProperty(INTERNAL_STAGE_TYPE).asAllowableValue(SnowflakeInternalStageType.class);
        final SnowflakeInternalStageTypeParameters parameters = getSnowflakeInternalStageTypeParameters(context, flowFile);
        final String internalStageName = internalStageType.getStage(parameters);
        final SnowflakeConnectionProviderService connectionProviderService =
                context.getProperty(SNOWFLAKE_CONNECTION_PROVIDER)
                        .asControllerService(SnowflakeConnectionProviderService.class);

        final String stagedFileName = UUID.randomUUID().toString();
        try (final InputStream inputStream = session.read(flowFile);
                final SnowflakeConnectionWrapper snowflakeConnection = connectionProviderService.getSnowflakeConnection()) {
            snowflakeConnection.unwrap()
                    .uploadStream(internalStageName, "", inputStream, stagedFileName, false);
        } catch (SQLException e) {
            getLogger().error("Failed to upload FlowFile content to internal Snowflake stage [{}]. Staged file path [{}]", internalStageName, stagedFileName, e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        } catch (IOException e) {
            throw new ProcessException("Failed to read FlowFile content", e);
        }

        flowFile = session.putAttribute(flowFile, ATTRIBUTE_STAGED_FILE_PATH, stagedFileName);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private SnowflakeInternalStageTypeParameters getSnowflakeInternalStageTypeParameters(ProcessContext context,
            FlowFile flowFile) {
        final String database = context.getProperty(DATABASE).evaluateAttributeExpressions().getValue();
        final String schema = context.getProperty(SCHEMA).evaluateAttributeExpressions().getValue();
        final String table = context.getProperty(TABLE).evaluateAttributeExpressions(flowFile).getValue();
        final String stageName = context.getProperty(INTERNAL_STAGE).evaluateAttributeExpressions(flowFile).getValue();
        return new SnowflakeInternalStageTypeParameters(database, schema, table, stageName);
    }
}
