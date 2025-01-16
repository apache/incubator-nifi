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
package org.apache.nifi.toolkit.cli.impl.command.nifi.pg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;
import org.apache.nifi.toolkit.client.FlowClient;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ProcessGroupBox;
import org.apache.nifi.toolkit.client.ProcessGroupClient;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientsEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * Command for importing a flow to NiFi from NiFi Registry.
 */
public class PGImport extends AbstractNiFiCommand<StringResult> {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public PGImport() {
        super("pg-import", StringResult.class);
    }

    @Override
    public String getDescription() {
        return "Creates a new process group by importing a versioned flow from a registry. If no process group id is " +
                "specified, then the created process group will be placed in the root group. If only one registry client " +
                "exists in NiFi, then it does not need to be specified and will be automatically selected. The x and y " +
                "coordinates for the position of the imported process group may be optionally specified. If left blank, " +
                "the position will automatically be selected to avoid overlapping with existing process groups.";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.INPUT_SOURCE.createOption());
        addOption(CommandOption.REGISTRY_CLIENT_ID.createOption());
        addOption(CommandOption.BUCKET_ID.createOption());
        addOption(CommandOption.FLOW_ID.createOption());
        addOption(CommandOption.FLOW_VERSION.createOption());
        addOption(CommandOption.FLOW_BRANCH.createOption());
        addOption(CommandOption.POS_X.createOption());
        addOption(CommandOption.POS_Y.createOption());
        addOption(CommandOption.KEEP_EXISTING_PARAMETER_CONTEXT.createOption());
    }

    @Override
    public StringResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException {

        final String inputSource = getArg(properties, CommandOption.INPUT_SOURCE);

        final String bucketId = getArg(properties, CommandOption.BUCKET_ID);
        final String flowId = getArg(properties, CommandOption.FLOW_ID);
        final String flowVersion = getArg(properties, CommandOption.FLOW_VERSION);
        final String flowBranch = getArg(properties, CommandOption.FLOW_BRANCH);

        final String posXStr = getArg(properties, CommandOption.POS_X);
        final String posYStr = getArg(properties, CommandOption.POS_Y);

        String keepExistingPC = getArg(properties, CommandOption.KEEP_EXISTING_PARAMETER_CONTEXT);

        final boolean posXExists = StringUtils.isNotBlank(posXStr);
        final boolean posYExists = StringUtils.isNotBlank(posYStr);
        final File input = new File(inputSource);

        if (StringUtils.isBlank(inputSource)) {
            if (StringUtils.isBlank(bucketId)) {
                throw new IllegalArgumentException("Input path is not specified so Bucket ID must be specified");
            }
            if (StringUtils.isBlank(flowId)) {
                throw new IllegalArgumentException("Input path is not specified so Flow ID must be specified");
            }
            if (StringUtils.isBlank(flowVersion)) {
                throw new IllegalArgumentException("Input path is not specified so Flow Version must be specified");
            }
        } else {
            if (!input.exists() || !input.isFile() || !input.canRead()) {
                throw new IllegalArgumentException("Specified input is not a local readable file: " + inputSource);
            }
            if (StringUtils.isNotBlank(bucketId)) {
                throw new IllegalArgumentException("Input path is specified so Bucket ID should not be specified");
            }
            if (StringUtils.isNotBlank(flowId)) {
                throw new IllegalArgumentException("Input path is specified so Flow ID should not be specified");
            }
            if (StringUtils.isNotBlank(flowVersion)) {
                throw new IllegalArgumentException("Input path is specified so Flow Version should not be specified");
            }
            if (StringUtils.isNotBlank(flowBranch)) {
                throw new IllegalArgumentException("Input path is specified so Flow Branch should not be specified");
            }
        }

        if ((posXExists && !posYExists)) {
            throw new IllegalArgumentException("Missing Y position - Please specify both X and Y, or specify neither");
        }

        if ((posYExists && !posXExists)) {
            throw new IllegalArgumentException("Missing X position - Please specify both X and Y, or specify neither");
        }

        if (StringUtils.isNotBlank(keepExistingPC) && !(keepExistingPC.equals("true") || keepExistingPC.equals("false"))) {
            throw new IllegalArgumentException("Keep Existing Parameter Context must be either true or false");
        } else if (StringUtils.isBlank(keepExistingPC)) {
            keepExistingPC = "true";
        }

        // get the optional id of the parent PG, otherwise fallback to the root group
        String parentPgId = getArg(properties, CommandOption.PG_ID);
        if (StringUtils.isBlank(parentPgId)) {
            final FlowClient flowClient = client.getFlowClient();
            parentPgId = flowClient.getRootGroupId();
        }

        final PositionDTO posDto = new PositionDTO();
        if (posXExists && posYExists) {
            posDto.setX(Double.parseDouble(posXStr));
            posDto.setY(Double.parseDouble(posYStr));
        } else {
            final ProcessGroupBox pgBox = client.getFlowClient().getSuggestedProcessGroupCoordinates(parentPgId);
            posDto.setX(Integer.valueOf(pgBox.getX()).doubleValue());
            posDto.setY(Integer.valueOf(pgBox.getY()).doubleValue());
        }

        final ProcessGroupClient pgClient = client.getProcessGroupClient();
        ProcessGroupEntity createdEntity = null;

        if (StringUtils.isBlank(inputSource)) {

            // if a registry client is specified use it, otherwise see if there is only one
            // available and use that, if more than one is available then throw an exception
            // because we don't know which one to use
            String registryId = getArg(properties, CommandOption.REGISTRY_CLIENT_ID);
            if (StringUtils.isBlank(registryId)) {
                final FlowRegistryClientsEntity registries = client.getControllerClient().getRegistryClients();

                final Set<FlowRegistryClientEntity> entities = registries.getRegistries();
                if (entities == null || entities.isEmpty()) {
                    throw new NiFiClientException("No registry clients available");
                }

                if (entities.size() == 1) {
                    registryId = entities.stream().findFirst().get().getId();
                } else {
                    throw new MissingOptionException(CommandOption.REGISTRY_CLIENT_ID.getLongName()
                            + " must be provided when there is more than one available");
                }
            }

            final VersionControlInformationDTO versionControlInfo = new VersionControlInformationDTO();
            versionControlInfo.setRegistryId(registryId);
            versionControlInfo.setBucketId(bucketId);
            versionControlInfo.setFlowId(flowId);
            versionControlInfo.setVersion(flowVersion);

            if (StringUtils.isNotBlank(flowBranch)) {
                versionControlInfo.setBranch(flowBranch);
            }

            final ProcessGroupDTO pgDto = new ProcessGroupDTO();
            pgDto.setVersionControlInformation(versionControlInfo);
            pgDto.setPosition(posDto);

            final ProcessGroupEntity pgEntity = new ProcessGroupEntity();
            pgEntity.setComponent(pgDto);
            pgEntity.setRevision(getInitialRevisionDTO());

            createdEntity = pgClient.createProcessGroup(parentPgId, pgEntity, Boolean.parseBoolean(keepExistingPC));

        } else {
            JsonNode rootNode = OBJECT_MAPPER.readTree(input);
            JsonNode flowContentsNode = rootNode.path("flowContents");
            String pgName = flowContentsNode.path("name").asText();
            createdEntity = pgClient.upload(parentPgId, input, pgName, posDto.getX(), posDto.getY());
        }

        return new StringResult(createdEntity.getId(), getContext().isInteractive());
    }

}
