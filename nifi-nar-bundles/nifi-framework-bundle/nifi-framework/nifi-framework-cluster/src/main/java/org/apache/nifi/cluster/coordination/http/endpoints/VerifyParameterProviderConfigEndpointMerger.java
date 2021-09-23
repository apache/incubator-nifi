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

package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.VerifyParameterProviderConfigRequestDTO;
import org.apache.nifi.web.api.entity.VerifyParameterProviderConfigRequestEntity;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class VerifyParameterProviderConfigEndpointMerger extends AbstractSingleEntityEndpoint<VerifyParameterProviderConfigRequestEntity> {
    public static final Pattern VERIFY_PARAMETER_PROVIDER_CONFIG_URI_PATTERN = Pattern.compile("/nifi-api/parameter-providers/[a-f0-9\\-]{36}/config/verification-requests(/[a-f0-9\\-]{36})?");

    @Override
    protected Class<VerifyParameterProviderConfigRequestEntity> getEntityClass() {
        return VerifyParameterProviderConfigRequestEntity.class;
    }

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return VERIFY_PARAMETER_PROVIDER_CONFIG_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected void mergeResponses(final VerifyParameterProviderConfigRequestEntity clientEntity, final Map<NodeIdentifier, VerifyParameterProviderConfigRequestEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        final VerifyParameterProviderConfigRequestDTO requestDto = clientEntity.getRequest();
        final List<ConfigVerificationResultDTO> results = requestDto.getResults();

        // If the result hasn't been set, the task is not yet complete, so we don't have to bother merging the results.
        if (results == null) {
            return;
        }

        // Aggregate the Config Verification Results across all nodes into a single List
        final ConfigVerificationResultMerger resultMerger = new ConfigVerificationResultMerger();
        for (final Map.Entry<NodeIdentifier, VerifyParameterProviderConfigRequestEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final VerifyParameterProviderConfigRequestEntity entity = entry.getValue();

            final List<ConfigVerificationResultDTO> nodeResults = entity.getRequest().getResults();
            resultMerger.addNodeResults(nodeId, nodeResults);
        }

        final List<ConfigVerificationResultDTO> aggregateResults = resultMerger.computeAggregateResults();
        clientEntity.getRequest().setResults(aggregateResults);
    }

}
