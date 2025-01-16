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
package org.apache.nifi.toolkit.client;

import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.CopyRequestEntity;
import org.apache.nifi.web.api.entity.CopyResponseEntity;
import org.apache.nifi.web.api.entity.CopySnippetRequestEntity;
import org.apache.nifi.web.api.entity.DropRequestEntity;
import org.apache.nifi.web.api.entity.FlowComparisonEntity;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.PasteRequestEntity;
import org.apache.nifi.web.api.entity.PasteResponseEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupImportEntity;
import org.apache.nifi.web.api.entity.ProcessGroupReplaceRequestEntity;

import java.io.File;
import java.io.IOException;

/**
 * Client for ProcessGroupResource.
 */
public interface ProcessGroupClient {

    ProcessGroupEntity createProcessGroup(String parentGroupdId, ProcessGroupEntity entity)
            throws NiFiClientException, IOException;

    ProcessGroupEntity createProcessGroup(String parentGroupdId, ProcessGroupEntity entity, boolean keepExisting)
            throws NiFiClientException, IOException;

    ProcessGroupEntity getProcessGroup(String processGroupId) throws NiFiClientException, IOException;

    ProcessGroupEntity updateProcessGroup(ProcessGroupEntity entity) throws NiFiClientException, IOException;

    ProcessGroupEntity deleteProcessGroup(ProcessGroupEntity entity) throws NiFiClientException, IOException;

    ControllerServiceEntity createControllerService(String processGroupId, ControllerServiceEntity controllerService)
            throws NiFiClientException, IOException;

    ProcessGroupReplaceRequestEntity replaceProcessGroup(String processGroupId, ProcessGroupImportEntity importEntity)
            throws NiFiClientException, IOException;

    ProcessGroupReplaceRequestEntity getProcessGroupReplaceRequest(String processGroupId, String requestId)
            throws NiFiClientException, IOException;

    ProcessGroupReplaceRequestEntity deleteProcessGroupReplaceRequest(String processGroupId, String requestId)
            throws NiFiClientException, IOException;

    FlowEntity copySnippet(String processGroupId, CopySnippetRequestEntity copySnippetRequestEntity)
            throws NiFiClientException, IOException;

    FlowComparisonEntity getLocalModifications(String processGroupId) throws NiFiClientException, IOException;

    File exportProcessGroup(String processGroupId, boolean includeReferencedServices, File outputFile) throws NiFiClientException, IOException;

    DropRequestEntity emptyQueues(String processGroupId) throws NiFiClientException, IOException;

    DropRequestEntity getEmptyQueuesRequest(String processGroupId, String requestId) throws NiFiClientException, IOException;

    CopyResponseEntity copy(String processGroupId, CopyRequestEntity copyRequestEntity) throws NiFiClientException, IOException;

    PasteResponseEntity paste(String processGroupId, PasteRequestEntity pasteRequestEntity) throws NiFiClientException, IOException;

    ProcessGroupEntity upload(String parentPgId, File file, String pgName, Double posX, Double posY) throws NiFiClientException, IOException;
}
