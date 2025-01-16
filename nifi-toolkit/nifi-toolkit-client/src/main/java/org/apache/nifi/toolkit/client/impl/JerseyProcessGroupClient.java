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
package org.apache.nifi.toolkit.client.impl;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ProcessGroupClient;
import org.apache.nifi.toolkit.client.RequestConfig;
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
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/**
 * Jersey implementation of ProcessGroupClient.
 */
public class JerseyProcessGroupClient extends AbstractJerseyClient implements ProcessGroupClient {

    private final WebTarget processGroupsTarget;

    public JerseyProcessGroupClient(final WebTarget baseTarget) {
        this(baseTarget, null);
    }

    public JerseyProcessGroupClient(final WebTarget baseTarget, final RequestConfig requestConfig) {
        super(requestConfig);
        this.processGroupsTarget = baseTarget.path("/process-groups");
    }

    @Override
    public ProcessGroupEntity createProcessGroup(final String parentGroupdId, final ProcessGroupEntity entity)
            throws NiFiClientException, IOException {
        return createProcessGroup(parentGroupdId, entity, true);
    }

    @Override
    public ProcessGroupEntity createProcessGroup(final String parentGroupdId, final ProcessGroupEntity entity, boolean keepExisting)
            throws NiFiClientException, IOException {

        if (StringUtils.isBlank(parentGroupdId)) {
            throw new IllegalArgumentException("Parent process group id cannot be null or blank");
        }

        if (entity == null) {
            throw new IllegalArgumentException("Process group entity cannot be null");
        }

        return executeAction("Error creating process group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/process-groups")
                    .queryParam("parameterContextHandlingStrategy", keepExisting ? "KEEP_EXISTING" : "REPLACE")
                    .resolveTemplate("id", parentGroupdId);

            return getRequestBuilder(target).post(
                    Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                    ProcessGroupEntity.class);
        });
    }

    @Override
    public ProcessGroupEntity getProcessGroup(final String processGroupId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        return executeAction("Error getting process group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).get(ProcessGroupEntity.class);
        });
    }

    @Override
    public ProcessGroupEntity updateProcessGroup(final ProcessGroupEntity entity)
            throws NiFiClientException, IOException {

        if (entity == null) {
            throw new IllegalArgumentException("Process group entity cannot be null");
        }

        return executeAction("Error updating process group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}")
                    .resolveTemplate("id", entity.getId());

            return getRequestBuilder(target).put(
                    Entity.entity(entity, MediaType.APPLICATION_JSON_TYPE),
                    ProcessGroupEntity.class);
        });
    }

    @Override
    public ProcessGroupEntity deleteProcessGroup(final ProcessGroupEntity entity) throws NiFiClientException, IOException {
        if (entity == null) {
            throw new IllegalArgumentException("Process group entity cannot be null");
        }
        if (entity.getRevision() == null || entity.getRevision().getVersion() == null) {
            throw new IllegalArgumentException("Process group revision cannot be null");
        }

        return executeAction("Error deleting process group", () -> {
            WebTarget target = processGroupsTarget
                    .path("{id}")
                    .resolveTemplate("id", entity.getId())
                    .queryParam("version", entity.getRevision().getVersion());

            if (entity.isDisconnectedNodeAcknowledged() == Boolean.TRUE) {
                target = target.queryParam("disconnectedNodeAcknowledged", "true");
            }

            return getRequestBuilder(target).delete(ProcessGroupEntity.class);
        });
    }

    @Override
    public ControllerServiceEntity createControllerService(
            final String processGroupId, final ControllerServiceEntity controllerService) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (controllerService == null) {
            throw new IllegalArgumentException("Controller service entity cannot be null");
        }

        return executeAction("Error creating controller service", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/controller-services")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).post(
                    Entity.entity(controllerService, MediaType.APPLICATION_JSON),
                    ControllerServiceEntity.class);
        });
    }

    @Override
    public ProcessGroupReplaceRequestEntity replaceProcessGroup(final String processGroupId, final ProcessGroupImportEntity importEntity)
            throws NiFiClientException, IOException {

        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (importEntity == null || importEntity.getVersionedFlowSnapshot() == null) {
            throw new IllegalArgumentException("ProcessGroupImportEntity cannot be null and must have a non-null VersionedFlowSnapshot");
        }

        return executeAction("Error creating process group replacement request", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{processGroupId}/replace-requests")
                    .resolveTemplate("processGroupId", processGroupId);

            return getRequestBuilder(target).post(
                    Entity.entity(importEntity, MediaType.APPLICATION_JSON_TYPE),
                    ProcessGroupReplaceRequestEntity.class);
        });
    }

    @Override
    public ProcessGroupReplaceRequestEntity getProcessGroupReplaceRequest(final String processGroupId, final String requestId)
            throws NiFiClientException, IOException {

        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (StringUtils.isBlank(requestId)) {
            throw new IllegalArgumentException("Request id cannot be null or blank");
        }

        return executeAction("Error getting process group replacement request", () -> {
            final WebTarget target = processGroupsTarget
                    .path("replace-requests/{requestId}")
                    .resolveTemplate("requestId", requestId);

            return getRequestBuilder(target).get(ProcessGroupReplaceRequestEntity.class);
        });
    }

    @Override
    public ProcessGroupReplaceRequestEntity deleteProcessGroupReplaceRequest(final String processGroupId, final String requestId)
            throws NiFiClientException, IOException {

        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (StringUtils.isBlank(requestId)) {
            throw new IllegalArgumentException("Request id cannot be null or blank");
        }

        return executeAction("Error deleting process group replacement request", () -> {
            final WebTarget target = processGroupsTarget
                    .path("replace-requests/{requestId}")
                    .resolveTemplate("requestId", requestId);

            return getRequestBuilder(target).delete(ProcessGroupReplaceRequestEntity.class);
        });
    }

    @Override
    public FlowEntity copySnippet(final String processGroupId, final CopySnippetRequestEntity copySnippetRequestEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (copySnippetRequestEntity == null) {
            throw new IllegalArgumentException("Snippet Request Entity cannot be null");
        }

        return executeAction("Error copying snippet to Process Group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/snippet-instance")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).post(
                    Entity.entity(copySnippetRequestEntity, MediaType.APPLICATION_JSON_TYPE),
                    FlowEntity.class);
        });
    }

    @Override
    public FlowComparisonEntity getLocalModifications(final String processGroupId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        return executeAction("Error retrieving list of local flow modifications", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/local-modifications")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).get(FlowComparisonEntity.class);
        });
    }

    @Override
    public File exportProcessGroup(final String processGroupId, final boolean includeReferencedServices, final File outputFile) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        return executeAction("Error getting process group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/download")
                    .resolveTemplate("id", processGroupId)
                    .queryParam("includeReferencedServices", includeReferencedServices);

            final Response response = getRequestBuilder(target)
                    .accept(MediaType.APPLICATION_JSON)
                    .get();
            return getFileContent(response, outputFile);
        });
    }

    @Override
    public DropRequestEntity emptyQueues(String processGroupId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        return executeAction("Error emptying queues in Process Group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/empty-all-connections-requests")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).post(null, DropRequestEntity.class);
        });
    }

    @Override
    public DropRequestEntity getEmptyQueuesRequest(String processGroupId, String requestId)
            throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }
        if (StringUtils.isBlank(requestId)) {
            throw new IllegalArgumentException("Request id cannot be null or blank");
        }

        return executeAction("Error getting Drop Request status for Process Group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/empty-all-connections-requests/{drop-request-id}")
                    .resolveTemplate("id", processGroupId)
                    .resolveTemplate("drop-request-id", requestId);

            return getRequestBuilder(target).get(DropRequestEntity.class);
        });
    }

    private File getFileContent(final Response response, final File outputFile) {
        final String contentDispositionHeader = response.getHeaderString("Content-Disposition");
        if (StringUtils.isBlank(contentDispositionHeader)) {
            throw new IllegalStateException("Content-Disposition header was blank or missing");
        }

        try (final InputStream responseInputStream = response.readEntity(InputStream.class)) {
            Files.copy(responseInputStream, outputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return outputFile;
        } catch (Exception e) {
            throw new IllegalStateException("Unable to write content due to: " + e.getMessage(), e);
        }
    }

    @Override
    public CopyResponseEntity copy(String processGroupId, CopyRequestEntity copyRequestEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (copyRequestEntity == null) {
            throw new IllegalArgumentException("Copy Request Entity cannot be null");
        }

        return executeAction("Error copying", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/copy")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).post(
                    Entity.entity(copyRequestEntity, MediaType.APPLICATION_JSON_TYPE),
                    CopyResponseEntity.class);
        });
    }

    @Override
    public PasteResponseEntity paste(String processGroupId, PasteRequestEntity pasteRequestEntity) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(processGroupId)) {
            throw new IllegalArgumentException("Process group id cannot be null or blank");
        }

        if (pasteRequestEntity == null) {
            throw new IllegalArgumentException("Paste Request Entity cannot be null");
        }

        return executeAction("Error pasting", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/paste")
                    .resolveTemplate("id", processGroupId);

            return getRequestBuilder(target).put(
                    Entity.entity(pasteRequestEntity, MediaType.APPLICATION_JSON_TYPE),
                    PasteResponseEntity.class);
        });
    }

    @Override
    public ProcessGroupEntity upload(String parentPgId, File file, String pgName, Double posX, Double posY) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(parentPgId)) {
            throw new IllegalArgumentException("Parent process group id cannot be null or blank");
        }
        if (file == null) {
            throw new IllegalArgumentException("File cannot be null");
        }
        if (!file.exists() || !file.canRead()) {
            throw new IllegalArgumentException("Specified file is not a local readable file: " + file.getAbsolutePath());
        }

        FormDataMultiPart form = new FormDataMultiPart();

        form.field("id", parentPgId);
        form.field("groupName", pgName);
        form.field("positionX", Double.toString(posX));
        form.field("positionY", Double.toString(posY));
        form.field("clientId", UUID.randomUUID().toString());
        form.bodyPart(new FileDataBodyPart("file", file, MediaType.APPLICATION_JSON_TYPE));

        return executeAction("Error uploading process group", () -> {
            final WebTarget target = processGroupsTarget
                    .path("{id}/process-groups/upload")
                    .resolveTemplate("id", parentPgId);
            return getRequestBuilder(target).post(
                    Entity.entity(form, MediaType.MULTIPART_FORM_DATA),
                    ProcessGroupEntity.class);
        });
    }
}