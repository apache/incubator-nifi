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
package org.apache.nifi.web;

import io.prometheus.client.CollectorRegistry;
import org.apache.nifi.authorization.AuthorizeAccess;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.c2.protocol.component.api.ControllerServiceDefinition;
import org.apache.nifi.c2.protocol.component.api.FlowAnalysisRuleDefinition;
import org.apache.nifi.c2.protocol.component.api.ParameterProviderDefinition;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.ReportingTaskDefinition;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.diagnostics.DiagnosticLevel;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.VersionedComponentAdditions;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterGroupConfiguration;
import org.apache.nifi.registry.flow.FlowLocation;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.FlowVersionLocation;
import org.apache.nifi.registry.flow.RegisterAction;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ComponentHistoryDTO;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.dto.FlowFileDTO;
import org.apache.nifi.web.api.dto.FlowRegistryClientDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterProviderDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.ResourceDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceOptionsDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.ActionEntity;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ComponentValidationResultEntity;
import org.apache.nifi.web.api.entity.ConfigurationAnalysisEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionStatisticsEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.ControllerBulletinsEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.CopyRequestEntity;
import org.apache.nifi.web.api.entity.CopyResponseEntity;
import org.apache.nifi.web.api.entity.CurrentUserEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisResultEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;
import org.apache.nifi.web.api.entity.FlowBreadcrumbEntity;
import org.apache.nifi.web.api.entity.FlowComparisonEntity;
import org.apache.nifi.web.api.entity.FlowConfigurationEntity;
import org.apache.nifi.web.api.entity.FlowEntity;
import org.apache.nifi.web.api.entity.FlowRegistryBranchEntity;
import org.apache.nifi.web.api.entity.FlowRegistryBucketEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.LatestProvenanceEventsEntity;
import org.apache.nifi.web.api.entity.NarDetailsEntity;
import org.apache.nifi.web.api.entity.NarSummaryEntity;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ParameterProviderReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.PasteResponseEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.PortStatusEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupRecursivity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorDiagnosticsEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorStatusEntity;
import org.apache.nifi.web.api.entity.ProcessorsRunStatusDetailsEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupPortEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;
import org.apache.nifi.web.api.entity.SnippetEntity;
import org.apache.nifi.web.api.entity.StartVersionControlRequestEntity;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;
import org.apache.nifi.web.api.entity.TenantsEntity;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.api.entity.VersionControlComponentMappingEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataEntity;
import org.apache.nifi.web.api.entity.VersionedReportingTaskImportResponseEntity;
import org.apache.nifi.web.api.request.FlowMetricsRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Defines the NiFiServiceFacade interface.
 */
public interface NiFiServiceFacade {

    // ----------------------------------------
    // Synchronization methods
    // ----------------------------------------

    /**
     * Authorizes access to the service facade.
     *
     * @param authorizeAccess authorize access callback
     */
    void authorizeAccess(AuthorizeAccess authorizeAccess);

    /**
     * Claims the specified revision for the specified user.
     *
     * @param revision revision
     * @param user user
     * @throws InvalidRevisionException invalid revision
     */
    void verifyRevision(Revision revision, NiFiUser user) throws InvalidRevisionException;

    /**
     * Claims the specified revisions for the specified user.
     *
     * @param revisions revisions
     * @param user user
     * @throws InvalidRevisionException invalid revision
     */
    void verifyRevisions(Set<Revision> revisions, NiFiUser user) throws InvalidRevisionException;

    /**
     * Gets the current revisions for the components based on the specified function.
     *
     * @param groupId group
     * @param getComponents callback
     * @return component revisions
     */
    Set<Revision> getRevisionsFromGroup(String groupId, Function<ProcessGroup, Set<String>> getComponents);

    /**
     * Gets the revisions from the specified snippet.
     *
     * @param snippetId snippet
     * @return component revisions from the snippet
     */
    Set<Revision> getRevisionsFromSnippet(String snippetId);



    // ----------------------------------------
    // Controller methods
    // ----------------------------------------
    ControllerDTO getSiteToSiteDetails();

    /**
     * Searches the controller for the specified query string.
     *
     * @param query query
     * @param activeGroupId the id of the group currently selected in the editor
     * @return results
     */
    SearchResultsDTO searchController(String query, String activeGroupId);

    /**
     * Submits a provenance request.
     *
     * @param query query
     * @return results
     */
    ProvenanceDTO submitProvenance(ProvenanceDTO query);

    /**
     * Submits a new replay request.
     *
     * @param eventId id
     * @return event
     */
    ProvenanceEventDTO submitReplay(Long eventId);

    /**
     * Submits a replay request for the latest event generated by the component with the given ID
     * @param componentId the ID the component
     * @return the event, or <code>null</code> if no event was available
     */
    ProvenanceEventDTO submitReplayLastEvent(String componentId);

    /**
     * Gets the content for the specified claim.
     *
     * @param eventId id
     * @param uri uri
     * @param contentDirection direction
     * @return content
     */
    DownloadableContent getContent(Long eventId, String uri, ContentDirection contentDirection);

    /**
     * Gets the content for the specified flowfile in the specified connection.
     *
     * @param connectionId connection
     * @param flowfileUuid flowfile
     * @param uri uri
     * @return content
     */
    DownloadableContent getContent(String connectionId, String flowfileUuid, String uri);

    /**
     * Retrieves provenance.
     *
     * @param queryId identifier
     * @param summarize whether to summarize the event dtos
     * @param incrementalResults whether to return any events if the search has not finished
     * @return result
     */
    ProvenanceDTO getProvenance(String queryId, Boolean summarize, Boolean incrementalResults);

    /**
     * Deletes provenance.
     *
     * @param queryId identifier
     */
    void deleteProvenance(String queryId);

    /**
     * Returns the available options for searching provenance.
     *
     * @return options
     */
    ProvenanceOptionsDTO getProvenanceSearchOptions();

    /**
     * Submits a lineage request.
     *
     * @param lineage lineage
     * @return lineage
     */
    LineageDTO submitLineage(LineageDTO lineage);

    /**
     * Gets the lineage with the specified id.
     *
     * @param lineageId id
     * @return lineage
     */
    LineageDTO getLineage(String lineageId);

    /**
     * Deletes the lineage with the specified id.
     *
     * @param lineageId lineage
     */
    void deleteLineage(String lineageId);

    /**
     * Gets the provenance event with the specified id.
     *
     * @param id id
     * @return event
     */
    ProvenanceEventDTO getProvenanceEvent(Long id);

    /**
     * Gets the latest provenance events for the specified component.
     * @param componentId the ID of the components to retrieve the latest events for
     * @param eventLimit the maximum number of events to return
     * @return the latest provenance events
     */
    LatestProvenanceEventsEntity getLatestProvenanceEvents(String componentId, int eventLimit);

    /**
     * Gets the configuration for this controller.
     *
     * @return Controller configuration transfer object
     */
    ControllerConfigurationEntity getControllerConfiguration();

    /**
     * Gets the snapshot for the given reporting task.
     *
     * @param reportingTaskId the id of the reporting task to get the snapshot for
     * @return the reporting task snapshot
     */
    VersionedReportingTaskSnapshot getVersionedReportingTaskSnapshot(String reportingTaskId);

    /**
     * Gets the snapshot of all reporting tasks.
     *
     * @return the reporting task snapshot
     */
    VersionedReportingTaskSnapshot getVersionedReportingTaskSnapshot();

    /**
     * Generates unique identifiers for all components in the snapshot so that multiple imports of the same snapshot
     * create multiple components.
     *
     * @param reportingTaskSnapshot the snapshot
     */
    void generateIdentifiersForImport(VersionedReportingTaskSnapshot reportingTaskSnapshot, Supplier<String> idGenerator);

    /**
     * Imports the reporting tasks and controller services in the given snapshot.
     *
     * @param reportingTaskSnapshot the snapshot to import
     * @return the response entity
     */
    VersionedReportingTaskImportResponseEntity importReportingTasks(VersionedReportingTaskSnapshot reportingTaskSnapshot);

    /**
     * Gets the controller level bulletins.
     *
     * @return Controller level bulletins
     */
    ControllerBulletinsEntity getControllerBulletins();

    /**
     * Gets the configuration for the flow.
     *
     * @return Flow configuration transfer object
     */
    FlowConfigurationEntity getFlowConfiguration();

    /**
     * Gets the metrics for the flow.
     */
    Collection<CollectorRegistry> generateFlowMetrics();

    /**
     * Generate metrics for the flow and return selected registries
     *
     * @param includeRegistries Set of Flow Metrics Registries to be returned
     * @return Collector Registries
     */
    Collection<CollectorRegistry> generateFlowMetrics(Set<FlowMetricsRegistry> includeRegistries);

    /**
     * Updates the configuration for this controller.
     *
     * @param revision Revision to compare with current base revision
     * @param controllerConfigurationDTO Controller configuration DTO
     * @return Controller configuration DTO
     */
    ControllerConfigurationEntity updateControllerConfiguration(Revision revision, ControllerConfigurationDTO controllerConfigurationDTO);

    /**
     * Returns the process group status.
     *
     * @param groupId group
     * @return The process group status
     */
    ProcessGroupStatusEntity getProcessGroupStatus(String groupId, boolean recursive);

    /**
     * Gets the process group status history.
     *
     * @param groupId id
     * @return history
     */
    StatusHistoryEntity getProcessGroupStatusHistory(String groupId);

    /**
     * Returns the controller status.
     *
     * @return status
     */
    ControllerStatusDTO getControllerStatus();

    /**
     * Updates the specified counter by setting its value to 0.
     *
     * @param counterId id
     * @return counter
     */
    CounterDTO updateCounter(String counterId);

    /**
     * Returns the counters.
     *
     * @return The counters
     */
    CountersDTO getCounters();

    /**
     * Returns the list of processor types.
     *
     * @param bundleGroupFilter if specified, must be member of bundle group
     * @param bundleArtifactFilter if specified, must be member of bundle artifact
     * @param typeFilter if specified, type must match
     * @return The list of available processor types matching specified criteria
     */
    Set<DocumentedTypeDTO> getProcessorTypes(final String bundleGroupFilter, final String bundleArtifactFilter, final String typeFilter);

    /**
     * Returns the list of controller service types.
     *
     * @param serviceType Filters only service types that implement this type
     * @param serviceBundleGroup if serviceType specified, the bundle group of the serviceType
     * @param serviceBundleArtifact if serviceType specified, the bundle artifact of the serviceType
     * @param serviceBundleVersion if serviceType specified, the bundle version of the serviceType
     * @param bundleGroupFilter if specified, must be member of bundle group
     * @param bundleArtifactFilter if specified, must be member of bundle artifact
     * @param typeFilter if specified, type must match
     * @return The list of available controller types matching specified criteria
     */
    Set<DocumentedTypeDTO> getControllerServiceTypes(final String serviceType, final String serviceBundleGroup, final String serviceBundleArtifact, final String serviceBundleVersion,
                                                     final String bundleGroupFilter, final String bundleArtifactFilter, final String typeFilter);

    /**
     * Returns the list of reporting task types.
     *
     * @param bundleGroupFilter if specified, must be member of bundle group
     * @param bundleArtifactFilter if specified, must be member of bundle artifact
     * @param typeFilter if specified, type must match
     * @return The list of available reporting task types matching specified criteria
     */
    Set<DocumentedTypeDTO> getReportingTaskTypes(final String bundleGroupFilter, final String bundleArtifactFilter, final String typeFilter);

    /**
     * Returns the list of flow registry client types.
     *
     * @return The list of available flow registry client types matching specified criteria
     */
    Set<DocumentedTypeDTO> getFlowRegistryTypes();

    /**
     * Returns the RuntimeManifest for this NiFi instance.
     *
     * @return the runtime manifest
     */
    RuntimeManifest getRuntimeManifest();

    /**
     * Return the ProcessorDefinition the specified Processor.
     *
     * @param group The bundle group
     * @param artifact The bundle artifact
     * @param version The bundle version
     * @param type The Processor type
     * @return The ProcessorDefinition
     */
    ProcessorDefinition getProcessorDefinition(String group, String artifact, String version, String type);

    /**
     * Return the ControllerServiceDefinition the specified Controller Service.
     *
     * @param group The bundle group
     * @param artifact The bundle artifact
     * @param version The bundle version
     * @param type The Controller Service type
     * @return The ControllerServiceDefinition
     */
    ControllerServiceDefinition getControllerServiceDefinition(String group, String artifact, String version, String type);

    /**
     * Return the ReportingTaskDefinition the specified Reporting Task.
     *
     * @param group The bundle group
     * @param artifact The bundle artifact
     * @param version The bundle version
     * @param type The Reporting Task type
     * @return The ReportingTaskDefinition
     */
    ReportingTaskDefinition getReportingTaskDefinition(String group, String artifact, String version, String type);

    /**
     * Return the ParameterProviderDefinition the specified Parameter Provider.
     *
     * @param group The bundle group
     * @param artifact The bundle artifact
     * @param version The bundle version
     * @param type The Parameter Provider type
     * @return The ParameterProviderDefinition
     */
    ParameterProviderDefinition getParameterProviderDefinition(String group, String artifact, String version, String type);

    /**
     * Return the FlowAnalysisRuleDefinition the specified Flow Analysis Rule.
     *
     * @param group The bundle group
     * @param artifact The bundle artifact
     * @param version The bundle version
     * @param type The Flow Analysis Rule type
     * @return The FlowAnalysisRuleDefinition
     */
    FlowAnalysisRuleDefinition getFlowAnalysisRuleDefinition(String group, String artifact, String version, String type);

    /**
     * Return the additionalDetails for the specified component.
     *
     * @param group The bundle group
     * @param artifact The bundle artifact
     * @param version The bundle version
     * @param type The component type
     * @return The additional details
     */
    String getAdditionalDetails(String group, String artifact, String version, String type);

    /**
     * Returns the list of parameter provider types.
     *
     * @param bundleGroupFilter if specified, must be member of bundle group
     * @param bundleArtifactFilter if specified, must be member of bundle artifact
     * @param typeFilter if specified, type must match
     * @return The list of available parameter provider types matching specified criteria
     */
    Set<DocumentedTypeDTO> getParameterProviderTypes(final String bundleGroupFilter, final String bundleArtifactFilter, final String typeFilter);

    /**
     * Returns the list of prioritizer types.
     *
     * @return The list of available prioritizer types
     */
    Set<DocumentedTypeDTO> getWorkQueuePrioritizerTypes();

    /**
     * Verifies the types of components in a versioned process group
     *
     * @param versionedGroup the proposed process group
     */
    void verifyComponentTypes(VersionedProcessGroup versionedGroup);

    /**
     * Verifies that the flow identified by the given Version Control Information can be imported into the Process Group
     * with the given id
     *
     * @param versionControlInfo the information about the versioned flow
     * @param versionedProcessGroup the contents to be imported
     * @param groupId the ID of the Process Group where the flow should be instantiated
     *
     * @throws IllegalStateException if the flow cannot be imported into the specified group
     */
    void verifyImportProcessGroup(VersionControlInformationDTO versionControlInfo, final VersionedProcessGroup versionedProcessGroup, String groupId);

    // ----------------------------------------
    // Processor methods
    // ----------------------------------------

    /**
     * Verifies the specified processor can be created.
     *
     * @param processorDTO processor
     */
    void verifyCreateProcessor(ProcessorDTO processorDTO);

    /**
     * Creates a new Processor.
     *
     * @param revision revision
     * @param groupId Group id
     * @param processorDTO The processor DTO
     * @return The new processor DTO
     */
    ProcessorEntity createProcessor(Revision revision, String groupId, ProcessorDTO processorDTO);

    /**
     * Gets the Processor transfer object for the specified id.
     *
     * @param id Id of the processor to return
     * @return The Processor transfer object
     */
    ProcessorEntity getProcessor(String id);

    /**
     * Terminates the Processor with the specified id
     *
     * @param id Id of the processor to terminate
     * @return The Processor transfer object
     */
    ProcessorEntity terminateProcessor(String id);

    /**
     * Verifies that the processor with the given id can be terminated at this time
     *
     * @param processorId the id of the processor
     */
    void verifyTerminateProcessor(String processorId);

    /**
     * Gets the Diagnostic information for the Processor with the specified id
     *
     * @param id the id of the processor
     * @return the diagnostics information for the processor
     */
    ProcessorDiagnosticsEntity getProcessorDiagnostics(String id);

    /**
     * Gets the processor status.
     *
     * @param id id
     * @return status
     */
    ProcessorStatusEntity getProcessorStatus(String id);

    /**
     * Gets the processor status history.
     *
     * @param id id
     * @return history
     */
    StatusHistoryEntity getProcessorStatusHistory(String id);

    // ----------------------------------------
    // System diagnostics history
    // ----------------------------------------

    /**
     * @return the system diagnostics history
     */
    StatusHistoryEntity getNodeStatusHistory();

    /**
     * Get the descriptor for the specified property of the specified processor.
     *
     * @param id id
     * @param property property
     * @param sensitive requested sensitive status
     * @return descriptor
     */
    PropertyDescriptorDTO getProcessorPropertyDescriptor(String id, String property, boolean sensitive);

    /**
     * Gets all the Processor transfer objects for this controller.
     *
     * @param groupId group
     * @param includeDescendants if processors from descendent groups should be included
     * @return List of all the Processor transfer object
     */
    Set<ProcessorEntity> getProcessors(String groupId, boolean includeDescendants);

    /**
     * Provides a ProcessorsRunStatusDetails that describes the current details of the run status for each processor whose id is provided
     * @param processorIds the set of all processor IDs that should be included
     * @return a ProcessorsRunStatusDetailsEntity that describes the current information about the processors' run status
     */
    ProcessorsRunStatusDetailsEntity getProcessorsRunStatusDetails(Set<String> processorIds, NiFiUser user);

    /**
     * Verifies the specified processor can be updated.
     *
     * @param processorDTO processor
     */
    void verifyUpdateProcessor(ProcessorDTO processorDTO);

    /**
     * Updates the specified Processor.
     *
     * @param revision Revision to compare with current base revision
     * @param processorDTO The processorDTO
     * @return The updated processor
     */
    ProcessorEntity updateProcessor(Revision revision, ProcessorDTO processorDTO);

    /**
     * Performs verification of the given Processor Configuration for the Processor with the given ID
     * @param processorId the id of the processor
     * @param properties the configured properties to verify
     * @param attributes a map of values that can be used for resolving FlowFile attributes for Expression Language
     * @return verification results
     */
    List<ConfigVerificationResultDTO> performProcessorConfigVerification(String processorId, Map<String, String> properties, Map<String, String> attributes);

    /**
     * Performs analysis of the given properties, determining which attributes are referenced by properties
     * @param processorId the ID of the processor
     * @param properties the properties
     * @return analysis results
     */
    ConfigurationAnalysisEntity analyzeProcessorConfiguration(String processorId, Map<String, String> properties);

    /**
     * Verifies the specified processor can be removed.
     *
     * @param processorId processor
     */
    void verifyDeleteProcessor(String processorId);

    /**
     * Deletes the specified processor.
     *
     * @param revision Revision to compare with current base revision
     * @param processorId The processor id to delete
     * @return snapshot
     */
    ProcessorEntity deleteProcessor(Revision revision, String processorId);

    // ----------------------------------------
    // Connections methods
    // ----------------------------------------

    /**
     * Gets the Connection transfer objects for the specified source processor.
     *
     * @param groupId group
     * @return The Connection transfer objects
     */
    Set<ConnectionEntity> getConnections(String groupId);

    /**
     * Gets the specified Connection transfer object.
     *
     * @param connectionId The ID of the connection
     * @return The Connection transfer object
     */
    ConnectionEntity getConnection(String connectionId);

    /**
     * Gets the status of the specified connection.
     *
     * @param connectionId connection
     * @return status
     */
    ConnectionStatusEntity getConnectionStatus(String connectionId);

    /**
     * Gets the status history of the specified connection.
     *
     * @param connectionId connection
     * @return history
     */
    StatusHistoryEntity getConnectionStatusHistory(String connectionId);

    /**
     * Gets analytical statistics for the specified connection.
     *
     * @param connectionId connection
     * @return statistics
     */
    ConnectionStatisticsEntity getConnectionStatistics(String connectionId);

    /**
     * Creates a new Relationship target.
     *
     * @param revision revision
     * @param groupId group
     * @param connectionDTO The Connection DTO
     * @return The Connection DTO
     */
    ConnectionEntity createConnection(Revision revision, String groupId, ConnectionDTO connectionDTO);

    /**
     * Determines if this connection can be listed.
     *
     * @param connectionId connection
     */
    void verifyListQueue(String connectionId);

    /**
     * Determines if this connection can be created.
     *
     * @param groupId group
     * @param connectionDTO connection
     */
    void verifyCreateConnection(String groupId, ConnectionDTO connectionDTO);

    /**
     * Determines if this connection can be updated.
     *
     * @param connectionDTO connection
     */
    void verifyUpdateConnection(ConnectionDTO connectionDTO);

    /**
     * Updates the specified Relationship target.
     *
     * @param revision Revision to compare with current base revision
     * @param connectionDTO The Connection DTO
     * @return The Connection DTO
     */
    ConnectionEntity updateConnection(Revision revision, ConnectionDTO connectionDTO);

    /**
     * Determines if this connection can be removed.
     *
     * @param connectionId connection
     */
    void verifyDeleteConnection(String connectionId);

    /**
     * Deletes the specified relationship target.
     *
     * @param revision Revision to compare with current base revision
     * @param connectionId The ID of the connection
     * @return snapshot
     */
    // TODO: Remove the id's from all of the delete methods. The Revision now contains
    // the component ID. We can't do this yet though because we have to wait for the
    // Revisions to be properly formed on the client/UI side
    ConnectionEntity deleteConnection(Revision revision, String connectionId);

    /**
     * Creates a new flow file drop request.
     *
     * @param connectionId The ID of the connection
     * @param dropRequestId The ID of the drop request
     * @return The DropRequest
     */
    DropRequestDTO createFlowFileDropRequest(String connectionId, String dropRequestId);

    /**
     * Gets the specified flow file drop request.
     *
     * @param connectionId The ID of the connection
     * @param dropRequestId The flow file drop request
     * @return The DropRequest
     */
    DropRequestDTO getFlowFileDropRequest(String connectionId, String dropRequestId);

    /**
     * Cancels/removes the specified flow file drop request.
     *
     * @param connectionId The ID of the connection
     * @param dropRequestId The flow file drop request
     * @return The DropRequest
     */
    DropRequestDTO deleteFlowFileDropRequest(String connectionId, String dropRequestId);

    /**
     * Creates a new flow file listing request.
     *
     * @param connectionId The ID of the connection
     * @param listingRequestId The ID of the listing request
     * @return The ListingRequest
     */
    ListingRequestDTO createFlowFileListingRequest(String connectionId, String listingRequestId);

    /**
     * Gets a new flow file listing request.
     *
     * @param connectionId The ID of the connection
     * @param listingRequestId The ID of the listing request
     * @return The ListingRequest
     */
    ListingRequestDTO getFlowFileListingRequest(String connectionId, String listingRequestId);

    /**
     * Deletes a new flow file listing request.
     *
     * @param connectionId The ID of the connection
     * @param listingRequestId The ID of the listing request
     * @return The ListingRequest
     */
    ListingRequestDTO deleteFlowFileListingRequest(String connectionId, String listingRequestId);

    /**
     * Gets the specified flowfile from the specified connection.
     *
     * @param connectionId The ID of the connection
     * @param flowFileUuid The UUID of the flowfile
     * @return The FlowFileDTO
     */
    FlowFileDTO getFlowFile(String connectionId, String flowFileUuid);

    // ----------------------------------------
    // InputPort methods
    // ----------------------------------------
    /**
     * Creates a new input port.
     *
     * @param revision revision
     * @param groupId The id of the group this port should be create in
     * @param inputPortDTO The input PortDTO
     * @return snapshot
     */
    PortEntity createInputPort(Revision revision, String groupId, PortDTO inputPortDTO);

    /**
     * Gets an input port.
     *
     * @param inputPortId The input port id
     * @return port
     */
    PortEntity getInputPort(String inputPortId);

    /**
     * Gets all input ports in a given group.
     *
     * @param groupId The id of the group
     * @return port
     */
    Set<PortEntity> getInputPorts(String groupId);

    /**
     * Gets the input port status.
     *
     * @param inputPortId input port
     * @return status
     */
    PortStatusEntity getInputPortStatus(String inputPortId);

    /**
     * Determines if the input port could be updated.
     *
     * @param inputPortDTO The id of the input port
     */
    void verifyUpdateInputPort(PortDTO inputPortDTO);

    /**
     * Updates the specified input port.
     *
     * @param revision Revision to compare with current base revision
     * @param inputPortDTO The input PortDTO
     * @return snapshot
     */
    PortEntity updateInputPort(Revision revision, PortDTO inputPortDTO);

    /**
     * Determines if the input port could be deleted.
     *
     * @param inputPortId The id of the input port
     */
    void verifyDeleteInputPort(String inputPortId);

    /**
     * Deletes the specified input port.
     *
     * @param revision Revision to compare with current base revision
     * @param inputPortId The id of the input port
     * @return snapshot
     */
    PortEntity deleteInputPort(Revision revision, String inputPortId);

    // ----------------------------------------
    // OutputPort methods
    // ----------------------------------------
    /**
     * Creates a new output port.
     *
     * @param revision revision
     * @param groupId The id of the group this port should be create in
     * @param outputPortDTO The output PortDTO
     * @return snapshot
     */
    PortEntity createOutputPort(Revision revision, String groupId, PortDTO outputPortDTO);

    /**
     * Gets an output port.
     *
     * @param outputPortId The output port id
     * @return port
     */
    PortEntity getOutputPort(String outputPortId);

    /**
     * Gets all output ports in a given group.
     *
     * @param groupId The id of the group
     * @return ports
     */
    Set<PortEntity> getOutputPorts(String groupId);

    /**
     * Gets the output port status.
     *
     * @param outputPortId output port
     * @return status
     */
    PortStatusEntity getOutputPortStatus(String outputPortId);

    /**
     * Determines if the output port could be updated.
     *
     * @param outputPortDTO The id of the output port
     */
    void verifyUpdateOutputPort(PortDTO outputPortDTO);

    /**
     * Updates the specified output port.
     *
     * @param revision Revision to compare with current base revision
     * @param outputPortDTO The output PortDTO
     * @return snapshot
     */
    PortEntity updateOutputPort(Revision revision, PortDTO outputPortDTO);

    /**
     * Determines if the output port could be deleted.
     *
     * @param outputPortId The id of the output port
     */
    void verifyDeleteOutputPort(String outputPortId);

    /**
     * Determines if the output port could be deleted.
     *
     * @param revision revision
     * @param outputPortId The id of the output port
     * @return snapshot
     */
    PortEntity deleteOutputPort(Revision revision, String outputPortId);

    /**
     * Verifies public input port unique constraint throughout the flow will be retained,
     * even if a new port is added with the given port id and name.
     *
     * @param portId port id
     * @param portName port name
     *
     * @throws IllegalStateException If there is any port with the same name or the same identifier
     */
    void verifyPublicInputPortUniqueness(final String portId, final String portName);

    /**
     * Verifies public output port unique constraint throughout the flow will be retained,
     * even if a new port is added with the given port id and name.
     *
     * @param portId port id
     * @param portName port name
     *
     * @throws IllegalStateException If there is any port with the same name or the same identifier
     */
    void verifyPublicOutputPortUniqueness(final String portId, final String portName);

    // ------------
    // Current user
    // ------------

    CurrentUserEntity getCurrentUser();

    // ----------------------------------------
    // Flow methods
    // ----------------------------------------

    /**
     * Returns the flow.
     *
     * @param groupId group
     * @param uiOnly whether or not the entity should be populated only with UI-related fields
     * @return the flow
     */
    ProcessGroupFlowEntity getProcessGroupFlow(String groupId, boolean uiOnly);

    /**
     * Returns the breadcrumbs for the specified group.
     *
     * @param groupId group id
     * @return the breadcrumbs
     */
    FlowBreadcrumbEntity getProcessGroupBreadcrumbs(String groupId);

    // ----------------------------------------
    // ProcessGroup methods
    // ----------------------------------------
    /**
     * Creates a new process group.
     *
     * @param revision revision
     * @param parentGroupId The id of the parent group
     * @param processGroupDTO The ProcessGroupDTO
     * @return snapshot
     */
    ProcessGroupEntity createProcessGroup(Revision revision, String parentGroupId, ProcessGroupDTO processGroupDTO);

    /**
     * Returns the process group.
     *
     * @param groupId group
     * @return ProcessGroup transfer object
     */
    ProcessGroupEntity getProcessGroup(String groupId);

    /**
     * Verifies that a Parameter Context matching the given DTO can be created
     * @param parameterContext the DTO that represents the Parameter Context
     * @throws IllegalStateException if a ParameterContext cannot be created for the given DTO
     */
    void verifyCreateParameterContext(ParameterContextDTO parameterContext);

    /**
     * Verifies that the Parameter Context with the ID identified by the given DTO can be updated to match
     * the given set of Parameters
     *
     * @param parameterContext the DTO that represents the updated Parameter Context
     * @param verifyComponentStates if <code>true</code>, will ensure that any processor referencing the parameter context is stopped/disabled and any controller service referencing the parameter
     * context is disabled. If <code>false</code>, these verifications will not be performed.
     */
    void verifyUpdateParameterContext(ParameterContextDTO parameterContext, boolean verifyComponentStates);

    /**
     * Returns the Set of all Parameter Context Entities for the current user
     * @return the Set of all Parameter Context Entities for the current user
     */
    Set<ParameterContextEntity> getParameterContexts();

    /**
     * Returns the Parameter Context with the given name
     * @param parameterContextName the name of the Parameter Context
     * @return the Parameter Context with the given name, or <code>null</code> if no Parameter Context exists with that name
     *
     * @throws org.apache.nifi.authorization.AccessDeniedException if a Parameter Context exists with the given name but the user does not have READ permissions to it
     */
    ParameterContext getParameterContextByName(String parameterContextName, NiFiUser user);

    /**
     * Returns the ParameterContextEntity for the ParameterContext with the given ID
     * @param parameterContextId the ID of the Parameter Context
     * @param includeInheritedParameters Whether to include inherited parameters (and thus overridden values)
     * @param user the user on whose behalf the Parameter Context is being retrieved
     * @return the ParameterContextEntity
     */
    ParameterContextEntity getParameterContext(String parameterContextId, boolean includeInheritedParameters, NiFiUser user);

    /**
     * Creates a new Parameter Context
     * @param revision the revision for the newly created Parameter Context
     * @param parameterContext the Parameter Context
     * @return a ParameterContextEntity representing the newly created ParameterContext
     */
    ParameterContextEntity createParameterContext(Revision revision, ParameterContextDTO parameterContext);

    /**
     * Updates the Parameter Context
     * @param revision the current revision of the Parameter Context
     * @param parameterContext the updated version of the ParameterContext
     * @return the updated Parameter Context Entity
     */
    ParameterContextEntity updateParameterContext(Revision revision, ParameterContextDTO parameterContext);

    /**
     * Deletes the Parameter Context
     * @param revision the revision of the Parameter Context
     * @param parameterContextId the ID of the Parameter Context
     * @return a Parameter Context Entity that represents the Parameter Context that was deleted
     */
    ParameterContextEntity deleteParameterContext(Revision revision, String parameterContextId);

    /**
     * Performs validation of all components that make use of the Parameter Context with the same ID as the given DTO, but validating against the Parameters
     * specified within the DTO
     * @param parameterContext the ParameterContext to validate against
     * @param user the user on whose behalf the validation is taking place
     * @return the ComponentValidationResultEntity for each component that makes use of the Parameter Context
     */
    List<ComponentValidationResultEntity> validateComponents(ParameterContextDTO parameterContext, NiFiUser user);

    /**
     * Ensures that the Parameter Context with the given ID can be deleted
     * @param parameterContextId the ID of the Parameter Context
     */
    void verifyDeleteParameterContext(String parameterContextId);

    /**
     * Gets all child/descendant process groups in the specified parent group.
     *
     * @param parentGroupId The id of the parent group
     * @param processGroupRecursivity how far to recurse into child/descendant groups
     * @return Set of all child or descendant process groups
     */
    Set<ProcessGroupEntity> getProcessGroups(String parentGroupId, ProcessGroupRecursivity processGroupRecursivity);

    /**
     * Verifies the contents of the specified process group can be scheduled or unscheduled.
     *
     * @param processGroupId The ProcessGroup id
     * @param componentIds the components
     * @param state scheduled state
     */
    void verifyScheduleComponents(String processGroupId, ScheduledState state, Set<String> componentIds);

    /**
     * Verifies the contents of the specified process group can be enabled or disabled.
     *
     * @param processGroupId the ProcessGroup id
     * @param state scheduled state
     * @param componentIds the components
     */
    void verifyEnableComponents(String processGroupId, ScheduledState state, Set<String> componentIds);

    /**
     * Verifies the controller services with the given ID's can be enabled or disabled
     *
     * @param processGroupId the ID of the process group
     * @param state the state
     * @param serviceIds the id's of the services
     */
    void verifyActivateControllerServices(String processGroupId, ControllerServiceState state, Collection<String> serviceIds);

    /**
     * Enables or disables the controller services with the given IDs & Revisions on behalf of the currently logged in user
     *
     * @param processGroupId the ID of the process group
     * @param state the desired state of the services
     * @param serviceRevisions a mapping of Controller Service ID to current Revision
     * @return snapshot
     */
    ActivateControllerServicesEntity activateControllerServices(String processGroupId, ControllerServiceState state, Map<String, Revision> serviceRevisions);

    ScheduleComponentsEntity enableComponents(String processGroupId, ScheduledState state, Map<String, Revision> componentRevisions);

    /**
     * Schedules all applicable components under the specified ProcessGroup on behalf of the currently logged in user.
     *
     * @param processGroupId The ProcessGroup id
     * @param state schedule state
     * @param componentRevisions components and their revision
     * @return snapshot
     */
    ScheduleComponentsEntity scheduleComponents(String processGroupId, ScheduledState state, Map<String, Revision> componentRevisions);

    /**
     * Updates the specified process group.
     *
     * @param revision Revision to compare with current base revision
     * @param processGroupDTO The ProcessGroupDTO
     * @return snapshot
     */
    ProcessGroupEntity updateProcessGroup(Revision revision, ProcessGroupDTO processGroupDTO);

    /**
     * Verifies that the Process Group identified by the given DTO can be updated in the manner appropriate according
     * to the DTO
     *
     * @param processGroupDTO the DTO that indicates the updates to occur
     */
    void verifyUpdateProcessGroup(ProcessGroupDTO processGroupDTO);

    /**
     * Verifies the specified process group can be removed.
     *
     * @param groupId The id of the process group
     */
    void verifyDeleteProcessGroup(String groupId);

    /**
     * Creates a request to drop flowfiles in all connections in a process group (recursively).
     *
     * @param processGroupId The ID of the process group
     * @param dropRequestId The ID of the drop request
     * @return The DropRequest
     */
    DropRequestDTO createDropAllFlowFilesInProcessGroup(final String processGroupId, final String dropRequestId);

    /**
     * Gets the specified request for dropping all flowfiles in a process group (recursively).
     *
     * @param processGroupId The ID of the process group
     * @param dropRequestId The ID of the drop request
     * @return The DropRequest
     */
    DropRequestDTO getDropAllFlowFilesRequest(final String processGroupId, final String dropRequestId);

    /**
     * Cancels/removes the specified request for dropping all flowfiles in a process group (recursively).
     *
     * @param processGroupId The ID of the process group
     * @param dropRequestId The ID of the drop request
     * @return The DropRequest
     */
    DropRequestDTO deleteDropAllFlowFilesRequest(String processGroupId, String dropRequestId);

    /**
     * Deletes the specified process group.
     *
     * @param revision Revision to compare with current base revision
     * @param groupId The id of the process group
     * @return snapshot
     */
    ProcessGroupEntity deleteProcessGroup(Revision revision, String groupId);

    // ----------------------------------------
    // RemoteProcessGroup methods
    // ----------------------------------------
    /**
     * Creates a new remote process group.
     *
     * @param revision revision
     * @param groupId The id of the parent group
     * @param remoteProcessGroupDTO The RemoteProcessGroupDTO
     * @return snapshot
     */
    RemoteProcessGroupEntity createRemoteProcessGroup(Revision revision, String groupId, RemoteProcessGroupDTO remoteProcessGroupDTO);

    /**
     * Determines whether or not the Remote Port with the given remotePortId is connected (i.e., there are incoming Connections if it's an Input Port
     * or there are outgoing Connections if it's an Output Port).
     *
     * @param remoteProcessGroupId the ID of the Remote Process Group
     * @param remotePortId the ID of the Port
     * @return <code>true</code> if remote port identified is connected, <code>false</code> if the port is not connected
     *
     * @throws ResourceNotFoundException if the port cannot be found
     */
    boolean isRemoteGroupPortConnected(String remoteProcessGroupId, String remotePortId);

    /**
     * Gets a remote process group.
     *
     * @param remoteProcessGroupId The id of the remote process group
     * @return group
     */
    RemoteProcessGroupEntity getRemoteProcessGroup(String remoteProcessGroupId);

    /**
     * Gets all remote process groups in the a given parent group.
     *
     * @param groupId The id of the parent group
     * @return group
     */
    Set<RemoteProcessGroupEntity> getRemoteProcessGroups(String groupId);

    /**
     * Gets the remote process group status.
     *
     * @param id remote process group
     * @return status
     */
    RemoteProcessGroupStatusEntity getRemoteProcessGroupStatus(String id);

    /**
     * Gets the remote process group status history.
     *
     * @param id The id of the remote process group
     * @return history
     */
    StatusHistoryEntity getRemoteProcessGroupStatusHistory(String id);


    /**
     * Verifies that transmission state of all remote process groups within the specified process group can be updated.
     * @param processGroupId The process group in which to verify remote process groups
     * @param shouldTransmit The transmission state to verify for
     */
    void verifyUpdateRemoteProcessGroups(String processGroupId, boolean shouldTransmit);

    /**
     * Verifies the specified remote process group can be updated.
     *
     * @param remoteProcessGroupDTO The RemoteProcessGroupDTO
     */
    void verifyUpdateRemoteProcessGroup(RemoteProcessGroupDTO remoteProcessGroupDTO);

    /**
     * Verifies the specified remote process group can update the specified remote input port.
     *
     * @param remoteProcessGroupId The id of the remote process group
     * @param remoteProcessGroupPortDTO The RemoteProcessGroupPortDTO
     */
    void verifyUpdateRemoteProcessGroupInputPort(String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO);

    /**
     * Verifies the specified remote process group can update the specified remote output port.
     *
     * @param remoteProcessGroupId The id of the remote process group
     * @param remoteProcessGroupPortDTO The RemoteProcessGroupPortDTO
     */
    void verifyUpdateRemoteProcessGroupOutputPort(String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO);

    /**
     * Updates the specified remote process group.
     *
     * @param revision Revision to compare with current base revision
     * @param remoteProcessGroupDTO The RemoteProcessGroupDTO
     * @return snapshot
     */
    RemoteProcessGroupEntity updateRemoteProcessGroup(Revision revision, RemoteProcessGroupDTO remoteProcessGroupDTO);

    /**
     * Updates the specified remote process groups input port.
     *
     * @param revision Revision to compare with current base revision
     * @param remoteProcessGroupId The id of the remote process group
     * @param remoteProcessGroupPortDTO The RemoteProcessGroupPortDTO
     * @return snapshot
     */
    RemoteProcessGroupPortEntity updateRemoteProcessGroupInputPort(Revision revision, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO);

    /**
     * Updates the specified remote process groups output port.
     *
     * @param revision Revision to compare with current base revision
     * @param remoteProcessGroupId The id of the remote process group
     * @param remoteProcessGroupPortDTO The RemoteProcessGroupPortDTO
     * @return snapshot
     */
    RemoteProcessGroupPortEntity updateRemoteProcessGroupOutputPort(Revision revision, String remoteProcessGroupId, RemoteProcessGroupPortDTO remoteProcessGroupPortDTO);

    /**
     * Verifies the remote process group can be deleted.
     *
     * @param remoteProcessGroupId The id of the remote process group
     */
    void verifyDeleteRemoteProcessGroup(String remoteProcessGroupId);

    /**
     * Deletes the specified remote process group.
     *
     * @param revision Revision to compare with current base revision
     * @param remoteProcessGroupId The id of the remote process group
     * @return snapshot
     */
    RemoteProcessGroupEntity deleteRemoteProcessGroup(Revision revision, String remoteProcessGroupId);


    /**
     * Create a system bulletin
     *
     * @param bulletinDTO bulletin to send to users
     * @param canRead allow users to read bulletin
     */
    BulletinEntity createBulletin(final BulletinDTO bulletinDTO, final Boolean canRead);

    // ----------------------------------------
    // Funnel methods
    // ----------------------------------------
    /**
     * Creates a funnel.
     *
     * @param revision revision
     * @param groupId group
     * @param funnelDTO funnel
     * @return The funnel DTO
     */
    FunnelEntity createFunnel(Revision revision, String groupId, FunnelDTO funnelDTO);

    /**
     * Gets the specified funnel.
     *
     * @param funnelId The funnel id
     * @return The funnel transfer object
     */
    FunnelEntity getFunnel(String funnelId);

    /**
     * Gets all of the funnels.
     *
     * @param groupId group
     * @return The funnel transfer objects
     */
    Set<FunnelEntity> getFunnels(String groupId);

    /**
     * Updates the specified funnel.
     *
     * @param revision Revision to compare with current base revision
     * @param funnelDTO The funnel DTO
     * @return The funnel DTO
     */
    FunnelEntity updateFunnel(Revision revision, FunnelDTO funnelDTO);

    /**
     * Verifies the specified funnel can be deleted.
     *
     * @param funnelId funnel
     */
    void verifyDeleteFunnel(String funnelId);

    /**
     * Deletes the specified funnel.
     *
     * @param revision Revision to compare with current base revision
     * @param funnelId The funnel id
     * @return snapshot
     */
    FunnelEntity deleteFunnel(Revision revision, String funnelId);


    // ----------------------------------------
    // Version Control methods
    // ----------------------------------------

    /**
     * Returns a FlowComparisonEntity that contains all of the local modifications since the Process Group
     * was last synchronized with the Flow Registry
     *
     * @param processGroupId the ID of the Process Group
     * @return a FlowComparisonEntity that contains all of the local modifications since the Process Group
     *         was last synchronized with the Flow Registry
     * @throws IllegalStateException if the Process Group with the given ID is not under version control
     */
    FlowComparisonEntity getLocalModifications(String processGroupId);

    /**
     * Determines whether the process group with the given id or any of its descendants are under version control.
     *
     * @param groupId the ID of the Process Group
     * @return <code>true</code> if any process group in the hierarchy is under version control, <code>false</code> otherwise.
     */
    boolean isAnyProcessGroupUnderVersionControl(final String groupId);

    /**
     * Returns the Version Control information for the Process Group with the given ID
     *
     * @param processGroupId the ID of the Process Group
     * @return the Version Control information that corresponds to the given Process Group, or <code>null</code> if the
     *         process group is not under version control
     */
    VersionControlInformationEntity getVersionControlInformation(String processGroupId);

    /**
     * Adds the given Versioned Flow to the registry specified by the given ID
     *
     * @param registryId the ID of the registry
     * @param flow the flow to add to the registry
     * @return a VersionedFlow that is fully populated, including identifiers
     *
     * @throws NiFiCoreException if unable to register flow
     */
    RegisteredFlow registerVersionedFlow(String registryId, RegisteredFlow flow);

    /**
     * Creates a snapshot of the Process Group with the given identifier, then creates a new Flow entity in the NiFi Registry
     * and adds the snapshot of the Process Group as the first version of that flow.
     *
     * @param groupId the UUID of the Process Group
     * @param requestEntity the details of the flow to create
     * @return a VersionControlComponentMappingEntity that contains the information needed to notify a Process Group where it is tracking to and map
     *         component ID's to their Versioned Component ID's
     */
    VersionControlComponentMappingEntity registerFlowWithFlowRegistry(String groupId, StartVersionControlRequestEntity requestEntity);

    /**
     * Deletes the specified Versioned Flow from the specified Flow Registry
     *
     * @param registryId the ID of the Flow Registry
     * @param branch the branch for the flow
     * @param bucketId the ID of the bucket
     * @param flowId the ID of the flow
     * @return the VersionedFlow that was deleted
     */
    RegisteredFlow deleteVersionedFlow(String registryId, String branch, String bucketId, String flowId);

    /**
     * Returns the differences of version B from version A.
     *
     * @param registryId the ID of the registry
     * @param versionLocationA Location of the baseline snapshot of the comparison
     * @param versionLocationB location of the compared snapshot
     * @return the differences between the snapshots
     */
    FlowComparisonEntity getVersionDifference(String registryId, FlowVersionLocation versionLocationA, FlowVersionLocation versionLocationB);

    /**
     * Adds the given snapshot to the already existing Versioned Flow, which resides in the given Flow Registry with the given id
     *
     * @param registryId the ID of the Flow Registry to persist the snapshot to
     * @param flow the flow where the snapshot should be persisted
     * @param snapshot the Snapshot to persist
     * @param parameterContexts a map of the Parameter Contexts to include keyed by name
     * @param parameterProviderReferences a map of the Parameter Providers referenced by any Parameter Context in the snapshot, keyed by identifier
     * @param externalControllerServiceReferences a mapping of controller service id to ExternalControllerServiceReference for any Controller Service that is referenced in the flow but not included
     * in the VersionedProcessGroup
     * @param comments about the snapshot
     * @param expectedVersion the version to save the flow as
     * @return the snapshot that represents what was stored in the registry
     *
     * @throws NiFiCoreException if unable to register the snapshot with the flow registry
     */
    RegisteredFlowSnapshot registerVersionedFlowSnapshot(String registryId, RegisteredFlow flow, VersionedProcessGroup snapshot,
                                                         Map<String, VersionedParameterContext> parameterContexts,
                                                         Map<String, ParameterProviderReference> parameterProviderReferences,
                                                         Map<String, ExternalControllerServiceReference> externalControllerServiceReferences,
                                                         String comments, String expectedVersion, RegisterAction registerAction);

    /**
     * Updates the Version Control Information on the Process Group with the given ID
     *
     * @param processGroupRevision the Revision of the Process Group
     * @param processGroupId the ID of the process group to update
     * @param versionControlInfo the new Version Control Information
     * @param versionedComponentMapping a mapping of component ID to Versioned Component ID
     *
     * @return a VersionControlInformationEntity that represents the newly updated Version Control information
     */
    VersionControlInformationEntity setVersionControlInformation(Revision processGroupRevision, String processGroupId, VersionControlInformationDTO versionControlInfo,
        Map<String, String> versionedComponentMapping);

    /**
     * Disconnects the specified Process Group from version control.
     *
     * @param revision revision
     * @param processGroupId group id
     * @return version control information prior to disconnecting
     */
    VersionControlInformationEntity deleteVersionControl(final Revision revision, final String processGroupId);

    /**
     * Retrieves the Versioned Flow Snapshot for the coordinates provided by the given Version Control Information DTO
     *
     * @param versionControlInfo the coordinates of the versioned flow
     * @param fetchRemoteFlows if the contents of Versioned Flow that is fetched contains a child/descendant Process Group
     *            that is also under Version Control, this indicates whether that remote flow should also be fetched
     * @return the VersionedFlowSnapshot that corresponds to the given coordinates
     *
     * @throws ResourceNotFoundException if the Versioned Flow Snapshot could not be found
     */
    FlowSnapshotContainer getVersionedFlowSnapshot(VersionControlInformationDTO versionControlInfo, boolean fetchRemoteFlows);

    /**
     * Get the latest Versioned Flow Snapshot from the registry for the Process Group with the given ID
     *
     * @param processGroupId the ID of the Process Group
     * @return the latest Versioned Flow Snapshot for download
     *
     * @throws ResourceNotFoundException if the Versioned Flow Snapshot could not be found
     */
    FlowSnapshotContainer getVersionedFlowSnapshotByGroupId(String processGroupId);

    /**
     * Copies the requested components from the specified Process Group.
     *
     * @param groupId the group id
     * @param copyRequest the copy request
     * @return the copy response
     */
    CopyResponseEntity copyComponents(String groupId, CopyRequestEntity copyRequest);

    /**
     * Get the current state of the Process Group with the given ID, converted to a Versioned Flow Snapshot
     *
     * @param processGroupId the ID of the Process Group
     * @return the current Process Group converted to a Versioned Flow Snapshot for download
     */
    RegisteredFlowSnapshot getCurrentFlowSnapshotByGroupId(String processGroupId);

    /**
     * Get the current state of the Process Group with the given ID, converted to a Versioned Flow Snapshot. Controller
     * Services referenced by the Components contained by the Process Group but are part of the parent Process Group(s)
     * will be included and will be considered as part of the requested Process Group.
     *
     * @param processGroupId the ID of the Process Group
     * @return the current Process Group converted to a Versioned Flow Snapshot for download
     */
    RegisteredFlowSnapshot getCurrentFlowSnapshotByGroupIdWithReferencedControllerServices(String processGroupId);

    /**
     * Returns the name of the Flow Registry that is registered with the given ID. If no Flow Registry exists with the given ID, will return
     * the ID itself as the name
     *
     * @param flowRegistryId the id of the flow registry
     * @return the name of the Flow Registry that is registered with the given ID, or the ID itself if no Flow Registry is registered with the given ID
     */
    String getFlowRegistryName(String flowRegistryId);

    /**
     * Determines which components currently exist in the Process Group with the given identifier and calculates which of those components
     * would be impacted by updating the Process Group to the provided snapshot
     *
     * @param processGroupId the ID of the Process Group to update
     * @param updatedSnapshot the snapshot to update the Process Group to
     * @return the set of all components that would be affected by updating the Process Group
     */
    Set<AffectedComponentEntity> getComponentsAffectedByFlowUpdate(String processGroupId, RegisteredFlowSnapshot updatedSnapshot);

    /**
     * Verifies that the Process Group with the given identifier can be updated to the proposed flow
     *  @param groupId the ID of the Process Group to update
     * @param proposedFlow the proposed flow
     * @param verifyConnectionRemoval whether or not to verify that connections that no longer exist in the proposed flow are eligible for deletion
     * @param verifyNotDirty whether or not to verify that the Process Group is not 'dirty'. If this value is <code>true</code>,
*            and the Process Group has been modified since it was last synchronized with the Flow Registry, then this method will
     */
    void verifyCanUpdate(String groupId, RegisteredFlowSnapshot proposedFlow, boolean verifyConnectionRemoval, boolean verifyNotDirty);

    /**
     * Verifies that the Processor with the given identifier is in a state where its configuration can be verified
     * @param processorId the ID of the processor
     */
    void verifyCanVerifyProcessorConfig(String processorId);

    /**
     * Verifies that the Controller Service with the given identifier is in a state where its configuration can be verified
     * @param controllerServiceId the ID of the service
     */
    void verifyCanVerifyControllerServiceConfig(String controllerServiceId);

    /**
     * Verifies that the Reporting Task with the given identifier is in a state where its configuration can be verified
     * @param reportingTaskId the ID of the service
     */
    void verifyCanVerifyReportingTaskConfig(String reportingTaskId);

    /**
     * Verifies that the Flow Analysis Rule with the given identifier is in a state where its configuration can be verified
     * @param flowAnalysisRuleId the ID of the flow analysis rule
     */
    void verifyCanVerifyFlowAnalysisRuleConfig(String flowAnalysisRuleId);

    /**
     * Verifies that the Parameter Provider with the given identifier is in a state where its configuration can be verified
     * @param parameterProviderId the ID of the service
     */
    void verifyCanVerifyParameterProviderConfig(String parameterProviderId);

    /**
     * Verifies that the Process Group with the given identifier can be saved to the flow registry
     *
     * @param groupId the ID of the Process Group
     * @param registryId the ID of the Flow Registry
     * @param flowLocation the flow location in the registry
     * @param saveAction the save action being performed
     *
     * @throws IllegalStateException if the Process Group cannot be saved to the flow registry with the coordinates specified
     */
    void verifyCanSaveToFlowRegistry(String groupId, String registryId, FlowLocation flowLocation, String saveAction);

    /**
     * Verifies that the Process Group with the given identifier can have its local modifications reverted to the given VersionedFlowSnapshot
     *
     * @param groupId the ID of the Process Group
     * @param versionedFlowSnapshot the Versioned Flow Snapshot
     *
     * @throws IllegalStateException if the Process Group cannot have its local modifications reverted
     */
    void verifyCanRevertLocalModifications(String groupId, RegisteredFlowSnapshot versionedFlowSnapshot);

    /**
     * Adds versioned components to the specified Process Group
     *
     * @param revision the revision of the Process Group
     * @param groupId the ID of the Process Group
     * @param additions the components to add
     * @param componentIdSeed the seed to use for generating new component ID's
     * @return the Paste response entity
     */
    PasteResponseEntity pasteComponents(Revision revision, String groupId, VersionedComponentAdditions additions, String componentIdSeed);

    /**
     * Updates the Process group with the given ID to match the new snapshot
     *
     * @param revision the revision of the Process Group
     * @param groupId the ID of the Process Group
     * @param versionControlInfo the Version Control information
     * @param snapshot the new snapshot
     * @param componentIdSeed the seed to use for generating new component ID's
     * @param updateSettings whether or not the process group's name and position should be updated
     * @param updateDescendantVersionedFlows if a child/descendant Process Group is under Version Control, specifies whether or not to
     *            update the contents of that Process Group
     * @return the Process Group
     */
    ProcessGroupEntity updateProcessGroupContents(Revision revision, String groupId, VersionControlInformationDTO versionControlInfo, RegisteredFlowSnapshot snapshot,
                                                  String componentIdSeed, boolean verifyNotModified, boolean updateSettings, boolean updateDescendantVersionedFlows);

    /**
     * Returns a Set representing all components that will be affected by updating the Parameter Contexts that are represented by the given DTOs.
     *
     * @param parameterContextDtos the Parameter Context DTOs that represent all changes that are to occur to a set of Parameter Contexts
     * @return a Set representing all components that will be affected by the updates
     */
    Set<AffectedComponentEntity> getComponentsAffectedByParameterContextUpdate(Collection<ParameterContextDTO> parameterContextDtos);

    /**
     * Returns an up-to-date representation of the component that is referenced by the given affected component
     * @param affectedComponent the affected component
     * @return an up-to-date representation of the affected component
     */
    AffectedComponentEntity getUpdatedAffectedComponentEntity(AffectedComponentEntity affectedComponent);

    /**
     * Returns a Set representing all Processors that reference any Parameters and that belong to the group with the given ID
     *
     * @param groupId the id of the process group
     * @return a Set representing all Processors that reference Parameters
     */
    Set<AffectedComponentEntity> getProcessorsReferencingParameter(String groupId);

    /**
     * Returns a Set representing all Controller Services that reference any Parameters and that belong to the group with the given ID
     *
     * @param groupId the id of the process group
     * @return a Set representing all Controller Services that reference Parameters
     */
    Set<AffectedComponentEntity> getControllerServicesReferencingParameter(String groupId);

    // ----------------------------------------
    // Component state methods
    // ----------------------------------------

    /**
     * Gets the state for the specified processor.
     *
     * @param processorId the processor id
     * @return  the component state
     */
    ComponentStateDTO getProcessorState(String processorId);

    /**
     * Verifies the processor state could be cleared.
     *
     * @param processorId the processor id
     */
    void verifyCanClearProcessorState(String processorId);

    /**
     * Clears the state for the specified processor.
     *
     * @param processorId the processor id
     */
    void clearProcessorState(String processorId);

    /**
     * Gets the state for the specified controller service.
     *
     * @param controllerServiceId the controller service id
     * @return  the component state
     */
    ComponentStateDTO getControllerServiceState(String controllerServiceId);

    /**
     * Verifies the controller service state could be cleared.
     *
     * @param controllerServiceId the controller service id
     */
    void verifyCanClearControllerServiceState(String controllerServiceId);

    /**
     * Clears the state for the specified controller service.
     *
     * @param controllerServiceId the controller service id
     */
    void clearControllerServiceState(String controllerServiceId);

    /**
     * Gets the state for the specified reporting task.
     *
     * @param reportingTaskId the reporting task id
     * @return  the component state
     */
    ComponentStateDTO getReportingTaskState(String reportingTaskId);

    /**
     * Verifies the reporting task state could be cleared.
     *
     * @param reportingTaskId the reporting task id
     */
    void verifyCanClearReportingTaskState(String reportingTaskId);

    /**
     * Clears the state for the specified reporting task.
     *
     * @param reportingTaskId the reporting task id
     */
    void clearReportingTaskState(String reportingTaskId);

    /**
     * Gets the state for the specified parameter provider.
     *
     * @param parameterProviderId the parameter provider id
     * @return  the component state
     */
    ComponentStateDTO getParameterProviderState(String parameterProviderId);

    /**
     * Verifies the parameter provider state could be cleared.
     *
     * @param parameterProviderId the parameter provider id
     */
    void verifyCanClearParameterProviderState(String parameterProviderId);

    /**
     * Clears the state for the specified parameter provider.
     *
     * @param parameterProviderId the parameter provider id
     */
    void clearParameterProviderState(String parameterProviderId);

    /**
     * Gets the state for the specified RemoteProcessGroup.
     *
     * @param remoteProcessGroupId the RemoteProcessGroup id
     * @return  the component state
     */
    ComponentStateDTO getRemoteProcessGroupState(String remoteProcessGroupId);


    // ----------------------------------------
    // Label methods
    // ----------------------------------------
    /**
     * Creates a label.
     *
     * @param revision revision
     * @param groupId group
     * @param labelDTO The label DTO
     * @return The label DTO
     */
    LabelEntity createLabel(Revision revision, String groupId, LabelDTO labelDTO);

    /**
     * Gets the specified label.
     *
     * @param labelId The label id
     * @return The label transfer object
     */
    LabelEntity getLabel(String labelId);

    /**
     * Gets all of the labels.
     *
     * @param groupId group
     * @return The label transfer objects
     */
    Set<LabelEntity> getLabels(String groupId);

    /**
     * Updates the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param labelDTO The label DTO
     * @return The label DTO
     */
    LabelEntity updateLabel(Revision revision, LabelDTO labelDTO);

    /**
     * Deletes the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param labelId The label id
     * @return snapshot
     */
    LabelEntity deleteLabel(Revision revision, String labelId);

    // ----------------------------------------
    // User methods
    // ----------------------------------------
    /**
     * Creates a user.
     * @param revision The starting revision
     * @param userDTO The user DTO
     * @return The user transfer object
     */
    UserEntity createUser(Revision revision, UserDTO userDTO);

    /**
     * Gets the user with the specified ID.
     * @param userId The user ID
     * @return The user transfer object
     */
    UserEntity getUser(String userId);

    /**
     * Gets all the users.
     * @return The user transfer objects
     */
    Set<UserEntity> getUsers();

    /**
     * Search for User and Group Tenants based on provided query string
     *
     * @param query Search query where null or empty returns unfiltered results
     * @return Tenants Entity with zero or more matched Users and User Groups
     */
    TenantsEntity searchTenants(String query);

    /**
     * Updates the specified user.
     * @param revision Revision to compare with current base revision
     * @param userDTO The user DTO
     * @return The user transfer object
     */
    UserEntity updateUser(Revision revision, UserDTO userDTO);

    /**
     * Deletes the specified user.
     * @param revision Revision to compare with current base revision
     * @param userId The user ID
     * @return The user transfer object of the deleted user
     */
    UserEntity deleteUser(Revision revision, String userId);

    // ----------------------------------------
    // Group methods
    // ----------------------------------------
    /**
     * Creates a user group.
     * @param revision The starting revision
     * @param userGroupDTO The user group DTO
     * @return The user group transfer object
     */
    UserGroupEntity createUserGroup(Revision revision, UserGroupDTO userGroupDTO);

    /**
     * Gets the user group with the specified ID.
     * @param userGroupId The user group ID
     * @return The user group transfer object
     */
    UserGroupEntity getUserGroup(String userGroupId);

    /**
     * Gets all user groups.
     * @return The user group transfer objects
     */
    Set<UserGroupEntity> getUserGroups();

    /**
     * Updates the specified user group.
     * @param revision Revision to compare with current base revision
     * @param userGroupDTO The user group DTO
     * @return The user group transfer object
     */
    UserGroupEntity updateUserGroup(Revision revision, UserGroupDTO userGroupDTO);

    /**
     * Deletes the specified user group.
     * @param revision Revision to compare with current base revision
     * @param userGroupId The user group ID
     * @return The user group transfer object of the deleted user group
     */
    UserGroupEntity deleteUserGroup(Revision revision, String userGroupId);

    // ----------------------------------------
    // AccessPolicy methods
    // ----------------------------------------
    /**
     * Creates an access policy.
     * @param revision The starting revision
     * @param accessPolicyDTO The access policy DTO
     * @return The access policy transfer object
     */
    AccessPolicyEntity createAccessPolicy(Revision revision, AccessPolicyDTO accessPolicyDTO);

    /**
     * Gets the access policy with the specified ID.
     * @param accessPolicyId access policy ID
     * @return The access policy transfer object
     */
    AccessPolicyEntity getAccessPolicy(String accessPolicyId);

    /**
     * Gets the access policy for the specified action, resource type, and component id.
     *
     * @param resource resource
     * @return access policy
     */
    AccessPolicyEntity getAccessPolicy(RequestAction requestAction, String resource);

    /**
     * Updates the specified access policy.
     * @param revision Revision to compare with current base revision
     * @param accessPolicyDTO The access policy DTO
     * @return The access policy transfer object
     */
    AccessPolicyEntity updateAccessPolicy(Revision revision, AccessPolicyDTO accessPolicyDTO);

    /**
     * Deletes the specified access policy.
     * @param revision Revision to compare with current base revision
     * @param accessPolicyId The access policy ID
     * @return The access policy transfer object of the deleted access policy
     */
    AccessPolicyEntity deleteAccessPolicy(Revision revision, String accessPolicyId);

    // ----------------------------------------
    // Controller Services methods
    // ----------------------------------------

    /**
     * Verifies the specified controller service can be created.
     *
     * @param controllerServiceDTO service
     */
    void verifyCreateControllerService(ControllerServiceDTO controllerServiceDTO);

    /**
     * Creates a controller service.
     *
     * @param revision revision
     * @param groupId the ID of the Process Group to add the Controller Service to
     * @param controllerServiceDTO The controller service DTO
     * @return The controller service DTO
     */
    ControllerServiceEntity createControllerService(Revision revision, String groupId, ControllerServiceDTO controllerServiceDTO);

    /**
     * Gets all controller services that belong to the given group and its parent/ancestor groups
     *
     * @param groupId the id of the process group of interest
     * @param includeAncestorGroups if true, parent and ancestor groups' services will be returned as well
     * @param includeDescendantGroups if true, child and descendant groups' services will be returned as well
     * @param includeReferencingComponents whether or not the response should include the referencing component services.
     *       These can be expensive to include so should not be included when the are not necessary.
     *
     * @return services
     */
    Set<ControllerServiceEntity> getControllerServices(String groupId, boolean includeAncestorGroups, boolean includeDescendantGroups, boolean includeReferencingComponents);

    /**
     * Gets the specified controller service.
     *
     * @param controllerServiceId id
     * @param includeReferencingComponents whether or not the response should include the referencing component services.
     *       These can be expensive to include so should not be included when the are not necessary.
     *
     * @return service
     */
    ControllerServiceEntity getControllerService(String controllerServiceId, boolean includeReferencingComponents);

    /**
     * Get the descriptor for the specified property of the specified controller service.
     *
     * @param id id
     * @param property property
     * @param sensitive requested sensitive status
     * @return property
     */
    PropertyDescriptorDTO getControllerServicePropertyDescriptor(String id, String property, boolean sensitive);

    /**
     * Gets the references for specified controller service.
     *
     * @param controllerServiceId id
     * @return service reference
     */
    ControllerServiceReferencingComponentsEntity getControllerServiceReferencingComponents(String controllerServiceId);

    /**
     * Updates the referencing components for the specified controller service.
     *
     * @param referenceRevisions revisions
     * @param controllerServiceId id
     * @param scheduledState state
     * @param controllerServiceState the value of state
     * @return The referencing component dtos
     */
    ControllerServiceReferencingComponentsEntity updateControllerServiceReferencingComponents(
        Map<String, Revision> referenceRevisions, String controllerServiceId, ScheduledState scheduledState, ControllerServiceState controllerServiceState);

    /**
     * Updates the specified controller service.
     *
     * @param revision Revision to compare with current base revision
     * @param controllerServiceDTO The controller service DTO
     * @return The controller service DTO
     */
    ControllerServiceEntity updateControllerService(Revision revision, ControllerServiceDTO controllerServiceDTO);

    /**
     * Moves the specified controller service.
     *
     * @param revision Revision to compare with current base revision
     * @param controllerServiceDTO The controller service DTO
     * @param newProcessGroupID The id of the process group the controller service is being moved to
     * @return The controller service DTO
     */
    ControllerServiceEntity moveControllerService(final Revision revision, final ControllerServiceDTO controllerServiceDTO, final String newProcessGroupID);

    /**
     * Performs verification of the given Configuration for the Controller Service with the given ID
     * @param controllerServiceId the id of the controller service
     * @param properties the configured properties to verify
     * @param variables a map of values that can be used for resolving FlowFile attributes for Expression Language
     * @return verification results
     */
    List<ConfigVerificationResultDTO> performControllerServiceConfigVerification(String controllerServiceId, Map<String, String> properties, Map<String, String> variables);

    /**
     * Performs analysis of the given properties, determining which attributes are referenced by properties
     * @param controllerServiceId the ID of the Controller Service
     * @param properties the properties
     * @return analysis results
     */
    ConfigurationAnalysisEntity analyzeControllerServiceConfiguration(String controllerServiceId, Map<String, String> properties);

    /**
     * Deletes the specified label.
     *
     * @param revision Revision to compare with current base revision
     * @param controllerServiceId The controller service id
     * @return snapshot
     */
    ControllerServiceEntity deleteControllerService(Revision revision, String controllerServiceId);

    /**
     * Verifies the specified controller service can be updated.
     *
     * @param controllerServiceDTO service
     */
    void verifyUpdateControllerService(ControllerServiceDTO controllerServiceDTO);

    /**
     * Verifies the referencing components of the specified controller service can be updated.
     *
     * @param controllerServiceId id
     * @param scheduledState schedule state
     * @param controllerServiceState service state
     */
    void verifyUpdateControllerServiceReferencingComponents(String controllerServiceId, ScheduledState scheduledState, ControllerServiceState controllerServiceState);

    /**
     * Verifies the specified controller service can be removed.
     *
     * @param controllerServiceId id
     */
    void verifyDeleteControllerService(String controllerServiceId);

    // ----------------------------------------
    // Parameter Provider methods
    // ----------------------------------------

    /**
     * Verifies the specified parameter provider can be created.
     *
     * @param parameterProviderDTO task
     */
    void verifyCreateParameterProvider(ParameterProviderDTO parameterProviderDTO);

    /**
     * Creates a parameter provider.
     *
     * @param revision revision
     * @param parameterProviderDTO The parameter provider DTO
     * @return The parameter provider DTO
     */
    ParameterProviderEntity createParameterProvider(Revision revision, ParameterProviderDTO parameterProviderDTO);

    /**
     * Gets all parameter providers.
     *
     * @return tasks
     */
    Set<ParameterProviderEntity> getParameterProviders();

    /**
     * Gets the specified parameter provider.
     *
     * @param parameterProviderId id
     * @return task
     */
    ParameterProviderEntity getParameterProvider(String parameterProviderId);

    /**
     * Verifies the specified parameter provider is able to fetch its parameters.
     *
     * @param parameterProviderId parameter provider id
     */
    void verifyCanFetchParameters(String parameterProviderId);

    /**
     * Fetches the parameters and caches them in the parameter provider.  Note that the parameters will not
     * be applied to the flow.
     * @param parameterProviderId parameter provider id
     * @return The parameter provider
     */
    ParameterProviderEntity fetchParameters(String parameterProviderId);

    /**
     * Verifies the specified parameter provider is able to apply fetched parameters to the flow.
     * @param parameterProviderId parameter provider id
     * @param parameterGroupConfigurations Configuration for each fetched Parameter Group.   Any parameters not found in this set will not be included
     *                                     in the update verification.
     */
    void verifyCanApplyParameters(String parameterProviderId, Collection<ParameterGroupConfiguration> parameterGroupConfigurations);

    /**
     * Returns a list of ParameterContext entities representing updates needed in order to apply the fetched
     * parameters from the parameter provider to the referencing parameter contexts
     * @param parameterProviderId parameter provider id
     * @param parameterGroupConfigurations Configuration for each fetched Parameter Group.  Any parameters not found in this set will not be included in the update.
     * @return The list of ParameterContextEntity objects representing required updates to referencing
     * parameter contexts
     */
    List<ParameterContextEntity> getParameterContextUpdatesForAppliedParameters(String parameterProviderId, Collection<ParameterGroupConfiguration> parameterGroupConfigurations);

    /**
     * Gets the references for specified parameter provider.
     *
     * @param parameterProviderId id
     * @return parameter provider references
     */
    ParameterProviderReferencingComponentsEntity getParameterProviderReferencingComponents(String parameterProviderId);

    /**
     * Get the descriptor for the specified property of the specified parameter provider.
     *
     * @param id id
     * @param property property
     * @return descriptor
     */
    PropertyDescriptorDTO getParameterProviderPropertyDescriptor(String id, String property);

    /**
     * Updates the specified parameter provider.
     *
     * @param revision Revision to compare with current base revision
     * @param parameterProviderDTO The parameter provider DTO
     * @return The parameter provider DTO
     */
    ParameterProviderEntity updateParameterProvider(Revision revision, ParameterProviderDTO parameterProviderDTO);

    /**
     * Performs verification of the given Configuration for the parameter provider with the given ID
     * @param parameterProviderId the id of the parameter provider
     * @param properties the configured properties to verify
     * @return verification results
     */
    List<ConfigVerificationResultDTO> performParameterProviderConfigVerification(String parameterProviderId, Map<String, String> properties);

    /**
     * Performs analysis of the given properties, determining which attributes are referenced by properties
     * @param parameterProviderId the ID of the parameter provider
     * @param properties the properties
     * @return analysis results
     */
    ConfigurationAnalysisEntity analyzeParameterProviderConfiguration(String parameterProviderId, Map<String, String> properties);

    /**
     * Deletes the specified parameter provider.
     *
     * @param revision Revision to compare with current base revision
     * @param parameterProviderId The parameter provider id
     * @return snapshot
     */
    ParameterProviderEntity deleteParameterProvider(Revision revision, String parameterProviderId);

    /**
     * Verifies the specified parameter provider can be updated.
     *
     * @param parameterProviderDTO parameter provider
     */
    void verifyUpdateParameterProvider(ParameterProviderDTO parameterProviderDTO);

    /**
     * Verifies the specified parameter provider can be removed.
     *
     * @param parameterProviderId id
     */
    void verifyDeleteParameterProvider(String parameterProviderId);

    // ----------------------------------------
    // Reporting Task methods
    // ----------------------------------------

    /**
     * Verifies the specified reporting task can be created.
     *
     * @param reportingTaskDTO task
     */
    void verifyCreateReportingTask(ReportingTaskDTO reportingTaskDTO);

    /**
     * Creates a reporting task.
     *
     * @param revision revision
     * @param reportingTaskDTO The reporting task DTO
     * @return The reporting task DTO
     */
    ReportingTaskEntity createReportingTask(Revision revision, ReportingTaskDTO reportingTaskDTO);

    /**
     * Gets all reporting tasks.
     *
     * @return tasks
     */
    Set<ReportingTaskEntity> getReportingTasks();

    /**
     * Gets the specified reporting task.
     *
     * @param reportingTaskId id
     * @return task
     */
    ReportingTaskEntity getReportingTask(String reportingTaskId);

    /**
     * Get the descriptor for the specified property of the specified reporting task.
     *
     * @param id id
     * @param property property
     * @param sensitive requested sensitive status
     * @return descriptor
     */
    PropertyDescriptorDTO getReportingTaskPropertyDescriptor(String id, String property, boolean sensitive);

    /**
     * Updates the specified reporting task.
     *
     * @param revision Revision to compare with current base revision
     * @param reportingTaskDTO The reporting task DTO
     * @return The reporting task DTO
     */
    ReportingTaskEntity updateReportingTask(Revision revision, ReportingTaskDTO reportingTaskDTO);

    /**
     * Performs verification of the given Configuration for the Reporting Task with the given ID
     * @param reportingTaskId the id of the reporting task
     * @param properties the configured properties to verify
     * @return verification results
     */
    List<ConfigVerificationResultDTO> performReportingTaskConfigVerification(String reportingTaskId, Map<String, String> properties);

    /**
     * Performs analysis of the given properties, determining which attributes are referenced by properties
     * @param reportingTaskId the ID of the Reporting Task
     * @param properties the properties
     * @return analysis results
     */
    ConfigurationAnalysisEntity analyzeReportingTaskConfiguration(String reportingTaskId, Map<String, String> properties);

    /**
     * Deletes the specified reporting task.
     *
     * @param revision Revision to compare with current base revision
     * @param reportingTaskId The reporting task id
     * @return snapshot
     */
    ReportingTaskEntity deleteReportingTask(Revision revision, String reportingTaskId);

    /**
     * Verifies the specified reporting task can be updated.
     *
     * @param reportingTaskDTO task
     */
    void verifyUpdateReportingTask(ReportingTaskDTO reportingTaskDTO);

    /**
     * Verifies the specified reporting task can be removed.
     *
     * @param reportingTaskId id
     */
    void verifyDeleteReportingTask(String reportingTaskId);

    // ----------------------------------------
    // Registry methods
    // ----------------------------------------

    /**
     * Creates a registry.
     *
     * @param revision revision
     * @param flowRegistryClientDTO The registry DTO
     * @return The reporting task DTO
     */
    FlowRegistryClientEntity createRegistryClient(Revision revision, FlowRegistryClientDTO flowRegistryClientDTO);

    /**
     * Gets a registry with the specified id.
     *
     * @param registryClientId registry client id
     * @return entity
     */
    FlowRegistryClientEntity getRegistryClient(String registryClientId);

    /**
     * Returns all registry clients.
     *
     * @return registry clients
     */
    Set<FlowRegistryClientEntity> getRegistryClients();

    /**
     * Gets all registries for the current user.
     *
     * @return registry clients
     */
    Set<FlowRegistryClientEntity> getRegistryClientsForUser();

    /**
     * Get the descriptor for the specified property of the specified flow registry client.
     *
     * @param id id
     * @param property property
     * @param sensitive requested sensitive status
     * @return descriptor
     */
    PropertyDescriptorDTO getRegistryClientPropertyDescriptor(final String id, final String property, final boolean sensitive);

    /**
     * Returns the default branch for the given registry client.
     *
     * @param registryClientId registry client id
     * @return the default branch
     */
    FlowRegistryBranchEntity getDefaultBranch(String registryClientId);

    /**
     * Returns the branches in the given registry.
     *
     * @param registryClientId registry client id
     * @return the branches
     */
    Set<FlowRegistryBranchEntity> getBranches(String registryClientId);

    /**
     * Gets all buckets for a given registry.
     *
     * @param registryClientId registry client id
     * @return the buckets
     */
    Set<FlowRegistryBucketEntity> getBucketsForUser(String registryClientId, String branch);

    /**
     * Gets the flows for the current user for the specified registry and bucket.
     *
     * @param registryClientId registry client id
     * @param branch the branch
     * @param bucketId bucket id
     * @return the flows
     */
    Set<VersionedFlowEntity> getFlowsForUser(String registryClientId, String branch, String bucketId);


    /**
     * Returns the details of a versioned flow from a given bucket of a given registry.
     *
     * @param registryClientId registry client id
     * @param branch the branch
     * @param bucketId bucket id
     * @param flowId flow id
     * @return the flow details
     */
    VersionedFlowEntity getFlowForUser(String registryClientId, String branch, String bucketId, String flowId);

    /**
     * Gets the versions of the specified registry, bucket, and flow for the current user.
     *
     * @param registryClientId registry client id
     * @param branch the branch
     * @param bucketId bucket id
     * @param flowId flow id
     * @return the versions of the flow
     */
    Set<VersionedFlowSnapshotMetadataEntity> getFlowVersionsForUser(String registryClientId, String branch, String bucketId, String flowId);

    /**
     * Updates the specified registry using the specified revision.
     *
     * @param revision revision
     * @param flowRegistryClientDTO the registry dto
     * @return the updated registry registry entity
     */
    FlowRegistryClientEntity updateRegistryClient(Revision revision, FlowRegistryClientDTO flowRegistryClientDTO);

    /**
     * Deletes the specified registry using the specified revision.
     *
     * @param revision revision
     * @param registryClientId id
     * @return the deleted registry entity
     */
    FlowRegistryClientEntity deleteRegistryClient(Revision revision, String registryClientId);

    /**
     * Verifies the specified registry can be removed.
     *
     * @param registryClientId the registry client id
     */
    void verifyDeleteRegistry(String registryClientId);

    // ----------------------------------------
    // History methods
    // ----------------------------------------
    /**
     * Returns actions that meet the specified criteria.
     *
     * @param historyQuery query
     * @return history
     */
    HistoryDTO getActions(HistoryQueryDTO historyQuery);

    /**
     * Returns the details for the specified action id.
     *
     * @param actionId id
     * @return action
     */
    ActionEntity getAction(Integer actionId);

    /**
     * Purges all actions up to the specified end date.
     *
     * @param endDate The end date
     */
    void deleteActions(Date endDate);

    /**
     * Gets the history for the specified property for the specified component.
     *
     * @param componentId id
     * @return history
     */
    ComponentHistoryDTO getComponentHistory(String componentId);

    // ----------------------------------------
    // Snippet methods
    // ----------------------------------------
    /**
     * Creates a new snippet based off the existing snippet.
     *
     * @param groupId group id
     * @param snippetId snippet id
     * @param originX x
     * @param originY y
     * @param idGenerationSeed the seed to use for generating UUID's. May be null.
     * @return snapshot
     */
    FlowEntity copySnippet(String groupId, String snippetId, Double originX, Double originY, String idGenerationSeed);

    /**
     * Creates a new snippet.
     *
     * @param snippet snippet
     * @return snapshot
     */
    SnippetEntity createSnippet(SnippetDTO snippet);

    /**
     * Determines if this snippet can be updated.
     *
     * @param snippetDto snippet
     */
    void verifyUpdateSnippet(SnippetDTO snippetDto, Set<String> affectedComponentIds);

    /**
     * If group id is specified, moves the specified snippet to the specified group.
     *
     * @param revisions revisions
     * @param snippetDto snippet
     * @return snapshot
     */
    SnippetEntity updateSnippet(Set<Revision> revisions, SnippetDTO snippetDto);

    /**
     * Determines if this snippet can be removed.
     *
     * @param id id
     * @param affectedComponentIds affected components
     */
    void verifyDeleteSnippet(String id, Set<String> affectedComponentIds);

    /**
     * Removes the specified snippet.
     *
     * @param revisions revisions
     * @param snippetId snippet
     * @return snapshot
     */
    SnippetEntity deleteSnippet(Set<Revision> revisions, String snippetId);

    // ----------------------------------------
    // Cluster methods
    // ----------------------------------------
    /**
     * @return true if controller is connected or trying to connect to the cluster
     */
    boolean isClustered();

    /**
     * @return the id of this node, if clustered. If not clustered, returns null
     */
    String getNodeId();

    /**
     * @return the contents of cluster
     */
    ClusterDTO getCluster();

    /**
     * Returns the contents of the node.
     *
     * @param nodeId a node identifier
     * @return the contents of the node
     */
    NodeDTO getNode(String nodeId);

    /**
     * Updates the contents of the node.
     *
     * @param nodeDTO a node transfer object
     * @return node
     */
    NodeDTO updateNode(NodeDTO nodeDTO);

    /**
     * Deletes the node.
     *
     * @param nodeId a node identifier
     */
    void deleteNode(String nodeId);

    // ----------------------------------------
    // BulletinBoard methods
    // ----------------------------------------
    /**
     * @param query query
     * @return the bulletin board for this NiFi
     */
    BulletinBoardDTO getBulletinBoard(BulletinQueryDTO query);

    // ----------------------------------------
    // System diagnostics methods
    // ----------------------------------------
    /**
     * @return the system diagnostics
     */
    SystemDiagnosticsDTO getSystemDiagnostics(DiagnosticLevel diagnosticLevel);

    // ----------------------------------------
    // Resources
    // ----------------------------------------
    /**
     * @return the resources
     */
    List<ResourceDTO> getResources();

    // ----------------------------------------
    // Bundle methods
    // ----------------------------------------

    /**
     * Discovers the compatible bundle details for the components in the specified Versioned Process Group and updates the Versioned Process Group
     * to reflect the appropriate bundles.
     *
     * @param versionedGroup the versioned group
     */
    void discoverCompatibleBundles(VersionedProcessGroup versionedGroup);

    /**
     * Discovers the compatible bundle details for the components in the specified snapshot and updates the snapshot to reflect the appropriate bundles.
     *
     * @param reportingTaskSnapshot the snapshot
     */
    void discoverCompatibleBundles(VersionedReportingTaskSnapshot reportingTaskSnapshot);

    /**
     * For any Controller Service that is found in the given Versioned Process Group, if that Controller Service is not itself included in the Versioned Process Groups,
     * attempts to find an existing Controller Service that matches the definition. If any is found, the component within the Versioned Process Group is updated to point
     * to the existing service.
     *  @param flowSnapshotContainer the flow snapshot container
     * @param parentGroupId the ID of the Process Group from which the Controller Services are inherited
     * @param user the NiFi user on whose behalf the request is happening; this user is used for validation so that only the Controller Services that the user has READ permissions to are included
     * @return Any unresolved controller services
     */
    Set<String> resolveInheritedControllerServices(FlowSnapshotContainer flowSnapshotContainer, String parentGroupId, NiFiUser user);

    /**
     * For any Parameter Provider that is found in the given Versioned Process Group, attempts to find an existing Parameter Provider that matches the definition. If any is found,
     * the Parameter Context within the Versioned Process Group is updated to point to the existing Parameter Provider.
     *
     * @param versionedFlowSnapshot the flow snapshot
     * @param user the NiFi user on whose behalf the request is happening; this user is used for validation so that only the Parameter Providers that the user has READ permissions to are included
     * @return any unresolved parameter provider ids
     */
    Set<String> resolveParameterProviders(RegisteredFlowSnapshot versionedFlowSnapshot, NiFiUser user);

    /**
     * @param type the component type
     * @param bundleDTO bundle to find the component
     * @return the bundle coordinate
     * @throws IllegalStateException no compatible bundle found
     */
    BundleCoordinate getCompatibleBundle(String type, BundleDTO bundleDTO);

    /**
     * @param classType the class name
     * @param bundleCoordinate the bundle coordinate
     * @return the temp component
     */
    ConfigurableComponent getTempComponent(String classType, BundleCoordinate bundleCoordinate);

    // ----------------------------------------
    // Flow Analysis Rule methods
    // ----------------------------------------

    /**
     * Gets all flow analysis rules.
     *
     * @return flow analysis rules
     */
    Set<FlowAnalysisRuleEntity> getFlowAnalysisRules();

    /**
     * Returns the list of flow analysis rule types.
     *
     * @param bundleGroupFilter    if specified, must be member of bundle group
     * @param bundleArtifactFilter if specified, must be member of bundle artifact
     * @param typeFilter           if specified, type must match
     * @return The list of available flow analysis rule types matching specified criteria
     */
    Set<DocumentedTypeDTO> getFlowAnalysisRuleTypes(String bundleGroupFilter, String bundleArtifactFilter, String typeFilter);

    /**
     * Verifies the specified flow analysis rule can be created.
     *
     * @param flowAnalysisRuleDTO flow analysis rule
     */
    void verifyCreateFlowAnalysisRule(FlowAnalysisRuleDTO flowAnalysisRuleDTO);

    /**
     * Verifies the specified flow analysis rule can be updated.
     *
     * @param flowAnalysisRuleDTO flow analysis rule
     */
    void verifyUpdateFlowAnalysisRule(FlowAnalysisRuleDTO flowAnalysisRuleDTO);

    /**
     * Performs verification of the given Configuration for the Flow Analysis Rule with the given ID
     * @param flowAnalysisRuleId the id of the flow analysis rule
     * @param properties the configured properties to verify
     * @return verification results
     */
    List<ConfigVerificationResultDTO> performFlowAnalysisRuleConfigVerification(String flowAnalysisRuleId, Map<String, String> properties);

    /**
     * Performs analysis of the given properties, determining which attributes are referenced by properties
     * @param flowAnalysisRuleId the ID of the Flow Analysis Rule
     * @param properties the properties
     * @return analysis results
     */
    ConfigurationAnalysisEntity analyzeFlowAnalysisRuleConfiguration(String flowAnalysisRuleId, Map<String, String> properties);

    /**
     * Verifies the specified flow analysis rule can be removed.
     *
     * @param flowAnalysisRuleId id of flow analysis rule
     */
    void verifyDeleteFlowAnalysisRule(final String flowAnalysisRuleId);

    /**
     * Verifies the state of a flow analysis rule can be cleared.
     *
     * @param flowAnalysisRuleId the flow analysis rule id
     */
    void verifyCanClearFlowAnalysisRuleState(String flowAnalysisRuleId);

    /**
     * Creates a flow analysis rule.
     *
     * @param revision            revision
     * @param flowAnalysisRuleDTO The flow analysis rule (as DTO)
     * @return The created flow analysis rule (wrapped in an Entity)
     */
    FlowAnalysisRuleEntity createFlowAnalysisRule(Revision revision, FlowAnalysisRuleDTO flowAnalysisRuleDTO);

    /**
     * Gets the flow analysis rule with the specified id.
     *
     * @param flowAnalysisRuleId id of the flow analysis rule
     * @return the flow analysis rule
     */
    FlowAnalysisRuleEntity getFlowAnalysisRule(String flowAnalysisRuleId);

    /**
     * Get the descriptor for the specified property of the flow analysis rule with the specified id.
     *
     * @param flowAnalysisRuleId id of the flow analysis rule
     * @param propertyName       property name
     * @param sensitive          requested sensitive status
     * @return descriptor
     */
    PropertyDescriptorDTO getFlowAnalysisRulePropertyDescriptor(String flowAnalysisRuleId, String propertyName, boolean sensitive);

    /**
     * Gets the state for the flow analysis rule with the specified id.
     *
     * @param flowAnalysisRuleId the flow analysis rule id
     * @return the component state
     */
    ComponentStateDTO getFlowAnalysisRuleState(String flowAnalysisRuleId);

    /**
     * Clears the state for the flow analysis rule with the specified id.
     *
     * @param flowAnalysisRuleId the flow analysis rule id
     */
    void clearFlowAnalysisRuleState(String flowAnalysisRuleId);

    /**
     * Updates the specified flow analysis rule.
     *
     * @param revision            Revision to compare with current base revision
     * @param flowAnalysisRuleDTO The flow analysis rule (as DTO)
     * @return The updated flow analysis rule (wrapped in an Entity)
     */
    FlowAnalysisRuleEntity updateFlowAnalysisRule(Revision revision, FlowAnalysisRuleDTO flowAnalysisRuleDTO);

    /**
     * Deletes the flow analysis rule with the specified id.
     *
     * @param revision           Revision to compare with current base revision
     * @param flowAnalysisRuleId The flow analysis rule id
     * @return snapshot of the deleted flow analysis rule (wrapped in an Entity)
     */
    FlowAnalysisRuleEntity deleteFlowAnalysisRule(Revision revision, String flowAnalysisRuleId);

    /**
     * @return all current rule violations
     */
    FlowAnalysisResultEntity getFlowAnalysisResult();

    /**
     * Returns the rule violations produced by the analysis of a given process group
     * (Recursive - includes violations for all analyzed child process groups as well)
     *
     * @param processGroupId the id of the process that was analyzed
     * @return rule violations produced by the analysis of the process group
     */
    FlowAnalysisResultEntity getFlowAnalysisResult(String processGroupId);

    // ----------------------------------------
    // NAR Manager methods
    // ----------------------------------------

    /**
     * Saves and installs an uploaded NAR contained in the input stream.
     *
     * Note - "upload" is purposely not part of the AOP write lock since an upload may take a significant amount of time and should not block all other operations.
     *
     * @param inputStream the contents of the NAR
     * @return the summary entity for the given NAR
     * @throws IOException if an I/O error occurs reading the input stream or persisting the NAR
     */
    NarSummaryEntity uploadNar(InputStream inputStream) throws IOException;

    /**
     * @return the summaries for all NARs contained in the NAR Manager
     */
    Set<NarSummaryEntity> getNarSummaries();

    /**
     * Retrieves the summary for the NAR with the given identifier.
     *
     * @param identifier the NAR identifier
     * @return the summary
     */
    NarSummaryEntity getNarSummary(String identifier);

    /**
     * Retrieves the details for the given NAR.
     *
     * @param identifier the NAR identifier
     * @return the entity containing the details
     */
    NarDetailsEntity getNarDetails(String identifier);

    /**
     * Gets an input stream to read the content of the given NAR.
     *
     * @param identifier the NAR identifier
     * @return the input stream containing the NAR content
     */
    InputStream readNar(String identifier);

    /**
     * Verifies the given NAR can be deleted.
     *
     * @param identifier the NAR identifier
     * @param forceDelete indicates if the NAR should be deleted even when components are instantiated
     */
    void verifyDeleteNar(String identifier, boolean forceDelete);

    /**
     * Deletes the given NAR.
     *
     * @param identifier the NAR identifier
     * @return the summary of the deleted NAR
     * @throws IOException if an I/O error occurs deleting the NAR
     */
    NarSummaryEntity deleteNar(String identifier) throws IOException;

    // ----------------------------------------
    // Asset Manager methods
    // ----------------------------------------

    /**
     * Verifies the given asset can be deleted from the given parameter context.
     *
     * @param parameterContextId the parameter context id
     * @param assetId the asset id
     */
    void verifyDeleteAsset(String parameterContextId, String assetId);

    /**
     * Deletes the given asset from the given parameter context.
     *
     * @param parameterContextId the parameter context id
     * @param assetId the asset id
     */
    AssetEntity deleteAsset(String parameterContextId, String assetId);

}
