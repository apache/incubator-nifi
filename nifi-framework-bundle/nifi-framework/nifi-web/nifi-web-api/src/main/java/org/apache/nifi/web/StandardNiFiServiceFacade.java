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
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.FlowChangePurgeDetails;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.AuthorizableLookup;
import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.AuthorizeAccess;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.UserContextKeys;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.EnforcePolicyPermissionsThroughBaseResource;
import org.apache.nifi.authorization.resource.OperationAuthorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.c2.protocol.component.api.ControllerServiceDefinition;
import org.apache.nifi.c2.protocol.component.api.FlowAnalysisRuleDefinition;
import org.apache.nifi.c2.protocol.component.api.ParameterProviderDefinition;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.ReportingTaskDefinition;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.heartbeat.HeartbeatMonitor;
import org.apache.nifi.cluster.coordination.heartbeat.NodeHeartbeat;
import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.coordination.node.OffloadCode;
import org.apache.nifi.cluster.event.NodeEvent;
import org.apache.nifi.cluster.manager.exception.IllegalNodeDeletionException;
import org.apache.nifi.cluster.manager.exception.UnknownNodeException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.Counter;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ParametersApplication;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.PropertyConfigurationMapper;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.claim.ContentDirection;
import org.apache.nifi.controller.serialization.VersionedReportingTaskImportResult;
import org.apache.nifi.controller.serialization.VersionedReportingTaskImporter;
import org.apache.nifi.controller.serialization.VersionedReportingTaskSnapshotMapper;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.history.ProcessGroupStatusDescriptor;
import org.apache.nifi.diagnostics.DiagnosticLevel;
import org.apache.nifi.diagnostics.StorageUsage;
import org.apache.nifi.diagnostics.SystemDiagnostics;
import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedExternalFlowMetadata;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.flow.VersionedReportingTaskSnapshot;
import org.apache.nifi.flowanalysis.FlowAnalysisRule;
import org.apache.nifi.groups.ComponentAdditions;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.ProcessGroupCounts;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.VersionedComponentAdditions;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarInstallRequest;
import org.apache.nifi.nar.NarManager;
import org.apache.nifi.nar.NarNode;
import org.apache.nifi.nar.NarSource;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextLookup;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterGroupConfiguration;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ParameterReferenceManager;
import org.apache.nifi.parameter.StandardParameterContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.prometheusutil.AbstractMetricsRegistry;
import org.apache.nifi.prometheusutil.BulletinMetricsRegistry;
import org.apache.nifi.prometheusutil.ClusterMetricsRegistry;
import org.apache.nifi.prometheusutil.ConnectionAnalyticsMetricsRegistry;
import org.apache.nifi.prometheusutil.JvmMetricsRegistry;
import org.apache.nifi.prometheusutil.NiFiMetricsRegistry;
import org.apache.nifi.prometheusutil.PrometheusMetricsUtil;
import org.apache.nifi.registry.flow.FlowLocation;
import org.apache.nifi.registry.flow.FlowRegistryBranch;
import org.apache.nifi.registry.flow.FlowRegistryBucket;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryClientContextFactory;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.FlowRegistryClientUserContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.FlowRegistryPermissions;
import org.apache.nifi.registry.flow.FlowRegistryUtil;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;
import org.apache.nifi.registry.flow.FlowVersionLocation;
import org.apache.nifi.registry.flow.RegisterAction;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.registry.flow.diff.ComparableDataFlow;
import org.apache.nifi.registry.flow.diff.ConciseEvolvingDifferenceDescriptor;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowComparator;
import org.apache.nifi.registry.flow.diff.FlowComparatorVersionedStrategy;
import org.apache.nifi.registry.flow.diff.FlowComparison;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.diff.StandardComparableDataFlow;
import org.apache.nifi.registry.flow.diff.StandardFlowComparator;
import org.apache.nifi.registry.flow.diff.StaticDifferenceDescriptor;
import org.apache.nifi.registry.flow.mapping.ComponentIdLookup;
import org.apache.nifi.registry.flow.mapping.FlowMappingOptions;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedComponent;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedPort;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessGroup;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.mapping.NiFiRegistryFlowMapper;
import org.apache.nifi.registry.flow.mapping.VersionedComponentStateLookup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.reporting.VerifiableReportingTask;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.FlowDifferenceFilters;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.validation.RuleViolation;
import org.apache.nifi.validation.RuleViolationsManager;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.AccessPolicySummaryDTO;
import org.apache.nifi.web.api.dto.AffectedComponentDTO;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ClusterDTO;
import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.ComponentDifferenceDTO;
import org.apache.nifi.web.api.dto.ComponentHistoryDTO;
import org.apache.nifi.web.api.dto.ComponentReferenceDTO;
import org.apache.nifi.web.api.dto.ComponentRestrictionPermissionDTO;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.ComponentValidationResultDTO;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.ConfigurationAnalysisDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.ControllerServiceReferencingComponentDTO;
import org.apache.nifi.web.api.dto.CounterDTO;
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.CountersSnapshotDTO;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.EntityFactory;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleViolationDTO;
import org.apache.nifi.web.api.dto.FlowConfigurationDTO;
import org.apache.nifi.web.api.dto.FlowFileDTO;
import org.apache.nifi.web.api.dto.FlowRegistryBranchDTO;
import org.apache.nifi.web.api.dto.FlowRegistryBucketDTO;
import org.apache.nifi.web.api.dto.FlowRegistryClientDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.NarCoordinateDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.dto.ParameterProviderConfigurationDTO;
import org.apache.nifi.web.api.dto.ParameterProviderDTO;
import org.apache.nifi.web.api.dto.ParameterProviderReferencingComponentDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PreviousValueDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessGroupNameDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.ProcessorRunStatusDetailsDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.PropertyHistoryDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.RequiredPermissionDTO;
import org.apache.nifi.web.api.dto.ResourceDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;
import org.apache.nifi.web.api.dto.SystemDiagnosticsDTO;
import org.apache.nifi.web.api.dto.TenantDTO;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.dto.VersionedFlowDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.dto.diagnostics.ConnectionDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.ControllerServiceDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.ProcessorDiagnosticsDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.provenance.LatestProvenanceEventsDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceOptionsDTO;
import org.apache.nifi.web.api.dto.provenance.lineage.LineageDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatisticsDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.dto.status.StatusSnapshotDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.AccessPolicySummaryEntity;
import org.apache.nifi.web.api.entity.ActionEntity;
import org.apache.nifi.web.api.entity.ActivateControllerServicesEntity;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ComponentReferenceEntity;
import org.apache.nifi.web.api.entity.ComponentValidationResultEntity;
import org.apache.nifi.web.api.entity.ConfigurationAnalysisEntity;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionStatisticsEntity;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;
import org.apache.nifi.web.api.entity.ControllerBulletinsEntity;
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;
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
import org.apache.nifi.web.api.entity.ParameterEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ParameterProviderReferencingComponentEntity;
import org.apache.nifi.web.api.entity.ParameterProviderReferencingComponentsEntity;
import org.apache.nifi.web.api.entity.PasteResponseEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.PortStatusEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupRecursivity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusSnapshotEntity;
import org.apache.nifi.web.api.entity.ProcessorDiagnosticsEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorRunStatusDetailsEntity;
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
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.TenantsEntity;
import org.apache.nifi.web.api.entity.UserEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;
import org.apache.nifi.web.api.entity.VersionControlComponentMappingEntity;
import org.apache.nifi.web.api.entity.VersionControlInformationEntity;
import org.apache.nifi.web.api.entity.VersionedFlowEntity;
import org.apache.nifi.web.api.entity.VersionedFlowSnapshotMetadataEntity;
import org.apache.nifi.web.api.entity.VersionedReportingTaskImportResponseEntity;
import org.apache.nifi.web.api.request.FlowMetricsRegistry;
import org.apache.nifi.web.controller.ControllerFacade;
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.apache.nifi.web.dao.ConnectionDAO;
import org.apache.nifi.web.dao.ControllerServiceDAO;
import org.apache.nifi.web.dao.FlowAnalysisRuleDAO;
import org.apache.nifi.web.dao.FlowRegistryDAO;
import org.apache.nifi.web.dao.FunnelDAO;
import org.apache.nifi.web.dao.LabelDAO;
import org.apache.nifi.web.dao.ParameterContextDAO;
import org.apache.nifi.web.dao.ParameterProviderDAO;
import org.apache.nifi.web.dao.PortDAO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.dao.ProcessorDAO;
import org.apache.nifi.web.dao.RemoteProcessGroupDAO;
import org.apache.nifi.web.dao.ReportingTaskDAO;
import org.apache.nifi.web.dao.SnippetDAO;
import org.apache.nifi.web.dao.UserDAO;
import org.apache.nifi.web.dao.UserGroupDAO;
import org.apache.nifi.web.revision.ExpiredRevisionClaimException;
import org.apache.nifi.web.revision.RevisionClaim;
import org.apache.nifi.web.revision.RevisionManager;
import org.apache.nifi.web.revision.RevisionUpdate;
import org.apache.nifi.web.revision.StandardRevisionClaim;
import org.apache.nifi.web.revision.StandardRevisionUpdate;
import org.apache.nifi.web.util.PredictionBasedParallelProcessingService;
import org.apache.nifi.web.util.SnippetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.oauth2.core.OAuth2Token;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;

/**
 * Implementation of NiFiServiceFacade that performs revision checking.
 */
@Service
public class StandardNiFiServiceFacade implements NiFiServiceFacade {
    private static final Logger logger = LoggerFactory.getLogger(StandardNiFiServiceFacade.class);
    private static final int VALIDATION_WAIT_MILLIS = 50;
    private static final String ROOT_PROCESS_GROUP = "RootProcessGroup";

    // nifi core components
    private ControllerFacade controllerFacade;
    private SnippetUtils snippetUtils;

    // revision manager
    private RevisionManager revisionManager;
    private BulletinRepository bulletinRepository;

    // data access objects
    private ProcessorDAO processorDAO;
    private ProcessGroupDAO processGroupDAO;
    private RemoteProcessGroupDAO remoteProcessGroupDAO;
    private LabelDAO labelDAO;
    private FunnelDAO funnelDAO;
    private SnippetDAO snippetDAO;
    private PortDAO inputPortDAO;
    private PortDAO outputPortDAO;
    private ConnectionDAO connectionDAO;
    private ControllerServiceDAO controllerServiceDAO;
    private ReportingTaskDAO reportingTaskDAO;
    private FlowAnalysisRuleDAO flowAnalysisRuleDAO;
    private ParameterProviderDAO parameterProviderDAO;
    private UserDAO userDAO;
    private UserGroupDAO userGroupDAO;
    private AccessPolicyDAO accessPolicyDAO;
    private FlowRegistryDAO flowRegistryDAO;
    private ParameterContextDAO parameterContextDAO;
    private ClusterCoordinator clusterCoordinator;
    private HeartbeatMonitor heartbeatMonitor;
    private LeaderElectionManager leaderElectionManager;

    // administrative services
    private AuditService auditService;

    // properties
    private NiFiProperties properties;
    private DtoFactory dtoFactory;
    private EntityFactory entityFactory;

    private Authorizer authorizer;

    private AuthorizableLookup authorizableLookup;

    // Prometheus Metrics objects
    private final JvmMetricsRegistry jvmMetricsRegistry = new JvmMetricsRegistry();
    private final ConnectionAnalyticsMetricsRegistry connectionAnalyticsMetricsRegistry = new ConnectionAnalyticsMetricsRegistry();
    private final ClusterMetricsRegistry clusterMetricsRegistry = new ClusterMetricsRegistry();

    private RuleViolationsManager ruleViolationsManager;
    private PredictionBasedParallelProcessingService parallelProcessingService;
    private NarManager narManager;
    private AssetManager assetManager;

    // -----------------------------------------
    // Synchronization methods
    // -----------------------------------------
    @Override
    public void authorizeAccess(final AuthorizeAccess authorizeAccess) {
        authorizeAccess.authorize(authorizableLookup);
    }

    @Override
    public void verifyRevision(final Revision revision, final NiFiUser user) {
        final Revision curRevision = revisionManager.getRevision(revision.getComponentId());
        if (revision.equals(curRevision)) {
            return;
        }

        throw new InvalidRevisionException(revision + " is not the most up-to-date revision. This component appears to have been modified. Retrieve the most up-to-date revision and try again.");
    }

    @Override
    public void verifyRevisions(final Set<Revision> revisions, final NiFiUser user) {
        for (final Revision revision : revisions) {
            verifyRevision(revision, user);
        }
    }

    @Override
    public Set<Revision> getRevisionsFromGroup(final String groupId, final Function<ProcessGroup, Set<String>> getComponents) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        final Set<String> componentIds = getComponents.apply(group);
        return componentIds.stream().map(id -> revisionManager.getRevision(id)).collect(Collectors.toSet());
    }

    @Override
    public Set<Revision> getRevisionsFromSnippet(final String snippetId) {
        final Snippet snippet = snippetDAO.getSnippet(snippetId);
        final Set<String> componentIds = new HashSet<>();
        componentIds.addAll(snippet.getProcessors().keySet());
        componentIds.addAll(snippet.getFunnels().keySet());
        componentIds.addAll(snippet.getLabels().keySet());
        componentIds.addAll(snippet.getConnections().keySet());
        componentIds.addAll(snippet.getInputPorts().keySet());
        componentIds.addAll(snippet.getOutputPorts().keySet());
        componentIds.addAll(snippet.getProcessGroups().keySet());
        componentIds.addAll(snippet.getRemoteProcessGroups().keySet());
        return componentIds.stream().map(id -> revisionManager.getRevision(id)).collect(Collectors.toSet());
    }

    // -----------------------------------------
    // Verification Operations
    // -----------------------------------------

    @Override
    public void verifyListQueue(final String connectionId) {
        connectionDAO.verifyList(connectionId);
    }

    @Override
    public void verifyCreateConnection(final String groupId, final ConnectionDTO connectionDTO) {
        connectionDAO.verifyCreate(groupId, connectionDTO);
    }

    @Override
    public void verifyUpdateConnection(final ConnectionDTO connectionDTO) {
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (connectionDAO.hasConnection(connectionDTO.getId())) {
            connectionDAO.verifyUpdate(connectionDTO);
        } else {
            connectionDAO.verifyCreate(connectionDTO.getParentGroupId(), connectionDTO);
        }
    }

    @Override
    public void verifyDeleteConnection(final String connectionId) {
        connectionDAO.verifyDelete(connectionId);
    }

    @Override
    public void verifyDeleteFunnel(final String funnelId) {
        funnelDAO.verifyDelete(funnelId);
    }

    @Override
    public void verifyUpdateInputPort(final PortDTO inputPortDTO) {
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (inputPortDAO.hasPort(inputPortDTO.getId())) {
            inputPortDAO.verifyUpdate(inputPortDTO);
        }
    }

    @Override
    public void verifyDeleteInputPort(final String inputPortId) {
        inputPortDAO.verifyDelete(inputPortId);
    }

    @Override
    public void verifyUpdateOutputPort(final PortDTO outputPortDTO) {
        // if connection does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (outputPortDAO.hasPort(outputPortDTO.getId())) {
            outputPortDAO.verifyUpdate(outputPortDTO);
        }
    }

    @Override
    public void verifyDeleteOutputPort(final String outputPortId) {
        outputPortDAO.verifyDelete(outputPortId);
    }

    @Override
    public void verifyCreateProcessor(ProcessorDTO processorDTO) {
        processorDAO.verifyCreate(processorDTO);
    }

    @Override
    public void verifyUpdateProcessor(final ProcessorDTO processorDTO) {
        // if group does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (processorDAO.hasProcessor(processorDTO.getId())) {
            processorDAO.verifyUpdate(processorDTO);
        } else {
            verifyCreateProcessor(processorDTO);
        }
    }

    @Override
    public void verifyCanVerifyProcessorConfig(final String processorId) {
        processorDAO.verifyConfigVerification(processorId);
    }

    @Override
    public void verifyCanVerifyControllerServiceConfig(final String controllerServiceId) {
        controllerServiceDAO.verifyConfigVerification(controllerServiceId);
    }

    @Override
    public void verifyCanVerifyReportingTaskConfig(final String reportingTaskId) {
        reportingTaskDAO.verifyConfigVerification(reportingTaskId);
    }

    @Override
    public void verifyCanVerifyFlowAnalysisRuleConfig(String flowAnalysisRuleId) {
        flowAnalysisRuleDAO.verifyConfigVerification(flowAnalysisRuleId);
    }

    @Override
    public void verifyCanVerifyParameterProviderConfig(final String parameterProviderId) {
        parameterProviderDAO.verifyConfigVerification(parameterProviderId);
    }

    @Override
    public void verifyDeleteProcessor(final String processorId) {
        processorDAO.verifyDelete(processorId);
    }

    @Override
    public void verifyScheduleComponents(final String groupId, final ScheduledState state, final Set<String> componentIds) {
        processGroupDAO.verifyScheduleComponents(groupId, state, componentIds);
    }

    @Override
    public void verifyEnableComponents(String processGroupId, ScheduledState state, Set<String> componentIds) {
        processGroupDAO.verifyEnableComponents(processGroupId, state, componentIds);
    }

    @Override
    public void verifyActivateControllerServices(final String groupId, final ControllerServiceState state, final Collection<String> serviceIds) {
        processGroupDAO.verifyActivateControllerServices(state, serviceIds);
    }

    @Override
    public void verifyDeleteProcessGroup(final String groupId) {
        processGroupDAO.verifyDelete(groupId);
    }

    @Override
    public void verifyUpdateRemoteProcessGroups(String processGroupId, boolean shouldTransmit) {
        List<RemoteProcessGroup> allRemoteProcessGroups = processGroupDAO.getProcessGroup(processGroupId).findAllRemoteProcessGroups();

        allRemoteProcessGroups.stream()
            .map(remoteProcessGroup -> {
                final RemoteProcessGroupDTO dto = new RemoteProcessGroupDTO();
                dto.setId(remoteProcessGroup.getIdentifier());
                dto.setTransmitting(shouldTransmit);
                return dto;
            })
            .forEach(this::verifyUpdateRemoteProcessGroup);
    }

    @Override
    public void verifyUpdateRemoteProcessGroup(final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        // if remote group does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (remoteProcessGroupDAO.hasRemoteProcessGroup(remoteProcessGroupDTO.getId())) {
            remoteProcessGroupDAO.verifyUpdate(remoteProcessGroupDTO);
        }
    }

    @Override
    public void verifyUpdateRemoteProcessGroupInputPort(final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        remoteProcessGroupDAO.verifyUpdateInputPort(remoteProcessGroupId, remoteProcessGroupPortDTO);
    }

    @Override
    public void verifyUpdateRemoteProcessGroupOutputPort(final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {
        remoteProcessGroupDAO.verifyUpdateOutputPort(remoteProcessGroupId, remoteProcessGroupPortDTO);
    }

    @Override
    public void verifyDeleteRemoteProcessGroup(final String remoteProcessGroupId) {
        remoteProcessGroupDAO.verifyDelete(remoteProcessGroupId);
    }

    @Override
    public void verifyCreateControllerService(ControllerServiceDTO controllerServiceDTO) {
        controllerServiceDAO.verifyCreate(controllerServiceDTO);
    }

    @Override
    public void verifyUpdateControllerService(final ControllerServiceDTO controllerServiceDTO) {
        // if service does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (controllerServiceDAO.hasControllerService(controllerServiceDTO.getId())) {
            controllerServiceDAO.verifyUpdate(controllerServiceDTO);
        } else {
            verifyCreateControllerService(controllerServiceDTO);
        }
    }

    @Override
    public void verifyUpdateControllerServiceReferencingComponents(final String controllerServiceId, final ScheduledState scheduledState, final ControllerServiceState controllerServiceState) {
        controllerServiceDAO.verifyUpdateReferencingComponents(controllerServiceId, scheduledState, controllerServiceState);
    }

    @Override
    public void verifyDeleteControllerService(final String controllerServiceId) {
        controllerServiceDAO.verifyDelete(controllerServiceId);
    }

    @Override
    public void verifyCreateReportingTask(ReportingTaskDTO reportingTaskDTO) {
        reportingTaskDAO.verifyCreate(reportingTaskDTO);
    }

    @Override
    public void verifyUpdateReportingTask(final ReportingTaskDTO reportingTaskDTO) {
        // if tasks does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (reportingTaskDAO.hasReportingTask(reportingTaskDTO.getId())) {
            reportingTaskDAO.verifyUpdate(reportingTaskDTO);
        } else {
            verifyCreateReportingTask(reportingTaskDTO);
        }
    }

    @Override
    public void verifyDeleteReportingTask(final String reportingTaskId) {
        reportingTaskDAO.verifyDelete(reportingTaskId);
    }

    @Override
    public void verifyCreateFlowAnalysisRule(FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        flowAnalysisRuleDAO.verifyCreate(flowAnalysisRuleDTO);
    }

    @Override
    public void verifyUpdateFlowAnalysisRule(final FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        // if the rule does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (flowAnalysisRuleDAO.hasFlowAnalysisRule(flowAnalysisRuleDTO.getId())) {
            flowAnalysisRuleDAO.verifyUpdate(flowAnalysisRuleDTO);
        } else {
            verifyCreateFlowAnalysisRule(flowAnalysisRuleDTO);
        }
    }

    @Override
    public List<ConfigVerificationResultDTO> performFlowAnalysisRuleConfigVerification(final String flowAnalysisRuleId, final Map<String, String> properties) {
        return flowAnalysisRuleDAO.verifyConfiguration(flowAnalysisRuleId, properties);
    }

    @Override
    public ConfigurationAnalysisEntity analyzeFlowAnalysisRuleConfiguration(final String flowAnalysisRuleId, final Map<String, String> properties) {
        final FlowAnalysisRuleNode taskNode = flowAnalysisRuleDAO.getFlowAnalysisRule(flowAnalysisRuleId);
        final ConfigurationAnalysisEntity configurationAnalysisEntity = analyzeConfiguration(taskNode, properties, null);
        return configurationAnalysisEntity;
    }

    @Override
    public void verifyDeleteFlowAnalysisRule(final String flowAnalysisRuleId) {
        flowAnalysisRuleDAO.verifyDelete(flowAnalysisRuleId);
    }

    @Override
    public void verifyCanClearFlowAnalysisRuleState(String flowAnalysisRuleId) {
        flowAnalysisRuleDAO.verifyClearState(flowAnalysisRuleId);
    }

    @Override
    public FlowAnalysisRuleEntity createFlowAnalysisRule(Revision revision, FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = new StandardRevisionClaim(revision);

        // update revision through revision manager
        final RevisionUpdate<FlowAnalysisRuleDTO> snapshot = revisionManager.updateRevision(claim, user, () -> {
            // create the flow analysis rule
            final FlowAnalysisRuleNode flowAnalysisRule = flowAnalysisRuleDAO.createFlowAnalysisRule(flowAnalysisRuleDTO);

            // save the update
            controllerFacade.save();
            awaitValidationCompletion(flowAnalysisRule);

            final FlowAnalysisRuleDTO dto = dtoFactory.createFlowAnalysisRuleDto(flowAnalysisRule);
            final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
            return new StandardRevisionUpdate<>(dto, lastMod);
        });

        final FlowAnalysisRuleNode flowAnalysisRule = flowAnalysisRuleDAO.getFlowAnalysisRule(flowAnalysisRuleDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(flowAnalysisRule);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(flowAnalysisRule));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(flowAnalysisRule.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createFlowAnalysisRuleEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, bulletinEntities);
    }

    @Override
    public FlowAnalysisRuleEntity getFlowAnalysisRule(String flowAnalysisRuleId) {
        final FlowAnalysisRuleNode flowAnalysisRule = flowAnalysisRuleDAO.getFlowAnalysisRule(flowAnalysisRuleId);
        return createFlowAnalysisRuleEntity(flowAnalysisRule);
    }

    private FlowAnalysisRuleEntity createFlowAnalysisRuleEntity(final FlowAnalysisRuleNode flowAnalysisRule) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(flowAnalysisRule.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(flowAnalysisRule);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(flowAnalysisRule));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(flowAnalysisRule.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createFlowAnalysisRuleEntity(dtoFactory.createFlowAnalysisRuleDto(flowAnalysisRule), revision, permissions, operatePermissions, bulletinEntities);
    }

    @Override
    public PropertyDescriptorDTO getFlowAnalysisRulePropertyDescriptor(String flowAnalysisRuleId, String propertyName, boolean sensitive) {
        final FlowAnalysisRuleNode flowAnalysisRuleNode = flowAnalysisRuleDAO.getFlowAnalysisRule(flowAnalysisRuleId);
        final PropertyDescriptor descriptor = getPropertyDescriptor(flowAnalysisRuleNode, propertyName, sensitive);
        return dtoFactory.createPropertyDescriptorDto(descriptor, null);
    }

    @Override
    public ComponentStateDTO getFlowAnalysisRuleState(String flowAnalysisRuleId) {
        final StateMap clusterState = isClustered() ? flowAnalysisRuleDAO.getState(flowAnalysisRuleId, Scope.CLUSTER) : null;
        final StateMap localState = flowAnalysisRuleDAO.getState(flowAnalysisRuleId, Scope.LOCAL);

        // flow analysis rule will be non null as it was already found when getting the state
        final FlowAnalysisRuleNode flowAnalysisRule = flowAnalysisRuleDAO.getFlowAnalysisRule(flowAnalysisRuleId);
        return dtoFactory.createComponentStateDTO(flowAnalysisRuleId, flowAnalysisRule.getFlowAnalysisRule().getClass(), localState, clusterState);
    }

    @Override
    public void clearFlowAnalysisRuleState(String flowAnalysisRuleId) {
        flowAnalysisRuleDAO.clearState(flowAnalysisRuleId);
    }

    @Override
    public FlowAnalysisRuleEntity updateFlowAnalysisRule(Revision revision, FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        // get the component, ensure we have access to it, and perform the update request
        final FlowAnalysisRuleNode flowAnalysisRule = flowAnalysisRuleDAO.getFlowAnalysisRule(flowAnalysisRuleDTO.getId());
        final RevisionUpdate<FlowAnalysisRuleDTO> snapshot = updateComponent(revision,
                flowAnalysisRule,
                () -> flowAnalysisRuleDAO.updateFlowAnalysisRule(flowAnalysisRuleDTO),
                rt -> {
                    awaitValidationCompletion(rt);
                    return dtoFactory.createFlowAnalysisRuleDto(rt);
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(flowAnalysisRule);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(flowAnalysisRule));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(flowAnalysisRule.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createFlowAnalysisRuleEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, bulletinEntities);
    }

    @Override
    public FlowAnalysisRuleEntity deleteFlowAnalysisRule(Revision revision, String flowAnalysisRuleId) {
        final FlowAnalysisRuleNode flowAnalysisRule = flowAnalysisRuleDAO.getFlowAnalysisRule(flowAnalysisRuleId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(flowAnalysisRule);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(flowAnalysisRule));
        final FlowAnalysisRuleDTO snapshot = deleteComponent(
                revision,
                flowAnalysisRule.getResource(),
                () -> flowAnalysisRuleDAO.deleteFlowAnalysisRule(flowAnalysisRuleId),
                true,
                dtoFactory.createFlowAnalysisRuleDto(flowAnalysisRule));

        return entityFactory.createFlowAnalysisRuleEntity(snapshot, null, permissions, operatePermissions, null);
    }

    @Override
    public void verifyCreateParameterProvider(ParameterProviderDTO parameterProviderDTO) {
        parameterProviderDAO.verifyCreate(parameterProviderDTO);
    }

    @Override
    public void verifyUpdateParameterProvider(final ParameterProviderDTO parameterProviderDTO) {
        // if provider does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (parameterProviderDAO.hasParameterProvider(parameterProviderDTO.getId())) {
            parameterProviderDAO.verifyUpdate(parameterProviderDTO);
        } else {
            verifyCreateParameterProvider(parameterProviderDTO);
        }
    }

    @Override
    public void verifyCanFetchParameters(final String parameterProviderId) {
        parameterProviderDAO.verifyCanFetchParameters(parameterProviderId);
    }

    @Override
    public void verifyDeleteParameterProvider(final String parameterProviderId) {
        parameterProviderDAO.verifyDelete(parameterProviderId);
    }

    @Override
    public void verifyCanApplyParameters(final String parameterProviderId, Collection<ParameterGroupConfiguration> parameterGroupConfigurations) {
        parameterProviderDAO.verifyCanApplyParameters(parameterProviderId, parameterGroupConfigurations);
    }

    // -----------------------------------------
    // Write Operations
    // -----------------------------------------

    @Override
    public AccessPolicyEntity updateAccessPolicy(final Revision revision, final AccessPolicyDTO accessPolicyDTO) {
        final Authorizable authorizable = authorizableLookup.getAccessPolicyById(accessPolicyDTO.getId());
        final RevisionUpdate<AccessPolicyDTO> snapshot = updateComponent(revision,
                authorizable,
                () -> accessPolicyDAO.updateAccessPolicy(accessPolicyDTO),
                accessPolicy -> {
                    final Set<TenantEntity> users = accessPolicy.getUsers().stream().map(mapUserIdToTenantEntity(false)).collect(Collectors.toSet());
                    final Set<TenantEntity> userGroups = accessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity(false)).collect(Collectors.toSet());
                    final ComponentReferenceEntity componentReference = createComponentReferenceEntity(accessPolicy.getResource());
                    return dtoFactory.createAccessPolicyDto(accessPolicy, userGroups, users, componentReference);
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizable);
        return entityFactory.createAccessPolicyEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    @Override
    public UserEntity updateUser(final Revision revision, final UserDTO userDTO) {
        final Authorizable usersAuthorizable = authorizableLookup.getTenant();
        final Set<Group> groups = userGroupDAO.getUserGroupsForUser(userDTO.getId());
        final Set<AccessPolicy> policies = userGroupDAO.getAccessPoliciesForUser(userDTO.getId());
        final RevisionUpdate<UserDTO> snapshot = updateComponent(revision,
                usersAuthorizable,
                () -> userDAO.updateUser(userDTO),
                user -> {
                    final Set<TenantEntity> tenantEntities = groups.stream().map(Group::getIdentifier).map(mapUserGroupIdToTenantEntity(false)).collect(Collectors.toSet());
                    final Set<AccessPolicySummaryEntity> policyEntities = policies.stream().map(this::createAccessPolicySummaryEntity).collect(Collectors.toSet());
                    return dtoFactory.createUserDto(user, tenantEntities, policyEntities);
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(usersAuthorizable);
        return entityFactory.createUserEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    @Override
    public UserGroupEntity updateUserGroup(final Revision revision, final UserGroupDTO userGroupDTO) {
        final Authorizable userGroupsAuthorizable = authorizableLookup.getTenant();
        final Set<AccessPolicy> policies = userGroupDAO.getAccessPoliciesForUserGroup(userGroupDTO.getId());
        final RevisionUpdate<UserGroupDTO> snapshot = updateComponent(revision,
                userGroupsAuthorizable,
                () -> userGroupDAO.updateUserGroup(userGroupDTO),
                userGroup -> {
                    final Set<TenantEntity> tenantEntities = userGroup.getUsers().stream().map(mapUserIdToTenantEntity(false)).collect(Collectors.toSet());
                    final Set<AccessPolicySummaryEntity> policyEntities = policies.stream().map(this::createAccessPolicySummaryEntity).collect(Collectors.toSet());
                    return dtoFactory.createUserGroupDto(userGroup, tenantEntities, policyEntities);
                }
        );

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(userGroupsAuthorizable);
        return entityFactory.createUserGroupEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    @Override
    public ConnectionEntity updateConnection(final Revision revision, final ConnectionDTO connectionDTO) {
        final Connection connectionNode = connectionDAO.getConnection(connectionDTO.getId());

        final RevisionUpdate<ConnectionDTO> snapshot = updateComponent(
                revision,
                connectionNode,
                () -> connectionDAO.updateConnection(connectionDTO),
                connection -> dtoFactory.createConnectionDto(connection));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connectionNode);
        final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionNode.getIdentifier()));
        return entityFactory.createConnectionEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status);
    }

    @Override
    public ProcessorEntity updateProcessor(final Revision revision, final ProcessorDTO processorDTO) {
        // get the component, ensure we have access to it, and perform the update request
        final ProcessorNode processorNode = processorDAO.getProcessor(processorDTO.getId());
        final RevisionUpdate<ProcessorDTO> snapshot = updateComponent(revision,
                processorNode,
                () -> processorDAO.updateProcessor(processorDTO),
                proc -> {
                    awaitValidationCompletion(proc);
                    return dtoFactory.createProcessorDto(proc);
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processorNode);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(processorNode));
        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processorNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processorNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createProcessorEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, status, bulletinEntities);
    }

    @Override
    public List<ConfigVerificationResultDTO> performProcessorConfigVerification(final String processorId, final Map<String, String> properties, final Map<String, String> attributes) {
        return processorDAO.verifyProcessorConfiguration(processorId, properties, attributes);
    }

    @Override
    public ConfigurationAnalysisEntity analyzeProcessorConfiguration(final String processorId, final Map<String, String> properties) {
        final ProcessorNode processorNode = processorDAO.getProcessor(processorId);
        final ProcessGroup processGroup = processorNode.getProcessGroup();
        final ParameterContext parameterContext = processGroup.getParameterContext();

        final ConfigurationAnalysisEntity configurationAnalysisEntity = analyzeConfiguration(processorNode, properties, parameterContext);
        return configurationAnalysisEntity;
    }

    private ConfigurationAnalysisEntity analyzeConfiguration(final ComponentNode componentNode, final Map<String, String> properties, final ParameterContext parameterContext) {
        final Map<String, String> referencedAttributes = determineReferencedAttributes(properties, componentNode, parameterContext);

        final ConfigurationAnalysisDTO dto = new ConfigurationAnalysisDTO();
        dto.setComponentId(componentNode.getIdentifier());
        dto.setProperties(properties);
        dto.setReferencedAttributes(referencedAttributes);
        dto.setSupportsVerification(isVerificationSupported(componentNode));

        final ConfigurationAnalysisEntity entity = new ConfigurationAnalysisEntity();
        entity.setConfigurationAnalysis(dto);
        return entity;
    }

    private boolean isVerificationSupported(final ComponentNode componentNode) {
        if (componentNode instanceof ProcessorNode) {
            return ((ProcessorNode) componentNode).getProcessor() instanceof VerifiableProcessor;
        } else if (componentNode instanceof ControllerServiceNode) {
            return ((ControllerServiceNode) componentNode).getControllerServiceImplementation() instanceof VerifiableControllerService;
        } else if (componentNode instanceof ReportingTaskNode) {
            return ((ReportingTaskNode) componentNode).getReportingTask() instanceof VerifiableReportingTask;
        } else {
            return false;
        }
    }

    private Map<String, String> determineReferencedAttributes(final Map<String, String> properties, final ComponentNode componentNode, final ParameterContext parameterContext) {
        final Map<String, String> mergedProperties = new LinkedHashMap<>();
        componentNode.getRawPropertyValues().forEach((desc, value) -> mergedProperties.put(desc.getName(), value));
        mergedProperties.putAll(properties);

        final Set<String> propertiesNotSupportingEL = new HashSet<>();
        for (final String propertyName : mergedProperties.keySet()) {
            final PropertyDescriptor descriptor = componentNode.getPropertyDescriptor(propertyName);
            final boolean allowsAttributes = descriptor.isExpressionLanguageSupported() || descriptor.getExpressionLanguageScope() == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
            if (descriptor.isSensitive() || !allowsAttributes) {
                propertiesNotSupportingEL.add(propertyName);
            }
        }
        propertiesNotSupportingEL.forEach(mergedProperties::remove);

        final PropertyConfigurationMapper configurationMapper = new PropertyConfigurationMapper();
        final Map<String, PropertyConfiguration> configurationMap = configurationMapper.mapRawPropertyValuesToPropertyConfiguration(componentNode, mergedProperties);

        final Map<String, String> referencedAttributes = new HashMap<>();
        for (final PropertyConfiguration propertyConfiguration : configurationMap.values()) {
            final String effectiveValue = propertyConfiguration.getEffectiveValue(parameterContext == null ? ParameterLookup.EMPTY : parameterContext);
            final Set<String> attributes = Query.prepareWithParametersPreEvaluated(effectiveValue).getExplicitlyReferencedAttributes();
            attributes.forEach(attr -> referencedAttributes.put(attr, null));
        }

        return referencedAttributes;
    }

    private void awaitValidationCompletion(final ComponentNode component) {
        component.getValidationStatus(VALIDATION_WAIT_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Override
    public LabelEntity updateLabel(final Revision revision, final LabelDTO labelDTO) {
        final Label labelNode = labelDAO.getLabel(labelDTO.getId());
        final RevisionUpdate<LabelDTO> snapshot = updateComponent(revision,
                labelNode,
                () -> labelDAO.updateLabel(labelDTO),
                label -> dtoFactory.createLabelDto(label));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(labelNode);
        return entityFactory.createLabelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    @Override
    public FunnelEntity updateFunnel(final Revision revision, final FunnelDTO funnelDTO) {
        final Funnel funnelNode = funnelDAO.getFunnel(funnelDTO.getId());
        final RevisionUpdate<FunnelDTO> snapshot = updateComponent(revision,
                funnelNode,
                () -> funnelDAO.updateFunnel(funnelDTO),
                funnel -> dtoFactory.createFunnelDto(funnel));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(funnelNode);
        return entityFactory.createFunnelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }


    /**
     * Updates a component with the given revision, using the provided supplier to call
     * into the appropriate DAO and the provided function to convert the component into a DTO.
     *
     * @param revision    the current revision
     * @param daoUpdate   a Supplier that will update the component via the appropriate DAO
     * @param dtoCreation a Function to convert a component into a dao
     * @param <D>         the DTO Type of the updated component
     * @param <C>         the Component Type of the updated component
     * @return A RevisionUpdate that represents the new configuration
     */
    private <D, C> RevisionUpdate<D> updateComponent(final Revision revision, final Authorizable authorizable, final Supplier<C> daoUpdate, final Function<C, D> dtoCreation) {
        try {
            final NiFiUser user = NiFiUserUtils.getNiFiUser();

            final RevisionUpdate<D> updatedComponent = revisionManager.updateRevision(new StandardRevisionClaim(revision), user, () -> {
                // get the updated component
                final C component = daoUpdate.get();

                // save updated controller
                controllerFacade.save();

                final D dto = dtoCreation.apply(component);

                final Revision updatedRevision = revisionManager.getRevision(revision.getComponentId()).incrementRevision(revision.getClientId());
                final FlowModification lastModification = new FlowModification(updatedRevision, user.getIdentity());
                return new StandardRevisionUpdate<>(dto, lastModification);
            });

            return updatedComponent;
        } catch (final ExpiredRevisionClaimException erce) {
            throw new InvalidRevisionException("Failed to update component " + authorizable, erce);
        }
    }


    @Override
    public void verifyUpdateSnippet(final SnippetDTO snippetDto, final Set<String> affectedComponentIds) {
        // if snippet does not exist, then the update request is likely creating it
        // so we don't verify since it will fail
        if (snippetDAO.hasSnippet(snippetDto.getId())) {
            snippetDAO.verifyUpdateSnippetComponent(snippetDto);
        }
    }

    @Override
    public SnippetEntity updateSnippet(final Set<Revision> revisions, final SnippetDTO snippetDto) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionClaim revisionClaim = new StandardRevisionClaim(revisions);

        final RevisionUpdate<SnippetDTO> snapshot;
        try {
            snapshot = revisionManager.updateRevision(revisionClaim, user, () -> {
                // get the updated component
                final Snippet snippet = snippetDAO.updateSnippetComponents(snippetDto);

                // drop the snippet
                snippetDAO.dropSnippet(snippet.getId());

                // save updated controller
                controllerFacade.save();

                // increment the revisions
                final Set<Revision> updatedRevisions = revisions.stream().map(revision -> {
                    final Revision currentRevision = revisionManager.getRevision(revision.getComponentId());
                    return currentRevision.incrementRevision(revision.getClientId());
                }).collect(Collectors.toSet());

                final SnippetDTO dto = dtoFactory.createSnippetDto(snippet);
                return new StandardRevisionUpdate<>(dto, null, updatedRevisions);
            });
        } catch (final ExpiredRevisionClaimException e) {
            throw new InvalidRevisionException("Failed to update Snippet", e);
        }

        return entityFactory.createSnippetEntity(snapshot.getComponent());
    }

    @Override
    public PortEntity updateInputPort(final Revision revision, final PortDTO inputPortDTO) {
        final Port inputPortNode = inputPortDAO.getPort(inputPortDTO.getId());
        final RevisionUpdate<PortDTO> snapshot = updateComponent(revision,
                inputPortNode,
                () -> inputPortDAO.updatePort(inputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(inputPortNode);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(inputPortNode));
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(inputPortNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(inputPortNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, status, bulletinEntities);
    }

    @Override
    public PortEntity updateOutputPort(final Revision revision, final PortDTO outputPortDTO) {
        final Port outputPortNode = outputPortDAO.getPort(outputPortDTO.getId());
        final RevisionUpdate<PortDTO> snapshot = updateComponent(revision,
                outputPortNode,
                () -> outputPortDAO.updatePort(outputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(outputPortNode);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(outputPortNode), NiFiUserUtils.getNiFiUser());
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(outputPortNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(outputPortNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, status, bulletinEntities);
    }

    @Override
    public RemoteProcessGroupEntity updateRemoteProcessGroup(final Revision revision, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());
        final RevisionUpdate<RemoteProcessGroupDTO> snapshot = updateComponent(
                revision,
                remoteProcessGroupNode,
                () -> remoteProcessGroupDAO.updateRemoteProcessGroup(remoteProcessGroupDTO),
                remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroupNode);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(remoteProcessGroupNode));
        final RevisionDTO updateRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(remoteProcessGroupNode,
                controllerFacade.getRemoteProcessGroupStatus(remoteProcessGroupNode.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(remoteProcessGroupNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createRemoteProcessGroupEntity(snapshot.getComponent(), updateRevision, permissions, operatePermissions, status, bulletinEntities);
    }

    @Override
    public RemoteProcessGroupPortEntity updateRemoteProcessGroupInputPort(
            final Revision revision, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {

        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupPortDTO.getGroupId());
        final RevisionUpdate<RemoteProcessGroupPortDTO> snapshot = updateComponent(
                revision,
                remoteProcessGroupNode,
                () -> remoteProcessGroupDAO.updateRemoteProcessGroupInputPort(remoteProcessGroupId, remoteProcessGroupPortDTO),
                remoteGroupPort -> dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroupNode);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(remoteProcessGroupNode));
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        return entityFactory.createRemoteProcessGroupPortEntity(snapshot.getComponent(), updatedRevision, permissions, operatePermissions);
    }

    @Override
    public RemoteProcessGroupPortEntity updateRemoteProcessGroupOutputPort(
            final Revision revision, final String remoteProcessGroupId, final RemoteProcessGroupPortDTO remoteProcessGroupPortDTO) {

        final RemoteProcessGroup remoteProcessGroupNode = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupPortDTO.getGroupId());
        final RevisionUpdate<RemoteProcessGroupPortDTO> snapshot = updateComponent(
                revision,
                remoteProcessGroupNode,
                () -> remoteProcessGroupDAO.updateRemoteProcessGroupOutputPort(remoteProcessGroupId, remoteProcessGroupPortDTO),
                remoteGroupPort -> dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroupNode);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(remoteProcessGroupNode));
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        return entityFactory.createRemoteProcessGroupPortEntity(snapshot.getComponent(), updatedRevision, permissions, operatePermissions);
    }

    @Override
    public void verifyCreateParameterContext(final ParameterContextDTO parameterContextDto) {
        parameterContextDAO.verifyCreate(parameterContextDto);
    }

    @Override
    public void verifyUpdateParameterContext(final ParameterContextDTO parameterContext, final boolean verifyComponentStates) {
        parameterContextDAO.verifyUpdate(parameterContext, verifyComponentStates);
    }

    @Override
    public ParameterContextEntity updateParameterContext(final Revision revision, final ParameterContextDTO parameterContextDto) {
        // get the component, ensure we have access to it, and perform the update request
        final ParameterContext parameterContext = parameterContextDAO.getParameterContext(parameterContextDto.getId());
        final RevisionUpdate<ParameterContextDTO> snapshot = updateComponent(revision,
            parameterContext,
            () -> parameterContextDAO.updateParameterContext(parameterContextDto),
            context -> dtoFactory.createParameterContextDto(context, revisionManager, false, parameterContextDAO));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(parameterContext);
        final RevisionDTO revisionDto = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        return entityFactory.createParameterContextEntity(snapshot.getComponent(), revisionDto, permissions);

    }

    @Override
    public ParameterContextEntity getParameterContext(final String parameterContextId, final boolean includeInheritedParameters, final NiFiUser user) {
        final ParameterContext parameterContext = parameterContextDAO.getParameterContext(parameterContextId);
        return createParameterContextEntity(parameterContext, includeInheritedParameters, user, parameterContextDAO);
    }

    @Override
    public Set<ParameterContextEntity> getParameterContexts() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final Set<ParameterContextEntity> entities = parameterContextDAO.getParameterContexts().stream()
            .map(context -> createParameterContextEntity(context, false, user, parameterContextDAO))
            .collect(Collectors.toSet());

        return entities;
    }

    @Override
    public ParameterContext getParameterContextByName(final String parameterContextName, final NiFiUser user) {
        final ParameterContext parameterContext = parameterContextDAO.getParameterContexts().stream()
            .filter(context -> context.getName().equals(parameterContextName))
            .findAny()
            .orElse(null);

        if (parameterContext == null) {
            return null;
        }

        final boolean authorized = parameterContext.isAuthorized(authorizer, RequestAction.READ, user);
        if (!authorized) {
            // Note that we do not call ParameterContext.authorize() because doing so would result in an error message indicating that the user does not have permission
            // to READ Parameter Context with ID ABC123, which tells the user that the Parameter Context ABC123 has the same name as the requested name. Instead, we simply indicate
            // that the user is unable to read the Parameter Context and provide the name, rather than the ID, so that information about which ID corresponds to the given name is not provided.
            throw new AccessDeniedException("Unable to read Parameter Context with name '" + parameterContextName + "'.");
        }

        return parameterContext;
    }

    private ParameterContextEntity createParameterContextEntity(final ParameterContext parameterContext, final boolean includeInheritedParameters, final NiFiUser user,
                                                                final ParameterContextLookup parameterContextLookup) {
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(parameterContext, user);
        final RevisionDTO revisionDto = dtoFactory.createRevisionDTO(revisionManager.getRevision(parameterContext.getIdentifier()));
        final ParameterContextDTO parameterContextDto = dtoFactory.createParameterContextDto(parameterContext, revisionManager, includeInheritedParameters,
                parameterContextLookup);
        final ParameterContextEntity entity = entityFactory.createParameterContextEntity(parameterContextDto, revisionDto, permissions);
        return entity;
    }

    @Override
    public List<ComponentValidationResultEntity> validateComponents(final ParameterContextDTO parameterContextDto, final NiFiUser nifiUser) {
        final ParameterContext parameterContext = parameterContextDAO.getParameterContext(parameterContextDto.getId());
        final Set<ProcessGroup> boundProcessGroups = parameterContext.getParameterReferenceManager().getProcessGroupsBound(parameterContext);
        final ParameterContext updatedParameterContext = new StandardParameterContext.Builder()
                .id(parameterContext.getIdentifier())
                .name(parameterContext.getName())
                .parameterReferenceManager(ParameterReferenceManager.EMPTY)
                .build();
        final Map<String, Parameter> parameters = new HashMap<>();
        parameterContextDto.getParameters().stream()
            .map(ParameterEntity::getParameter)
            .map(this::createParameter)
            .forEach(param -> parameters.put(param.getDescriptor().getName(), param));
        updatedParameterContext.setParameters(parameters);

        final List<ComponentValidationResultEntity> validationResults = new ArrayList<>();
        for (final ProcessGroup processGroup : boundProcessGroups) {
            for (final ProcessorNode processorNode : processGroup.getProcessors()) {
                if (!processorNode.isReferencingParameter()) {
                    continue;
                }

                final ComponentValidationResultEntity componentValidationResultEntity = validateComponent(processorNode, updatedParameterContext, nifiUser);
                validationResults.add(componentValidationResultEntity);
            }

            for (final ControllerServiceNode serviceNode : processGroup.getControllerServices(false)) {
                if (!serviceNode.isReferencingParameter()) {
                    continue;
                }

                final ComponentValidationResultEntity componentValidationResultEntity = validateComponent(serviceNode, updatedParameterContext, nifiUser);
                validationResults.add(componentValidationResultEntity);
            }
        }

        return validationResults;
    }

    private ComponentValidationResultEntity validateComponent(final ComponentNode componentNode, final ParameterContext parameterContext, final NiFiUser user) {
        final ValidationState newState = componentNode.performValidation(componentNode.getProperties(), componentNode.getAnnotationData(), parameterContext);
        final ComponentValidationResultDTO resultDto = dtoFactory.createComponentValidationResultDto(componentNode, newState);

        final Revision revision = revisionManager.getRevision(componentNode.getIdentifier());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(componentNode, user);
        final RevisionDTO revisionDto = dtoFactory.createRevisionDTO(revision);

        final ComponentValidationResultEntity componentValidationResultEntity = entityFactory.createComponentValidationResultEntity(resultDto, revisionDto, permissions);
        return componentValidationResultEntity;
    }

    private Parameter createParameter(final ParameterDTO dto) {
        if (dto.getDescription() == null && dto.getSensitive() == null && dto.getValue() == null && dto.getReferencedAssets() == null) {
            return null; // null description, sensitivity flag, and value indicates a deletion, which we want to represent as a null Parameter.
        }

        final String dtoValue = dto.getValue();
        final List<AssetReferenceDTO> referencedAssets = dto.getReferencedAssets();
        final boolean referencesAsset = referencedAssets != null && !referencedAssets.isEmpty();
        final String parameterContextId = dto.getParameterContext() == null ? null : dto.getParameterContext().getId();

        final String value;
        List<Asset> assets = null;
        if (dtoValue == null && !referencesAsset && Boolean.TRUE.equals(dto.getValueRemoved())) {
            value = null;
        } else if (referencesAsset)  {
            assets = getAssets(referencedAssets);
            value = null;
        } else {
            value = dto.getValue();
        }

        return new Parameter.Builder()
            .name(dto.getName())
            .description(dto.getDescription())
            .sensitive(Boolean.TRUE.equals(dto.getSensitive()))
            .value(value)
            .referencedAssets(assets)
            .parameterContextId(parameterContextId)
            .provided(dto.getProvided())
            .build();
    }

    private List<Asset> getAssets(final List<AssetReferenceDTO> referencedAssets) {
        return Stream.ofNullable(referencedAssets)
                .flatMap(Collection::stream)
                .map(AssetReferenceDTO::getId)
                .map(this::getAsset)
                .collect(Collectors.toList());
    }

    private Asset getAsset(final String assetId) {
        return assetManager.getAsset(assetId).orElseThrow(() -> new ResourceNotFoundException("Unable to find asset with id " + assetId));
    }

    @Override
    public ParameterContextEntity createParameterContext(final Revision revision, final ParameterContextDTO parameterContextDto) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = new StandardRevisionClaim(revision);

        // update revision through revision manager
        final RevisionUpdate<ParameterContextDTO> snapshot = revisionManager.updateRevision(claim, user, () -> {
            // create the parameter context
            final ParameterContext parameterContext = parameterContextDAO.createParameterContext(parameterContextDto);

            // save the update
            controllerFacade.save();

            final ParameterContextDTO dto = dtoFactory.createParameterContextDto(parameterContext, revisionManager, false,
                    parameterContextDAO);
            final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
            return new StandardRevisionUpdate<>(dto, lastMod);
        });

        final ParameterContext parameterContext = parameterContextDAO.getParameterContext(parameterContextDto.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(parameterContext);

        return entityFactory.createParameterContextEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    @Override
    public void verifyDeleteParameterContext(final String parameterContextId) {
        parameterContextDAO.verifyDelete(parameterContextId);
    }

    @Override
    public ParameterContextEntity deleteParameterContext(final Revision revision, final String parameterContextId) {
        final ParameterContext parameterContext = parameterContextDAO.getParameterContext(parameterContextId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(parameterContext);
        final ParameterContextDTO snapshot = deleteComponent(
            revision,
            parameterContext.getResource(),
            () -> parameterContextDAO.deleteParameterContext(parameterContextId),
            true,
            dtoFactory.createParameterContextDto(parameterContext, revisionManager, false, parameterContextDAO));

        return entityFactory.createParameterContextEntity(snapshot, null, permissions);

    }


    @Override
    public Set<AffectedComponentEntity> getProcessorsReferencingParameter(final String groupId) {
        return getComponentsReferencingParameter(groupId, ProcessGroup::getProcessors);
    }

    @Override
    public Set<AffectedComponentEntity> getControllerServicesReferencingParameter(String groupId) {
        return getComponentsReferencingParameter(groupId, group -> group.getControllerServices(false));
    }

    private Set<AffectedComponentEntity> getComponentsReferencingParameter(final String groupId, final Function<ProcessGroup, Collection<? extends ComponentNode>> componentFunction) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        final Set<ComponentNode> affectedComponents = new HashSet<>();

        componentFunction.apply(group).stream()
            .filter(ComponentNode::isReferencingParameter)
            .forEach(affectedComponents::add);

        return dtoFactory.createAffectedComponentEntities(affectedComponents, revisionManager);
    }

    @Override
    public AffectedComponentEntity getUpdatedAffectedComponentEntity(final AffectedComponentEntity affectedComponent) {
        final AffectedComponentDTO dto = affectedComponent.getComponent();
        if (dto == null) {
            return affectedComponent;
        }

        final String groupId = affectedComponent.getProcessGroup().getId();
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);

        final String componentType = dto.getReferenceType();
        if (AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE.equals(componentType)) {
            final ControllerServiceNode serviceNode = processGroup.getControllerService(dto.getId());
            return dtoFactory.createAffectedComponentEntity(serviceNode, revisionManager);
        } else if (AffectedComponentDTO.COMPONENT_TYPE_PROCESSOR.equals(componentType)) {
            final ProcessorNode processorNode = processGroup.getProcessor(dto.getId());
            return dtoFactory.createAffectedComponentEntity(processorNode, revisionManager);
        } else if (AffectedComponentDTO.COMPONENT_TYPE_INPUT_PORT.equals(componentType)) {
            final Port inputPort = processGroup.getInputPort(dto.getId());
            final PortEntity portEntity = createInputPortEntity(inputPort);
            return dtoFactory.createAffectedComponentEntity(portEntity, AffectedComponentDTO.COMPONENT_TYPE_INPUT_PORT);
        } else if (AffectedComponentDTO.COMPONENT_TYPE_OUTPUT_PORT.equals(componentType)) {
            final Port outputPort = processGroup.getOutputPort(dto.getId());
            final PortEntity portEntity = createOutputPortEntity(outputPort);
            return dtoFactory.createAffectedComponentEntity(portEntity, AffectedComponentDTO.COMPONENT_TYPE_OUTPUT_PORT);
        } else if (AffectedComponentDTO.COMPONENT_TYPE_REMOTE_INPUT_PORT.equals(componentType)) {
            final RemoteGroupPort remoteGroupPort = processGroup.findRemoteGroupPort(dto.getId());
            final RemoteProcessGroupEntity rpgEntity = createRemoteGroupEntity(remoteGroupPort.getRemoteProcessGroup(), NiFiUserUtils.getNiFiUser());
            final RemoteProcessGroupPortDTO remotePortDto = dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort);
            return dtoFactory.createAffectedComponentEntity(remotePortDto, AffectedComponentDTO.COMPONENT_TYPE_REMOTE_INPUT_PORT, rpgEntity);
        } else if (AffectedComponentDTO.COMPONENT_TYPE_REMOTE_OUTPUT_PORT.equals(componentType)) {
            final RemoteGroupPort remoteGroupPort = processGroup.findRemoteGroupPort(dto.getId());
            final RemoteProcessGroupEntity rpgEntity = createRemoteGroupEntity(remoteGroupPort.getRemoteProcessGroup(), NiFiUserUtils.getNiFiUser());
            final RemoteProcessGroupPortDTO remotePortDto = dtoFactory.createRemoteProcessGroupPortDto(remoteGroupPort);
            return dtoFactory.createAffectedComponentEntity(remotePortDto, AffectedComponentDTO.COMPONENT_TYPE_REMOTE_OUTPUT_PORT, rpgEntity);
        } else if (AffectedComponentDTO.COMPONENT_TYPE_STATELESS_GROUP.equals(componentType)) {
            final ProcessGroup statelessGroup = processGroupDAO.getProcessGroup(groupId);
            final ProcessGroupEntity statelessGroupEntity = createProcessGroupEntity(statelessGroup);
            return dtoFactory.createAffectedComponentEntity(statelessGroupEntity);
        }

        return affectedComponent;
    }

    @Override
    public Set<AffectedComponentEntity> getComponentsAffectedByParameterContextUpdate(final Collection<ParameterContextDTO> parameterContextDTOs) {
        final Set<AffectedComponentEntity> allAffectedComponents = new HashSet<>();
        for (final ParameterContextDTO parameterContextDTO : parameterContextDTOs) {
            allAffectedComponents.addAll(getComponentsAffectedByParameterContextUpdate(parameterContextDTO, true));
        }
        return allAffectedComponents;
    }

    private boolean isGroupAffectedByParameterContext(final ProcessGroup group, final String contextId) {
        if (group == null) {
            return false;
        }

        final ParameterContext context = group.getParameterContext();
        if (context == null) {
            return false;
        }

        if (context.getIdentifier().equals(contextId)) {
            return true;
        }

        return context.inheritsFrom(contextId);
    }

    private ProcessGroup getStatelessParent(final ProcessGroup group) {
        if (group == null) {
            return null;
        }

        final ExecutionEngine engine = group.getExecutionEngine();
        if (engine == ExecutionEngine.STATELESS) {
            return group;
        }

        return getStatelessParent(group.getParent());
    }

    private Set<AffectedComponentEntity> getComponentsAffectedByParameterContextUpdate(final ParameterContextDTO parameterContextDto, final boolean includeInactive) {
        final ProcessGroup rootGroup = processGroupDAO.getProcessGroup("root");
        final List<ProcessGroup> groupsReferencingParameterContext = rootGroup.findAllProcessGroups(
            group -> isGroupAffectedByParameterContext(group, parameterContextDto.getId()));

        setEffectiveParameterUpdates(parameterContextDto);

        final Set<String> updatedParameterNames = getUpdatedParameterNames(parameterContextDto);

        // Clear set of Affected Components for each Parameter. This parameter is read-only and it will be populated below.
        for (final ParameterEntity parameterEntity : parameterContextDto.getParameters()) {
            parameterEntity.getParameter().setReferencingComponents(new HashSet<>());
        }

        final Set<ComponentNode> affectedComponents = new HashSet<>();
        final Set<ProcessGroup> affectedStatelessGroups = new HashSet<>();

        for (final ProcessGroup group : groupsReferencingParameterContext) {
            final ProcessGroup statelessParent = getStatelessParent(group);
            if (statelessParent != null) {
                if (includeInactive || statelessParent.isStatelessActive()) {
                    affectedStatelessGroups.add(statelessParent);
                    continue;
                }
            }

            for (final ProcessorNode processor : group.getProcessors()) {
                if (includeInactive || processor.isRunning()) {
                    final Set<String> referencedParams = processor.getReferencedParameterNames();
                    final boolean referencesUpdatedParam = referencedParams.stream().anyMatch(updatedParameterNames::contains);

                    if (referencesUpdatedParam) {
                        affectedComponents.add(processor);

                        final AffectedComponentEntity affectedComponentEntity = dtoFactory.createAffectedComponentEntity(processor, revisionManager);

                        for (final String referencedParam : referencedParams) {
                            for (final ParameterEntity paramEntity : parameterContextDto.getParameters()) {
                                final ParameterDTO paramDto = paramEntity.getParameter();
                                if (referencedParam.equals(paramDto.getName())) {
                                    paramDto.getReferencingComponents().add(affectedComponentEntity);
                                }
                            }
                        }
                    }
                }
            }

            for (final ControllerServiceNode service : group.getControllerServices(false)) {
                if (includeInactive || service.isActive()) {
                    final Set<String> referencedParams = service.getReferencedParameterNames();
                    final Set<String> updatedReferencedParams = referencedParams.stream().filter(updatedParameterNames::contains).collect(Collectors.toSet());

                    final List<ParameterDTO> affectedParameterDtos = new ArrayList<>();
                    for (final String referencedParam : referencedParams) {
                        for (final ParameterEntity paramEntity : parameterContextDto.getParameters()) {
                            final ParameterDTO paramDto = paramEntity.getParameter();
                            if (referencedParam.equals(paramDto.getName())) {
                                affectedParameterDtos.add(paramDto);
                            }
                        }
                    }

                    if (!updatedReferencedParams.isEmpty()) {
                        addReferencingComponents(service, affectedComponents, affectedParameterDtos, includeInactive);
                    }
                }
            }
        }

        // Create AffectedComponentEntity for each affected component
        final Set<AffectedComponentEntity> affectedComponentEntities = new HashSet<>();

        final Set<AffectedComponentEntity> individualComponents = dtoFactory.createAffectedComponentEntities(affectedComponents, revisionManager);
        affectedComponentEntities.addAll(individualComponents);

        for (final ProcessGroup group : affectedStatelessGroups) {
            final ProcessGroupEntity groupEntity = createProcessGroupEntity(group);
            final AffectedComponentEntity affectedComponentEntity = dtoFactory.createAffectedComponentEntity(groupEntity);
            affectedComponentEntities.add(affectedComponentEntity);
        }

        return affectedComponentEntities;
    }

    private void setEffectiveParameterUpdates(final ParameterContextDTO parameterContextDto) {
        final ParameterContext parameterContext = parameterContextDAO.getParameterContext(parameterContextDto.getId());

        final Map<String, Parameter> parameterUpdates = parameterContextDAO.getParameters(parameterContextDto, parameterContext);
        final List<ParameterContext> inheritedParameterContexts = parameterContextDAO.getInheritedParameterContexts(parameterContextDto);
        final Map<String, Parameter> proposedParameterUpdates = parameterContext.getEffectiveParameterUpdates(parameterUpdates, inheritedParameterContexts);
        final Map<String, ParameterEntity> parameterEntities = parameterContextDto.getParameters().stream()
                .collect(Collectors.toMap(entity -> entity.getParameter().getName(), Function.identity()));
        parameterContextDto.getParameters().clear();

        for (final Map.Entry<String, Parameter> entry : proposedParameterUpdates.entrySet()) {
            final String parameterName = entry.getKey();
            final Parameter parameter = entry.getValue();
            final ParameterEntity parameterEntity;
            if (parameterEntities.containsKey(parameterName)) {
                parameterEntity = parameterEntities.get(parameterName);
            } else if (parameter == null) {
                parameterEntity = new ParameterEntity();
                final ParameterDTO parameterDTO = new ParameterDTO();
                parameterDTO.setName(parameterName);
                parameterEntity.setParameter(parameterDTO);
            } else {
                parameterEntity = dtoFactory.createParameterEntity(parameterContext, parameter, revisionManager, parameterContextDAO);
            }

            // Parameter is inherited if either this is the removal of a parameter not directly in this context, or it's parameter not specified directly in the DTO
            final boolean isInherited = (parameter == null && !parameterContext.getParameters().containsKey(new ParameterDescriptor.Builder().name(parameterName).build()))
                    || (parameter != null && !parameterEntities.containsKey(parameterName));
            parameterEntity.getParameter().setInherited(isInherited);
            parameterContextDto.getParameters().add(parameterEntity);
        }
    }

    private void addReferencingComponents(final ControllerServiceNode service, final Set<ComponentNode> affectedComponents, final List<ParameterDTO> affectedParameterDtos,
                                          final boolean includeInactive) {

        // We keep a mapping of Affected Components for the Parameter Context Update as well as a set of all Affected Components for each updated Parameter.
        // We must update both of these.
        affectedComponents.add(service);

        // Update Parameter DTO to also reflect the Affected Component.
        final AffectedComponentEntity affectedComponentEntity = dtoFactory.createAffectedComponentEntity(service, revisionManager);
        affectedParameterDtos.forEach(dto -> dto.getReferencingComponents().add(affectedComponentEntity));

        for (final ComponentNode referencingComponent : service.getReferences().getReferencingComponents()) {
            if (includeInactive || isActive(referencingComponent)) {
                // We must update both the Set of Affected Components as well as the Affected Components for the referenced parameter.
                affectedComponents.add(referencingComponent);

                final AffectedComponentEntity referencingComponentEntity = dtoFactory.createAffectedComponentEntity(referencingComponent, revisionManager);
                affectedParameterDtos.forEach(dto -> dto.getReferencingComponents().add(referencingComponentEntity));

                if (referencingComponent instanceof ControllerServiceNode) {
                    addReferencingComponents((ControllerServiceNode) referencingComponent, affectedComponents, affectedParameterDtos, includeInactive);
                }
            }
        }
    }

    private boolean isActive(final ComponentNode componentNode) {
        if (componentNode instanceof ControllerServiceNode) {
            return ((ControllerServiceNode) componentNode).isActive();
        }

        if (componentNode instanceof ProcessorNode) {
            return ((ProcessorNode) componentNode).isRunning();
        }

        return false;
    }

    private Set<String> getUpdatedParameterNames(final ParameterContextDTO parameterContextDto) {
        final ParameterContext parameterContext = parameterContextDAO.getParameterContext(parameterContextDto.getId());

        final Set<String> updatedParameters = new HashSet<>();
        for (final ParameterEntity parameterEntity : parameterContextDto.getParameters()) {
            final ParameterDTO parameterDto = parameterEntity.getParameter();
            final String updatedValue = parameterDto.getValue();
            final String parameterName = parameterDto.getName();

            final Optional<Parameter> parameterOption = parameterContext.getParameter(parameterName);
            if (!parameterOption.isPresent()) {
                updatedParameters.add(parameterName);
                continue;
            }

            final Parameter parameter = parameterOption.get();
            final boolean valueUpdated = !Objects.equals(updatedValue, parameter.getValue());
            // Sensitivity can be updated for provided parameters only
            final boolean sensitivityUpdated = parameterDto.getSensitive() != null && parameterDto.getSensitive() != parameter.getDescriptor().isSensitive();
            final boolean descriptionUpdated = parameterDto.getDescription() != null && !parameterDto.getDescription().equals(parameter.getDescriptor().getDescription());
            final boolean updated = valueUpdated || descriptionUpdated || sensitivityUpdated;
            if (updated) {
                updatedParameters.add(parameterName);
            }
        }

        return updatedParameters;
    }


    @Override
    public ProcessGroupEntity updateProcessGroup(final Revision revision, final ProcessGroupDTO processGroupDTO) {
        final ProcessGroup processGroupNode = processGroupDAO.getProcessGroup(processGroupDTO.getId());
        final RevisionUpdate<ProcessGroupDTO> snapshot = updateComponent(revision,
                processGroupNode,
                () -> processGroupDAO.updateProcessGroup(processGroupDTO),
                processGroup -> dtoFactory.createProcessGroupDto(processGroup));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroupNode);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());
        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(processGroupNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = getProcessGroupBulletins(processGroupNode);
        return entityFactory.createProcessGroupEntity(snapshot.getComponent(), updatedRevision, permissions, status, bulletinEntities);
    }

    @Override
    public void verifyUpdateProcessGroup(ProcessGroupDTO processGroupDTO) {
        if (processGroupDAO.hasProcessGroup(processGroupDTO.getId())) {
            processGroupDAO.verifyUpdate(processGroupDTO);
        }
    }

    @Override
    public ScheduleComponentsEntity enableComponents(String processGroupId, ScheduledState state, Map<String, Revision> componentRevisions) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final RevisionUpdate<ScheduleComponentsEntity> updatedComponent = revisionManager.updateRevision(new StandardRevisionClaim(componentRevisions.values()), user, () -> {
            // schedule the components
            processGroupDAO.enableComponents(processGroupId, state, componentRevisions.keySet());

            // update the revisions
            final Map<String, Revision> updatedRevisions = new HashMap<>();
            for (final Revision revision : componentRevisions.values()) {
                final Revision currentRevision = revisionManager.getRevision(revision.getComponentId());
                updatedRevisions.put(revision.getComponentId(), currentRevision.incrementRevision(revision.getClientId()));
            }

            // save
            controllerFacade.save();

            // gather details for response
            final ScheduleComponentsEntity entity = new ScheduleComponentsEntity();
            entity.setId(processGroupId);
            entity.setState(state.name());
            return new StandardRevisionUpdate<>(entity, null, new HashSet<>(updatedRevisions.values()));
        });

        return updatedComponent.getComponent();
    }

    @Override
    public ScheduleComponentsEntity scheduleComponents(final String processGroupId, final ScheduledState state, final Map<String, Revision> componentRevisions) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionUpdate<ScheduleComponentsEntity> updatedComponent = revisionManager.updateRevision(new StandardRevisionClaim(componentRevisions.values()), user, () -> {
            // schedule the components
            processGroupDAO.scheduleComponents(processGroupId, state, componentRevisions.keySet());

            // update the revisions
            final Map<String, Revision> updatedRevisions = new HashMap<>();
            for (final Revision revision : componentRevisions.values()) {
                final Revision currentRevision = revisionManager.getRevision(revision.getComponentId());
                updatedRevisions.put(revision.getComponentId(), currentRevision.incrementRevision(revision.getClientId()));
            }

            // save
            controllerFacade.save();

            // gather details for response
            final ScheduleComponentsEntity entity = new ScheduleComponentsEntity();
            entity.setId(processGroupId);
            entity.setState(state.name());
            return new StandardRevisionUpdate<>(entity, null, new HashSet<>(updatedRevisions.values()));
        });

        return updatedComponent.getComponent();
    }

    @Override
    public ActivateControllerServicesEntity activateControllerServices(final String processGroupId, final ControllerServiceState state, final Map<String, Revision> serviceRevisions) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionUpdate<ActivateControllerServicesEntity> updatedComponent = revisionManager.updateRevision(new StandardRevisionClaim(serviceRevisions.values()), user,
                () -> {
                    // schedule the components
                    processGroupDAO.activateControllerServices(processGroupId, state, serviceRevisions.keySet());

                    // update the revisions
                    final Map<String, Revision> updatedRevisions = new HashMap<>();
                    for (final Revision revision : serviceRevisions.values()) {
                        final Revision currentRevision = revisionManager.getRevision(revision.getComponentId());
                        updatedRevisions.put(revision.getComponentId(), currentRevision.incrementRevision(revision.getClientId()));
                    }

                    // save
                    controllerFacade.save();

                    // gather details for response
                    final ActivateControllerServicesEntity entity = new ActivateControllerServicesEntity();
                    entity.setId(processGroupId);
                    entity.setState(state.name());
                    return new StandardRevisionUpdate<>(entity, null, new HashSet<>(updatedRevisions.values()));
                });

        return updatedComponent.getComponent();
    }


    @Override
    public ControllerConfigurationEntity updateControllerConfiguration(final Revision revision, final ControllerConfigurationDTO controllerConfigurationDTO) {
        final RevisionUpdate<ControllerConfigurationDTO> updatedComponent = updateComponent(
                revision,
                controllerFacade,
                () -> {
                    if (controllerConfigurationDTO.getMaxTimerDrivenThreadCount() != null) {
                        controllerFacade.setMaxTimerDrivenThreadCount(controllerConfigurationDTO.getMaxTimerDrivenThreadCount());
                    }

                    return controllerConfigurationDTO;
                },
                controller -> dtoFactory.createControllerConfigurationDto(controllerFacade));

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerFacade);
        final RevisionDTO updateRevision = dtoFactory.createRevisionDTO(updatedComponent.getLastModification());
        return entityFactory.createControllerConfigurationEntity(updatedComponent.getComponent(), updateRevision, permissions);
    }


    @Override
    public NodeDTO updateNode(final NodeDTO nodeDTO) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }
        final String userDn = user.getIdentity();

        final NodeIdentifier nodeId = clusterCoordinator.getNodeIdentifier(nodeDTO.getNodeId());
        if (nodeId == null) {
            throw new UnknownNodeException("No node exists with ID " + nodeDTO.getNodeId());
        }


        if (NodeConnectionState.CONNECTING.name().equalsIgnoreCase(nodeDTO.getStatus())) {
            clusterCoordinator.requestNodeConnect(nodeId, userDn);
        } else if (NodeConnectionState.OFFLOADING.name().equalsIgnoreCase(nodeDTO.getStatus())) {
            clusterCoordinator.requestNodeOffload(nodeId, OffloadCode.OFFLOADED,
                    "User " + userDn + " requested that node be offloaded");
        } else if (NodeConnectionState.DISCONNECTING.name().equalsIgnoreCase(nodeDTO.getStatus())) {
            clusterCoordinator.requestNodeDisconnect(nodeId, DisconnectionCode.USER_DISCONNECTED,
                    "User " + userDn + " requested that node be disconnected from cluster");
        }

        return getNode(nodeId);
    }

    @Override
    public CounterDTO updateCounter(final String counterId) {
        return dtoFactory.createCounterDto(controllerFacade.resetCounter(counterId));
    }

    @Override
    public void verifyCanClearProcessorState(final String processorId) {
        processorDAO.verifyClearState(processorId);
    }

    @Override
    public void clearProcessorState(final String processorId) {
        processorDAO.clearState(processorId);
    }

    @Override
    public void verifyCanClearControllerServiceState(final String controllerServiceId) {
        controllerServiceDAO.verifyClearState(controllerServiceId);
    }

    @Override
    public void clearControllerServiceState(final String controllerServiceId) {
        controllerServiceDAO.clearState(controllerServiceId);
    }

    @Override
    public void verifyCanClearReportingTaskState(final String reportingTaskId) {
        reportingTaskDAO.verifyClearState(reportingTaskId);
    }

    @Override
    public void clearReportingTaskState(final String reportingTaskId) {
        reportingTaskDAO.clearState(reportingTaskId);
    }

    @Override
    public void verifyCanClearParameterProviderState(final String parameterProviderId) {
        parameterProviderDAO.verifyClearState(parameterProviderId);
    }

    @Override
    public void clearParameterProviderState(final String parameterProviderId) {
        parameterProviderDAO.clearState(parameterProviderId);
    }

    @Override
    public ComponentStateDTO getRemoteProcessGroupState(String remoteProcessGroupId) {
        final StateMap clusterState = isClustered() ? remoteProcessGroupDAO.getState(remoteProcessGroupId, Scope.CLUSTER) : null;
        final StateMap localState = remoteProcessGroupDAO.getState(remoteProcessGroupId, Scope.LOCAL);

        // processor will be non null as it was already found when getting the state
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        return dtoFactory.createComponentStateDTO(remoteProcessGroupId, remoteProcessGroup.getClass(), localState, clusterState);
    }

    @Override
    public ConnectionEntity deleteConnection(final Revision revision, final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connection);
        final ConnectionDTO snapshot = deleteComponent(
                revision,
                connection.getResource(),
                () -> connectionDAO.deleteConnection(connectionId),
                false, // no policies to remove
                dtoFactory.createConnectionDto(connection));

        return entityFactory.createConnectionEntity(snapshot, null, permissions, null);
    }

    @Override
    public DropRequestDTO deleteFlowFileDropRequest(final String connectionId, final String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.deleteFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO deleteFlowFileListingRequest(final String connectionId, final String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.deleteFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        if (connection.getSource() != null) {
            listRequest.setSourceRunning(connection.getSource().isRunning());
        }
        if (connection.getDestination() != null) {
            listRequest.setDestinationRunning(connection.getDestination().isRunning());
        }

        return listRequest;
    }

    @Override
    public ProcessorEntity deleteProcessor(final Revision revision, final String processorId) {
        final ProcessorNode processor = processorDAO.getProcessor(processorId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processor);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(processor));
        final ProcessorDTO snapshot = deleteComponent(
                revision,
                processor.getResource(),
                () -> processorDAO.deleteProcessor(processorId),
                true,
                dtoFactory.createProcessorDto(processor));

        return entityFactory.createProcessorEntity(snapshot, null, permissions, operatePermissions, null, null);
    }

    @Override
    public ProcessorEntity terminateProcessor(final String processorId) {
        processorDAO.terminate(processorId);
        return getProcessor(processorId);
    }

    @Override
    public void verifyTerminateProcessor(final String processorId) {
        processorDAO.verifyTerminate(processorId);
    }

    @Override
    public LabelEntity deleteLabel(final Revision revision, final String labelId) {
        final Label label = labelDAO.getLabel(labelId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(label);
        final LabelDTO snapshot = deleteComponent(
                revision,
                label.getResource(),
                () -> labelDAO.deleteLabel(labelId),
                true,
                dtoFactory.createLabelDto(label));

        return entityFactory.createLabelEntity(snapshot, null, permissions);
    }

    @Override
    public UserEntity deleteUser(final Revision revision, final String userId) {
        final User user = userDAO.getUser(userId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        final Set<TenantEntity> userGroups = user != null ? userGroupDAO.getUserGroupsForUser(userId).stream()
                .map(g -> g.getIdentifier()).map(mapUserGroupIdToTenantEntity(false)).collect(Collectors.toSet()) : null;
        final Set<AccessPolicySummaryEntity> policyEntities = user != null ? userGroupDAO.getAccessPoliciesForUser(userId).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet()) : null;

        final String resourceIdentifier = ResourceFactory.getTenantResource().getIdentifier() + "/" + userId;
        final UserDTO snapshot = deleteComponent(
                revision,
                new Resource() {
                    @Override
                    public String getIdentifier() {
                        return resourceIdentifier;
                    }

                    @Override
                    public String getName() {
                        return resourceIdentifier;
                    }

                    @Override
                    public String getSafeDescription() {
                        return "User " + userId;
                    }
                },
                () -> userDAO.deleteUser(userId),
                false, // no user specific policies to remove
                dtoFactory.createUserDto(user, userGroups, policyEntities));

        return entityFactory.createUserEntity(snapshot, null, permissions);
    }

    @Override
    public UserGroupEntity deleteUserGroup(final Revision revision, final String userGroupId) {
        final Group userGroup = userGroupDAO.getUserGroup(userGroupId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        final Set<TenantEntity> users = userGroup != null ? userGroup.getUsers().stream()
                .map(mapUserIdToTenantEntity(false)).collect(Collectors.toSet()) : null;
        final Set<AccessPolicySummaryEntity> policyEntities = userGroupDAO.getAccessPoliciesForUserGroup(userGroup.getIdentifier()).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());

        final String resourceIdentifier = ResourceFactory.getTenantResource().getIdentifier() + "/" + userGroupId;
        final UserGroupDTO snapshot = deleteComponent(
                revision,
                new Resource() {
                    @Override
                    public String getIdentifier() {
                        return resourceIdentifier;
                    }

                    @Override
                    public String getName() {
                        return resourceIdentifier;
                    }

                    @Override
                    public String getSafeDescription() {
                        return "User Group " + userGroupId;
                    }
                },
                () -> userGroupDAO.deleteUserGroup(userGroupId),
                false, // no user group specific policies to remove
                dtoFactory.createUserGroupDto(userGroup, users, policyEntities));

        return entityFactory.createUserGroupEntity(snapshot, null, permissions);
    }

    @Override
    public AccessPolicyEntity deleteAccessPolicy(final Revision revision, final String accessPolicyId) {
        final AccessPolicy accessPolicy = accessPolicyDAO.getAccessPolicy(accessPolicyId);
        final ComponentReferenceEntity componentReference = createComponentReferenceEntity(accessPolicy.getResource());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getAccessPolicyById(accessPolicyId));
        final Set<TenantEntity> userGroups = accessPolicy != null ? accessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity(false)).collect(Collectors.toSet()) : null;
        final Set<TenantEntity> users = accessPolicy != null ? accessPolicy.getUsers().stream().map(mapUserIdToTenantEntity(false)).collect(Collectors.toSet()) : null;
        final AccessPolicyDTO snapshot = deleteComponent(
                revision,
                new Resource() {
                    @Override
                    public String getIdentifier() {
                        return accessPolicy.getResource();
                    }

                    @Override
                    public String getName() {
                        return accessPolicy.getResource();
                    }

                    @Override
                    public String getSafeDescription() {
                        return "Policy " + accessPolicyId;
                    }
                },
                () -> accessPolicyDAO.deleteAccessPolicy(accessPolicyId),
                false, // no need to clean up any policies as it's already been removed above
                dtoFactory.createAccessPolicyDto(accessPolicy, userGroups, users, componentReference));

        return entityFactory.createAccessPolicyEntity(snapshot, null, permissions);
    }

    @Override
    public FunnelEntity deleteFunnel(final Revision revision, final String funnelId) {
        final Funnel funnel = funnelDAO.getFunnel(funnelId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(funnel);
        final FunnelDTO snapshot = deleteComponent(
                revision,
                funnel.getResource(),
                () -> funnelDAO.deleteFunnel(funnelId),
                true,
                dtoFactory.createFunnelDto(funnel));

        return entityFactory.createFunnelEntity(snapshot, null, permissions);
    }

    /**
     * Deletes a component using the Optimistic Locking Manager
     *
     * @param revision     the current revision
     * @param resource the resource being removed
     * @param deleteAction the action that deletes the component via the appropriate DAO object
     * @param cleanUpPolicies whether or not the policies for this resource should be removed as well - not necessary when there are
     *                        no component specific policies or if the policies of the component are inherited
     * @return a dto that represents the new configuration
     */
    private <D, C> D deleteComponent(final Revision revision, final Resource resource, final Runnable deleteAction, final boolean cleanUpPolicies, final D dto) {
        final RevisionClaim claim = new StandardRevisionClaim(revision);
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        return revisionManager.deleteRevision(claim, user, () -> {
            logger.debug("Attempting to delete component {} with claim {}", resource.getIdentifier(), claim);

            // run the delete action
            deleteAction.run();

            // save the flow
            controllerFacade.save();
            logger.debug("Deletion of component {} was successful", resource.getIdentifier());

            if (cleanUpPolicies) {
                cleanUpPolicies(resource);
            }

            return dto;
        });
    }

    /**
     * Clean up the policies for the specified component resource.
     *
     * @param componentResource the resource for the component
     */
    private void cleanUpPolicies(final Resource componentResource) {
        // ensure the authorizer supports configuration
        if (accessPolicyDAO.supportsConfigurableAuthorizer()) {
            final List<Resource> resources = new ArrayList<>();
            resources.add(componentResource);
            resources.add(ResourceFactory.getDataResource(componentResource));
            resources.add(ResourceFactory.getProvenanceDataResource(componentResource));
            resources.add(ResourceFactory.getDataTransferResource(componentResource));
            resources.add(ResourceFactory.getPolicyResource(componentResource));
            resources.add(ResourceFactory.getOperationResource(componentResource));

            for (final Resource resource : resources) {
                for (final RequestAction action : RequestAction.values()) {
                    try {
                        // since the component is being deleted, also delete any relevant access policies
                        final AccessPolicy readPolicy = accessPolicyDAO.getAccessPolicy(action, resource.getIdentifier());
                        if (readPolicy != null) {
                            accessPolicyDAO.deleteAccessPolicy(readPolicy.getIdentifier());
                        }
                    } catch (final Exception e) {
                        logger.warn("Unable to remove access policy for {} {} after component removal.", action, resource.getIdentifier(), e);
                    }
                }
            }
        }
    }

    @Override
    public void verifyDeleteSnippet(final String snippetId, final Set<String> affectedComponentIds) {
        snippetDAO.verifyDeleteSnippetComponents(snippetId);
    }

    @Override
    public SnippetEntity deleteSnippet(final Set<Revision> revisions, final String snippetId) {
        final Snippet snippet = snippetDAO.getSnippet(snippetId);

        // grab the resources in the snippet so we can delete the policies afterwards
        final Set<Resource> snippetResources = new HashSet<>();
        snippet.getProcessors().keySet().forEach(id -> snippetResources.add(processorDAO.getProcessor(id).getResource()));
        snippet.getInputPorts().keySet().forEach(id -> snippetResources.add(inputPortDAO.getPort(id).getResource()));
        snippet.getOutputPorts().keySet().forEach(id -> snippetResources.add(outputPortDAO.getPort(id).getResource()));
        snippet.getFunnels().keySet().forEach(id -> snippetResources.add(funnelDAO.getFunnel(id).getResource()));
        snippet.getLabels().keySet().forEach(id -> snippetResources.add(labelDAO.getLabel(id).getResource()));
        snippet.getRemoteProcessGroups().keySet().forEach(id -> snippetResources.add(remoteProcessGroupDAO.getRemoteProcessGroup(id).getResource()));
        snippet.getProcessGroups().keySet().forEach(id -> {
            final ProcessGroup processGroup = processGroupDAO.getProcessGroup(id);

            // add the process group
            snippetResources.add(processGroup.getResource());

            // add each encapsulated component
            processGroup.findAllProcessors().forEach(processor -> snippetResources.add(processor.getResource()));
            processGroup.findAllInputPorts().forEach(inputPort -> snippetResources.add(inputPort.getResource()));
            processGroup.findAllOutputPorts().forEach(outputPort -> snippetResources.add(outputPort.getResource()));
            processGroup.findAllFunnels().forEach(funnel -> snippetResources.add(funnel.getResource()));
            processGroup.findAllLabels().forEach(label -> snippetResources.add(label.getResource()));
            processGroup.findAllProcessGroups().forEach(childGroup -> snippetResources.add(childGroup.getResource()));
            processGroup.findAllRemoteProcessGroups().forEach(remoteProcessGroup -> snippetResources.add(remoteProcessGroup.getResource()));
            processGroup.findAllControllerServices().forEach(controllerService -> snippetResources.add(controllerService.getResource()));
        });

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionClaim claim = new StandardRevisionClaim(revisions);
        final SnippetDTO dto = revisionManager.deleteRevision(claim, user, () -> {
            // delete the components in the snippet
            snippetDAO.deleteSnippetComponents(snippetId);

            // drop the snippet
            snippetDAO.dropSnippet(snippetId);

            // save
            controllerFacade.save();

            // create the dto for the snippet that was just removed
            return dtoFactory.createSnippetDto(snippet);
        });

        // clean up component policies
        snippetResources.forEach(resource -> cleanUpPolicies(resource));

        return entityFactory.createSnippetEntity(dto);
    }

    @Override
    public PortEntity deleteInputPort(final Revision revision, final String inputPortId) {
        final Port port = inputPortDAO.getPort(inputPortId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(port));
        final PortDTO snapshot = deleteComponent(
                revision,
                port.getResource(),
                () -> inputPortDAO.deletePort(inputPortId),
                true,
                dtoFactory.createPortDto(port));

        return entityFactory.createPortEntity(snapshot, null, permissions, operatePermissions, null, null);
    }

    @Override
    public PortEntity deleteOutputPort(final Revision revision, final String outputPortId) {
        final Port port = outputPortDAO.getPort(outputPortId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(port));
        final PortDTO snapshot = deleteComponent(
                revision,
                port.getResource(),
                () -> outputPortDAO.deletePort(outputPortId),
                true,
                dtoFactory.createPortDto(port));

        return entityFactory.createPortEntity(snapshot, null, permissions, operatePermissions, null, null);
    }

    @Override
    public DropRequestDTO createDropAllFlowFilesInProcessGroup(final String processGroupId, final String dropRequestId) {
        return dtoFactory.createDropRequestDTO(processGroupDAO.createDropAllFlowFilesRequest(processGroupId, dropRequestId));
    }

    @Override
    public DropRequestDTO getDropAllFlowFilesRequest(final String processGroupId, final String dropRequestId) {
        return dtoFactory.createDropRequestDTO(processGroupDAO.getDropAllFlowFilesRequest(processGroupId, dropRequestId));
    }

    @Override
    public DropRequestDTO deleteDropAllFlowFilesRequest(String processGroupId, String dropRequestId) {
        return dtoFactory.createDropRequestDTO(processGroupDAO.deleteDropAllFlowFilesRequest(processGroupId, dropRequestId));
    }

    @Override
    public ProcessGroupEntity deleteProcessGroup(final Revision revision, final String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);

        // grab the resources in the snippet so we can delete the policies afterwards
        final Set<Resource> groupResources = new HashSet<>();
        processGroup.findAllProcessors().forEach(processor -> groupResources.add(processor.getResource()));
        processGroup.findAllInputPorts().forEach(inputPort -> groupResources.add(inputPort.getResource()));
        processGroup.findAllOutputPorts().forEach(outputPort -> groupResources.add(outputPort.getResource()));
        processGroup.findAllFunnels().forEach(funnel -> groupResources.add(funnel.getResource()));
        processGroup.findAllLabels().forEach(label -> groupResources.add(label.getResource()));
        processGroup.findAllProcessGroups().forEach(childGroup -> groupResources.add(childGroup.getResource()));
        processGroup.findAllRemoteProcessGroups().forEach(remoteProcessGroup -> groupResources.add(remoteProcessGroup.getResource()));
        processGroup.findAllControllerServices().forEach(controllerService -> groupResources.add(controllerService.getResource()));

        final ProcessGroupDTO snapshot = deleteComponent(
                revision,
                processGroup.getResource(),
                () -> processGroupDAO.deleteProcessGroup(groupId),
                true,
                dtoFactory.createProcessGroupDto(processGroup));

        // delete all applicable component policies
        groupResources.forEach(groupResource -> cleanUpPolicies(groupResource));

        return entityFactory.createProcessGroupEntity(snapshot, null, permissions, null, null);
    }

    @Override
    public RemoteProcessGroupEntity deleteRemoteProcessGroup(final Revision revision, final String remoteProcessGroupId) {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroup);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(remoteProcessGroup));
        final RemoteProcessGroupDTO snapshot = deleteComponent(
                revision,
                remoteProcessGroup.getResource(),
                () -> remoteProcessGroupDAO.deleteRemoteProcessGroup(remoteProcessGroupId),
                true,
                dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        return entityFactory.createRemoteProcessGroupEntity(snapshot, null, permissions, operatePermissions, null, null);
    }

    @Override
    public ConnectionEntity createConnection(final Revision revision, final String groupId, final ConnectionDTO connectionDTO) {
        final RevisionUpdate<ConnectionDTO> snapshot = createComponent(
                revision,
                connectionDTO,
                () -> connectionDAO.createConnection(groupId, connectionDTO),
                connection -> dtoFactory.createConnectionDto(connection));

        final Connection connection = connectionDAO.getConnection(connectionDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connection);
        final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionDTO.getId()));
        return entityFactory.createConnectionEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status);
    }

    @Override
    public DropRequestDTO createFlowFileDropRequest(final String connectionId, final String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.createFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO createFlowFileListingRequest(final String connectionId, final String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.createFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        if (connection.getSource() != null) {
            listRequest.setSourceRunning(connection.getSource().isRunning());
        }
        if (connection.getDestination() != null) {
            listRequest.setDestinationRunning(connection.getDestination().isRunning());
        }

        return listRequest;
    }

    @Override
    public ProcessorEntity createProcessor(final Revision revision, final String groupId, final ProcessorDTO processorDTO) {
        final RevisionUpdate<ProcessorDTO> snapshot = createComponent(
                revision,
                processorDTO,
                () -> processorDAO.createProcessor(groupId, processorDTO),
                processor -> {
                    awaitValidationCompletion(processor);
                    return dtoFactory.createProcessorDto(processor);
                });

        final ProcessorNode processor = processorDAO.getProcessor(processorDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processor);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(processor));
        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processorDTO.getId()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processorDTO.getId()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createProcessorEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, status, bulletinEntities);
    }

    @Override
    public LabelEntity createLabel(final Revision revision, final String groupId, final LabelDTO labelDTO) {
        final RevisionUpdate<LabelDTO> snapshot = createComponent(
                revision,
                labelDTO,
                () -> labelDAO.createLabel(groupId, labelDTO),
                label -> dtoFactory.createLabelDto(label));

        final Label label = labelDAO.getLabel(labelDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(label);
        return entityFactory.createLabelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    /**
     * Creates a component using the optimistic locking manager.
     *
     * @param componentDto the DTO that will be used to create the component
     * @param daoCreation  A Supplier that will create the NiFi Component to use
     * @param dtoCreation  a Function that will convert the NiFi Component into a corresponding DTO
     * @param <D>          the DTO Type
     * @param <C>          the NiFi Component Type
     * @return a RevisionUpdate that represents the updated configuration
     */
    private <D, C> RevisionUpdate<D> createComponent(final Revision revision, final ComponentDTO componentDto, final Supplier<C> daoCreation, final Function<C, D> dtoCreation) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // read lock on the containing group
        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = new StandardRevisionClaim(revision);

        // update revision through revision manager
        return revisionManager.updateRevision(claim, user, () -> {
            // add the component
            final C component = daoCreation.get();

            // save the flow
            controllerFacade.save();

            final D dto = dtoCreation.apply(component);
            final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
            return new StandardRevisionUpdate<>(dto, lastMod);
        });
    }

    @Override
    public BulletinEntity createBulletin(final BulletinDTO bulletinDTO, final Boolean canRead) {
        final Bulletin bulletin = BulletinFactory.createBulletin(bulletinDTO.getCategory(), bulletinDTO.getLevel(), bulletinDTO.getMessage());
        bulletinRepository.addBulletin(bulletin);
        return entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), canRead);
    }

    @Override
    public FunnelEntity createFunnel(final Revision revision, final String groupId, final FunnelDTO funnelDTO) {
        final RevisionUpdate<FunnelDTO> snapshot = createComponent(
                revision,
                funnelDTO,
                () -> funnelDAO.createFunnel(groupId, funnelDTO),
                funnel -> dtoFactory.createFunnelDto(funnel));

        final Funnel funnel = funnelDAO.getFunnel(funnelDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(funnel);
        return entityFactory.createFunnelEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions);
    }

    @Override
    public AccessPolicyEntity createAccessPolicy(final Revision revision, final AccessPolicyDTO accessPolicyDTO) {
        final Authorizable tenantAuthorizable = authorizableLookup.getTenant();
        final String creator = NiFiUserUtils.getNiFiUserIdentity();

        final AccessPolicy newAccessPolicy = accessPolicyDAO.createAccessPolicy(accessPolicyDTO);
        final ComponentReferenceEntity componentReference = createComponentReferenceEntity(newAccessPolicy.getResource());
        final AccessPolicyDTO newAccessPolicyDto = dtoFactory.createAccessPolicyDto(newAccessPolicy,
                newAccessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity(false)).collect(Collectors.toSet()),
                newAccessPolicy.getUsers().stream().map(userId -> {
                    final RevisionDTO userRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userId));
                    return entityFactory.createTenantEntity(dtoFactory.createTenantDTO(userDAO.getUser(userId)), userRevision,
                            dtoFactory.createPermissionsDto(tenantAuthorizable));
                }).collect(Collectors.toSet()), componentReference);

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getAccessPolicyById(accessPolicyDTO.getId()));
        return entityFactory.createAccessPolicyEntity(newAccessPolicyDto, dtoFactory.createRevisionDTO(new FlowModification(revision, creator)), permissions);
    }

    @Override
    public UserEntity createUser(final Revision revision, final UserDTO userDTO) {
        final String creator = NiFiUserUtils.getNiFiUserIdentity();
        final User newUser = userDAO.createUser(userDTO);
        final Set<TenantEntity> tenantEntities = userGroupDAO.getUserGroupsForUser(newUser.getIdentifier()).stream()
                .map(g -> g.getIdentifier()).map(mapUserGroupIdToTenantEntity(false)).collect(Collectors.toSet());
        final Set<AccessPolicySummaryEntity> policyEntities = userGroupDAO.getAccessPoliciesForUser(newUser.getIdentifier()).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());
        final UserDTO newUserDto = dtoFactory.createUserDto(newUser, tenantEntities, policyEntities);

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        return entityFactory.createUserEntity(newUserDto, dtoFactory.createRevisionDTO(new FlowModification(revision, creator)), permissions);
    }

    private ComponentReferenceEntity createComponentReferenceEntity(final String resource) {
        ComponentReferenceEntity componentReferenceEntity = null;
        try {
            // get the component authorizable
            Authorizable componentAuthorizable = authorizableLookup.getAuthorizableFromResource(resource);

            // if this represents an authorizable whose policy permissions are enforced through the base resource,
            // get the underlying base authorizable for the component reference
            if (componentAuthorizable instanceof EnforcePolicyPermissionsThroughBaseResource) {
                componentAuthorizable = ((EnforcePolicyPermissionsThroughBaseResource) componentAuthorizable).getBaseAuthorizable();
            }

            final ComponentReferenceDTO componentReference = dtoFactory.createComponentReferenceDto(componentAuthorizable);
            if (componentReference != null) {
                final PermissionsDTO componentReferencePermissions = dtoFactory.createPermissionsDto(componentAuthorizable);
                final RevisionDTO componentReferenceRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(componentReference.getId()));
                componentReferenceEntity = entityFactory.createComponentReferenceEntity(componentReference, componentReferenceRevision, componentReferencePermissions);
            }
        } catch (final ResourceNotFoundException ignored) {
            // component not found for the specified resource
        }

        return componentReferenceEntity;
    }

    private AccessPolicySummaryEntity createAccessPolicySummaryEntity(final AccessPolicy ap) {
        final ComponentReferenceEntity componentReference = createComponentReferenceEntity(ap.getResource());
        final AccessPolicySummaryDTO apSummary = dtoFactory.createAccessPolicySummaryDto(ap, componentReference);
        final PermissionsDTO apPermissions = dtoFactory.createPermissionsDto(authorizableLookup.getAccessPolicyById(ap.getIdentifier()));
        final RevisionDTO apRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(ap.getIdentifier()));
        return entityFactory.createAccessPolicySummaryEntity(apSummary, apRevision, apPermissions);
    }

    @Override
    public UserGroupEntity createUserGroup(final Revision revision, final UserGroupDTO userGroupDTO) {
        final String creator = NiFiUserUtils.getNiFiUserIdentity();
        final Group newUserGroup = userGroupDAO.createUserGroup(userGroupDTO);
        final Set<TenantEntity> tenantEntities = newUserGroup.getUsers().stream().map(mapUserIdToTenantEntity(false)).collect(Collectors.toSet());
        final Set<AccessPolicySummaryEntity> policyEntities = userGroupDAO.getAccessPoliciesForUserGroup(newUserGroup.getIdentifier()).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());
        final UserGroupDTO newUserGroupDto = dtoFactory.createUserGroupDto(newUserGroup, tenantEntities, policyEntities);

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        return entityFactory.createUserGroupEntity(newUserGroupDto, dtoFactory.createRevisionDTO(new FlowModification(revision, creator)), permissions);
    }

    private void validateSnippetContents(final FlowSnippetDTO flow) {
        // validate any processors
        if (flow.getProcessors() != null) {
            for (final ProcessorDTO processorDTO : flow.getProcessors()) {
                final ProcessorNode processorNode = processorDAO.getProcessor(processorDTO.getId());
                processorDTO.setValidationStatus(processorNode.getValidationStatus().name());

                final Collection<ValidationResult> validationErrors = processorNode.getValidationErrors();
                if (validationErrors != null && !validationErrors.isEmpty()) {
                    final List<String> errors = new ArrayList<>();
                    for (final ValidationResult validationResult : validationErrors) {
                        errors.add(validationResult.toString());
                    }
                    processorDTO.setValidationErrors(errors);
                }
            }
        }

        if (flow.getInputPorts() != null) {
            for (final PortDTO portDTO : flow.getInputPorts()) {
                final Port port = inputPortDAO.getPort(portDTO.getId());
                final Collection<ValidationResult> validationErrors = port.getValidationErrors();
                if (validationErrors != null && !validationErrors.isEmpty()) {
                    final List<String> errors = new ArrayList<>();
                    for (final ValidationResult validationResult : validationErrors) {
                        errors.add(validationResult.toString());
                    }
                    portDTO.setValidationErrors(errors);
                }
            }
        }

        if (flow.getOutputPorts() != null) {
            for (final PortDTO portDTO : flow.getOutputPorts()) {
                final Port port = outputPortDAO.getPort(portDTO.getId());
                final Collection<ValidationResult> validationErrors = port.getValidationErrors();
                if (validationErrors != null && !validationErrors.isEmpty()) {
                    final List<String> errors = new ArrayList<>();
                    for (final ValidationResult validationResult : validationErrors) {
                        errors.add(validationResult.toString());
                    }
                    portDTO.setValidationErrors(errors);
                }
            }
        }

        // get any remote process group issues
        if (flow.getRemoteProcessGroups() != null) {
            for (final RemoteProcessGroupDTO remoteProcessGroupDTO : flow.getRemoteProcessGroups()) {
                final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());

                if (remoteProcessGroup.getAuthorizationIssue() != null) {
                    remoteProcessGroupDTO.setAuthorizationIssues(Arrays.asList(remoteProcessGroup.getAuthorizationIssue()));
                }
            }
        }
    }

    @Override
    public FlowEntity copySnippet(final String groupId, final String snippetId, final Double originX, final Double originY, final String idGenerationSeed) {
        // create the new snippet
        final FlowSnippetDTO snippet = snippetDAO.copySnippet(groupId, snippetId, originX, originY, idGenerationSeed);

        // save the flow
        controllerFacade.save();

        // drop the snippet
        snippetDAO.dropSnippet(snippetId);

        // post process new flow snippet
        final FlowDTO flowDto = postProcessNewFlowSnippet(groupId, snippet);

        final FlowEntity flowEntity = new FlowEntity();
        flowEntity.setFlow(flowDto);
        return flowEntity;
    }

    @Override
    public SnippetEntity createSnippet(final SnippetDTO snippetDTO) {
        // add the component
        final Snippet snippet = snippetDAO.createSnippet(snippetDTO);

        // save the flow
        controllerFacade.save();

        final SnippetDTO dto = dtoFactory.createSnippetDto(snippet);
        final RevisionUpdate<SnippetDTO> snapshot = new StandardRevisionUpdate<>(dto, null);

        return entityFactory.createSnippetEntity(snapshot.getComponent());
    }

    @Override
    public PortEntity createInputPort(final Revision revision, final String groupId, final PortDTO inputPortDTO) {
        final RevisionUpdate<PortDTO> snapshot = createComponent(
                revision,
                inputPortDTO,
                () -> inputPortDAO.createPort(groupId, inputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final Port port = inputPortDAO.getPort(inputPortDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(port));
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(port.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, status, bulletinEntities);
    }

    @Override
    public PortEntity createOutputPort(final Revision revision, final String groupId, final PortDTO outputPortDTO) {
        final RevisionUpdate<PortDTO> snapshot = createComponent(
                revision,
                outputPortDTO,
                () -> outputPortDAO.createPort(groupId, outputPortDTO),
                port -> dtoFactory.createPortDto(port));

        final Port port = outputPortDAO.getPort(outputPortDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(port));
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(port.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, status, bulletinEntities);
    }

    @Override
    public ProcessGroupEntity createProcessGroup(final Revision revision, final String parentGroupId, final ProcessGroupDTO processGroupDTO) {
        final RevisionUpdate<ProcessGroupDTO> snapshot = createComponent(
                revision,
                processGroupDTO,
                () -> processGroupDAO.createProcessGroup(parentGroupId, processGroupDTO),
                processGroup -> dtoFactory.createProcessGroupDto(processGroup));

        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);
        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(processGroup.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processGroup.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createProcessGroupEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, status, bulletinEntities);
    }

    @Override
    public RemoteProcessGroupEntity createRemoteProcessGroup(final Revision revision, final String groupId, final RemoteProcessGroupDTO remoteProcessGroupDTO) {
        final RevisionUpdate<RemoteProcessGroupDTO> snapshot = createComponent(
                revision,
                remoteProcessGroupDTO,
                () -> remoteProcessGroupDAO.createRemoteProcessGroup(groupId, remoteProcessGroupDTO),
                remoteProcessGroup -> dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));

        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroup);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(remoteProcessGroup));
        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(remoteProcessGroup, controllerFacade.getRemoteProcessGroupStatus(remoteProcessGroup.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(remoteProcessGroup.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createRemoteProcessGroupEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()),
                permissions, operatePermissions, status, bulletinEntities);
    }

    @Override
    public boolean isRemoteGroupPortConnected(final String remoteProcessGroupId, final String remotePortId) {
        final RemoteProcessGroup rpg = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        RemoteGroupPort port = rpg.getInputPort(remotePortId);
        if (port != null) {
            return port.hasIncomingConnection();
        }

        port = rpg.getOutputPort(remotePortId);
        if (port != null) {
            return !port.getConnections().isEmpty();
        }

        throw new ResourceNotFoundException("Could not find Port with ID " + remotePortId + " as a child of RemoteProcessGroup with ID " + remoteProcessGroupId);
    }

    @Override
    public void verifyComponentTypes(final VersionedProcessGroup versionedGroup) {
    }

    @Override
    public void verifyImportProcessGroup(final VersionControlInformationDTO versionControlInfo, final VersionedProcessGroup contents, final String groupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        verifyImportProcessGroup(versionControlInfo, contents, group);
    }

    private void verifyImportProcessGroup(final VersionControlInformationDTO vciDto, final VersionedProcessGroup contents, final ProcessGroup group) {
        if (group == null) {
            return;
        }

        final VersionControlInformation vci = group.getVersionControlInformation();
        if (vci != null) {
            // Note that we do not compare the Registry ID here because there could be two registry clients
            // that point to the same server (one could point to localhost while another points to 127.0.0.1, for instance)..
            if (Objects.equals(vciDto.getBucketId(), vci.getBucketIdentifier())
                && Objects.equals(vciDto.getFlowId(), vci.getFlowIdentifier())) {

                throw new IllegalStateException("Cannot import the specified Versioned Flow into the Process Group because doing so would cause a recursive dataflow. "
                    + "If Process Group A contains Process Group B, then Process Group B is not allowed to contain the flow identified by Process Group A.");
            }
        }

        final Set<VersionedProcessGroup> childGroups = contents.getProcessGroups();
        if (childGroups != null) {
            for (final VersionedProcessGroup childGroup : childGroups) {
                final VersionedFlowCoordinates childCoordinates = childGroup.getVersionedFlowCoordinates();
                if (childCoordinates != null) {
                    final VersionControlInformationDTO childVci = new VersionControlInformationDTO();
                    childVci.setBranch(childCoordinates.getBranch());
                    childVci.setBucketId(childCoordinates.getBucketId());
                    childVci.setFlowId(childCoordinates.getFlowId());
                    verifyImportProcessGroup(childVci, childGroup, group);
                }
            }
        }

        verifyImportProcessGroup(vciDto, contents, group.getParent());
    }

    /**
     * Ensures default values are populated for all components in this snippet. This is necessary to handle when existing properties have default values introduced.
     *
     * @param snippet snippet
     */
    private void ensureDefaultPropertyValuesArePopulated(final FlowSnippetDTO snippet) {
        if (snippet != null) {
            if (snippet.getControllerServices() != null) {
                snippet.getControllerServices().forEach(dto -> {
                    if (dto.getProperties() == null) {
                        dto.setProperties(new LinkedHashMap<>());
                    }

                    try {
                        final ConfigurableComponent configurableComponent = controllerFacade.getTemporaryComponent(dto.getType(), dto.getBundle());
                        configurableComponent.getPropertyDescriptors().forEach(descriptor -> {
                            if (dto.getProperties().get(descriptor.getName()) == null) {
                                dto.getProperties().put(descriptor.getName(), descriptor.getDefaultValue());
                            }
                        });
                    } catch (final Exception e) {
                        logger.warn("Unable to create ControllerService of type {} to populate default values.", dto.getType());
                    }
                });
            }

            if (snippet.getProcessors() != null) {
                snippet.getProcessors().forEach(dto -> {
                    if (dto.getConfig() == null) {
                        dto.setConfig(new ProcessorConfigDTO());
                    }

                    final ProcessorConfigDTO config = dto.getConfig();
                    if (config.getProperties() == null) {
                        config.setProperties(new LinkedHashMap<>());
                    }

                    try {
                        final ConfigurableComponent configurableComponent = controllerFacade.getTemporaryComponent(dto.getType(), dto.getBundle());
                        configurableComponent.getPropertyDescriptors().forEach(descriptor -> {
                            if (config.getProperties().get(descriptor.getName()) == null) {
                                config.getProperties().put(descriptor.getName(), descriptor.getDefaultValue());
                            }
                        });
                    } catch (final Exception e) {
                        logger.warn("Unable to create Processor of type {} to populate default values.", dto.getType());
                    }
                });
            }

            if (snippet.getProcessGroups() != null) {
                snippet.getProcessGroups().forEach(processGroup -> {
                    ensureDefaultPropertyValuesArePopulated(processGroup.getContents());
                });
            }
        }
    }

    /**
     * Post processes a new flow snippet including validation, removing the snippet, and DTO conversion.
     *
     * @param groupId group id
     * @param snippet snippet
     * @return flow dto
     */
    private FlowDTO postProcessNewFlowSnippet(final String groupId, final FlowSnippetDTO snippet) {
        // validate the new snippet
        validateSnippetContents(snippet);

        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        final ProcessGroupStatus groupStatus = controllerFacade.getProcessGroupStatus(groupId);
        return dtoFactory.createFlowDto(group, groupStatus, snippet, revisionManager, this::getProcessGroupBulletins);
    }

    @Override
    public ControllerServiceEntity createControllerService(final Revision revision, final String groupId, final ControllerServiceDTO controllerServiceDTO) {
        controllerServiceDTO.setParentGroupId(groupId);

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = new StandardRevisionClaim(revision);

        final RevisionUpdate<ControllerServiceDTO> snapshot;
        if (groupId == null) {
            // update revision through revision manager
            snapshot = revisionManager.updateRevision(claim, user, () -> {
                // Unfortunately, we can not use the createComponent() method here because createComponent() wants to obtain the read lock
                // on the group. The Controller Service may or may not have a Process Group (it won't if it's controller-scoped).
                final ControllerServiceNode controllerService = controllerServiceDAO.createControllerService(controllerServiceDTO);
                controllerFacade.save();

                awaitValidationCompletion(controllerService);
                final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(controllerService);

                final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
                return new StandardRevisionUpdate<>(dto, lastMod);
            });
        } else {
            snapshot = revisionManager.updateRevision(claim, user, () -> {
                final ControllerServiceNode controllerService = controllerServiceDAO.createControllerService(controllerServiceDTO);
                controllerFacade.save();

                awaitValidationCompletion(controllerService);
                final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(controllerService);

                final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
                return new StandardRevisionUpdate<>(dto, lastMod);
            });
        }

        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerService);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(controllerService));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(controllerServiceDTO.getId()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createControllerServiceEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, bulletinEntities);
    }

    @Override
    public ControllerServiceEntity updateControllerService(final Revision revision, final ControllerServiceDTO controllerServiceDTO) {
        // get the component, ensure we have access to it, and perform the update request
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceDTO.getId());
        final RevisionUpdate<ControllerServiceDTO> snapshot = updateComponent(revision,
                controllerService,
                () -> controllerServiceDAO.updateControllerService(controllerServiceDTO),
                cs -> {
                    awaitValidationCompletion(cs);
                    final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(cs);
                    final ControllerServiceReference ref = controllerService.getReferences();
                    final ControllerServiceReferencingComponentsEntity referencingComponentsEntity = createControllerServiceReferencingComponentsEntity(ref);
                    dto.setReferencingComponents(referencingComponentsEntity.getControllerServiceReferencingComponents());
                    return dto;
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerService);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(controllerService));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(controllerServiceDTO.getId()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createControllerServiceEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, bulletinEntities);
    }

    @Override
    public ControllerServiceEntity moveControllerService(final Revision revision, final ControllerServiceDTO controllerServiceDTO, final String newProcessGroupID) {
        // get the component, ensure we have access to it, and perform the move request
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceDTO.getId());
        final RevisionUpdate<ControllerServiceDTO> snapshot = updateComponent(revision,
                controllerService,
                () -> {
                    final ProcessGroup oldParentGroup = controllerService.getProcessGroup();
                    controllerService.setMoving(true);
                    oldParentGroup.removeControllerService(controllerService);
                    if (!oldParentGroup.isRootGroup() && oldParentGroup.getParent().getIdentifier().equals(newProcessGroupID)) {
                        // move to parent process group
                        oldParentGroup.getParent().addControllerService(controllerService);
                    } else {
                        // move to child process group
                        oldParentGroup.getProcessGroup(newProcessGroupID).addControllerService(controllerService);
                    }
                    return controllerService;
                },
                cs -> {
                    final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(cs);
                    final ControllerServiceReference ref = controllerService.getReferences();
                    final ControllerServiceReferencingComponentsEntity referencingComponentsEntity = createControllerServiceReferencingComponentsEntity(ref);
                    dto.setReferencingComponents(referencingComponentsEntity.getControllerServiceReferencingComponents());
                    return dto;
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerService);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(controllerService));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(controllerServiceDTO.getId()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        controllerService.performValidation();
        return entityFactory.createControllerServiceEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, bulletinEntities);
    }

    @Override
    public List<ConfigVerificationResultDTO> performControllerServiceConfigVerification(final String controllerServiceId, final Map<String, String> properties, final Map<String, String> variables) {
        return controllerServiceDAO.verifyConfiguration(controllerServiceId, properties, variables);
    }

    @Override
    public ConfigurationAnalysisEntity analyzeControllerServiceConfiguration(final String controllerServiceId, final Map<String, String> properties) {
        final ControllerServiceNode serviceNode = controllerServiceDAO.getControllerService(controllerServiceId);
        final ProcessGroup processGroup = serviceNode.getProcessGroup();
        final ParameterContext parameterContext = processGroup == null ? null : processGroup.getParameterContext();

        final ConfigurationAnalysisEntity configurationAnalysisEntity = analyzeConfiguration(serviceNode, properties, parameterContext);
        return configurationAnalysisEntity;
    }

    @Override
    public ControllerServiceReferencingComponentsEntity updateControllerServiceReferencingComponents(
            final Map<String, Revision> referenceRevisions, final String controllerServiceId, final ScheduledState scheduledState, final ControllerServiceState controllerServiceState) {

        final RevisionClaim claim = new StandardRevisionClaim(referenceRevisions.values());

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final RevisionUpdate<ControllerServiceReferencingComponentsEntity> update = revisionManager.updateRevision(claim, user,
                () -> {
                    final Set<ComponentNode> updated = controllerServiceDAO.updateControllerServiceReferencingComponents(controllerServiceId, scheduledState, controllerServiceState);
                    controllerFacade.save();

                    final ControllerServiceReference updatedReference = controllerServiceDAO.getControllerService(controllerServiceId).getReferences();

                    // get the revisions of the updated components
                    final Map<String, Revision> updatedRevisions = new HashMap<>();
                    for (final ComponentNode component : updated) {
                        final Revision currentRevision = revisionManager.getRevision(component.getIdentifier());
                        final Revision requestRevision = referenceRevisions.get(component.getIdentifier());
                        updatedRevisions.put(component.getIdentifier(), currentRevision.incrementRevision(requestRevision.getClientId()));
                    }

                    // ensure the revision for all referencing components is included regardless of whether they were updated in this request
                    for (final ComponentNode component : updatedReference.findRecursiveReferences(ComponentNode.class)) {
                        updatedRevisions.putIfAbsent(component.getIdentifier(), revisionManager.getRevision(component.getIdentifier()));
                    }

                    final ControllerServiceReferencingComponentsEntity entity = createControllerServiceReferencingComponentsEntity(updatedReference, updatedRevisions);
                    return new StandardRevisionUpdate<>(entity, null, new HashSet<>(updatedRevisions.values()));
                });

        return update.getComponent();
    }

    /**
     * Finds the identifiers for all components referencing a ControllerService.
     *
     * @param reference      ControllerServiceReference
     * @param visited        ControllerServices we've already visited
     */
    private void findControllerServiceReferencingComponentIdentifiers(final ControllerServiceReference reference, final Set<ControllerServiceNode> visited) {
        for (final ComponentNode component : reference.getReferencingComponents()) {

            // if this is a ControllerService consider it's referencing components
            if (component instanceof ControllerServiceNode) {
                final ControllerServiceNode node = (ControllerServiceNode) component;
                if (!visited.contains(node)) {
                    visited.add(node);
                    findControllerServiceReferencingComponentIdentifiers(node.getReferences(), visited);
                }
            }
        }
    }

    /**
     * Creates entities for components referencing a ControllerService using their current revision.
     *
     * @param reference ControllerServiceReference
     * @return The entity
     */
    private ControllerServiceReferencingComponentsEntity createControllerServiceReferencingComponentsEntity(final ControllerServiceReference reference) {
        final Set<ControllerServiceNode> visited = new HashSet<>();
        visited.add(reference.getReferencedComponent());
        findControllerServiceReferencingComponentIdentifiers(reference, visited);

        final Map<String, Revision> referencingRevisions = new HashMap<>();
        for (final ComponentNode component : reference.getReferencingComponents()) {
            referencingRevisions.put(component.getIdentifier(), revisionManager.getRevision(component.getIdentifier()));
        }

        return createControllerServiceReferencingComponentsEntity(reference, referencingRevisions);
    }

    /**
     * Creates entities for components referencing a ControllerService using the specified revisions.
     *
     * @param reference ControllerServiceReference
     * @param revisions The revisions
     * @return The entity
     */
    private ControllerServiceReferencingComponentsEntity createControllerServiceReferencingComponentsEntity(
            final ControllerServiceReference reference, final Map<String, Revision> revisions) {
        final Set<ControllerServiceNode> visited = new HashSet<>();
        visited.add(reference.getReferencedComponent());
        return createControllerServiceReferencingComponentsEntity(reference, revisions, visited);
    }

    /**
     * Creates entities for components referencing a ControllerServcie using the specified revisions.
     *
     * @param reference ControllerServiceReference
     * @param revisions The revisions
     * @param visited   Which services we've already considered (in case of cycle)
     * @return The entity
     */
    private ControllerServiceReferencingComponentsEntity createControllerServiceReferencingComponentsEntity(
            final ControllerServiceReference reference, final Map<String, Revision> revisions, final Set<ControllerServiceNode> visited) {

        final String modifier = NiFiUserUtils.getNiFiUserIdentity();
        final Set<ComponentNode> referencingComponents = reference.getReferencingComponents();

        final Set<ControllerServiceReferencingComponentEntity> componentEntities = new HashSet<>();
        for (final ComponentNode refComponent : referencingComponents) {
            final PermissionsDTO permissions = dtoFactory.createPermissionsDto(refComponent);
            final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(refComponent));

            final Revision revision = revisions.get(refComponent.getIdentifier());
            final FlowModification flowMod = new FlowModification(revision, modifier);
            final RevisionDTO revisionDto = dtoFactory.createRevisionDTO(flowMod);
            final ControllerServiceReferencingComponentDTO dto = dtoFactory.createControllerServiceReferencingComponentDTO(refComponent);

            if (refComponent instanceof ControllerServiceNode) {
                final ControllerServiceNode node = (ControllerServiceNode) refComponent;

                // indicate if we've hit a cycle
                dto.setReferenceCycle(visited.contains(node));

                // mark node as visited before building the reference cycle
                visited.add(node);

                // if we haven't encountered this service before include it's referencing components
                if (!dto.getReferenceCycle()) {
                    final ControllerServiceReference refReferences = node.getReferences();
                    final Map<String, Revision> referencingRevisions = new HashMap<>(revisions);
                    for (final ComponentNode component : refReferences.getReferencingComponents()) {
                        referencingRevisions.putIfAbsent(component.getIdentifier(), revisionManager.getRevision(component.getIdentifier()));
                    }
                    final ControllerServiceReferencingComponentsEntity references = createControllerServiceReferencingComponentsEntity(refReferences, referencingRevisions, visited);
                    dto.setReferencingComponents(references.getControllerServiceReferencingComponents());
                }
            }

            final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(refComponent.getIdentifier()));
            componentEntities.add(entityFactory.createControllerServiceReferencingComponentEntity(refComponent.getIdentifier(), dto, revisionDto, permissions, operatePermissions, bulletins));
        }

        final ControllerServiceReferencingComponentsEntity entity = new ControllerServiceReferencingComponentsEntity();
        entity.setControllerServiceReferencingComponents(componentEntities);
        return entity;
    }

    @Override
    public ControllerServiceEntity deleteControllerService(final Revision revision, final String controllerServiceId) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerService);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(controllerService));
        final ControllerServiceDTO snapshot = deleteComponent(
                revision,
                controllerService.getResource(),
                () -> controllerServiceDAO.deleteControllerService(controllerServiceId),
                true,
                dtoFactory.createControllerServiceDto(controllerService));

        return entityFactory.createControllerServiceEntity(snapshot, null, permissions, operatePermissions, null);
    }


    @Override
    public FlowRegistryClientEntity createRegistryClient(final Revision revision, final FlowRegistryClientDTO flowRegistryClientDTO) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = new StandardRevisionClaim(revision);

        // update revision through revision manager
        final RevisionUpdate<FlowRegistryClientDTO> snapshot = revisionManager.updateRevision(claim, user, () -> {
            // add the component
            final FlowRegistryClientNode registryClient = flowRegistryDAO.createFlowRegistryClient(flowRegistryClientDTO);

            // save the flow
            controllerFacade.save();
            awaitValidationCompletion(registryClient);


            final FlowRegistryClientDTO dto = dtoFactory.createRegistryDto(registryClient);
            final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
            return new StandardRevisionUpdate<>(dto, lastMod);
        });

        return createRegistryClientEntity(flowRegistryClientDTO, snapshot);
    }

    @Override
    public FlowRegistryClientEntity getRegistryClient(final String registryClientId) {
        final FlowRegistryClientNode flowRegistryClient = flowRegistryDAO.getFlowRegistryClient(registryClientId);
        return createRegistryClientEntity(flowRegistryClient);
    }

    @Override
    public Set<FlowRegistryClientEntity> getRegistryClients() {
        return flowRegistryDAO.getFlowRegistryClients().stream()
            .map(this::createRegistryClientEntity)
            .collect(Collectors.toSet());
    }

    @Override
    public Set<FlowRegistryClientEntity> getRegistryClientsForUser() {
        return flowRegistryDAO.getFlowRegistryClientsForUser(FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser())).stream()
            .map(this::createRegistryClientEntity)
            .collect(Collectors.toSet());
    }

    @Override
    public PropertyDescriptorDTO getRegistryClientPropertyDescriptor(final String id, final String property, final boolean sensitive) {
        final FlowRegistryClientNode flowRegistryClient = flowRegistryDAO.getFlowRegistryClient(id);
        final PropertyDescriptor descriptor = getPropertyDescriptor(flowRegistryClient, property, sensitive);
        return dtoFactory.createPropertyDescriptorDto(descriptor, null);
    }

    @Override
    public FlowRegistryClientEntity updateRegistryClient(final Revision revision, final FlowRegistryClientDTO flowRegistryClientDTO) {
        final FlowRegistryClientNode flowRegistryClient = flowRegistryDAO.getFlowRegistryClient(flowRegistryClientDTO.getId());

        final RevisionUpdate<FlowRegistryClientDTO> snapshot = updateComponent(revision,
                flowRegistryClient,
                () -> flowRegistryDAO.updateFlowRegistryClient(flowRegistryClientDTO),
                client -> {
                    awaitValidationCompletion(client);
                    return dtoFactory.createRegistryDto(client);
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(flowRegistryClient);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(flowRegistryClient));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(flowRegistryClient.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createFlowRegistryClientEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, bulletinEntities);
    }

    @Override
    public FlowRegistryClientEntity deleteRegistryClient(final Revision revision, final String registryClientId) {
        final FlowRegistryClientNode flowRegistryClient = flowRegistryDAO.getFlowRegistryClient(registryClientId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(flowRegistryClient);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(flowRegistryClient));
        final FlowRegistryClientDTO snapshot = deleteComponent(
                revision,
                flowRegistryClient.getResource(),
                () -> flowRegistryDAO.removeFlowRegistry(registryClientId),
                true,
                dtoFactory.createRegistryDto(flowRegistryClient)
        );

        return entityFactory.createFlowRegistryClientEntity(snapshot, null, permissions, operatePermissions, null);
    }

    @Override
    public FlowRegistryBranchEntity getDefaultBranch(final String registryClientId) {
        final FlowRegistryClientUserContext clientUserContext = FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser());
        final FlowRegistryBranch branch = flowRegistryDAO.getDefaultBranchForUser(clientUserContext, registryClientId);
        final FlowRegistryBranchDTO branchDto = dtoFactory.createBranchDTO(branch);
        return entityFactory.createBranchEntity(branchDto);
    }

    @Override
    public Set<FlowRegistryBranchEntity> getBranches(final String registryClientId) {
        final FlowRegistryClientUserContext clientUserContext = FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser());
        return flowRegistryDAO.getBranchesForUser(clientUserContext, registryClientId).stream()
                .filter(Objects::nonNull)
                .map(branch -> {
                    final FlowRegistryBranchDTO branchDto = dtoFactory.createBranchDTO(branch);
                    return entityFactory.createBranchEntity(branchDto);
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public Set<FlowRegistryBucketEntity> getBucketsForUser(final String registryClientId, final String branch) {
        final FlowRegistryClientUserContext clientUserContext = FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser());
        return flowRegistryDAO.getBucketsForUser(clientUserContext, registryClientId, branch).stream()
                .map(bucket -> {
                    if (bucket == null) {
                        return null;
                    }

                    final FlowRegistryBucketDTO dto = new FlowRegistryBucketDTO();
                    dto.setId(bucket.getIdentifier());
                    dto.setName(bucket.getName());
                    dto.setDescription(bucket.getDescription());
                    dto.setCreated(bucket.getCreatedTimestamp());

                    final FlowRegistryPermissions regPermissions = bucket.getPermissions();
                    final PermissionsDTO permissions = new PermissionsDTO();
                    permissions.setCanRead(regPermissions.getCanRead());
                    permissions.setCanWrite(regPermissions.getCanWrite());

                    return entityFactory.createBucketEntity(dto, permissions);
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public Set<VersionedFlowEntity> getFlowsForUser(final String registryClientId, final String branch, final String bucketId) {
        final FlowRegistryClientUserContext clientUserContext = FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser());
        return flowRegistryDAO.getFlowsForUser(clientUserContext, registryClientId, branch, bucketId).stream()
                .map(rf -> createVersionedFlowEntity(registryClientId, rf))
                .collect(Collectors.toSet());
    }

    @Override
    public VersionedFlowEntity getFlowForUser(final String registryClientId, final String branch, final String bucketId, final String flowId) {
        final FlowRegistryClientUserContext clientUserContext = FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser());
        final RegisteredFlow flow = flowRegistryDAO.getFlowForUser(clientUserContext, registryClientId, branch, bucketId, flowId);
        return createVersionedFlowEntity(registryClientId, flow);
    }

    @Override
    public Set<VersionedFlowSnapshotMetadataEntity> getFlowVersionsForUser(final String registryClientId, final String branch, final String bucketId, final String flowId) {
        final FlowRegistryClientUserContext clientUserContext = FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser());
        return flowRegistryDAO.getFlowVersionsForUser(clientUserContext, registryClientId, branch, bucketId, flowId).stream()
                .map(md -> createVersionedFlowSnapshotMetadataEntity(registryClientId, md))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public void verifyDeleteRegistry(String registryClientId) {
        processGroupDAO.verifyDeleteFlowRegistry(registryClientId);
    }

    private FlowRegistryClientEntity createRegistryClientEntity(final FlowRegistryClientDTO flowRegistryClientDTO, final RevisionUpdate<FlowRegistryClientDTO> snapshot) {
        final FlowRegistryClientNode flowRegistryClient = flowRegistryDAO.getFlowRegistryClient(flowRegistryClientDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getController());
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(authorizableLookup.getController()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(flowRegistryClient.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        final RevisionDTO revisionDto = dtoFactory.createRevisionDTO(revisionManager.getRevision(flowRegistryClientDTO.getId()));
        return entityFactory.createFlowRegistryClientEntity(snapshot.getComponent(), revisionDto, permissions, operatePermissions, bulletinEntities);
    }

    private FlowRegistryClientEntity createRegistryClientEntity(final FlowRegistryClientNode flowRegistryClientNode) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(flowRegistryClientNode.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getController());
        final FlowRegistryClientDTO dto = dtoFactory.createRegistryDto(flowRegistryClientNode);
        return entityFactory.createFlowRegistryClientEntity(dto, revision, permissions);
    }

    private VersionedFlowEntity createVersionedFlowEntity(final String registryId, final RegisteredFlow registeredFlow) {
        if (registeredFlow == null) {
            return null;
        }

        final VersionedFlowDTO dto = new VersionedFlowDTO();
        dto.setRegistryId(registryId);
        dto.setBranch(registeredFlow.getBranch());
        dto.setBucketId(registeredFlow.getBucketIdentifier());
        dto.setFlowId(registeredFlow.getIdentifier());
        dto.setFlowName(registeredFlow.getName());
        dto.setDescription(registeredFlow.getDescription());

        final VersionedFlowEntity entity = new VersionedFlowEntity();
        entity.setVersionedFlow(dto);

        return entity;
    }

    private VersionedFlowSnapshotMetadataEntity createVersionedFlowSnapshotMetadataEntity(final String registryId, final RegisteredFlowSnapshotMetadata metadata) {
        if (metadata == null) {
            return null;
        }

        final VersionedFlowSnapshotMetadataEntity entity = new VersionedFlowSnapshotMetadataEntity();
        entity.setRegistryId(registryId);
        entity.setVersionedFlowSnapshotMetadata(metadata);

        return entity;
    }

    @Override
    public ReportingTaskEntity createReportingTask(final Revision revision, final ReportingTaskDTO reportingTaskDTO) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = new StandardRevisionClaim(revision);

        // update revision through revision manager
        final RevisionUpdate<ReportingTaskDTO> snapshot = revisionManager.updateRevision(claim, user, () -> {
            // create the reporting task
            final ReportingTaskNode reportingTask = reportingTaskDAO.createReportingTask(reportingTaskDTO);

            // save the update
            controllerFacade.save();
            awaitValidationCompletion(reportingTask);

            final ReportingTaskDTO dto = dtoFactory.createReportingTaskDto(reportingTask);
            final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
            return new StandardRevisionUpdate<>(dto, lastMod);
        });

        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(reportingTask);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(reportingTask));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(reportingTask.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createReportingTaskEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, bulletinEntities);
    }

    @Override
    public ReportingTaskEntity updateReportingTask(final Revision revision, final ReportingTaskDTO reportingTaskDTO) {
        // get the component, ensure we have access to it, and perform the update request
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskDTO.getId());
        final RevisionUpdate<ReportingTaskDTO> snapshot = updateComponent(revision,
                reportingTask,
                () -> reportingTaskDAO.updateReportingTask(reportingTaskDTO),
                rt -> {
                    awaitValidationCompletion(rt);
                    return dtoFactory.createReportingTaskDto(rt);
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(reportingTask);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(reportingTask));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(reportingTask.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createReportingTaskEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, operatePermissions, bulletinEntities);
    }

    @Override
    public List<ConfigVerificationResultDTO> performReportingTaskConfigVerification(final String reportingTaskId, final Map<String, String> properties) {
        return reportingTaskDAO.verifyConfiguration(reportingTaskId, properties);
    }

    @Override
    public ConfigurationAnalysisEntity analyzeReportingTaskConfiguration(final String reportingTaskId, final Map<String, String> properties) {
        final ReportingTaskNode taskNode = reportingTaskDAO.getReportingTask(reportingTaskId);
        final ConfigurationAnalysisEntity configurationAnalysisEntity = analyzeConfiguration(taskNode, properties, null);
        return configurationAnalysisEntity;
    }

    @Override
    public ReportingTaskEntity deleteReportingTask(final Revision revision, final String reportingTaskId) {
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(reportingTask);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(reportingTask));
        final ReportingTaskDTO snapshot = deleteComponent(
                revision,
                reportingTask.getResource(),
                () -> reportingTaskDAO.deleteReportingTask(reportingTaskId),
                true,
                dtoFactory.createReportingTaskDto(reportingTask));

        return entityFactory.createReportingTaskEntity(snapshot, null, permissions, operatePermissions, null);
    }

    @Override
    public ParameterProviderEntity createParameterProvider(final Revision revision, final ParameterProviderDTO parameterProviderDTO) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // request claim for component to be created... revision already verified (version == 0)
        final RevisionClaim claim = new StandardRevisionClaim(revision);

        // update revision through revision manager
        final RevisionUpdate<ParameterProviderDTO> snapshot = revisionManager.updateRevision(claim, user, () -> {
            // create the parameter provider
            final ParameterProviderNode parameterProvider = parameterProviderDAO.createParameterProvider(parameterProviderDTO);

            // save the update
            controllerFacade.save();
            awaitValidationCompletion(parameterProvider);

            final ParameterProviderDTO dto = dtoFactory.createParameterProviderDto(parameterProvider);
            final FlowModification lastMod = new FlowModification(revision.incrementRevision(revision.getClientId()), user.getIdentity());
            return new StandardRevisionUpdate<>(dto, lastMod);
        });

        final ParameterProviderNode parameterProvider = parameterProviderDAO.getParameterProvider(parameterProviderDTO.getId());
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(parameterProvider);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(parameterProvider.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createParameterProviderEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, bulletinEntities);
    }

    @Override
    public ParameterProviderEntity updateParameterProvider(final Revision revision, final ParameterProviderDTO parameterProviderDTO) {
        // get the component, ensure we have access to it, and perform the update request
        final ParameterProviderNode parameterProvider = parameterProviderDAO.getParameterProvider(parameterProviderDTO.getId());
        final RevisionUpdate<ParameterProviderDTO> snapshot = updateComponent(revision,
                parameterProvider,
                () -> parameterProviderDAO.updateParameterProvider(parameterProviderDTO),
                rt -> {
                    awaitValidationCompletion(rt);
                    return dtoFactory.createParameterProviderDto(rt);
                });

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(parameterProvider);
        dtoFactory.createPermissionsDto(new OperationAuthorizable(parameterProvider));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(parameterProvider.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createParameterProviderEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()), permissions, bulletinEntities);
    }

    @Override
    public List<ConfigVerificationResultDTO> performParameterProviderConfigVerification(final String parameterProviderId, final Map<String, String> properties) {
        return parameterProviderDAO.verifyConfiguration(parameterProviderId, properties);
    }

    @Override
    public ConfigurationAnalysisEntity analyzeParameterProviderConfiguration(final String parameterProviderId, final Map<String, String> properties) {
        final ParameterProviderNode taskNode = parameterProviderDAO.getParameterProvider(parameterProviderId);
        final ConfigurationAnalysisEntity configurationAnalysisEntity = analyzeConfiguration(taskNode, properties, null);
        return configurationAnalysisEntity;
    }

    @Override
    public ParameterProviderEntity deleteParameterProvider(final Revision revision, final String parameterProviderId) {
        final ParameterProviderNode parameterProvider = parameterProviderDAO.getParameterProvider(parameterProviderId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(parameterProvider);
        dtoFactory.createPermissionsDto(new OperationAuthorizable(parameterProvider));
        final ParameterProviderDTO snapshot = deleteComponent(
                revision,
                parameterProvider.getResource(),
                () -> parameterProviderDAO.deleteParameterProvider(parameterProviderId),
                true,
                dtoFactory.createParameterProviderDto(parameterProvider));

        return entityFactory.createParameterProviderEntity(snapshot, null, permissions, null);
    }

    @Override
    public ParameterProviderEntity fetchParameters(final String parameterProviderId) {
        final ParameterProviderNode parameterProvider = parameterProviderDAO.getParameterProvider(parameterProviderId);

        awaitValidationCompletion(parameterProvider);
        parameterProvider.fetchParameters();

        final ParameterProviderEntity entity = createParameterProviderEntity(parameterProvider);
        return entity;
    }

    @Override
    public List<ParameterContextEntity> getParameterContextUpdatesForAppliedParameters(final String parameterProviderId, final Collection<ParameterGroupConfiguration> parameterGroupConfigurations) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final Map<String, ParameterGroupConfiguration> parameterGroupConfigurationMap = parameterGroupConfigurations.stream()
                .collect(Collectors.toMap(ParameterGroupConfiguration::getParameterContextName, Function.identity()));

        final List<ParametersApplication> parametersApplications = parameterProviderDAO.getFetchedParametersToApply(parameterProviderId, parameterGroupConfigurations);
        return parametersApplications.stream()
                .filter(parametersApplication -> !parametersApplication.getParameterUpdates().isEmpty())
                .map(parametersApplication -> {
                    final ParameterContext parameterContext = parametersApplication.getParameterContext();
                    final ParameterGroupConfiguration parameterGroupConfiguration = parameterGroupConfigurationMap.get(parameterContext.getName());
                    final ParameterContextEntity entity = createParameterContextEntity(parameterContext, false, user, parameterContextDAO);
                    final ParameterProviderConfigurationDTO parameterProviderConfiguration = entity.getComponent().getParameterProviderConfiguration().getComponent();
                    parameterProviderConfiguration.setSynchronized(parameterGroupConfiguration.isSynchronized());

                    final Map<String, Parameter> parameterUpdates = parametersApplication.getParameterUpdates();
                    final Map<String, ParameterEntity> currentParameterEntities = entity.getComponent().getParameters()
                            .stream()
                            .collect(Collectors.toMap(
                                    parameterEntity -> parameterEntity.getParameter().getName(),
                                    Function.identity()
                            ));
                    final Set<ParameterEntity> updatedParameterEntities = new LinkedHashSet<>();
                    entity.getComponent().setParameters(updatedParameterEntities);
                    for (final Map.Entry<String, Parameter> parameterEntry : parameterUpdates.entrySet()) {
                        final String parameterName = parameterEntry.getKey();
                        final Parameter parameter = parameterEntry.getValue();
                        final ParameterEntity parameterEntity;
                        if (parameter == null) {
                            parameterEntity = currentParameterEntities.get(parameterName);
                            if (parameterEntity != null) {
                                final ParameterDTO parameterDTO = parameterEntity.getParameter();
                                // These three fields must be null in order for ParameterContextDAO to recognize this as a parameter deletion
                                parameterDTO.setDescription(null);
                                parameterDTO.setSensitive(null);
                                parameterDTO.setValue(null);
                                updatedParameterEntities.add(parameterEntity);
                            }
                        } else {
                            parameterEntity = dtoFactory.createParameterEntity(parameterContext, parameter, revisionManager, parameterContextDAO);
                            // Need to unmask in order to actually apply the value
                            if (parameterEntity.getParameter() != null && parameterEntity.getParameter().getSensitive() != null
                                     && parameterEntity.getParameter().getSensitive()) {
                                parameterEntity.getParameter().setValue(parameter.getValue());
                            }
                            updatedParameterEntities.add(parameterEntity);
                        }
                    }
                    return entity;
                })
                .collect(Collectors.toList());
    }

    @Override
    public void deleteActions(final Date endDate) {
        // get the user from the request
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // create the purge details
        final FlowChangePurgeDetails details = new FlowChangePurgeDetails();
        details.setEndDate(endDate);

        // create a purge action to record that records are being removed
        final FlowChangeAction purgeAction = new FlowChangeAction();
        purgeAction.setUserIdentity(user.getIdentity());
        purgeAction.setOperation(Operation.Purge);
        purgeAction.setTimestamp(new Date());
        purgeAction.setSourceId("Flow Controller");
        purgeAction.setSourceName("History");
        purgeAction.setSourceType(Component.Controller);
        purgeAction.setActionDetails(details);

        // purge corresponding actions
        auditService.purgeActions(endDate, purgeAction);
    }

    @Override
    public ProvenanceDTO submitProvenance(final ProvenanceDTO query) {
        return controllerFacade.submitProvenance(query);
    }

    @Override
    public void deleteProvenance(final String queryId) {
        controllerFacade.deleteProvenanceQuery(queryId);
    }

    @Override
    public LineageDTO submitLineage(final LineageDTO lineage) {
        return controllerFacade.submitLineage(lineage);
    }

    @Override
    public void deleteLineage(final String lineageId) {
        controllerFacade.deleteLineage(lineageId);
    }

    @Override
    public ProvenanceEventDTO submitReplay(final Long eventId) {
        return controllerFacade.submitReplay(eventId);
    }

    @Override
    public ProvenanceEventDTO submitReplayLastEvent(final String componentId) {
        return controllerFacade.submitReplayLastEvent(componentId);
    }

    // -----------------------------------------
    // Read Operations
    // -----------------------------------------

    @Override
    public SearchResultsDTO searchController(final String query, final String activeGroupId) {
        return controllerFacade.search(query, activeGroupId);
    }

    @Override
    public DownloadableContent getContent(final String connectionId, final String flowFileUuid, final String uri) {
        return connectionDAO.getContent(connectionId, flowFileUuid, uri);
    }

    @Override
    public DownloadableContent getContent(final Long eventId, final String uri, final ContentDirection contentDirection) {
        return controllerFacade.getContent(eventId, uri, contentDirection);
    }

    @Override
    public ProvenanceDTO getProvenance(final String queryId, final Boolean summarize, final Boolean incrementalResults) {
        return controllerFacade.getProvenanceQuery(queryId, summarize, incrementalResults);
    }

    @Override
    public LineageDTO getLineage(final String lineageId) {
        return controllerFacade.getLineage(lineageId);
    }

    @Override
    public ProvenanceOptionsDTO getProvenanceSearchOptions() {
        return controllerFacade.getProvenanceSearchOptions();
    }

    @Override
    public ProvenanceEventDTO getProvenanceEvent(final Long id) {
        return controllerFacade.getProvenanceEvent(id);
    }

    @Override
    public LatestProvenanceEventsEntity getLatestProvenanceEvents(final String componentId, final int eventLimit) {
        final LatestProvenanceEventsDTO dto = controllerFacade.getLatestProvenanceEvents(componentId, eventLimit);

        final LatestProvenanceEventsEntity entity = new LatestProvenanceEventsEntity();
        entity.setLatestProvenanceEvents(dto);
        return entity;
    }

    @Override
    public ProcessGroupStatusEntity getProcessGroupStatus(final String groupId, final boolean recursive) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);
        final ProcessGroupStatusDTO dto = dtoFactory.createProcessGroupStatusDto(processGroup, controllerFacade.getProcessGroupStatus(groupId));

        // prune the response as necessary
        if (!recursive) {
            pruneChildGroups(dto.getAggregateSnapshot());
            if (dto.getNodeSnapshots() != null) {
                for (final NodeProcessGroupStatusSnapshotDTO nodeSnapshot : dto.getNodeSnapshots()) {
                    pruneChildGroups(nodeSnapshot.getStatusSnapshot());
                }
            }
        }

        return entityFactory.createProcessGroupStatusEntity(dto, permissions);
    }

    private void pruneChildGroups(final ProcessGroupStatusSnapshotDTO snapshot) {
        for (final ProcessGroupStatusSnapshotEntity childProcessGroupStatusEntity : snapshot.getProcessGroupStatusSnapshots()) {
            final ProcessGroupStatusSnapshotDTO childProcessGroupStatus = childProcessGroupStatusEntity.getProcessGroupStatusSnapshot();
            childProcessGroupStatus.setConnectionStatusSnapshots(null);
            childProcessGroupStatus.setProcessGroupStatusSnapshots(null);
            childProcessGroupStatus.setInputPortStatusSnapshots(null);
            childProcessGroupStatus.setOutputPortStatusSnapshots(null);
            childProcessGroupStatus.setProcessorStatusSnapshots(null);
            childProcessGroupStatus.setRemoteProcessGroupStatusSnapshots(null);
        }
    }

    @Override
    public ControllerStatusDTO getControllerStatus() {
        return controllerFacade.getControllerStatus();
    }

    @Override
    public ComponentStateDTO getProcessorState(final String processorId) {
        final StateMap clusterState = isClustered() ? processorDAO.getState(processorId, Scope.CLUSTER) : null;
        final StateMap localState = processorDAO.getState(processorId, Scope.LOCAL);

        // processor will be non-null as it was already found when getting the state
        final ProcessorNode processor = processorDAO.getProcessor(processorId);
        return dtoFactory.createComponentStateDTO(processorId, processor.getProcessor().getClass(), localState, clusterState);
    }

    @Override
    public ComponentStateDTO getControllerServiceState(final String controllerServiceId) {
        final StateMap clusterState = isClustered() ? controllerServiceDAO.getState(controllerServiceId, Scope.CLUSTER) : null;
        final StateMap localState = controllerServiceDAO.getState(controllerServiceId, Scope.LOCAL);

        // controller service will be non-null as it was already found when getting the state
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
        return dtoFactory.createComponentStateDTO(controllerServiceId, controllerService.getControllerServiceImplementation().getClass(), localState, clusterState);
    }

    @Override
    public ComponentStateDTO getReportingTaskState(final String reportingTaskId) {
        final StateMap clusterState = isClustered() ? reportingTaskDAO.getState(reportingTaskId, Scope.CLUSTER) : null;
        final StateMap localState = reportingTaskDAO.getState(reportingTaskId, Scope.LOCAL);

        // reporting task will be non-null as it was already found when getting the state
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
        return dtoFactory.createComponentStateDTO(reportingTaskId, reportingTask.getReportingTask().getClass(), localState, clusterState);
    }

    @Override
    public ComponentStateDTO getParameterProviderState(final String parameterProviderId) {
        final StateMap clusterState = isClustered() ? parameterProviderDAO.getState(parameterProviderId, Scope.CLUSTER) : null;
        final StateMap localState = parameterProviderDAO.getState(parameterProviderId, Scope.LOCAL);

        // parameter provider will be non-null as it was already found when getting the state
        final ParameterProviderNode parameterProvider = parameterProviderDAO.getParameterProvider(parameterProviderId);
        return dtoFactory.createComponentStateDTO(parameterProviderId, parameterProvider.getParameterProvider().getClass(), localState, clusterState);
    }

    @Override
    public CountersDTO getCounters() {
        final List<Counter> counters = controllerFacade.getCounters();
        final Set<CounterDTO> counterDTOs = new LinkedHashSet<>(counters.size());
        for (final Counter counter : counters) {
            counterDTOs.add(dtoFactory.createCounterDto(counter));
        }

        final CountersSnapshotDTO snapshotDto = dtoFactory.createCountersDto(counterDTOs);
        final CountersDTO countersDto = new CountersDTO();
        countersDto.setAggregateSnapshot(snapshotDto);

        return countersDto;
    }

    private ConnectionEntity createConnectionEntity(final Connection connection) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(connection.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connection);
        final ConnectionStatusDTO status = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connection.getIdentifier()));
        return entityFactory.createConnectionEntity(dtoFactory.createConnectionDto(connection), revision, permissions, status);
    }

    @Override
    public Set<ConnectionEntity> getConnections(final String groupId) {
        final Set<Connection> connections = connectionDAO.getConnections(groupId);
        return connections.stream()
            .map(connection -> createConnectionEntity(connection))
            .collect(Collectors.toSet());
    }

    @Override
    public ConnectionEntity getConnection(final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        return createConnectionEntity(connection);
    }

    @Override
    public DropRequestDTO getFlowFileDropRequest(final String connectionId, final String dropRequestId) {
        return dtoFactory.createDropRequestDTO(connectionDAO.getFlowFileDropRequest(connectionId, dropRequestId));
    }

    @Override
    public ListingRequestDTO getFlowFileListingRequest(final String connectionId, final String listingRequestId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final ListingRequestDTO listRequest = dtoFactory.createListingRequestDTO(connectionDAO.getFlowFileListingRequest(connectionId, listingRequestId));

        // include whether the source and destination are running
        if (connection.getSource() != null) {
            listRequest.setSourceRunning(connection.getSource().isRunning());
        }
        if (connection.getDestination() != null) {
            listRequest.setDestinationRunning(connection.getDestination().isRunning());
        }

        return listRequest;
    }

    @Override
    public FlowFileDTO getFlowFile(final String connectionId, final String flowFileUuid) {
        return dtoFactory.createFlowFileDTO(connectionDAO.getFlowFile(connectionId, flowFileUuid));
    }

    @Override
    public ConnectionStatusEntity getConnectionStatus(final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connection);
        final ConnectionStatusDTO dto = dtoFactory.createConnectionStatusDto(controllerFacade.getConnectionStatus(connectionId));
        return entityFactory.createConnectionStatusEntity(dto, permissions);
    }

    @Override
    public StatusHistoryEntity getConnectionStatusHistory(final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connection);
        final StatusHistoryDTO dto = controllerFacade.getConnectionStatusHistory(connectionId);
        return entityFactory.createStatusHistoryEntity(dto, permissions);
    }

    @Override
    public ConnectionStatisticsEntity getConnectionStatistics(final String connectionId) {
        final Connection connection = connectionDAO.getConnection(connectionId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(connection);
        final ConnectionStatisticsDTO dto = dtoFactory.createConnectionStatisticsDto(connection, controllerFacade.getConnectionStatusAnalytics(connectionId));
        return entityFactory.createConnectionStatisticsEntity(dto, permissions);
    }

    private ProcessorEntity createProcessorEntity(final ProcessorNode processor, final NiFiUser user) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(processor.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processor, user);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(processor));
        final ProcessorStatusDTO status = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processor.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processor.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        final ProcessorDTO processorDTO = dtoFactory.createProcessorDto(processor);
        return entityFactory.createProcessorEntity(processorDTO, revision, permissions, operatePermissions, status, bulletinEntities);
    }

    @Override
    public Set<ProcessorEntity> getProcessors(final String groupId, final boolean includeDescendants) {
        final Set<ProcessorNode> processors = processorDAO.getProcessors(groupId, includeDescendants);
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        return processors.stream()
            .map(processor -> createProcessorEntity(processor, user))
            .collect(Collectors.toSet());
    }

    @Override
    public ProcessorsRunStatusDetailsEntity getProcessorsRunStatusDetails(final Set<String> processorIds, final NiFiUser user) {
        final List<ProcessorRunStatusDetailsEntity> runStatusDetails = processorIds.stream()
            .map(processorDAO::getProcessor)
            .map(processor -> createRunStatusDetailsEntity(processor, user))
            .collect(Collectors.toList());

        final ProcessorsRunStatusDetailsEntity entity = new ProcessorsRunStatusDetailsEntity();
        entity.setRunStatusDetails(runStatusDetails);
        return entity;
    }

    private ProcessorRunStatusDetailsEntity createRunStatusDetailsEntity(final ProcessorNode processor, final NiFiUser user) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(processor.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processor, user);
        final ProcessorStatus processorStatus = controllerFacade.getProcessorStatus(processor.getIdentifier());
        final ProcessorRunStatusDetailsDTO runStatusDetailsDto = dtoFactory.createProcessorRunStatusDetailsDto(processor, processorStatus);

        if (!permissions.getCanRead()) {
            runStatusDetailsDto.setName(null);
            runStatusDetailsDto.setValidationErrors(null);
        }

        final ProcessorRunStatusDetailsEntity entity = new ProcessorRunStatusDetailsEntity();
        entity.setPermissions(permissions);
        entity.setRevision(revision);
        entity.setRunStatusDetails(runStatusDetailsDto);
        return entity;
    }

    @Override
    public Set<DocumentedTypeDTO> getWorkQueuePrioritizerTypes() {
        return controllerFacade.getFlowFileComparatorTypes();
    }

    @Override
    public Set<DocumentedTypeDTO> getProcessorTypes(final String bundleGroup, final String bundleArtifact, final String type) {
        return controllerFacade.getFlowFileProcessorTypes(bundleGroup, bundleArtifact, type);
    }

    @Override
    public Set<DocumentedTypeDTO> getControllerServiceTypes(final String serviceType, final String serviceBundleGroup, final String serviceBundleArtifact, final String serviceBundleVersion,
                                                            final String bundleGroup, final String bundleArtifact, final String type) {
        return controllerFacade.getControllerServiceTypes(serviceType, serviceBundleGroup, serviceBundleArtifact, serviceBundleVersion, bundleGroup, bundleArtifact, type);
    }

    @Override
    public Set<DocumentedTypeDTO> getReportingTaskTypes(final String bundleGroup, final String bundleArtifact, final String type) {
        return controllerFacade.getReportingTaskTypes(bundleGroup, bundleArtifact, type);
    }

    @Override
    public Set<DocumentedTypeDTO> getFlowRegistryTypes() {
        return controllerFacade.getFlowRegistryTypes();
    }

    @Override
    public RuntimeManifest getRuntimeManifest() {
        return controllerFacade.getRuntimeManifest();
    }

    @Override
    public ProcessorDefinition getProcessorDefinition(String group, String artifact, String version, String type) {
        final ProcessorDefinition processorDefinition = controllerFacade.getProcessorDefinition(group, artifact, version, type);
        if (processorDefinition == null) {
            throw new ResourceNotFoundException("Unable to find definition for [%s:%s:%s:%s]".formatted(group, artifact, version, type));
        }
        return processorDefinition;
    }

    @Override
    public ControllerServiceDefinition getControllerServiceDefinition(String group, String artifact, String version, String type) {
        final ControllerServiceDefinition controllerServiceDefinition = controllerFacade.getControllerServiceDefinition(group, artifact, version, type);
        if (controllerServiceDefinition == null) {
            throw new ResourceNotFoundException("Unable to find definition for [%s:%s:%s:%s]".formatted(group, artifact, version, type));
        }
        return controllerServiceDefinition;
    }

    @Override
    public ReportingTaskDefinition getReportingTaskDefinition(String group, String artifact, String version, String type) {
        final ReportingTaskDefinition reportingTaskDefinition = controllerFacade.getReportingTaskDefinition(group, artifact, version, type);
        if (reportingTaskDefinition == null) {
            throw new ResourceNotFoundException("Unable to find definition for [%s:%s:%s:%s]".formatted(group, artifact, version, type));
        }
        return reportingTaskDefinition;
    }

    @Override
    public ParameterProviderDefinition getParameterProviderDefinition(String group, String artifact, String version, String type) {
        final ParameterProviderDefinition parameterProviderDefinition = controllerFacade.getParameterProviderDefinition(group, artifact, version, type);
        if (parameterProviderDefinition == null) {
            throw new ResourceNotFoundException("Unable to find definition for [%s:%s:%s:%s]".formatted(group, artifact, version, type));
        }
        return parameterProviderDefinition;
    }

    @Override
    public FlowAnalysisRuleDefinition getFlowAnalysisRuleDefinition(String group, String artifact, String version, String type) {
        final FlowAnalysisRuleDefinition flowAnalysisRuleDefinition = controllerFacade.getFlowAnalysisRuleDefinition(group, artifact, version, type);
        if (flowAnalysisRuleDefinition == null) {
            throw new ResourceNotFoundException("Unable to find definition for [%s:%s:%s:%s]".formatted(group, artifact, version, type));
        }
        return flowAnalysisRuleDefinition;
    }

    @Override
    public String getAdditionalDetails(String group, String artifact, String version, String type) {
        return controllerFacade.getAdditionalDetails(group, artifact, version, type);
    }

    @Override
    public Set<DocumentedTypeDTO> getParameterProviderTypes(final String bundleGroup, final String bundleArtifact, final String type) {
        return controllerFacade.getParameterProviderTypes(bundleGroup, bundleArtifact, type);
    }

    @Override
    public ProcessorEntity getProcessor(final String id) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        return createProcessorEntity(processor, NiFiUserUtils.getNiFiUser());
    }

    @Override
    public PropertyDescriptorDTO getProcessorPropertyDescriptor(final String id, final String property, final boolean sensitive) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        final PropertyDescriptor descriptor = getPropertyDescriptor(processor, property, sensitive);
        return dtoFactory.createPropertyDescriptorDto(descriptor, processor.getProcessGroup().getIdentifier());
    }

    @Override
    public ProcessorStatusEntity getProcessorStatus(final String id) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processor);
        final ProcessorStatusDTO dto = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(id));
        return entityFactory.createProcessorStatusEntity(dto, permissions);
    }

    @Override
    public StatusHistoryEntity getProcessorStatusHistory(final String id) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processor);
        final StatusHistoryDTO dto = controllerFacade.getProcessorStatusHistory(id);
        return entityFactory.createStatusHistoryEntity(dto, permissions);
    }

    @Override
    public StatusHistoryEntity getNodeStatusHistory() {
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerFacade, NiFiUserUtils.getNiFiUser());
        final StatusHistoryDTO dto = controllerFacade.getNodeStatusHistory();
        return entityFactory.createStatusHistoryEntity(dto, permissions);
    }

    private boolean authorizeBulletin(final Bulletin bulletin) {
        final String sourceId = bulletin.getSourceId();
        final ComponentType type = bulletin.getSourceType();

        final Authorizable authorizable;
        try {
            authorizable = switch (type) {
                case PROCESSOR -> authorizableLookup.getProcessor(sourceId).getAuthorizable();
                case REPORTING_TASK -> authorizableLookup.getReportingTask(sourceId).getAuthorizable();
                case FLOW_ANALYSIS_RULE -> authorizableLookup.getFlowAnalysisRule(sourceId).getAuthorizable();
                case PARAMETER_PROVIDER -> authorizableLookup.getParameterProvider(sourceId).getAuthorizable();
                case CONTROLLER_SERVICE -> authorizableLookup.getControllerService(sourceId).getAuthorizable();
                case FLOW_CONTROLLER -> controllerFacade;
                case INPUT_PORT -> authorizableLookup.getInputPort(sourceId);
                case OUTPUT_PORT -> authorizableLookup.getOutputPort(sourceId);
                case REMOTE_PROCESS_GROUP -> authorizableLookup.getRemoteProcessGroup(sourceId);
                case PROCESS_GROUP -> authorizableLookup.getProcessGroup(sourceId).getAuthorizable();
                default ->
                        throw new WebApplicationException(Response.serverError().entity("An unexpected type of component is the source of this bulletin.").build());
            };
        } catch (final ResourceNotFoundException e) {
            // if the underlying component is gone, disallow
            return false;
        }

        // perform the authorization
        final AuthorizationResult result = authorizable.checkAuthorization(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        return Result.Approved.equals(result.getResult());
    }

    @Override
    public BulletinBoardDTO getBulletinBoard(final BulletinQueryDTO query) {
        // build the query
        final BulletinQuery.Builder queryBuilder = new BulletinQuery.Builder()
                .groupIdMatches(query.getGroupId())
                .sourceIdMatches(query.getSourceId())
                .nameMatches(query.getName())
                .messageMatches(query.getMessage())
                .after(query.getAfter())
                .limit(query.getLimit());

        // perform the query
        final List<Bulletin> results = bulletinRepository.findBulletins(queryBuilder.build());

        // perform the query and generate the results - iterating in reverse order since we are
        // getting the most recent results by ordering by timestamp desc above. this gets the
        // exact results we want but in reverse order
        final List<BulletinEntity> bulletinEntities = new ArrayList<>();
        for (final ListIterator<Bulletin> bulletinIter = results.listIterator(results.size()); bulletinIter.hasPrevious(); ) {
            final Bulletin bulletin = bulletinIter.previous();
            bulletinEntities.add(entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), authorizeBulletin(bulletin)));
        }

        // create the bulletin board
        final BulletinBoardDTO bulletinBoard = new BulletinBoardDTO();
        bulletinBoard.setBulletins(bulletinEntities);
        bulletinBoard.setGenerated(new Date());
        return bulletinBoard;
    }

    @Override
    public SystemDiagnosticsDTO getSystemDiagnostics(final DiagnosticLevel diagnosticLevel) {
        final SystemDiagnostics sysDiagnostics = controllerFacade.getSystemDiagnostics();
        return dtoFactory.createSystemDiagnosticsDto(sysDiagnostics, diagnosticLevel);
    }

    @Override
    public List<ResourceDTO> getResources() {
        final List<Resource> resources = controllerFacade.getResources();
        final List<ResourceDTO> resourceDtos = new ArrayList<>(resources.size());
        for (final Resource resource : resources) {
            resourceDtos.add(dtoFactory.createResourceDto(resource));
        }
        return resourceDtos;
    }

    @Override
    public void discoverCompatibleBundles(final VersionedProcessGroup versionedGroup) {
        BundleUtils.discoverCompatibleBundles(controllerFacade.getExtensionManager(), versionedGroup);
    }

    @Override
    public void discoverCompatibleBundles(final VersionedReportingTaskSnapshot reportingTaskSnapshot) {
        BundleUtils.discoverCompatibleBundles(controllerFacade.getExtensionManager(), reportingTaskSnapshot);
    }

    @Override
    public Set<String> resolveParameterProviders(final RegisteredFlowSnapshot versionedFlowSnapshot, final NiFiUser user) {
        final Map<String, ParameterProviderReference> parameterProviderReferences = versionedFlowSnapshot.getParameterProviders();
        if (parameterProviderReferences == null || parameterProviderReferences.isEmpty()
                || versionedFlowSnapshot.getParameterContexts() == null || versionedFlowSnapshot.getParameterContexts().isEmpty()) {
            return Collections.emptySet();
        }

        final Set<ParameterProviderNode> parameterProviderNodes = parameterProviderDAO.getParameterProviders().stream()
                .filter(provider -> provider.isAuthorized(authorizer, RequestAction.READ, user))
                .collect(Collectors.toSet());

        final Set<String> unresolvedParameterProviderIds = new HashSet<>();
        for (final VersionedParameterContext parameterContext : versionedFlowSnapshot.getParameterContexts().values()) {
            final String proposedParameterProviderId = parameterContext.getParameterProvider();

            if (proposedParameterProviderId != null) {
                // attempt to resolve the parameter provider
                resolveParameterProvider(parameterContext, parameterProviderNodes, parameterProviderReferences);

                // if the parameter provider is unchanged it means that the referenced provider is not in
                // parameter provider nodes because it doesn't exist or the user does not have access to
                // it. it could also be unchanged if the id already matches an available provider.
                final String resolvedParameterProviderId = parameterContext.getParameterProvider();
                if (proposedParameterProviderId.equals(resolvedParameterProviderId)) {
                    unresolvedParameterProviderIds.add(proposedParameterProviderId);
                }
            }
        }

        return unresolvedParameterProviderIds;
    }

    private void resolveParameterProvider(final VersionedParameterContext parameterContext, final Set<ParameterProviderNode> availableParameterProviders,
                                          final Map<String, ParameterProviderReference> parameterProviderReferences) {

        final String referencedParameterProviderId = parameterContext.getParameterProvider();
        if (referencedParameterProviderId == null) {
            return;
        }

        final ParameterProviderReference parameterProviderReference = parameterProviderReferences == null ? null : parameterProviderReferences.get(referencedParameterProviderId);
        if (parameterProviderReference == null) {
            return;
        }
        final ExtensionManager extensionManager = controllerFacade.getExtensionManager();
        final BundleCoordinate compatibleBundle = BundleUtils.discoverCompatibleBundle(extensionManager, parameterProviderReference.getType(), parameterProviderReference.getBundle());
        final ConfigurableComponent tempComponent = extensionManager.getTempComponent(parameterProviderReference.getType(), compatibleBundle);
        final Class<?> referencedProviderClass = tempComponent.getClass();

        final String parameterProviderReferenceName = parameterProviderReference.getName();
        final List<ParameterProviderNode> matchingParameterProviders = availableParameterProviders.stream()
                .filter(provider -> provider.getName().equals(parameterProviderReferenceName))
                .filter(provider -> referencedProviderClass.isAssignableFrom(provider.getParameterProvider().getClass()))
                .collect(Collectors.toList());

        if (matchingParameterProviders.size() != 1) {
            return;
        }

        final ParameterProviderNode matchingProviderNode = matchingParameterProviders.get(0);
        final String resolvedId = matchingProviderNode.getIdentifier();

        parameterContext.setParameterProvider(resolvedId);
    }

    @Override
    public Set<String> resolveInheritedControllerServices(final FlowSnapshotContainer flowSnapshotContainer, final String processGroupId, final NiFiUser user) {
        return controllerFacade.getControllerServiceResolver().resolveInheritedControllerServices(flowSnapshotContainer, processGroupId, user);
    }

    @Override
    public BundleCoordinate getCompatibleBundle(String type, BundleDTO bundleDTO) {
        return BundleUtils.getCompatibleBundle(controllerFacade.getExtensionManager(), type, bundleDTO);
    }

    @Override
    public ConfigurableComponent getTempComponent(String classType, BundleCoordinate bundleCoordinate) {
        return controllerFacade.getExtensionManager().getTempComponent(classType, bundleCoordinate);
    }

    /**
     * Ensures the specified user has permission to access the specified port. This method does
     * not utilize the DataTransferAuthorizable as that will enforce the entire chain is
     * authorized for the transfer. This method is only invoked when obtaining the site to site
     * details so the entire chain isn't necessary.
     */
    private boolean isUserAuthorized(final NiFiUser user, final Port port) {
        final boolean isSiteToSiteSecure = Boolean.TRUE.equals(properties.isSiteToSiteSecure());

        // if site to site is not secure, allow all users
        if (!isSiteToSiteSecure) {
            return true;
        }

        final Map<String, String> userContext;
        if (user.getClientAddress() != null && !user.getClientAddress().trim().isEmpty()) {
            userContext = new HashMap<>();
            userContext.put(UserContextKeys.CLIENT_ADDRESS.name(), user.getClientAddress());
        } else {
            userContext = null;
        }

        final AuthorizationRequest request = new AuthorizationRequest.Builder()
                .resource(ResourceFactory.getDataTransferResource(port.getResource()))
                .identity(user.getIdentity())
                .groups(user.getAllGroups())
                .anonymous(user.isAnonymous())
                .accessAttempt(false)
                .action(RequestAction.WRITE)
                .userContext(userContext)
                .explanationSupplier(() -> "Unable to retrieve port details.")
                .build();

        final AuthorizationResult result = authorizer.authorize(request);
        return Result.Approved.equals(result.getResult());
    }

    @Override
    public ControllerDTO getSiteToSiteDetails() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        // serialize the input ports this NiFi has access to
        final Set<PortDTO> inputPortDtos = new LinkedHashSet<>();
        for (final Port inputPort : controllerFacade.getPublicInputPorts()) {
            if (isUserAuthorized(user, inputPort)) {
                final PortDTO dto = new PortDTO();
                dto.setId(inputPort.getIdentifier());
                dto.setName(inputPort.getName());
                dto.setComments(inputPort.getComments());
                dto.setState(inputPort.getScheduledState().toString());
                inputPortDtos.add(dto);
            }
        }

        // serialize the output ports this NiFi has access to
        final Set<PortDTO> outputPortDtos = new LinkedHashSet<>();
        for (final Port outputPort : controllerFacade.getPublicOutputPorts()) {
            if (isUserAuthorized(user, outputPort)) {
                final PortDTO dto = new PortDTO();
                dto.setId(outputPort.getIdentifier());
                dto.setName(outputPort.getName());
                dto.setComments(outputPort.getComments());
                dto.setState(outputPort.getScheduledState().toString());
                outputPortDtos.add(dto);
            }
        }

        // get the root group
        final ProcessGroup rootGroup = processGroupDAO.getProcessGroup(controllerFacade.getRootGroupId());
        final ProcessGroupCounts counts = rootGroup.getCounts();

        // create the controller dto
        final ControllerDTO controllerDTO = new ControllerDTO();
        controllerDTO.setId(controllerFacade.getRootGroupId());
        controllerDTO.setInstanceId(controllerFacade.getInstanceId());
        controllerDTO.setName(controllerFacade.getName());
        controllerDTO.setComments(controllerFacade.getComments());
        controllerDTO.setInputPorts(inputPortDtos);
        controllerDTO.setOutputPorts(outputPortDtos);
        controllerDTO.setInputPortCount(inputPortDtos.size());
        controllerDTO.setOutputPortCount(outputPortDtos.size());
        controllerDTO.setRunningCount(counts.getRunningCount());
        controllerDTO.setStoppedCount(counts.getStoppedCount());
        controllerDTO.setInvalidCount(counts.getInvalidCount());
        controllerDTO.setDisabledCount(counts.getDisabledCount());

        // determine the site to site configuration
        controllerDTO.setRemoteSiteListeningPort(controllerFacade.getRemoteSiteListeningPort());
        controllerDTO.setRemoteSiteHttpListeningPort(controllerFacade.getRemoteSiteListeningHttpPort());
        controllerDTO.setSiteToSiteSecure(controllerFacade.isRemoteSiteCommsSecure());

        return controllerDTO;
    }

    @Override
    public ControllerConfigurationEntity getControllerConfiguration() {
        final Revision rev = revisionManager.getRevision(FlowController.class.getSimpleName());
        final ControllerConfigurationDTO dto = dtoFactory.createControllerConfigurationDto(controllerFacade);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(controllerFacade);
        final RevisionDTO revision = dtoFactory.createRevisionDTO(rev);
        return entityFactory.createControllerConfigurationEntity(dto, revision, permissions);
    }

    @Override
    public VersionedReportingTaskSnapshot getVersionedReportingTaskSnapshot(final String reportingTaskId) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final ReportingTaskNode reportingTaskNode = reportingTaskDAO.getReportingTask(reportingTaskId);
        return getVersionedReportingTaskSnapshot(Collections.singleton(reportingTaskNode), user);
    }

    @Override
    public VersionedReportingTaskSnapshot getVersionedReportingTaskSnapshot() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final Set<ReportingTaskNode> reportingTaskNodes = reportingTaskDAO.getReportingTasks();
        return getVersionedReportingTaskSnapshot(reportingTaskNodes, user);
    }

    private VersionedReportingTaskSnapshot getVersionedReportingTaskSnapshot(final Set<ReportingTaskNode> reportingTaskNodes, final NiFiUser user) {
        final Set<ControllerServiceNode> serviceNodes = new HashSet<>();
        reportingTaskNodes.forEach(reportingTaskNode -> {
            reportingTaskNode.authorize(authorizer, RequestAction.READ, user);
            findReferencedControllerServices(reportingTaskNode, serviceNodes, user);
        });

        final ExtensionManager extensionManager = controllerFacade.getExtensionManager();
        final ControllerServiceProvider serviceProvider = controllerFacade.getControllerServiceProvider();
        final VersionedReportingTaskSnapshotMapper snapshotMapper = new VersionedReportingTaskSnapshotMapper(extensionManager, serviceProvider);
        return snapshotMapper.createMapping(reportingTaskNodes, serviceNodes);
    }

    private void findReferencedControllerServices(final ComponentNode componentNode, final Set<ControllerServiceNode> serviceNodes, final NiFiUser user) {
        componentNode.getPropertyDescriptors().forEach(descriptor -> {
            if (descriptor.getControllerServiceDefinition() != null) {
                final String serviceId = componentNode.getEffectivePropertyValue(descriptor);
                if (serviceId != null) {
                    try {
                        final ControllerServiceNode serviceNode = controllerServiceDAO.getControllerService(serviceId);
                        serviceNode.authorize(authorizer, RequestAction.READ, user);
                        if (serviceNodes.add(serviceNode)) {
                            findReferencedControllerServices(serviceNode, serviceNodes, user);
                        }
                    } catch (ResourceNotFoundException ignored) {
                        // ignore if the resource is not found, if the referenced service was previously deleted, it should not stop this action
                    }
                }
            }
        });
    }

    @Override
    public void generateIdentifiersForImport(final VersionedReportingTaskSnapshot reportingTaskSnapshot, final Supplier<String> idGenerator) {
        final List<VersionedReportingTask> reportingTasks = Optional.ofNullable(reportingTaskSnapshot.getReportingTasks()).orElse(Collections.emptyList());
        final List<VersionedControllerService> controllerServices = Optional.ofNullable(reportingTaskSnapshot.getControllerServices()).orElse(Collections.emptyList());

        // First generate all new ids and instance ids for each component, maintaining a mapping from old id to new instance id, the instance id
        // will be used to create the component, so we want to update any CS references to use the instance id
        final Map<String, String> oldIdToNewInstanceIdMap = new HashMap<>();
        controllerServices.forEach(controllerService -> generateIdentifiersForImport(controllerService, idGenerator, oldIdToNewInstanceIdMap));
        reportingTasks.forEach(reportingTask -> generateIdentifiersForImport(reportingTask, idGenerator, oldIdToNewInstanceIdMap));

        // Now go back through all components and update any property values that referenced old CS ids
        controllerServices.forEach(controllerService -> updateControllerServiceReferences(controllerService, oldIdToNewInstanceIdMap));
        reportingTasks.forEach(reportingTask -> updateControllerServiceReferences(reportingTask, oldIdToNewInstanceIdMap));
    }

    private void generateIdentifiersForImport(final VersionedConfigurableExtension extension, final Supplier<String> idGenerator,
                                              final Map<String, String> oldIdToNewIdMap) {
        final String identifier = idGenerator.get();
        final String instanceIdentifier = idGenerator.get();
        oldIdToNewIdMap.put(extension.getIdentifier(), instanceIdentifier);
        extension.setIdentifier(identifier);
        extension.setInstanceIdentifier(instanceIdentifier);
    }

    private void updateControllerServiceReferences(final VersionedConfigurableExtension extension, final Map<String, String> oldIdToNewIdMap) {
        final Map<String, String> propertyValues = Optional.ofNullable(extension.getProperties()).orElse(Collections.emptyMap());
        final Map<String, VersionedPropertyDescriptor> propertyDescriptors = Optional.ofNullable(extension.getPropertyDescriptors()).orElse(Collections.emptyMap());

        propertyDescriptors.forEach((propName, propDescriptor) -> {
            if (propDescriptor.getIdentifiesControllerService()) {
                final String oldServiceId = propertyValues.get(propName);
                if (oldServiceId != null) {
                    final String newServiceId = oldIdToNewIdMap.get(oldServiceId);
                    propertyValues.put(propName, newServiceId);
                }
            }
        });
    }

    @Override
    public VersionedReportingTaskImportResponseEntity importReportingTasks(final VersionedReportingTaskSnapshot reportingTaskSnapshot) {
        final VersionedReportingTaskImporter reportingTaskImporter = controllerFacade.createReportingTaskImporter();
        final VersionedReportingTaskImportResult importResult = reportingTaskImporter.importSnapshot(reportingTaskSnapshot);

        controllerFacade.save();

        final Set<ReportingTaskEntity> reportingTaskEntities = importResult.getReportingTaskNodes().stream()
                .map(this::createReportingTaskEntity)
                .collect(Collectors.toSet());

        final Set<ControllerServiceEntity> controllerServiceEntities = importResult.getControllerServiceNodes().stream()
                .map(serviceNode -> createControllerServiceEntity(serviceNode, false))
                .collect(Collectors.toSet());

        final VersionedReportingTaskImportResponseEntity importResponseEntity = new VersionedReportingTaskImportResponseEntity();
        importResponseEntity.setReportingTasks(reportingTaskEntities);
        importResponseEntity.setControllerServices(controllerServiceEntities);
        return importResponseEntity;
    }

    @Override
    public ControllerBulletinsEntity getControllerBulletins() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final ControllerBulletinsEntity controllerBulletinsEntity = new ControllerBulletinsEntity();

        final List<BulletinEntity> controllerBulletinEntities = new ArrayList<>();

        final Authorizable controllerAuthorizable = authorizableLookup.getController();
        final boolean authorized = controllerAuthorizable.isAuthorized(authorizer, RequestAction.READ, user);
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForController());
        controllerBulletinEntities.addAll(bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, authorized)).collect(Collectors.toList()));

        // get the controller service bulletins
        final BulletinQuery controllerServiceQuery = new BulletinQuery.Builder().sourceType(ComponentType.CONTROLLER_SERVICE).build();
        final List<Bulletin> allControllerServiceBulletins = bulletinRepository.findBulletins(controllerServiceQuery);
        final List<BulletinEntity> controllerServiceBulletinEntities = new ArrayList<>();
        for (final Bulletin bulletin : allControllerServiceBulletins) {
            try {
                final Authorizable controllerServiceAuthorizable = authorizableLookup.getControllerService(bulletin.getSourceId()).getAuthorizable();
                final boolean controllerServiceAuthorized = controllerServiceAuthorizable.isAuthorized(authorizer, RequestAction.READ, user);

                final BulletinEntity controllerServiceBulletin = entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), controllerServiceAuthorized);
                controllerServiceBulletinEntities.add(controllerServiceBulletin);
                controllerBulletinEntities.add(controllerServiceBulletin);
            } catch (final ResourceNotFoundException ignored) {
                // controller service missing.. skip
            }
        }
        controllerBulletinsEntity.setControllerServiceBulletins(controllerServiceBulletinEntities);

        // get the reporting task bulletins
        final BulletinQuery reportingTaskQuery = new BulletinQuery.Builder().sourceType(ComponentType.REPORTING_TASK).build();
        final List<Bulletin> allReportingTaskBulletins = bulletinRepository.findBulletins(reportingTaskQuery);
        final List<BulletinEntity> reportingTaskBulletinEntities = new ArrayList<>();
        for (final Bulletin bulletin : allReportingTaskBulletins) {
            try {
                final Authorizable reportingTaskAuthorizable = authorizableLookup.getReportingTask(bulletin.getSourceId()).getAuthorizable();
                final boolean reportingTaskAuthorizableAuthorized = reportingTaskAuthorizable.isAuthorized(authorizer, RequestAction.READ, user);

                final BulletinEntity reportingTaskBulletin = entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), reportingTaskAuthorizableAuthorized);
                reportingTaskBulletinEntities.add(reportingTaskBulletin);
                controllerBulletinEntities.add(reportingTaskBulletin);
            } catch (final ResourceNotFoundException ignored) {
                // reporting task missing.. skip
            }
        }
        controllerBulletinsEntity.setReportingTaskBulletins(reportingTaskBulletinEntities);

        // get the flow analysis rule bulletins
        final BulletinQuery flowAnalysisRuleQuery = new BulletinQuery.Builder().sourceType(ComponentType.FLOW_ANALYSIS_RULE).build();
        final List<Bulletin> allFlowAnalysisRuleBulletins = bulletinRepository.findBulletins(flowAnalysisRuleQuery);
        final List<BulletinEntity> flowAnalysisRuleBulletinEntities = new ArrayList<>();
        for (final Bulletin bulletin : allFlowAnalysisRuleBulletins) {
            try {
                final Authorizable flowAnalysisRuleAuthorizable = authorizableLookup.getFlowAnalysisRule(bulletin.getSourceId()).getAuthorizable();
                final boolean flowAnalysisRuleAuthorizableAuthorized = flowAnalysisRuleAuthorizable.isAuthorized(authorizer, RequestAction.READ, user);

                final BulletinEntity flowAnalysisRuleBulletin = entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), flowAnalysisRuleAuthorizableAuthorized);
                flowAnalysisRuleBulletinEntities.add(flowAnalysisRuleBulletin);
                controllerBulletinEntities.add(flowAnalysisRuleBulletin);
            } catch (final ResourceNotFoundException ignored) {
                // flow analysis rule missing.. skip
            }
        }
        controllerBulletinsEntity.setFlowAnalysisRuleBulletins(flowAnalysisRuleBulletinEntities);

        // get the parameter provider bulletins
        final BulletinQuery parameterProviderQuery = new BulletinQuery.Builder().sourceType(ComponentType.PARAMETER_PROVIDER).build();
        final List<Bulletin> allParameterProviderBulletins = bulletinRepository.findBulletins(parameterProviderQuery);
        final List<BulletinEntity> parameterProviderBulletinEntities = new ArrayList<>();
        for (final Bulletin bulletin : allParameterProviderBulletins) {
            try {
                final Authorizable parameterProviderAuthorizable = authorizableLookup.getParameterProvider(bulletin.getSourceId()).getAuthorizable();
                final boolean parameterProviderAuthorizableAuthorized = parameterProviderAuthorizable.isAuthorized(authorizer, RequestAction.READ, user);

                final BulletinEntity parameterProviderBulletin = entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), parameterProviderAuthorizableAuthorized);
                parameterProviderBulletinEntities.add(parameterProviderBulletin);
                controllerBulletinEntities.add(parameterProviderBulletin);
            } catch (final ResourceNotFoundException ignored) {
                // parameter provider missing.. skip
            }
        }
        controllerBulletinsEntity.setParameterProviderBulletins(parameterProviderBulletinEntities);

        // get the flow registry client task bulletins
        final BulletinQuery flowRegistryClientQuery = new BulletinQuery.Builder().sourceType(ComponentType.FLOW_REGISTRY_CLIENT).build();
        final List<Bulletin> allFlowClientRegistryBulletins = bulletinRepository.findBulletins(flowRegistryClientQuery);
        final List<BulletinEntity> flowRegistryClientBulletinEntities = new ArrayList<>();
        for (final Bulletin bulletin : allFlowClientRegistryBulletins) {
            try {
                final Authorizable flowRegistryClientAuthorizable = authorizableLookup.getFlowRegistryClient(bulletin.getSourceId()).getAuthorizable();
                final boolean flowRegistryClientkAuthorizableAuthorized = flowRegistryClientAuthorizable.isAuthorized(authorizer, RequestAction.READ, user);

                final BulletinEntity flowRegistryClient = entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), flowRegistryClientkAuthorizableAuthorized);
                flowRegistryClientBulletinEntities.add(flowRegistryClient);
                controllerBulletinEntities.add(flowRegistryClient);
            } catch (final ResourceNotFoundException ignored) {
                // flow registry client missing.. skip
            }
        }
        controllerBulletinsEntity.setFlowRegistryClientBulletins(flowRegistryClientBulletinEntities);

        controllerBulletinsEntity.setBulletins(pruneAndSortBulletins(controllerBulletinEntities, BulletinRepository.MAX_BULLETINS_FOR_CONTROLLER));
        return controllerBulletinsEntity;
    }

    @Override
    public FlowConfigurationEntity getFlowConfiguration() {
        final FlowConfigurationDTO dto = dtoFactory.createFlowConfigurationDto(properties.getDefaultBackPressureObjectThreshold(), properties.getDefaultBackPressureDataSizeThreshold());
        final FlowConfigurationEntity entity = new FlowConfigurationEntity();
        entity.setFlowConfiguration(dto);
        return entity;
    }

    @Override
    public AccessPolicyEntity getAccessPolicy(final String accessPolicyId) {
        final AccessPolicy accessPolicy = accessPolicyDAO.getAccessPolicy(accessPolicyId);
        return createAccessPolicyEntity(accessPolicy);
    }

    @Override
    public AccessPolicyEntity getAccessPolicy(final RequestAction requestAction, final String resource) {
        Authorizable authorizable;
        try {
            authorizable = authorizableLookup.getAuthorizableFromResource(resource);
        } catch (final ResourceNotFoundException e) {
            // unable to find the underlying authorizable... user authorized based on top level /policies... create
            // an anonymous authorizable to attempt to locate an existing policy for this resource
            authorizable = new Authorizable() {
                @Override
                public Authorizable getParentAuthorizable() {
                    return null;
                }

                @Override
                public Resource getResource() {
                    return new Resource() {
                        @Override
                        public String getIdentifier() {
                            return resource;
                        }

                        @Override
                        public String getName() {
                            return resource;
                        }

                        @Override
                        public String getSafeDescription() {
                            return "Policy " + resource;
                        }
                    };
                }
            };
        }

        final AccessPolicy accessPolicy = accessPolicyDAO.getAccessPolicy(requestAction, authorizable);
        return createAccessPolicyEntity(accessPolicy);
    }

    private AccessPolicyEntity createAccessPolicyEntity(final AccessPolicy accessPolicy) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(accessPolicy.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getAccessPolicyById(accessPolicy.getIdentifier()));
        final ComponentReferenceEntity componentReference = createComponentReferenceEntity(accessPolicy.getResource());
        return entityFactory.createAccessPolicyEntity(
                dtoFactory.createAccessPolicyDto(accessPolicy,
                        accessPolicy.getGroups().stream().map(mapUserGroupIdToTenantEntity(false)).collect(Collectors.toSet()),
                        accessPolicy.getUsers().stream().map(mapUserIdToTenantEntity(false)).collect(Collectors.toSet()), componentReference),
                revision, permissions);
    }

    @Override
    public UserEntity getUser(final String userId) {
        final User user = userDAO.getUser(userId);
        return createUserEntity(user, true);
    }

    @Override
    public Set<UserEntity> getUsers() {
        final Set<User> users = userDAO.getUsers();
        return users.stream()
            .map(user -> createUserEntity(user, false))
            .collect(Collectors.toSet());
    }

    /**
     * Search for User and Group Tenants with optimized conversion from specific objects to Tenant objects
     *
     * @param query Search query where null or empty returns unfiltered results
     * @return Tenants Entity containing zero or more matching Users and Groups
     */
    @Override
    public TenantsEntity searchTenants(final String query) {
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());

        final Set<TenantEntity> usersFound = userDAO.getUsers()
                .stream()
                .filter(user -> isMatched(user.getIdentity(), query))
                .map(user -> {
                    final TenantDTO tenant = dtoFactory.createTenantDTO(user);
                    final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(tenant.getId()));
                    return entityFactory.createTenantEntity(tenant, revision, permissions);
                })
                .collect(Collectors.toSet());

        final Set<TenantEntity> userGroupsFound = userGroupDAO.getUserGroups()
                .stream()
                .filter(userGroup -> isMatched(userGroup.getName(), query))
                .map(userGroup -> {
                    final TenantDTO tenant = dtoFactory.createTenantDTO(userGroup);
                    final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(tenant.getId()));
                    return entityFactory.createTenantEntity(tenant, revision, permissions);
                })
                .collect(Collectors.toSet());

        final TenantsEntity tenantsEntity = new TenantsEntity();
        tenantsEntity.setUsers(usersFound);
        tenantsEntity.setUserGroups(userGroupsFound);
        return tenantsEntity;
    }

    private boolean isMatched(final String label, final String query) {
        return StringUtils.isEmpty(query) || containsIgnoreCase(label, query);
    }

    private UserEntity createUserEntity(final User user, final boolean enforceUserExistence) {
        final RevisionDTO userRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(user.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        final Set<TenantEntity> userGroups = userGroupDAO.getUserGroupsForUser(user.getIdentifier()).stream()
                .map(g -> g.getIdentifier()).map(mapUserGroupIdToTenantEntity(enforceUserExistence)).collect(Collectors.toSet());
        final Set<AccessPolicySummaryEntity> policyEntities = userGroupDAO.getAccessPoliciesForUser(user.getIdentifier()).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());
        return entityFactory.createUserEntity(dtoFactory.createUserDto(user, userGroups, policyEntities), userRevision, permissions);
    }

    private UserGroupEntity createUserGroupEntity(final Group userGroup, final boolean enforceGroupExistence) {
        final RevisionDTO userGroupRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userGroup.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(authorizableLookup.getTenant());
        final Set<TenantEntity> users = userGroup.getUsers().stream().map(mapUserIdToTenantEntity(enforceGroupExistence)).collect(Collectors.toSet());
        final Set<AccessPolicySummaryEntity> policyEntities = userGroupDAO.getAccessPoliciesForUserGroup(userGroup.getIdentifier()).stream()
                .map(ap -> createAccessPolicySummaryEntity(ap)).collect(Collectors.toSet());
        return entityFactory.createUserGroupEntity(dtoFactory.createUserGroupDto(userGroup, users, policyEntities), userGroupRevision, permissions);
    }

    @Override
    public UserGroupEntity getUserGroup(final String userGroupId) {
        final Group userGroup = userGroupDAO.getUserGroup(userGroupId);
        return createUserGroupEntity(userGroup, true);
    }

    @Override
    public Set<UserGroupEntity> getUserGroups() {
        final Set<Group> userGroups = userGroupDAO.getUserGroups();
        return userGroups.stream()
            .map(userGroup -> createUserGroupEntity(userGroup, false))
            .collect(Collectors.toSet());
    }

    private LabelEntity createLabelEntity(final Label label) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(label.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(label);
        return entityFactory.createLabelEntity(dtoFactory.createLabelDto(label), revision, permissions);
    }

    @Override
    public Set<LabelEntity> getLabels(final String groupId) {
        final Set<Label> labels = labelDAO.getLabels(groupId);
        return labels.stream()
            .map(label -> createLabelEntity(label))
            .collect(Collectors.toSet());
    }

    @Override
    public LabelEntity getLabel(final String labelId) {
        final Label label = labelDAO.getLabel(labelId);
        return createLabelEntity(label);
    }

    private FunnelEntity createFunnelEntity(final Funnel funnel) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(funnel.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(funnel);
        return entityFactory.createFunnelEntity(dtoFactory.createFunnelDto(funnel), revision, permissions);
    }

    @Override
    public Set<FunnelEntity> getFunnels(final String groupId) {
        final Set<Funnel> funnels = funnelDAO.getFunnels(groupId);
        return funnels.stream()
            .map(funnel -> createFunnelEntity(funnel))
            .collect(Collectors.toSet());
    }

    @Override
    public FunnelEntity getFunnel(final String funnelId) {
        final Funnel funnel = funnelDAO.getFunnel(funnelId);
        return createFunnelEntity(funnel);
    }

    private PortEntity createInputPortEntity(final Port port) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(port.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port, NiFiUserUtils.getNiFiUser());
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(port), NiFiUserUtils.getNiFiUser());
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(port.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, permissions, operatePermissions, status, bulletinEntities);
    }

    private PortEntity createOutputPortEntity(final Port port) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(port.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(port, NiFiUserUtils.getNiFiUser());
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(port), NiFiUserUtils.getNiFiUser());
        final PortStatusDTO status = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(port.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(port.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createPortEntity(dtoFactory.createPortDto(port), revision, permissions, operatePermissions, status, bulletinEntities);
    }

    @Override
    public Set<PortEntity> getInputPorts(final String groupId) {
        final Set<Port> inputPorts = inputPortDAO.getPorts(groupId);
        return inputPorts.stream()
            .map(port -> createInputPortEntity(port))
            .collect(Collectors.toSet());
    }

    @Override
    public Set<PortEntity> getOutputPorts(final String groupId) {
        final Set<Port> ports = outputPortDAO.getPorts(groupId);
        return ports.stream()
            .map(port -> createOutputPortEntity(port))
            .collect(Collectors.toSet());
    }

    private ProcessGroupEntity createProcessGroupEntity(final ProcessGroup group) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(group.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(group);
        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(group.getIdentifier()));
        final List<BulletinEntity> bulletins = getProcessGroupBulletins(group);
        return entityFactory.createProcessGroupEntity(dtoFactory.createProcessGroupDto(group), revision, permissions, status, bulletins);
    }

    private List<BulletinEntity> getProcessGroupBulletins(final ProcessGroup group) {
        final List<Bulletin> bulletins = new ArrayList<>(bulletinRepository.findBulletinsForGroupBySource(group.getIdentifier()));

        for (final ProcessGroup descendantGroup : group.findAllProcessGroups()) {
            bulletins.addAll(bulletinRepository.findBulletinsForGroupBySource(descendantGroup.getIdentifier()));
        }

        List<BulletinEntity> bulletinEntities = new ArrayList<>();
        for (final Bulletin bulletin : bulletins) {
            bulletinEntities.add(entityFactory.createBulletinEntity(dtoFactory.createBulletinDto(bulletin), authorizeBulletin(bulletin)));
        }

        return pruneAndSortBulletins(bulletinEntities, BulletinRepository.MAX_BULLETINS_PER_COMPONENT);
    }

    private List<BulletinEntity> pruneAndSortBulletins(final List<BulletinEntity> bulletinEntities, final int maxBulletins) {
        // sort the bulletins
        Collections.sort(bulletinEntities, (o1, o2) -> {
            if (o1 == null && o2 == null) {
                return 0;
            }
            if (o1 == null) {
                return 1;
            }
            if (o2 == null) {
                return -1;
            }

            return -Long.compare(o1.getId(), o2.getId());
        });

        // prune the response to only include the max number of bulletins
        if (bulletinEntities.size() > maxBulletins) {
            return bulletinEntities.subList(0, maxBulletins);
        } else {
            return bulletinEntities;
        }
    }

    @Override
    public Set<ProcessGroupEntity> getProcessGroups(final String parentGroupId, final ProcessGroupRecursivity processGroupRecursivity) {
        final Set<ProcessGroup> groups = processGroupDAO.getProcessGroups(parentGroupId, processGroupRecursivity);
        return groups.stream()
            .map(this::createProcessGroupEntity)
            .collect(Collectors.toSet());
    }

    private RemoteProcessGroupEntity createRemoteGroupEntity(final RemoteProcessGroup rpg, final NiFiUser user) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(rpg.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(rpg, user);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(rpg), user);
        final RemoteProcessGroupStatusDTO status = dtoFactory.createRemoteProcessGroupStatusDto(rpg, controllerFacade.getRemoteProcessGroupStatus(rpg.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(rpg.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createRemoteProcessGroupEntity(dtoFactory.createRemoteProcessGroupDto(rpg), revision, permissions, operatePermissions, status, bulletinEntities);
    }

    @Override
    public Set<RemoteProcessGroupEntity> getRemoteProcessGroups(final String groupId) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final Set<RemoteProcessGroup> rpgs = remoteProcessGroupDAO.getRemoteProcessGroups(groupId);
        return rpgs.stream()
            .map(rpg -> createRemoteGroupEntity(rpg, user))
            .collect(Collectors.toSet());
    }

    @Override
    public PortEntity getInputPort(final String inputPortId) {
        final Port port = inputPortDAO.getPort(inputPortId);
        return createInputPortEntity(port);
    }

    @Override
    public PortStatusEntity getInputPortStatus(final String inputPortId) {
        final Port inputPort = inputPortDAO.getPort(inputPortId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(inputPort);
        final PortStatusDTO dto = dtoFactory.createPortStatusDto(controllerFacade.getInputPortStatus(inputPortId));
        return entityFactory.createPortStatusEntity(dto, permissions);
    }

    @Override
    public PortEntity getOutputPort(final String outputPortId) {
        final Port port = outputPortDAO.getPort(outputPortId);
        return createOutputPortEntity(port);
    }

    @Override
    public PortStatusEntity getOutputPortStatus(final String outputPortId) {
        final Port outputPort = outputPortDAO.getPort(outputPortId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(outputPort);
        final PortStatusDTO dto = dtoFactory.createPortStatusDto(controllerFacade.getOutputPortStatus(outputPortId));
        return entityFactory.createPortStatusEntity(dto, permissions);
    }

    @Override
    public RemoteProcessGroupEntity getRemoteProcessGroup(final String remoteProcessGroupId) {
        final RemoteProcessGroup rpg = remoteProcessGroupDAO.getRemoteProcessGroup(remoteProcessGroupId);
        return createRemoteGroupEntity(rpg, NiFiUserUtils.getNiFiUser());
    }

    @Override
    public RemoteProcessGroupStatusEntity getRemoteProcessGroupStatus(final String id) {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(id);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroup);
        final RemoteProcessGroupStatusDTO dto = dtoFactory.createRemoteProcessGroupStatusDto(remoteProcessGroup, controllerFacade.getRemoteProcessGroupStatus(id));
        return entityFactory.createRemoteProcessGroupStatusEntity(dto, permissions);
    }

    @Override
    public StatusHistoryEntity getRemoteProcessGroupStatusHistory(final String id) {
        final RemoteProcessGroup remoteProcessGroup = remoteProcessGroupDAO.getRemoteProcessGroup(id);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(remoteProcessGroup);
        final StatusHistoryDTO dto = controllerFacade.getRemoteProcessGroupStatusHistory(id);
        return entityFactory.createStatusHistoryEntity(dto, permissions);
    }

    @Override
    public CurrentUserEntity getCurrentUser() {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final CurrentUserEntity entity = new CurrentUserEntity();
        entity.setIdentity(user.getIdentity());
        entity.setAnonymous(user.isAnonymous());
        entity.setLogoutSupported(isLogoutSupported());
        entity.setProvenancePermissions(dtoFactory.createPermissionsDto(authorizableLookup.getProvenance()));
        entity.setCountersPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getCounters()));
        entity.setTenantsPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getTenant()));
        entity.setControllerPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getController()));
        entity.setPoliciesPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getPolicies()));
        entity.setSystemPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getSystem()));
        entity.setParameterContextPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getParameterContexts()));
        entity.setCanVersionFlows(CollectionUtils.isNotEmpty(flowRegistryDAO.getFlowRegistryClients().stream().map(c -> c.getIdentifier()).collect(Collectors.toSet())));

        entity.setRestrictedComponentsPermissions(dtoFactory.createPermissionsDto(authorizableLookup.getRestrictedComponents()));

        final Set<ComponentRestrictionPermissionDTO> componentRestrictionPermissions = new HashSet<>();
        Arrays.stream(RequiredPermission.values()).forEach(requiredPermission -> {
            final PermissionsDTO restrictionPermissions = dtoFactory.createPermissionsDto(authorizableLookup.getRestrictedComponents(requiredPermission));

            final RequiredPermissionDTO requiredPermissionDto = new RequiredPermissionDTO();
            requiredPermissionDto.setId(requiredPermission.getPermissionIdentifier());
            requiredPermissionDto.setLabel(requiredPermission.getPermissionLabel());

            final ComponentRestrictionPermissionDTO componentRestrictionPermissionDto = new ComponentRestrictionPermissionDTO();
            componentRestrictionPermissionDto.setRequiredPermission(requiredPermissionDto);
            componentRestrictionPermissionDto.setPermissions(restrictionPermissions);

            componentRestrictionPermissions.add(componentRestrictionPermissionDto);
        });
        entity.setComponentRestrictionPermissions(componentRestrictionPermissions);

        return entity;
    }

    @Override
    public ProcessGroupFlowEntity getProcessGroupFlow(final String groupId, final boolean uiOnly) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);

        // Get the Process Group Status but we only need a status depth of one because for any child process group,
        // we ignore the status of each individual components. I.e., if Process Group A has child Group B, and child Group B
        // has a Processor, we don't care about the individual stats of that Processor because the ProcessGroupFlowEntity
        // doesn't include that anyway. So we can avoid including the information in the status that is returned.
        final ProcessGroupStatus groupStatus = controllerFacade.getProcessGroupStatus(groupId, 1);
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(processGroup.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);
        return entityFactory.createProcessGroupFlowEntity(dtoFactory.createProcessGroupFlowDto(processGroup, groupStatus,
                revisionManager, this::getProcessGroupBulletins, uiOnly), revision, permissions);
    }

    @Override
    public FlowBreadcrumbEntity getProcessGroupBreadcrumbs(final String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        return dtoFactory.createBreadcrumbEntity(processGroup);
    }

    @Override
    public ProcessGroupEntity getProcessGroup(final String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        return createProcessGroupEntity(processGroup);
    }

    private ControllerServiceEntity createControllerServiceEntity(final ControllerServiceNode serviceNode, final boolean includeReferencingComponents) {
        final ControllerServiceDTO dto = dtoFactory.createControllerServiceDto(serviceNode);

        if (includeReferencingComponents) {
            final ControllerServiceReference ref = serviceNode.getReferences();
            final ControllerServiceReferencingComponentsEntity referencingComponentsEntity = createControllerServiceReferencingComponentsEntity(ref);
            dto.setReferencingComponents(referencingComponentsEntity.getControllerServiceReferencingComponents());
        }

        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(serviceNode.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(serviceNode, NiFiUserUtils.getNiFiUser());
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(serviceNode), NiFiUserUtils.getNiFiUser());
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(serviceNode.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createControllerServiceEntity(dto, revision, permissions, operatePermissions, bulletinEntities);
    }

    @Override
    public Set<ControllerServiceEntity> getControllerServices(final String groupId, final boolean includeAncestorGroups, final boolean includeDescendantGroups,
                                                              final boolean includeReferencingComponents) {
        final Set<ControllerServiceNode> serviceNodes = controllerServiceDAO.getControllerServices(groupId, includeAncestorGroups, includeDescendantGroups);

        return serviceNodes.stream()
            .map(serviceNode -> createControllerServiceEntity(serviceNode, includeReferencingComponents))
            .collect(Collectors.toSet());
    }

    @Override
    public ControllerServiceEntity getControllerService(final String controllerServiceId, final boolean includeReferencingComponents) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);
        return createControllerServiceEntity(controllerService, includeReferencingComponents);
    }

    @Override
    public PropertyDescriptorDTO getControllerServicePropertyDescriptor(final String id, final String property, final boolean sensitive) {
        final ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(id);
        final PropertyDescriptor descriptor = getPropertyDescriptor(controllerService, property, sensitive);
        final String groupId = controllerService.getProcessGroup() == null ? null : controllerService.getProcessGroup().getIdentifier();
        return dtoFactory.createPropertyDescriptorDto(descriptor, groupId);
    }

    @Override
    public ControllerServiceReferencingComponentsEntity getControllerServiceReferencingComponents(final String controllerServiceId) {
        final ControllerServiceNode service = controllerServiceDAO.getControllerService(controllerServiceId);
        final ControllerServiceReference ref = service.getReferences();
        return createControllerServiceReferencingComponentsEntity(ref);
    }

    private ReportingTaskEntity createReportingTaskEntity(final ReportingTaskNode reportingTask) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(reportingTask.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(reportingTask);
        final PermissionsDTO operatePermissions = dtoFactory.createPermissionsDto(new OperationAuthorizable(reportingTask));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(reportingTask.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createReportingTaskEntity(dtoFactory.createReportingTaskDto(reportingTask), revision, permissions, operatePermissions, bulletinEntities);
    }

    @Override
    public Set<ReportingTaskEntity> getReportingTasks() {
        final Set<ReportingTaskNode> reportingTasks = reportingTaskDAO.getReportingTasks();
        return reportingTasks.stream()
            .map(reportingTask -> createReportingTaskEntity(reportingTask))
            .collect(Collectors.toSet());
    }

    @Override
    public ReportingTaskEntity getReportingTask(final String reportingTaskId) {
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);
        return createReportingTaskEntity(reportingTask);
    }

    @Override
    public PropertyDescriptorDTO getReportingTaskPropertyDescriptor(final String id, final String property, final boolean sensitive) {
        final ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(id);
        final PropertyDescriptor descriptor = getPropertyDescriptor(reportingTask, property, sensitive);
        return dtoFactory.createPropertyDescriptorDto(descriptor, null);
    }

    private ParameterProviderEntity createParameterProviderEntity(final ParameterProviderNode parameterProvider) {
        final RevisionDTO revision = dtoFactory.createRevisionDTO(revisionManager.getRevision(parameterProvider.getIdentifier()));
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(parameterProvider);
        dtoFactory.createPermissionsDto(new OperationAuthorizable(parameterProvider));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(parameterProvider.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createParameterProviderEntity(dtoFactory.createParameterProviderDto(parameterProvider),
                revision, permissions, bulletinEntities);
    }

    @Override
    public Set<ParameterProviderEntity> getParameterProviders() {
        final Set<ParameterProviderNode> parameterProviders = parameterProviderDAO.getParameterProviders();
        return parameterProviders.stream()
                .map(parameterProvider -> createParameterProviderEntity(parameterProvider))
                .collect(Collectors.toSet());
    }

    @Override
    public ParameterProviderEntity getParameterProvider(final String parameterProviderId) {
        final ParameterProviderNode parameterProvider = parameterProviderDAO.getParameterProvider(parameterProviderId);
        return createParameterProviderEntity(parameterProvider);
    }

    @Override
    public ParameterProviderReferencingComponentsEntity getParameterProviderReferencingComponents(final String parameterProviderId) {
        final ParameterProviderNode provider = parameterProviderDAO.getParameterProvider(parameterProviderId);
        return createParameterProviderReferencingComponentsEntity(provider.getReferences());
    }

    /**
     * Creates entities for components referencing a ParameterProvider using their current revision.
     *
     * @param references ParameterContexts that reference the parameter provider
     * @return The entity
     */
    private ParameterProviderReferencingComponentsEntity createParameterProviderReferencingComponentsEntity(final Set<ParameterContext> references) {
        final Map<String, Revision> referencingRevisions = new HashMap<>();
        for (final ParameterContext reference : references) {
            referencingRevisions.put(reference.getIdentifier(), revisionManager.getRevision(reference.getIdentifier()));
        }

        return createParameterProviderReferencingComponentsEntity(references, referencingRevisions);
    }

    /**
     * Creates entities for components referencing a ParameterProvider using the specified revisions.
     *
     * @param references referencing ParameterContexts
     * @param revisions The revisions
     * @return The entity
     */
    private ParameterProviderReferencingComponentsEntity createParameterProviderReferencingComponentsEntity(
            final Set<ParameterContext> references, final Map<String, Revision> revisions) {

        final String modifier = NiFiUserUtils.getNiFiUserIdentity();

        final Set<ParameterProviderReferencingComponentEntity> componentEntities = new HashSet<>();
        for (final ParameterContext reference : references) {
            final PermissionsDTO permissions = dtoFactory.createPermissionsDto(reference);

            final Revision revision = revisions.get(reference.getIdentifier());
            final FlowModification flowMod = new FlowModification(revision, modifier);
            final RevisionDTO revisionDto = dtoFactory.createRevisionDTO(flowMod);
            final ParameterProviderReferencingComponentDTO dto = dtoFactory.createParameterProviderReferencingComponentDTO(reference);

            final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(reference.getIdentifier()));
            componentEntities.add(entityFactory.createParameterProviderReferencingComponentEntity(reference.getIdentifier(), dto, revisionDto, permissions, bulletins));
        }

        final ParameterProviderReferencingComponentsEntity entity = new ParameterProviderReferencingComponentsEntity();
        entity.setParameterProviderReferencingComponents(componentEntities);
        return entity;
    }

    @Override
    public PropertyDescriptorDTO getParameterProviderPropertyDescriptor(final String id, final String property) {
        final ParameterProviderNode parameterProvider = parameterProviderDAO.getParameterProvider(id);
        PropertyDescriptor descriptor = parameterProvider.getParameterProvider().getPropertyDescriptor(property);

        // return an invalid descriptor if the parameter provider doesn't support this property
        if (descriptor == null) {
            descriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        }

        return dtoFactory.createPropertyDescriptorDto(descriptor, null);
    }

    @Override
    public StatusHistoryEntity getProcessGroupStatusHistory(final String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);
        final StatusHistoryDTO dto = controllerFacade.getProcessGroupStatusHistory(groupId);
        return entityFactory.createStatusHistoryEntity(dto, permissions);
    }

    @Override
    public VersionControlComponentMappingEntity registerFlowWithFlowRegistry(final String groupId, final StartVersionControlRequestEntity requestEntity) {
        final VersionedFlowDTO versionedFlowDto = requestEntity.getVersionedFlow();

        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final VersionControlInformation currentVci = processGroup.getVersionControlInformation();

        final String snapshotVersion = currentVci == null ? null : currentVci.getVersion();
        final RegisterAction registerAction = RegisterAction.valueOf(versionedFlowDto.getAction());

        // Create a VersionedProcessGroup snapshot of the flow as it is currently.
        final InstantiatedVersionedProcessGroup versionedProcessGroup = createFlowSnapshot(groupId);
        final Map<String, ParameterProviderReference> parameterProviderReferences = new HashMap<>();
        final Map<String, VersionedParameterContext> parameterContexts = createVersionedParameterContexts(processGroup, parameterProviderReferences);

        final String registryId = requestEntity.getVersionedFlow().getRegistryId();

        final String selectedBranch;
        final String submittedBranch = versionedFlowDto.getBranch();
        final String currentVciBranch = currentVci == null ? null : currentVci.getBranch();
        if (submittedBranch == null) {
            selectedBranch = currentVciBranch == null ? getDefaultBranchName(registryId) : currentVciBranch;
        } else {
            selectedBranch = submittedBranch;
        }

        final RegisteredFlow versionedFlow = new RegisteredFlow();
        versionedFlow.setBranch(selectedBranch);
        versionedFlow.setBucketIdentifier(versionedFlowDto.getBucketId());
        versionedFlow.setCreatedTimestamp(System.currentTimeMillis());
        versionedFlow.setDescription(versionedFlowDto.getDescription());
        versionedFlow.setLastModifiedTimestamp(versionedFlow.getCreatedTimestamp());
        versionedFlow.setName(versionedFlowDto.getFlowName());
        versionedFlow.setIdentifier(versionedFlowDto.getFlowId());

        // Add the Versioned Flow and first snapshot to the Flow Registry
        final RegisteredFlowSnapshot registeredSnapshot;
        final RegisteredFlow registeredFlow;
        final boolean registerNewFlow = versionedFlowDto.getFlowId() == null;

        try {
            // first, create the flow in the registry, if necessary
            if (registerNewFlow) {
                registeredFlow = registerVersionedFlow(registryId, versionedFlow);
            } else {
                registeredFlow = getVersionedFlow(registryId, selectedBranch, versionedFlowDto.getBucketId(), versionedFlowDto.getFlowId());
            }
        } catch (final FlowRegistryException e) {
            throw new IllegalArgumentException(e.getLocalizedMessage());
        } catch (final IOException ioe) {
            throw new IllegalStateException("Failed to communicate with Flow Registry when attempting to create the flow");
        }

        try {
            // add a snapshot to the flow in the registry
            registeredSnapshot = registerVersionedFlowSnapshot(registryId, registeredFlow, versionedProcessGroup, parameterContexts, parameterProviderReferences,
                    versionedProcessGroup.getExternalControllerServiceReferences(), versionedFlowDto.getComments(), snapshotVersion, registerAction);
        } catch (final NiFiCoreException e) {
            // If the flow has been created, but failed to add a snapshot,
            // then we need to capture the created versioned flow information as a partial successful result.
            if (registerNewFlow) {
                try {
                    final FlowLocation flowLocation = new FlowLocation(selectedBranch, versionedFlowDto.getBucketId(), registeredFlow.getIdentifier());
                    final FlowRegistryClientNode flowRegistryClientNode = flowRegistryDAO.getFlowRegistryClient(registryId);
                    flowRegistryClientNode.deregisterFlow(FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser()), flowLocation);
                } catch (final IOException | FlowRegistryException e2) {
                    throw new NiFiCoreException("Failed to remove flow from Flow Registry due to " + e2.getMessage(), e2);
                }
            }

            throw e;
        }

        final FlowRegistryBucket bucket = registeredSnapshot.getBucket();
        final RegisteredFlow flow = registeredSnapshot.getFlow();

        // Update the Process Group with the new VersionControlInformation. (Send this to all nodes).
        final VersionControlInformationDTO vci = new VersionControlInformationDTO();
        vci.setBranch(flow.getBranch());
        vci.setBucketId(bucket.getIdentifier());
        vci.setBucketName(bucket.getName());
        vci.setFlowId(flow.getIdentifier());
        vci.setFlowName(flow.getName());
        vci.setFlowDescription(flow.getDescription());
        vci.setGroupId(groupId);
        vci.setRegistryId(registryId);
        vci.setRegistryName(getFlowRegistryName(registryId));
        vci.setStorageLocation(getStorageLocation(registeredSnapshot));
        vci.setVersion(registeredSnapshot.getSnapshotMetadata().getVersion());
        vci.setState(VersionedFlowState.UP_TO_DATE.name());

        return createVersionControlComponentMappingEntity(groupId, versionedProcessGroup, vci);
    }

    private String getStorageLocation(final RegisteredFlowSnapshot registeredSnapshot) {
        VersionedFlowCoordinates versionedFlowCoordinates = registeredSnapshot.getFlowContents().getVersionedFlowCoordinates();

        if (versionedFlowCoordinates == null) {
            return null;
        }

        return versionedFlowCoordinates.getStorageLocation();
    }

    private VersionControlComponentMappingEntity createVersionControlComponentMappingEntity(String groupId, InstantiatedVersionedProcessGroup versionedProcessGroup, VersionControlInformationDTO vci) {
        final Map<String, String> mapping = dtoFactory.createVersionControlComponentMappingDto(versionedProcessGroup);

        final Revision groupRevision = revisionManager.getRevision(groupId);
        final RevisionDTO groupRevisionDto = dtoFactory.createRevisionDTO(groupRevision);

        final VersionControlComponentMappingEntity entity = new VersionControlComponentMappingEntity();
        entity.setVersionControlInformation(vci);
        entity.setProcessGroupRevision(groupRevisionDto);
        entity.setVersionControlComponentMapping(mapping);
        return entity;
    }

    @Override
    public CopyResponseEntity copyComponents(final String groupId, final CopyRequestEntity copyRequest) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);

        final FlowMappingOptions mappingOptions = new FlowMappingOptions.Builder()
                .sensitiveValueEncryptor(null)
                .stateLookup(VersionedComponentStateLookup.ENABLED_OR_DISABLED)
                .componentIdLookup(ComponentIdLookup.VERSIONED_OR_GENERATE)
                .mapPropertyDescriptors(true)
                .mapSensitiveConfiguration(false)
                .mapInstanceIdentifiers(true)
                .mapControllerServiceReferencesToVersionedId(true)
                .mapFlowRegistryClientId(true)
                .mapAssetReferences(false)
                .build();

        final NiFiRegistryFlowMapper mapper = makeNiFiRegistryFlowMapper(controllerFacade.getExtensionManager(), mappingOptions);
        final InstantiatedVersionedProcessGroup versionedProcessGroup =
                mapper.mapProcessGroup(processGroup, controllerFacade.getControllerServiceProvider(), controllerFacade.getFlowManager(), true);

        final Set<VersionedProcessGroup> versionedProcessGroups = versionedProcessGroup.getProcessGroups().stream()
                .filter(pg -> copyRequest.getProcessGroups().contains(pg.getInstanceIdentifier()))
                .collect(Collectors.toSet());
        final Set<VersionedRemoteProcessGroup> versionedRemoteProcessGroups = versionedProcessGroup.getRemoteProcessGroups().stream()
                .filter(rpg -> copyRequest.getRemoteProcessGroups().contains(rpg.getInstanceIdentifier()))
                .collect(Collectors.toSet());
        final Set<VersionedProcessor> versionedProcessors = versionedProcessGroup.getProcessors().stream()
                .filter(p -> copyRequest.getProcessors().contains(p.getInstanceIdentifier()))
                .collect(Collectors.toSet());
        final Set<VersionedPort> versionedInputPorts = versionedProcessGroup.getInputPorts().stream()
                .filter(ip -> copyRequest.getInputPorts().contains(ip.getInstanceIdentifier()))
                .collect(Collectors.toSet());
        final Set<VersionedPort> versionedOutputPorts = versionedProcessGroup.getOutputPorts().stream()
                .filter(op -> copyRequest.getOutputPorts().contains(op.getInstanceIdentifier()))
                .collect(Collectors.toSet());
        final Set<VersionedFunnel> versionedFunnels = versionedProcessGroup.getFunnels().stream()
                .filter(f -> copyRequest.getFunnels().contains(f.getInstanceIdentifier()))
                .collect(Collectors.toSet());
        final Set<VersionedLabel> versionedLabels = versionedProcessGroup.getLabels().stream()
                .filter(l -> copyRequest.getLabels().contains(l.getInstanceIdentifier()))
                .collect(Collectors.toSet());
        final Set<VersionedConnection> versionedConnections = versionedProcessGroup.getConnections().stream()
                .filter(c -> copyRequest.getConnections().contains(c.getInstanceIdentifier()))
                .collect(Collectors.toSet());

        // include any top level services as external as the top level isn't included
        final Map<String, ExternalControllerServiceReference> externalControllerServices =
                versionedProcessGroup.getExternalControllerServiceReferences().entrySet().stream()
                        .filter(e -> isServiceReferenced(e.getKey(), versionedProcessors, Collections.emptySet(), versionedProcessGroups))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // move any service at the current level into external services
        versionedProcessGroup.getControllerServices().stream()
                .filter(cs -> isServiceReferenced(cs.getIdentifier(), versionedProcessors, Collections.emptySet(), versionedProcessGroups))
                .forEach(vcs -> {
            final ExternalControllerServiceReference externalControllerService = new ExternalControllerServiceReference();
            externalControllerService.setIdentifier(vcs.getIdentifier());
            externalControllerService.setName(vcs.getName());
            externalControllerServices.put(vcs.getIdentifier(), externalControllerService);
        });

        final Map<String, VersionedParameterContext> parameterContexts = new HashMap<>();
        final Map<String, ParameterProviderReference> parameterProviderReferences = new HashMap<>();

        // Create a complete (include descendant flows) map of parameter contexts
        processGroup.getProcessGroups().stream()
                .filter(pg -> copyRequest.getProcessGroups().contains(pg.getIdentifier()))
                .forEach(pg -> parameterContexts.putAll(mapper.mapParameterContexts(pg, true, parameterProviderReferences)));

        // build the copy response payload
        final CopyResponseEntity copyResponseEntity = new CopyResponseEntity();
        copyResponseEntity.setId(UUID.randomUUID().toString());
        copyResponseEntity.setExternalControllerServiceReferences(externalControllerServices);
        copyResponseEntity.setParameterContexts(parameterContexts);
        copyResponseEntity.setParameterProviders(parameterProviderReferences);
        copyResponseEntity.setProcessGroups(versionedProcessGroups);
        copyResponseEntity.setRemoteProcessGroups(versionedRemoteProcessGroups);
        copyResponseEntity.setProcessors(versionedProcessors);
        copyResponseEntity.setInputPorts(versionedInputPorts);
        copyResponseEntity.setOutputPorts(versionedOutputPorts);
        copyResponseEntity.setFunnels(versionedFunnels);
        copyResponseEntity.setLabels(versionedLabels);
        copyResponseEntity.setConnections(versionedConnections);

        return copyResponseEntity;
    }

    private boolean isServiceReferenced(final String serviceId, final Set<VersionedProcessor> processors,
                                        final Set<VersionedControllerService> services, final Set<VersionedProcessGroup> groups) {
        final boolean usedInProcessor = processors.stream().anyMatch(p -> p.getProperties().containsValue(serviceId));
        if (usedInProcessor) {
            return true;
        }

        final boolean usedInService = services.stream().anyMatch(cs -> cs.getProperties().containsValue(serviceId));
        if (usedInService) {
            return true;
        }

        return groups.stream().anyMatch(pg -> isServiceReferenced(serviceId, pg.getProcessors(), pg.getControllerServices(), pg.getProcessGroups()));
    }

    @Override
    public RegisteredFlowSnapshot getCurrentFlowSnapshotByGroupId(final String processGroupId) {
        return getCurrentFlowSnapshotByGroupId(processGroupId, false);
    }

    @Override
    public RegisteredFlowSnapshot getCurrentFlowSnapshotByGroupIdWithReferencedControllerServices(final String processGroupId) {
        return getCurrentFlowSnapshotByGroupId(processGroupId, true);
    }

    private Set<String> getAllSubGroups(ProcessGroup processGroup) {
        final Set<String> result = processGroup.findAllProcessGroups().stream()
                .map(ProcessGroup::getIdentifier)
                .collect(Collectors.toSet());
        result.add(processGroup.getIdentifier());
        return result;
    }

    private RegisteredFlowSnapshot getCurrentFlowSnapshotByGroupId(final String processGroupId, final boolean includeReferencedControllerServices) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupId);

        // Create a complete (include descendant flows) VersionedProcessGroup snapshot of the flow as it is
        // currently without any registry related fields populated, even if the flow is currently versioned.
        final NiFiRegistryFlowMapper mapper = makeNiFiRegistryFlowMapper(controllerFacade.getExtensionManager());
        final InstantiatedVersionedProcessGroup nonVersionedProcessGroup =
                mapper.mapNonVersionedProcessGroup(processGroup, controllerFacade.getControllerServiceProvider());

        final Map<String, ParameterProviderReference> parameterProviderReferences = new HashMap<>();

        // Create a complete (include descendant flows) map of parameter contexts
        final Map<String, VersionedParameterContext> parameterContexts = mapper.mapParameterContexts(processGroup, true, parameterProviderReferences);

        final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences =
                Optional.ofNullable(nonVersionedProcessGroup.getExternalControllerServiceReferences()).orElse(Collections.emptyMap());
        final Set<VersionedControllerService> controllerServices = new HashSet<>(nonVersionedProcessGroup.getControllerServices());
        final RegisteredFlowSnapshot nonVersionedFlowSnapshot = new RegisteredFlowSnapshot();

        ProcessGroup parentGroup = processGroup.getParent();

        if (includeReferencedControllerServices && parentGroup != null) {
            final Set<VersionedControllerService> externalServices = new HashSet<>();

            do {
                final Set<ControllerServiceNode> controllerServiceNodes = parentGroup.getControllerServices(false);

                for (final ControllerServiceNode controllerServiceNode : controllerServiceNodes) {
                    final VersionedControllerService versionedControllerService =
                            mapper.mapControllerService(controllerServiceNode, controllerFacade.getControllerServiceProvider(), getAllSubGroups(processGroup), externalControllerServiceReferences);

                    if (externalControllerServiceReferences.keySet().contains(versionedControllerService.getIdentifier())) {
                        versionedControllerService.setGroupIdentifier(processGroupId);
                        externalServices.add(versionedControllerService);
                    }
                }
            } while ((parentGroup = parentGroup.getParent()) != null);

            controllerServices.addAll(externalServices);
            nonVersionedFlowSnapshot.setExternalControllerServices(new HashMap<>());
        } else {
            nonVersionedFlowSnapshot.setExternalControllerServices(externalControllerServiceReferences);
        }

        nonVersionedProcessGroup.setControllerServices(controllerServices);
        nonVersionedFlowSnapshot.setFlowContents(nonVersionedProcessGroup);
        nonVersionedFlowSnapshot.setParameterProviders(parameterProviderReferences);
        nonVersionedFlowSnapshot.setParameterContexts(parameterContexts);
        nonVersionedFlowSnapshot.setFlowEncodingVersion(FlowRegistryUtil.FLOW_ENCODING_VERSION);

        return nonVersionedFlowSnapshot;
    }

    @Override
    public FlowSnapshotContainer getVersionedFlowSnapshotByGroupId(final String processGroupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupId);
        final VersionControlInformation versionControlInfo = processGroup.getVersionControlInformation();

        return getVersionedFlowSnapshot(versionControlInfo.getRegistryIdentifier(), versionControlInfo.getBranch(), versionControlInfo.getBucketIdentifier(),
                versionControlInfo.getFlowIdentifier(), versionControlInfo.getVersion(), true);
    }

    @Override
    public FlowSnapshotContainer getVersionedFlowSnapshot(final VersionControlInformationDTO versionControlInfo, final boolean fetchRemoteFlows) {
        return getVersionedFlowSnapshot(versionControlInfo.getRegistryId(), versionControlInfo.getBranch(), versionControlInfo.getBucketId(),
                versionControlInfo.getFlowId(), versionControlInfo.getVersion(), fetchRemoteFlows);
    }

    /**
     *
     * @param registryId        the id of the registry to retrieve the versioned flow from
     * @param branch            the name of the branch within the registry
     * @param bucketId          the id of the bucket within the registry
     * @param flowId            the id of the flow within the bucket/registry
     * @param flowVersion       the version of the flow to retrieve
     * @param fetchRemoteFlows  indicator to include remote flows when retrieving the flow
     * @return a VersionedFlowSnapshot from a registry with the given version
     */
    private FlowSnapshotContainer getVersionedFlowSnapshot(final String registryId, final String branch, final String bucketId, final String flowId,
                                                           final String flowVersion, final boolean fetchRemoteFlows) {
        final FlowRegistryClientNode flowRegistry = flowRegistryDAO.getFlowRegistryClient(registryId);
        if (flowRegistry == null) {
            throw new ResourceNotFoundException("Could not find any Flow Registry registered with identifier " + registryId);
        }

        try {
            final FlowRegistryClientUserContext clientUserContext = FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser());
            final String selectedBranch = branch == null ? flowRegistry.getDefaultBranch(clientUserContext).getName() : branch;
            final FlowVersionLocation flowVersionLocation = new FlowVersionLocation(selectedBranch, bucketId, flowId, flowVersion);
            return flowRegistry.getFlowContents(clientUserContext, flowVersionLocation, fetchRemoteFlows);
        } catch (final FlowRegistryException e) {
            logger.error(e.getMessage(), e);
            throw new IllegalArgumentException("The Flow Registry with ID " + registryId + " reports that no Flow exists with Bucket "
                    + bucketId + ", Flow " + flowId + ", Version " + flowVersion, e);
        } catch (final IOException ioe) {
            throw new IllegalStateException("Failed to communicate with Flow Registry when attempting to retrieve a versioned flow", ioe);
        }
    }

    @Override
    public RegisteredFlow deleteVersionedFlow(final String registryId, final String branch, final String bucketId, final String flowId) {
        final FlowRegistryClientNode registry = flowRegistryDAO.getFlowRegistryClient(registryId);
        if (registry == null) {
            throw new IllegalArgumentException("No Flow Registry exists with ID " + registryId);
        }

        try {
            final FlowLocation flowLocation = new FlowLocation(branch, bucketId, flowId);
            return registry.deregisterFlow(FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser()), flowLocation);
        } catch (final IOException | FlowRegistryException e) {
            throw new NiFiCoreException("Failed to remove flow from Flow Registry due to " + e.getMessage(), e);
        }
    }

    @Override
    public FlowComparisonEntity getVersionDifference(final String registryId, FlowVersionLocation versionLocationA, FlowVersionLocation versionLocationB) {
        final FlowComparisonEntity result = new FlowComparisonEntity();

        if (versionLocationA.equals(versionLocationB)) {
            // If both versions are the same, there is no need for comparison. Comparing them should have the same result but with the cost of some calls to the registry.
            // Note: because of this optimization we return an empty non-error response in case of non-existing registry, bucket, flow or version if the versions are the same.
            result.setComponentDifferences(Collections.emptySet());
            return result;
        }

        final FlowSnapshotContainer snapshotA = this.getVersionedFlowSnapshot(
                registryId, versionLocationA.getBranch(), versionLocationA.getBucketId(), versionLocationA.getFlowId(), versionLocationA.getVersion(), true);
        final FlowSnapshotContainer snapshotB = this.getVersionedFlowSnapshot(
                registryId, versionLocationB.getBranch(), versionLocationB.getBucketId(), versionLocationB.getFlowId(), versionLocationB.getVersion(), true);

        final VersionedProcessGroup flowContentsA = snapshotA.getFlowSnapshot().getFlowContents();
        final VersionedProcessGroup flowContentsB = snapshotB.getFlowSnapshot().getFlowContents();

        final FlowComparator flowComparator = new StandardFlowComparator(
                new StandardComparableDataFlow("Flow A", flowContentsA),
                new StandardComparableDataFlow("Flow B", flowContentsB),
                Collections.emptySet(), // Replacement of an external ControllerService is recognized as property change
                new ConciseEvolvingDifferenceDescriptor(),
                Function.identity(),
                VersionedComponent::getIdentifier,
                FlowComparatorVersionedStrategy.DEEP
            );

        final FlowComparison flowComparison = flowComparator.compare();
        final Set<ComponentDifferenceDTO> differenceDtos = dtoFactory.createComponentDifferenceDtosForLocalModifications(flowComparison, flowContentsA, controllerFacade.getFlowManager());
        result.setComponentDifferences(differenceDtos);

        return result;
    }

    @Override
    public boolean isAnyProcessGroupUnderVersionControl(final String groupId) {
        return isProcessGroupUnderVersionControl(processGroupDAO.getProcessGroup(groupId));
    }

    private boolean isProcessGroupUnderVersionControl(final ProcessGroup processGroup) {
        if (processGroup.getVersionControlInformation() != null) {
            return true;
        }
        final Set<ProcessGroup> childGroups = processGroup.getProcessGroups();
        if (childGroups != null) {
            return childGroups.stream()
                    .anyMatch(childGroup -> isProcessGroupUnderVersionControl(childGroup));
        }
        return false;
    }

    @Override
    public VersionControlInformationEntity getVersionControlInformation(final String groupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final VersionControlInformation versionControlInfo = processGroup.getVersionControlInformation();
        if (versionControlInfo == null) {
            return null;
        }

        final VersionControlInformationDTO versionControlDto = dtoFactory.createVersionControlInformationDto(processGroup);
        final RevisionDTO groupRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(groupId));
        return entityFactory.createVersionControlInformationEntity(versionControlDto, groupRevision);
    }

    private InstantiatedVersionedProcessGroup createFlowSnapshot(final String processGroupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupId);
        final NiFiRegistryFlowMapper mapper = makeNiFiRegistryFlowMapper(controllerFacade.getExtensionManager());
        final InstantiatedVersionedProcessGroup versionedGroup = mapper.mapProcessGroup(processGroup, controllerFacade.getControllerServiceProvider(), controllerFacade.getFlowManager(), false);
        return versionedGroup;
    }

    private Map<String, VersionedParameterContext> createVersionedParameterContexts(final ProcessGroup processGroup, final Map<String, ParameterProviderReference> parameterProviderReferences) {
        final NiFiRegistryFlowMapper mapper = makeNiFiRegistryFlowMapper(controllerFacade.getExtensionManager());
        return mapper.mapParameterContexts(processGroup, false, parameterProviderReferences);
    }

    @Override
    public FlowComparisonEntity getLocalModifications(final String processGroupId) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupId);
        final VersionControlInformation versionControlInfo = processGroup.getVersionControlInformation();
        if (versionControlInfo == null) {
            throw new IllegalStateException("Process Group with ID " + processGroupId + " is not under Version Control");
        }

        final FlowRegistryClientNode flowRegistry = flowRegistryDAO.getFlowRegistryClient(versionControlInfo.getRegistryIdentifier());
        if (flowRegistry == null) {
            throw new IllegalStateException("Process Group with ID " + processGroupId + " is tracking to a flow in Flow Registry with ID " + versionControlInfo.getRegistryIdentifier()
                + " but cannot find a Flow Registry with that identifier");
        }

        VersionedProcessGroup registryGroup = null;
        final VersionControlInformation vci = processGroup.getVersionControlInformation();
        if (vci != null) {
            registryGroup = vci.getFlowSnapshot();
        }

        if (registryGroup == null) {
            try {
                final FlowVersionLocation flowVersionLocation = new FlowVersionLocation(versionControlInfo.getBranch(), versionControlInfo.getBucketIdentifier(),
                        versionControlInfo.getFlowIdentifier(), versionControlInfo.getVersion());
                final FlowSnapshotContainer flowSnapshotContainer = flowRegistry.getFlowContents(FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser()),
                        flowVersionLocation, true);
                final RegisteredFlowSnapshot versionedFlowSnapshot = flowSnapshotContainer.getFlowSnapshot();
                registryGroup = versionedFlowSnapshot.getFlowContents();
            } catch (final IOException | FlowRegistryException e) {
                throw new NiFiCoreException("Failed to retrieve flow with Flow Registry in order to calculate local differences due to " + e.getMessage(), e);
            }
        }

        final NiFiRegistryFlowMapper mapper = makeNiFiRegistryFlowMapper(controllerFacade.getExtensionManager());
        final VersionedProcessGroup localGroup = mapper.mapProcessGroup(processGroup, controllerFacade.getControllerServiceProvider(), controllerFacade.getFlowManager(), true);

        final ComparableDataFlow localFlow = new StandardComparableDataFlow("Local Flow", localGroup);
        final ComparableDataFlow registryFlow = new StandardComparableDataFlow("Versioned Flow", registryGroup);

        final Set<String> ancestorServiceIds = processGroup.getAncestorServiceIds();
        final FlowComparator flowComparator = new StandardFlowComparator(registryFlow, localFlow, ancestorServiceIds, new ConciseEvolvingDifferenceDescriptor(),
            Function.identity(), VersionedComponent::getIdentifier, FlowComparatorVersionedStrategy.SHALLOW);
        final FlowComparison flowComparison = flowComparator.compare();

        final Set<ComponentDifferenceDTO> differenceDtos = dtoFactory.createComponentDifferenceDtosForLocalModifications(flowComparison, localGroup, controllerFacade.getFlowManager());

        final FlowComparisonEntity entity = new FlowComparisonEntity();
        entity.setComponentDifferences(differenceDtos);
        return entity;
    }

    @Override
    public RegisteredFlow registerVersionedFlow(final String registryId, final RegisteredFlow flow) {
        final FlowRegistryClientNode registry = flowRegistryDAO.getFlowRegistryClient(registryId);
        if (registry == null) {
            throw new ResourceNotFoundException("No Flow Registry exists with ID " + registryId);
        }

        try {
            final String generatedId = registry.generateFlowId(flow.getName());
            flow.setIdentifier(generatedId);
            return registry.registerFlow(FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser()), flow);
        } catch (final IOException | FlowRegistryException e) {
            throw new NiFiCoreException("Failed to register flow with Flow Registry due to " + e.getMessage(), e);
        }
    }

    private String getDefaultBranchName(final String registryId) {
        final FlowRegistryClientNode registry = flowRegistryDAO.getFlowRegistryClient(registryId);
        if (registry == null) {
            throw new ResourceNotFoundException("No Flow Registry exists with ID " + registryId);
        }

        try {
            final FlowRegistryBranch defaultBranch = registry.getDefaultBranch(FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser()));
            return defaultBranch.getName();
        } catch (final FlowRegistryException e) {
            throw new IllegalArgumentException(e.getLocalizedMessage());
        } catch (final IOException ioe) {
            throw new IllegalStateException("Failed to communicate with Flow Registry when attempting to get default branch");
        }
    }

    private RegisteredFlow getVersionedFlow(final String registryId, final String branch, final String bucketId, final String flowId) throws IOException, FlowRegistryException {
        final FlowRegistryClientNode registry = flowRegistryDAO.getFlowRegistryClient(registryId);
        if (registry == null) {
            throw new ResourceNotFoundException("No Flow Registry exists with ID " + registryId);
        }

        final FlowLocation flowLocation = new FlowLocation(branch, bucketId, flowId);
        return registry.getFlow(FlowRegistryClientContextFactory.getContextForUser(NiFiUserUtils.getNiFiUser()), flowLocation);
    }

    @Override
    public RegisteredFlowSnapshot registerVersionedFlowSnapshot(final String registryId, final RegisteredFlow flow, final VersionedProcessGroup snapshot,
                                                                final Map<String, VersionedParameterContext> parameterContexts,
                                                                final Map<String, ParameterProviderReference> parameterProviderReferences,
                                                                final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences, final String comments,
                                                                final String expectedVersion, final RegisterAction registerAction) {
        final FlowRegistryClientNode registry = flowRegistryDAO.getFlowRegistryClient(registryId);
        if (registry == null) {
            throw new ResourceNotFoundException("No Flow Registry exists with ID " + registryId);
        }

        try {
            return registry.registerFlowSnapshot(FlowRegistryClientContextFactory.getContextForUser(
                    NiFiUserUtils.getNiFiUser()), flow, snapshot, externalControllerServiceReferences, parameterContexts,
                    parameterProviderReferences, comments, expectedVersion, registerAction);
        } catch (final IOException | FlowRegistryException e) {
            throw new NiFiCoreException("Failed to register flow with Flow Registry due to " + e.getMessage(), e);
        }
    }

    @Override
    public VersionControlInformationEntity setVersionControlInformation(final Revision revision, final String processGroupId,
            final VersionControlInformationDTO versionControlInfo, final Map<String, String> versionedComponentMapping) {

        final ProcessGroup group = processGroupDAO.getProcessGroup(processGroupId);

        final RevisionUpdate<VersionControlInformationDTO> snapshot = updateComponent(revision,
            group,
            () -> processGroupDAO.updateVersionControlInformation(versionControlInfo, versionedComponentMapping),
            processGroup -> dtoFactory.createVersionControlInformationDto(processGroup));

        return entityFactory.createVersionControlInformationEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()));
    }

    @Override
    public VersionControlInformationEntity deleteVersionControl(final Revision revision, final String processGroupId) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(processGroupId);

        final RevisionUpdate<VersionControlInformationDTO> snapshot = updateComponent(revision,
            group,
            () -> processGroupDAO.disconnectVersionControl(processGroupId),
            processGroup -> dtoFactory.createVersionControlInformationDto(group));

        return entityFactory.createVersionControlInformationEntity(snapshot.getComponent(), dtoFactory.createRevisionDTO(snapshot.getLastModification()));
    }

    @Override
    public void verifyCanUpdate(final String groupId, final RegisteredFlowSnapshot proposedFlow, final boolean verifyConnectionRemoval, final boolean verifyNotDirty) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        final VersionedExternalFlow externalFlow = createVersionedExternalFlow(proposedFlow);
        group.verifyCanUpdate(externalFlow, verifyConnectionRemoval, verifyNotDirty);
    }

    private static VersionedExternalFlow createVersionedExternalFlow(final RegisteredFlowSnapshot flowSnapshot) {
        final VersionedExternalFlowMetadata externalFlowMetadata = new VersionedExternalFlowMetadata();
        final RegisteredFlowSnapshotMetadata snapshotMetadata = flowSnapshot.getSnapshotMetadata();
        if (snapshotMetadata != null) {
            externalFlowMetadata.setAuthor(snapshotMetadata.getAuthor());
            externalFlowMetadata.setBucketIdentifier(snapshotMetadata.getBucketIdentifier());
            externalFlowMetadata.setComments(snapshotMetadata.getComments());
            externalFlowMetadata.setFlowIdentifier(snapshotMetadata.getFlowIdentifier());
            externalFlowMetadata.setTimestamp(snapshotMetadata.getTimestamp());
            externalFlowMetadata.setVersion(snapshotMetadata.getVersion());
        }

        final RegisteredFlow versionedFlow = flowSnapshot.getFlow();
        if (versionedFlow == null) {
            externalFlowMetadata.setFlowName(flowSnapshot.getFlowContents().getName());
        } else {
            externalFlowMetadata.setFlowName(versionedFlow.getName());
        }

        final VersionedExternalFlow externalFlow = new VersionedExternalFlow();
        externalFlow.setFlowContents(flowSnapshot.getFlowContents());
        externalFlow.setExternalControllerServices(flowSnapshot.getExternalControllerServices());
        externalFlow.setParameterContexts(flowSnapshot.getParameterContexts());
        externalFlow.setMetadata(externalFlowMetadata);
        externalFlow.setParameterProviders(flowSnapshot.getParameterProviders());

        return externalFlow;
    }

    @Override
    public void verifyCanSaveToFlowRegistry(final String groupId, final String registryId, final FlowLocation flowLocation, final String saveAction) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.verifyCanSaveToFlowRegistry(registryId, flowLocation, saveAction);
    }

    @Override
    public void verifyCanRevertLocalModifications(final String groupId, final RegisteredFlowSnapshot versionedFlowSnapshot) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(groupId);
        group.verifyCanRevertLocalModifications();

        // verify that the process group can be updated to the given snapshot. We do not verify that connections can
        // be removed, because the flow may still be running, and it only matters that the connections can be removed once the components
        // have been stopped.
        final VersionedExternalFlow externalFlow = createVersionedExternalFlow(versionedFlowSnapshot);
        group.verifyCanUpdate(externalFlow, false, false);
    }

    @Override
    public Set<AffectedComponentEntity> getComponentsAffectedByFlowUpdate(final String processGroupId, final RegisteredFlowSnapshot updatedSnapshot) {
        final ProcessGroup group = processGroupDAO.getProcessGroup(processGroupId);

        final NiFiRegistryFlowMapper mapper = makeNiFiRegistryFlowMapper(controllerFacade.getExtensionManager());
        final VersionedProcessGroup localContents = mapper.mapProcessGroup(group, controllerFacade.getControllerServiceProvider(), controllerFacade.getFlowManager(), true);

        final ComparableDataFlow localFlow = new StandardComparableDataFlow("Current Flow", localContents);
        final ComparableDataFlow proposedFlow = new StandardComparableDataFlow("New Flow", updatedSnapshot.getFlowContents());

        final Set<String> ancestorServiceIds = group.getAncestorServiceIds();
        final FlowComparator flowComparator = new StandardFlowComparator(localFlow, proposedFlow, ancestorServiceIds, new StaticDifferenceDescriptor(),
            Function.identity(), VersionedComponent::getIdentifier, FlowComparatorVersionedStrategy.DEEP);
        final FlowComparison comparison = flowComparator.compare();

        final FlowManager flowManager = controllerFacade.getFlowManager();
        final Set<AffectedComponentEntity> affectedComponents = comparison.getDifferences().stream()
            .filter(difference -> difference.getDifferenceType() != DifferenceType.COMPONENT_ADDED) // components that are added are not components that will be affected in the local flow.
            .filter(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS)
            .filter(diff -> !FlowDifferenceFilters.isNewPropertyWithDefaultValue(diff, flowManager))
            .filter(diff -> !FlowDifferenceFilters.isNewRelationshipAutoTerminatedAndDefaulted(diff, proposedFlow.getContents(), flowManager))
            .filter(diff -> !FlowDifferenceFilters.isScheduledStateNew(diff))
            .filter(diff -> !FlowDifferenceFilters.isLocalScheduleStateChange(diff))
            .filter(diff -> !FlowDifferenceFilters.isPropertyMissingFromGhostComponent(diff, flowManager))
            .filter(difference -> difference.getDifferenceType() != DifferenceType.POSITION_CHANGED)
            .map(difference -> {
                final VersionedComponent localComponent = difference.getComponentA();

                final String state;
                final ProcessGroup localGroup;
                switch (localComponent.getComponentType()) {
                    case CONTROLLER_SERVICE:
                        final String serviceId = localComponent.getInstanceIdentifier();
                        final ControllerServiceNode serviceNode = controllerServiceDAO.getControllerService(serviceId);
                        localGroup = serviceNode.getProcessGroup();
                        state = serviceNode.getState().name();
                        break;
                    case PROCESSOR:
                        final String processorId = localComponent.getInstanceIdentifier();
                        final ProcessorNode procNode = processorDAO.getProcessor(processorId);
                        localGroup = procNode.getProcessGroup();
                        state = procNode.getPhysicalScheduledState().name();
                        break;
                    case REMOTE_INPUT_PORT:
                        final InstantiatedVersionedRemoteGroupPort remoteInputPort = (InstantiatedVersionedRemoteGroupPort) localComponent;
                        final RemoteProcessGroup inputPortRpg = remoteProcessGroupDAO.getRemoteProcessGroup(remoteInputPort.getInstanceGroupId());
                        localGroup = inputPortRpg.getProcessGroup();
                        state = inputPortRpg.getInputPort(remoteInputPort.getInstanceIdentifier()).getScheduledState().name();
                        break;
                    case REMOTE_OUTPUT_PORT:
                        final InstantiatedVersionedRemoteGroupPort remoteOutputPort = (InstantiatedVersionedRemoteGroupPort) localComponent;
                        final RemoteProcessGroup outputPortRpg = remoteProcessGroupDAO.getRemoteProcessGroup(remoteOutputPort.getInstanceGroupId());
                        localGroup = outputPortRpg.getProcessGroup();
                        state = outputPortRpg.getOutputPort(remoteOutputPort.getInstanceIdentifier()).getScheduledState().name();
                        break;
                    case INPUT_PORT:
                        final InstantiatedVersionedPort versionedInputPort = (InstantiatedVersionedPort) localComponent;
                        final Port inputPort = getInputPort(versionedInputPort);
                        if (inputPort == null) {
                            localGroup = null;
                            state = null;
                        } else {
                            localGroup = inputPort.getProcessGroup();
                            state = inputPort.getScheduledState().name();
                        }
                        break;
                    case OUTPUT_PORT:
                        final InstantiatedVersionedPort versionedOutputPort = (InstantiatedVersionedPort) localComponent;
                        final Port outputPort = getOutputPort(versionedOutputPort);
                        if (outputPort == null) {
                            localGroup = null;
                            state = null;
                        } else {
                            localGroup = outputPort.getProcessGroup();
                            state = outputPort.getScheduledState().name();
                        }
                        break;
                    default:
                        state = null;
                        localGroup = null;
                        break;
                }

                if (localGroup != null && localGroup.resolveExecutionEngine() == ExecutionEngine.STATELESS) {
                    return createStatelessGroupAffectedComponentEntity(localGroup);
                }

                return createAffectedComponentEntity((InstantiatedVersionedComponent) localComponent, localComponent.getComponentType().name(), state);
            })
            .collect(Collectors.toCollection(HashSet::new));

        for (final FlowDifference difference : comparison.getDifferences()) {
            // Ignore differences for adding remote ports
            if (FlowDifferenceFilters.isAddedOrRemovedRemotePort(difference)) {
                continue;
            }

            // Ignore name changes to public ports
            if (FlowDifferenceFilters.isPublicPortNameChange(difference)) {
                continue;
            }

            if (FlowDifferenceFilters.isNewPropertyWithDefaultValue(difference, controllerFacade.getFlowManager())) {
                continue;
            }

            if (FlowDifferenceFilters.isNewRelationshipAutoTerminatedAndDefaulted(difference, updatedSnapshot.getFlowContents(), controllerFacade.getFlowManager())) {
                continue;
            }

            final VersionedComponent localComponent = difference.getComponentA();
            if (localComponent == null) {
                continue;
            }

            // If any Process Group is removed, consider all components below that Process Group as an affected component
            if (difference.getDifferenceType() == DifferenceType.COMPONENT_REMOVED && localComponent.getComponentType() == org.apache.nifi.flow.ComponentType.PROCESS_GROUP) {
                final String localGroupId = ((InstantiatedVersionedProcessGroup) localComponent).getInstanceIdentifier();
                final ProcessGroup localGroup = processGroupDAO.getProcessGroup(localGroupId);

                localGroup.findAllProcessors().stream()
                    .map(comp -> createAffectedComponentEntity(comp))
                    .forEach(affectedComponents::add);
                localGroup.findAllFunnels().stream()
                    .map(comp -> createAffectedComponentEntity(comp))
                    .forEach(affectedComponents::add);
                localGroup.findAllInputPorts().stream()
                    .map(comp -> createAffectedComponentEntity(comp))
                    .forEach(affectedComponents::add);
                localGroup.findAllOutputPorts().stream()
                    .map(comp -> createAffectedComponentEntity(comp))
                    .forEach(affectedComponents::add);
                localGroup.findAllRemoteProcessGroups().stream()
                    .flatMap(rpg -> Stream.concat(rpg.getInputPorts().stream(), rpg.getOutputPorts().stream()))
                    .map(comp -> createAffectedComponentEntity(comp))
                    .forEach(affectedComponents::add);
                localGroup.findAllControllerServices().stream()
                    .map(comp -> createAffectedComponentEntity(comp))
                    .forEach(affectedComponents::add);
            }

            if (localComponent.getComponentType() == org.apache.nifi.flow.ComponentType.CONTROLLER_SERVICE) {
                final String serviceId = localComponent.getInstanceIdentifier();
                final ControllerServiceNode serviceNode = controllerServiceDAO.getControllerService(serviceId);

                final List<ControllerServiceNode> referencingServices = serviceNode.getReferences().findRecursiveReferences(ControllerServiceNode.class);
                for (final ControllerServiceNode referencingService : referencingServices) {
                    affectedComponents.add(createAffectedComponentEntity(referencingService));
                }

                final List<ProcessorNode> referencingProcessors = serviceNode.getReferences().findRecursiveReferences(ProcessorNode.class);
                for (final ProcessorNode referencingProcessor : referencingProcessors) {
                    affectedComponents.add(createAffectedComponentEntity(referencingProcessor));
                }
            }
        }

        // Create a map of all connectable components by versioned component ID to the connectable component itself
        final Map<String, List<Connectable>> connectablesByVersionId = new HashMap<>();
        mapToConnectableId(group.findAllFunnels(), connectablesByVersionId);
        mapToConnectableId(group.findAllInputPorts(), connectablesByVersionId);
        mapToConnectableId(group.findAllOutputPorts(), connectablesByVersionId);
        mapToConnectableId(group.findAllProcessors(), connectablesByVersionId);

        final List<RemoteGroupPort> remotePorts = new ArrayList<>();
        for (final RemoteProcessGroup rpg : group.findAllRemoteProcessGroups()) {
            remotePorts.addAll(rpg.getInputPorts());
            remotePorts.addAll(rpg.getOutputPorts());
        }
        mapToConnectableId(remotePorts, connectablesByVersionId);

        // If any connection is added or modified, we need to stop both the source (if it exists in the flow currently)
        // and the destination (if it exists in the flow currently).
        for (final FlowDifference difference : comparison.getDifferences()) {
            VersionedComponent component = difference.getComponentA();
            if (component == null) {
                component = difference.getComponentB();
            }

            if (component.getComponentType() != org.apache.nifi.flow.ComponentType.CONNECTION) {
                continue;
            }

            final VersionedConnection connection = (VersionedConnection) component;

            final String sourceVersionedId = connection.getSource().getId();
            final List<Connectable> sources = connectablesByVersionId.get(sourceVersionedId);
            if (sources != null) {
                for (final Connectable source : sources) {
                    affectedComponents.add(createAffectedComponentEntity(source));
                }
            }

            final String destinationVersionId = connection.getDestination().getId();
            final List<Connectable> destinations = connectablesByVersionId.get(destinationVersionId);
            if (destinations != null) {
                for (final Connectable destination : destinations) {
                    affectedComponents.add(createAffectedComponentEntity(destination));
                }
            }
        }

        return affectedComponents;
    }

    private Port getInputPort(final InstantiatedVersionedPort port) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(port.getInstanceGroupId());
        if (processGroup == null) {
            return null;
        }

        return processGroup.getInputPort(port.getInstanceIdentifier());
    }

    private Port getOutputPort(final InstantiatedVersionedPort port) {
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(port.getInstanceGroupId());
        if (processGroup == null) {
            return null;
        }

        return processGroup.getOutputPort(port.getInstanceIdentifier());
    }

    private void mapToConnectableId(final Collection<? extends Connectable> connectables, final Map<String, List<Connectable>> destination) {
        for (final Connectable connectable : connectables) {
            final Optional<String> versionedIdOption = connectable.getVersionedComponentId();

            // Determine the Versioned ID by using the ID that is assigned, if one is. Otherwise,
            // we will calculate the Versioned ID. This allows us to map connectables that currently are not under
            // version control. We have to do this so that if we are changing flow versions and have a component that is running and it does not exist
            // in the Versioned Flow, we still need to be able to create an AffectedComponentDTO for it.
            final String versionedId;
            if (versionedIdOption.isPresent()) {
                versionedId = versionedIdOption.get();
            } else {
                versionedId = NiFiRegistryFlowMapper.generateVersionedComponentId(connectable.getIdentifier());
            }

            final List<Connectable> byVersionedId = destination.computeIfAbsent(versionedId, key -> new ArrayList<>());
            byVersionedId.add(connectable);
        }
    }


    private ProcessGroupNameDTO createProcessGroupNameDto(final ProcessGroup group) {
        if (group == null) {
            return null;
        }

        final ProcessGroupNameDTO dto = new ProcessGroupNameDTO();
        dto.setId(group.getIdentifier());
        dto.setName(group.getName());
        return dto;
    }

    private AffectedComponentEntity createAffectedComponentEntity(final Connectable connectable) {
        final AffectedComponentEntity entity = new AffectedComponentEntity();
        entity.setRevision(dtoFactory.createRevisionDTO(revisionManager.getRevision(connectable.getIdentifier())));
        entity.setId(connectable.getIdentifier());
        entity.setReferenceType(connectable.getConnectableType().name());
        entity.setProcessGroup(createProcessGroupNameDto(connectable.getProcessGroup()));

        final Authorizable authorizable = getAuthorizable(connectable);
        final PermissionsDTO permissionsDto = dtoFactory.createPermissionsDto(authorizable);
        entity.setPermissions(permissionsDto);

        final AffectedComponentDTO dto = new AffectedComponentDTO();
        dto.setId(connectable.getIdentifier());
        dto.setReferenceType(connectable.getConnectableType().name());
        dto.setState(connectable.getScheduledState().name());
        dto.setName(connectable.getName());

        final String groupId = connectable instanceof RemoteGroupPort ? ((RemoteGroupPort) connectable).getRemoteProcessGroup().getIdentifier() : connectable.getProcessGroupIdentifier();
        dto.setProcessGroupId(groupId);

        entity.setComponent(dto);
        return entity;
    }

    private AffectedComponentEntity createAffectedComponentEntity(final ControllerServiceNode serviceNode) {
        final AffectedComponentEntity entity = new AffectedComponentEntity();
        entity.setRevision(dtoFactory.createRevisionDTO(revisionManager.getRevision(serviceNode.getIdentifier())));
        entity.setId(serviceNode.getIdentifier());
        entity.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE);
        entity.setProcessGroup(createProcessGroupNameDto(serviceNode.getProcessGroup()));

        final Authorizable authorizable = authorizableLookup.getControllerService(serviceNode.getIdentifier()).getAuthorizable();
        final PermissionsDTO permissionsDto = dtoFactory.createPermissionsDto(authorizable);
        entity.setPermissions(permissionsDto);

        final AffectedComponentDTO dto = new AffectedComponentDTO();
        dto.setId(serviceNode.getIdentifier());
        dto.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_CONTROLLER_SERVICE);
        dto.setProcessGroupId(serviceNode.getProcessGroupIdentifier());
        dto.setState(serviceNode.getState().name());

        entity.setComponent(dto);
        return entity;
    }

    private AffectedComponentEntity createStatelessGroupAffectedComponentEntity(final ProcessGroup group) {
        final AffectedComponentEntity entity = new AffectedComponentEntity();
        entity.setRevision(dtoFactory.createRevisionDTO(revisionManager.getRevision(group.getIdentifier())));
        entity.setId(group.getIdentifier());
        entity.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_STATELESS_GROUP);
        entity.setProcessGroup(createProcessGroupNameDto(group.getParent()));

        final PermissionsDTO permissionsDto = dtoFactory.createPermissionsDto(group);
        entity.setPermissions(permissionsDto);

        final AffectedComponentDTO dto = new AffectedComponentDTO();
        dto.setId(group.getIdentifier());
        dto.setReferenceType(AffectedComponentDTO.COMPONENT_TYPE_STATELESS_GROUP);
        dto.setProcessGroupId(group.getProcessGroupIdentifier());
        dto.setState(group.getStatelessScheduledState().name());

        entity.setComponent(dto);
        return entity;

    }

    private AffectedComponentEntity createAffectedComponentEntity(final InstantiatedVersionedComponent instance, final String componentTypeName, final String componentState) {
        final AffectedComponentEntity entity = new AffectedComponentEntity();
        entity.setRevision(dtoFactory.createRevisionDTO(revisionManager.getRevision(instance.getInstanceIdentifier())));
        entity.setId(instance.getInstanceIdentifier());
        entity.setReferenceType(componentTypeName);

        final String groupId = instance.getInstanceGroupId();
        if (groupId != null) {
            final ProcessGroupNameDTO groupNameDto = new ProcessGroupNameDTO();
            groupNameDto.setId(groupId);
            entity.setProcessGroup(groupNameDto);
        }

        final Authorizable authorizable = getAuthorizable(componentTypeName, instance);
        final PermissionsDTO permissionsDto = dtoFactory.createPermissionsDto(authorizable);
        entity.setPermissions(permissionsDto);

        final AffectedComponentDTO dto = new AffectedComponentDTO();
        dto.setId(instance.getInstanceIdentifier());
        dto.setReferenceType(componentTypeName);
        dto.setProcessGroupId(instance.getInstanceGroupId());
        dto.setState(componentState);

        entity.setComponent(dto);
        return entity;
    }


    private Authorizable getAuthorizable(final Connectable connectable) {
        switch (connectable.getConnectableType()) {
            case REMOTE_INPUT_PORT:
            case REMOTE_OUTPUT_PORT:
                final String rpgId = ((RemoteGroupPort) connectable).getRemoteProcessGroup().getIdentifier();
                return authorizableLookup.getRemoteProcessGroup(rpgId);
            default:
                return authorizableLookup.getLocalConnectable(connectable.getIdentifier());
        }
    }

    private Authorizable getAuthorizable(final String componentTypeName, final InstantiatedVersionedComponent versionedComponent) {
        final String componentId = versionedComponent.getInstanceIdentifier();

        if (componentTypeName.equals(org.apache.nifi.flow.ComponentType.CONTROLLER_SERVICE.name())) {
            return authorizableLookup.getControllerService(componentId).getAuthorizable();
        }

        if (componentTypeName.equals(org.apache.nifi.flow.ComponentType.CONNECTION.name())) {
            return authorizableLookup.getConnection(componentId).getAuthorizable();
        }

        if (componentTypeName.equals(org.apache.nifi.flow.ComponentType.FUNNEL.name())) {
            return authorizableLookup.getFunnel(componentId);
        }

        if (componentTypeName.equals(org.apache.nifi.flow.ComponentType.INPUT_PORT.name())) {
            return authorizableLookup.getInputPort(componentId);
        }

        if (componentTypeName.equals(org.apache.nifi.flow.ComponentType.OUTPUT_PORT.name())) {
            return authorizableLookup.getOutputPort(componentId);
        }

        if (componentTypeName.equals(org.apache.nifi.flow.ComponentType.LABEL.name())) {
            return authorizableLookup.getLabel(componentId);
        }

        if (componentTypeName.equals(org.apache.nifi.flow.ComponentType.PROCESS_GROUP.name())) {
            return authorizableLookup.getProcessGroup(componentId).getAuthorizable();
        }

        if (componentTypeName.equals(org.apache.nifi.flow.ComponentType.PROCESSOR.name())) {
            return authorizableLookup.getProcessor(componentId).getAuthorizable();
        }

        if (componentTypeName.equals(org.apache.nifi.flow.ComponentType.REMOTE_INPUT_PORT.name())) {
            return authorizableLookup.getRemoteProcessGroup(versionedComponent.getInstanceGroupId());
        }

        if (componentTypeName.equals(org.apache.nifi.flow.ComponentType.REMOTE_OUTPUT_PORT.name())) {
            return authorizableLookup.getRemoteProcessGroup(versionedComponent.getInstanceGroupId());
        }

        if (componentTypeName.equals(org.apache.nifi.flow.ComponentType.REMOTE_PROCESS_GROUP.name())) {
            return authorizableLookup.getRemoteProcessGroup(componentId);
        }

        return null;
    }

    @Override
    public String getFlowRegistryName(final String flowRegistryId) {
        final FlowRegistryClientNode flowRegistry = flowRegistryDAO.getFlowRegistryClient(flowRegistryId);
        return flowRegistry == null ? flowRegistryId : flowRegistry.getName();
    }

    private List<Revision> getComponentRevisions(final ProcessGroup processGroup, final boolean includeGroupRevision) {
        final List<Revision> revisions = new ArrayList<>();
        if (includeGroupRevision) {
            revisions.add(revisionManager.getRevision(processGroup.getIdentifier()));
        }

        processGroup.findAllConnections().stream()
            .map(component -> revisionManager.getRevision(component.getIdentifier()))
            .forEach(revisions::add);
        processGroup.findAllControllerServices().stream()
            .map(component -> revisionManager.getRevision(component.getIdentifier()))
            .forEach(revisions::add);
        processGroup.findAllFunnels().stream()
            .map(component -> revisionManager.getRevision(component.getIdentifier()))
            .forEach(revisions::add);
        processGroup.findAllInputPorts().stream()
            .map(component -> revisionManager.getRevision(component.getIdentifier()))
            .forEach(revisions::add);
        processGroup.findAllOutputPorts().stream()
            .map(component -> revisionManager.getRevision(component.getIdentifier()))
            .forEach(revisions::add);
        processGroup.findAllLabels().stream()
            .map(component -> revisionManager.getRevision(component.getIdentifier()))
            .forEach(revisions::add);
        processGroup.findAllProcessGroups().stream()
            .map(component -> revisionManager.getRevision(component.getIdentifier()))
            .forEach(revisions::add);
        processGroup.findAllProcessors().stream()
            .map(component -> revisionManager.getRevision(component.getIdentifier()))
            .forEach(revisions::add);
        processGroup.findAllRemoteProcessGroups().stream()
            .map(component -> revisionManager.getRevision(component.getIdentifier()))
            .forEach(revisions::add);
        processGroup.findAllRemoteProcessGroups().stream()
            .flatMap(rpg -> Stream.concat(rpg.getInputPorts().stream(), rpg.getOutputPorts().stream()))
            .map(component -> revisionManager.getRevision(component.getIdentifier()))
            .forEach(revisions::add);

        return revisions;
    }

    /**
     * For each versioned processor, if there is an instance id and that instance exists locally,
     * all sensitive properties from the local instance is copied into the versioned processor.
     *
     * @param processors the versioned processors to consider
     */
    private void copySensitiveProcessorProperties(final Set<VersionedProcessor> processors) {
        final FlowManager flowManager = controllerFacade.getFlowManager();

        processors.forEach(p -> {
            if (p.getInstanceIdentifier() != null) {
                final ProcessorNode copiedInstance = flowManager.getProcessorNode(p.getInstanceIdentifier());
                if (copiedInstance != null) {
                    copiedInstance.getProperties().keySet().stream()
                            .filter(PropertyDescriptor::isSensitive)
                            .forEach(pd -> {
                                p.getProperties().put(pd.getName(), copiedInstance.getRawPropertyValue(pd));
                            });
                }
            }
        });
    }

    /**
     * For each versioned service, if there is an instance id and that instance exists locally,
     * all sensitive properties from the local instance is copied into the versioned service.
     *
     * @param services the versioned services to consider
     */
    private void copySensitiveServiceProperties(final Set<VersionedControllerService> services) {
        final FlowManager flowManager = controllerFacade.getFlowManager();

        services.forEach(s -> {
            if (s.getInstanceIdentifier() != null) {
                final ControllerServiceNode copiedInstance = flowManager.getControllerServiceNode(s.getInstanceIdentifier());
                if (copiedInstance != null) {
                    copiedInstance.getProperties().keySet().stream()
                            .filter(PropertyDescriptor::isSensitive)
                            .forEach(pd -> {
                                s.getProperties().put(pd.getName(), copiedInstance.getRawPropertyValue(pd));
                            });
                }
            }
        });
    }

    /**
     * For each versioned group, all versioned processors and services will attempt to copy sensitive
     * properties from a local instance, if possible.
     *
     * @param groups the versioned groups to consider
     */
    private void copySensitiveDescendantProperties(final Set<VersionedProcessGroup> groups) {
        groups.forEach(pg -> {
            copySensitiveServiceProperties(pg.getControllerServices());
            copySensitiveProcessorProperties(pg.getProcessors());
            copySensitiveDescendantProperties(pg.getProcessGroups());
        });
    }

    @Override
    public PasteResponseEntity pasteComponents(final Revision revision, final String groupId, final VersionedComponentAdditions additions, final String componentIdSeed) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final RevisionUpdate<FlowSnippetDTO> snapshot = revisionManager.updateRevision(new StandardRevisionClaim(revision), user, () -> {
            // preprocess the additions and copy over any sensitive properties
            copySensitiveServiceProperties(additions.getControllerServices());
            copySensitiveProcessorProperties(additions.getProcessors());
            copySensitiveDescendantProperties(additions.getProcessGroups());

            // add the versioned components
            final ComponentAdditions componentAdditions = processGroupDAO.addVersionedComponents(groupId, additions, componentIdSeed);

            // save
            controllerFacade.save();

            // gather details for response
            final Set<ControllerServiceDTO> services = componentAdditions.getControllerServices().stream()
                    .map(s -> dtoFactory.createControllerServiceDto(s))
                    .collect(Collectors.toSet());
            final Set<ProcessorDTO> processors = componentAdditions.getProcessors().stream()
                    .map(p -> dtoFactory.createProcessorDto(p))
                    .collect(Collectors.toSet());
            final Set<PortDTO> inputPorts = componentAdditions.getInputPorts().stream()
                    .map(ip -> dtoFactory.createPortDto(ip))
                    .collect(Collectors.toSet());
            final Set<PortDTO> outputPorts = componentAdditions.getOutputPorts().stream()
                    .map(op -> dtoFactory.createPortDto(op))
                    .collect(Collectors.toSet());
            final Set<FunnelDTO> funnels = componentAdditions.getFunnels().stream()
                    .map(f -> dtoFactory.createFunnelDto(f))
                    .collect(Collectors.toSet());
            final Set<LabelDTO> labels = componentAdditions.getLabels().stream()
                    .map(l -> dtoFactory.createLabelDto(l))
                    .collect(Collectors.toSet());
            final Set<RemoteProcessGroupDTO> remoteProcessGroups = componentAdditions.getRemoteProcessGroups().stream()
                    .map(rpg -> dtoFactory.createRemoteProcessGroupDto(rpg))
                    .collect(Collectors.toSet());
            final Set<ProcessGroupDTO> processGroups = componentAdditions.getProcessGroups().stream()
                    .map(pg -> dtoFactory.createProcessGroupDto(pg))
                    .collect(Collectors.toSet());
            final Set<ConnectionDTO> connections = componentAdditions.getConnections().stream()
                    .map(c -> dtoFactory.createConnectionDto(c))
                    .collect(Collectors.toSet());

            // return the details using a flow snippet dto
            final FlowSnippetDTO flowSnippetDTO = new FlowSnippetDTO();
            flowSnippetDTO.setControllerServices(services);
            flowSnippetDTO.setProcessors(processors);
            flowSnippetDTO.setInputPorts(inputPorts);
            flowSnippetDTO.setOutputPorts(outputPorts);
            flowSnippetDTO.setFunnels(funnels);
            flowSnippetDTO.setLabels(labels);
            flowSnippetDTO.setRemoteProcessGroups(remoteProcessGroups);
            flowSnippetDTO.setProcessGroups(processGroups);
            flowSnippetDTO.setConnections(connections);

            final Revision updatedRevision = revisionManager.getRevision(revision.getComponentId()).incrementRevision(revision.getClientId());
            final FlowModification lastModification = new FlowModification(updatedRevision, user.getIdentity());
            return new StandardRevisionUpdate<>(flowSnippetDTO, lastModification);
        });

        // post process new flow snippet
        final FlowDTO flowDto = postProcessNewFlowSnippet(groupId, snapshot.getComponent());
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(snapshot.getLastModification());

        final PasteResponseEntity pasteEntity = new PasteResponseEntity();
        pasteEntity.setFlow(flowDto);
        pasteEntity.setRevision(updatedRevision);
        return pasteEntity;
    }

    @Override
    public ProcessGroupEntity updateProcessGroupContents(final Revision revision, final String groupId, final VersionControlInformationDTO versionControlInfo,
                                                         final RegisteredFlowSnapshot proposedFlowSnapshot, final String componentIdSeed, final boolean verifyNotModified,
                                                         final boolean updateSettings, final boolean updateDescendantVersionedFlows) {

        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final List<Revision> revisions = getComponentRevisions(processGroup, false);
        revisions.add(revision);

        final RevisionClaim revisionClaim = new StandardRevisionClaim(revisions);

        final RevisionUpdate<ProcessGroupDTO> revisionUpdate = revisionManager.updateRevision(revisionClaim, user, () -> {
            // update the Process Group
            final VersionedExternalFlow externalFlow = createVersionedExternalFlow(proposedFlowSnapshot);
            processGroupDAO.updateProcessGroupFlow(groupId, externalFlow, versionControlInfo, componentIdSeed, verifyNotModified, updateSettings,
                updateDescendantVersionedFlows);

            // update the revisions
            final Set<Revision> updatedRevisions = revisions.stream()
                .map(rev -> revisionManager.getRevision(rev.getComponentId()).incrementRevision(revision.getClientId()))
                .collect(Collectors.toSet());

            // save
            controllerFacade.save();

            // gather details for response
            final ProcessGroupDTO dto = dtoFactory.createProcessGroupDto(processGroup);

            final Revision updatedRevision = revisionManager.getRevision(groupId).incrementRevision(revision.getClientId());
            final FlowModification lastModification = new FlowModification(updatedRevision, user.getIdentity());
            return new StandardRevisionUpdate<>(dto, lastModification, updatedRevisions);
        });

        final FlowModification lastModification = revisionUpdate.getLastModification();

        final PermissionsDTO permissions = dtoFactory.createPermissionsDto(processGroup);
        final RevisionDTO updatedRevision = dtoFactory.createRevisionDTO(lastModification);
        final ProcessGroupStatusDTO status = dtoFactory.createConciseProcessGroupStatusDto(controllerFacade.getProcessGroupStatus(processGroup.getIdentifier()));
        final List<BulletinDTO> bulletins = dtoFactory.createBulletinDtos(bulletinRepository.findBulletinsForSource(processGroup.getIdentifier()));
        final List<BulletinEntity> bulletinEntities = bulletins.stream().map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissions.getCanRead())).collect(Collectors.toList());
        return entityFactory.createProcessGroupEntity(revisionUpdate.getComponent(), updatedRevision, permissions, status, bulletinEntities);
    }

    private AuthorizationResult authorizeAction(final Action action) {
        final String sourceId = action.getSourceId();
        final Component type = action.getSourceType();

        Authorizable authorizable;
        try {
            switch (type) {
                case Processor:
                    authorizable = authorizableLookup.getProcessor(sourceId).getAuthorizable();
                    break;
                case ReportingTask:
                    authorizable = authorizableLookup.getReportingTask(sourceId).getAuthorizable();
                    break;
                case FlowAnalysisRule:
                    authorizable = authorizableLookup.getFlowAnalysisRule(sourceId).getAuthorizable();
                    break;
                case FlowRegistryClient:
                    authorizable = authorizableLookup.getFlowRegistryClient(sourceId).getAuthorizable();
                    break;
                case ControllerService:
                    authorizable = authorizableLookup.getControllerService(sourceId).getAuthorizable();
                    break;
                case Controller:
                    authorizable = controllerFacade;
                    break;
                case InputPort:
                    authorizable = authorizableLookup.getInputPort(sourceId);
                    break;
                case OutputPort:
                    authorizable = authorizableLookup.getOutputPort(sourceId);
                    break;
                case ProcessGroup:
                    authorizable = authorizableLookup.getProcessGroup(sourceId).getAuthorizable();
                    break;
                case RemoteProcessGroup:
                    authorizable = authorizableLookup.getRemoteProcessGroup(sourceId);
                    break;
                case Funnel:
                    authorizable = authorizableLookup.getFunnel(sourceId);
                    break;
                case Connection:
                    authorizable = authorizableLookup.getConnection(sourceId).getAuthorizable();
                    break;
                case ParameterContext:
                    authorizable = authorizableLookup.getParameterContext(sourceId);
                    break;
                case ParameterProvider:
                    authorizable = authorizableLookup.getParameterProvider(sourceId).getAuthorizable();
                    break;
                case AccessPolicy:
                    authorizable = authorizableLookup.getAccessPolicyById(sourceId);
                    break;
                case User:
                case UserGroup:
                    authorizable = authorizableLookup.getTenant();
                    break;
                case Label:
                    authorizable = authorizableLookup.getLabel(sourceId);
                    break;
                default:
                    throw new WebApplicationException(Response.serverError().entity("An unexpected type of component is the source of this action.").build());
            }
        } catch (final ResourceNotFoundException e) {
            // if the underlying component is gone, use the controller to see if permissions should be allowed
            authorizable = controllerFacade;
        }

        // perform the authorization
        return authorizable.checkAuthorization(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
    }

    @Override
    public HistoryDTO getActions(final HistoryQueryDTO historyQueryDto) {
        // extract the query criteria
        final HistoryQuery historyQuery = new HistoryQuery();
        historyQuery.setStartDate(historyQueryDto.getStartDate());
        historyQuery.setEndDate(historyQueryDto.getEndDate());
        historyQuery.setSourceId(historyQueryDto.getSourceId());
        historyQuery.setUserIdentity(historyQueryDto.getUserIdentity());
        historyQuery.setOffset(historyQueryDto.getOffset());
        historyQuery.setCount(historyQueryDto.getCount());
        historyQuery.setSortColumn(historyQueryDto.getSortColumn());
        historyQuery.setSortOrder(historyQueryDto.getSortOrder());

        // perform the query
        final History history = auditService.getActions(historyQuery);

        // only retain authorized actions
        final HistoryDTO historyDto = dtoFactory.createHistoryDto(history);
        if (history.getActions() != null) {
            final List<ActionEntity> actionEntities = new ArrayList<>();
            for (final Action action : history.getActions()) {
                final AuthorizationResult result = authorizeAction(action);
                actionEntities.add(entityFactory.createActionEntity(dtoFactory.createActionDto(action), Result.Approved.equals(result.getResult())));
            }
            historyDto.setActions(actionEntities);
        }

        // create the response
        return historyDto;
    }

    @Override
    public ActionEntity getAction(final Integer actionId) {
        // get the action
        final Action action = auditService.getAction(actionId);

        // ensure the action was found
        if (action == null) {
            throw new ResourceNotFoundException(String.format("Unable to find action with id '%s'.", actionId));
        }

        final AuthorizationResult result = authorizeAction(action);
        final boolean authorized = Result.Approved.equals(result.getResult());
        if (!authorized) {
            throw new AccessDeniedException(result.getExplanation());
        }

        // return the action
        return entityFactory.createActionEntity(dtoFactory.createActionDto(action), authorized);
    }

    @Override
    public ComponentHistoryDTO getComponentHistory(final String componentId) {
        final Map<String, PropertyHistoryDTO> propertyHistoryDtos = new LinkedHashMap<>();
        final Map<String, List<PreviousValue>> propertyHistory = auditService.getPreviousValues(componentId);

        for (final Map.Entry<String, List<PreviousValue>> entry : propertyHistory.entrySet()) {
            final List<PreviousValueDTO> previousValueDtos = new ArrayList<>();

            for (final PreviousValue previousValue : entry.getValue()) {
                final PreviousValueDTO dto = new PreviousValueDTO();
                dto.setPreviousValue(previousValue.getPreviousValue());
                dto.setTimestamp(previousValue.getTimestamp());
                dto.setUserIdentity(previousValue.getUserIdentity());
                previousValueDtos.add(dto);
            }

            if (!previousValueDtos.isEmpty()) {
                final PropertyHistoryDTO propertyHistoryDto = new PropertyHistoryDTO();
                propertyHistoryDto.setPreviousValues(previousValueDtos);
                propertyHistoryDtos.put(entry.getKey(), propertyHistoryDto);
            }
        }

        final ComponentHistoryDTO history = new ComponentHistoryDTO();
        history.setComponentId(componentId);
        history.setPropertyHistory(propertyHistoryDtos);

        return history;
    }

    @Override
    public ProcessorDiagnosticsEntity getProcessorDiagnostics(final String id) {
        final ProcessorNode processor = processorDAO.getProcessor(id);
        final ProcessorStatus processorStatus = controllerFacade.getProcessorStatus(id);

        // Generate Processor Diagnostics
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        final ProcessorDiagnosticsDTO dto = controllerFacade.getProcessorDiagnostics(processor, processorStatus, bulletinRepository, serviceId -> {
            final ControllerServiceNode serviceNode = controllerServiceDAO.getControllerService(serviceId);
            return createControllerServiceEntity(serviceNode, true);
        });

        // Filter anything out of diagnostics that the user is not authorized to see.
        final List<JVMDiagnosticsSnapshotDTO> jvmDiagnosticsSnaphots = new ArrayList<>();
        final JVMDiagnosticsDTO jvmDiagnostics = dto.getJvmDiagnostics();
        jvmDiagnosticsSnaphots.add(jvmDiagnostics.getAggregateSnapshot());

        // filter controller-related information
        final boolean canReadController = authorizableLookup.getController().isAuthorized(authorizer, RequestAction.READ, user);
        if (!canReadController) {
            for (final JVMDiagnosticsSnapshotDTO snapshot : jvmDiagnosticsSnaphots) {
                snapshot.setControllerDiagnostics(null);
            }
        }

        // filter system diagnostics information
        final boolean canReadSystem = authorizableLookup.getSystem().isAuthorized(authorizer, RequestAction.READ, user);
        if (!canReadSystem) {
            for (final JVMDiagnosticsSnapshotDTO snapshot : jvmDiagnosticsSnaphots) {
                snapshot.setSystemDiagnosticsDto(null);
            }
        }

        final boolean canReadFlow = authorizableLookup.getFlow().isAuthorized(authorizer, RequestAction.READ, user);
        if (!canReadFlow) {
            for (final JVMDiagnosticsSnapshotDTO snapshot : jvmDiagnosticsSnaphots) {
                snapshot.setFlowDiagnosticsDto(null);
            }
        }

        // filter connections
        final Predicate<ConnectionDiagnosticsDTO> connectionAuthorized = connectionDiagnostics -> {
            final String connectionId = connectionDiagnostics.getConnection().getId();
            return authorizableLookup.getConnection(connectionId).getAuthorizable().isAuthorized(authorizer, RequestAction.READ, user);
        };

        // Filter incoming connections by what user is authorized to READ
        final Set<ConnectionDiagnosticsDTO> incoming = dto.getIncomingConnections();
        final Set<ConnectionDiagnosticsDTO> filteredIncoming = incoming.stream()
            .filter(connectionAuthorized)
            .collect(Collectors.toSet());

        dto.setIncomingConnections(filteredIncoming);

        // Filter outgoing connections by what user is authorized to READ
        final Set<ConnectionDiagnosticsDTO> outgoing = dto.getOutgoingConnections();
        final Set<ConnectionDiagnosticsDTO> filteredOutgoing = outgoing.stream()
            .filter(connectionAuthorized)
            .collect(Collectors.toSet());
        dto.setOutgoingConnections(filteredOutgoing);

        // Filter out any controller services that are referenced by the Processor
        final Set<ControllerServiceDiagnosticsDTO> referencedServices = dto.getReferencedControllerServices();
        final Set<ControllerServiceDiagnosticsDTO> filteredReferencedServices = referencedServices.stream()
            .filter(csDiagnostics -> {
                final String csId = csDiagnostics.getControllerService().getId();
                return authorizableLookup.getControllerService(csId).getAuthorizable().isAuthorized(authorizer, RequestAction.READ, user);
            })
            .map(csDiagnostics -> {
                // Filter out any referencing components because those are generally not relevant from this context.
                final ControllerServiceDTO serviceDto = csDiagnostics.getControllerService().getComponent();
                if (serviceDto != null) {
                    serviceDto.setReferencingComponents(null);
                }
                return csDiagnostics;
            })
            .collect(Collectors.toSet());
        dto.setReferencedControllerServices(filteredReferencedServices);

        final Revision revision = revisionManager.getRevision(id);
        final RevisionDTO revisionDto = dtoFactory.createRevisionDTO(revision);
        final PermissionsDTO permissionsDto = dtoFactory.createPermissionsDto(processor);
        final List<BulletinEntity> bulletins = bulletinRepository.findBulletinsForSource(id).stream()
            .map(bulletin -> dtoFactory.createBulletinDto(bulletin))
            .map(bulletin -> entityFactory.createBulletinEntity(bulletin, permissionsDto.getCanRead()))
            .collect(Collectors.toList());

        final ProcessorStatusDTO processorStatusDto = dtoFactory.createProcessorStatusDto(controllerFacade.getProcessorStatus(processor.getIdentifier()));
        return entityFactory.createProcessorDiagnosticsEntity(dto, revisionDto, permissionsDto, processorStatusDto, bulletins);
    }

    protected Collection<AbstractMetricsRegistry> populateFlowMetrics() {
        // Include registries which are fully refreshed upon each invocation
        NiFiMetricsRegistry nifiMetricsRegistry = new NiFiMetricsRegistry();
        BulletinMetricsRegistry bulletinMetricsRegistry = new BulletinMetricsRegistry();

        final NodeIdentifier node = controllerFacade.getNodeId();
        final String instId = StringUtils.isEmpty(controllerFacade.getInstanceId()) ? "" : controllerFacade.getInstanceId();
        final String instanceId = node == null ? instId : node.getId();
        ProcessGroupStatus rootPGStatus = controllerFacade.getProcessGroupStatus("root");

        PrometheusMetricsUtil.createNifiMetrics(nifiMetricsRegistry, rootPGStatus, instanceId, "", ROOT_PROCESS_GROUP,
                PrometheusMetricsUtil.METRICS_STRATEGY_COMPONENTS.getValue());

        // Add the total byte counts (read/written) to the NiFi metrics registry
        FlowFileEventRepository flowFileEventRepository = controllerFacade.getFlowFileEventRepository();
        final String rootPGId = StringUtils.isEmpty(rootPGStatus.getId()) ? "" : rootPGStatus.getId();
        final String rootPGName = StringUtils.isEmpty(rootPGStatus.getName()) ? "" : rootPGStatus.getName();
        final FlowFileEvent aggregateEvent = flowFileEventRepository.reportAggregateEvent();
        nifiMetricsRegistry.setDataPoint(aggregateEvent.getBytesRead(), "TOTAL_BYTES_READ",
                instanceId, ROOT_PROCESS_GROUP, rootPGName, rootPGId, "");
        nifiMetricsRegistry.setDataPoint(aggregateEvent.getBytesWritten(), "TOTAL_BYTES_WRITTEN",
                instanceId, ROOT_PROCESS_GROUP, rootPGName, rootPGId, "");
        nifiMetricsRegistry.setDataPoint(aggregateEvent.getBytesSent(), "TOTAL_BYTES_SENT",
                instanceId, ROOT_PROCESS_GROUP, rootPGName, rootPGId, "");
        nifiMetricsRegistry.setDataPoint(aggregateEvent.getBytesReceived(), "TOTAL_BYTES_RECEIVED",
                instanceId, ROOT_PROCESS_GROUP, rootPGName, rootPGId, "");

        //Add flow file repository, content repository and provenance repository usage to NiFi metrics
        final StorageUsage flowFileRepositoryUsage = controllerFacade.getFlowFileRepositoryStorageUsage();
        final Map<String, StorageUsage> contentRepositoryUsage = controllerFacade.getContentRepositoryStorageUsage();
        final Map<String, StorageUsage> provenanceRepositoryUsage = controllerFacade.getProvenanceRepositoryStorageUsage();

        PrometheusMetricsUtil.createStorageUsageMetrics(nifiMetricsRegistry, flowFileRepositoryUsage, contentRepositoryUsage, provenanceRepositoryUsage,
                instanceId, ROOT_PROCESS_GROUP, rootPGName, rootPGId, "");

        //Add total task duration for root to the NiFi metrics registry
        // The latest aggregated status history is the last element in the list so we need the last element only
        final StatusHistoryEntity rootGPStatusHistory = getProcessGroupStatusHistory(rootPGId);
        final List<StatusSnapshotDTO> aggregatedStatusHistory = rootGPStatusHistory.getStatusHistory().getAggregateSnapshots();
        final int lastIndex = aggregatedStatusHistory.size() - 1;
        final String taskDurationInMillis = ProcessGroupStatusDescriptor.TASK_MILLIS.getField();
        long taskDuration = 0;
        if (!aggregatedStatusHistory.isEmpty()) {
            final StatusSnapshotDTO latestStatusHistory = aggregatedStatusHistory.get(lastIndex);
            taskDuration = latestStatusHistory.getStatusMetrics().get(taskDurationInMillis);
        }
        nifiMetricsRegistry.setDataPoint(taskDuration, "TOTAL_TASK_DURATION",
                instanceId, ROOT_PROCESS_GROUP, rootPGName, rootPGId, "");

        PrometheusMetricsUtil.createJvmMetrics(jvmMetricsRegistry, JmxJvmMetrics.getInstance(), instanceId);

        final Map<String, Double> aggregatedMetrics = new HashMap<>();
        PrometheusMetricsUtil.aggregatePercentUsed(rootPGStatus, aggregatedMetrics);
        PrometheusMetricsUtil.createAggregatedNifiMetrics(nifiMetricsRegistry, aggregatedMetrics, instanceId, ROOT_PROCESS_GROUP, rootPGName, rootPGId);

        // Get Connection Status Analytics (predictions, e.g.)
        Collection<Map<String, Long>> predictions = parallelProcessingService.createConnectionStatusAnalyticsMetricsAndCollectPredictions(
                controllerFacade, connectionAnalyticsMetricsRegistry, instanceId);

        predictions.forEach((prediction) -> PrometheusMetricsUtil.aggregateConnectionPredictionMetrics(aggregatedMetrics, prediction));
        PrometheusMetricsUtil.createAggregatedConnectionStatusAnalyticsMetrics(connectionAnalyticsMetricsRegistry, aggregatedMetrics, instanceId, ROOT_PROCESS_GROUP, rootPGName, rootPGId);

        // Create a query to get all bulletins
        final BulletinQueryDTO query = new BulletinQueryDTO();
        BulletinBoardDTO bulletinBoardDTO = getBulletinBoard(query);
        for (BulletinEntity bulletinEntity : bulletinBoardDTO.getBulletins()) {
            BulletinDTO bulletin = bulletinEntity.getBulletin();
            if (bulletin != null) {
                PrometheusMetricsUtil.createBulletinMetrics(bulletinMetricsRegistry, instanceId,
                        "Bulletin",
                        String.valueOf(bulletin.getId()),
                        bulletin.getGroupId() == null ? "" : bulletin.getGroupId(),
                        bulletin.getNodeAddress() == null ? "" : bulletin.getNodeAddress(),
                        bulletin.getCategory(),
                        bulletin.getSourceName(),
                        bulletin.getSourceId(),
                        bulletin.getLevel()
                );
            }
        }

        // Collect cluster summary metrics
        int connectedNodeCount = 0;
        int totalNodeCount = 0;
        String connectedNodesLabel = "Not clustered";
        if (clusterCoordinator != null && clusterCoordinator.isConnected()) {
            final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = clusterCoordinator.getConnectionStates();
            for (final List<NodeIdentifier> nodeList : stateMap.values()) {
                totalNodeCount += nodeList.size();
            }
            final List<NodeIdentifier> connectedNodeIds = stateMap.get(NodeConnectionState.CONNECTED);
            connectedNodeCount = (connectedNodeIds == null) ? 0 : connectedNodeIds.size();

            connectedNodesLabel = connectedNodeCount + " / " + totalNodeCount;
        }
        final boolean isClustered = clusterCoordinator != null;
        final boolean isConnectedToCluster = isClustered() && clusterCoordinator.isConnected();
        PrometheusMetricsUtil.createClusterMetrics(clusterMetricsRegistry, instanceId, isClustered, isConnectedToCluster, connectedNodesLabel, connectedNodeCount, totalNodeCount);
        Collection<AbstractMetricsRegistry> metricsRegistries = Arrays.asList(
                nifiMetricsRegistry,
                jvmMetricsRegistry,
                connectionAnalyticsMetricsRegistry,
                bulletinMetricsRegistry,
                clusterMetricsRegistry
        );

        return metricsRegistries;
    }

    @Override
    public Collection<CollectorRegistry> generateFlowMetrics() {

        return populateFlowMetrics().stream().map(AbstractMetricsRegistry::getRegistry)
                                             .collect(Collectors.toList());
    }

    @Override
    public Collection<CollectorRegistry> generateFlowMetrics(final Set<FlowMetricsRegistry> includeRegistries) {
        final Set<FlowMetricsRegistry> selectedRegistries = includeRegistries.isEmpty() ? new HashSet<>(Arrays.asList(FlowMetricsRegistry.values())) : includeRegistries;

        final Set<Class<? extends AbstractMetricsRegistry>> registryClasses = selectedRegistries.stream()
                .map(FlowMetricsRegistry::getRegistryClass)
                .collect(Collectors.toSet());

        Collection<AbstractMetricsRegistry> configuredRegistries = populateFlowMetrics();

        return configuredRegistries.stream()
                .filter(configuredRegistry -> registryClasses.contains(configuredRegistry.getClass()))
                .map(AbstractMetricsRegistry::getRegistry)
                .collect(Collectors.toList());
    }

    @Override
    public boolean isClustered() {
        return controllerFacade.isClustered();
    }

    @Override
    public String getNodeId() {
        final NodeIdentifier nodeId = controllerFacade.getNodeId();
        if (nodeId != null) {
            return nodeId.getId();
        } else {
            return null;
        }
    }

    @Override
    public ClusterDTO getCluster() {
        // create cluster summary dto
        final ClusterDTO clusterDto = new ClusterDTO();

        // set current time
        clusterDto.setGenerated(new Date());

        // create node dtos
        final List<NodeDTO> nodeDtos = clusterCoordinator.getNodeIdentifiers().stream()
            .map(nodeId -> getNode(nodeId))
            .collect(Collectors.toList());
        clusterDto.setNodes(nodeDtos);

        return clusterDto;
    }

    @Override
    public NodeDTO getNode(final String nodeId) {
        final NodeIdentifier nodeIdentifier = clusterCoordinator.getNodeIdentifier(nodeId);
        return getNode(nodeIdentifier);
    }

    private NodeDTO getNode(final NodeIdentifier nodeId) {
        final NodeConnectionStatus nodeStatus = clusterCoordinator.getConnectionStatus(nodeId);
        final List<NodeEvent> events = clusterCoordinator.getNodeEvents(nodeId);
        final Set<String> roles = getRoles(nodeId);
        final NodeHeartbeat heartbeat = heartbeatMonitor.getLatestHeartbeat(nodeId);
        return dtoFactory.createNodeDTO(nodeId, nodeStatus, heartbeat, events, roles);
    }

    private Set<String> getRoles(final NodeIdentifier nodeId) {
        final Set<String> roles = new HashSet<>();
        final String nodeAddress = nodeId.getSocketAddress() + ":" + nodeId.getSocketPort();

        for (final String roleName : ClusterRoles.getAllRoles()) {
            final Optional<String> leader = leaderElectionManager.getLeader(roleName);
            if (!leader.isPresent()) {
                continue;
            }

            final String leaderAddress = leader.get();
            if (leaderAddress.equals(nodeAddress)) {
                roles.add(roleName);
            }
        }

        return roles;
    }

    @Override
    public void deleteNode(final String nodeId) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user == null) {
            throw new WebApplicationException(new Throwable("Unable to access details for current user."));
        }

        final String userDn = user.getIdentity();
        final NodeIdentifier nodeIdentifier = clusterCoordinator.getNodeIdentifier(nodeId);
        if (nodeIdentifier == null) {
            throw new UnknownNodeException("Cannot remove Node with ID " + nodeId + " because it is not part of the cluster");
        }

        final NodeConnectionStatus nodeConnectionStatus = clusterCoordinator.getConnectionStatus(nodeIdentifier);
        if (!nodeConnectionStatus.getState().equals(NodeConnectionState.OFFLOADED) && !nodeConnectionStatus.getState().equals(NodeConnectionState.DISCONNECTED)) {
            throw new IllegalNodeDeletionException("Cannot remove Node with ID " + nodeId +
                    " because it is not disconnected or offloaded, current state = " + nodeConnectionStatus.getState());
        }

        clusterCoordinator.removeNode(nodeIdentifier, userDn);
        heartbeatMonitor.removeHeartbeat(nodeIdentifier);
    }

    @Override
    public Set<DocumentedTypeDTO> getFlowAnalysisRuleTypes(final String bundleGroup, final String bundleArtifact, final String type) {
        return controllerFacade.getFlowAnalysisRuleTypes(bundleGroup, bundleArtifact, type);
    }

    @Override
    public Set<FlowAnalysisRuleEntity> getFlowAnalysisRules() {
        Set<FlowAnalysisRuleEntity> flowAnalysisRules = flowAnalysisRuleDAO.getFlowAnalysisRules().stream()
            .map(flowAnalysisRule -> createFlowAnalysisRuleEntity(flowAnalysisRule))
            .collect(Collectors.toSet());

        return flowAnalysisRules;
    }

    @Override
    public FlowAnalysisResultEntity getFlowAnalysisResult() {
        Collection<RuleViolation> ruleViolations = ruleViolationsManager.getAllRuleViolations();

        FlowAnalysisResultEntity flowAnalysisResultEntity = createFlowAnalysisResultEntity(ruleViolations);

        return flowAnalysisResultEntity;
    }

    @Override
    public FlowAnalysisResultEntity getFlowAnalysisResult(String processGroupId) {
        Set<RuleViolation> ruleViolations = getRuleViolationStream(processGroupId).collect(Collectors.toSet());

        FlowAnalysisResultEntity flowAnalysisResultEntity = createFlowAnalysisResultEntity(ruleViolations);

        return flowAnalysisResultEntity;
    }

    public Stream<RuleViolation> getRuleViolationStream(String processGroupId) {
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupId);

        Collection<RuleViolation> ruleViolations = ruleViolationsManager.getRuleViolationsForGroup(processGroupId);

        Stream<RuleViolation> ruleViolationStreamForGroupAndAllChildren = Stream.concat(
            ruleViolations.stream(),
            processGroup.getProcessGroups().stream()
                .map(ProcessGroup::getIdentifier)
                .flatMap(this::getRuleViolationStream)
        );

        return ruleViolationStreamForGroupAndAllChildren;
    }

    public FlowAnalysisResultEntity createFlowAnalysisResultEntity(Collection<RuleViolation> ruleViolations) {
        FlowAnalysisResultEntity entity = new FlowAnalysisResultEntity();

        controllerFacade.getFlowManager().getFlowAnalyzer().ifPresent(
            flowAnalyzer -> entity.setFlowAnalysisPending(flowAnalyzer.isFlowAnalysisRequired())
        );

        List<FlowAnalysisRuleDTO> flowAnalysisRuleDtos = flowAnalysisRuleDAO.getFlowAnalysisRules().stream()
            .filter(FlowAnalysisRuleNode::isEnabled)
            .sorted(Comparator.comparing(FlowAnalysisRuleNode::getName))
            .map(flowAnalysisRule -> dtoFactory.createFlowAnalysisRuleDto(flowAnalysisRule))
            .collect(Collectors.toList());

        List<FlowAnalysisRuleViolationDTO> ruleViolationDtos = ruleViolations.stream()
            .sorted(Comparator.comparing(RuleViolation::getSubjectId)
                    .thenComparing(RuleViolation::getScope)
                    .thenComparing(RuleViolation::getRuleId)
                    .thenComparing(RuleViolation::getIssueId)
            )
            .map(ruleViolation -> {
                FlowAnalysisRuleViolationDTO ruleViolationDto = new FlowAnalysisRuleViolationDTO();

                String subjectId = ruleViolation.getSubjectId();
                String groupId = ruleViolation.getGroupId();

                ruleViolationDto.setScope(ruleViolation.getScope());
                ruleViolationDto.setSubjectId(subjectId);
                ruleViolationDto.setRuleId(ruleViolation.getRuleId());
                ruleViolationDto.setIssueId(ruleViolation.getIssueId());

                ruleViolationDto.setSubjectComponentType(ruleViolation.getSubjectComponentType().name());
                ruleViolationDto.setEnforcementPolicy(ruleViolation.getEnforcementPolicy().toString());

                PermissionsDTO subjectPermissionDto = createPermissionDto(
                        subjectId,
                        ruleViolation.getSubjectComponentType(),
                        groupId
                );
                ruleViolationDto.setSubjectPermissionDto(subjectPermissionDto);

                if (subjectPermissionDto.getCanRead()) {
                    ruleViolationDto.setGroupId(groupId);
                    ruleViolationDto.setSubjectDisplayName(ruleViolation.getSubjectDisplayName());
                    ruleViolationDto.setViolationMessage(ruleViolation.getViolationMessage());
                }

                return ruleViolationDto;
            })
            .collect(Collectors.toList());

        entity.setRules(flowAnalysisRuleDtos);
        entity.setRuleViolations(ruleViolationDtos);

        return entity;
    }

    @Override
    public NarSummaryEntity uploadNar(final InputStream inputStream) throws IOException {
        final NarInstallRequest installRequest = NarInstallRequest.builder()
                .source(NarSource.UPLOAD)
                .sourceIdentifier(NarSource.UPLOAD.name().toLowerCase())
                .inputStream(inputStream)
                .build();
        final NarNode narNode = narManager.installNar(installRequest);
        final NarSummaryDTO narSummaryDTO = dtoFactory.createNarSummaryDto(narNode);
        return entityFactory.createNarSummaryEntity(narSummaryDTO);
    }

    @Override
    public Set<NarSummaryEntity> getNarSummaries() {
        return narManager.getNars().stream()
                .sorted((o1, o2) -> {
                    final BundleCoordinate coordinate1 = o1.getManifest().getCoordinate();
                    final BundleCoordinate coordinate2 = o2.getManifest().getCoordinate();
                    return Comparator.comparing(BundleCoordinate::getGroup)
                            .thenComparing(BundleCoordinate::getId)
                            .thenComparing(BundleCoordinate::getVersion)
                            .compare(coordinate1, coordinate2);
                })
                .map(dtoFactory::createNarSummaryDto)
                .map(entityFactory::createNarSummaryEntity)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    public NarSummaryEntity getNarSummary(final String identifier) {
        final NarNode narNode = narManager.getNar(identifier)
                .orElseThrow(() -> new ResourceNotFoundException("A NAR does not exist with the given identifier"));
        final NarSummaryDTO narSummaryDTO = dtoFactory.createNarSummaryDto(narNode);
        return entityFactory.createNarSummaryEntity(narSummaryDTO);
    }

    @Override
    public NarDetailsEntity getNarDetails(final String identifier) {
        final NarNode narNode = narManager.getNar(identifier)
                .orElseThrow(() -> new ResourceNotFoundException("A NAR does not exist with the given identifier"));

        final BundleCoordinate coordinate = narNode.getManifest().getCoordinate();
        final Set<ExtensionDefinition> extensionDefinitions = new HashSet<>();
        extensionDefinitions.addAll(controllerFacade.getExtensionManager().getTypes(coordinate));
        extensionDefinitions.addAll(controllerFacade.getExtensionManager().getPythonExtensions(coordinate));

        final Set<NarCoordinateDTO> dependentCoordinates = new HashSet<>();
        final Set<Bundle> dependentBundles = controllerFacade.getExtensionManager().getDependentBundles(coordinate);
        if (dependentBundles != null) {
            for (final Bundle dependentBundle : dependentBundles) {
                final NarCoordinateDTO dependentCoordinate = dtoFactory.createNarCoordinateDto(dependentBundle.getBundleDetails().getCoordinate());
                dependentCoordinates.add(dependentCoordinate);
            }
        }

        final NarDetailsEntity componentTypesEntity = new NarDetailsEntity();
        componentTypesEntity.setNarSummary(dtoFactory.createNarSummaryDto(narNode));
        componentTypesEntity.setDependentCoordinates(dependentCoordinates);
        componentTypesEntity.setProcessorTypes(dtoFactory.fromDocumentedTypes(getTypes(extensionDefinitions, Processor.class)));
        componentTypesEntity.setControllerServiceTypes(dtoFactory.fromDocumentedTypes(getTypes(extensionDefinitions, ControllerService.class)));
        componentTypesEntity.setReportingTaskTypes(dtoFactory.fromDocumentedTypes(getTypes(extensionDefinitions, ReportingTask.class)));
        componentTypesEntity.setParameterProviderTypes(dtoFactory.fromDocumentedTypes(getTypes(extensionDefinitions, ParameterProvider.class)));
        componentTypesEntity.setFlowRegistryClientTypes(dtoFactory.fromDocumentedTypes(getTypes(extensionDefinitions, FlowRegistryClient.class)));
        componentTypesEntity.setFlowAnalysisRuleTypes(dtoFactory.fromDocumentedTypes(getTypes(extensionDefinitions, FlowAnalysisRule.class)));
        return componentTypesEntity;
    }

    private Set<ExtensionDefinition> getTypes(final Set<ExtensionDefinition> extensionDefinitions, final Class<?> extensionType) {
        return extensionDefinitions.stream()
                .filter(extensionDefinition -> extensionDefinition.getExtensionType().equals(extensionType))
                .collect(Collectors.toSet());
    }

    @Override
    public InputStream readNar(final String identifier) {
        return narManager.readNar(identifier);
    }

    @Override
    public void verifyDeleteNar(final String identifier, final boolean forceDelete) {
        narManager.verifyDeleteNar(identifier, forceDelete);
    }

    @Override
    public NarSummaryEntity deleteNar(final String identifier) throws IOException {
        final NarNode narNode = narManager.deleteNar(identifier);
        final NarSummaryDTO narSummaryDTO = dtoFactory.createNarSummaryDto(narNode);
        return entityFactory.createNarSummaryEntity(narSummaryDTO);
    }

    @Override
    public void verifyDeleteAsset(final String parameterContextId, final String assetId) {
        final ParameterContext parameterContext = parameterContextDAO.getParameterContext(parameterContextId);
        final Set<String> referencingParameterNames = getReferencingParameterNames(parameterContext, assetId);
        if (!referencingParameterNames.isEmpty()) {
            final String joinedParametersNames = String.join(", ", referencingParameterNames);
            throw new IllegalStateException("Unable to delete Asset [%s] because it is currently references by Parameters [%s]".formatted(assetId, joinedParametersNames));
        }
    }

    private Set<String> getReferencingParameterNames(final ParameterContext parameterContext, final String assetId) {
        final Set<String> referencingParameterNames = new HashSet<>();
        for (final Parameter parameter : parameterContext.getParameters().values()) {
            if (parameter.getReferencedAssets() != null) {
                for (final Asset asset : parameter.getReferencedAssets()) {
                    if (asset.getIdentifier().equals(assetId)) {
                        referencingParameterNames.add(parameter.getDescriptor().getName());
                    }
                }
            }
        }
        return referencingParameterNames;
    }

    @Override
    public AssetEntity deleteAsset(final String parameterContextId, final String assetId) {
        verifyDeleteAsset(parameterContextId, assetId);
        final Asset deletedAsset = assetManager.deleteAsset(assetId)
                .orElseThrow(() -> new ResourceNotFoundException("Asset does not exist with id [%s]".formatted(assetId)));
        return dtoFactory.createAssetEntity(deletedAsset);
    }

    private PermissionsDTO createPermissionDto(
            final String id,
            final org.apache.nifi.flow.ComponentType subjectComponentType,
            final String groupId
    ) {
        final InstantiatedVersionedComponent versionedComponent = new InstantiatedVersionedComponent() {
            @Override
            public String getInstanceIdentifier() {
                return id;
            }

            @Override
            public String getInstanceGroupId() {
                return groupId;
            }
        };

        Authorizable authorizable;
        try {
            authorizable = getAuthorizable(subjectComponentType.name(), versionedComponent);
        } catch (Exception e) {
            authorizable = null;
        }

        final PermissionsDTO permissionDto;
        if (authorizable != null) {
            permissionDto = dtoFactory.createPermissionsDto(authorizable, NiFiUserUtils.getNiFiUser());
        } else {
            permissionDto = new PermissionsDTO();
            permissionDto.setCanRead(false);
            permissionDto.setCanWrite(false);
        }

        return permissionDto;
    }

    /* reusable function declarations for converting ids to tenant entities */
    private Function<String, TenantEntity> mapUserGroupIdToTenantEntity(final boolean enforceGroupExistence) {
        return userGroupId -> {
            final RevisionDTO userGroupRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userGroupId));

            final Group group;
            if (enforceGroupExistence || userGroupDAO.hasUserGroup(userGroupId)) {
                group = userGroupDAO.getUserGroup(userGroupId);
            } else {
                group = new Group.Builder().identifier(userGroupId).name("Group ID - " + userGroupId + " (removed externally)").build();
            }

            return entityFactory.createTenantEntity(dtoFactory.createTenantDTO(group), userGroupRevision,
                    dtoFactory.createPermissionsDto(authorizableLookup.getTenant()));
        };
    }

    private Function<String, TenantEntity> mapUserIdToTenantEntity(final boolean enforceUserExistence) {
        return userId -> {
            final RevisionDTO userRevision = dtoFactory.createRevisionDTO(revisionManager.getRevision(userId));

            final User user;
            if (enforceUserExistence || userDAO.hasUser(userId)) {
                user = userDAO.getUser(userId);
            } else {
                user = new User.Builder().identifier(userId).identity("User ID - " + userId + " (removed externally)").build();
            }

            return entityFactory.createTenantEntity(dtoFactory.createTenantDTO(user), userRevision,
                    dtoFactory.createPermissionsDto(authorizableLookup.getTenant()));
        };
    }

    private PropertyDescriptor getPropertyDescriptor(final ComponentNode componentNode, final String property, final boolean sensitive) {
        final PropertyDescriptor propertyDescriptor;

        final PropertyDescriptor componentDescriptor = componentNode.getPropertyDescriptor(property);
        if (componentDescriptor == null) {
            propertyDescriptor = new PropertyDescriptor.Builder().name(property).addValidator(Validator.INVALID).dynamic(true).build();
        } else if (
                componentDescriptor.isDynamic() && (
                        // Allow setting sensitive status for properties marked as sensitive in previous requests
                        componentNode.isSensitiveDynamicProperty(property) || (
                                // Allow setting sensitive status for properties not marked as sensitive in supporting components
                                !componentDescriptor.isSensitive() && componentNode.isSupportsSensitiveDynamicProperties()
                        )
                )
        ) {
            propertyDescriptor = new PropertyDescriptor.Builder().fromPropertyDescriptor(componentDescriptor).sensitive(sensitive).build();
        } else {
            propertyDescriptor = componentDescriptor;
        }

        return propertyDescriptor;
    }

    private boolean isLogoutSupported() {
        // Logout is supported when authenticated using a JSON Web Token
        return NiFiUserUtils.getAuthenticationCredentials()
                .map(credentials -> credentials instanceof OAuth2Token)
                .orElse(false);
    }

    @Override
    public void verifyPublicInputPortUniqueness(final String portId, final String portName) {
        inputPortDAO.verifyPublicPortUniqueness(portId, portName);
    }

    @Override
    public void verifyPublicOutputPortUniqueness(final String portId, final String portName) {
        outputPortDAO.verifyPublicPortUniqueness(portId, portName);
    }

    /**
     * Create a new flow mapper using a mockable method for testing
     *
     * @param extensionManager  the extension manager to create the flow mapper with
     * @return a new NiFiRegistryFlowMapper instance
     */
    protected NiFiRegistryFlowMapper makeNiFiRegistryFlowMapper(final ExtensionManager extensionManager) {
        return new NiFiRegistryFlowMapper(extensionManager);
    }

    /**
     * Create a new flow mapper using a mockable method for testing
     *
     * @param extensionManager  the extension manager to create the flow mapper with
     * @param options  the flow mapping options
     * @return a new NiFiRegistryFlowMapper instance
     */
    protected NiFiRegistryFlowMapper makeNiFiRegistryFlowMapper(final ExtensionManager extensionManager, final FlowMappingOptions options) {
        return new NiFiRegistryFlowMapper(extensionManager, options);
    }

    @Autowired
    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Autowired
    public void setControllerFacade(final ControllerFacade controllerFacade) {
        this.controllerFacade = controllerFacade;
    }

    @Autowired
    public void setRemoteProcessGroupDAO(final RemoteProcessGroupDAO remoteProcessGroupDAO) {
        this.remoteProcessGroupDAO = remoteProcessGroupDAO;
    }

    @Autowired
    public void setLabelDAO(final LabelDAO labelDAO) {
        this.labelDAO = labelDAO;
    }

    @Autowired
    public void setFunnelDAO(final FunnelDAO funnelDAO) {
        this.funnelDAO = funnelDAO;
    }

    @Autowired
    public void setSnippetDAO(final SnippetDAO snippetDAO) {
        this.snippetDAO = snippetDAO;
    }

    @Autowired
    public void setProcessorDAO(final ProcessorDAO processorDAO) {
        this.processorDAO = processorDAO;
    }

    @Autowired
    public void setConnectionDAO(final ConnectionDAO connectionDAO) {
        this.connectionDAO = connectionDAO;
    }

    @Autowired
    public void setAuditService(final AuditService auditService) {
        this.auditService = auditService;
    }

    @Autowired
    public void setRevisionManager(final RevisionManager revisionManager) {
        this.revisionManager = revisionManager;
    }

    @Autowired
    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

    @Autowired
    public void setEntityFactory(final EntityFactory entityFactory) {
        this.entityFactory = entityFactory;
    }

    @Qualifier("standardInputPortDAO")
    @Autowired
    public void setInputPortDAO(final PortDAO inputPortDAO) {
        this.inputPortDAO = inputPortDAO;
    }

    @Qualifier("standardOutputPortDAO")
    @Autowired
    public void setOutputPortDAO(final PortDAO outputPortDAO) {
        this.outputPortDAO = outputPortDAO;
    }

    @Autowired
    public void setProcessGroupDAO(final ProcessGroupDAO processGroupDAO) {
        this.processGroupDAO = processGroupDAO;
    }

    @Autowired
    public void setControllerServiceDAO(final ControllerServiceDAO controllerServiceDAO) {
        this.controllerServiceDAO = controllerServiceDAO;
    }

    @Autowired
    public void setReportingTaskDAO(final ReportingTaskDAO reportingTaskDAO) {
        this.reportingTaskDAO = reportingTaskDAO;
    }

    @Autowired
    public void setFlowAnalysisRuleDAO(FlowAnalysisRuleDAO flowAnalysisRuleDAO) {
        this.flowAnalysisRuleDAO = flowAnalysisRuleDAO;
    }

    @Autowired
    public void setParameterProviderDAO(final ParameterProviderDAO parameterProviderDAO) {
        this.parameterProviderDAO = parameterProviderDAO;
    }

    @Autowired
    public void setParameterContextDAO(final ParameterContextDAO parameterContextDAO) {
        this.parameterContextDAO = parameterContextDAO;
    }

    @Autowired
    public void setSnippetUtils(final SnippetUtils snippetUtils) {
        this.snippetUtils = snippetUtils;
    }

    @Autowired
    public void setAuthorizableLookup(final AuthorizableLookup authorizableLookup) {
        this.authorizableLookup = authorizableLookup;
    }

    @Autowired
    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    @Autowired
    public void setUserDAO(final UserDAO userDAO) {
        this.userDAO = userDAO;
    }

    @Autowired
    public void setUserGroupDAO(final UserGroupDAO userGroupDAO) {
        this.userGroupDAO = userGroupDAO;
    }

    @Autowired
    public void setAccessPolicyDAO(final AccessPolicyDAO accessPolicyDAO) {
        this.accessPolicyDAO = accessPolicyDAO;
    }

    @Autowired
    public void setClusterCoordinator(final ClusterCoordinator coordinator) {
        this.clusterCoordinator = coordinator;
    }

    @Autowired(required = false)
    public void setHeartbeatMonitor(final HeartbeatMonitor heartbeatMonitor) {
        this.heartbeatMonitor = heartbeatMonitor;
    }

    @Autowired
    public void setBulletinRepository(final BulletinRepository bulletinRepository) {
        this.bulletinRepository = bulletinRepository;
    }

    @Autowired
    public void setLeaderElectionManager(final LeaderElectionManager leaderElectionManager) {
        this.leaderElectionManager = leaderElectionManager;
    }

    @Autowired
    public void setFlowRegistryDAO(FlowRegistryDAO flowRegistryDao) {
        this.flowRegistryDAO = flowRegistryDao;
    }

    @Autowired
    public void setRuleViolationsManager(RuleViolationsManager ruleViolationsManager) {
        this.ruleViolationsManager = ruleViolationsManager;
    }

    @Autowired
    public void setParallelProcessingService(PredictionBasedParallelProcessingService parallelProcessingService) {
        this.parallelProcessingService = parallelProcessingService;
    }

    @Autowired
    public void setNarManager(final NarManager narManager) {
        this.narManager = narManager;
    }

    @Autowired
    public void setAssetManager(final AssetManager assetManager) {
        this.assetManager = assetManager;
    }
}
