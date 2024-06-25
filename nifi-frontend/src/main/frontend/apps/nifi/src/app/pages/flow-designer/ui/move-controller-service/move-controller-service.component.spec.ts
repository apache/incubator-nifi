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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ControllerServiceEntity } from './../../../../state/shared/index';
import { MoveControllerService } from './move-controller-service.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../state/contoller-service-state/controller-service-state.reducer';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { ComponentType } from '../../../../state/shared';
import { MoveControllerServiceDialogRequest } from '../../state/controller-services';
import { ProcessGroupFlow } from './../../state/flow/index';

describe('MoveControllerService', () => {
    let component: MoveControllerService;
    let fixture: ComponentFixture<MoveControllerService>;
    const flow: ProcessGroupFlow = {
        id: 'cc4eefda-018d-1000-35ee-194d439257e4',
        parameterContext: null,
        uri: 'http://localhost/nifi-api/flow/process-groups/cc4eefda-018d-1000-35ee-194d439257e4',
        parentGroupId: '96456bc3-018b-1000-05d8-51720bebae4b',
        breadcrumb: {
            id: 'cc4eefda-018d-1000-35ee-194d439257e4',
            permissions: {
                canRead: true,
                canWrite: true
            },
            breadcrumb: {
                id: 'cc4eefda-018d-1000-35ee-194d439257e4',
                name: 'pg1'
            },
            parentBreadcrumb: {
                id: '96456bc3-018b-1000-05d8-51720bebae4b',
                permissions: {
                    canRead: true,
                    canWrite: true
                },
                breadcrumb: {
                    id: '96456bc3-018b-1000-05d8-51720bebae4b',
                    name: 'NiFi Flow'
                },
                versionedFlowState: ''
            },
            versionedFlowState: ''
        },
        flow: {
            processGroups: [
                {
                    revision: {
                        version: 0
                    },
                    id: 'cc4f110c-018d-1000-d4b6-d933a69c9d53',
                    position: {
                        x: 450.00000073872457,
                        y: 89.00000213800337
                    },
                    permissions: {
                        canRead: true,
                        canWrite: true
                    },
                    component: {
                        id: 'cc4f110c-018d-1000-d4b6-d933a69c9d53',
                        versionedComponentId: '11610e12-1853-3179-9e6b-714a77c470e2',
                        parentGroupId: 'cc4eefda-018d-1000-35ee-194d439257e4',
                        position: {
                            x: 450.00000073872457,
                            y: 89.00000213800337
                        },
                        name: 'pg2',
                        comments: '',
                        flowfileConcurrency: 'UNBOUNDED',
                        flowfileOutboundPolicy: 'STREAM_WHEN_AVAILABLE',
                        defaultFlowFileExpiration: '0 sec',
                        defaultBackPressureObjectThreshold: 10000,
                        defaultBackPressureDataSizeThreshold: '1 GB',
                        executionEngine: 'INHERITED',
                        maxConcurrentTasks: 1,
                        statelessFlowTimeout: '1 min',
                        runningCount: 0,
                        stoppedCount: 0,
                        invalidCount: 0,
                        disabledCount: 0,
                        activeRemotePortCount: 0,
                        inactiveRemotePortCount: 0,
                        upToDateCount: 0,
                        locallyModifiedCount: 0,
                        staleCount: 0,
                        locallyModifiedAndStaleCount: 0,
                        syncFailureCount: 0,
                        localInputPortCount: 0,
                        localOutputPortCount: 0,
                        publicInputPortCount: 0,
                        publicOutputPortCount: 0,
                        statelessGroupScheduledState: 'STOPPED',
                        inputPortCount: 0,
                        outputPortCount: 0
                    }
                }
            ],
            remoteProcessGroups: [],
            processors: [
                {
                    revision: {
                        version: 0
                    },
                    id: '018e1000-93c7-1b09-09f3-8e5a070f0381',
                    position: {
                        x: 760,
                        y: 272
                    },
                    permissions: {
                        canRead: true,
                        canWrite: true
                    },
                    component: {
                        id: '018e1000-93c7-1b09-09f3-8e5a070f0381',
                        versionedComponentId: '401e9313-5c31-3bb8-ac48-c34af331b2af',
                        parentGroupId: 'cc4eefda-018d-1000-35ee-194d439257e4',
                        position: {
                            x: 760,
                            y: 272
                        },
                        name: 'ConsumeKafkaRecord_2_6 t1',
                        type: 'org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-kafka-2-6-nar',
                            version: '2.0.0-SNAPSHOT'
                        },
                        state: 'STOPPED',
                        style: {},
                        relationships: [
                            {
                                name: 'parse.failure',
                                description:
                                    'If a message from Kafka cannot be parsed using the configured Record Reader, the contents of the message will be routed to this Relationship as its own individual FlowFile.',
                                autoTerminate: true,
                                retry: false
                            },
                            {
                                name: 'success',
                                description:
                                    'FlowFiles received from Kafka.  Depending on demarcation strategy it is a flow file per message or a bundle of messages grouped by topic and partition.',
                                autoTerminate: true,
                                retry: false
                            }
                        ],
                        supportsParallelProcessing: true,
                        supportsBatching: false,
                        supportsSensitiveDynamicProperties: false,
                        persistsState: false,
                        restricted: false,
                        deprecated: false,
                        executionNodeRestricted: false,
                        multipleVersionsAvailable: false,
                        inputRequirement: 'INPUT_FORBIDDEN',
                        config: {
                            schedulingPeriod: '0 sec',
                            schedulingStrategy: 'TIMER_DRIVEN',
                            executionNode: 'ALL',
                            penaltyDuration: '30 sec',
                            yieldDuration: '1 sec',
                            bulletinLevel: 'WARN',
                            runDurationMillis: 0,
                            concurrentlySchedulableTaskCount: 1,
                            autoTerminatedRelationships: ['parse.failure', 'success'],
                            comments: '',
                            lossTolerant: false,
                            retryCount: 10,
                            retriedRelationships: [],
                            backoffMechanism: 'PENALIZE_FLOWFILE',
                            maxBackoffPeriod: '10 mins'
                        },
                        validationStatus: 'VALID',
                        extensionMissing: false
                    }
                },
                {
                    revision: {
                        version: 0
                    },
                    id: '018e1002-93c7-1b09-913c-8102bce39edb',
                    position: {
                        x: 855,
                        y: 655
                    },
                    permissions: {
                        canRead: true,
                        canWrite: true
                    },
                    component: {
                        id: '018e1002-93c7-1b09-913c-8102bce39edb',
                        versionedComponentId: '1bd9430e-e943-33bd-9eca-40fc4187c17c',
                        parentGroupId: 'cc4eefda-018d-1000-35ee-194d439257e4',
                        position: {
                            x: 855,
                            y: 655
                        },
                        name: 'Budokai',
                        type: 'org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-kafka-2-6-nar',
                            version: '2.0.0-SNAPSHOT'
                        },
                        state: 'STOPPED',
                        style: {},
                        relationships: [
                            {
                                name: 'parse.failure',
                                description:
                                    'If a message from Kafka cannot be parsed using the configured Record Reader, the contents of the message will be routed to this Relationship as its own individual FlowFile.',
                                autoTerminate: true,
                                retry: false
                            },
                            {
                                name: 'success',
                                description:
                                    'FlowFiles received from Kafka.  Depending on demarcation strategy it is a flow file per message or a bundle of messages grouped by topic and partition.',
                                autoTerminate: true,
                                retry: false
                            }
                        ],
                        supportsParallelProcessing: true,
                        supportsBatching: false,
                        supportsSensitiveDynamicProperties: false,
                        persistsState: false,
                        restricted: false,
                        deprecated: false,
                        executionNodeRestricted: false,
                        multipleVersionsAvailable: false,
                        inputRequirement: 'INPUT_FORBIDDEN',
                        config: {
                            schedulingPeriod: '0 sec',
                            schedulingStrategy: 'TIMER_DRIVEN',
                            executionNode: 'ALL',
                            penaltyDuration: '30 sec',
                            yieldDuration: '1 sec',
                            bulletinLevel: 'WARN',
                            runDurationMillis: 0,
                            concurrentlySchedulableTaskCount: 1,
                            autoTerminatedRelationships: ['parse.failure', 'success'],
                            comments: '',
                            lossTolerant: false,
                            retryCount: 10,
                            retriedRelationships: [],
                            backoffMechanism: 'PENALIZE_FLOWFILE',
                            maxBackoffPeriod: '10 mins'
                        },
                        validationStatus: 'VALID',
                        extensionMissing: false
                    }
                },
                {
                    revision: {
                        version: 0
                    },
                    id: '0b0993c7-018e-1000-34f0-eac5126a49d7',
                    position: {
                        x: 480,
                        y: 408
                    },
                    permissions: {
                        canRead: true,
                        canWrite: true
                    },
                    component: {
                        id: '0b0993c7-018e-1000-34f0-eac5126a49d7',
                        versionedComponentId: '2f6b8ff4-218e-3a5c-8abc-8a8146852732',
                        parentGroupId: 'cc4eefda-018d-1000-35ee-194d439257e4',
                        position: {
                            x: 480,
                            y: 408
                        },
                        name: 'ConsumeKafkaRecord_2_6',
                        type: 'org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-kafka-2-6-nar',
                            version: '2.0.0-SNAPSHOT'
                        },
                        state: 'RUNNING',
                        style: {},
                        relationships: [
                            {
                                name: 'parse.failure',
                                description:
                                    'If a message from Kafka cannot be parsed using the configured Record Reader, the contents of the message will be routed to this Relationship as its own individual FlowFile.',
                                autoTerminate: true,
                                retry: false
                            },
                            {
                                name: 'success',
                                description:
                                    'FlowFiles received from Kafka.  Depending on demarcation strategy it is a flow file per message or a bundle of messages grouped by topic and partition.',
                                autoTerminate: true,
                                retry: false
                            }
                        ],
                        supportsParallelProcessing: true,
                        supportsBatching: false,
                        supportsSensitiveDynamicProperties: false,
                        persistsState: false,
                        restricted: false,
                        deprecated: false,
                        executionNodeRestricted: false,
                        multipleVersionsAvailable: false,
                        inputRequirement: 'INPUT_FORBIDDEN',
                        config: {
                            schedulingPeriod: '0 sec',
                            schedulingStrategy: 'TIMER_DRIVEN',
                            executionNode: 'ALL',
                            penaltyDuration: '30 sec',
                            yieldDuration: '1 sec',
                            bulletinLevel: 'WARN',
                            runDurationMillis: 0,
                            concurrentlySchedulableTaskCount: 1,
                            autoTerminatedRelationships: ['parse.failure', 'success'],
                            comments: '',
                            lossTolerant: false,
                            retryCount: 10,
                            retriedRelationships: [],
                            backoffMechanism: 'PENALIZE_FLOWFILE',
                            maxBackoffPeriod: '10 mins'
                        },
                        validationStatus: 'VALID',
                        extensionMissing: false
                    }
                }
            ],
            inputPorts: [],
            outputPorts: [],
            connections: [],
            labels: [],
            funnels: []
        },
        lastRefreshed: '13:19:03 UTC'
    };

    const processGroupEntity = {
        revision: {
            version: 0
        },
        id: 'cc4eefda-018d-1000-35ee-194d439257e4',
        uri: 'http://127.0.0.1:4200/nifi-api/process-groups/cc4eefda-018d-1000-35ee-194d439257e4',
        position: {
            x: 24,
            y: -88
        },
        permissions: {
            canRead: true,
            canWrite: true
        },
        bulletins: [],
        component: {
            id: 'cc4eefda-018d-1000-35ee-194d439257e4',
            versionedComponentId: 'eadc98b0-c3b8-345f-80f4-4e92e1130b8b',
            parentGroupId: '96456bc3-018b-1000-05d8-51720bebae4b',
            position: {
                x: 24,
                y: -88
            },
            name: 'pg1',
            comments: '',
            flowfileConcurrency: 'UNBOUNDED',
            flowfileOutboundPolicy: 'STREAM_WHEN_AVAILABLE',
            defaultFlowFileExpiration: '0 sec',
            defaultBackPressureObjectThreshold: 10000,
            defaultBackPressureDataSizeThreshold: '1 GB',
            executionEngine: 'INHERITED',
            maxConcurrentTasks: 1,
            statelessFlowTimeout: '1 min',
            runningCount: 0,
            stoppedCount: 0,
            invalidCount: 3,
            disabledCount: 0,
            activeRemotePortCount: 0,
            inactiveRemotePortCount: 0,
            upToDateCount: 0,
            locallyModifiedCount: 0,
            staleCount: 0,
            locallyModifiedAndStaleCount: 0,
            syncFailureCount: 0,
            localInputPortCount: 0,
            localOutputPortCount: 0,
            publicInputPortCount: 0,
            publicOutputPortCount: 0,
            statelessGroupScheduledState: 'STOPPED',
            contents: {
                processGroups: [
                    {
                        id: 'cc4f110c-018d-1000-d4b6-d933a69c9d53',
                        versionedComponentId: '11610e12-1853-3179-9e6b-714a77c470e2',
                        parentGroupId: 'cc4eefda-018d-1000-35ee-194d439257e4',
                        position: {
                            x: 450.00000073872457,
                            y: 89.00000213800337
                        },
                        name: 'pg2',
                        comments: '',
                        flowfileConcurrency: 'UNBOUNDED',
                        flowfileOutboundPolicy: 'STREAM_WHEN_AVAILABLE',
                        defaultFlowFileExpiration: '0 sec',
                        defaultBackPressureObjectThreshold: 10000,
                        defaultBackPressureDataSizeThreshold: '1 GB',
                        executionEngine: 'INHERITED',
                        maxConcurrentTasks: 1,
                        statelessFlowTimeout: '1 min',
                        runningCount: 0,
                        stoppedCount: 0,
                        invalidCount: 0,
                        disabledCount: 0,
                        activeRemotePortCount: 0,
                        inactiveRemotePortCount: 0,
                        upToDateCount: 0,
                        locallyModifiedCount: 0,
                        staleCount: 0,
                        locallyModifiedAndStaleCount: 0,
                        syncFailureCount: 0,
                        localInputPortCount: 0,
                        localOutputPortCount: 0,
                        publicInputPortCount: 0,
                        publicOutputPortCount: 0,
                        statelessGroupScheduledState: 'STOPPED',
                        contents: {
                            processGroups: [],
                            remoteProcessGroups: [],
                            processors: [],
                            inputPorts: [],
                            outputPorts: [],
                            connections: [],
                            labels: [],
                            funnels: [],
                            controllerServices: []
                        },
                        inputPortCount: 0,
                        outputPortCount: 0
                    }
                ],
                remoteProcessGroups: [],
                processors: [
                    {
                        id: '018e1000-93c7-1b09-09f3-8e5a070f0381',
                        versionedComponentId: '401e9313-5c31-3bb8-ac48-c34af331b2af',
                        parentGroupId: 'cc4eefda-018d-1000-35ee-194d439257e4',
                        position: {
                            x: 760,
                            y: 272
                        },
                        name: 'ConsumeKafkaRecord_2_6 t1',
                        type: 'org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-kafka-2-6-nar',
                            version: '2.0.0-SNAPSHOT'
                        },
                        state: 'STOPPED',
                        style: {},
                        relationships: [
                            {
                                name: 'parse.failure',
                                description:
                                    'If a message from Kafka cannot be parsed using the configured Record Reader, the contents of the message will be routed to this Relationship as its own individual FlowFile.',
                                autoTerminate: true,
                                retry: false
                            },
                            {
                                name: 'success',
                                description:
                                    'FlowFiles received from Kafka.  Depending on demarcation strategy it is a flow file per message or a bundle of messages grouped by topic and partition.',
                                autoTerminate: true,
                                retry: false
                            }
                        ],
                        supportsParallelProcessing: true,
                        supportsBatching: false,
                        supportsSensitiveDynamicProperties: false,
                        persistsState: false,
                        restricted: false,
                        deprecated: false,
                        executionNodeRestricted: false,
                        multipleVersionsAvailable: false,
                        inputRequirement: 'INPUT_FORBIDDEN',
                        config: {
                            properties: {
                                'bootstrap.servers': 'localhost:9092',
                                topic: '321',
                                topic_type: 'names',
                                'record-reader': '9648c622-018b-1000-c78f-2ef4bc78b696',
                                'record-writer': '9648dc34-018b-1000-9fb4-636b6abe9a10',
                                'group.id': '2',
                                'output-strategy': 'USE_VALUE',
                                'header-name-regex': null,
                                'key-attribute-encoding': 'utf-8',
                                'key-format': 'byte-array',
                                'key-record-reader': null,
                                'Commit Offsets': 'true',
                                'max-uncommit-offset-wait': '1 secs',
                                'honor-transactions': 'true',
                                'security.protocol': 'PLAINTEXT',
                                'sasl.mechanism': 'GSSAPI',
                                'kerberos-user-service': null,
                                'sasl.kerberos.service.name': null,
                                'sasl.username': null,
                                'sasl.password': null,
                                'sasl.token.auth': 'false',
                                'aws.profile.name': null,
                                'ssl.context.service': null,
                                'separate-by-key': 'false',
                                'auto.offset.reset': 'latest',
                                'message-header-encoding': 'UTF-8',
                                'max.poll.records': '10000',
                                'Communications Timeout': '60 secs'
                            },
                            descriptors: {
                                'bootstrap.servers': {
                                    name: 'bootstrap.servers',
                                    displayName: 'Kafka Brokers',
                                    description: 'Comma-separated list of Kafka Brokers in the format host:port',
                                    defaultValue: 'localhost:9092',
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                topic: {
                                    name: 'topic',
                                    displayName: 'Topic Name(s)',
                                    description:
                                        'The name of the Kafka Topic(s) to pull from. More than one can be supplied if comma separated.',
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                topic_type: {
                                    name: 'topic_type',
                                    displayName: 'Topic Name Format',
                                    description:
                                        'Specifies whether the Topic(s) provided are a comma separated list of names or a single regular expression',
                                    defaultValue: 'names',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'names',
                                                value: 'names',
                                                description:
                                                    'Topic is a full topic name or comma separated list of names'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'pattern',
                                                value: 'pattern',
                                                description: 'Topic is a regex using the Java Pattern syntax'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'record-reader': {
                                    name: 'record-reader',
                                    displayName: 'Value Record Reader',
                                    description: 'The Record Reader to use for incoming FlowFiles',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'AvroReader',
                                                value: '9648c622-018b-1000-c78f-2ef4bc78b696'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.serialization.RecordReaderFactory',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'record-writer': {
                                    name: 'record-writer',
                                    displayName: 'Record Value Writer',
                                    description:
                                        'The Record Writer to use in order to serialize the data before sending to Kafka',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'AvroRecordSetWriter',
                                                value: '9648dc34-018b-1000-9fb4-636b6abe9a10'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'AvroRecordSetWriter',
                                                value: '3c57fe11-0190-1000-9c99-9faab34f0d5e'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.serialization.RecordSetWriterFactory',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'group.id': {
                                    name: 'group.id',
                                    displayName: 'Group ID',
                                    description:
                                        "A Group ID is used to identify consumers that are within the same consumer group. Corresponds to Kafka's 'group.id' property.",
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                'output-strategy': {
                                    name: 'output-strategy',
                                    displayName: 'Output Strategy',
                                    description: 'The format used to output the Kafka record into a FlowFile record.',
                                    defaultValue: 'USE_VALUE',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'Use Content as Value',
                                                value: 'USE_VALUE',
                                                description: 'Write only the Kafka Record value to the FlowFile record.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Use Wrapper',
                                                value: 'USE_WRAPPER',
                                                description:
                                                    'Write the Kafka Record key, value, headers, and metadata into the FlowFile record. (See processor usage for more information.)'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'header-name-regex': {
                                    name: 'header-name-regex',
                                    displayName: 'Headers to Add as Attributes (Regex)',
                                    description:
                                        'A Regular Expression that is matched against all message headers. Any message header whose name matches the regex will be added to the FlowFile as an Attribute. If not specified, no Header values will be added as FlowFile attributes. If two messages have a different value for the same header and that header is selected by the provided regex, then those two messages must be added to different FlowFiles. As a result, users should be cautious about using a regex like ".*" if messages are expected to have header values that are unique per message, such as an identifier or timestamp, because it will prevent NiFi from bundling the messages together efficiently.',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'output-strategy',
                                            dependentValues: ['USE_VALUE']
                                        }
                                    ]
                                },
                                'key-attribute-encoding': {
                                    name: 'key-attribute-encoding',
                                    displayName: 'Key Attribute Encoding',
                                    description:
                                        "If the <Separate By Key> property is set to true, FlowFiles that are emitted have an attribute named 'kafka.key'. This property dictates how the value of the attribute should be encoded.",
                                    defaultValue: 'utf-8',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'UTF-8 Encoded',
                                                value: 'utf-8',
                                                description: 'The key is interpreted as a UTF-8 Encoded string.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Hex Encoded',
                                                value: 'hex',
                                                description:
                                                    'The key is interpreted as arbitrary binary data and is encoded using hexadecimal characters with uppercase letters'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Do Not Add Key as Attribute',
                                                value: 'do-not-add',
                                                description: 'The key will not be added as an Attribute'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'output-strategy',
                                            dependentValues: ['USE_VALUE']
                                        }
                                    ]
                                },
                                'key-format': {
                                    name: 'key-format',
                                    displayName: 'Key Format',
                                    description: "Specifies how to represent the Kafka Record's Key in the output",
                                    defaultValue: 'byte-array',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'String',
                                                value: 'string',
                                                description: 'Format the Kafka ConsumerRecord key as a UTF-8 string.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Byte Array',
                                                value: 'byte-array',
                                                description: 'Format the Kafka ConsumerRecord key as a byte array.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Record',
                                                value: 'record',
                                                description: 'Format the Kafka ConsumerRecord key as a record.'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'output-strategy',
                                            dependentValues: ['USE_WRAPPER']
                                        }
                                    ]
                                },
                                'key-record-reader': {
                                    name: 'key-record-reader',
                                    displayName: 'Key Record Reader',
                                    description:
                                        "The Record Reader to use for parsing the Kafka Record's key into a Record",
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'AvroReader',
                                                value: '9648c622-018b-1000-c78f-2ef4bc78b696'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.serialization.RecordReaderFactory',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: [
                                        {
                                            propertyName: 'key-format',
                                            dependentValues: ['record']
                                        }
                                    ]
                                },
                                'Commit Offsets': {
                                    name: 'Commit Offsets',
                                    displayName: 'Commit Offsets',
                                    description:
                                        "Specifies whether or not this Processor should commit the offsets to Kafka after receiving messages. This value should be false when a PublishKafkaRecord processor is expected to commit the offsets using Exactly Once semantics, and should be reserved for dataflows that are designed to run within Stateless NiFi. See Processor's Usage / Additional Details for more information. Note that setting this value to false can lead to significant data duplication or potentially even data loss if the dataflow is not properly configured.",
                                    defaultValue: 'true',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'max-uncommit-offset-wait': {
                                    name: 'max-uncommit-offset-wait',
                                    displayName: 'Max Uncommitted Time',
                                    description:
                                        "Specifies the maximum amount of time allowed to pass before offsets must be committed. This value impacts how often offsets will be committed.  Committing offsets less often increases throughput but also increases the window of potential data duplication in the event of a rebalance or JVM restart between commits.  This value is also related to maximum poll records and the use of a message demarcator.  When using a message demarcator we can have far more uncommitted messages than when we're not as there is much less for us to keep track of in memory.",
                                    defaultValue: '1 secs',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'Commit Offsets',
                                            dependentValues: ['true']
                                        }
                                    ]
                                },
                                'honor-transactions': {
                                    name: 'honor-transactions',
                                    displayName: 'Honor Transactions',
                                    description:
                                        'Specifies whether or not NiFi should honor transactional guarantees when communicating with Kafka. If false, the Processor will use an "isolation level" of read_uncomitted. This means that messages will be received as soon as they are written to Kafka but will be pulled, even if the producer cancels the transactions. If this value is true, NiFi will not receive any messages for which the producer\'s transaction was canceled, but this can result in some latency since the consumer must wait for the producer to finish its entire transaction instead of pulling as the messages become available.',
                                    defaultValue: 'true',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'security.protocol': {
                                    name: 'security.protocol',
                                    displayName: 'Security Protocol',
                                    description:
                                        'Security protocol used to communicate with brokers. Corresponds to Kafka Client security.protocol property',
                                    defaultValue: 'PLAINTEXT',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'PLAINTEXT',
                                                value: 'PLAINTEXT'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SSL',
                                                value: 'SSL'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SASL_PLAINTEXT',
                                                value: 'SASL_PLAINTEXT'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SASL_SSL',
                                                value: 'SASL_SSL'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'sasl.mechanism': {
                                    name: 'sasl.mechanism',
                                    displayName: 'SASL Mechanism',
                                    description:
                                        'SASL mechanism used for authentication. Corresponds to Kafka Client sasl.mechanism property',
                                    defaultValue: 'GSSAPI',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'GSSAPI',
                                                value: 'GSSAPI',
                                                description: 'General Security Services API for Kerberos authentication'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'PLAIN',
                                                value: 'PLAIN',
                                                description: 'Plain username and password authentication'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SCRAM-SHA-256',
                                                value: 'SCRAM-SHA-256',
                                                description:
                                                    'Salted Challenge Response Authentication Mechanism using SHA-512 with username and password'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SCRAM-SHA-512',
                                                value: 'SCRAM-SHA-512',
                                                description:
                                                    'Salted Challenge Response Authentication Mechanism using SHA-256 with username and password'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'kerberos-user-service': {
                                    name: 'kerberos-user-service',
                                    displayName: 'Kerberos User Service',
                                    description: 'Service supporting user authentication with Kerberos',
                                    allowableValues: [],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService:
                                        'org.apache.nifi.kerberos.SelfContainedKerberosUserService',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'sasl.kerberos.service.name': {
                                    name: 'sasl.kerberos.service.name',
                                    displayName: 'Kerberos Service Name',
                                    description:
                                        'The service name that matches the primary name of the Kafka server configured in the broker JAAS configuration',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                'sasl.username': {
                                    name: 'sasl.username',
                                    displayName: 'Username',
                                    description:
                                        'Username provided with configured password when using PLAIN or SCRAM SASL Mechanisms',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['PLAIN', 'SCRAM-SHA-512', 'SCRAM-SHA-256']
                                        }
                                    ]
                                },
                                'sasl.password': {
                                    name: 'sasl.password',
                                    displayName: 'Password',
                                    description:
                                        'Password provided with configured username when using PLAIN or SCRAM SASL Mechanisms',
                                    required: false,
                                    sensitive: true,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['PLAIN', 'SCRAM-SHA-512', 'SCRAM-SHA-256']
                                        }
                                    ]
                                },
                                'sasl.token.auth': {
                                    name: 'sasl.token.auth',
                                    displayName: 'Token Authentication',
                                    description:
                                        'Enables or disables Token authentication when using SCRAM SASL Mechanisms',
                                    defaultValue: 'false',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['SCRAM-SHA-512', 'SCRAM-SHA-256']
                                        }
                                    ]
                                },
                                'aws.profile.name': {
                                    name: 'aws.profile.name',
                                    displayName: 'AWS Profile Name',
                                    description:
                                        'The Amazon Web Services Profile to select when multiple profiles are available.',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope: 'Environment variables and FlowFile Attributes',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['AWS_MSK_IAM']
                                        }
                                    ]
                                },
                                'ssl.context.service': {
                                    name: 'ssl.context.service',
                                    displayName: 'SSL Context Service',
                                    description: 'Service supporting SSL communication with Kafka brokers',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'StandardRestrictedSSLContextService',
                                                value: 'f5bf3c0a-018d-1000-7c8e-910f220b215e'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'aal1',
                                                value: '3c5951d8-0190-1000-0ed6-320905f27a2f'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.ssl.SSLContextService',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'separate-by-key': {
                                    name: 'separate-by-key',
                                    displayName: 'Separate By Key',
                                    description:
                                        'If true, two Records will only be added to the same FlowFile if both of the Kafka Messages have identical keys.',
                                    defaultValue: 'false',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'auto.offset.reset': {
                                    name: 'auto.offset.reset',
                                    displayName: 'Offset Reset',
                                    description:
                                        "Allows you to manage the condition when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted). Corresponds to Kafka's 'auto.offset.reset' property.",
                                    defaultValue: 'latest',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'earliest',
                                                value: 'earliest',
                                                description: 'Automatically reset the offset to the earliest offset'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'latest',
                                                value: 'latest',
                                                description: 'Automatically reset the offset to the latest offset'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'none',
                                                value: 'none',
                                                description:
                                                    "Throw exception to the consumer if no previous offset is found for the consumer's group"
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'message-header-encoding': {
                                    name: 'message-header-encoding',
                                    displayName: 'Message Header Encoding',
                                    description:
                                        'Any message header that is found on a Kafka message will be added to the outbound FlowFile as an attribute. This property indicates the Character Encoding to use for deserializing the headers.',
                                    defaultValue: 'UTF-8',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'max.poll.records': {
                                    name: 'max.poll.records',
                                    displayName: 'Max Poll Records',
                                    description:
                                        'Specifies the maximum number of records Kafka should return in a single poll.',
                                    defaultValue: '10000',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'Communications Timeout': {
                                    name: 'Communications Timeout',
                                    displayName: 'Communications Timeout',
                                    description:
                                        'Specifies the timeout that the consumer should use when communicating with the Kafka Broker',
                                    defaultValue: '60 secs',
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                }
                            },
                            schedulingPeriod: '0 sec',
                            schedulingStrategy: 'TIMER_DRIVEN',
                            executionNode: 'ALL',
                            penaltyDuration: '30 sec',
                            yieldDuration: '1 sec',
                            bulletinLevel: 'WARN',
                            runDurationMillis: 0,
                            concurrentlySchedulableTaskCount: 1,
                            autoTerminatedRelationships: ['parse.failure', 'success'],
                            comments: '',
                            lossTolerant: false,
                            defaultConcurrentTasks: {
                                TIMER_DRIVEN: '1',
                                CRON_DRIVEN: '1'
                            },
                            defaultSchedulingPeriod: {
                                TIMER_DRIVEN: '0 sec',
                                CRON_DRIVEN: '* * * * * ?'
                            },
                            retryCount: 10,
                            retriedRelationships: [],
                            backoffMechanism: 'PENALIZE_FLOWFILE',
                            maxBackoffPeriod: '10 mins'
                        },
                        validationErrors: [
                            "'Value Record Reader' validated against '9648c622-018b-1000-c78f-2ef4bc78b696' is invalid because Controller Service with ID 9648c622-018b-1000-c78f-2ef4bc78b696 is disabled"
                        ],
                        validationStatus: 'INVALID',
                        extensionMissing: false
                    },
                    {
                        id: '018e1002-93c7-1b09-913c-8102bce39edb',
                        versionedComponentId: '1bd9430e-e943-33bd-9eca-40fc4187c17c',
                        parentGroupId: 'cc4eefda-018d-1000-35ee-194d439257e4',
                        position: {
                            x: 855,
                            y: 655
                        },
                        name: 'Budokai',
                        type: 'org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-kafka-2-6-nar',
                            version: '2.0.0-SNAPSHOT'
                        },
                        state: 'STOPPED',
                        style: {},
                        relationships: [
                            {
                                name: 'parse.failure',
                                description:
                                    'If a message from Kafka cannot be parsed using the configured Record Reader, the contents of the message will be routed to this Relationship as its own individual FlowFile.',
                                autoTerminate: true,
                                retry: false
                            },
                            {
                                name: 'success',
                                description:
                                    'FlowFiles received from Kafka.  Depending on demarcation strategy it is a flow file per message or a bundle of messages grouped by topic and partition.',
                                autoTerminate: true,
                                retry: false
                            }
                        ],
                        supportsParallelProcessing: true,
                        supportsBatching: false,
                        supportsSensitiveDynamicProperties: false,
                        persistsState: false,
                        restricted: false,
                        deprecated: false,
                        executionNodeRestricted: false,
                        multipleVersionsAvailable: false,
                        inputRequirement: 'INPUT_FORBIDDEN',
                        config: {
                            properties: {
                                'bootstrap.servers': 'localhost:9092',
                                topic: '321',
                                topic_type: 'names',
                                'record-reader': '9648c622-018b-1000-c78f-2ef4bc78b696',
                                'record-writer': '9648dc34-018b-1000-9fb4-636b6abe9a10',
                                'group.id': '2',
                                'output-strategy': 'USE_VALUE',
                                'header-name-regex': null,
                                'key-attribute-encoding': 'utf-8',
                                'key-format': 'byte-array',
                                'key-record-reader': null,
                                'Commit Offsets': 'true',
                                'max-uncommit-offset-wait': '1 secs',
                                'honor-transactions': 'true',
                                'security.protocol': 'PLAINTEXT',
                                'sasl.mechanism': 'GSSAPI',
                                'kerberos-user-service': null,
                                'sasl.kerberos.service.name': null,
                                'sasl.username': null,
                                'sasl.password': null,
                                'sasl.token.auth': 'false',
                                'aws.profile.name': null,
                                'ssl.context.service': null,
                                'separate-by-key': 'false',
                                'auto.offset.reset': 'latest',
                                'message-header-encoding': 'UTF-8',
                                'max.poll.records': '10000',
                                'Communications Timeout': '60 secs'
                            },
                            descriptors: {
                                'bootstrap.servers': {
                                    name: 'bootstrap.servers',
                                    displayName: 'Kafka Brokers',
                                    description: 'Comma-separated list of Kafka Brokers in the format host:port',
                                    defaultValue: 'localhost:9092',
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                topic: {
                                    name: 'topic',
                                    displayName: 'Topic Name(s)',
                                    description:
                                        'The name of the Kafka Topic(s) to pull from. More than one can be supplied if comma separated.',
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                topic_type: {
                                    name: 'topic_type',
                                    displayName: 'Topic Name Format',
                                    description:
                                        'Specifies whether the Topic(s) provided are a comma separated list of names or a single regular expression',
                                    defaultValue: 'names',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'names',
                                                value: 'names',
                                                description:
                                                    'Topic is a full topic name or comma separated list of names'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'pattern',
                                                value: 'pattern',
                                                description: 'Topic is a regex using the Java Pattern syntax'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'record-reader': {
                                    name: 'record-reader',
                                    displayName: 'Value Record Reader',
                                    description: 'The Record Reader to use for incoming FlowFiles',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'AvroReader',
                                                value: '9648c622-018b-1000-c78f-2ef4bc78b696'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.serialization.RecordReaderFactory',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'record-writer': {
                                    name: 'record-writer',
                                    displayName: 'Record Value Writer',
                                    description:
                                        'The Record Writer to use in order to serialize the data before sending to Kafka',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'AvroRecordSetWriter',
                                                value: '9648dc34-018b-1000-9fb4-636b6abe9a10'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'AvroRecordSetWriter',
                                                value: '3c57fe11-0190-1000-9c99-9faab34f0d5e'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.serialization.RecordSetWriterFactory',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'group.id': {
                                    name: 'group.id',
                                    displayName: 'Group ID',
                                    description:
                                        "A Group ID is used to identify consumers that are within the same consumer group. Corresponds to Kafka's 'group.id' property.",
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                'output-strategy': {
                                    name: 'output-strategy',
                                    displayName: 'Output Strategy',
                                    description: 'The format used to output the Kafka record into a FlowFile record.',
                                    defaultValue: 'USE_VALUE',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'Use Content as Value',
                                                value: 'USE_VALUE',
                                                description: 'Write only the Kafka Record value to the FlowFile record.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Use Wrapper',
                                                value: 'USE_WRAPPER',
                                                description:
                                                    'Write the Kafka Record key, value, headers, and metadata into the FlowFile record. (See processor usage for more information.)'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'header-name-regex': {
                                    name: 'header-name-regex',
                                    displayName: 'Headers to Add as Attributes (Regex)',
                                    description:
                                        'A Regular Expression that is matched against all message headers. Any message header whose name matches the regex will be added to the FlowFile as an Attribute. If not specified, no Header values will be added as FlowFile attributes. If two messages have a different value for the same header and that header is selected by the provided regex, then those two messages must be added to different FlowFiles. As a result, users should be cautious about using a regex like ".*" if messages are expected to have header values that are unique per message, such as an identifier or timestamp, because it will prevent NiFi from bundling the messages together efficiently.',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'output-strategy',
                                            dependentValues: ['USE_VALUE']
                                        }
                                    ]
                                },
                                'key-attribute-encoding': {
                                    name: 'key-attribute-encoding',
                                    displayName: 'Key Attribute Encoding',
                                    description:
                                        "If the <Separate By Key> property is set to true, FlowFiles that are emitted have an attribute named 'kafka.key'. This property dictates how the value of the attribute should be encoded.",
                                    defaultValue: 'utf-8',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'UTF-8 Encoded',
                                                value: 'utf-8',
                                                description: 'The key is interpreted as a UTF-8 Encoded string.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Hex Encoded',
                                                value: 'hex',
                                                description:
                                                    'The key is interpreted as arbitrary binary data and is encoded using hexadecimal characters with uppercase letters'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Do Not Add Key as Attribute',
                                                value: 'do-not-add',
                                                description: 'The key will not be added as an Attribute'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'output-strategy',
                                            dependentValues: ['USE_VALUE']
                                        }
                                    ]
                                },
                                'key-format': {
                                    name: 'key-format',
                                    displayName: 'Key Format',
                                    description: "Specifies how to represent the Kafka Record's Key in the output",
                                    defaultValue: 'byte-array',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'String',
                                                value: 'string',
                                                description: 'Format the Kafka ConsumerRecord key as a UTF-8 string.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Byte Array',
                                                value: 'byte-array',
                                                description: 'Format the Kafka ConsumerRecord key as a byte array.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Record',
                                                value: 'record',
                                                description: 'Format the Kafka ConsumerRecord key as a record.'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'output-strategy',
                                            dependentValues: ['USE_WRAPPER']
                                        }
                                    ]
                                },
                                'key-record-reader': {
                                    name: 'key-record-reader',
                                    displayName: 'Key Record Reader',
                                    description:
                                        "The Record Reader to use for parsing the Kafka Record's key into a Record",
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'AvroReader',
                                                value: '9648c622-018b-1000-c78f-2ef4bc78b696'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.serialization.RecordReaderFactory',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: [
                                        {
                                            propertyName: 'key-format',
                                            dependentValues: ['record']
                                        }
                                    ]
                                },
                                'Commit Offsets': {
                                    name: 'Commit Offsets',
                                    displayName: 'Commit Offsets',
                                    description:
                                        "Specifies whether or not this Processor should commit the offsets to Kafka after receiving messages. This value should be false when a PublishKafkaRecord processor is expected to commit the offsets using Exactly Once semantics, and should be reserved for dataflows that are designed to run within Stateless NiFi. See Processor's Usage / Additional Details for more information. Note that setting this value to false can lead to significant data duplication or potentially even data loss if the dataflow is not properly configured.",
                                    defaultValue: 'true',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'max-uncommit-offset-wait': {
                                    name: 'max-uncommit-offset-wait',
                                    displayName: 'Max Uncommitted Time',
                                    description:
                                        "Specifies the maximum amount of time allowed to pass before offsets must be committed. This value impacts how often offsets will be committed.  Committing offsets less often increases throughput but also increases the window of potential data duplication in the event of a rebalance or JVM restart between commits.  This value is also related to maximum poll records and the use of a message demarcator.  When using a message demarcator we can have far more uncommitted messages than when we're not as there is much less for us to keep track of in memory.",
                                    defaultValue: '1 secs',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'Commit Offsets',
                                            dependentValues: ['true']
                                        }
                                    ]
                                },
                                'honor-transactions': {
                                    name: 'honor-transactions',
                                    displayName: 'Honor Transactions',
                                    description:
                                        'Specifies whether or not NiFi should honor transactional guarantees when communicating with Kafka. If false, the Processor will use an "isolation level" of read_uncomitted. This means that messages will be received as soon as they are written to Kafka but will be pulled, even if the producer cancels the transactions. If this value is true, NiFi will not receive any messages for which the producer\'s transaction was canceled, but this can result in some latency since the consumer must wait for the producer to finish its entire transaction instead of pulling as the messages become available.',
                                    defaultValue: 'true',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'security.protocol': {
                                    name: 'security.protocol',
                                    displayName: 'Security Protocol',
                                    description:
                                        'Security protocol used to communicate with brokers. Corresponds to Kafka Client security.protocol property',
                                    defaultValue: 'PLAINTEXT',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'PLAINTEXT',
                                                value: 'PLAINTEXT'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SSL',
                                                value: 'SSL'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SASL_PLAINTEXT',
                                                value: 'SASL_PLAINTEXT'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SASL_SSL',
                                                value: 'SASL_SSL'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'sasl.mechanism': {
                                    name: 'sasl.mechanism',
                                    displayName: 'SASL Mechanism',
                                    description:
                                        'SASL mechanism used for authentication. Corresponds to Kafka Client sasl.mechanism property',
                                    defaultValue: 'GSSAPI',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'GSSAPI',
                                                value: 'GSSAPI',
                                                description: 'General Security Services API for Kerberos authentication'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'PLAIN',
                                                value: 'PLAIN',
                                                description: 'Plain username and password authentication'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SCRAM-SHA-256',
                                                value: 'SCRAM-SHA-256',
                                                description:
                                                    'Salted Challenge Response Authentication Mechanism using SHA-512 with username and password'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SCRAM-SHA-512',
                                                value: 'SCRAM-SHA-512',
                                                description:
                                                    'Salted Challenge Response Authentication Mechanism using SHA-256 with username and password'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'kerberos-user-service': {
                                    name: 'kerberos-user-service',
                                    displayName: 'Kerberos User Service',
                                    description: 'Service supporting user authentication with Kerberos',
                                    allowableValues: [],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService:
                                        'org.apache.nifi.kerberos.SelfContainedKerberosUserService',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'sasl.kerberos.service.name': {
                                    name: 'sasl.kerberos.service.name',
                                    displayName: 'Kerberos Service Name',
                                    description:
                                        'The service name that matches the primary name of the Kafka server configured in the broker JAAS configuration',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                'sasl.username': {
                                    name: 'sasl.username',
                                    displayName: 'Username',
                                    description:
                                        'Username provided with configured password when using PLAIN or SCRAM SASL Mechanisms',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['PLAIN', 'SCRAM-SHA-512', 'SCRAM-SHA-256']
                                        }
                                    ]
                                },
                                'sasl.password': {
                                    name: 'sasl.password',
                                    displayName: 'Password',
                                    description:
                                        'Password provided with configured username when using PLAIN or SCRAM SASL Mechanisms',
                                    required: false,
                                    sensitive: true,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['PLAIN', 'SCRAM-SHA-512', 'SCRAM-SHA-256']
                                        }
                                    ]
                                },
                                'sasl.token.auth': {
                                    name: 'sasl.token.auth',
                                    displayName: 'Token Authentication',
                                    description:
                                        'Enables or disables Token authentication when using SCRAM SASL Mechanisms',
                                    defaultValue: 'false',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['SCRAM-SHA-512', 'SCRAM-SHA-256']
                                        }
                                    ]
                                },
                                'aws.profile.name': {
                                    name: 'aws.profile.name',
                                    displayName: 'AWS Profile Name',
                                    description:
                                        'The Amazon Web Services Profile to select when multiple profiles are available.',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope: 'Environment variables and FlowFile Attributes',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['AWS_MSK_IAM']
                                        }
                                    ]
                                },
                                'ssl.context.service': {
                                    name: 'ssl.context.service',
                                    displayName: 'SSL Context Service',
                                    description: 'Service supporting SSL communication with Kafka brokers',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'StandardRestrictedSSLContextService',
                                                value: 'f5bf3c0a-018d-1000-7c8e-910f220b215e'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'aal1',
                                                value: '3c5951d8-0190-1000-0ed6-320905f27a2f'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.ssl.SSLContextService',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'separate-by-key': {
                                    name: 'separate-by-key',
                                    displayName: 'Separate By Key',
                                    description:
                                        'If true, two Records will only be added to the same FlowFile if both of the Kafka Messages have identical keys.',
                                    defaultValue: 'false',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'auto.offset.reset': {
                                    name: 'auto.offset.reset',
                                    displayName: 'Offset Reset',
                                    description:
                                        "Allows you to manage the condition when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted). Corresponds to Kafka's 'auto.offset.reset' property.",
                                    defaultValue: 'latest',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'earliest',
                                                value: 'earliest',
                                                description: 'Automatically reset the offset to the earliest offset'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'latest',
                                                value: 'latest',
                                                description: 'Automatically reset the offset to the latest offset'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'none',
                                                value: 'none',
                                                description:
                                                    "Throw exception to the consumer if no previous offset is found for the consumer's group"
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'message-header-encoding': {
                                    name: 'message-header-encoding',
                                    displayName: 'Message Header Encoding',
                                    description:
                                        'Any message header that is found on a Kafka message will be added to the outbound FlowFile as an attribute. This property indicates the Character Encoding to use for deserializing the headers.',
                                    defaultValue: 'UTF-8',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'max.poll.records': {
                                    name: 'max.poll.records',
                                    displayName: 'Max Poll Records',
                                    description:
                                        'Specifies the maximum number of records Kafka should return in a single poll.',
                                    defaultValue: '10000',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'Communications Timeout': {
                                    name: 'Communications Timeout',
                                    displayName: 'Communications Timeout',
                                    description:
                                        'Specifies the timeout that the consumer should use when communicating with the Kafka Broker',
                                    defaultValue: '60 secs',
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                }
                            },
                            schedulingPeriod: '0 sec',
                            schedulingStrategy: 'TIMER_DRIVEN',
                            executionNode: 'ALL',
                            penaltyDuration: '30 sec',
                            yieldDuration: '1 sec',
                            bulletinLevel: 'WARN',
                            runDurationMillis: 0,
                            concurrentlySchedulableTaskCount: 1,
                            autoTerminatedRelationships: ['parse.failure', 'success'],
                            comments: '',
                            lossTolerant: false,
                            defaultConcurrentTasks: {
                                TIMER_DRIVEN: '1',
                                CRON_DRIVEN: '1'
                            },
                            defaultSchedulingPeriod: {
                                TIMER_DRIVEN: '0 sec',
                                CRON_DRIVEN: '* * * * * ?'
                            },
                            retryCount: 10,
                            retriedRelationships: [],
                            backoffMechanism: 'PENALIZE_FLOWFILE',
                            maxBackoffPeriod: '10 mins'
                        },
                        validationErrors: [
                            "'Value Record Reader' validated against '9648c622-018b-1000-c78f-2ef4bc78b696' is invalid because Controller Service with ID 9648c622-018b-1000-c78f-2ef4bc78b696 is disabled"
                        ],
                        validationStatus: 'INVALID',
                        extensionMissing: false
                    },
                    {
                        id: '0b0993c7-018e-1000-34f0-eac5126a49d7',
                        versionedComponentId: '2f6b8ff4-218e-3a5c-8abc-8a8146852732',
                        parentGroupId: 'cc4eefda-018d-1000-35ee-194d439257e4',
                        position: {
                            x: 480,
                            y: 408
                        },
                        name: 'ConsumeKafkaRecord_2_6',
                        type: 'org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-kafka-2-6-nar',
                            version: '2.0.0-SNAPSHOT'
                        },
                        state: 'STOPPED',
                        style: {},
                        relationships: [
                            {
                                name: 'parse.failure',
                                description:
                                    'If a message from Kafka cannot be parsed using the configured Record Reader, the contents of the message will be routed to this Relationship as its own individual FlowFile.',
                                autoTerminate: true,
                                retry: false
                            },
                            {
                                name: 'success',
                                description:
                                    'FlowFiles received from Kafka.  Depending on demarcation strategy it is a flow file per message or a bundle of messages grouped by topic and partition.',
                                autoTerminate: true,
                                retry: false
                            }
                        ],
                        supportsParallelProcessing: true,
                        supportsBatching: false,
                        supportsSensitiveDynamicProperties: false,
                        persistsState: false,
                        restricted: false,
                        deprecated: false,
                        executionNodeRestricted: false,
                        multipleVersionsAvailable: false,
                        inputRequirement: 'INPUT_FORBIDDEN',
                        config: {
                            properties: {
                                'bootstrap.servers': 'localhost:9092',
                                topic: '321',
                                topic_type: 'names',
                                'record-reader': '9648c622-018b-1000-c78f-2ef4bc78b696',
                                'record-writer': '9648dc34-018b-1000-9fb4-636b6abe9a10',
                                'group.id': '2',
                                'output-strategy': 'USE_VALUE',
                                'header-name-regex': null,
                                'key-attribute-encoding': 'utf-8',
                                'key-format': 'byte-array',
                                'key-record-reader': null,
                                'Commit Offsets': 'true',
                                'max-uncommit-offset-wait': '1 secs',
                                'honor-transactions': 'true',
                                'security.protocol': 'PLAINTEXT',
                                'sasl.mechanism': 'GSSAPI',
                                'kerberos-user-service': null,
                                'sasl.kerberos.service.name': null,
                                'sasl.username': null,
                                'sasl.password': null,
                                'sasl.token.auth': 'false',
                                'aws.profile.name': null,
                                'ssl.context.service': null,
                                'separate-by-key': 'false',
                                'auto.offset.reset': 'latest',
                                'message-header-encoding': 'UTF-8',
                                'max.poll.records': '10000',
                                'Communications Timeout': '60 secs'
                            },
                            descriptors: {
                                'bootstrap.servers': {
                                    name: 'bootstrap.servers',
                                    displayName: 'Kafka Brokers',
                                    description: 'Comma-separated list of Kafka Brokers in the format host:port',
                                    defaultValue: 'localhost:9092',
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                topic: {
                                    name: 'topic',
                                    displayName: 'Topic Name(s)',
                                    description:
                                        'The name of the Kafka Topic(s) to pull from. More than one can be supplied if comma separated.',
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                topic_type: {
                                    name: 'topic_type',
                                    displayName: 'Topic Name Format',
                                    description:
                                        'Specifies whether the Topic(s) provided are a comma separated list of names or a single regular expression',
                                    defaultValue: 'names',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'names',
                                                value: 'names',
                                                description:
                                                    'Topic is a full topic name or comma separated list of names'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'pattern',
                                                value: 'pattern',
                                                description: 'Topic is a regex using the Java Pattern syntax'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'record-reader': {
                                    name: 'record-reader',
                                    displayName: 'Value Record Reader',
                                    description: 'The Record Reader to use for incoming FlowFiles',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'AvroReader',
                                                value: '9648c622-018b-1000-c78f-2ef4bc78b696'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.serialization.RecordReaderFactory',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'record-writer': {
                                    name: 'record-writer',
                                    displayName: 'Record Value Writer',
                                    description:
                                        'The Record Writer to use in order to serialize the data before sending to Kafka',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'AvroRecordSetWriter',
                                                value: '9648dc34-018b-1000-9fb4-636b6abe9a10'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'AvroRecordSetWriter',
                                                value: '3c57fe11-0190-1000-9c99-9faab34f0d5e'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.serialization.RecordSetWriterFactory',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'group.id': {
                                    name: 'group.id',
                                    displayName: 'Group ID',
                                    description:
                                        "A Group ID is used to identify consumers that are within the same consumer group. Corresponds to Kafka's 'group.id' property.",
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                'output-strategy': {
                                    name: 'output-strategy',
                                    displayName: 'Output Strategy',
                                    description: 'The format used to output the Kafka record into a FlowFile record.',
                                    defaultValue: 'USE_VALUE',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'Use Content as Value',
                                                value: 'USE_VALUE',
                                                description: 'Write only the Kafka Record value to the FlowFile record.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Use Wrapper',
                                                value: 'USE_WRAPPER',
                                                description:
                                                    'Write the Kafka Record key, value, headers, and metadata into the FlowFile record. (See processor usage for more information.)'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'header-name-regex': {
                                    name: 'header-name-regex',
                                    displayName: 'Headers to Add as Attributes (Regex)',
                                    description:
                                        'A Regular Expression that is matched against all message headers. Any message header whose name matches the regex will be added to the FlowFile as an Attribute. If not specified, no Header values will be added as FlowFile attributes. If two messages have a different value for the same header and that header is selected by the provided regex, then those two messages must be added to different FlowFiles. As a result, users should be cautious about using a regex like ".*" if messages are expected to have header values that are unique per message, such as an identifier or timestamp, because it will prevent NiFi from bundling the messages together efficiently.',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'output-strategy',
                                            dependentValues: ['USE_VALUE']
                                        }
                                    ]
                                },
                                'key-attribute-encoding': {
                                    name: 'key-attribute-encoding',
                                    displayName: 'Key Attribute Encoding',
                                    description:
                                        "If the <Separate By Key> property is set to true, FlowFiles that are emitted have an attribute named 'kafka.key'. This property dictates how the value of the attribute should be encoded.",
                                    defaultValue: 'utf-8',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'UTF-8 Encoded',
                                                value: 'utf-8',
                                                description: 'The key is interpreted as a UTF-8 Encoded string.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Hex Encoded',
                                                value: 'hex',
                                                description:
                                                    'The key is interpreted as arbitrary binary data and is encoded using hexadecimal characters with uppercase letters'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Do Not Add Key as Attribute',
                                                value: 'do-not-add',
                                                description: 'The key will not be added as an Attribute'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'output-strategy',
                                            dependentValues: ['USE_VALUE']
                                        }
                                    ]
                                },
                                'key-format': {
                                    name: 'key-format',
                                    displayName: 'Key Format',
                                    description: "Specifies how to represent the Kafka Record's Key in the output",
                                    defaultValue: 'byte-array',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'String',
                                                value: 'string',
                                                description: 'Format the Kafka ConsumerRecord key as a UTF-8 string.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Byte Array',
                                                value: 'byte-array',
                                                description: 'Format the Kafka ConsumerRecord key as a byte array.'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'Record',
                                                value: 'record',
                                                description: 'Format the Kafka ConsumerRecord key as a record.'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'output-strategy',
                                            dependentValues: ['USE_WRAPPER']
                                        }
                                    ]
                                },
                                'key-record-reader': {
                                    name: 'key-record-reader',
                                    displayName: 'Key Record Reader',
                                    description:
                                        "The Record Reader to use for parsing the Kafka Record's key into a Record",
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'AvroReader',
                                                value: '9648c622-018b-1000-c78f-2ef4bc78b696'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.serialization.RecordReaderFactory',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: [
                                        {
                                            propertyName: 'key-format',
                                            dependentValues: ['record']
                                        }
                                    ]
                                },
                                'Commit Offsets': {
                                    name: 'Commit Offsets',
                                    displayName: 'Commit Offsets',
                                    description:
                                        "Specifies whether or not this Processor should commit the offsets to Kafka after receiving messages. This value should be false when a PublishKafkaRecord processor is expected to commit the offsets using Exactly Once semantics, and should be reserved for dataflows that are designed to run within Stateless NiFi. See Processor's Usage / Additional Details for more information. Note that setting this value to false can lead to significant data duplication or potentially even data loss if the dataflow is not properly configured.",
                                    defaultValue: 'true',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'max-uncommit-offset-wait': {
                                    name: 'max-uncommit-offset-wait',
                                    displayName: 'Max Uncommitted Time',
                                    description:
                                        "Specifies the maximum amount of time allowed to pass before offsets must be committed. This value impacts how often offsets will be committed.  Committing offsets less often increases throughput but also increases the window of potential data duplication in the event of a rebalance or JVM restart between commits.  This value is also related to maximum poll records and the use of a message demarcator.  When using a message demarcator we can have far more uncommitted messages than when we're not as there is much less for us to keep track of in memory.",
                                    defaultValue: '1 secs',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'Commit Offsets',
                                            dependentValues: ['true']
                                        }
                                    ]
                                },
                                'honor-transactions': {
                                    name: 'honor-transactions',
                                    displayName: 'Honor Transactions',
                                    description:
                                        'Specifies whether or not NiFi should honor transactional guarantees when communicating with Kafka. If false, the Processor will use an "isolation level" of read_uncomitted. This means that messages will be received as soon as they are written to Kafka but will be pulled, even if the producer cancels the transactions. If this value is true, NiFi will not receive any messages for which the producer\'s transaction was canceled, but this can result in some latency since the consumer must wait for the producer to finish its entire transaction instead of pulling as the messages become available.',
                                    defaultValue: 'true',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'security.protocol': {
                                    name: 'security.protocol',
                                    displayName: 'Security Protocol',
                                    description:
                                        'Security protocol used to communicate with brokers. Corresponds to Kafka Client security.protocol property',
                                    defaultValue: 'PLAINTEXT',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'PLAINTEXT',
                                                value: 'PLAINTEXT'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SSL',
                                                value: 'SSL'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SASL_PLAINTEXT',
                                                value: 'SASL_PLAINTEXT'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SASL_SSL',
                                                value: 'SASL_SSL'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'sasl.mechanism': {
                                    name: 'sasl.mechanism',
                                    displayName: 'SASL Mechanism',
                                    description:
                                        'SASL mechanism used for authentication. Corresponds to Kafka Client sasl.mechanism property',
                                    defaultValue: 'GSSAPI',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'GSSAPI',
                                                value: 'GSSAPI',
                                                description: 'General Security Services API for Kerberos authentication'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'PLAIN',
                                                value: 'PLAIN',
                                                description: 'Plain username and password authentication'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SCRAM-SHA-256',
                                                value: 'SCRAM-SHA-256',
                                                description:
                                                    'Salted Challenge Response Authentication Mechanism using SHA-512 with username and password'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'SCRAM-SHA-512',
                                                value: 'SCRAM-SHA-512',
                                                description:
                                                    'Salted Challenge Response Authentication Mechanism using SHA-256 with username and password'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'kerberos-user-service': {
                                    name: 'kerberos-user-service',
                                    displayName: 'Kerberos User Service',
                                    description: 'Service supporting user authentication with Kerberos',
                                    allowableValues: [],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService:
                                        'org.apache.nifi.kerberos.SelfContainedKerberosUserService',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'sasl.kerberos.service.name': {
                                    name: 'sasl.kerberos.service.name',
                                    displayName: 'Kerberos Service Name',
                                    description:
                                        'The service name that matches the primary name of the Kafka server configured in the broker JAAS configuration',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: []
                                },
                                'sasl.username': {
                                    name: 'sasl.username',
                                    displayName: 'Username',
                                    description:
                                        'Username provided with configured password when using PLAIN or SCRAM SASL Mechanisms',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['PLAIN', 'SCRAM-SHA-512', 'SCRAM-SHA-256']
                                        }
                                    ]
                                },
                                'sasl.password': {
                                    name: 'sasl.password',
                                    displayName: 'Password',
                                    description:
                                        'Password provided with configured username when using PLAIN or SCRAM SASL Mechanisms',
                                    required: false,
                                    sensitive: true,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope:
                                        'Environment variables defined at JVM level and system properties',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['PLAIN', 'SCRAM-SHA-512', 'SCRAM-SHA-256']
                                        }
                                    ]
                                },
                                'sasl.token.auth': {
                                    name: 'sasl.token.auth',
                                    displayName: 'Token Authentication',
                                    description:
                                        'Enables or disables Token authentication when using SCRAM SASL Mechanisms',
                                    defaultValue: 'false',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['SCRAM-SHA-512', 'SCRAM-SHA-256']
                                        }
                                    ]
                                },
                                'aws.profile.name': {
                                    name: 'aws.profile.name',
                                    displayName: 'AWS Profile Name',
                                    description:
                                        'The Amazon Web Services Profile to select when multiple profiles are available.',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: true,
                                    expressionLanguageScope: 'Environment variables and FlowFile Attributes',
                                    dependencies: [
                                        {
                                            propertyName: 'sasl.mechanism',
                                            dependentValues: ['AWS_MSK_IAM']
                                        }
                                    ]
                                },
                                'ssl.context.service': {
                                    name: 'ssl.context.service',
                                    displayName: 'SSL Context Service',
                                    description: 'Service supporting SSL communication with Kafka brokers',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'StandardRestrictedSSLContextService',
                                                value: 'f5bf3c0a-018d-1000-7c8e-910f220b215e'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'aal1',
                                                value: '3c5951d8-0190-1000-0ed6-320905f27a2f'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    identifiesControllerService: 'org.apache.nifi.ssl.SSLContextService',
                                    identifiesControllerServiceBundle: {
                                        group: 'org.apache.nifi',
                                        artifact: 'nifi-standard-services-api-nar',
                                        version: '2.0.0-SNAPSHOT'
                                    },
                                    dependencies: []
                                },
                                'separate-by-key': {
                                    name: 'separate-by-key',
                                    displayName: 'Separate By Key',
                                    description:
                                        'If true, two Records will only be added to the same FlowFile if both of the Kafka Messages have identical keys.',
                                    defaultValue: 'false',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'true',
                                                value: 'true'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'false',
                                                value: 'false'
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'auto.offset.reset': {
                                    name: 'auto.offset.reset',
                                    displayName: 'Offset Reset',
                                    description:
                                        "Allows you to manage the condition when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted). Corresponds to Kafka's 'auto.offset.reset' property.",
                                    defaultValue: 'latest',
                                    allowableValues: [
                                        {
                                            allowableValue: {
                                                displayName: 'earliest',
                                                value: 'earliest',
                                                description: 'Automatically reset the offset to the earliest offset'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'latest',
                                                value: 'latest',
                                                description: 'Automatically reset the offset to the latest offset'
                                            },
                                            canRead: true
                                        },
                                        {
                                            allowableValue: {
                                                displayName: 'none',
                                                value: 'none',
                                                description:
                                                    "Throw exception to the consumer if no previous offset is found for the consumer's group"
                                            },
                                            canRead: true
                                        }
                                    ],
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'message-header-encoding': {
                                    name: 'message-header-encoding',
                                    displayName: 'Message Header Encoding',
                                    description:
                                        'Any message header that is found on a Kafka message will be added to the outbound FlowFile as an attribute. This property indicates the Character Encoding to use for deserializing the headers.',
                                    defaultValue: 'UTF-8',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'max.poll.records': {
                                    name: 'max.poll.records',
                                    displayName: 'Max Poll Records',
                                    description:
                                        'Specifies the maximum number of records Kafka should return in a single poll.',
                                    defaultValue: '10000',
                                    required: false,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                },
                                'Communications Timeout': {
                                    name: 'Communications Timeout',
                                    displayName: 'Communications Timeout',
                                    description:
                                        'Specifies the timeout that the consumer should use when communicating with the Kafka Broker',
                                    defaultValue: '60 secs',
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                }
                            },
                            schedulingPeriod: '0 sec',
                            schedulingStrategy: 'TIMER_DRIVEN',
                            executionNode: 'ALL',
                            penaltyDuration: '30 sec',
                            yieldDuration: '1 sec',
                            bulletinLevel: 'WARN',
                            runDurationMillis: 0,
                            concurrentlySchedulableTaskCount: 1,
                            autoTerminatedRelationships: ['parse.failure', 'success'],
                            comments: '',
                            lossTolerant: false,
                            defaultConcurrentTasks: {
                                TIMER_DRIVEN: '1',
                                CRON_DRIVEN: '1'
                            },
                            defaultSchedulingPeriod: {
                                TIMER_DRIVEN: '0 sec',
                                CRON_DRIVEN: '* * * * * ?'
                            },
                            retryCount: 10,
                            retriedRelationships: [],
                            backoffMechanism: 'PENALIZE_FLOWFILE',
                            maxBackoffPeriod: '10 mins'
                        },
                        validationErrors: [
                            "'Value Record Reader' validated against '9648c622-018b-1000-c78f-2ef4bc78b696' is invalid because Controller Service with ID 9648c622-018b-1000-c78f-2ef4bc78b696 is disabled"
                        ],
                        validationStatus: 'INVALID',
                        extensionMissing: false
                    }
                ],
                inputPorts: [],
                outputPorts: [],
                connections: [],
                labels: [],
                funnels: [
                    {
                        id: '372db66f-0190-1000-85e6-3d55ebc72e3c',
                        versionedComponentId: 'a29524a7-2797-31d0-8362-512efaf0cebe',
                        parentGroupId: 'cc4eefda-018d-1000-35ee-194d439257e4',
                        position: {
                            x: 341.1209991259036,
                            y: 215.00003332754773
                        }
                    }
                ],
                controllerServices: []
            },
            inputPortCount: 0,
            outputPortCount: 0
        },
        status: {
            id: 'cc4eefda-018d-1000-35ee-194d439257e4',
            name: 'pg1',
            statsLastRefreshed: '14:58:25 UTC',
            aggregateSnapshot: {
                id: 'cc4eefda-018d-1000-35ee-194d439257e4',
                name: 'pg1',
                statelessActiveThreadCount: 0,
                flowFilesIn: 0,
                bytesIn: 0,
                input: '0 (0 bytes)',
                flowFilesQueued: 0,
                bytesQueued: 0,
                queued: '0 (0 bytes)',
                queuedCount: '0',
                queuedSize: '0 bytes',
                bytesRead: 0,
                read: '0 bytes',
                bytesWritten: 0,
                written: '0 bytes',
                flowFilesOut: 0,
                bytesOut: 0,
                output: '0 (0 bytes)',
                flowFilesTransferred: 0,
                bytesTransferred: 0,
                transferred: '0 (0 bytes)',
                bytesReceived: 0,
                flowFilesReceived: 0,
                received: '0 (0 bytes)',
                bytesSent: 0,
                flowFilesSent: 0,
                sent: '0 (0 bytes)',
                activeThreadCount: 0,
                terminatedThreadCount: 0,
                processingNanos: 0
            }
        },
        runningCount: 0,
        stoppedCount: 0,
        invalidCount: 3,
        disabledCount: 0,
        activeRemotePortCount: 0,
        inactiveRemotePortCount: 0,
        upToDateCount: 0,
        locallyModifiedCount: 0,
        staleCount: 0,
        locallyModifiedAndStaleCount: 0,
        syncFailureCount: 0,
        localInputPortCount: 0,
        localOutputPortCount: 0,
        publicInputPortCount: 0,
        publicOutputPortCount: 0,
        inputPortCount: 0,
        outputPortCount: 0
    };

    const parentControllerServices: ControllerServiceEntity[] = [
        {
            revision: {
                version: 0
            },
            id: 'f5bf3c0a-018d-1000-7c8e-910f220b215e',
            uri: 'http://127.0.0.1:4200/nifi-api/controller-services/f5bf3c0a-018d-1000-7c8e-910f220b215e',
            permissions: {
                canRead: true,
                canWrite: true
            },
            bulletins: [],
            parentGroupId: '96456bc3-018b-1000-05d8-51720bebae4b',
            component: {
                id: 'f5bf3c0a-018d-1000-7c8e-910f220b215e',
                versionedComponentId: 'a42a73b5-27a6-3ee7-b740-43345b3849de',
                parentGroupId: '96456bc3-018b-1000-05d8-51720bebae4b',
                name: 'StandardRestrictedSSLContextService',
                type: 'org.apache.nifi.ssl.StandardRestrictedSSLContextService',
                bundle: {
                    group: 'org.apache.nifi',
                    artifact: 'nifi-ssl-context-service-nar',
                    version: '2.0.0-SNAPSHOT'
                },
                controllerServiceApis: [
                    {
                        type: 'org.apache.nifi.ssl.SSLContextService',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-standard-services-api-nar',
                            version: '2.0.0-SNAPSHOT'
                        }
                    },
                    {
                        type: 'org.apache.nifi.ssl.RestrictedSSLContextService',
                        bundle: {
                            group: 'org.apache.nifi',
                            artifact: 'nifi-standard-services-api-nar',
                            version: '2.0.0-SNAPSHOT'
                        }
                    }
                ],
                comments: '',
                state: 'ENABLED',
                persistsState: false,
                restricted: false,
                deprecated: false,
                multipleVersionsAvailable: false,
                supportsSensitiveDynamicProperties: false,
                properties: {
                    'Keystore Filename': '/home/user/nifi-ui/conf/certificate.p12',
                    'Keystore Password': '********',
                    'key-password': null,
                    'Keystore Type': 'PKCS12',
                    'Truststore Filename': '/home/user/nifi-ui/conf/truststore.jks',
                    'Truststore Password': '********',
                    'Truststore Type': 'JKS',
                    'SSL Protocol': 'TLS'
                },
                descriptors: {
                    'Keystore Filename': {
                        name: 'Keystore Filename',
                        displayName: 'Keystore Filename',
                        description: 'The fully-qualified filename of the Keystore',
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: true,
                        expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                        dependencies: []
                    },
                    'Keystore Password': {
                        name: 'Keystore Password',
                        displayName: 'Keystore Password',
                        description: 'The password for the Keystore',
                        required: false,
                        sensitive: true,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    'key-password': {
                        name: 'key-password',
                        displayName: 'Key Password',
                        description:
                            'The password for the key. If this is not specified, but the Keystore Filename, Password, and Type are specified, then the Keystore Password will be assumed to be the same as the Key Password.',
                        required: false,
                        sensitive: true,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    'Keystore Type': {
                        name: 'Keystore Type',
                        displayName: 'Keystore Type',
                        description: 'The Type of the Keystore',
                        allowableValues: [
                            {
                                allowableValue: {
                                    displayName: 'BCFKS',
                                    value: 'BCFKS'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'PKCS12',
                                    value: 'PKCS12'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'JKS',
                                    value: 'JKS'
                                },
                                canRead: true
                            }
                        ],
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    'Truststore Filename': {
                        name: 'Truststore Filename',
                        displayName: 'Truststore Filename',
                        description: 'The fully-qualified filename of the Truststore',
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: true,
                        expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                        dependencies: []
                    },
                    'Truststore Password': {
                        name: 'Truststore Password',
                        displayName: 'Truststore Password',
                        description: 'The password for the Truststore',
                        required: false,
                        sensitive: true,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    'Truststore Type': {
                        name: 'Truststore Type',
                        displayName: 'Truststore Type',
                        description: 'The Type of the Truststore',
                        allowableValues: [
                            {
                                allowableValue: {
                                    displayName: 'BCFKS',
                                    value: 'BCFKS'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'PKCS12',
                                    value: 'PKCS12'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'JKS',
                                    value: 'JKS'
                                },
                                canRead: true
                            }
                        ],
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    },
                    'SSL Protocol': {
                        name: 'SSL Protocol',
                        displayName: 'TLS Protocol',
                        description:
                            'TLS Protocol Version for encrypted connections. Supported versions depend on the specific version of Java used.',
                        defaultValue: 'TLS',
                        allowableValues: [
                            {
                                allowableValue: {
                                    displayName: 'TLS',
                                    value: 'TLS',
                                    description:
                                        'Negotiate latest protocol version based on platform supported versions'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'TLSv1.3',
                                    value: 'TLSv1.3',
                                    description: 'Require TLSv1.3 protocol version'
                                },
                                canRead: true
                            },
                            {
                                allowableValue: {
                                    displayName: 'TLSv1.2',
                                    value: 'TLSv1.2',
                                    description: 'Require TLSv1.2 protocol version'
                                },
                                canRead: true
                            }
                        ],
                        required: false,
                        sensitive: false,
                        dynamic: false,
                        supportsEl: false,
                        expressionLanguageScope: 'Not Supported',
                        dependencies: []
                    }
                },
                referencingComponents: [
                    {
                        revision: {
                            version: 0,
                            lastModifier: 'anonymous'
                        },
                        id: '3e4d7943-018e-1000-ef14-0b244d7070d5',
                        permissions: {
                            canRead: true,
                            canWrite: true
                        },
                        bulletins: [],
                        component: {
                            groupId: '96456bc3-018b-1000-05d8-51720bebae4b',
                            id: '3e4d7943-018e-1000-ef14-0b244d7070d5',
                            name: 'DeleteSQS',
                            type: 'DeleteSQS',
                            state: 'STOPPED',
                            validationErrors: [
                                "'AWS Credentials Provider Service' validated against 'f59560b6-018d-1000-dab9-f19f7d688d65' is invalid because Controller Service with ID f59560b6-018d-1000-dab9-f19f7d688d65 is disabled"
                            ],
                            referenceType: 'Processor',
                            activeThreadCount: 0
                        },
                        operatePermissions: {
                            canRead: true,
                            canWrite: true
                        }
                    },
                    {
                        revision: {
                            version: 0,
                            lastModifier: 'anonymous'
                        },
                        id: 'f5c2e95c-018d-1000-9ed4-5309b99f7fc3',
                        permissions: {
                            canRead: true,
                            canWrite: true
                        },
                        bulletins: [],
                        component: {
                            groupId: 'cc4f110c-018d-1000-d4b6-d933a69c9d53',
                            id: 'f5c2e95c-018d-1000-9ed4-5309b99f7fc3',
                            name: 'MongoDBControllerService',
                            type: 'MongoDBControllerService',
                            state: 'DISABLED',
                            validationErrors: ["'Mongo URI' is invalid because Mongo URI is required"],
                            referenceType: 'ControllerService',
                            referenceCycle: false,
                            referencingComponents: []
                        },
                        operatePermissions: {
                            canRead: true,
                            canWrite: true
                        }
                    },
                    {
                        revision: {
                            version: 0,
                            lastModifier: 'anonymous'
                        },
                        id: '3e08b4c2-018e-1000-9cea-5fb844cd9c0f',
                        permissions: {
                            canRead: true,
                            canWrite: true
                        },
                        bulletins: [],
                        component: {
                            groupId: '96456bc3-018b-1000-05d8-51720bebae4b',
                            id: '3e08b4c2-018e-1000-9cea-5fb844cd9c0f',
                            name: 'PutSQS',
                            type: 'PutSQS',
                            state: 'STOPPED',
                            validationErrors: [
                                "'AWS Credentials Provider Service' validated against 'f59560b6-018d-1000-dab9-f19f7d688d65' is invalid because Controller Service with ID f59560b6-018d-1000-dab9-f19f7d688d65 is disabled"
                            ],
                            referenceType: 'Processor',
                            activeThreadCount: 0
                        },
                        operatePermissions: {
                            canRead: true,
                            canWrite: true
                        }
                    },
                    {
                        revision: {
                            version: 0,
                            lastModifier: 'anonymous'
                        },
                        id: 'f0b8b3e3-018d-1000-23ea-400e3a621493',
                        permissions: {
                            canRead: true,
                            canWrite: true
                        },
                        bulletins: [],
                        component: {
                            groupId: '96456bc3-018b-1000-05d8-51720bebae4b',
                            id: 'f0b8b3e3-018d-1000-23ea-400e3a621493',
                            name: 'PutSNS',
                            type: 'PutSNS',
                            state: 'STOPPED',
                            validationErrors: [
                                "'AWS Credentials Provider Service' validated against 'f59560b6-018d-1000-dab9-f19f7d688d65' is invalid because Controller Service with ID f59560b6-018d-1000-dab9-f19f7d688d65 is disabled",
                                "'Upstream Connections' is invalid because Processor requires an upstream connection but currently has none"
                            ],
                            referenceType: 'Processor',
                            activeThreadCount: 0
                        },
                        operatePermissions: {
                            canRead: true,
                            canWrite: true
                        }
                    },
                    {
                        revision: {
                            version: 0,
                            lastModifier: 'anonymous'
                        },
                        id: '3e44b03b-018e-1000-2dbf-2914e599ae1a',
                        permissions: {
                            canRead: true,
                            canWrite: true
                        },
                        bulletins: [],
                        component: {
                            groupId: '96456bc3-018b-1000-05d8-51720bebae4b',
                            id: '3e44b03b-018e-1000-2dbf-2914e599ae1a',
                            name: 'GetSQS',
                            type: 'GetSQS',
                            state: 'STOPPED',
                            validationErrors: [
                                "'AWS Credentials Provider Service' validated against 'f59560b6-018d-1000-dab9-f19f7d688d65' is invalid because Controller Service with ID f59560b6-018d-1000-dab9-f19f7d688d65 is disabled"
                            ],
                            referenceType: 'Processor',
                            activeThreadCount: 0
                        },
                        operatePermissions: {
                            canRead: true,
                            canWrite: true
                        }
                    }
                ],
                validationStatus: 'VALID',
                bulletinLevel: 'WARN',
                extensionMissing: false
            },
            operatePermissions: {
                canRead: true,
                canWrite: true
            },
            status: {
                runStatus: 'ENABLED',
                validationStatus: 'VALID'
            }
        }
    ];

    const controllerService = {
        revision: {
            clientId: 'b37eba61-156a-47a4-875f-945cd153b876',
            version: 7
        },
        id: '92db6ee4-018c-1000-1061-e3476c3f4e9f',
        uri: 'https://localhost:4200/nifi-api/controller-services/92db6ee4-018c-1000-1061-e3476c3f4e9f',
        permissions: {
            canRead: true,
            canWrite: true
        },
        bulletins: [
            {
                id: 0,
                groupId: 'asdf',
                sourceId: 'asdf',
                timestamp: '14:08:44 EST',
                canRead: true,
                bulletin: {
                    id: 0,
                    category: 'asdf',
                    groupId: 'asdf',
                    sourceId: 'asdf',
                    sourceName: 'asdf',
                    sourceType: ComponentType.Processor,
                    level: 'ERROR',
                    message: 'asdf',
                    timestamp: '14:08:44 EST'
                }
            }
        ],
        component: {
            id: '92db6ee4-018c-1000-1061-e3476c3f4e9f',
            name: 'AvroReader',
            type: 'org.apache.nifi.avro.AvroReader',
            bundle: {
                group: 'org.apache.nifi',
                artifact: 'nifi-record-serialization-services-nar',
                version: '2.0.0-SNAPSHOT'
            },
            controllerServiceApis: [
                {
                    type: 'org.apache.nifi.serialization.RecordReaderFactory',
                    bundle: {
                        group: 'org.apache.nifi',
                        artifact: 'nifi-standard-services-api-nar',
                        version: '2.0.0-SNAPSHOT'
                    }
                }
            ],
            state: 'DISABLED',
            persistsState: false,
            restricted: false,
            deprecated: false,
            multipleVersionsAvailable: false,
            supportsSensitiveDynamicProperties: false,
            properties: {
                'schema-access-strategy': 'embedded-avro-schema',
                'schema-registry': null,
                'schema-name': '${schema.name}',
                'schema-version': null,
                'schema-branch': null,
                'schema-text': '${avro.schema}',
                'schema-reference-reader': null,
                'cache-size': '1000'
            },
            descriptors: {
                'schema-access-strategy': {
                    name: 'schema-access-strategy',
                    displayName: 'Schema Access Strategy',
                    description: 'Specifies how to obtain the schema that is to be used for interpreting the data.',
                    defaultValue: 'embedded-avro-schema',
                    allowableValues: [
                        {
                            allowableValue: {
                                displayName: "Use 'Schema Name' Property",
                                value: 'schema-name',
                                description:
                                    "The name of the Schema to use is specified by the 'Schema Name' Property. The value of this property is used to lookup the Schema in the configured Schema Registry service."
                            },
                            canRead: true
                        },
                        {
                            allowableValue: {
                                displayName: "Use 'Schema Text' Property",
                                value: 'schema-text-property',
                                description:
                                    "The text of the Schema itself is specified by the 'Schema Text' Property. The value of this property must be a valid Avro Schema. If Expression Language is used, the value of the 'Schema Text' property must be valid after substituting the expressions."
                            },
                            canRead: true
                        },
                        {
                            allowableValue: {
                                displayName: 'Schema Reference Reader',
                                value: 'schema-reference-reader',
                                description:
                                    'The schema reference information will be provided through a configured Schema Reference Reader service implementation.'
                            },
                            canRead: true
                        },
                        {
                            allowableValue: {
                                displayName: 'Use Embedded Avro Schema',
                                value: 'embedded-avro-schema',
                                description:
                                    'The FlowFile has the Avro Schema embedded within the content, and this schema will be used.'
                            },
                            canRead: true
                        }
                    ],
                    required: true,
                    sensitive: false,
                    dynamic: false,
                    supportsEl: false,
                    expressionLanguageScope: 'Not Supported',
                    dependencies: []
                },
                'schema-registry': {
                    name: 'schema-registry',
                    displayName: 'Schema Registry',
                    description: 'Specifies the Controller Service to use for the Schema Registry',
                    allowableValues: [],
                    required: false,
                    sensitive: false,
                    dynamic: false,
                    supportsEl: false,
                    expressionLanguageScope: 'Not Supported',
                    identifiesControllerService: 'org.apache.nifi.schemaregistry.services.SchemaRegistry',
                    identifiesControllerServiceBundle: {
                        group: 'org.apache.nifi',
                        artifact: 'nifi-standard-services-api-nar',
                        version: '2.0.0-SNAPSHOT'
                    },
                    dependencies: [
                        {
                            propertyName: 'schema-access-strategy',
                            dependentValues: ['schema-reference-reader', 'schema-name']
                        }
                    ]
                },
                'schema-name': {
                    name: 'schema-name',
                    displayName: 'Schema Name',
                    description: 'Specifies the name of the schema to lookup in the Schema Registry property',
                    defaultValue: '${schema.name}',
                    required: false,
                    sensitive: false,
                    dynamic: false,
                    supportsEl: true,
                    expressionLanguageScope: 'Environment variables and FlowFile Attributes',
                    dependencies: [
                        {
                            propertyName: 'schema-access-strategy',
                            dependentValues: ['schema-name']
                        }
                    ]
                },
                'schema-version': {
                    name: 'schema-version',
                    displayName: 'Schema Version',
                    description:
                        'Specifies the version of the schema to lookup in the Schema Registry. If not specified then the latest version of the schema will be retrieved.',
                    required: false,
                    sensitive: false,
                    dynamic: false,
                    supportsEl: true,
                    expressionLanguageScope: 'Environment variables and FlowFile Attributes',
                    dependencies: [
                        {
                            propertyName: 'schema-access-strategy',
                            dependentValues: ['schema-name']
                        }
                    ]
                },
                'schema-branch': {
                    name: 'schema-branch',
                    displayName: 'Schema Branch',
                    description:
                        'Specifies the name of the branch to use when looking up the schema in the Schema Registry property. If the chosen Schema Registry does not support branching, this value will be ignored.',
                    required: false,
                    sensitive: false,
                    dynamic: false,
                    supportsEl: true,
                    expressionLanguageScope: 'Environment variables and FlowFile Attributes',
                    dependencies: [
                        {
                            propertyName: 'schema-access-strategy',
                            dependentValues: ['schema-name']
                        }
                    ]
                },
                'schema-text': {
                    name: 'schema-text',
                    displayName: 'Schema Text',
                    description: 'The text of an Avro-formatted Schema',
                    defaultValue: '${avro.schema}',
                    required: false,
                    sensitive: false,
                    dynamic: false,
                    supportsEl: true,
                    expressionLanguageScope: 'Environment variables and FlowFile Attributes',
                    dependencies: [
                        {
                            propertyName: 'schema-access-strategy',
                            dependentValues: ['schema-text-property']
                        }
                    ]
                },
                'schema-reference-reader': {
                    name: 'schema-reference-reader',
                    displayName: 'Schema Reference Reader',
                    description:
                        'Service implementation responsible for reading FlowFile attributes or content to determine the Schema Reference Identifier',
                    allowableValues: [],
                    required: true,
                    sensitive: false,
                    dynamic: false,
                    supportsEl: false,
                    expressionLanguageScope: 'Not Supported',
                    identifiesControllerService: 'org.apache.nifi.schemaregistry.services.SchemaReferenceReader',
                    identifiesControllerServiceBundle: {
                        group: 'org.apache.nifi',
                        artifact: 'nifi-standard-services-api-nar',
                        version: '2.0.0-SNAPSHOT'
                    },
                    dependencies: [
                        {
                            propertyName: 'schema-access-strategy',
                            dependentValues: ['schema-reference-reader']
                        }
                    ]
                },
                'cache-size': {
                    name: 'cache-size',
                    displayName: 'Cache Size',
                    description: 'Specifies how many Schemas should be cached',
                    defaultValue: '1000',
                    required: true,
                    sensitive: false,
                    dynamic: false,
                    supportsEl: false,
                    expressionLanguageScope: 'Not Supported',
                    dependencies: []
                }
            },
            referencingComponents: [
                {
                    revision: {
                        clientId: 'b37eba61-156a-47a4-875f-945cd153b876',
                        version: 5,
                        lastModifier: 'admin'
                    },
                    id: '92db918c-018c-1000-ccd5-0796caa6d463',
                    permissions: {
                        canRead: true,
                        canWrite: true
                    },
                    bulletins: [
                        {
                            id: 0,
                            groupId: 'asdf',
                            sourceId: 'asdf',
                            timestamp: '14:08:44 EST',
                            canRead: true,
                            bulletin: {
                                id: 0,
                                category: 'asdf',
                                groupId: 'asdf',
                                sourceId: 'asdf',
                                sourceName: 'asdf',
                                level: 'ERROR',
                                message: 'asdf',
                                timestamp: '14:08:44 EST'
                            }
                        }
                    ],
                    component: {
                        id: '92db918c-018c-1000-ccd5-0796caa6d463',
                        name: 'ReaderLookup',
                        type: 'ReaderLookup',
                        state: 'DISABLED',
                        validationErrors: [
                            "'avro' validated against '92db6ee4-018c-1000-1061-e3476c3f4e9f' is invalid because Controller Service with ID 92db6ee4-018c-1000-1061-e3476c3f4e9f is disabled"
                        ],
                        referenceType: 'ControllerService',
                        referenceCycle: false,
                        referencingComponents: []
                    },
                    operatePermissions: {
                        canRead: false,
                        canWrite: false
                    }
                }
            ],
            validationStatus: 'VALID',
            bulletinLevel: 'WARN',
            extensionMissing: false
        },
        operatePermissions: {
            canRead: false,
            canWrite: false
        },
        status: {
            runStatus: 'DISABLED',
            validationStatus: 'VALID'
        }
    };

    const data: MoveControllerServiceDialogRequest = {
        id: '92db6ee4-018c-1000-1061-e3476c3f4e9f',
        processGroupFlow: flow,
        processGroupEntity: processGroupEntity,
        parentControllerServices: parentControllerServices,
        controllerService: controllerService
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [MoveControllerService, NoopAnimationsModule, MatDialogModule],
            providers: [
                provideMockStore({ initialState }),
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: data
                },
                { provide: MatDialogRef, useValue: null }
            ]
        });
        fixture = TestBed.createComponent(MoveControllerService);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
