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
import { MatSidenavModule } from '@angular/material/sidenav';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { Canvas } from './canvas.component';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../state/flow/flow.reducer';
import { ContextMenu } from '../../../../ui/common/context-menu/context-menu.component';
import { CdkContextMenuTrigger } from '@angular/cdk/menu';
import { selectBreadcrumbs } from '../../state/flow/flow.selectors';
import { BreadcrumbEntity } from '../../state/shared';
import { MockComponent } from 'ng-mocks';
import { GraphControls } from './graph-controls/graph-controls.component';
import { HeaderComponent } from './header/header.component';
import { FooterComponent } from './footer/footer.component';
import { canvasFeatureKey } from '../../state';
import { flowFeatureKey } from '../../state/flow';
import { FlowAnalysisDrawerComponent } from './header/flow-analysis-drawer/flow-analysis-drawer.component';
import { CanvasActionsService } from '../../service/canvas-actions.service';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { CopyResponseEntity } from '../../../../state/copy';

describe('Canvas', () => {
    let component: Canvas;
    let fixture: ComponentFixture<Canvas>;

    beforeEach(() => {
        const breadcrumbEntity: BreadcrumbEntity = {
            id: '',
            permissions: {
                canRead: false,
                canWrite: false
            },
            versionedFlowState: '',
            breadcrumb: {
                id: '',
                name: ''
            }
        };

        TestBed.configureTestingModule({
            declarations: [Canvas],
            imports: [
                CdkContextMenuTrigger,
                ContextMenu,
                MatSidenavModule,
                NoopAnimationsModule,
                MatSidenavModule,
                HttpClientTestingModule,
                MockComponent(GraphControls),
                MockComponent(HeaderComponent),
                MockComponent(FooterComponent),
                FlowAnalysisDrawerComponent
            ],
            providers: [
                provideMockStore({
                    initialState: {
                        [canvasFeatureKey]: {
                            [flowFeatureKey]: initialState
                        }
                    },
                    selectors: [
                        {
                            selector: selectBreadcrumbs,
                            value: breadcrumbEntity
                        }
                    ]
                }),
                CanvasActionsService
            ]
        });
        fixture = TestBed.createComponent(Canvas);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('toCopyResponseEntity', () => {
        it('should not convert generic string to CopyResponseEntity object', () => {
            expect(component.toCopyResponseEntity('this is a test')).toBeNull();
        });
        it('should not convert unexpected object to CopyResponseEntity object', () => {
            expect(component.toCopyResponseEntity('{ "name": "test name" }')).toBeNull();
        });
        it('should not produce a valid CopyResponseEntity if there are no components', () => {
            const noComponentsCopyResponseString = '{ "id": "test", "processors": [], "processGroups": [] }';
            expect(component.toCopyResponseEntity(noComponentsCopyResponseString)).toBeNull();
        });
        it('should convert valid CopyResponseEntity string to object', () => {
            const parsedObject = component.toCopyResponseEntity(JSON.stringify(validCopyResponse));
            expect(parsedObject).not.toBeNull();
            expect(parsedObject?.id).toBe('356b56df-5a72-49c3-9bf9-881b7e1db21f');
            expect(parsedObject?.processGroups).toHaveLength(1);
            expect(parsedObject?.processGroups![0].processors).toHaveLength(3);
        });
        it('should not produce a valid CopyResponseEntity if there are no components in the FlowDefinition string', () => {
            const noComponentsFlowString =
                '{ "flow": {}, "flowContents": { "id": "test", "processors": [], "processGroups": [] } }';
            expect(component.toCopyResponseEntity(noComponentsFlowString)).toBeNull();
        });
        it('should convert valid FlowDefinition string to CopyResponseObject', () => {
            const parsedObject = component.toCopyResponseEntity(JSON.stringify(validFlowDefinition));
            expect(parsedObject).not.toBeNull();
            expect(parsedObject?.id).toBe('flow-contents-group');
            expect(parsedObject?.processGroups).toHaveLength(1);
            expect(parsedObject?.processGroups![0].processors).toHaveLength(3);
        });

        const validCopyResponse: CopyResponseEntity = {
            id: '356b56df-5a72-49c3-9bf9-881b7e1db21f',
            externalControllerServiceReferences: {
                '9748216b-3ec7-35e7-9cd8-890bec13bd6a': {
                    identifier: '9748216b-3ec7-35e7-9cd8-890bec13bd6a',
                    name: 'CSVReader'
                }
            },
            parameterContexts: {
                'inherited param context': {
                    name: 'inherited param context',
                    parameters: [
                        {
                            name: 'bearer_token',
                            description: '',
                            sensitive: true,
                            provided: false
                        },
                        {
                            name: 'asdfasf',
                            description: '',
                            sensitive: false,
                            provided: false
                        },
                        {
                            name: 'asfasdfasdfasdf',
                            description: '',
                            sensitive: false,
                            provided: false,
                            value: 'asdfasdfasfd'
                        },
                        {
                            name: 'hello',
                            description: '',
                            sensitive: false,
                            provided: false,
                            value: 'Yo Yo'
                        },
                        {
                            name: 'Bearer Token',
                            description: '',
                            sensitive: true,
                            provided: false
                        },
                        {
                            name: 'Record Writer',
                            description: '',
                            sensitive: false,
                            provided: false,
                            value: '268248a8-0193-1000-6122-15e198fc48dd'
                        }
                    ],
                    inheritedParameterContexts: ['Default Params'],
                    componentType: 'PARAMETER_CONTEXT'
                },
                'Default Params': {
                    name: 'Default Params',
                    parameters: [
                        {
                            name: 'Transform Cache Size',
                            description: '',
                            sensitive: false,
                            provided: false,
                            value: '10 kb'
                        },
                        {
                            name: 'Batch Size',
                            description: '',
                            sensitive: false,
                            provided: false
                        },
                        {
                            name: 'Github Personal Access Token',
                            description: '',
                            sensitive: true,
                            provided: false
                        },
                        {
                            name: 'test',
                            description: 'test',
                            sensitive: false,
                            provided: false
                        },
                        {
                            name: 'bytes',
                            description: '',
                            sensitive: false,
                            provided: false,
                            value: '15 B'
                        },
                        {
                            name: 'Cache Size',
                            description: '',
                            sensitive: false,
                            provided: false,
                            value: '1000'
                        },
                        {
                            name: 'hello',
                            description: '',
                            sensitive: false,
                            provided: false,
                            value: 'Hello!'
                        },
                        {
                            name: 'my_pwd',
                            description: '',
                            sensitive: true,
                            provided: false
                        },
                        {
                            name: 'Data Format',
                            description: '',
                            sensitive: false,
                            provided: false,
                            value: 'Text'
                        }
                    ],
                    inheritedParameterContexts: [],
                    componentType: 'PARAMETER_CONTEXT'
                }
            },
            parameterProviders: {},
            processGroups: [
                {
                    identifier: '9da8125e-bad8-32fa-aeeb-2a61c864cd0d',
                    instanceIdentifier: '6b92cde8-0194-1000-605f-d8b211bef857',
                    name: 'test',
                    comments: '',
                    position: {
                        x: 29.5,
                        y: -371.5
                    },
                    processGroups: [],
                    remoteProcessGroups: [],
                    processors: [
                        {
                            identifier: '7a07ab43-1f59-34a1-96d2-e2b397ff8abc',
                            instanceIdentifier: 'b533f941-bfbe-3bc3-0e5d-493a95397fae',
                            name: 'InvokeHTTP',
                            comments: '',
                            position: {
                                x: -552,
                                y: -8
                            },
                            type: 'org.apache.nifi.processors.standard.InvokeHTTP',
                            bundle: {
                                group: 'org.apache.nifi',
                                artifact: 'nifi-standard-nar',
                                version: '2.2.0-SNAPSHOT'
                            },
                            properties: {
                                'Request Content-Encoding': 'DISABLED',
                                'proxy-configuration-service': null,
                                'Request Multipart Form-Data Filename Enabled': 'true',
                                'Request Chunked Transfer-Encoding Enabled': 'false',
                                'Response Header Request Attributes Prefix': null,
                                'HTTP/2 Disabled': 'False',
                                'Connection Timeout': '5 secs',
                                'Response Cookie Strategy': 'DISABLED',
                                'Socket Read Timeout': '15 secs',
                                password: 'test',
                                'Socket Idle Connections': '5',
                                'Request Body Enabled': 'true',
                                Auth: '#{bearer_token}',
                                'HTTP URL': 'http://google.com',
                                'Request OAuth2 Access Token Provider': null,
                                'Socket Idle Timeout': '5 mins',
                                'Response Redirects Enabled': 'False',
                                'Socket Write Timeout': '15 secs',
                                'Request Header Attributes Pattern': null,
                                'Response FlowFile Naming Strategy': 'RANDOM',
                                'Response Cache Enabled': 'false',
                                'Request Date Header Enabled': 'True',
                                'Request Failure Penalization Enabled': 'false',
                                'Response Body Attribute Size': '256',
                                'SSL Context Service': null,
                                'Response Generation Required': 'false',
                                'Request User-Agent': null,
                                'Response Header Request Attributes Enabled': 'false',
                                'HTTP Method': 'GET',
                                'Request Username': null,
                                'Request Content-Type': '${mime.type}',
                                'Response Body Attribute Name': null,
                                'Request Digest Authentication Enabled': 'false',
                                'Request Multipart Form-Data Name': null,
                                'Response Cache Size': '10MB',
                                'Response Body Ignored': 'false'
                            },
                            propertyDescriptors: {
                                'Request Content-Encoding': {
                                    name: 'Request Content-Encoding',
                                    displayName: 'Request Content-Encoding',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'proxy-configuration-service': {
                                    name: 'proxy-configuration-service',
                                    displayName: 'Proxy Configuration Service',
                                    identifiesControllerService: true,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request Multipart Form-Data Filename Enabled': {
                                    name: 'Request Multipart Form-Data Filename Enabled',
                                    displayName: 'Request Multipart Form-Data Filename Enabled',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request Chunked Transfer-Encoding Enabled': {
                                    name: 'Request Chunked Transfer-Encoding Enabled',
                                    displayName: 'Request Chunked Transfer-Encoding Enabled',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Response Header Request Attributes Prefix': {
                                    name: 'Response Header Request Attributes Prefix',
                                    displayName: 'Response Header Request Attributes Prefix',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'HTTP/2 Disabled': {
                                    name: 'HTTP/2 Disabled',
                                    displayName: 'HTTP/2 Disabled',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Connection Timeout': {
                                    name: 'Connection Timeout',
                                    displayName: 'Connection Timeout',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Response Cookie Strategy': {
                                    name: 'Response Cookie Strategy',
                                    displayName: 'Response Cookie Strategy',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request Password': {
                                    name: 'Request Password',
                                    displayName: 'Request Password',
                                    identifiesControllerService: false,
                                    sensitive: true,
                                    dynamic: false
                                },
                                'Socket Read Timeout': {
                                    name: 'Socket Read Timeout',
                                    displayName: 'Socket Read Timeout',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                password: {
                                    name: 'password',
                                    displayName: 'password',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: true
                                },
                                'Socket Idle Connections': {
                                    name: 'Socket Idle Connections',
                                    displayName: 'Socket Idle Connections',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request Body Enabled': {
                                    name: 'Request Body Enabled',
                                    displayName: 'Request Body Enabled',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                Auth: {
                                    name: 'Auth',
                                    displayName: 'Auth',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: true
                                },
                                'HTTP URL': {
                                    name: 'HTTP URL',
                                    displayName: 'HTTP URL',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request OAuth2 Access Token Provider': {
                                    name: 'Request OAuth2 Access Token Provider',
                                    displayName: 'Request OAuth2 Access Token Provider',
                                    identifiesControllerService: true,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Socket Idle Timeout': {
                                    name: 'Socket Idle Timeout',
                                    displayName: 'Socket Idle Timeout',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Response Redirects Enabled': {
                                    name: 'Response Redirects Enabled',
                                    displayName: 'Response Redirects Enabled',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Socket Write Timeout': {
                                    name: 'Socket Write Timeout',
                                    displayName: 'Socket Write Timeout',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request Header Attributes Pattern': {
                                    name: 'Request Header Attributes Pattern',
                                    displayName: 'Request Header Attributes Pattern',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Response FlowFile Naming Strategy': {
                                    name: 'Response FlowFile Naming Strategy',
                                    displayName: 'Response FlowFile Naming Strategy',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Response Cache Enabled': {
                                    name: 'Response Cache Enabled',
                                    displayName: 'Response Cache Enabled',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request Date Header Enabled': {
                                    name: 'Request Date Header Enabled',
                                    displayName: 'Request Date Header Enabled',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request Failure Penalization Enabled': {
                                    name: 'Request Failure Penalization Enabled',
                                    displayName: 'Request Failure Penalization Enabled',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Response Body Attribute Size': {
                                    name: 'Response Body Attribute Size',
                                    displayName: 'Response Body Attribute Size',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'SSL Context Service': {
                                    name: 'SSL Context Service',
                                    displayName: 'SSL Context Service',
                                    identifiesControllerService: true,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Response Generation Required': {
                                    name: 'Response Generation Required',
                                    displayName: 'Response Generation Required',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request User-Agent': {
                                    name: 'Request User-Agent',
                                    displayName: 'Request User-Agent',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Response Header Request Attributes Enabled': {
                                    name: 'Response Header Request Attributes Enabled',
                                    displayName: 'Response Header Request Attributes Enabled',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'HTTP Method': {
                                    name: 'HTTP Method',
                                    displayName: 'HTTP Method',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request Username': {
                                    name: 'Request Username',
                                    displayName: 'Request Username',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request Content-Type': {
                                    name: 'Request Content-Type',
                                    displayName: 'Request Content-Type',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Response Body Attribute Name': {
                                    name: 'Response Body Attribute Name',
                                    displayName: 'Response Body Attribute Name',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request Digest Authentication Enabled': {
                                    name: 'Request Digest Authentication Enabled',
                                    displayName: 'Request Digest Authentication Enabled',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Request Multipart Form-Data Name': {
                                    name: 'Request Multipart Form-Data Name',
                                    displayName: 'Request Multipart Form-Data Name',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Response Cache Size': {
                                    name: 'Response Cache Size',
                                    displayName: 'Response Cache Size',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Response Body Ignored': {
                                    name: 'Response Body Ignored',
                                    displayName: 'Response Body Ignored',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                }
                            },
                            style: {},
                            schedulingPeriod: '0 sec',
                            schedulingStrategy: 'TIMER_DRIVEN',
                            executionNode: 'ALL',
                            penaltyDuration: '30 sec',
                            yieldDuration: '1 sec',
                            bulletinLevel: 'WARN',
                            runDurationMillis: 0,
                            concurrentlySchedulableTaskCount: 1,
                            autoTerminatedRelationships: [],
                            scheduledState: 'ENABLED',
                            retryCount: 10,
                            retriedRelationships: [],
                            backoffMechanism: 'PENALIZE_FLOWFILE',
                            maxBackoffPeriod: '10 mins',
                            componentType: 'PROCESSOR',
                            groupIdentifier: '9da8125e-bad8-32fa-aeeb-2a61c864cd0d'
                        },
                        {
                            identifier: '2dfe7e72-3e78-3145-abd4-edafa584a5c4',
                            instanceIdentifier: '31b5895a-c1de-3cf4-6cfe-3eb52e34a3e4',
                            name: 'AttributeRollingWindow',
                            comments: '',
                            position: {
                                x: -50.49997239367087,
                                y: -104.49999952697408
                            },
                            type: 'org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow',
                            bundle: {
                                group: 'org.apache.nifi',
                                artifact: 'nifi-stateful-analysis-nar',
                                version: '2.2.0-SNAPSHOT'
                            },
                            properties: {
                                'Value to track': null,
                                'Time window': null,
                                'Sub-window length': null
                            },
                            propertyDescriptors: {
                                'Value to track': {
                                    name: 'Value to track',
                                    displayName: 'Value to track',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Time window': {
                                    name: 'Time window',
                                    displayName: 'Time window',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Sub-window length': {
                                    name: 'Sub-window length',
                                    displayName: 'Sub-window length',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                }
                            },
                            style: {},
                            schedulingPeriod: '0 sec',
                            schedulingStrategy: 'TIMER_DRIVEN',
                            executionNode: 'ALL',
                            penaltyDuration: '30 sec',
                            yieldDuration: '1 sec',
                            bulletinLevel: 'WARN',
                            runDurationMillis: 0,
                            concurrentlySchedulableTaskCount: 1,
                            autoTerminatedRelationships: ['set state fail', 'failure'],
                            scheduledState: 'ENABLED',
                            retryCount: 10,
                            retriedRelationships: [],
                            backoffMechanism: 'PENALIZE_FLOWFILE',
                            maxBackoffPeriod: '10 mins',
                            componentType: 'PROCESSOR',
                            groupIdentifier: '9da8125e-bad8-32fa-aeeb-2a61c864cd0d'
                        },
                        {
                            identifier: '7e71127d-c9ee-3d3d-8e86-9bc871f06ab5',
                            instanceIdentifier: '708eeb86-0194-1000-80f1-6332ced1d590',
                            name: 'ConvertRecord',
                            comments: '',
                            position: {
                                x: -928,
                                y: -96
                            },
                            type: 'org.apache.nifi.processors.standard.ConvertRecord',
                            bundle: {
                                group: 'org.apache.nifi',
                                artifact: 'nifi-standard-nar',
                                version: '2.2.0-SNAPSHOT'
                            },
                            properties: {
                                'Include Zero Record FlowFiles': 'true',
                                'Record Writer': null,
                                'Record Reader': '9748216b-3ec7-35e7-9cd8-890bec13bd6a'
                            },
                            propertyDescriptors: {
                                'Include Zero Record FlowFiles': {
                                    name: 'Include Zero Record FlowFiles',
                                    displayName: 'Include Zero Record FlowFiles',
                                    identifiesControllerService: false,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Record Writer': {
                                    name: 'Record Writer',
                                    displayName: 'Record Writer',
                                    identifiesControllerService: true,
                                    sensitive: false,
                                    dynamic: false
                                },
                                'Record Reader': {
                                    name: 'Record Reader',
                                    displayName: 'Record Reader',
                                    identifiesControllerService: true,
                                    sensitive: false,
                                    dynamic: false
                                }
                            },
                            style: {},
                            schedulingPeriod: '0 sec',
                            schedulingStrategy: 'TIMER_DRIVEN',
                            executionNode: 'ALL',
                            penaltyDuration: '30 sec',
                            yieldDuration: '1 sec',
                            bulletinLevel: 'WARN',
                            runDurationMillis: 0,
                            concurrentlySchedulableTaskCount: 1,
                            autoTerminatedRelationships: [],
                            scheduledState: 'ENABLED',
                            retryCount: 10,
                            retriedRelationships: [],
                            backoffMechanism: 'PENALIZE_FLOWFILE',
                            maxBackoffPeriod: '10 mins',
                            componentType: 'PROCESSOR',
                            groupIdentifier: '9da8125e-bad8-32fa-aeeb-2a61c864cd0d'
                        }
                    ],
                    inputPorts: [],
                    outputPorts: [],
                    connections: [
                        {
                            identifier: 'f43a379c-8e08-3b8b-9b9d-79e37c7d28ef',
                            instanceIdentifier: 'f8411bc2-d353-3acc-53a4-39becddc2a9d',
                            name: '',
                            source: {
                                id: '2dfe7e72-3e78-3145-abd4-edafa584a5c4',
                                type: 'PROCESSOR',
                                groupId: '9da8125e-bad8-32fa-aeeb-2a61c864cd0d',
                                name: 'AttributeRollingWindow',
                                comments: '',
                                instanceIdentifier: '31b5895a-c1de-3cf4-6cfe-3eb52e34a3e4'
                            },
                            destination: {
                                id: '2d909769-0b49-3c50-83fa-b833a569c118',
                                type: 'FUNNEL',
                                groupId: '9da8125e-bad8-32fa-aeeb-2a61c864cd0d',
                                name: 'Funnel',
                                comments: '',
                                instanceIdentifier: '63fa4b81-c752-3ada-4954-4c3df28ef894'
                            },
                            labelIndex: 0,
                            zIndex: 0,
                            selectedRelationships: ['success'],
                            backPressureObjectThreshold: 10000,
                            backPressureDataSizeThreshold: '1 GB',
                            flowFileExpiration: '0 sec',
                            prioritizers: [],
                            bends: [],
                            loadBalanceStrategy: 'DO_NOT_LOAD_BALANCE',
                            partitioningAttribute: '',
                            loadBalanceCompression: 'DO_NOT_COMPRESS',
                            componentType: 'CONNECTION',
                            groupIdentifier: '9da8125e-bad8-32fa-aeeb-2a61c864cd0d'
                        }
                    ],
                    labels: [],
                    funnels: [
                        {
                            identifier: '2d909769-0b49-3c50-83fa-b833a569c118',
                            instanceIdentifier: '63fa4b81-c752-3ada-4954-4c3df28ef894',
                            position: {
                                x: 48,
                                y: 240
                            },
                            componentType: 'FUNNEL',
                            groupIdentifier: '9da8125e-bad8-32fa-aeeb-2a61c864cd0d'
                        }
                    ],
                    controllerServices: [],
                    versionedFlowCoordinates: {
                        registryId: '6b8ef04c-0194-1000-26bf-2c346fee70a6',
                        storageLocation: 'git@github.com:rfellows/flows.git',
                        branch: 'main',
                        bucketId: 'default',
                        flowId: 'test-version-stuff',
                        version: 'c0aeec6a038060e8dab4942712f198ed70c95cc4'
                    },
                    parameterContextName: 'inherited param context',
                    defaultFlowFileExpiration: '0 sec',
                    defaultBackPressureObjectThreshold: 10000,
                    defaultBackPressureDataSizeThreshold: '1 GB',
                    scheduledState: 'ENABLED',
                    executionEngine: 'INHERITED',
                    maxConcurrentTasks: 1,
                    statelessFlowTimeout: '1 min',
                    componentType: 'PROCESS_GROUP',
                    flowFileConcurrency: 'UNBOUNDED',
                    flowFileOutboundPolicy: 'STREAM_WHEN_AVAILABLE',
                    groupIdentifier: '278c0567-a31d-3bca-ad94-056706298851'
                }
            ],
            remoteProcessGroups: [],
            processors: [],
            inputPorts: [],
            outputPorts: [],
            connections: [],
            labels: [],
            funnels: []
        };

        const validFlowDefinition = {
            externalControllerServices: {
                '9748216b-3ec7-35e7-9cd8-890bec13bd6a': {
                    identifier: '9748216b-3ec7-35e7-9cd8-890bec13bd6a',
                    name: 'CSVReader'
                }
            },
            flow: {
                createdTimestamp: 1732298378956,
                description: '',
                identifier: 'test-version-stuff',
                lastModifiedTimestamp: 1732298378956,
                name: 'test version stuff',
                versionCount: 0
            },
            flowContents: {
                comments: '',
                componentType: 'PROCESS_GROUP',
                connections: [
                    {
                        backPressureDataSizeThreshold: '1 GB',
                        backPressureObjectThreshold: 10000,
                        bends: [],
                        componentType: 'CONNECTION',
                        destination: {
                            comments: '',
                            groupId: 'flow-contents-group',
                            id: '2d909769-0b49-3c50-83fa-b833a569c118',
                            name: 'Funnel',
                            type: 'FUNNEL'
                        },
                        flowFileExpiration: '0 sec',
                        groupIdentifier: 'flow-contents-group',
                        identifier: 'f43a379c-8e08-3b8b-9b9d-79e37c7d28ef',
                        labelIndex: 0,
                        loadBalanceCompression: 'DO_NOT_COMPRESS',
                        loadBalanceStrategy: 'DO_NOT_LOAD_BALANCE',
                        name: '',
                        partitioningAttribute: '',
                        prioritizers: [],
                        selectedRelationships: ['success'],
                        source: {
                            comments: '',
                            groupId: 'flow-contents-group',
                            id: '2dfe7e72-3e78-3145-abd4-edafa584a5c4',
                            name: 'AttributeRollingWindow',
                            type: 'PROCESSOR'
                        },
                        zIndex: 0
                    }
                ],
                controllerServices: [],
                defaultBackPressureDataSizeThreshold: '1 GB',
                defaultBackPressureObjectThreshold: 10001,
                defaultFlowFileExpiration: '0 sec',
                executionEngine: 'INHERITED',
                externalControllerServiceReferences: {
                    '9748216b-3ec7-35e7-9cd8-890bec13bd6a': {
                        identifier: '9748216b-3ec7-35e7-9cd8-890bec13bd6a',
                        name: 'CSVReader'
                    }
                },
                flowFileConcurrency: 'UNBOUNDED',
                flowFileOutboundPolicy: 'STREAM_WHEN_AVAILABLE',
                funnels: [
                    {
                        componentType: 'FUNNEL',
                        groupIdentifier: 'flow-contents-group',
                        identifier: '2d909769-0b49-3c50-83fa-b833a569c118',
                        position: {
                            x: 48.0,
                            y: 240.0
                        }
                    }
                ],
                identifier: 'flow-contents-group',
                inputPorts: [],
                labels: [],
                maxConcurrentTasks: 1,
                name: 'test',
                outputPorts: [],
                parameterContextName: 'inherited param context',
                position: {
                    x: 0.0,
                    y: 0.0
                },
                processGroups: [],
                processors: [
                    {
                        autoTerminatedRelationships: [],
                        backoffMechanism: 'PENALIZE_FLOWFILE',
                        bulletinLevel: 'WARN',
                        bundle: {
                            artifact: 'nifi-standard-nar',
                            group: 'org.apache.nifi',
                            version: '2.2.0-SNAPSHOT'
                        },
                        comments: '',
                        componentType: 'PROCESSOR',
                        concurrentlySchedulableTaskCount: 1,
                        executionNode: 'ALL',
                        groupIdentifier: 'flow-contents-group',
                        identifier: '7a07ab43-1f59-34a1-96d2-e2b397ff8abc',
                        maxBackoffPeriod: '10 mins',
                        name: 'InvokeHTTP',
                        penaltyDuration: '30 sec',
                        position: {
                            x: -552.0,
                            y: -8.0
                        },
                        properties: {
                            'Request Content-Encoding': 'DISABLED',
                            'Request Multipart Form-Data Filename Enabled': 'true',
                            'Request Chunked Transfer-Encoding Enabled': 'false',
                            'HTTP/2 Disabled': 'False',
                            'Connection Timeout': '5 secs',
                            'Response Cookie Strategy': 'DISABLED',
                            'Socket Read Timeout': '15 secs',
                            password: 'test',
                            'Socket Idle Connections': '5',
                            'Request Body Enabled': 'true',
                            Auth: '#{bearer_token}',
                            'HTTP URL': 'http://google.com',
                            'Socket Idle Timeout': '5 mins',
                            'Response Redirects Enabled': 'False',
                            'Socket Write Timeout': '15 secs',
                            'Response FlowFile Naming Strategy': 'RANDOM',
                            'Response Cache Enabled': 'false',
                            'Request Date Header Enabled': 'True',
                            'Request Failure Penalization Enabled': 'false',
                            'Response Body Attribute Size': '256',
                            'Response Generation Required': 'false',
                            'Response Header Request Attributes Enabled': 'false',
                            'HTTP Method': 'GET',
                            'Request Content-Type': '${mime.type}',
                            'Request Digest Authentication Enabled': 'false',
                            'Response Cache Size': '10MB',
                            'Response Body Ignored': 'false'
                        },
                        propertyDescriptors: {
                            'Request Content-Encoding': {
                                displayName: 'Request Content-Encoding',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Content-Encoding',
                                sensitive: false
                            },
                            'proxy-configuration-service': {
                                displayName: 'Proxy Configuration Service',
                                dynamic: false,
                                identifiesControllerService: true,
                                name: 'proxy-configuration-service',
                                sensitive: false
                            },
                            'Request Multipart Form-Data Filename Enabled': {
                                displayName: 'Request Multipart Form-Data Filename Enabled',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Multipart Form-Data Filename Enabled',
                                sensitive: false
                            },
                            'Request Chunked Transfer-Encoding Enabled': {
                                displayName: 'Request Chunked Transfer-Encoding Enabled',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Chunked Transfer-Encoding Enabled',
                                sensitive: false
                            },
                            'Response Header Request Attributes Prefix': {
                                displayName: 'Response Header Request Attributes Prefix',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Response Header Request Attributes Prefix',
                                sensitive: false
                            },
                            'HTTP/2 Disabled': {
                                displayName: 'HTTP/2 Disabled',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'HTTP/2 Disabled',
                                sensitive: false
                            },
                            'Connection Timeout': {
                                displayName: 'Connection Timeout',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Connection Timeout',
                                sensitive: false
                            },
                            'Response Cookie Strategy': {
                                displayName: 'Response Cookie Strategy',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Response Cookie Strategy',
                                sensitive: false
                            },
                            'Request Password': {
                                displayName: 'Request Password',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Password',
                                sensitive: true
                            },
                            'Socket Read Timeout': {
                                displayName: 'Socket Read Timeout',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Socket Read Timeout',
                                sensitive: false
                            },
                            password: {
                                displayName: 'password',
                                dynamic: true,
                                identifiesControllerService: false,
                                name: 'password',
                                sensitive: false
                            },
                            'Socket Idle Connections': {
                                displayName: 'Socket Idle Connections',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Socket Idle Connections',
                                sensitive: false
                            },
                            'Request Body Enabled': {
                                displayName: 'Request Body Enabled',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Body Enabled',
                                sensitive: false
                            },
                            Auth: {
                                displayName: 'Auth',
                                dynamic: true,
                                identifiesControllerService: false,
                                name: 'Auth',
                                sensitive: false
                            },
                            'HTTP URL': {
                                displayName: 'HTTP URL',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'HTTP URL',
                                sensitive: false
                            },
                            'Request OAuth2 Access Token Provider': {
                                displayName: 'Request OAuth2 Access Token Provider',
                                dynamic: false,
                                identifiesControllerService: true,
                                name: 'Request OAuth2 Access Token Provider',
                                sensitive: false
                            },
                            'Socket Idle Timeout': {
                                displayName: 'Socket Idle Timeout',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Socket Idle Timeout',
                                sensitive: false
                            },
                            'Response Redirects Enabled': {
                                displayName: 'Response Redirects Enabled',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Response Redirects Enabled',
                                sensitive: false
                            },
                            'Socket Write Timeout': {
                                displayName: 'Socket Write Timeout',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Socket Write Timeout',
                                sensitive: false
                            },
                            'Request Header Attributes Pattern': {
                                displayName: 'Request Header Attributes Pattern',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Header Attributes Pattern',
                                sensitive: false
                            },
                            'Response FlowFile Naming Strategy': {
                                displayName: 'Response FlowFile Naming Strategy',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Response FlowFile Naming Strategy',
                                sensitive: false
                            },
                            'Response Cache Enabled': {
                                displayName: 'Response Cache Enabled',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Response Cache Enabled',
                                sensitive: false
                            },
                            'Request Date Header Enabled': {
                                displayName: 'Request Date Header Enabled',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Date Header Enabled',
                                sensitive: false
                            },
                            'Request Failure Penalization Enabled': {
                                displayName: 'Request Failure Penalization Enabled',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Failure Penalization Enabled',
                                sensitive: false
                            },
                            'Response Body Attribute Size': {
                                displayName: 'Response Body Attribute Size',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Response Body Attribute Size',
                                sensitive: false
                            },
                            'SSL Context Service': {
                                displayName: 'SSL Context Service',
                                dynamic: false,
                                identifiesControllerService: true,
                                name: 'SSL Context Service',
                                sensitive: false
                            },
                            'Response Generation Required': {
                                displayName: 'Response Generation Required',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Response Generation Required',
                                sensitive: false
                            },
                            'Request User-Agent': {
                                displayName: 'Request User-Agent',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request User-Agent',
                                sensitive: false
                            },
                            'Response Header Request Attributes Enabled': {
                                displayName: 'Response Header Request Attributes Enabled',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Response Header Request Attributes Enabled',
                                sensitive: false
                            },
                            'HTTP Method': {
                                displayName: 'HTTP Method',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'HTTP Method',
                                sensitive: false
                            },
                            'Request Username': {
                                displayName: 'Request Username',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Username',
                                sensitive: false
                            },
                            'Request Content-Type': {
                                displayName: 'Request Content-Type',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Content-Type',
                                sensitive: false
                            },
                            'Response Body Attribute Name': {
                                displayName: 'Response Body Attribute Name',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Response Body Attribute Name',
                                sensitive: false
                            },
                            'Request Digest Authentication Enabled': {
                                displayName: 'Request Digest Authentication Enabled',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Digest Authentication Enabled',
                                sensitive: false
                            },
                            'Request Multipart Form-Data Name': {
                                displayName: 'Request Multipart Form-Data Name',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Request Multipart Form-Data Name',
                                sensitive: false
                            },
                            'Response Cache Size': {
                                displayName: 'Response Cache Size',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Response Cache Size',
                                sensitive: false
                            },
                            'Response Body Ignored': {
                                displayName: 'Response Body Ignored',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Response Body Ignored',
                                sensitive: false
                            }
                        },
                        retriedRelationships: [],
                        retryCount: 10,
                        runDurationMillis: 0,
                        scheduledState: 'ENABLED',
                        schedulingPeriod: '0 sec',
                        schedulingStrategy: 'TIMER_DRIVEN',
                        style: {},
                        type: 'org.apache.nifi.processors.standard.InvokeHTTP',
                        yieldDuration: '1 sec'
                    },
                    {
                        autoTerminatedRelationships: ['set state fail', 'failure'],
                        backoffMechanism: 'PENALIZE_FLOWFILE',
                        bulletinLevel: 'WARN',
                        bundle: {
                            artifact: 'nifi-stateful-analysis-nar',
                            group: 'org.apache.nifi',
                            version: '2.2.0-SNAPSHOT'
                        },
                        comments: '',
                        componentType: 'PROCESSOR',
                        concurrentlySchedulableTaskCount: 1,
                        executionNode: 'ALL',
                        groupIdentifier: 'flow-contents-group',
                        identifier: '2dfe7e72-3e78-3145-abd4-edafa584a5c4',
                        maxBackoffPeriod: '10 mins',
                        name: 'AttributeRollingWindow',
                        penaltyDuration: '30 sec',
                        position: {
                            x: -50.49997239367087,
                            y: -104.49999952697408
                        },
                        properties: {},
                        propertyDescriptors: {
                            'Value to track': {
                                displayName: 'Value to track',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Value to track',
                                sensitive: false
                            },
                            'Time window': {
                                displayName: 'Time window',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Time window',
                                sensitive: false
                            },
                            'Sub-window length': {
                                displayName: 'Sub-window length',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Sub-window length',
                                sensitive: false
                            }
                        },
                        retriedRelationships: [],
                        retryCount: 10,
                        runDurationMillis: 0,
                        scheduledState: 'ENABLED',
                        schedulingPeriod: '0 sec',
                        schedulingStrategy: 'TIMER_DRIVEN',
                        style: {},
                        type: 'org.apache.nifi.processors.stateful.analysis.AttributeRollingWindow',
                        yieldDuration: '1 sec'
                    },
                    {
                        autoTerminatedRelationships: [],
                        backoffMechanism: 'PENALIZE_FLOWFILE',
                        bulletinLevel: 'WARN',
                        bundle: {
                            artifact: 'nifi-standard-nar',
                            group: 'org.apache.nifi',
                            version: '2.2.0-SNAPSHOT'
                        },
                        comments: '',
                        componentType: 'PROCESSOR',
                        concurrentlySchedulableTaskCount: 1,
                        executionNode: 'ALL',
                        groupIdentifier: 'flow-contents-group',
                        identifier: '7e71127d-c9ee-3d3d-8e86-9bc871f06ab5',
                        maxBackoffPeriod: '10 mins',
                        name: 'ConvertRecord',
                        penaltyDuration: '30 sec',
                        position: {
                            x: -928.0,
                            y: -96.0
                        },
                        properties: {
                            'Include Zero Record FlowFiles': 'true',
                            'Record Reader': '9748216b-3ec7-35e7-9cd8-890bec13bd6a'
                        },
                        propertyDescriptors: {
                            'Include Zero Record FlowFiles': {
                                displayName: 'Include Zero Record FlowFiles',
                                dynamic: false,
                                identifiesControllerService: false,
                                name: 'Include Zero Record FlowFiles',
                                sensitive: false
                            },
                            'Record Writer': {
                                displayName: 'Record Writer',
                                dynamic: false,
                                identifiesControllerService: true,
                                name: 'Record Writer',
                                sensitive: false
                            },
                            'Record Reader': {
                                displayName: 'Record Reader',
                                dynamic: false,
                                identifiesControllerService: true,
                                name: 'Record Reader',
                                sensitive: false
                            }
                        },
                        retriedRelationships: [],
                        retryCount: 10,
                        runDurationMillis: 0,
                        scheduledState: 'ENABLED',
                        schedulingPeriod: '0 sec',
                        schedulingStrategy: 'TIMER_DRIVEN',
                        style: {},
                        type: 'org.apache.nifi.processors.standard.ConvertRecord',
                        yieldDuration: '1 sec'
                    }
                ],
                remoteProcessGroups: [],
                scheduledState: 'ENABLED',
                statelessFlowTimeout: '1 min'
            },
            flowEncodingVersion: '1.0',
            latest: false,
            parameterContexts: {
                'inherited param context': {
                    componentType: 'PARAMETER_CONTEXT',
                    inheritedParameterContexts: ['Default Params'],
                    name: 'inherited param context',
                    parameters: [
                        {
                            description: '',
                            name: 'bearer_token',
                            provided: false,
                            sensitive: true
                        },
                        {
                            description: '',
                            name: 'asdfasf',
                            provided: false,
                            sensitive: false
                        },
                        {
                            description: '',
                            name: 'asfasdfasdfasdf',
                            provided: false,
                            sensitive: false,
                            value: 'asdfasdfasfd'
                        },
                        {
                            description: '',
                            name: 'hello',
                            provided: false,
                            sensitive: false,
                            value: 'Yo Yo'
                        },
                        {
                            description: '',
                            name: 'Bearer Token',
                            provided: false,
                            sensitive: true
                        },
                        {
                            description: '',
                            name: 'Record Writer',
                            provided: false,
                            sensitive: false,
                            value: '268248a8-0193-1000-6122-15e198fc48dd'
                        }
                    ]
                },
                'Default Params': {
                    componentType: 'PARAMETER_CONTEXT',
                    inheritedParameterContexts: [],
                    name: 'Default Params',
                    parameters: [
                        {
                            description: '',
                            name: 'Transform Cache Size',
                            provided: false,
                            sensitive: false,
                            value: '10 kb'
                        },
                        {
                            description: '',
                            name: 'Batch Size',
                            provided: false,
                            sensitive: false
                        },
                        {
                            description: '',
                            name: 'Github Personal Access Token',
                            provided: false,
                            sensitive: true
                        },
                        {
                            description: 'test',
                            name: 'test',
                            provided: false,
                            sensitive: false
                        },
                        {
                            description: '',
                            name: 'bytes',
                            provided: false,
                            sensitive: false,
                            value: '15 B'
                        },
                        {
                            description: '',
                            name: 'Cache Size',
                            provided: false,
                            sensitive: false,
                            value: '1000'
                        },
                        {
                            description: '',
                            name: 'hello',
                            provided: false,
                            sensitive: false,
                            value: 'Hello!'
                        },
                        {
                            description: '',
                            name: 'my_pwd',
                            provided: false,
                            sensitive: true
                        },
                        {
                            description: '',
                            name: 'Data Format',
                            provided: false,
                            sensitive: false,
                            value: 'Text'
                        }
                    ]
                }
            },
            parameterProviders: {},
            snapshotMetadata: {
                author: 'rfellows',
                flowIdentifier: 'test-version-stuff',
                timestamp: 0
            }
        };
    });
});
