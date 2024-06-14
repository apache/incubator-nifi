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

import { MoveControllerService } from './move-controller-service.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { provideMockStore } from '@ngrx/store/testing';
import { initialState } from '../../../../state/contoller-service-state/controller-service-state.reducer';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { ComponentType, MoveControllerServiceDialogRequest } from '../../../../state/shared';

describe('MoveControllerService', () => {
    let component: MoveControllerService;
    let fixture: ComponentFixture<MoveControllerService>;
    console.log("describe() MoveControllerService");
    const data: MoveControllerServiceDialogRequest = {
        id: '92db6ee4-018c-1000-1061-e3476c3f4e9f',
        processGroupFlow: null,
        controllerService: {
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
        }
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
