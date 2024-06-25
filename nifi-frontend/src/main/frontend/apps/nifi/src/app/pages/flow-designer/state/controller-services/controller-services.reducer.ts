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

import { createReducer, on } from '@ngrx/store';
import {
    configureControllerService,
    configureControllerServiceSuccess,
    moveControllerServiceSuccess,
    controllerServicesBannerApiError,
    createControllerService,
    createControllerServiceSuccess,
    deleteControllerService,
    deleteControllerServiceSuccess,
    inlineCreateControllerServiceSuccess,
    loadControllerServices,
    loadControllerServicesSuccess,
    resetControllerServicesState
} from './controller-services.actions';
import { produce } from 'immer';
import { ControllerServicesState } from './index';

export const initialState: ControllerServicesState = {
    processGroupId: 'root',
    controllerServices: [],
    breadcrumb: {
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
    },
    parameterContext: null,
    saving: false,
    loadedTimestamp: '',
    status: 'pending'
};

export const controllerServicesReducer = createReducer(
    initialState,
    on(resetControllerServicesState, () => ({
        ...initialState
    })),
    on(loadControllerServices, (state) => ({
        ...state,
        status: 'loading' as const
    })),
    on(loadControllerServicesSuccess, (state, { response }) => ({
        ...state,
        processGroupId: response.processGroupId,
        controllerServices: response.controllerServices,
        breadcrumb: response.breadcrumb,
        parameterContext: response.parameterContext,
        loadedTimestamp: response.loadedTimestamp,
        processGroupFlow: response.processGroupFlow,
        status: 'success' as const
    })),
    on(controllerServicesBannerApiError, (state) => ({
        ...state,
        saving: false
    })),
    on(createControllerService, configureControllerService, deleteControllerService, (state) => ({
        ...state,
        saving: true
    })),
    on(createControllerServiceSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.controllerServices.push(response.controllerService);
            draftState.saving = false;
        });
    }),
    on(inlineCreateControllerServiceSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            draftState.controllerServices.push(response.controllerService);
        });
    }),
    on(configureControllerServiceSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.controllerServices.findIndex((f: any) => response.id === f.id);
            if (componentIndex > -1) {
                draftState.controllerServices[componentIndex] = response.controllerService;
            }
            draftState.saving = false;
        });
    }),
    on(moveControllerServiceSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.controllerServices.findIndex(
                (f: any) => response.controllerService.id === f.id
            );
            if (componentIndex > -1) {
                draftState.controllerServices.splice(componentIndex, 1);
            }
            draftState.saving = false;
        });
    }),
    on(deleteControllerServiceSuccess, (state, { response }) => {
        return produce(state, (draftState) => {
            const componentIndex: number = draftState.controllerServices.findIndex(
                (f: any) => response.controllerService.id === f.id
            );
            if (componentIndex > -1) {
                draftState.controllerServices.splice(componentIndex, 1);
            }
            draftState.saving = false;
        });
    })
);
