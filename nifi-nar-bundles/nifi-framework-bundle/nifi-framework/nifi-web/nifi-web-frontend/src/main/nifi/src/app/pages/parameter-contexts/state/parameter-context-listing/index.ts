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

import {
    ParameterContextReferenceEntity,
    ParameterContextUpdateRequestEntity,
    ParameterEntity,
    ParameterProviderConfigurationEntity,
    Permissions,
    Revision
} from '../../../../state/shared';

export const parameterContextListingFeatureKey = 'parameterContextListing';

export interface LoadParameterContextsResponse {
    parameterContexts: ParameterContextEntity[];
    loadedTimestamp: string;
}

export interface CreateParameterContextRequest {
    payload: any;
}

export interface CreateParameterContextSuccess {
    parameterContext: ParameterContextEntity;
}

export interface GetEffectiveParameterContext {
    id: string;
}

export interface EditParameterContextRequest {
    parameterContext?: ParameterContextEntity;
}

export interface DeleteParameterContextRequest {
    parameterContext: ParameterContextEntity;
}

export interface DeleteParameterContextSuccess {
    parameterContext: ParameterContextEntity;
}

export interface SelectParameterContextRequest {
    id: string;
}

export interface ParameterContextEntity {
    revision: Revision;
    permissions: Permissions;
    id: string;
    uri: string;
    component: ParameterContext;
}

export interface ParameterContext {
    id: string;
    name: string;
    description: string;
    parameters: ParameterEntity[];
    boundProcessGroups: BoundProcessGroup[];
    inheritedParameterContexts: ParameterContextReferenceEntity[];
    parameterProviderConfiguration?: ParameterProviderConfigurationEntity;
}

// TODO - Replace this with ProcessGroupEntity was available
export interface BoundProcessGroup {
    permissions: Permissions;
    id: string;
    component: any;
}

export interface ParameterContextListingState {
    parameterContexts: ParameterContextEntity[];
    updateRequestEntity: ParameterContextUpdateRequestEntity | null;
    saving: boolean;
    loadedTimestamp: string;
    status: 'pending' | 'loading' | 'success';
}
