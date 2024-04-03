/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Client } from '../../../service/client.service';
import { NiFiCommon } from '../../../service/nifi-common.service';
import { Observable } from 'rxjs';
import {
    ConfigureParameterProviderRequest,
    CreateParameterProviderRequest,
    DeleteParameterProviderRequest,
    FetchParameterProviderParametersRequest,
    ParameterProviderApplyParametersRequest,
    ParameterProviderEntity,
    ParameterProviderParameterApplicationEntity
} from '../state/parameter-providers';
import { PropertyDescriptorRetriever } from '../../../state/shared';

@Injectable({ providedIn: 'root' })
export class ParameterProviderService implements PropertyDescriptorRetriever {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private nifiCommon: NiFiCommon
    ) {}

    getParameterProviders(): Observable<any> {
        return this.httpClient.get(`${ParameterProviderService.API}/flow/parameter-providers`);
    }

    getParameterProvider(id: string): Observable<any> {
        return this.httpClient.get(`${ParameterProviderService.API}/parameter-providers/${id}`);
    }

    createParameterProvider(request: CreateParameterProviderRequest) {
        return this.httpClient.post(`${ParameterProviderService.API}/controller/parameter-providers`, {
            revision: request.revision,
            component: {
                bundle: request.parameterProviderBundle,
                type: request.parameterProviderType
            }
        });
    }

    deleteParameterProvider(request: DeleteParameterProviderRequest) {
        const entity: ParameterProviderEntity = request.parameterProvider;
        const revision: any = this.client.getRevision(entity);
        const params: any = {
            ...revision,
            disconnectedNodeAcknowledged: false
        };
        return this.httpClient.delete(this.nifiCommon.stripProtocol(entity.uri), { params });
    }

    getPropertyDescriptor(id: string, propertyName: string, sensitive: boolean): Observable<any> {
        const params: any = {
            propertyName,
            sensitive
        };
        return this.httpClient.get(`${ParameterProviderService.API}/parameter-providers/${id}/descriptors`, {
            params
        });
    }

    updateParameterProvider(configureRequest: ConfigureParameterProviderRequest): Observable<any> {
        return this.httpClient.put(this.nifiCommon.stripProtocol(configureRequest.uri), configureRequest.payload);
    }

    fetchParameters(request: FetchParameterProviderParametersRequest): Observable<any> {
        return this.httpClient.post(
            `${ParameterProviderService.API}/parameter-providers/${request.id}/parameters/fetch-requests`,
            {
                id: request.id,
                revision: request.revision
            },
            { params: { disconnectedNodeAcknowledged: false } }
        );
    }

    applyParameters(request: ParameterProviderParameterApplicationEntity): Observable<any> {
        return this.httpClient.post(
            `${ParameterProviderService.API}/parameter-providers/${request.id}/apply-parameters-requests`,
            request
        );
    }

    pollParameterProviderParametersUpdateRequest(
        updateRequest: ParameterProviderApplyParametersRequest
    ): Observable<any> {
        return this.httpClient.get(this.nifiCommon.stripProtocol(updateRequest.uri));
    }

    deleteParameterProviderParametersUpdateRequest(
        updateRequest: ParameterProviderApplyParametersRequest
    ): Observable<any> {
        return this.httpClient.delete(this.nifiCommon.stripProtocol(updateRequest.uri));
    }
}
