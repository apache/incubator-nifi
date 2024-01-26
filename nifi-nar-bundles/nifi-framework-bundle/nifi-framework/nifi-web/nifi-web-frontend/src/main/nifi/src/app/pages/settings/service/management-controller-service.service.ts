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

import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import {
    ConfigureControllerServiceRequest,
    DeleteControllerServiceRequest
} from '../state/management-controller-services';
import { Client } from '../../../service/client.service';
import { NiFiCommon } from '../../../service/nifi-common.service';
import {
    ControllerServiceCreator,
    ControllerServiceEntity,
    CreateControllerServiceRequest,
    PropertyDescriptorRetriever
} from '../../../state/shared';

@Injectable({ providedIn: 'root' })
export class ManagementControllerServiceService implements ControllerServiceCreator, PropertyDescriptorRetriever {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private nifiCommon: NiFiCommon
    ) {}

    getControllerServices(): Observable<any> {
        const uiOnly: any = { uiOnly: true };
        return this.httpClient.get(`${ManagementControllerServiceService.API}/flow/controller/controller-services`, {
            params: uiOnly
        });
    }

    createControllerService(createControllerService: CreateControllerServiceRequest): Observable<any> {
        return this.httpClient.post(`${ManagementControllerServiceService.API}/controller/controller-services`, {
            revision: createControllerService.revision,
            component: {
                bundle: createControllerService.controllerServiceBundle,
                type: createControllerService.controllerServiceType
            }
        });
    }

    getPropertyDescriptor(id: string, propertyName: string, sensitive: boolean): Observable<any> {
        const params: any = {
            propertyName,
            sensitive
        };
        return this.httpClient.get(`${ManagementControllerServiceService.API}/controller-services/${id}/descriptors`, {
            params
        });
    }

    updateControllerService(configureControllerService: ConfigureControllerServiceRequest): Observable<any> {
        return this.httpClient.put(
            this.nifiCommon.stripProtocol(configureControllerService.uri),
            configureControllerService.payload
        );
    }

    deleteControllerService(deleteControllerService: DeleteControllerServiceRequest): Observable<any> {
        const entity: ControllerServiceEntity = deleteControllerService.controllerService;
        const revision: any = this.client.getRevision(entity);
        return this.httpClient.delete(this.nifiCommon.stripProtocol(entity.uri), { params: revision });
    }
}
