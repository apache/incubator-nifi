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

import { createAction, props } from '@ngrx/store';
import {
    ConfigureControllerServiceRequest,
    ConfigureControllerServiceSuccess,
    CreateControllerServiceSuccess,
    MoveControllerServiceDialogRequest,
    MoveControllerServiceRequest,
    MoveControllerServiceSuccess,
    DeleteControllerServiceRequest,
    DeleteControllerServiceSuccess,
    LoadControllerServicesRequest,
    LoadControllerServicesResponse,
    SelectControllerServiceRequest
} from './index';
import {
    CreateControllerServiceRequest,
    DisableControllerServiceDialogRequest,
    EditControllerServiceDialogRequest,
    FetchComponentVersionsRequest,
    SetEnableControllerServiceDialogRequest
} from '../../../../state/shared';

export const resetControllerServicesState = createAction('[Controller Services] Reset Controller Services State');

export const loadControllerServices = createAction(
    '[Controller Services] Load Controller Services',
    props<{ request: LoadControllerServicesRequest }>()
);

export const loadControllerServicesSuccess = createAction(
    '[Controller Services] Load Controller Services Success',
    props<{ response: LoadControllerServicesResponse }>()
);

export const controllerServicesBannerApiError = createAction(
    '[Controller Services] Controller Services Banner Api Error',
    props<{ error: string }>()
);

export const openNewControllerServiceDialog = createAction('[Controller Services] Open New Controller Service Dialog');

export const createControllerService = createAction(
    '[Controller Services] Create Controller Service',
    props<{ request: CreateControllerServiceRequest }>()
);

export const createControllerServiceSuccess = createAction(
    '[Controller Services] Create Controller Service Success',
    props<{ response: CreateControllerServiceSuccess }>()
);

export const inlineCreateControllerServiceSuccess = createAction(
    '[Controller Services] Inline Create Controller Service Success',
    props<{ response: CreateControllerServiceSuccess }>()
);

export const navigateToService = createAction(
    '[Controller Services] Navigate To Service',
    props<{ request: SelectControllerServiceRequest }>()
);

export const navigateToEditService = createAction(
    '[Controller Services] Navigate To Edit Service',
    props<{ id: string }>()
);

export const navigateToAdvancedServiceUi = createAction(
    '[Controller Services] Navigate To Advanced Service UI',
    props<{ id: string }>()
);

export const navigateToManageComponentPolicies = createAction(
    '[Controller Services] Navigate To Manage Component Policies',
    props<{ id: string }>()
);

export const openConfigureControllerServiceDialog = createAction(
    '[Controller Services] Open Configure Controller Service Dialog',
    props<{ request: EditControllerServiceDialogRequest }>()
);

export const configureControllerService = createAction(
    '[Controller Services] Configure Controller Service',
    props<{ request: ConfigureControllerServiceRequest }>()
);

export const configureControllerServiceSuccess = createAction(
    '[Controller Services] Configure Controller Service Success',
    props<{ response: ConfigureControllerServiceSuccess }>()
);

export const openMoveControllerServiceDialog = createAction(
    '[Controller Services] Open Move Controller Service Dialog',
    props<{ request: MoveControllerServiceDialogRequest }>()
);

export const moveControllerService = createAction(
    '[Controller Services] Move Controller Service',
    props<{ request: MoveControllerServiceRequest }>()
);

export const moveControllerServiceSuccess = createAction(
    '[Controller Services] Move Controller Service Success',
    props<{ response: MoveControllerServiceSuccess }>()
);

export const openEnableControllerServiceDialog = createAction(
    '[Controller Services] Open Enable Controller Service Dialog',
    props<{ request: SetEnableControllerServiceDialogRequest }>()
);

export const openDisableControllerServiceDialog = createAction(
    '[Controller Services] Open Disable Controller Service Dialog',
    props<{ request: DisableControllerServiceDialogRequest }>()
);

export const promptControllerServiceDeletion = createAction(
    '[Controller Services] Prompt Controller Service Deletion',
    props<{ request: DeleteControllerServiceRequest }>()
);

export const deleteControllerService = createAction(
    '[Controller Services] Delete Controller Service',
    props<{ request: DeleteControllerServiceRequest }>()
);

export const deleteControllerServiceSuccess = createAction(
    '[Controller Services] Delete Controller Service Success',
    props<{ response: DeleteControllerServiceSuccess }>()
);

export const selectControllerService = createAction(
    '[Controller Services] Select Controller Service',
    props<{ request: SelectControllerServiceRequest }>()
);

export const openChangeControllerServiceVersionDialog = createAction(
    `[Controller Services] Open Change Controller Service Version Dialog`,
    props<{ request: FetchComponentVersionsRequest }>()
);
