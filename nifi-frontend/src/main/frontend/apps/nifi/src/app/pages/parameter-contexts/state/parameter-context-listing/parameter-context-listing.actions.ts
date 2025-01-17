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
import { LoadParameterContextsResponse, SelectParameterContextRequest, GetEffectiveParameterContext } from './index';
import { PollParameterContextUpdateSuccess, SubmitParameterContextUpdate } from '../../../../state/shared';
import {
    CreateParameterContextRequest,
    CreateParameterContextSuccess,
    DeleteParameterContextRequest,
    DeleteParameterContextSuccess,
    EditParameterContextRequest
} from '../../../../ui/common/parameter-context';

export const loadParameterContexts = createAction('[Parameter Context Listing] Load Parameter Contexts');

export const loadParameterContextsSuccess = createAction(
    '[Parameter Context Listing] Load Parameter Contexts Success',
    props<{ response: LoadParameterContextsResponse }>()
);

export const parameterContextListingSnackbarApiError = createAction(
    '[Parameter Context Listing] Load Parameter Context Listing Snackbar Api Error',
    props<{ error: string }>()
);

export const parameterContextListingBannerApiError = createAction(
    '[Parameter Context Listing] Load Parameter Context Listing Banner Api Error',
    props<{ error: string }>()
);

export const openNewParameterContextDialog = createAction(
    '[Parameter Context Listing] Open New Parameter Context Dialog'
);

export const createParameterContext = createAction(
    '[Parameter Context Listing] Create Parameter Context',
    props<{ request: CreateParameterContextRequest }>()
);

export const createParameterContextSuccess = createAction(
    '[Parameter Context Listing] Create Parameter Context Success',
    props<{ response: CreateParameterContextSuccess }>()
);

export const navigateToEditParameterContext = createAction(
    '[Parameter Context Listing] Navigate To Edit Parameter Context',
    props<{ id: string }>()
);

export const navigateToManageComponentPolicies = createAction(
    '[Parameter Context Listing] Navigate To Manage Component Policies',
    props<{ id: string }>()
);

export const getEffectiveParameterContextAndOpenDialog = createAction(
    '[Parameter Context Listing] Get Effective Parameter Context Open Dialog',
    props<{ request: GetEffectiveParameterContext }>()
);

export const openParameterContextDialog = createAction(
    '[Parameter Context Listing] Open Configure Parameter Context Dialog',
    props<{ request: EditParameterContextRequest }>()
);

export const submitParameterContextUpdateRequest = createAction(
    '[Parameter Context Listing] Submit Parameter Context Update Request',
    props<{ request: SubmitParameterContextUpdate }>()
);

export const submitParameterContextUpdateRequestSuccess = createAction(
    '[Parameter Context Listing] Submit Parameter Context Update Request Success',
    props<{ response: PollParameterContextUpdateSuccess }>()
);

export const startPollingParameterContextUpdateRequest = createAction(
    '[Parameter Context Listing] Start Polling Parameter Context Update Request'
);

export const pollParameterContextUpdateRequest = createAction(
    '[Parameter Context Listing] Poll Parameter Context Update Request'
);

export const pollParameterContextUpdateRequestSuccess = createAction(
    '[Parameter Context Listing] Poll Parameter Context Update Request Success',
    props<{ response: PollParameterContextUpdateSuccess }>()
);

export const stopPollingParameterContextUpdateRequest = createAction(
    '[Parameter Context Listing] Stop Polling Parameter Context Update Request'
);

export const deleteParameterContextUpdateRequest = createAction(
    '[Parameter Context Listing] Delete Parameter Context Update Request'
);

export const deleteParameterContextUpdateRequestSuccess = createAction(
    '[Parameter Context Listing] Delete Parameter Context Update Request Success',
    props<{ response: PollParameterContextUpdateSuccess }>()
);

export const editParameterContextComplete = createAction('[Parameter Context Listing] Edit Parameter Context Complete');

export const promptParameterContextDeletion = createAction(
    '[Parameter Context Listing] Prompt Parameter Context Deletion',
    props<{ request: DeleteParameterContextRequest }>()
);

export const deleteParameterContext = createAction(
    '[Parameter Context Listing] Delete Parameter Context',
    props<{ request: DeleteParameterContextRequest }>()
);

export const deleteParameterContextSuccess = createAction(
    '[Parameter Context Listing] Delete Parameter Context Success',
    props<{ response: DeleteParameterContextSuccess }>()
);

export const selectParameterContext = createAction(
    '[Parameter Context Listing] Select Parameter Context',
    props<{ request: SelectParameterContextRequest }>()
);

export const showOkDialog = createAction(
    '[Parameter Context Listing] Show Ok Dialog',
    props<{ title: string; message: string }>()
);
