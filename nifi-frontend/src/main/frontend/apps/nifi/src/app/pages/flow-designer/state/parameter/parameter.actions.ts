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
import { PollParameterContextUpdateSuccess, SubmitParameterContextUpdate } from '../../../../state/shared';
import {
    CreateParameterContextRequest,
    CreateParameterContextSuccess,
    OpenCreateParameterContextRequest
} from '../../../../ui/common/parameter-context';

export const parameterApiError = createAction('[Parameter] Parameter Error', props<{ error: string }>());

export const submitParameterContextUpdateRequest = createAction(
    '[Parameter] Submit Parameter Context Update Request',
    props<{ request: SubmitParameterContextUpdate }>()
);

export const submitParameterContextUpdateRequestSuccess = createAction(
    '[Parameter] Submit Parameter Context Update Request Success',
    props<{ response: PollParameterContextUpdateSuccess }>()
);

export const startPollingParameterContextUpdateRequest = createAction(
    '[Parameter] Start Polling Parameter Context Update Request'
);

export const pollParameterContextUpdateRequest = createAction('[Parameter] Poll Parameter Context Update Request');

export const pollParameterContextUpdateRequestSuccess = createAction(
    '[Parameter] Poll Parameter Context Update Request Success',
    props<{ response: PollParameterContextUpdateSuccess }>()
);

export const stopPollingParameterContextUpdateRequest = createAction(
    '[Parameter] Stop Polling Parameter Context Update Request'
);

export const deleteParameterContextUpdateRequest = createAction('[Parameter] Delete Parameter Context Update Request');

export const editParameterContextComplete = createAction('[Parameter] Edit Parameter Context Complete');

export const openNewParameterContextDialog = createAction(
    '[Parameter Context] Open New Parameter Context Dialog',
    props<{ request: OpenCreateParameterContextRequest }>()
);

export const createParameterContext = createAction(
    '[Parameter Context] Create Parameter Context',
    props<{ request: CreateParameterContextRequest }>()
);

export const createParameterContextSuccess = createAction(
    '[Parameter Context] Create Parameter Context Success',
    props<{ response: CreateParameterContextSuccess }>()
);

export const parameterContextSnackbarApiError = createAction(
    '[Parameter Context] Parameter Context Snackbar Api Error',
    props<{ error: string }>()
);

export const parameterContextBannerApiError = createAction(
    '[Parameter Context] Parameter Context Banner Api Error',
    props<{ error: string }>()
);
