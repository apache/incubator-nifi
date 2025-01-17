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
import { MatDialog } from '@angular/material/dialog';
import { catchError, EMPTY, filter, map, Observable, switchMap, takeUntil, tap } from 'rxjs';
import { Store } from '@ngrx/store';
import { HttpErrorResponse } from '@angular/common/http';
import { NiFiState } from '../../../state';
import { Client } from '../../../service/client.service';
import { EditParameterRequest, EditParameterResponse, ParameterContext, ParameterEntity } from '../../../state/shared';
import { EditParameterDialog } from '../../../ui/common/edit-parameter-dialog/edit-parameter-dialog.component';
import { selectParameterSaving, selectParameterState } from '../state/parameter/parameter.selectors';
import { ParameterState } from '../state/parameter';
import * as ErrorActions from '../../../state/error/error.actions';
import * as ParameterActions from '../state/parameter/parameter.actions';
import { MEDIUM_DIALOG } from '@nifi/shared';
import { ClusterConnectionService } from '../../../service/cluster-connection.service';
import { ErrorHelper } from '../../../service/error-helper.service';
import { ParameterContextService } from '../../parameter-contexts/service/parameter-contexts.service';

export interface ConvertToParameterResponse {
    propertyValue: string;
    parameterContext?: ParameterContext;
}

@Injectable({
    providedIn: 'root'
})
export class ParameterHelperService {
    constructor(
        private dialog: MatDialog,
        private store: Store<NiFiState>,
        private parameterContextService: ParameterContextService,
        private clusterConnectionService: ClusterConnectionService,
        private client: Client,
        private errorHelper: ErrorHelper
    ) {}

    /**
     * Returns a function that can be used to pass into a PropertyTable to convert a Property into a Parameter, inline.
     *
     * @param parameterContextId the current Parameter Context id
     */
    convertToParameter(
        parameterContextId: string
    ): (name: string, sensitive: boolean, value: string | null) => Observable<ConvertToParameterResponse> {
        return (name: string, sensitive: boolean, value: string | null) => {
            return this.parameterContextService.getParameterContext(parameterContextId, false).pipe(
                catchError((errorResponse: HttpErrorResponse) => {
                    this.store.dispatch(
                        ErrorActions.snackBarError({ error: this.errorHelper.getErrorString(errorResponse) })
                    );

                    // consider the error handled and allow the user to reattempt the action
                    return EMPTY;
                }),
                switchMap((parameterContextEntity) => {
                    const existingParameters: string[] = parameterContextEntity.component.parameters.map(
                        (parameterEntity: ParameterEntity) => parameterEntity.parameter.name
                    );
                    const convertToParameterDialogRequest: EditParameterRequest = {
                        parameter: {
                            name,
                            value,
                            sensitive,
                            description: ''
                        },
                        existingParameters,
                        isNewParameterContext: false,
                        isConvert: true
                    };
                    const convertToParameterDialogReference = this.dialog.open(EditParameterDialog, {
                        ...MEDIUM_DIALOG,
                        data: convertToParameterDialogRequest
                    });

                    convertToParameterDialogReference.componentInstance.saving$ =
                        this.store.select(selectParameterSaving);

                    convertToParameterDialogReference.componentInstance.cancel.pipe(
                        takeUntil(convertToParameterDialogReference.afterClosed()),
                        tap(() => ParameterActions.stopPollingParameterContextUpdateRequest())
                    );

                    return convertToParameterDialogReference.componentInstance.editParameter.pipe(
                        takeUntil(convertToParameterDialogReference.afterClosed()),
                        switchMap((dialogResponse: EditParameterResponse) => {
                            this.store.dispatch(
                                ParameterActions.submitParameterContextUpdateRequest({
                                    request: {
                                        id: parameterContextId,
                                        payload: {
                                            id: parameterContextEntity.id,
                                            revision: this.client.getRevision(parameterContextEntity),
                                            disconnectedNodeAcknowledged:
                                                this.clusterConnectionService.isDisconnectionAcknowledged(),
                                            component: {
                                                id: parameterContextEntity.id,
                                                parameters: [{ parameter: dialogResponse.parameter }],
                                                inheritedParameterContexts:
                                                    parameterContextEntity.component.inheritedParameterContexts
                                            }
                                        }
                                    }
                                })
                            );

                            let parameterContext: ParameterContext;

                            return this.store.select(selectParameterState).pipe(
                                takeUntil(convertToParameterDialogReference.afterClosed()),
                                tap((parameterState: ParameterState) => {
                                    if (parameterState.error) {
                                        // if the convert to parameter sequence stores an error,
                                        // throw it to avoid the completion mapping logic below
                                        throw new Error(parameterState.error);
                                    } else if (parameterState.updateRequestEntity?.request.failureReason) {
                                        // if the convert to parameter sequence completes successfully
                                        // with an error, throw the message
                                        throw new Error(parameterState.updateRequestEntity?.request.failureReason);
                                    }

                                    if (parameterState.saving) {
                                        parameterContext = parameterState.updateRequestEntity?.request.parameterContext;
                                    }
                                }),
                                filter((parameterState) => !parameterState.saving),
                                map(() => {
                                    convertToParameterDialogReference.close();

                                    return {
                                        propertyValue: `#{${dialogResponse.parameter.name}}`,
                                        parameterContext
                                    } as ConvertToParameterResponse;
                                }),
                                catchError((error) => {
                                    convertToParameterDialogReference.close();

                                    // show the error in the snack and complete the edit to reset it's state
                                    this.store.dispatch(ErrorActions.snackBarError({ error: error.message }));
                                    this.store.dispatch(ParameterActions.editParameterContextComplete());

                                    // consider the error handled and allow the user to reattempt the action
                                    return EMPTY;
                                })
                            );
                        })
                    );
                })
            );
        };
    }
}
