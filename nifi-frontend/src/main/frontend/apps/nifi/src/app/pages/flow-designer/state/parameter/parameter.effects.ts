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
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { concatLatestFrom } from '@ngrx/operators';
import * as ParameterActions from './parameter.actions';
import { Store } from '@ngrx/store';
import { CanvasState } from '../index';
import {
    asyncScheduler,
    catchError,
    filter,
    from,
    interval,
    map,
    Observable,
    of,
    switchMap,
    take,
    takeUntil,
    tap
} from 'rxjs';
import { isDefinedAndNotNull, MEDIUM_DIALOG, NiFiCommon, Parameter, Storage, XL_DIALOG } from '@nifi/shared';
import { EditParameterRequest, EditParameterResponse, ParameterContextUpdateRequest } from '../../../../state/shared';
import { selectSaving, selectUpdateRequest } from './parameter.selectors';
import { ParameterService } from '../../service/parameter.service';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { MatDialog, MatDialogRef } from '@angular/material/dialog';
import { EditParameterContext } from '../../../../ui/common/parameter-context/edit-parameter-context/edit-parameter-context.component';
import { ParameterContextService } from '../../../parameter-contexts/service/parameter-contexts.service';
import { EditParameterDialog } from '../../../../ui/common/edit-parameter-dialog/edit-parameter-dialog.component';
import * as ErrorActions from '../../../../state/error/error.actions';
import { ErrorContextKey } from '../../../../state/error';

@Injectable()
export class ParameterEffects {
    private parameterContextDialogRef: MatDialogRef<EditParameterContext, any> | undefined;

    constructor(
        private actions$: Actions,
        private store: Store<CanvasState>,
        private parameterService: ParameterService,
        private parameterContextService: ParameterContextService,
        private errorHelper: ErrorHelper,
        private storage: Storage,
        private dialog: MatDialog
    ) {}

    submitParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.submitParameterContextUpdateRequest),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterService.submitParameterContextUpdate(request)).pipe(
                    map((response) =>
                        ParameterActions.submitParameterContextUpdateRequestSuccess({
                            response: {
                                requestEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ParameterActions.parameterApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    submitParameterContextUpdateRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.submitParameterContextUpdateRequestSuccess),
            map((action) => action.response),
            switchMap((response) => {
                const updateRequest: ParameterContextUpdateRequest = response.requestEntity.request;
                if (updateRequest.complete) {
                    return of(ParameterActions.deleteParameterContextUpdateRequest());
                } else {
                    return of(ParameterActions.startPollingParameterContextUpdateRequest());
                }
            })
        )
    );

    startPollingParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.startPollingParameterContextUpdateRequest),
            switchMap(() =>
                interval(2000, asyncScheduler).pipe(
                    takeUntil(this.actions$.pipe(ofType(ParameterActions.stopPollingParameterContextUpdateRequest)))
                )
            ),
            switchMap(() => of(ParameterActions.pollParameterContextUpdateRequest()))
        )
    );

    pollParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.pollParameterContextUpdateRequest),
            concatLatestFrom(() => this.store.select(selectUpdateRequest).pipe(isDefinedAndNotNull())),
            switchMap(([, updateRequest]) =>
                from(this.parameterService.pollParameterContextUpdate(updateRequest.request)).pipe(
                    map((response) =>
                        ParameterActions.pollParameterContextUpdateRequestSuccess({
                            response: {
                                requestEntity: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(
                            ParameterActions.parameterApiError({
                                error: this.errorHelper.getErrorString(errorResponse)
                            })
                        )
                    )
                )
            )
        )
    );

    pollParameterContextUpdateRequestSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.pollParameterContextUpdateRequestSuccess),
            map((action) => action.response),
            filter((response) => response.requestEntity.request.complete),
            switchMap(() => of(ParameterActions.stopPollingParameterContextUpdateRequest()))
        )
    );

    stopPollingParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.stopPollingParameterContextUpdateRequest),
            switchMap(() => of(ParameterActions.deleteParameterContextUpdateRequest()))
        )
    );

    deleteParameterContextUpdateRequest$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.deleteParameterContextUpdateRequest),
            concatLatestFrom(() => this.store.select(selectUpdateRequest)),
            switchMap(([, updateRequest]) => {
                if (updateRequest) {
                    return from(this.parameterService.deleteParameterContextUpdate(updateRequest.request)).pipe(
                        map(() => ParameterActions.editParameterContextComplete()),
                        catchError((errorResponse: HttpErrorResponse) =>
                            of(
                                ParameterActions.parameterApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            )
                        )
                    );
                } else {
                    return of(ParameterActions.editParameterContextComplete());
                }
            })
        )
    );

    openNewParameterContextDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterActions.openNewParameterContextDialog),
                tap((action) => {
                    this.storage.setItem<number>(NiFiCommon.EDIT_PARAMETER_CONTEXT_DIALOG_ID, 0);

                    this.parameterContextDialogRef = this.dialog.open(EditParameterContext, {
                        ...XL_DIALOG,
                        data: {}
                    });

                    this.parameterContextDialogRef.componentInstance.availableParameterContexts$ = of(
                        action.request.parameterContexts
                    );
                    this.parameterContextDialogRef.componentInstance.saving$ = this.store.select(selectSaving);

                    this.parameterContextDialogRef.componentInstance.createNewParameter = (
                        existingParameters: string[]
                    ): Observable<EditParameterResponse> => {
                        const dialogRequest: EditParameterRequest = { existingParameters, isNewParameterContext: true };
                        const newParameterDialogReference = this.dialog.open(EditParameterDialog, {
                            ...MEDIUM_DIALOG,
                            data: dialogRequest
                        });

                        newParameterDialogReference.componentInstance.saving$ = of(false);

                        return newParameterDialogReference.componentInstance.editParameter.pipe(
                            take(1),
                            map((dialogResponse: EditParameterResponse) => {
                                newParameterDialogReference.close();

                                return {
                                    ...dialogResponse
                                };
                            })
                        );
                    };

                    this.parameterContextDialogRef.componentInstance.editParameter = (
                        parameter: Parameter
                    ): Observable<EditParameterResponse> => {
                        const dialogRequest: EditParameterRequest = {
                            parameter: {
                                ...parameter
                            },
                            isNewParameterContext: true
                        };
                        const editParameterDialogReference = this.dialog.open(EditParameterDialog, {
                            ...MEDIUM_DIALOG,
                            data: dialogRequest
                        });

                        editParameterDialogReference.componentInstance.saving$ = of(false);

                        return editParameterDialogReference.componentInstance.editParameter.pipe(
                            take(1),
                            map((dialogResponse: EditParameterResponse) => {
                                editParameterDialogReference.close();

                                return {
                                    ...dialogResponse
                                };
                            })
                        );
                    };

                    this.parameterContextDialogRef.componentInstance.addParameterContext
                        .pipe(takeUntil(this.parameterContextDialogRef.afterClosed()))
                        .subscribe((payload: any) => {
                            this.store.dispatch(
                                ParameterActions.createParameterContext({
                                    request: { payload }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    createParameterContext$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.createParameterContext),
            map((action) => action.request),
            switchMap((request) =>
                from(this.parameterContextService.createParameterContext(request)).pipe(
                    map((response) =>
                        ParameterActions.createParameterContextSuccess({
                            response: {
                                parameterContext: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                ParameterActions.parameterContextBannerApiError({
                                    error: this.errorHelper.getErrorString(errorResponse)
                                })
                            );
                        } else {
                            this.dialog.closeAll();
                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                )
            )
        )
    );

    createParameterContextSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ParameterActions.createParameterContextSuccess),
                tap(() => {
                    this.parameterContextDialogRef?.close();
                })
            ),
        { dispatch: false }
    );

    parameterContextSnackbarApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.parameterContextSnackbarApiError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.snackBarError({ error })))
        )
    );

    parameterContextBannerApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ParameterActions.parameterContextBannerApiError),
            map((action) => action.error),
            switchMap((error) =>
                of(
                    ErrorActions.addBannerError({
                        errorContext: { errors: [error], context: ErrorContextKey.PARAMETER_CONTEXTS }
                    })
                )
            )
        )
    );
}
