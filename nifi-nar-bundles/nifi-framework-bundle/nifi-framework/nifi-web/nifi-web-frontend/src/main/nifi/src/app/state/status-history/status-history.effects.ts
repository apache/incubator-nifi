/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { Store } from '@ngrx/store';
import { NiFiState } from '../index';
import { StatusHistoryService } from '../../service/status-history.service';
import * as StatusHistoryActions from './status-history.actions';
import { StatusHistoryRequest } from './index';
import { catchError, from, map, of, switchMap, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { StatusHistory } from '../../ui/common/status-history/status-history.component';
import { ComponentType } from '../shared';

@Injectable()
export class StatusHistoryEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private statusHistoryService: StatusHistoryService,
        private dialog: MatDialog
    ) {}

    loadStatusHistory$ = createEffect(() =>
        this.actions$.pipe(
            ofType(StatusHistoryActions.loadStatusHistory),
            map((action) => action.request),
            switchMap((request: StatusHistoryRequest) =>
                from(
                    this.statusHistoryService
                        .getProcessorStatusHistory(request.componentType, request.componentId)
                        .pipe(
                            map((response: any) =>
                                StatusHistoryActions.loadStatusHistorySuccess({
                                    response: {
                                        statusHistory: {
                                            canRead: response.canRead,
                                            statusHistory: response.statusHistory
                                        }
                                    }
                                })
                            ),
                            catchError((error) =>
                                of(
                                    StatusHistoryActions.statusHistoryApiError({
                                        error: error.error
                                    })
                                )
                            )
                        )
                )
            )
        )
    );

    openStatusHistoryDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(StatusHistoryActions.openStatusHistoryDialog),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(StatusHistory, {
                        data: request,
                        panelClass: 'large-dialog'
                    });

                    dialogReference.afterClosed().subscribe((response) => {
                        if (response !== 'ROUTED') {
                            this.store.dispatch(
                                StatusHistoryActions.viewStatusHistoryComplete({
                                    request: {
                                        source: request.source,
                                        componentType: request.componentType,
                                        componentId: request.componentId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );
}
