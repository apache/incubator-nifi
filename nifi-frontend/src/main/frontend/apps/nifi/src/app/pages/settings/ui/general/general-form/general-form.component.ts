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

import { Component, Input } from '@angular/core';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { ControllerEntity, GeneralState, UpdateControllerConfigRequest } from '../../../state/general';
import { Store } from '@ngrx/store';
import { updateControllerConfig } from '../../../state/general/general.actions';
import { Client } from '../../../../../service/client.service';
import { selectCurrentUser } from '../../../../../state/current-user/current-user.selectors';
import { selectSaving } from '../../../state/general/general.selectors';
import { ClusterConnectionService } from '../../../../../service/cluster-connection.service';
import { NifiTooltipDirective, TextTip } from '@nifi/shared';
import { AsyncPipe } from '@angular/common';
import { MatFormField } from '@angular/material/form-field';
import { MatLabel } from '@angular/material/select';
import { MatInput } from '@angular/material/input';
import { MatButton } from '@angular/material/button';

@Component({
    selector: 'general-form',
    templateUrl: './general-form.component.html',
    imports: [AsyncPipe, ReactiveFormsModule, MatFormField, MatLabel, NifiTooltipDirective, MatInput, MatButton],
    styleUrls: ['./general-form.component.scss']
})
export class GeneralForm {
    private _controller!: ControllerEntity;

    @Input() set controller(controller: ControllerEntity) {
        this._controller = controller;
        this.controllerForm.get('timerDrivenThreadCount')?.setValue(controller.component.maxTimerDrivenThreadCount);
    }

    saving$ = this.store.select(selectSaving);
    currentUser$ = this.store.select(selectCurrentUser);
    controllerForm: FormGroup;

    constructor(
        private formBuilder: FormBuilder,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService,
        private store: Store<GeneralState>
    ) {
        // build the form
        this.controllerForm = this.formBuilder.group({
            timerDrivenThreadCount: new FormControl('', Validators.required)
        });
    }

    apply(): void {
        const payload: UpdateControllerConfigRequest = {
            controller: {
                revision: this.client.getRevision(this._controller),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                component: {
                    maxTimerDrivenThreadCount: this.controllerForm.get('timerDrivenThreadCount')?.value
                }
            }
        };

        this.store.dispatch(updateControllerConfig({ request: payload }));
    }

    protected readonly TextTip = TextTip;
}
