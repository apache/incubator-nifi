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

import { Component, Inject, Input } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';

import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe, NgTemplateOutlet } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { TextTip, NifiTooltipDirective, SelectOption } from '@nifi/shared';
import { Store } from '@ngrx/store';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { NiFiState } from 'apps/nifi/src/app/state';
import { NgIf } from '@angular/common';
import {
    ControllerServiceEntity,
    ControllerServiceReferencingComponent,
    ControllerServiceReferencingComponentEntity
} from 'apps/nifi/src/app/state/shared';
import { ControllerServiceApi } from 'apps/nifi/src/app/ui/common/controller-service/controller-service-api/controller-service-api.component';
import { ControllerServiceReferences } from 'apps/nifi/src/app/ui/common/controller-service/controller-service-references/controller-service-references.component';
import { NifiSpinnerDirective } from 'apps/nifi/src/app/ui/common/spinner/nifi-spinner.directive';
import { MoveControllerServiceDialogRequestSuccess } from '../../../state/controller-services';
import { moveControllerService } from '../../../state/controller-services/controller-services.actions';
import { BreadcrumbEntity } from '../../../state/shared';

@Component({
    selector: 'move-controller-service',
    standalone: true,
    templateUrl: './move-controller-service.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        MatTabsModule,
        MatOptionModule,
        MatSelectModule,
        ControllerServiceApi,
        ControllerServiceReferences,
        AsyncPipe,
        NifiSpinnerDirective,
        NifiTooltipDirective,
        NgTemplateOutlet,
        NgIf
    ],
    styleUrls: ['./move-controller-service.component.scss']
})
export class MoveControllerService extends CloseOnEscapeDialog {
    @Input() goToReferencingComponent!: (component: ControllerServiceReferencingComponent) => void;
    protected readonly TextTip = TextTip;
    protected processGroupOptions: SelectOption[] = [];
    controllerService: ControllerServiceEntity;

    moveControllerServiceForm: FormGroup;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: MoveControllerServiceDialogRequestSuccess,
        private store: Store<NiFiState>,
        private formBuilder: FormBuilder
    ) {
        super();

        this.controllerService = request.controllerService;

        // build the form
        this.moveControllerServiceForm = this.formBuilder.group({
            processGroups: new FormControl('Process Group', Validators.required)
        });

        this.processGroupOptions = request.options;

        const firstEnabled = this.processGroupOptions.findIndex((pg) => !pg.disabled);
        if (firstEnabled != -1) {
            this.moveControllerServiceForm.controls['processGroups'].setValue(
                this.processGroupOptions[firstEnabled].value
            );
        } else {
            this.moveControllerServiceForm.controls['processGroups'].addValidators(() => {
                return { invalid: true };
            });
        }
    }

    submitForm() {
        this.store.dispatch(
            moveControllerService({
                request: {
                    controllerService: this.request.controllerService,
                    data: {
                        parentGroupId: this.moveControllerServiceForm.get('processGroups')?.value,
                        revision: this.request.controllerService.revision
                    }
                }
            })
        );
    }

    override isDirty(): boolean {
        return this.moveControllerServiceForm.dirty;
    }
}
