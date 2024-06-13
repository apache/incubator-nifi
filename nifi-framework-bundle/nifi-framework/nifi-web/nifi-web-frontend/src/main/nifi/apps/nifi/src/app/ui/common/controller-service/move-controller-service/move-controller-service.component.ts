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

import { Component, Inject, Input, TemplateRef, ViewChild } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import {
    ControllerServiceReferencingComponent,
    MoveControllerServiceDialogRequest,
} from '../../../../state/shared';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe, NgTemplateOutlet } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { ControllerServiceApi } from '../controller-service-api/controller-service-api.component';
import { ControllerServiceReferences } from '../controller-service-references/controller-service-references.component';
import { NifiSpinnerDirective } from '../../spinner/nifi-spinner.directive';
import { TextTip } from '../../tooltips/text-tip/text-tip.component';
import { NifiTooltipDirective } from '../../tooltips/nifi-tooltip.directive';
import { ControllerServiceState } from '../../../../state/contoller-service-state';
import { Store } from '@ngrx/store';
import {
    setControllerService,
    stopPollingControllerService,
} from '../../../../state/contoller-service-state/controller-service-state.actions';
import { selectControllerService } from '../../../../state/contoller-service-state/controller-service-state.selectors';
import { CloseOnEscapeDialog } from '../../close-on-escape-dialog/close-on-escape-dialog.component';
import { moveControllerService } from './../../../../pages/flow-designer/state/controller-services/controller-services.actions';
import { SelectOption } from './../../../../state/shared/index';

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
        NgTemplateOutlet
    ],
    styleUrls: ['./move-controller-service.component.scss']
})
export class MoveControllerService extends CloseOnEscapeDialog {
    @Input() goToReferencingComponent!: (component: ControllerServiceReferencingComponent) => void;
    protected readonly TextTip = TextTip;
    protected controllerServiceActionProcessGroups: SelectOption[] = [];

    controllerService$ = this.store.select(selectControllerService);

    moveControllerServiceForm: FormGroup;

    @ViewChild('stepComplete') stepComplete!: TemplateRef<any>;
    @ViewChild('stepError') stepError!: TemplateRef<any>;
    @ViewChild('stepInProgress') stepInProgress!: TemplateRef<any>;
    @ViewChild('stepNotStarted') stepNotStarted!: TemplateRef<any>;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: MoveControllerServiceDialogRequest,
        private store: Store<ControllerServiceState>,
        private formBuilder: FormBuilder
    ) {
        super();

        request.processGroupFlow.subscribe((flow:any) => {
            let processGroups: SelectOption[] = [];
            let processGroupflow = flow.processGroupFlow;
            if (processGroupflow.breadcrumb.hasOwnProperty('parentBreadcrumb')) {
                let parentBreadcrumb = processGroupflow.breadcrumb.parentBreadcrumb;
                if (parentBreadcrumb.permissions.canRead && parentBreadcrumb.permissions.canWrite) {
                    let option: SelectOption = {
                        text: parentBreadcrumb.breadcrumb.name + ' (Parent)',
                        value: parentBreadcrumb.breadcrumb.id
                    }
                    processGroups.push(option);
                }
            }

            processGroupflow.flow.processGroups.forEach((child:any) => {
                if (child.permissions.canRead && child.permissions.canWrite) {
                    let option: SelectOption = {
                        text: child.component.name,
                        value: child.component.id
                    }
                    processGroups.push(option);
                }
            });

            this.controllerServiceActionProcessGroups = processGroups;
            if (processGroups.length > 0) {
                this.moveControllerServiceForm.controls['processGroups'].setValue(processGroups[0].value);
            }
        });

        // build the form
        this.moveControllerServiceForm = this.formBuilder.group({
            processGroups: new FormControl("Process Group", Validators.required)
        });

        this.store.dispatch(
            setControllerService({
                request: {
                    controllerService: request.controllerService
                }
            })
        );

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

    cancelClicked(): void {
        this.store.dispatch(stopPollingControllerService());
    }

    override isDirty(): boolean {
        return this.moveControllerServiceForm.dirty;
    }
}
