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
import {
    ControllerServiceEntity,
    ControllerServiceReferencingComponent,
    ControllerServiceReferencingComponentEntity
} from '../../../../state/shared';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe, NgTemplateOutlet } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { ControllerServiceApi } from '../../../../ui/common/controller-service/controller-service-api/controller-service-api.component';
import { ControllerServiceReferences } from '../../../../ui/common/controller-service/controller-service-references/controller-service-references.component';
import { NifiSpinnerDirective } from '../../../../ui/common/spinner/nifi-spinner.directive';
import { TextTip, NifiTooltipDirective, SelectOption } from '@nifi/shared';
import { Store } from '@ngrx/store';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { moveControllerService } from '../../state/controller-services/controller-services.actions';
import { NiFiState } from 'apps/nifi/src/app/state';
import { MoveControllerServiceDialogRequestSuccess } from '../../state/controller-services';
import { NgIf } from '@angular/common';
import { BreadcrumbEntity } from '../../state/shared';

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
    protected controllerServiceActionProcessGroups: SelectOption[] = [];
    protected disableSubmit: boolean = false;

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

        const parentControllerServices = request.parentControllerServices.filter(
            (cs) => cs.parentGroupId != request.controllerService.parentGroupId
        );

        const processGroups: SelectOption[] = [];
        this.loadParentOption(request.breadcrumb, parentControllerServices, processGroups);

        this.loadChildOptions(request.childProcessGroupOptions, request.processGroupEntity, processGroups);
        this.controllerServiceActionProcessGroups = processGroups;

        const firstEnabled = processGroups.findIndex((pg) => !pg.disabled);
        if (firstEnabled != -1) {
            this.moveControllerServiceForm.controls['processGroups'].setValue(processGroups[firstEnabled].value);
        } else {
            if (processGroups.length > 0) {
                this.moveControllerServiceForm.controls['processGroups'].setValue(processGroups[0].value);
            }
            this.disableSubmit = true;
        }
    }

    loadParentOption(
        breadcrumb: BreadcrumbEntity,
        parentControllerServices: ControllerServiceEntity[],
        processGroups: SelectOption[]
    ) {
        if (breadcrumb.parentBreadcrumb != undefined) {
            const parentBreadcrumb = breadcrumb.parentBreadcrumb;
            if (parentBreadcrumb.permissions.canRead && parentBreadcrumb.permissions.canWrite) {
                const option: SelectOption = {
                    text: parentBreadcrumb.breadcrumb.name + ' (Parent)',
                    value: parentBreadcrumb.breadcrumb.id
                };

                let errorMsg = '';
                const descriptors = this.controllerService.component.descriptors;
                for (const descriptor in descriptors) {
                    if (descriptors[descriptor].identifiesControllerService != undefined) {
                        const controllerId = this.controllerService.component.properties[descriptors[descriptor].name];
                        if (
                            controllerId != null &&
                            !parentControllerServices.some((service) => service.id == controllerId)
                        ) {
                            errorMsg += '[' + descriptors[descriptor].name + ']';
                        }
                    }
                }

                if (errorMsg != '') {
                    option.description =
                        'The following properties reference controller services that would be out of scope for this ' +
                        'process group: ' +
                        errorMsg;
                    option.disabled = true;
                } else {
                    option.disabled = false;
                }

                processGroups.push(option);
            }
        }
    }

    loadChildOptions(
        childProcessGroupOptions: SelectOption[],
        currentProcessGroupEntity: any,
        processGroups: SelectOption[]
    ) {
        const referencingComponents: ControllerServiceReferencingComponentEntity[] =
            this.controllerService.component.referencingComponents;
        childProcessGroupOptions.forEach((child: SelectOption) => {
            const option: SelectOption = {
                text: child.text,
                value: child.value
            };

            const root = this.getProcessGroupById(currentProcessGroupEntity.component, child.value ?? '');
            let errorMsg = '';

            if (root == null) {
                option.description = 'Error loading process group root.';
                option.disabled = true;
                processGroups.push(option);
            } else {
                for (const component of referencingComponents) {
                    if (!this.processGroupContainsComponent(root, component.component.groupId)) {
                        errorMsg += '[' + component.component.name + ']';
                    }
                }

                if (errorMsg != '') {
                    option.description =
                        'The following components would be out of scope for this process group: ' + errorMsg;
                    option.disabled = true;
                } else {
                    option.disabled = false;
                }

                processGroups.push(option);
            }
        });
    }

    processGroupContainsComponent(processGroup: any, groupId: string): boolean {
        if (processGroup.contents != undefined) {
            if (processGroup.id == groupId) {
                return true;
            } else {
                for (const pg of processGroup.contents.processGroups) {
                    if (this.processGroupContainsComponent(pg, groupId)) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    getProcessGroupById(root: any, processGroupId: string): any {
        if (root != undefined) {
            if (root.id == processGroupId) {
                return root;
            } else {
                if (root.contents != undefined) {
                    for (const pg of root.contents.processGroups) {
                        const result = this.getProcessGroupById(pg, processGroupId);
                        if (result != null) {
                            return result;
                        }
                    }
                }
                return null;
            }
        }
        return null;
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
