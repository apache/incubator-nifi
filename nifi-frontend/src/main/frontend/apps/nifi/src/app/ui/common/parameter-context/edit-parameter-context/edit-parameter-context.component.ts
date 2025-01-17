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

import { Component, EventEmitter, Inject, Input, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { Observable } from 'rxjs';
import { EditParameterContextRequest } from '../../../../pages/parameter-contexts/state/parameter-context-listing';
import { NifiSpinnerDirective } from '../../spinner/nifi-spinner.directive';
import { Client } from '../../../../service/client.service';
import { ParameterTable } from '../../../../pages/parameter-contexts/ui/parameter-context-listing/parameter-table/parameter-table.component';
import {
    EditParameterResponse,
    ParameterContext,
    ParameterContextEntity,
    ParameterContextUpdateRequestEntity,
    ParameterEntity,
    ParameterProviderConfiguration
} from '../../../../state/shared';
import { ProcessGroupReferences } from '../../../../pages/parameter-contexts/ui/parameter-context-listing/process-group-references/process-group-references.component';
import { ParameterContextInheritance } from '../parameter-context-inheritance/parameter-context-inheritance.component';
import { ParameterReferences } from '../../parameter-references/parameter-references.component';
import { RouterLink } from '@angular/router';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { TabbedDialog } from '../../tabbed-dialog/tabbed-dialog.component';
import { NiFiCommon, TextTip, NifiTooltipDirective, CopyDirective, Parameter } from '@nifi/shared';
import { ErrorContextKey } from '../../../../state/error';
import { ContextErrorBanner } from '../../context-error-banner/context-error-banner.component';

@Component({
    selector: 'edit-parameter-context',
    standalone: true,
    templateUrl: './edit-parameter-context.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        MatTabsModule,
        MatOptionModule,
        MatSelectModule,
        AsyncPipe,
        NifiSpinnerDirective,
        NifiSpinnerDirective,
        ParameterTable,
        ProcessGroupReferences,
        ParameterContextInheritance,
        ParameterReferences,
        RouterLink,
        NifiTooltipDirective,
        ContextErrorBanner,
        CopyDirective
    ],
    styleUrls: ['./edit-parameter-context.component.scss']
})
export class EditParameterContext extends TabbedDialog {
    @Input() createNewParameter!: (existingParameters: string[]) => Observable<EditParameterResponse>;
    @Input() editParameter!: (parameter: Parameter) => Observable<EditParameterResponse>;
    @Input() updateRequest!: Observable<ParameterContextUpdateRequestEntity | null>;
    @Input() availableParameterContexts$!: Observable<ParameterContextEntity[]>;
    @Input() saving$!: Observable<boolean>;

    @Output() addParameterContext: EventEmitter<any> = new EventEmitter<any>();
    @Output() editParameterContext: EventEmitter<any> = new EventEmitter<any>();

    editParameterContextForm: FormGroup;
    readonly: boolean;

    isNew: boolean;
    parameterProvider: ParameterProviderConfiguration | null = null;

    parameters!: ParameterEntity[];

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditParameterContextRequest,
        private formBuilder: FormBuilder,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService
    ) {
        super(NiFiCommon.EDIT_PARAMETER_CONTEXT_DIALOG_ID);

        if (request.parameterContext) {
            this.isNew = false;
            this.readonly = !request.parameterContext.permissions.canWrite;

            // @ts-ignore - component will be defined since the user has permissions to edit the context, but it is optional as defined by the type
            const parameterContext: ParameterContext = request.parameterContext.component;

            this.editParameterContextForm = this.formBuilder.group({
                name: new FormControl(parameterContext.name, Validators.required),
                description: new FormControl(parameterContext.description),
                parameters: new FormControl({
                    value: parameterContext.parameters,
                    disabled: this.readonly
                }),
                inheritedParameterContexts: new FormControl({
                    value: parameterContext.inheritedParameterContexts,
                    disabled: this.readonly
                })
            });
            if (parameterContext.parameterProviderConfiguration) {
                this.parameterProvider = parameterContext.parameterProviderConfiguration.component;
            }
        } else {
            this.isNew = true;
            this.readonly = false;

            this.editParameterContextForm = this.formBuilder.group({
                name: new FormControl('', Validators.required),
                description: new FormControl(''),
                parameters: new FormControl([]),
                inheritedParameterContexts: new FormControl([])
            });
        }
    }

    getUpdatedParameters(): string {
        if (this.parameters) {
            const updatedParameters: string[] = this.parameters.map(
                (parameterEntity) => parameterEntity.parameter.name
            );
            return updatedParameters.join(', ');
        }
        return '';
    }

    submitForm() {
        if (this.isNew) {
            const payload: any = {
                revision: {
                    version: 0,
                    clientId: this.client.getClientId()
                },
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                component: {
                    name: this.editParameterContextForm.get('name')?.value,
                    description: this.editParameterContextForm.get('description')?.value,
                    parameters: this.editParameterContextForm.get('parameters')?.value,
                    inheritedParameterContexts: this.editParameterContextForm.get('inheritedParameterContexts')?.value
                }
            };

            this.addParameterContext.next(payload);
        } else {
            // @ts-ignore
            const pc: ParameterContextEntity = this.request.parameterContext;

            if (this.editParameterContextForm.get('parameters')?.dirty) {
                this.parameters = this.editParameterContextForm.get('parameters')?.value;
            } else {
                this.parameters = [];
            }

            // The backend api doesn't support providing both a parameter value and referenced assets
            // even though it returns both from the GET api. We must strip the value out if there are
            // referenced assets.
            const updatedParameters: ParameterEntity[] = this.parameters.slice();
            updatedParameters.forEach((parameter: ParameterEntity) => {
                if ((parameter.parameter.referencedAssets || []).length > 0) {
                    parameter.parameter.value = null;
                }
            });

            const payload: any = {
                revision: this.client.getRevision(pc),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                id: pc.id,
                component: {
                    id: pc.id,
                    name: this.editParameterContextForm.get('name')?.value,
                    description: this.editParameterContextForm.get('description')?.value,
                    parameters: updatedParameters,
                    inheritedParameterContexts: this.editParameterContextForm.get('inheritedParameterContexts')?.value
                }
            };

            this.editParameterContext.next(payload);
        }
    }

    getParameterProviderLink(parameterProvider: ParameterProviderConfiguration): string[] {
        return ['/settings', 'parameter-providers', parameterProvider.parameterProviderId];
    }

    protected readonly TextTip = TextTip;

    override isDirty(): boolean {
        return this.editParameterContextForm.dirty;
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
