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

import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { Settings } from './settings.component';
import { reducers, settingsFeatureKey } from '../state';
import { SettingsRoutingModule } from './settings-routing.module';
import { GeneralEffects } from '../state/general/general.effects';
import { ManagementControllerServicesEffects } from '../state/management-controller-services/management-controller-services.effects';
import { MatTabsModule } from '@angular/material/tabs';
import { ReportingTasksEffects } from '../state/reporting-tasks/reporting-tasks.effects';
import { RegistryClientsEffects } from '../state/registry-clients/registry-clients.effects';
import { FlowAnalysisRulesEffects } from '../state/flow-analysis-rules/flow-analysis-rules.effects';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { ParameterProvidersEffects } from '../state/parameter-providers/parameter-providers.effects';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';

@NgModule({
    declarations: [Settings],
    exports: [Settings],
    imports: [
        CommonModule,
        SettingsRoutingModule,
        StoreModule.forFeature(settingsFeatureKey, reducers),
        EffectsModule.forFeature(
            GeneralEffects,
            ManagementControllerServicesEffects,
            ReportingTasksEffects,
            FlowAnalysisRulesEffects,
            RegistryClientsEffects,
            ParameterProvidersEffects
        ),
        MatTabsModule,
        Navigation,
        BannerText
    ]
})
export class SettingsModule {}
