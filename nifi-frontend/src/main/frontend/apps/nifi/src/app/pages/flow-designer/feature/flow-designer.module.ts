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
import { CommonModule, NgOptimizedImage } from '@angular/common';
import { FlowDesigner } from './flow-designer.component';
import { FlowDesignerRoutingModule } from './flow-designer-routing.module';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { FlowEffects } from '../state/flow/flow.effects';
import { TransformEffects } from '../state/transform/transform.effects';
import { canvasFeatureKey, reducers } from '../state';
import { MatDialogModule } from '@angular/material/dialog';
import { ControllerServicesEffects } from '../state/controller-services/controller-services.effects';
import { ParameterEffects } from '../state/parameter/parameter.effects';
import { QueueEffects } from '../state/queue/queue.effects';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';
import { FlowAnalysisEffects } from '../state/flow-analysis/flow-analysis.effects';

@NgModule({
    declarations: [FlowDesigner],
    exports: [FlowDesigner],
    imports: [
        CommonModule,
        FlowDesignerRoutingModule,
        StoreModule.forFeature(canvasFeatureKey, reducers),
        EffectsModule.forFeature(
            FlowEffects,
            TransformEffects,
            ControllerServicesEffects,
            ParameterEffects,
            QueueEffects,
            FlowAnalysisEffects
        ),
        NgOptimizedImage,
        MatDialogModule,
        BannerText
    ]
})
export class FlowDesignerModule {}
