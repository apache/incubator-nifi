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
import { FlowDesignerComponent } from './flow-designer.component';
import { FlowDesignerRoutingModule } from './flow-designer-routing.module';
import { HeaderModule } from '../ui/header/header.module';
import { FooterModule } from '../ui/footer/footer.module';
import { CanvasModule } from '../ui/canvas/canvas.module';
import { flowReducer } from '../state/flow/flow.reducer';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { FlowEffects } from '../state/flow/flow.effects';
import { transformReducer } from '../state/transform/transform.reducer';
import { TransformEffects } from '../state/transform/transform.effects';

@NgModule({
    declarations: [FlowDesignerComponent],
    exports: [FlowDesignerComponent],
    imports: [
        CommonModule,
        HeaderModule,
        CanvasModule,
        FooterModule,
        FlowDesignerRoutingModule,
        StoreModule.forFeature('flowState', flowReducer),
        StoreModule.forFeature('transform', transformReducer),
        EffectsModule.forFeature([FlowEffects, TransformEffects])
    ]
})
export class FlowDesignerModule {}
