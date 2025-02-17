/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { NgModule } from '@angular/core';
import { Bulletins } from './bulletins.component';
import { CommonModule } from '@angular/common';
import { StoreModule } from '@ngrx/store';
import { bulletinsFeatureKey, reducers } from '../state';
import { EffectsModule } from '@ngrx/effects';
import { BulletinBoardEffects } from '../state/bulletin-board/bulletin-board.effects';
import { BulletinsRoutingModule } from './bulletins-routing.module';
import { BulletinBoard } from '../ui/bulletin-board/bulletin-board.component';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';

@NgModule({
    declarations: [Bulletins],
    exports: [Bulletins],
    imports: [
        CommonModule,
        BulletinsRoutingModule,
        StoreModule.forFeature(bulletinsFeatureKey, reducers),
        EffectsModule.forFeature(BulletinBoardEffects),
        BulletinBoard,
        Navigation,
        BannerText
    ]
})
export class BulletinsModule {}
