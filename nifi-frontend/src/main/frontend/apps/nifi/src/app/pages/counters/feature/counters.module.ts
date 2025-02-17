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
import { CommonModule } from '@angular/common';
import { Counters } from './counters.component';
import { CountersRoutingModule } from './counters-routing.module';
import { StoreModule } from '@ngrx/store';
import { countersFeatureKey, reducers } from '../state';
import { EffectsModule } from '@ngrx/effects';
import { CounterListingEffects } from '../state/counter-listing/counter-listing.effects';
import { MatDialogModule } from '@angular/material/dialog';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';
import { CounterListing } from '../ui/counter-listing/counter-listing.component';

@NgModule({
    declarations: [Counters],
    exports: [Counters],
    imports: [
        CommonModule,
        CountersRoutingModule,
        StoreModule.forFeature(countersFeatureKey, reducers),
        EffectsModule.forFeature(CounterListingEffects),
        CounterListing,
        MatDialogModule,
        Navigation,
        BannerText
    ]
})
export class CountersModule {}
