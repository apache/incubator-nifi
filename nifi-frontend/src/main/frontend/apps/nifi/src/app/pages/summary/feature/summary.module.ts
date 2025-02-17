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
import { Summary } from './summary.component';
import { CommonModule } from '@angular/common';
import { SummaryRoutingModule } from './summary-routing.module';
import { MatTabsModule } from '@angular/material/tabs';
import { StoreModule } from '@ngrx/store';
import { reducers, summaryFeatureKey } from '../state';
import { EffectsModule } from '@ngrx/effects';
import { SummaryListingEffects } from '../state/summary-listing/summary-listing.effects';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { ComponentClusterStatusEffects } from '../state/component-cluster-status/component-cluster-status.effects';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';

@NgModule({
    declarations: [Summary],
    exports: [Summary],
    imports: [
        CommonModule,
        SummaryRoutingModule,
        MatTabsModule,
        StoreModule.forFeature(summaryFeatureKey, reducers),
        EffectsModule.forFeature(SummaryListingEffects, ComponentClusterStatusEffects),
        NgxSkeletonLoaderModule,
        Navigation,
        BannerText
    ]
})
export class SummaryModule {}
