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
import { Users } from './users.component';
import { UsersRoutingModule } from './users-routing.module';
import { StoreModule } from '@ngrx/store';
import { usersFeatureKey, reducers } from '../state';
import { EffectsModule } from '@ngrx/effects';
import { MatDialogModule } from '@angular/material/dialog';
import { UserListingEffects } from '../state/user-listing/user-listing.effects';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';
import { UserListing } from '../ui/user-listing/user-listing.component';

@NgModule({
    declarations: [Users],
    exports: [Users],
    imports: [
        CommonModule,
        UsersRoutingModule,
        StoreModule.forFeature(usersFeatureKey, reducers),
        EffectsModule.forFeature(UserListingEffects),
        MatDialogModule,
        UserListing,
        Navigation,
        BannerText
    ]
})
export class UsersModule {}
