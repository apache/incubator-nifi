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

import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatButton } from '@angular/material/button';
import { ClusterNode } from '../../../state/cluster-listing';
import { CloseOnEscapeDialog } from '../../../../../ui/common/close-on-escape-dialog/close-on-escape-dialog.component';

@Component({
    selector: 'cluster-node-detail-dialog',
    standalone: true,
    imports: [MatDialogModule, MatButton],
    templateUrl: './cluster-node-detail-dialog.component.html',
    styleUrl: './cluster-node-detail-dialog.component.scss'
})
export class ClusterNodeDetailDialog extends CloseOnEscapeDialog {
    node: ClusterNode;

    constructor(@Inject(MAT_DIALOG_DATA) public request: ClusterNode) {
        super();
        this.node = request;
    }
}
