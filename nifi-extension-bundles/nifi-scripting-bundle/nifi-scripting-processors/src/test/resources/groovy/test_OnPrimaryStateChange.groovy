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


import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange
import org.apache.nifi.annotation.notification.PrimaryNodeState
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.Relationship


class MyRecordProcessor extends AbstractProcessor {

    def REL_SUCCESS = new Relationship.Builder().name("success").description('FlowFiles that were successfully processed are routed here').build()
    def REL_FAILURE = new Relationship.Builder().name("failure").description('FlowFiles are routed here if an error occurs during processing').build()

    static boolean primaryNode = false

    @OnPrimaryNodeStateChange
    void onPrimaryNodeStateChange(final PrimaryNodeState newState) {
        primaryNode = true
    }

    @Override
    Set<Relationship> getRelationships() {
        [REL_SUCCESS, REL_FAILURE] as Set<Relationship>
    }

    @Override
    void onTrigger(ProcessContext context, ProcessSession session) {
        def flowFile = session.create()
        session.putAttribute(flowFile, 'isPrimaryNode', primaryNode.toString())
        session.transfer(flowFile, REL_SUCCESS)
    }
}

processor = new MyRecordProcessor()