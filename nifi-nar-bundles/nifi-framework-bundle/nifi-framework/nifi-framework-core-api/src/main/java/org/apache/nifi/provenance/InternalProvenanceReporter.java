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

package org.apache.nifi.provenance;

import org.apache.nifi.flowfile.FlowFile;

import java.util.Collection;
import java.util.Set;

/**
 * An extension of the ProvenanceReporter that provides methods that are meant to be used only internally by the framework
 * and not by the extensions that have access to the Provenance Reporter.
 */
public interface InternalProvenanceReporter extends ProvenanceReporter {
    ProvenanceEventRecord generateDropEvent(FlowFile flowFile, String explanation);

    void clone(FlowFile parent, FlowFile child, boolean verifyFlowFile);

    ProvenanceEventRecord generateJoinEvent(Collection<FlowFile> parents, FlowFile child);

    void remove(final ProvenanceEventRecord event);

    void clear();

    void migrate(InternalProvenanceReporter newOwner, Collection<String> flowFileIds);

    void receiveMigration(Set<ProvenanceEventRecord> events);

    Set<ProvenanceEventRecord> getEvents();

    ProvenanceEventBuilder build(FlowFile flowFile, ProvenanceEventType eventType);

    ProvenanceEventRecord drop(FlowFile flowFile, String explanation);

    void expire(FlowFile flowFile, String details);
}
