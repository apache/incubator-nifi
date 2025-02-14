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
package org.apache.nifi.prometheusutil;

import io.prometheus.client.Gauge;

public class NiFiMetricsRegistry extends AbstractMetricsRegistry {

    public NiFiMetricsRegistry() {

        // Processor / Process Group metrics
        nameToGaugeMap.put("AMOUNT_FLOWFILES_SENT", Gauge.build()
                .name("nifi_amount_flowfiles_sent")
                .help("Total number of FlowFiles sent by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_FLOWFILES_TRANSFERRED", Gauge.build()
                .name("nifi_amount_flowfiles_transferred")
                .help("Total number of FlowFiles transferred by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_FLOWFILES_RECEIVED",  Gauge.build()
                .name("nifi_amount_flowfiles_received")
                .help("Total number of FlowFiles received by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_FLOWFILES_REMOVED",  Gauge.build()
                .name("nifi_amount_flowfiles_removed")
                .help("Total number of FlowFiles removed by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_BYTES_SENT",  Gauge.build()
                .name("nifi_amount_bytes_sent")
                .help("Total number of bytes sent by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("TOTAL_BYTES_SENT",  Gauge.build()
                .name("nifi_total_bytes_sent")
                .help("Running total number of bytes sent by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_BYTES_READ",  Gauge.build()
                .name("nifi_amount_bytes_read")
                .help("Total number of bytes read by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("TOTAL_BYTES_READ", Gauge.build().name("nifi_total_bytes_read")
                .help("Running total number of bytes read by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("TOTAL_BYTES_WRITTEN", Gauge.build().name("nifi_total_bytes_written")
                .help("Running total number of bytes written by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_BYTES_WRITTEN",  Gauge.build()
                .name("nifi_amount_bytes_written")
                .help("Total number of bytes written by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_BYTES_RECEIVED",  Gauge.build()
                .name("nifi_amount_bytes_received")
                .help("Total number of bytes received by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("TOTAL_BYTES_RECEIVED",  Gauge.build()
                .name("nifi_total_bytes_received")
                .help("Running total number of bytes received by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_BYTES_TRANSFERRED",  Gauge.build()
                .name("nifi_amount_bytes_transferred")
                .help("Total number of Bytes transferred by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_THREADS_TOTAL_ACTIVE",  Gauge.build()
                .name("nifi_amount_threads_active")
                .help("Total number of threads active for the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_THREADS_TOTAL_TERMINATED",  Gauge.build()
                .name("nifi_amount_threads_terminated")
                .help("Total number of threads terminated for the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        nameToGaugeMap.put("SIZE_CONTENT_OUTPUT_TOTAL",  Gauge.build()
                .name("nifi_size_content_output_total")
                .help("Total size of content output by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("SIZE_CONTENT_INPUT_TOTAL",  Gauge.build()
                .name("nifi_size_content_input_total")
                .help("Total size of content input by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("SIZE_CONTENT_QUEUED_TOTAL",  Gauge.build()
                .name("nifi_size_content_queued_total")
                .help("Total size of content queued in the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_ITEMS_OUTPUT",  Gauge.build()
                .name("nifi_amount_items_output")
                .help("Total number of items output by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_ITEMS_INPUT",  Gauge.build()
                .name("nifi_amount_items_input")
                .help("Total number of items input by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("AMOUNT_ITEMS_QUEUED",  Gauge.build()
                .name("nifi_amount_items_queued")
                .help("Total number of items queued by the component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("TOTAL_TASK_DURATION",  Gauge.build()
                .name("nifi_total_task_duration")
                .help("The total number of thread-milliseconds that the component has used to complete its tasks in the past 5 minutes")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
                .register(registry));

        // Processor metrics
        nameToGaugeMap.put("PROCESSOR_COUNTERS",  Gauge.build()
                .name("nifi_processor_counters")
                .help("Counters exposed by NiFi Processors")
                .labelNames("processor_name", "counter_name", "processor_id", "instance")
                .register(registry));

        // Connection metrics
        nameToGaugeMap.put("BACKPRESSURE_BYTES_THRESHOLD",  Gauge.build()
                .name("nifi_backpressure_bytes_threshold")
                .help("The number of bytes that can be queued before backpressure is applied")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("BACKPRESSURE_OBJECT_THRESHOLD",  Gauge.build()
                .name("nifi_backpressure_object_threshold")
                .help("The number of flow files that can be queued before backpressure is applied")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("IS_BACKPRESSURE_ENABLED",  Gauge.build()
                .name("nifi_backpressure_enabled")
                .help("Whether backpressure has been applied for this component. Values are 0 or 1")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("PERCENT_USED_BYTES",  Gauge.build()
                .name("nifi_percent_used_bytes")
                .help("The percentage of connection that is filled based on content bytes")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("PERCENT_USED_COUNT",  Gauge.build()
                .name("nifi_percent_used_count")
                .help("The percentage of connection that is filled based on object count")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        // Port metrics
        nameToGaugeMap.put("IS_TRANSMITTING",  Gauge.build()
                .name("nifi_transmitting")
                .help("Whether this component is transmitting data. Values are 0 or 1")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "run_status")
                .register(registry));

        // Remote Process Group (RPG) metrics
        nameToGaugeMap.put("ACTIVE_REMOTE_PORT_COUNT",  Gauge.build()
                .name("nifi_active_remote_port_count")
                .help("The number of active remote ports associated with this component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("INACTIVE_REMOTE_PORT_COUNT",  Gauge.build()
                .name("nifi_inactive_remote_port_count")
                .help("The number of inactive remote ports associated with this component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("AVERAGE_LINEAGE_DURATION",  Gauge.build()
                .name("nifi_average_lineage_duration")
                .help("The average lineage duration (in milliseconds) for all flow file processed by this component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                        "source_id", "source_name", "destination_id", "destination_name")
                .register(registry));

        nameToGaugeMap.put("FLOW_FILE_REPO_TOTAL_SPACE_BYTES", Gauge.build().name("nifi_flow_file_repo_total_space_bytes")
                .help("Flowfile repository total space in bytes")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("FLOW_FILE_REPO_USED_SPACE_BYTES", Gauge.build().name("nifi_flow_file_repo_used_space_bytes")
                .help("Flowfile repository used space in bytes")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("FLOW_FILE_REPO_FREE_SPACE_BYTES", Gauge.build().name("nifi_flow_file_repo_free_space_bytes")
                .help("Flowfile repository free space in bytes")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("CONTENT_REPO_TOTAL_SPACE_BYTES", Gauge.build().name("nifi_content_repo_total_space_bytes")
                .help("Content repository total space in bytes")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("CONTENT_REPO_USED_SPACE_BYTES", Gauge.build().name("nifi_content_repo_used_space_bytes")
                .help("Content repository used space in bytes")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("CONTENT_REPO_FREE_SPACE_BYTES", Gauge.build().name("nifi_content_repo_free_space_bytes")
                .help("Content repository free space in bytes")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("PROVENANCE_REPO_TOTAL_SPACE_BYTES", Gauge.build().name("nifi_provenance_repo_total_space_bytes")
                .help("Provenance repository total space in bytes")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("PROVENANCE_REPO_USED_SPACE_BYTES", Gauge.build().name("nifi_provenance_repo_used_space_bytes")
                .help("Provenance repository used space in bytes")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("PROVENANCE_REPO_FREE_SPACE_BYTES", Gauge.build().name("nifi_provenance_repo_free_space_bytes")
                .help("Provenance repository free space in bytes")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("PROCESSING_PERF_CPU_MILLIS", Gauge.build()
                .name("nifi_processing_performance_cpu_duration")
                .help("Estimated CPU time (in milliseconds) used by this component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("PROCESSING_PERF_GC_MILLIS", Gauge.build()
                .name("nifi_processing_performance_gc_duration")
                .help("Estimated garbage collection time (in milliseconds) used by this component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("PROCESSING_PERF_READ_MILLIS", Gauge.build()
                .name("nifi_processing_performance_content_read_duration")
                .help("Estimated content read time (in milliseconds) used by this component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("PROCESSING_PERF_WRITE_MILLIS", Gauge.build()
                .name("nifi_processing_performance_content_write_duration")
                .help("Estimated content write time (in milliseconds) used by this component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));

        nameToGaugeMap.put("PROCESSING_PERF_COMMIT_MILLIS", Gauge.build()
                .name("nifi_processing_performance_session_commit_duration")
                .help("Estimated session commit time (in milliseconds) used by this component")
                .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "repo_identifier")
                .register(registry));
    }
}
