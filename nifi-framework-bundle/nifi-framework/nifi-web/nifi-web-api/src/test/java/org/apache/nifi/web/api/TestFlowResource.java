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
package org.apache.nifi.web.api;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessingPerformanceStatus;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.prometheusutil.BulletinMetricsRegistry;
import org.apache.nifi.prometheusutil.ClusterMetricsRegistry;
import org.apache.nifi.prometheusutil.ConnectionAnalyticsMetricsRegistry;
import org.apache.nifi.prometheusutil.JvmMetricsRegistry;
import org.apache.nifi.prometheusutil.NiFiMetricsRegistry;
import org.apache.nifi.prometheusutil.PrometheusMetricsUtil;
import org.apache.nifi.registry.flow.FlowVersionLocation;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ComponentDifferenceDTO;
import org.apache.nifi.web.api.dto.DifferenceDTO;
import org.apache.nifi.web.api.entity.FlowComparisonEntity;
import org.apache.nifi.web.api.request.FlowMetricsProducer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestFlowResource {
    private static final String LABEL_VALUE = TestFlowResource.class.getSimpleName();
    private static final String OTHER_LABEL_VALUE = JmxJvmMetrics.class.getSimpleName();
    private static final String THREAD_COUNT_NAME = "nifi_jvm_thread_count";
    private static final String HEAP_USAGE_NAME = "nifi_jvm_heap_usage";
    private static final String HEAP_USED_NAME = "nifi_jvm_heap_used";
    private static final String HEAP_STARTS_WITH_PATTERN = "nifi_jvm_heap.*";
    private static final String PROCESSING_STARTS_WITH_PATTERN = "^nifi_processing_.*";
    private static final String THREAD_COUNT_LABEL = String.format("nifi_jvm_thread_count{instance=\"%s\"", LABEL_VALUE);
    private static final String THREAD_COUNT_OTHER_LABEL = String.format("nifi_jvm_thread_count{instance=\"%s\"", OTHER_LABEL_VALUE);
    private static final String ROOT_FIELD_NAME = "beans";
    private static final String SAMPLE_NAME_JVM = ".*jvm.*";
    private static final String SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP = "RootProcessGroup";
    private static final String SAMPLE_LABEL_VALUES_PROCESS_GROUP = "ProcessGroup";
    private static final String COMPONENT_TYPE_LABEL = "component_type";
    private static final int COMPONENT_TYPE_VALUE_INDEX = 1;
    private static final String CLUSTER_TYPE_LABEL = "cluster";
    private static final String CLUSTER_LABEL_KEY = "instance";
    private static final String SAMPLE_REGISTRY_ID = "0e87642a-7720-4799-a3bd-04db74b86e85";
    private static final String SAMPLE_BRANCH_ID_A = "c302f541-976e-4c51-952d-345516444e3d";
    private static final String SAMPLE_BUCKET_ID_A = "23da421d-a8da-4fa3-939e-658d8f35b972";
    private static final String SAMPLE_FLOW_ID_A = "34e4c8c5-f61d-45a4-8035-2aa3641ae904";
    private static final String SAMPLE_BRANCH_ID_B = "fae2ef59-eb0d-4de6-ae31-342089fd229f";
    private static final String SAMPLE_BUCKET_ID_B = "42998285-d06c-41dd-a757-7a14ab9673f4";
    private static final String SAMPLE_FLOW_ID_B = "e6483662-9226-41c1-adec-10357af97ce2";

    @InjectMocks
    private FlowResource resource = new FlowResource();

    @Mock
    private NiFiServiceFacade serviceFacade;

    @Test
    public void testGetFlowMetricsProducerInvalid() {
        assertThrows(ResourceNotFoundException.class, () -> resource.getFlowMetrics(String.class.toString(), Collections.emptySet(), null, null, null));
    }

    @Test
    public void testGetFlowMetricsPrometheus() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries(false);
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), null, null, null);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(THREAD_COUNT_NAME), "Thread Count name not found");
        assertTrue(output.contains(HEAP_USAGE_NAME), "Heap Usage name not found");
        assertEquals(0, Pattern.compile(PROCESSING_STARTS_WITH_PATTERN, Pattern.MULTILINE).matcher(output).results().count());
    }

    @Test
    public void testGetFlowMetricsPrometheusWithPerformanceStatus() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries(true);
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), null, null, null);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(THREAD_COUNT_NAME), "Thread Count name not found");
        assertTrue(output.contains(HEAP_USAGE_NAME), "Heap Usage name not found");
        assertEquals(5, Pattern.compile(PROCESSING_STARTS_WITH_PATTERN, Pattern.MULTILINE).matcher(output).results().count());
    }

    @Test
    public void testGetFlowMetricsPrometheusSampleName() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries(false);
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), THREAD_COUNT_NAME, null, null);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(THREAD_COUNT_NAME), "Thread Count name not found");
        assertFalse(output.contains(HEAP_USAGE_NAME), "Heap Usage name not filtered");
    }

    @Test
    public void testGetFlowMetricsPrometheusSampleNameStartsWithPattern() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries(false);
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), HEAP_STARTS_WITH_PATTERN, null, null);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(HEAP_USAGE_NAME), "Heap Usage name not found");
        assertTrue(output.contains(HEAP_USED_NAME), "Heap Used name not found");
        assertFalse(output.contains(THREAD_COUNT_NAME), "Heap Usage name not filtered");
    }

    @Test
    public void testGetFlowMetricsPrometheusSampleLabelValue() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries(false);
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), null, LABEL_VALUE, null);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(LABEL_VALUE), "Label Value not found");
        assertFalse(output.contains(OTHER_LABEL_VALUE), "Other Label Value not filtered");
    }

    @Test
    public void testGetFlowMetricsPrometheusSampleNameAndSampleLabelValue() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistries(false);
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.PROMETHEUS.getProducer(), Collections.emptySet(), THREAD_COUNT_NAME, LABEL_VALUE, null);

        assertNotNull(response);
        assertEquals(MediaType.valueOf(TextFormat.CONTENT_TYPE_004), response.getMediaType());

        final String output = getResponseOutput(response);

        assertTrue(output.contains(THREAD_COUNT_NAME), "Thread Count name not found");
        assertTrue(output.contains(THREAD_COUNT_LABEL), "Thread Count with label not found");
        assertTrue(output.contains(THREAD_COUNT_OTHER_LABEL), "Thread Count with other label not found");
        assertTrue(output.contains(HEAP_USAGE_NAME), "Heap Usage name not found");
    }

    @Test
    public void testGetFlowMetricsPrometheusAsJson() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistriesForJson();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.JSON.getProducer(), Collections.emptySet(), null, null, ROOT_FIELD_NAME);

        assertNotNull(response);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());

        final Map<String, List<Sample>> metrics = convertJsonResponseToMap(response);
        assertEquals(1, metrics.keySet().size());
        assertTrue(metrics.containsKey(ROOT_FIELD_NAME));

        final List<Sample> registryList = metrics.get(ROOT_FIELD_NAME);
        assertEquals(13, registryList.size());

        final Map<String, Long> result = getResult(registryList);
        assertEquals(3L, result.get(SAMPLE_NAME_JVM));
        assertEquals(4L, result.get(SAMPLE_LABEL_VALUES_PROCESS_GROUP));
        assertEquals(2L, result.get(SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP));
        assertEquals(4L, result.get(CLUSTER_LABEL_KEY));
    }

    @Test
    public void testGetFlowMetricsPrometheusAsJsonSampleName() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistriesForJson();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.JSON.getProducer(), Collections.emptySet(), SAMPLE_NAME_JVM, null, ROOT_FIELD_NAME);
        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());

        final Map<String, List<Sample>> metrics = convertJsonResponseToMap(response);
        assertEquals(1, metrics.keySet().size());
        assertTrue(metrics.containsKey(ROOT_FIELD_NAME));

        final List<Sample> registryList = metrics.get(ROOT_FIELD_NAME);
        assertEquals(3, registryList.size());

        final Map<String, Long> result = getResult(registryList);
        assertEquals(3L, result.get(SAMPLE_NAME_JVM));
    }

    @Test
    public void testGetFlowMetricsPrometheusAsJsonSampleNameStartsWithPattern() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistriesForJson();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.JSON.getProducer(), Collections.emptySet(), HEAP_STARTS_WITH_PATTERN, null, ROOT_FIELD_NAME);
        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());

        final Map<String, List<Sample>> metrics = convertJsonResponseToMap(response);
        assertEquals(1, metrics.keySet().size());
        assertTrue(metrics.containsKey(ROOT_FIELD_NAME));

        final List<Sample> registryList = metrics.get(ROOT_FIELD_NAME);
        assertEquals(2, registryList.size());

        final Map<String, Long> result = getResult(registryList);
        assertEquals(2L, result.get(SAMPLE_NAME_JVM));
    }

    @Test
    public void testGetFlowMetricsPrometheusAsJsonSampleLabelValue() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistriesForJson();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.JSON.getProducer(), Collections.emptySet(), null, SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP, ROOT_FIELD_NAME);
        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());

        final Map<String, List<Sample>> metrics = convertJsonResponseToMap(response);
        assertEquals(1, metrics.keySet().size());
        assertTrue(metrics.containsKey(ROOT_FIELD_NAME));

        final List<Sample> registryList = metrics.get(ROOT_FIELD_NAME);
        assertEquals(2, registryList.size());

        final Map<String, Long> result = getResult(registryList);
        assertEquals(2L, result.get(SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP));
    }

    @Test
    public void testGetFlowMetricsPrometheusAsJsonSampleNameAndSampleLabelValue() throws IOException {
        final List<CollectorRegistry> registries = getCollectorRegistriesForJson();
        when(serviceFacade.generateFlowMetrics(anySet())).thenReturn(registries);

        final Response response = resource.getFlowMetrics(FlowMetricsProducer.JSON.getProducer(), Collections.emptySet(), SAMPLE_NAME_JVM, SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP, ROOT_FIELD_NAME);
        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());

        final Map<String, List<Sample>> metrics = convertJsonResponseToMap(response);
        assertEquals(1, metrics.keySet().size());
        assertTrue(metrics.containsKey(ROOT_FIELD_NAME));

        final List<Sample> registryList = metrics.get(ROOT_FIELD_NAME);
        assertEquals(5, registryList.size());

        final Map<String, Long> result = getResult(registryList);
        assertEquals(3L, result.get(SAMPLE_NAME_JVM));
        assertEquals(2L, result.get(SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP));
    }

    @Test
    public void testGetVersionDifferencesWithoutLimitations() {
        setUpGetVersionDifference();

        final Response response = resource.getVersionDifferences(
                SAMPLE_REGISTRY_ID, SAMPLE_BRANCH_ID_A, SAMPLE_BUCKET_ID_A, SAMPLE_FLOW_ID_A, "1", SAMPLE_BRANCH_ID_B, SAMPLE_BUCKET_ID_B, SAMPLE_FLOW_ID_B, "2", 0, 0);
        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());
        assertInstanceOf(FlowComparisonEntity.class, response.getEntity());

        final FlowComparisonEntity entity = (FlowComparisonEntity) response.getEntity();
        final List<DifferenceDTO> differences = entity.getComponentDifferences().stream().map(ComponentDifferenceDTO::getDifferences).flatMap(Collection::stream).collect(Collectors.toList());
        assertEquals(5, differences.size());
    }

    @Test
    public void testGetVersionDifferencesFromBeginningWithPartialResults() {
        setUpGetVersionDifference();

        final Response response = resource.getVersionDifferences(
            SAMPLE_REGISTRY_ID, SAMPLE_BRANCH_ID_A, SAMPLE_BUCKET_ID_A, SAMPLE_FLOW_ID_A, "1", SAMPLE_BRANCH_ID_B, SAMPLE_BUCKET_ID_B, SAMPLE_FLOW_ID_B, "2", 0, 2
        );

        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());
        assertInstanceOf(FlowComparisonEntity.class, response.getEntity());

        final FlowComparisonEntity entity = (FlowComparisonEntity) response.getEntity();
        final List<DifferenceDTO> differences = entity.getComponentDifferences().stream().map(ComponentDifferenceDTO::getDifferences).flatMap(Collection::stream).collect(Collectors.toList());
        assertEquals(2, differences.size());
        assertEquals(createDifference("Component Added", "Connection was added"), differences.get(0));
        assertEquals(createDifference("Property Value Changed", "From '0B' to '1KB'"), differences.get(1));
    }

    @Test
    public void testGetVersionDifferencesFromBeginningExtendedWithPartialResults() {
        setUpGetVersionDifference();

        final Response response = resource.getVersionDifferences(
            SAMPLE_REGISTRY_ID, SAMPLE_BRANCH_ID_A, SAMPLE_BUCKET_ID_A, SAMPLE_FLOW_ID_A, "1", SAMPLE_BRANCH_ID_B, SAMPLE_BUCKET_ID_B, SAMPLE_FLOW_ID_B, "2", 0, 3
        );

        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());
        assertInstanceOf(FlowComparisonEntity.class, response.getEntity());

        final FlowComparisonEntity entity = (FlowComparisonEntity) response.getEntity();
        final List<DifferenceDTO> differences = entity.getComponentDifferences().stream().map(ComponentDifferenceDTO::getDifferences).flatMap(Collection::stream).collect(Collectors.toList());
        assertEquals(3, differences.size());
        assertEquals(createDifference("Component Added", "Connection was added"), differences.get(0));
        assertEquals(createDifference("Property Value Changed", "From '0B' to '1KB'"), differences.get(1));
        assertEquals(createDifference("Position Changed", "Position was changed"), differences.get(2));
    }

    @Test
    public void testGetVersionDifferencesWithOffsetAndPartialResults() {
        setUpGetVersionDifference();

        final Response response = resource.getVersionDifferences(
            SAMPLE_REGISTRY_ID, SAMPLE_BRANCH_ID_A, SAMPLE_BUCKET_ID_A, SAMPLE_FLOW_ID_A, "1", SAMPLE_BRANCH_ID_B, SAMPLE_BUCKET_ID_B, SAMPLE_FLOW_ID_B, "2", 2, 3
        );

        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());
        assertInstanceOf(FlowComparisonEntity.class, response.getEntity());

        final FlowComparisonEntity entity = (FlowComparisonEntity) response.getEntity();
        final List<DifferenceDTO> differences = entity.getComponentDifferences().stream().map(ComponentDifferenceDTO::getDifferences).flatMap(Collection::stream).collect(Collectors.toList());
        assertEquals(3, differences.size());
        assertEquals(createDifference("Position Changed", "Position was changed"), differences.get(0));
        assertEquals(createDifference("Property Value Changed", "From 'false' to 'true'"), differences.get(1));
        assertEquals(createDifference("Component Added", "Processor was added"), differences.get(2));
    }

    @Test
    public void testGetVersionDifferencesWithOffsetAndOnlyPartialResult() {
        setUpGetVersionDifference();

        final Response response = resource.getVersionDifferences(
            SAMPLE_REGISTRY_ID, SAMPLE_BRANCH_ID_A, SAMPLE_BUCKET_ID_A, SAMPLE_FLOW_ID_A, "1", SAMPLE_BRANCH_ID_B, SAMPLE_BUCKET_ID_B, SAMPLE_FLOW_ID_B, "2", 2, 1
        );

        assertNotNull(response);
        assertEquals(MediaType.valueOf(MediaType.APPLICATION_JSON), response.getMediaType());
        assertInstanceOf(FlowComparisonEntity.class, response.getEntity());

        final FlowComparisonEntity entity = (FlowComparisonEntity) response.getEntity();
        final List<DifferenceDTO> differences = entity.getComponentDifferences().stream().map(ComponentDifferenceDTO::getDifferences).flatMap(Collection::stream).collect(Collectors.toList());
        assertEquals(1, differences.size());
        assertEquals(createDifference("Position Changed", "Position was changed"), differences.get(0));
    }

    @Test
    public void testWithProcessorPerformanceStatus() {

    }

    private void setUpGetVersionDifference() {
        doReturn(getDifferences()).when(serviceFacade).getVersionDifference(anyString(), any(FlowVersionLocation.class), any(FlowVersionLocation.class));
    }

    private static DifferenceDTO createDifference(final String type, final String difference) {
        final DifferenceDTO result = new DifferenceDTO();
        result.setDifferenceType(type);
        result.setDifference(difference);
        return result;
    }

    private static FlowComparisonEntity getDifferences() {
        final FlowComparisonEntity differences = new FlowComparisonEntity();
        final Set<ComponentDifferenceDTO> componentDifferences = new HashSet<>();

        final ComponentDifferenceDTO changedComponent1 = new ComponentDifferenceDTO();
        changedComponent1.setComponentId("d72f9efe-506d-30e8-8a9f-257a69e73cd2");
        changedComponent1.setComponentName("LogAttribute");
        changedComponent1.setComponentType("Processor");
        changedComponent1.setDifferences(List.of(createDifference("Component Added", "Processor was added")));

        final ComponentDifferenceDTO changedComponent2 = new ComponentDifferenceDTO();
        changedComponent2.setComponentId("46aa1d19-65ee-32f5-83dc-e14a8d3f7e7f");
        changedComponent2.setComponentName("GenerateFlowFile");
        changedComponent2.setComponentType("Processor");
        changedComponent2.setDifferences(List.of(
            createDifference("Property Value Changed", "From '0B' to '1KB'"),
            createDifference("Position Changed", "Position was changed"),
            createDifference("Property Value Changed", "From 'false' to 'true'")
        ));

        final ComponentDifferenceDTO changedComponent3 = new ComponentDifferenceDTO();
        changedComponent3.setComponentId("cfd8f7ec-3f40-3763-af15-2dc0e227ed61");
        changedComponent3.setComponentName("");
        changedComponent3.setComponentType("Connection");
        changedComponent3.setDifferences(List.of(createDifference("Component Added", "Connection was added")));

        componentDifferences.add(changedComponent1);
        componentDifferences.add(changedComponent2);
        componentDifferences.add(changedComponent3);
        differences.setComponentDifferences(componentDifferences);

        return differences;
    }

    private String getResponseOutput(final Response response) throws IOException {
        final StreamingOutput streamingOutput = (StreamingOutput) response.getEntity();
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        streamingOutput.write(outputStream);
        final byte[] outputBytes = outputStream.toByteArray();
        return new String(outputBytes, StandardCharsets.UTF_8);
    }

    private List<CollectorRegistry> getCollectorRegistries(boolean includeProcessorPerfStatus) {
        final JvmMetricsRegistry jvmMetricsRegistry = new JvmMetricsRegistry();
        final CollectorRegistry jvmCollectorRegistry = PrometheusMetricsUtil.createJvmMetrics(jvmMetricsRegistry, JmxJvmMetrics.getInstance(), LABEL_VALUE);
        final CollectorRegistry otherJvmCollectorRegistry = PrometheusMetricsUtil.createJvmMetrics(jvmMetricsRegistry, JmxJvmMetrics.getInstance(), OTHER_LABEL_VALUE);
        final NiFiMetricsRegistry niFiMetricsRegistry = new NiFiMetricsRegistry();
        final ProcessGroupStatus processGroupStatus;
        if (includeProcessorPerfStatus) {
            processGroupStatus = makeTestProcessGroupStatusWithPerformance();
        } else {
            processGroupStatus = makeTestProcessGroupStatus();
        }
        final CollectorRegistry nifiCollectionRegistry = PrometheusMetricsUtil.createNifiMetrics(niFiMetricsRegistry, processGroupStatus, "", "", "", "");
        return Arrays.asList(jvmCollectorRegistry, otherJvmCollectorRegistry, nifiCollectionRegistry);
    }

    private static @NotNull ProcessGroupStatus makeTestProcessGroupStatus() {
        final ProcessGroupStatus processGroupStatus = new ProcessGroupStatus();
        processGroupStatus.setId("1234");
        processGroupStatus.setFlowFilesReceived(5);
        processGroupStatus.setBytesReceived(10000);
        processGroupStatus.setFlowFilesSent(10);
        processGroupStatus.setBytesSent(20000);
        processGroupStatus.setQueuedCount(100);
        processGroupStatus.setQueuedContentSize(1024L);
        processGroupStatus.setBytesRead(60000L);
        processGroupStatus.setBytesWritten(80000L);
        processGroupStatus.setActiveThreadCount(5);
        processGroupStatus.setOutputContentSize(1000L);
        processGroupStatus.setInputContentSize(1000L);
        processGroupStatus.setInputCount(1);
        processGroupStatus.setOutputCount(1);
        return processGroupStatus;
    }

    private static @NotNull ProcessGroupStatus makeTestProcessGroupStatusWithPerformance() {
        ProcessGroupStatus status = makeTestProcessGroupStatus();
        ProcessingPerformanceStatus performanceStatus = new ProcessingPerformanceStatus();
        performanceStatus.setContentReadDuration(1L);
        performanceStatus.setContentWriteDuration(1L);
        performanceStatus.setIdentifier("");
        performanceStatus.setSessionCommitDuration(1L);
        performanceStatus.setGarbageCollectionDuration(1L);
        performanceStatus.setCpuDuration(1L);
        status.setProcessingPerformanceStatus(performanceStatus);
        return status;
    }

    private Map<String, List<Sample>> convertJsonResponseToMap(final Response response) throws IOException {
        final TypeReference<HashMap<String, List<Sample>>> typeReference = new TypeReference<>() {
        };
        final ObjectMapper mapper = new ObjectMapper();
        final SimpleModule module = new SimpleModule();

        module.addDeserializer(Sample.class, new SampleDeserializer());
        mapper.registerModule(module);

        final String json = getResponseOutput(response);
        return mapper.readValue(json, typeReference);
    }

    private Map<String, Long> getResult(final List<Sample> registries) {
        return registries.stream()
                .collect(Collectors.groupingBy(
                        sample -> getResultKey(sample),
                        Collectors.counting()));
    }

    private String getResultKey(final Sample sample) {
        if (sample.labelNames.contains(COMPONENT_TYPE_LABEL)) {
            return sample.labelValues.get(COMPONENT_TYPE_VALUE_INDEX);
        }
        if (sample.name.startsWith(CLUSTER_TYPE_LABEL)) {
            return CLUSTER_LABEL_KEY;
        }
        return SAMPLE_NAME_JVM;
    }

    private static List<CollectorRegistry> getCollectorRegistriesForJson() {
        final List<CollectorRegistry> registryList = new ArrayList<>();

        registryList.add(getNifiMetricsRegistry());
        registryList.add(getConnectionMetricsRegistry());
        registryList.add(getJvmMetricsRegistry());
        registryList.add(getBulletinMetricsRegistry());
        registryList.add(getClusterMetricsRegistry());

        return registryList;

    }

    private static CollectorRegistry getNifiMetricsRegistry() {
        final NiFiMetricsRegistry registry = new NiFiMetricsRegistry();

        registry.setDataPoint(136, "TOTAL_BYTES_READ",
                "RootPGId", SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP, "rootPGName", "", "");
        registry.setDataPoint(136, "TOTAL_BYTES_READ",
                "PGId", SAMPLE_LABEL_VALUES_PROCESS_GROUP, "PGName", "RootPGId", "");

        return registry.getRegistry();
    }

    private static CollectorRegistry getConnectionMetricsRegistry() {
        final ConnectionAnalyticsMetricsRegistry connectionMetricsRegistry = new ConnectionAnalyticsMetricsRegistry();

        connectionMetricsRegistry.setDataPoint(1.0,
                "TIME_TO_BYTES_BACKPRESSURE_PREDICTION",
                "PGId", SAMPLE_LABEL_VALUES_PROCESS_GROUP, "success", "connComponentId",
                "RootPGId", "sourceId", "sourceName", "destinationId", "destinationName");
        connectionMetricsRegistry.setDataPoint(1.0,
                "TIME_TO_BYTES_BACKPRESSURE_PREDICTION",
                "RootPGId", SAMPLE_LABEL_VALUES_ROOT_PROCESS_GROUP, "rootPGName", "", "", "", "", "", "");

        return connectionMetricsRegistry.getRegistry();
    }

    private static CollectorRegistry getJvmMetricsRegistry() {
        final JvmMetricsRegistry jvmMetricsRegistry = new JvmMetricsRegistry();

        jvmMetricsRegistry.setDataPoint(4.0, "JVM_HEAP_USED", "instanceId");
        jvmMetricsRegistry.setDataPoint(6.0, "JVM_HEAP_USAGE", "instanceId");
        jvmMetricsRegistry.setDataPoint(10.0, "JVM_THREAD_COUNT", "instanceId");

        return jvmMetricsRegistry.getRegistry();
    }

    private static CollectorRegistry getBulletinMetricsRegistry() {
        final BulletinMetricsRegistry bulletinMetricsRegistry = new BulletinMetricsRegistry();

        bulletinMetricsRegistry.setDataPoint(1, "BULLETIN", "B1Id", SAMPLE_LABEL_VALUES_PROCESS_GROUP, "PGId", "RootPGId",
                "nodeAddress", "category", "sourceName", "sourceId", "level");
        bulletinMetricsRegistry.setDataPoint(1, "BULLETIN", "B2Id", SAMPLE_LABEL_VALUES_PROCESS_GROUP, "PGId", "RootPGId",
                "nodeAddress", "category", "sourceName", "sourceId", "level");

        return bulletinMetricsRegistry.getRegistry();
    }

    private static CollectorRegistry getClusterMetricsRegistry() {
        final ClusterMetricsRegistry clusterMetricsRegistry = new ClusterMetricsRegistry();

        clusterMetricsRegistry.setDataPoint(1, "IS_CLUSTERED", "B1Id");
        clusterMetricsRegistry.setDataPoint(1, "IS_CONNECTED_TO_CLUSTER", "B1Id");
        clusterMetricsRegistry.setDataPoint(2, "CONNECTED_NODE_COUNT", "B1Id", "2 / 3");
        clusterMetricsRegistry.setDataPoint(3, "TOTAL_NODE_COUNT", "B1Id");

        return clusterMetricsRegistry.getRegistry();
    }


    private static class SampleDeserializer extends StdDeserializer<Sample> {
        protected SampleDeserializer() {
            super(Sample.class);
        }

        @Override
        public Sample deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            final JsonNode node = jsonParser.getCodec().readTree(jsonParser);

            final String name = node.get("name").asText();
            final List<String> labelNames = new ArrayList<>();
            node.get("labelNames").elements().forEachRemaining(e -> labelNames.add(e.asText()));
            final List<String> labelValues = new ArrayList<>();
            node.get("labelValues").elements().forEachRemaining(e -> labelValues.add(e.asText()));
            final double value = node.get("value").asDouble();

            return new Sample(name, labelNames, labelValues, value);
        }
    }
}