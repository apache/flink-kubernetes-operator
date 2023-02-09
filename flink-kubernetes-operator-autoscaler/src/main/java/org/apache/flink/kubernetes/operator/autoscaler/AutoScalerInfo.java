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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.utils.JobVertexSerDeModule;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/** Class for encapsulating information stored for each resource when using the autoscaler. */
public class AutoScalerInfo {

    private static final Logger LOG = LoggerFactory.getLogger(JobAutoScalerImpl.class);

    private static final String LABEL_COMPONENT_AUTOSCALER = "autoscaler";

    private static final String COLLECTED_METRICS_KEY = "collectedMetrics";
    private static final String SCALING_HISTORY_KEY = "scalingHistory";
    private static final String JOB_UPDATE_TS_KEY = "jobUpdateTs";

    private static final ObjectMapper YAML_MAPPER =
            new ObjectMapper(new YAMLFactory())
                    .registerModule(new JavaTimeModule())
                    .registerModule(new JobVertexSerDeModule());

    private final ConfigMap configMap;
    private Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory;

    public AutoScalerInfo(ConfigMap configMap) {
        this.configMap = configMap;
    }

    @VisibleForTesting
    public AutoScalerInfo(Map<String, String> data) {
        this(new ConfigMap());
        configMap.setData(Preconditions.checkNotNull(data));
    }

    @SneakyThrows
    public SortedMap<Instant, Map<JobVertexID, Map<ScalingMetric, Double>>> getMetricHistory() {
        var historyYaml = configMap.getData().get(COLLECTED_METRICS_KEY);
        if (historyYaml == null) {
            return new TreeMap<>();
        }

        return YAML_MAPPER.readValue(historyYaml, new TypeReference<>() {});
    }

    @SneakyThrows
    public void updateMetricHistory(
            Instant jobUpdateTs,
            SortedMap<Instant, Map<JobVertexID, Map<ScalingMetric, Double>>> history) {
        configMap.getData().put(COLLECTED_METRICS_KEY, YAML_MAPPER.writeValueAsString(history));
        configMap.getData().put(JOB_UPDATE_TS_KEY, jobUpdateTs.toString());
    }

    @SneakyThrows
    public void updateVertexList(List<JobVertexID> vertexList) {
        // Make sure to init history
        getScalingHistory();

        if (scalingHistory.keySet().removeIf(v -> !vertexList.contains(v))) {
            configMap
                    .getData()
                    .put(SCALING_HISTORY_KEY, YAML_MAPPER.writeValueAsString(scalingHistory));
        }
    }

    public void clearMetricHistory() {
        configMap.getData().remove(COLLECTED_METRICS_KEY);
        configMap.getData().remove(JOB_UPDATE_TS_KEY);
    }

    public Optional<Instant> getJobUpdateTs() {
        return Optional.ofNullable(configMap.getData().get(JOB_UPDATE_TS_KEY)).map(Instant::parse);
    }

    @SneakyThrows
    public Map<JobVertexID, SortedMap<Instant, ScalingSummary>> getScalingHistory() {
        if (scalingHistory != null) {
            return scalingHistory;
        }
        var yaml = configMap.getData().get(SCALING_HISTORY_KEY);
        scalingHistory =
                yaml == null
                        ? new HashMap<>()
                        : YAML_MAPPER.readValue(yaml, new TypeReference<>() {});
        return scalingHistory;
    }

    @SneakyThrows
    public void addToScalingHistory(
            Instant now, Map<JobVertexID, ScalingSummary> summaries, Configuration conf) {
        // Make sure to init history
        getScalingHistory();

        summaries.forEach(
                (id, summary) ->
                        scalingHistory.computeIfAbsent(id, j -> new TreeMap<>()).put(now, summary));

        var entryIt = scalingHistory.entrySet().iterator();
        while (entryIt.hasNext()) {
            var entry = entryIt.next();
            // Limit how long past scaling decisions are remembered
            entry.setValue(
                    entry.getValue()
                            .tailMap(
                                    now.minus(
                                            conf.get(
                                                    AutoScalerOptions
                                                            .VERTEX_SCALING_HISTORY_AGE))));
            var vertexHistory = entry.getValue();
            while (vertexHistory.size()
                    > conf.get(AutoScalerOptions.VERTEX_SCALING_HISTORY_COUNT)) {
                vertexHistory.remove(vertexHistory.firstKey());
            }
            if (vertexHistory.isEmpty()) {
                entryIt.remove();
            }
        }

        configMap
                .getData()
                .put(SCALING_HISTORY_KEY, YAML_MAPPER.writeValueAsString(scalingHistory));
    }

    public void replaceInKubernetes(KubernetesClient client) {
        client.resource(configMap).replace();
    }

    public static AutoScalerInfo forResource(
            AbstractFlinkResource<?, ?> cr, KubernetesClient kubeClient) {

        var objectMeta = new ObjectMeta();
        objectMeta.setName("autoscaler-" + cr.getMetadata().getName());
        objectMeta.setNamespace(cr.getMetadata().getNamespace());

        ConfigMap infoCm =
                getScalingInfoConfigMap(objectMeta, kubeClient)
                        .orElseGet(
                                () -> {
                                    LOG.info("Creating scaling info config map");

                                    objectMeta.setLabels(
                                            Map.of(
                                                    Constants.LABEL_COMPONENT_KEY,
                                                    LABEL_COMPONENT_AUTOSCALER,
                                                    Constants.LABEL_APP_KEY,
                                                    cr.getMetadata().getName()));
                                    var cm = new ConfigMap();
                                    cm.setMetadata(objectMeta);
                                    cm.addOwnerReference(cr);
                                    cm.setData(new HashMap<>());
                                    return kubeClient.resource(cm).create();
                                });

        return new AutoScalerInfo(infoCm);
    }

    private static Optional<ConfigMap> getScalingInfoConfigMap(
            ObjectMeta objectMeta, KubernetesClient kubeClient) {
        return Optional.ofNullable(
                kubeClient
                        .configMaps()
                        .inNamespace(objectMeta.getNamespace())
                        .withName(objectMeta.getName())
                        .get());
    }
}
