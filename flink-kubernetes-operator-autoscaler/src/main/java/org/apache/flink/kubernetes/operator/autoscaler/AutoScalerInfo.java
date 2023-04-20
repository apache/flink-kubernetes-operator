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
import org.apache.flink.kubernetes.operator.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.kubernetes.operator.autoscaler.utils.AutoScalerSerDeModule;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** Class for encapsulating information stored for each resource when using the autoscaler. */
public class AutoScalerInfo {

    private static final Logger LOG = LoggerFactory.getLogger(AutoScalerInfo.class);

    private static final String LABEL_COMPONENT_AUTOSCALER = "autoscaler";

    protected static final String COLLECTED_METRICS_KEY = "collectedMetrics";
    protected static final String SCALING_HISTORY_KEY = "scalingHistory";
    protected static final String JOB_UPDATE_TS_KEY = "jobUpdateTs";

    protected static final int MAX_CM_BYTES = 1000000;

    protected static final ObjectMapper YAML_MAPPER =
            new ObjectMapper(yamlFactory())
                    .registerModule(new JavaTimeModule())
                    .registerModule(new AutoScalerSerDeModule());

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

    public SortedMap<Instant, CollectedMetrics> getMetricHistory() {
        var historyYaml = configMap.getData().get(COLLECTED_METRICS_KEY);
        if (historyYaml == null) {
            return new TreeMap<>();
        }

        try {
            return YAML_MAPPER.readValue(decompress(historyYaml), new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            LOG.error(
                    "Could not deserialize metric history, possibly the format changed. Discarding...");
            return new TreeMap<>();
        }
    }

    @SneakyThrows
    public void updateMetricHistory(
            Instant jobUpdateTs, SortedMap<Instant, CollectedMetrics> history) {

        configMap
                .getData()
                .put(COLLECTED_METRICS_KEY, compress(YAML_MAPPER.writeValueAsString(history)));
        configMap.getData().put(JOB_UPDATE_TS_KEY, jobUpdateTs.toString());
    }

    @SneakyThrows
    public void updateVertexList(List<JobVertexID> vertexList) {
        // Make sure to init history
        getScalingHistory();

        if (scalingHistory.keySet().removeIf(v -> !vertexList.contains(v))) {
            storeScalingHistory();
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
        var yaml = decompress(configMap.getData().get(SCALING_HISTORY_KEY));
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

        storeScalingHistory();
    }

    private void storeScalingHistory() throws Exception {
        configMap
                .getData()
                .put(SCALING_HISTORY_KEY, compress(YAML_MAPPER.writeValueAsString(scalingHistory)));
    }

    public void replaceInKubernetes(KubernetesClient client) throws Exception {
        trimHistoryToMaxCmSize();
        client.resource(configMap).update();
    }

    @VisibleForTesting
    protected void trimHistoryToMaxCmSize() throws Exception {
        var data = configMap.getData();

        int scalingHistorySize = data.getOrDefault(SCALING_HISTORY_KEY, "").length();
        int metricHistorySize = data.getOrDefault(COLLECTED_METRICS_KEY, "").length();

        SortedMap<Instant, CollectedMetrics> metricHistory = null;
        while (scalingHistorySize + metricHistorySize > MAX_CM_BYTES) {
            if (metricHistory == null) {
                metricHistory = getMetricHistory();
            }
            if (metricHistory.isEmpty()) {
                return;
            }
            var firstKey = metricHistory.firstKey();
            LOG.info("Trimming metric history by removing {}", firstKey);
            metricHistory.remove(firstKey);
            String compressed = compress(YAML_MAPPER.writeValueAsString(metricHistory));
            data.put(COLLECTED_METRICS_KEY, compressed);
            metricHistorySize = compressed.length();
        }
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

    private static String compress(String original) throws IOException {
        ByteArrayOutputStream rstBao = new ByteArrayOutputStream();
        try (var zos = new GZIPOutputStream(rstBao)) {
            zos.write(original.getBytes(StandardCharsets.UTF_8));
        }

        return Base64.getEncoder().encodeToString(rstBao.toByteArray());
    }

    private static String decompress(String compressed) {
        if (compressed == null) {
            return null;
        }

        try {
            byte[] bytes = Base64.getDecoder().decode(compressed);
            try (var zi = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
                return IOUtils.toString(zi, StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            LOG.warn("Error while decompressing scaling data, treating as uncompressed");
            // Fall back to non-compressed for migration
            return compressed;
        }
    }

    private static YAMLFactory yamlFactory() {
        // Set yaml size limit to 10mb
        var loaderOptions = new LoaderOptions();
        loaderOptions.setCodePointLimit(20 * 1024 * 1024);
        return YAMLFactory.builder().loaderOptions(loaderOptions).build();
    }
}
