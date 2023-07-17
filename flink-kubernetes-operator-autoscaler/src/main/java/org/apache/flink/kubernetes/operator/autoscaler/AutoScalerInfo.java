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
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.kubernetes.operator.autoscaler.utils.AutoScalerSerDeModule;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.fabric8.kubernetes.api.model.ConfigMap;
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
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** Class for encapsulating information stored for each resource when using the autoscaler. */
public class AutoScalerInfo {

    private static final Logger LOG = LoggerFactory.getLogger(AutoScalerInfo.class);

    protected static final String COLLECTED_METRICS_KEY = "collectedMetrics";
    protected static final String SCALING_HISTORY_KEY = "scalingHistory";

    protected static final String PARALLELISM_OVERRIDES_KEY = "parallelismOverrides";

    protected static final int MAX_CM_BYTES = 1000000;

    protected static final ObjectMapper YAML_MAPPER =
            new ObjectMapper(yamlFactory())
                    .registerModule(new JavaTimeModule())
                    .registerModule(new AutoScalerSerDeModule());

    private ConfigMap configMap;
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
        } catch (JacksonException e) {
            LOG.error(
                    "Could not deserialize metric history, possibly the format changed. Discarding...");
            configMap.getData().remove(COLLECTED_METRICS_KEY);
            return new TreeMap<>();
        }
    }

    @SneakyThrows
    public void updateMetricHistory(SortedMap<Instant, CollectedMetrics> history) {
        configMap
                .getData()
                .put(COLLECTED_METRICS_KEY, compress(YAML_MAPPER.writeValueAsString(history)));
    }

    @SneakyThrows
    public void updateVertexList(List<JobVertexID> vertexList, Instant now, Configuration conf) {
        // Make sure to init history
        getScalingHistory(now, conf);

        if (scalingHistory.keySet().removeIf(v -> !vertexList.contains(v))) {
            storeScalingHistory();
        }
    }

    public void clearMetricHistory() {
        configMap.getData().remove(COLLECTED_METRICS_KEY);
    }

    private void trimScalingHistory(Instant now, Configuration conf) {
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
    }

    @SneakyThrows
    public Map<JobVertexID, SortedMap<Instant, ScalingSummary>> getScalingHistory(
            Instant now, Configuration conf) {
        if (scalingHistory != null) {
            trimScalingHistory(now, conf);
            return scalingHistory;
        }
        var yaml = decompress(configMap.getData().get(SCALING_HISTORY_KEY));
        if (yaml == null) {
            scalingHistory = new HashMap<>();
            return scalingHistory;
        }
        try {
            scalingHistory = YAML_MAPPER.readValue(yaml, new TypeReference<>() {});
        } catch (JacksonException e) {
            LOG.error(
                    "Could not deserialize scaling history, possibly the format changed. Discarding...");
            configMap.getData().remove(SCALING_HISTORY_KEY);
            scalingHistory = new HashMap<>();
        }
        return scalingHistory;
    }

    @SneakyThrows
    public void addToScalingHistory(
            Instant now, Map<JobVertexID, ScalingSummary> summaries, Configuration conf) {
        // Make sure to init history
        getScalingHistory(now, conf);

        summaries.forEach(
                (id, summary) ->
                        scalingHistory.computeIfAbsent(id, j -> new TreeMap<>()).put(now, summary));

        storeScalingHistory();
    }

    public void setCurrentOverrides(Map<String, String> overrides) {
        configMap
                .getData()
                .put(
                        PARALLELISM_OVERRIDES_KEY,
                        ConfigurationUtils.convertValue(overrides, String.class));
    }

    public Map<String, String> getCurrentOverrides() {
        var overridesStr = configMap.getData().get(PARALLELISM_OVERRIDES_KEY);
        if (overridesStr == null) {
            return Map.of();
        }
        return ConfigurationUtils.convertValue(overridesStr, Map.class);
    }

    public void removeCurrentOverrides() {
        configMap.getData().remove(PARALLELISM_OVERRIDES_KEY);
    }

    private void storeScalingHistory() throws Exception {
        configMap
                .getData()
                .put(SCALING_HISTORY_KEY, compress(YAML_MAPPER.writeValueAsString(scalingHistory)));
    }

    public void replaceInKubernetes(KubernetesClient client) throws Exception {
        trimHistoryToMaxCmSize();
        try {
            configMap = client.resource(configMap).update();
        } catch (Exception e) {
            LOG.error(
                    "Error while updating autoscaler info configmap, invalidating to clear the cache",
                    e);
            configMap = null;
            throw e;
        }
    }

    public boolean isValid() {
        return configMap != null;
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

    @VisibleForTesting
    protected ConfigMap getConfigMap() {
        return configMap;
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
