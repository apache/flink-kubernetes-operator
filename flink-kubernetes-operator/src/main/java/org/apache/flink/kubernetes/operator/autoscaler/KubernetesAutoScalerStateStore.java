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
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.utils.AutoScalerSerDeModule;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
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
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** An AutoscalerStateStore which persists its state in Kubernetes ConfigMaps. */
public class KubernetesAutoScalerStateStore
        implements AutoScalerStateStore<ResourceID, KubernetesJobAutoScalerContext> {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesAutoScalerStateStore.class);

    @VisibleForTesting protected static final String SCALING_HISTORY_KEY = "scalingHistory";

    @VisibleForTesting protected static final String COLLECTED_METRICS_KEY = "collectedMetrics";

    @VisibleForTesting
    protected static final String PARALLELISM_OVERRIDES_KEY = "parallelismOverrides";

    @VisibleForTesting protected static final int MAX_CM_BYTES = 1000000;

    protected static final ObjectMapper YAML_MAPPER =
            new ObjectMapper(yamlFactory())
                    .registerModule(new JavaTimeModule())
                    .registerModule(new AutoScalerSerDeModule());

    private final ConfigMapStore configMapStore;

    public KubernetesAutoScalerStateStore(ConfigMapStore configMapStore) {
        this.configMapStore = configMapStore;
    }

    @Override
    public void storeScalingHistory(
            KubernetesJobAutoScalerContext jobContext,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {
        configMapStore.putSerializedState(
                jobContext, SCALING_HISTORY_KEY, serializeScalingHistory(scalingHistory));
    }

    @Override
    public Optional<Map<JobVertexID, SortedMap<Instant, ScalingSummary>>> getScalingHistory(
            KubernetesJobAutoScalerContext jobContext) {
        Optional<String> serializedScalingHistory =
                configMapStore.getSerializedState(jobContext, SCALING_HISTORY_KEY);
        if (serializedScalingHistory.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(deserializeScalingHistory(serializedScalingHistory.get()));
        } catch (JacksonException e) {
            LOG.error(
                    "Could not deserialize scaling history, possibly the format changed. Discarding...",
                    e);
            configMapStore.removeSerializedState(jobContext, SCALING_HISTORY_KEY);
            return Optional.empty();
        }
    }

    @Override
    public void removeScalingHistory(KubernetesJobAutoScalerContext jobContext) {
        configMapStore.removeSerializedState(jobContext, SCALING_HISTORY_KEY);
    }

    @Override
    public void storeEvaluatedMetrics(
            KubernetesJobAutoScalerContext jobContext,
            SortedMap<Instant, CollectedMetrics> evaluatedMetrics) {
        configMapStore.putSerializedState(
                jobContext, COLLECTED_METRICS_KEY, serializeEvaluatedMetrics(evaluatedMetrics));
    }

    @Override
    public Optional<SortedMap<Instant, CollectedMetrics>> getEvaluatedMetrics(
            KubernetesJobAutoScalerContext jobContext) {
        Optional<String> serializedEvaluatedMetricsOpt =
                configMapStore.getSerializedState(jobContext, COLLECTED_METRICS_KEY);
        if (serializedEvaluatedMetricsOpt.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(deserializeEvaluatedMetrics(serializedEvaluatedMetricsOpt.get()));
        } catch (JacksonException e) {
            LOG.error(
                    "Could not deserialize metric history, possibly the format changed. Discarding...",
                    e);
            configMapStore.removeSerializedState(jobContext, COLLECTED_METRICS_KEY);
            return Optional.empty();
        }
    }

    @Override
    public void removeEvaluatedMetrics(KubernetesJobAutoScalerContext jobContext) {
        configMapStore.removeSerializedState(jobContext, COLLECTED_METRICS_KEY);
    }

    @Override
    public void storeParallelismOverrides(
            KubernetesJobAutoScalerContext jobContext, Map<String, String> parallelismOverrides) {
        configMapStore.putSerializedState(
                jobContext,
                PARALLELISM_OVERRIDES_KEY,
                serializeParallelismOverrides(parallelismOverrides));
    }

    @Override
    public Optional<Map<String, String>> getParallelismOverrides(
            KubernetesJobAutoScalerContext jobContext) {
        return configMapStore
                .getSerializedState(jobContext, PARALLELISM_OVERRIDES_KEY)
                .map(KubernetesAutoScalerStateStore::deserializeParallelismOverrides);
    }

    @Override
    public void removeParallelismOverrides(KubernetesJobAutoScalerContext jobContext) {
        configMapStore.removeSerializedState(jobContext, PARALLELISM_OVERRIDES_KEY);
    }

    @Override
    public void flush(KubernetesJobAutoScalerContext jobContext) {
        trimHistoryToMaxCmSize(jobContext);
        configMapStore.flush(jobContext);
    }

    @Override
    public void removeInfoFromCache(ResourceID resourceID) {
        configMapStore.removeInfoFromCache(resourceID);
    }

    @SneakyThrows
    protected static String serializeScalingHistory(
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) {
        return compress(YAML_MAPPER.writeValueAsString(scalingHistory));
    }

    private static Map<JobVertexID, SortedMap<Instant, ScalingSummary>> deserializeScalingHistory(
            String scalingHistory) throws JacksonException {
        return YAML_MAPPER.readValue(decompress(scalingHistory), new TypeReference<>() {});
    }

    @VisibleForTesting
    @SneakyThrows
    protected static String serializeEvaluatedMetrics(
            SortedMap<Instant, CollectedMetrics> evaluatedMetrics) {
        return compress(YAML_MAPPER.writeValueAsString(evaluatedMetrics));
    }

    private static SortedMap<Instant, CollectedMetrics> deserializeEvaluatedMetrics(
            String evaluatedMetrics) throws JacksonException {
        return YAML_MAPPER.readValue(decompress(evaluatedMetrics), new TypeReference<>() {});
    }

    private static String serializeParallelismOverrides(Map<String, String> overrides) {
        return ConfigurationUtils.convertValue(overrides, String.class);
    }

    private static Map<String, String> deserializeParallelismOverrides(String overrides) {
        return ConfigurationUtils.convertValue(overrides, Map.class);
    }

    @VisibleForTesting
    protected void trimHistoryToMaxCmSize(KubernetesJobAutoScalerContext context) {
        int scalingHistorySize =
                configMapStore
                        .getSerializedState(context, SCALING_HISTORY_KEY)
                        .map(String::length)
                        .orElse(0);
        int metricHistorySize =
                configMapStore
                        .getSerializedState(context, COLLECTED_METRICS_KEY)
                        .map(String::length)
                        .orElse(0);

        Optional<SortedMap<Instant, CollectedMetrics>> evaluatedMetricsOpt =
                getEvaluatedMetrics(context);
        if (evaluatedMetricsOpt.isEmpty()) {
            return;
        }
        SortedMap<Instant, CollectedMetrics> metricHistory = evaluatedMetricsOpt.get();
        while (scalingHistorySize + metricHistorySize > MAX_CM_BYTES) {
            if (metricHistory.isEmpty()) {
                return;
            }
            var firstKey = metricHistory.firstKey();
            LOG.info("Trimming metric history by removing {}", firstKey);
            metricHistory.remove(firstKey);
            String compressed = serializeEvaluatedMetrics(metricHistory);
            configMapStore.putSerializedState(context, COLLECTED_METRICS_KEY, compressed);
            metricHistorySize = compressed.length();
        }
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
