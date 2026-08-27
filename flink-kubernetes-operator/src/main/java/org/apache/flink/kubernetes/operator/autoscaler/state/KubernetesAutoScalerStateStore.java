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

package org.apache.flink.kubernetes.operator.autoscaler.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.DelayedScaleDown;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.autoscaler.utils.AutoScalerSerDeModule;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JacksonException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.LoaderOptions;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** An AutoscalerStateStore which persists its state in Kubernetes ConfigMaps. */
public class KubernetesAutoScalerStateStore
        implements AutoScalerStateStore<ResourceID, KubernetesJobAutoScalerContext> {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesAutoScalerStateStore.class);

    @VisibleForTesting protected static final String SCALING_HISTORY_KEY = "scalingHistory";
    @VisibleForTesting protected static final String SCALING_TRACKING_KEY = "scalingTracking";
    @VisibleForTesting protected static final String COLLECTED_METRICS_KEY = "collectedMetrics";

    @VisibleForTesting
    /* Be careful with changing this field name or the internal structure. Otherwise the parallelism of all autoscaled pipelines might get reset! */
    protected static final String PARALLELISM_OVERRIDES_KEY = "parallelismOverrides";

    protected static final String CONFIG_OVERRIDES_KEY = "configOverrides";

    @VisibleForTesting protected static final String DELAYED_SCALE_DOWN = "delayedScaleDown";

    @VisibleForTesting protected static final int MAX_CM_BYTES = 1000000;

    /* Caps decompressed value size against gzip bombs. Legit values are a few MB at most
     * (MAX_CM_BYTES compressed, ~2.5-4x ratio); this is well below the YAML loader's limit,
     * which guards the parser, not decompression memory. */
    @VisibleForTesting protected static final int MAX_DECOMPRESSED_BYTES = 8 * 1024 * 1024;

    protected static final ObjectMapper YAML_MAPPER =
            new ObjectMapper(yamlFactory())
                    .registerModule(new JavaTimeModule())
                    .registerModule(new AutoScalerSerDeModule())
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);

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
    public void storeScalingTracking(
            KubernetesJobAutoScalerContext jobContext, ScalingTracking scalingTrack) {
        configMapStore.putSerializedState(
                jobContext, SCALING_TRACKING_KEY, serializeScalingTracking(scalingTrack));
    }

    @Nonnull
    @Override
    public Map<JobVertexID, SortedMap<Instant, ScalingSummary>> getScalingHistory(
            KubernetesJobAutoScalerContext jobContext) {
        return readState(
                jobContext,
                SCALING_HISTORY_KEY,
                KubernetesAutoScalerStateStore::deserializeScalingHistory,
                HashMap::new);
    }

    @Override
    public ScalingTracking getScalingTracking(KubernetesJobAutoScalerContext jobContext) {
        return readState(
                jobContext,
                SCALING_TRACKING_KEY,
                KubernetesAutoScalerStateStore::deserializeScalingTracking,
                ScalingTracking::new);
    }

    @Override
    public void removeScalingHistory(KubernetesJobAutoScalerContext jobContext) {
        configMapStore.removeSerializedState(jobContext, SCALING_HISTORY_KEY);
    }

    @Override
    public void storeCollectedMetrics(
            KubernetesJobAutoScalerContext jobContext,
            SortedMap<Instant, CollectedMetrics> metrics) {
        configMapStore.putSerializedState(
                jobContext, COLLECTED_METRICS_KEY, serializeEvaluatedMetrics(metrics));
    }

    @Nonnull
    @Override
    public SortedMap<Instant, CollectedMetrics> getCollectedMetrics(
            KubernetesJobAutoScalerContext jobContext) {
        return readState(
                jobContext,
                COLLECTED_METRICS_KEY,
                KubernetesAutoScalerStateStore::deserializeEvaluatedMetrics,
                TreeMap::new);
    }

    @Override
    public void removeCollectedMetrics(KubernetesJobAutoScalerContext jobContext) {
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

    @Nonnull
    @Override
    public Map<String, String> getParallelismOverrides(KubernetesJobAutoScalerContext jobContext) {
        return readState(
                jobContext,
                PARALLELISM_OVERRIDES_KEY,
                KubernetesAutoScalerStateStore::sanitizeParallelismOverrides,
                HashMap::new);
    }

    @Nonnull
    @Override
    public ConfigChanges getConfigChanges(KubernetesJobAutoScalerContext jobContext) {
        return readState(
                jobContext,
                CONFIG_OVERRIDES_KEY,
                KubernetesAutoScalerStateStore::deserializeConfigOverrides,
                ConfigChanges::new);
    }

    @Override
    public void storeConfigChanges(
            KubernetesJobAutoScalerContext jobContext, ConfigChanges overrides) {
        configMapStore.putSerializedState(
                jobContext, CONFIG_OVERRIDES_KEY, serializeConfigOverrides(overrides));
    }

    @Override
    public void removeConfigChanges(KubernetesJobAutoScalerContext jobContext) {
        configMapStore.removeSerializedState(jobContext, CONFIG_OVERRIDES_KEY);
    }

    @Override
    public void removeParallelismOverrides(KubernetesJobAutoScalerContext jobContext) {
        configMapStore.removeSerializedState(jobContext, PARALLELISM_OVERRIDES_KEY);
    }

    @Override
    public void storeDelayedScaleDown(
            KubernetesJobAutoScalerContext jobContext, DelayedScaleDown delayedScaleDown)
            throws Exception {
        configMapStore.putSerializedState(
                jobContext, DELAYED_SCALE_DOWN, serializeDelayedScaleDown(delayedScaleDown));
    }

    @Nonnull
    @Override
    public DelayedScaleDown getDelayedScaleDown(KubernetesJobAutoScalerContext jobContext) {
        return readState(
                jobContext,
                DELAYED_SCALE_DOWN,
                KubernetesAutoScalerStateStore::deserializeDelayedScaleDown,
                DelayedScaleDown::new);
    }

    /** Deserializes a single stored state value, allowed to fail on untrusted input. */
    @FunctionalInterface
    private interface StateDeserializer<T> {
        T deserialize(String serialized) throws Exception;
    }

    /**
     * Reads and deserializes one autoscaler state entry, treating the stored value as untrusted.
     * The autoscaler ConfigMap is writable by any workload in the namespace, so any failure while
     * reading an entry (unexpected schema, corrupt or oversized payload, invalid value) discards
     * that entry and falls back to empty, rather than propagating into the reconcile loop or being
     * acted upon. {@code Exception} is caught deliberately so no deserialization path can escape
     * this boundary.
     */
    private <T> T readState(
            KubernetesJobAutoScalerContext jobContext,
            String key,
            StateDeserializer<T> deserializer,
            Supplier<T> emptyValue) {
        Optional<String> serialized = configMapStore.getSerializedState(jobContext, key);
        if (serialized.isEmpty()) {
            return emptyValue.get();
        }
        try {
            return deserializer.deserialize(serialized.get());
        } catch (Exception e) {
            LOG.error(
                    "Discarding invalid autoscaler state '{}' for {}.",
                    key,
                    jobContext.getJobKey(),
                    e);
            configMapStore.removeSerializedState(jobContext, key);
            return emptyValue.get();
        }
    }

    @Override
    public void clearAll(KubernetesJobAutoScalerContext jobContext) {
        configMapStore.clearAll(jobContext);
    }

    @Override
    public void flush(KubernetesJobAutoScalerContext jobContext) {
        trimHistoryToMaxCmSize(jobContext);
        configMapStore.flush(jobContext);
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

    @SneakyThrows
    protected static String serializeScalingTracking(ScalingTracking scalingTracking) {
        return compress(YAML_MAPPER.writeValueAsString(scalingTracking));
    }

    private static ScalingTracking deserializeScalingTracking(String scalingTracking)
            throws JacksonException {
        return YAML_MAPPER.readValue(decompress(scalingTracking), new TypeReference<>() {});
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
        return ConfigurationUtils.convertValue(overrides, String.class, false);
    }

    private static Map<String, String> deserializeParallelismOverrides(String overrides) {
        return ConfigurationUtils.convertValue(overrides, Map.class);
    }

    /**
     * Deserializes the parallelism overrides and drops any entry whose value is not a positive
     * integer, since a vertex parallelism below one is never valid and the stored value is
     * untrusted. A malformed map as a whole still fails deserialization and is discarded upstream.
     */
    private static Map<String, String> sanitizeParallelismOverrides(String serialized) {
        Map<String, String> overrides = deserializeParallelismOverrides(serialized);
        Map<String, String> sanitized = new HashMap<>();
        overrides.forEach(
                (vertexId, parallelism) -> {
                    if (isPositiveInt(parallelism)) {
                        sanitized.put(vertexId, parallelism);
                    } else {
                        LOG.warn(
                                "Dropping invalid parallelism override {}={} from autoscaler state.",
                                vertexId,
                                parallelism);
                    }
                });
        return sanitized;
    }

    private static boolean isPositiveInt(String value) {
        try {
            return Integer.parseInt(value.trim()) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Nullable
    private static String serializeConfigOverrides(ConfigChanges configChanges) {
        try {
            return YAML_MAPPER.writeValueAsString(configChanges);
        } catch (Exception e) {
            LOG.error("Failed to serialize ConfigOverrides", e);
            return null;
        }
    }

    private static ConfigChanges deserializeConfigOverrides(String configOverrides)
            throws JacksonException {
        return YAML_MAPPER.readValue(configOverrides, new TypeReference<>() {});
    }

    private static String serializeDelayedScaleDown(DelayedScaleDown delayedScaleDown)
            throws JacksonException {
        return YAML_MAPPER.writeValueAsString(delayedScaleDown);
    }

    private static DelayedScaleDown deserializeDelayedScaleDown(String delayedScaleDown)
            throws JacksonException {
        return YAML_MAPPER.readValue(delayedScaleDown, new TypeReference<>() {});
    }

    @VisibleForTesting
    protected void trimHistoryToMaxCmSize(KubernetesJobAutoScalerContext context) {
        int scalingHistorySize =
                configMapStore
                        .getSerializedState(context, SCALING_HISTORY_KEY)
                        .map(String::length)
                        .orElse(0);

        int scalingTrackingSize =
                configMapStore
                        .getSerializedState(context, SCALING_TRACKING_KEY)
                        .map(String::length)
                        .orElse(0);

        int metricHistorySize =
                configMapStore
                        .getSerializedState(context, COLLECTED_METRICS_KEY)
                        .map(String::length)
                        .orElse(0);

        SortedMap<Instant, CollectedMetrics> metricHistory = getCollectedMetrics(context);
        while (scalingHistorySize + metricHistorySize + scalingTrackingSize > MAX_CM_BYTES) {
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
                return readBounded(zi, MAX_DECOMPRESSED_BYTES);
            }
        } catch (Exception e) {
            LOG.warn("Error while decompressing scaling data, treating as uncompressed");
            // Fall back to non-compressed for migration
            return compressed;
        }
    }

    private static String readBounded(InputStream in, int maxBytes) throws IOException {
        var out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int totalRead = 0;
        int read;
        while ((read = in.read(buffer)) != -1) {
            totalRead += read;
            if (totalRead > maxBytes) {
                throw new IOException(
                        "Refusing to decompress data larger than " + maxBytes + " bytes");
            }
            out.write(buffer, 0, read);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    private static YAMLFactory yamlFactory() {
        // Set yaml size limit to 10mb
        var loaderOptions = new LoaderOptions();
        loaderOptions.setCodePointLimit(20 * 1024 * 1024);
        return YAMLFactory.builder().loaderOptions(loaderOptions).build();
    }

    @Override
    public void close() throws Exception {}
}
