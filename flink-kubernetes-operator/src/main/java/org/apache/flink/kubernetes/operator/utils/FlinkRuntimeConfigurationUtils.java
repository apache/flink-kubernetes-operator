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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.runtime.rest.messages.JobConfigInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for mapping Flink REST API responses (execution config, checkpoint config) into
 * Flink {@link org.apache.flink.configuration.ConfigOption} key-value pairs suitable for the
 * runtime configuration cache.
 */
public class FlinkRuntimeConfigurationUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkRuntimeConfigurationUtils.class);

    private static final org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind
                    .ObjectMapper
            CHECKPOINT_CONFIG_MAPPER =
                    new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind
                            .ObjectMapper();

    /**
     * Maps REST API JSON field names from {@link CheckpointConfigInfo} to Flink {@link
     * org.apache.flink.configuration.ConfigOption} keys. Both sides use Flink's own constants for
     * compile-time safety.
     */
    public enum CheckpointConfigMapping {
        PROCESSING_MODE(
                CheckpointConfigInfo.FIELD_NAME_PROCESSING_MODE,
                CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE.key(),
                false),
        INTERVAL(
                CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_INTERVAL,
                CheckpointingOptions.CHECKPOINTING_INTERVAL.key(),
                true),
        TIMEOUT(
                CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_TIMEOUT,
                CheckpointingOptions.CHECKPOINTING_TIMEOUT.key(),
                true),
        MIN_PAUSE(
                CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_MIN_PAUSE,
                CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS.key(),
                true),
        MAX_CONCURRENT(
                CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_MAX_CONCURRENT,
                CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS.key(),
                false),
        TOLERABLE_FAILURES(
                CheckpointConfigInfo.FIELD_NAME_TOLERABLE_FAILED_CHECKPOINTS,
                CheckpointingOptions.TOLERABLE_FAILURE_NUMBER.key(),
                false),
        UNALIGNED(
                CheckpointConfigInfo.FIELD_NAME_UNALIGNED_CHECKPOINTS,
                CheckpointingOptions.ENABLE_UNALIGNED.key(),
                false),
        ALIGNED_TIMEOUT(
                CheckpointConfigInfo.FIELD_NAME_ALIGNED_CHECKPOINT_TIMEOUT,
                CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT.key(),
                true),
        CHECKPOINTS_AFTER_TASKS_FINISH(
                CheckpointConfigInfo.FIELD_NAME_CHECKPOINTS_AFTER_TASKS_FINISH,
                CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH.key(),
                false),
        STATE_CHANGELOG(
                CheckpointConfigInfo.FIELD_NAME_STATE_CHANGELOG,
                StateChangelogOptions.ENABLE_STATE_CHANGE_LOG.key(),
                false),
        PERIODIC_MATERIALIZATION_INTERVAL(
                CheckpointConfigInfo.FIELD_NAME_PERIODIC_MATERIALIZATION_INTERVAL,
                StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL.key(),
                true),
        CHANGELOG_STORAGE(
                CheckpointConfigInfo.FIELD_NAME_CHANGELOG_STORAGE,
                StateChangelogOptions.STATE_CHANGE_LOG_STORAGE.key(),
                false);

        private final String jsonField;
        private final String configKey;
        private final boolean isDuration;

        CheckpointConfigMapping(String jsonField, String configKey, boolean isDuration) {
            this.jsonField = jsonField;
            this.configKey = configKey;
            this.isDuration = isDuration;
        }

        public String getJsonField() {
            return jsonField;
        }
    }

    private static final Map<String, String> STATE_BACKEND_NAMES =
            Map.of(
                    "EmbeddedRocksDBStateBackend", "rocksdb",
                    "HashMapStateBackend", "hashmap");

    private static final Map<String, String> CHECKPOINT_STORAGE_NAMES =
            Map.of(
                    "FileSystemCheckpointStorage", "filesystem",
                    "JobManagerCheckpointStorage", "jobmanager");

    private FlinkRuntimeConfigurationUtils() {}

    /**
     * Extract execution configuration (parallelism, object-reuse, global job parameters) from a
     * {@link JobConfigInfo} REST response.
     */
    public static Map<String, String> mapJobConfiguration(JobConfigInfo configurationInfo) {
        Map<String, String> jobConfig = new HashMap<>();
        if (configurationInfo == null || configurationInfo.getExecutionConfigInfo() == null) {
            return jobConfig;
        }
        var execInfo = configurationInfo.getExecutionConfigInfo();
        jobConfig.put(
                CoreOptions.DEFAULT_PARALLELISM.key(), String.valueOf(execInfo.getParallelism()));
        jobConfig.put(PipelineOptions.OBJECT_REUSE.key(), String.valueOf(execInfo.isObjectReuse()));
        jobConfig.putAll(execInfo.getGlobalJobParameters());
        return jobConfig;
    }

    /**
     * Convert a {@link CheckpointConfigInfo} REST response into Flink configuration key-value
     * pairs.
     */
    public static Map<String, String> mapCheckpointConfiguration(
            CheckpointConfigInfo checkpointConfigInfo) {
        Map<String, Object> rawResponse =
                CHECKPOINT_CONFIG_MAPPER.convertValue(
                        checkpointConfigInfo,
                        new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type
                                        .TypeReference<
                                Map<String, Object>>() {});

        LOG.debug("Raw checkpoint configuration response: {}", rawResponse);
        return mapCheckpointFields(rawResponse);
    }

    private static Map<String, String> mapCheckpointFields(Map<String, Object> rawResponse) {
        Map<String, String> mappedConfig = new HashMap<>();

        for (CheckpointConfigMapping mapping : CheckpointConfigMapping.values()) {
            if (!rawResponse.containsKey(mapping.jsonField)) {
                continue;
            }
            String value = String.valueOf(rawResponse.get(mapping.jsonField));

            if (mapping == CheckpointConfigMapping.PROCESSING_MODE) {
                value = value.toUpperCase();
            }
            if (mapping.isDuration) {
                value += "ms";
            }

            mappedConfig.put(mapping.configKey, value);
        }

        mapExternalizedCheckpointInfo(rawResponse, mappedConfig);
        mapStateBackendAndStorage(rawResponse, mappedConfig);

        LOG.debug(
                "Mapped {} checkpoint configuration entries: {}",
                mappedConfig.size(),
                mappedConfig);
        return mappedConfig;
    }

    private static void mapExternalizedCheckpointInfo(
            Map<String, Object> rawResponse, Map<String, String> mappedConfig) {
        Object externalizationObj =
                rawResponse.get(CheckpointConfigInfo.FIELD_NAME_EXTERNALIZED_CHECKPOINT_CONFIG);
        if (externalizationObj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> externalization = (Map<String, Object>) externalizationObj;
            Boolean enabled =
                    (Boolean)
                            externalization.get(
                                    CheckpointConfigInfo.ExternalizedCheckpointInfo
                                            .FIELD_NAME_ENABLED);
            Boolean deleteOnCancellation =
                    (Boolean)
                            externalization.get(
                                    CheckpointConfigInfo.ExternalizedCheckpointInfo
                                            .FIELD_NAME_DELETE_ON_CANCELLATION);

            String retention = "NO_EXTERNALIZED_CHECKPOINTS";
            if (Boolean.TRUE.equals(enabled)) {
                retention =
                        Boolean.TRUE.equals(deleteOnCancellation)
                                ? "DELETE_ON_CANCELLATION"
                                : "RETAIN_ON_CANCELLATION";
            }
            mappedConfig.put(
                    CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION.key(), retention);
        }
    }

    private static void mapStateBackendAndStorage(
            Map<String, Object> rawResponse, Map<String, String> mappedConfig) {
        if (rawResponse.containsKey(CheckpointConfigInfo.FIELD_NAME_STATE_BACKEND)) {
            String raw =
                    String.valueOf(rawResponse.get(CheckpointConfigInfo.FIELD_NAME_STATE_BACKEND));
            mappedConfig.put(
                    StateBackendOptions.STATE_BACKEND.key(),
                    STATE_BACKEND_NAMES.getOrDefault(raw, raw.toLowerCase()));
        }
        if (rawResponse.containsKey(CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_STORAGE)) {
            String raw =
                    String.valueOf(
                            rawResponse.get(CheckpointConfigInfo.FIELD_NAME_CHECKPOINT_STORAGE));
            mappedConfig.put(
                    CheckpointingOptions.CHECKPOINT_STORAGE.key(),
                    CHECKPOINT_STORAGE_NAMES.getOrDefault(raw, raw.toLowerCase()));
        }
    }
}
