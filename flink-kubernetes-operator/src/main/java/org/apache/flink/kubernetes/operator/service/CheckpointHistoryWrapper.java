/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.service;

import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.util.Optional;

import static org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics.FIELD_NAME_TRIGGER_TIMESTAMP;
import static org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics.FIELD_NAME_HISTORY;
import static org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics.FIELD_NAME_LATEST_CHECKPOINTS;
import static org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics.LatestCheckpoints.FIELD_NAME_COMPLETED;
import static org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics.LatestCheckpoints.FIELD_NAME_RESTORED;
import static org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics.LatestCheckpoints.FIELD_NAME_SAVEPOINT;
import static org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics.RestoredCheckpointStatistics.FIELD_NAME_EXTERNAL_PATH;
import static org.apache.flink.runtime.rest.messages.checkpoints.CheckpointingStatistics.RestoredCheckpointStatistics.FIELD_NAME_ID;

/** Custom Response for handling checkpoint history in a multi-version compatible way. */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
public class CheckpointHistoryWrapper implements ResponseBody {

    @JsonProperty(FIELD_NAME_LATEST_CHECKPOINTS)
    private ObjectNode latestCheckpoints;

    @JsonProperty(FIELD_NAME_HISTORY)
    private ArrayNode history;

    public Optional<PendingCheckpointInfo> getInProgressCheckpoint() {
        if (history == null || history.isEmpty()) {
            return Optional.empty();
        }

        var lastCp = history.get(0);
        var status =
                CheckpointStatsStatus.valueOf(
                        lastCp.get(CheckpointStatistics.FIELD_NAME_STATUS).asText());
        if (status.isInProgress()) {
            return Optional.of(
                    new PendingCheckpointInfo(
                            lastCp.get(FIELD_NAME_ID).asLong(),
                            lastCp.get(FIELD_NAME_TRIGGER_TIMESTAMP).asLong()));
        }
        return Optional.empty();
    }

    public Optional<CompletedCheckpointInfo> getLatestCompletedCheckpoint() {
        if (latestCheckpoints == null) {
            return Optional.empty();
        }

        var latestCheckpoint = getCheckpointInfo(FIELD_NAME_RESTORED).orElse(null);

        var completed = getCheckpointInfo(FIELD_NAME_COMPLETED).orElse(null);
        if (latestCheckpoint == null || (completed != null && completed.id > latestCheckpoint.id)) {
            latestCheckpoint = completed;
        }
        var savepoint = getCheckpointInfo(FIELD_NAME_SAVEPOINT).orElse(null);
        if (latestCheckpoint == null || (savepoint != null && savepoint.id > latestCheckpoint.id)) {
            latestCheckpoint = savepoint;
        }

        return Optional.ofNullable(latestCheckpoint);
    }

    private Optional<CompletedCheckpointInfo> getCheckpointInfo(String field) {
        return Optional.ofNullable(latestCheckpoints.get(field))
                .filter(
                        checkpoint ->
                                checkpoint.has(FIELD_NAME_ID)
                                        && checkpoint.has(FIELD_NAME_EXTERNAL_PATH))
                .map(
                        checkpoint ->
                                new CompletedCheckpointInfo(
                                        checkpoint.get(FIELD_NAME_ID).asLong(),
                                        checkpoint.get(FIELD_NAME_EXTERNAL_PATH).asText(),
                                        getCheckpointTimestamp(checkpoint)));
    }

    private long getCheckpointTimestamp(JsonNode checkpoint) {
        if (checkpoint.has(FIELD_NAME_TRIGGER_TIMESTAMP)) {
            return checkpoint.get(FIELD_NAME_TRIGGER_TIMESTAMP).asLong();
        } else {
            return checkpoint
                    .get(
                            CheckpointingStatistics.RestoredCheckpointStatistics
                                    .FIELD_NAME_RESTORE_TIMESTAMP)
                    .asLong();
        }
    }

    /** Information about the latest completed checkpoint/savepoint. */
    @Value
    public static class CompletedCheckpointInfo {
        long id;
        String externalPointer;
        long timestamp;
    }

    /** Information about the currently pending checkpoint/savepoint. */
    @Value
    public static class PendingCheckpointInfo {
        long id;
        long timestamp;
    }
}
