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

package org.apache.flink.kubernetes.operator.api.status;

import org.apache.flink.annotation.Experimental;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Stores checkpoint-related information. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Deprecated
public class CheckpointInfo implements SnapshotInfo {
    /** Last completed checkpoint by the operator. */
    private Checkpoint lastCheckpoint;

    /** Trigger id of a pending checkpoint operation. */
    private String triggerId;

    /** Trigger timestamp of a pending checkpoint operation. */
    private Long triggerTimestamp;

    /** Checkpoint trigger mechanism. */
    private SnapshotTriggerType triggerType;

    /** Checkpoint format. */
    private CheckpointType formatType;

    /** Trigger timestamp of last periodic checkpoint operation. */
    private long lastPeriodicCheckpointTimestamp = 0L;

    public void setTrigger(
            String triggerId, SnapshotTriggerType triggerType, CheckpointType formatType) {
        this.triggerId = triggerId;
        this.triggerTimestamp = System.currentTimeMillis();
        this.triggerType = triggerType;
        this.formatType = formatType;
    }

    public void resetTrigger() {
        this.triggerId = null;
        this.triggerTimestamp = null;
        this.triggerType = null;
        this.formatType = null;
    }

    /**
     * Update last checkpoint info.
     *
     * @param checkpoint Checkpoint to be added.
     */
    public void updateLastCheckpoint(Checkpoint checkpoint) {
        lastCheckpoint = checkpoint;
        if (checkpoint.getTriggerType() == SnapshotTriggerType.PERIODIC) {
            lastPeriodicCheckpointTimestamp = checkpoint.getTimeStamp();
        }
        resetTrigger();
    }

    @JsonIgnore
    @Override
    public Long getLastTriggerNonce() {
        return lastCheckpoint == null ? null : lastCheckpoint.getTriggerNonce();
    }

    @JsonIgnore
    @Override
    public long getLastPeriodicTriggerTimestamp() {
        return lastPeriodicCheckpointTimestamp;
    }

    @JsonIgnore
    @Override
    public SnapshotTriggerType getLastTriggerType() {
        return lastCheckpoint == null ? null : lastCheckpoint.getTriggerType();
    }

    @JsonIgnore
    @Override
    public String formatErrorMessage(Long triggerNonce) {
        return SnapshotTriggerType.PERIODIC == triggerType
                ? "Periodic checkpoint failed"
                : "Checkpoint failed for checkpointTriggerNonce: " + triggerNonce;
    }

    @JsonIgnore
    @Override
    public Snapshot getLastSnapshot() {
        return lastCheckpoint;
    }
}
