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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** Stores savepoint related information. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Deprecated
public class SavepointInfo implements SnapshotInfo {
    /**
     * Last completed savepoint by the operator for manual and periodic snapshots. Only used if
     * FlinkStateSnapshot resources are disabled.
     */
    private static final Logger LOG = LoggerFactory.getLogger(SavepointInfo.class);

    /** Last completed savepoint by the operator. */
    private Savepoint lastSavepoint;

    /** Trigger id of a pending savepoint operation. */
    private String triggerId;

    /** Trigger timestamp of a pending savepoint operation. */
    private Long triggerTimestamp;

    /** Savepoint trigger mechanism. */
    private SnapshotTriggerType triggerType;

    /** Savepoint format. */
    private SavepointFormatType formatType;

    /** List of recent savepoints. */
    private List<Savepoint> savepointHistory = new ArrayList<>();

    /** Trigger timestamp of last periodic savepoint operation. */
    private long lastPeriodicSavepointTimestamp = 0L;

    public void setTrigger(
            String triggerId, SnapshotTriggerType triggerType, SavepointFormatType formatType) {
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
     * Update last savepoint info and add the savepoint to the history if it isn't already the most
     * recent savepoint.
     *
     * @param savepoint Savepoint to be added.
     */
    public void updateLastSavepoint(Savepoint savepoint) {
        if (savepoint == null) {
            // In terminal states we have to handle the case when there is actually no savepoint to
            // not restore from an old one
            lastSavepoint = null;
        } else if (lastSavepoint == null
                || !lastSavepoint.getLocation().equals(savepoint.getLocation())) {
            LOG.debug("Updating last savepoint to {}", savepoint);
            lastSavepoint = savepoint;
            savepointHistory.add(savepoint);
            if (savepoint.getTriggerType() == SnapshotTriggerType.PERIODIC) {
                lastPeriodicSavepointTimestamp = savepoint.getTimeStamp();
            }
        }
        resetTrigger();
    }

    @JsonIgnore
    @Override
    public Long getLastTriggerNonce() {
        return lastSavepoint == null ? null : lastSavepoint.getTriggerNonce();
    }

    @JsonIgnore
    @Override
    public long getLastPeriodicTriggerTimestamp() {
        return lastPeriodicSavepointTimestamp;
    }

    @JsonIgnore
    @Override
    public SnapshotTriggerType getLastTriggerType() {
        return lastSavepoint == null ? null : lastSavepoint.getTriggerType();
    }

    @JsonIgnore
    @Override
    public String formatErrorMessage(Long triggerNonce) {
        return SnapshotTriggerType.PERIODIC == triggerType
                ? "Periodic savepoint failed"
                : "Savepoint failed for savepointTriggerNonce: " + triggerNonce;
    }

    @JsonIgnore
    @Override
    public Snapshot getLastSnapshot() {
        return lastSavepoint;
    }
}
