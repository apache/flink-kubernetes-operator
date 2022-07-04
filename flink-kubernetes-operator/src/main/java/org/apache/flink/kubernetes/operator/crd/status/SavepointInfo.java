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

package org.apache.flink.kubernetes.operator.crd.status;

import org.apache.flink.annotation.Experimental;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/** Stores savepoint related information. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SavepointInfo {
    /** Last completed savepoint by the operator. */
    private Savepoint lastSavepoint;

    /** Trigger id of a pending savepoint operation. */
    private String triggerId = "";

    /** Trigger timestamp of a pending savepoint operation. */
    private long triggerTimestamp = 0L;

    /** Savepoint trigger mechanism. */
    private SavepointTriggerType triggerType = SavepointTriggerType.UNKNOWN;

    /** List of recent savepoints. */
    private List<Savepoint> savepointHistory = new ArrayList<>();

    /** Trigger timestamp of last periodic savepoint operation. */
    private long lastPeriodicSavepointTimestamp = 0L;

    public void setTrigger(String triggerId, SavepointTriggerType triggerType) {
        this.triggerId = triggerId;
        this.triggerTimestamp = System.currentTimeMillis();
        this.triggerType = triggerType;
    }

    public void resetTrigger() {
        this.triggerId = "";
        this.triggerTimestamp = 0L;
    }

    /**
     * Update last savepoint info and add the savepoint to the history if it isn't already the most
     * recent savepoint.
     *
     * @param savepoint Savepoint to be added.
     */
    public void updateLastSavepoint(Savepoint savepoint) {
        if (lastSavepoint == null || !lastSavepoint.getLocation().equals(savepoint.getLocation())) {
            lastSavepoint = savepoint;
            savepointHistory.add(savepoint);
            if (savepoint.getTriggerType() == SavepointTriggerType.PERIODIC) {
                lastPeriodicSavepointTimestamp = savepoint.getTimeStamp();
            }
        }
        resetTrigger();
    }
}
