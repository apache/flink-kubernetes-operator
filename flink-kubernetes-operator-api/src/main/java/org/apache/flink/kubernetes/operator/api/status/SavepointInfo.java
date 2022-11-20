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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Stores savepoint related information. */
@Experimental
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SavepointInfo {
    /**
     * Last savepoint attempted or completed by the operator. For the last completed savepoint, call
     * retrieveLastCompletedSavepoint().
     */
    private Savepoint lastSavepoint;

    /** Trigger id of a pending savepoint operation. */
    private String triggerId;

    /**
     * Trigger timestamp of a pending savepoint operation.
     *
     * @deprecated This field is deprecated and will always be null. Use information from
     *     lastSavepoint instead
     */
    @Deprecated(since = "1.3", forRemoval = true)
    private Long triggerTimestamp;

    /**
     * Savepoint trigger mechanism of a pending savepoint operation.
     *
     * @deprecated This field is deprecated and will always be null. Use information from
     *     lastSavepoint instead
     */
    @Deprecated(since = "1.3", forRemoval = true)
    private SavepointTriggerType triggerType;

    /**
     * Savepoint format type of a pending savepoint operation.
     *
     * @deprecated This field is deprecated and will always be null. Use information from
     *     lastSavepoint instead
     */
    @Deprecated(since = "1.3", forRemoval = true)
    private SavepointFormatType formatType;

    /** List of recent savepoints. */
    private List<Savepoint> savepointHistory = new ArrayList<>();

    /** Trigger timestamp of last periodic savepoint operation. */
    private long lastPeriodicSavepointTimestamp = 0L;

    public void setTrigger(
            String triggerId,
            SavepointTriggerType triggerType,
            SavepointFormatType formatType,
            Long triggerNonce) {
        this.triggerId = triggerId;
        lastSavepoint =
                new Savepoint(
                        System.currentTimeMillis(), null, triggerType, formatType, triggerNonce);
    }

    public void resetTrigger() {
        triggerId = null;
        // clear the deprecated field set by old version
        triggerType = null;
        triggerTimestamp = null;
        formatType = null;
    }

    /**
     * Update last savepoint info and add the savepoint to the history if it isn't already the most
     * recent savepoint.
     *
     * @param savepoint Savepoint to be added.
     */
    public void updateLastSavepoint(Savepoint savepoint) {
        if (savepoint == null) {
            return;
        }
        if (lastSavepoint == null
                || retrieveLastCompletedSavepoint().isEmpty()
                || !StringUtils.equals(
                        savepoint.getLocation(),
                        retrieveLastCompletedSavepoint().get().getLocation())) {
            lastSavepoint = savepoint;
            savepointHistory.add(savepoint);
            if (savepoint.getTriggerType() == SavepointTriggerType.PERIODIC) {
                lastPeriodicSavepointTimestamp = savepoint.getTimeStamp();
            }
        }
        resetTrigger();
    }

    /**
     * @return only the last completed savepoint. If no savepoint was ever completed, return
     *     Optional.empty().
     */
    public Optional<Savepoint> retrieveLastCompletedSavepoint() {
        if (savepointHistory == null || savepointHistory.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(savepointHistory.get(savepointHistory.size() - 1));
    }
}
