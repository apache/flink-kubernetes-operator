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
import org.apache.commons.lang3.StringUtils;

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
    private Long triggerTimestamp = 0L;

    /** List of recent savepoints. */
    private List<Savepoint> savepointHistory = new ArrayList<>();

    public void setTrigger(String triggerId) {
        this.triggerId = triggerId;
        this.triggerTimestamp = System.currentTimeMillis();
    }

    public boolean resetTrigger() {
        boolean reseted = StringUtils.isNotEmpty(this.triggerId);
        this.triggerId = "";
        this.triggerTimestamp = 0L;
        return reseted;
    }

    public void updateLastSavepoint(Savepoint savepoint) {
        lastSavepoint = savepoint;
        resetTrigger();
    }

    /**
     * Add the savepoint to the history if it isn't already the most recent savepoint.
     *
     * @param newSavepoint
     */
    public void addSavepointToHistory(Savepoint newSavepoint) {
        if (!savepointHistory.isEmpty()) {
            Savepoint recentSp = savepointHistory.get(savepointHistory.size() - 1);
            if (recentSp.getLocation().equals(newSavepoint.getLocation())) {
                return;
            }
        }
        savepointHistory.add(newSavepoint);
    }
}
