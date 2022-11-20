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

package org.apache.flink.kubernetes.operator;

import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Testing utilities. */
public class SavepointTestUtils {
    /**
     * Set up trigger info from a CR of version 1.2 with pending savepoint and no completed
     * savepoint.
     *
     * @param savepointInfo
     * @param withOldCR
     */
    public static void setupCRV1_2TrigerInfo(SavepointInfo savepointInfo, boolean withOldCR) {
        if (!withOldCR) {
            return;
        }
        assertNotNull(savepointInfo);
        var lastSavepoint = savepointInfo.getLastSavepoint();
        if (lastSavepoint != null) {
            savepointInfo.setFormatType(lastSavepoint.getFormatType());
            savepointInfo.setTriggerTimestamp(lastSavepoint.getTimeStamp());
            savepointInfo.setTriggerType(lastSavepoint.getTriggerType());
            savepointInfo.setLastSavepoint(
                    savepointInfo.retrieveLastCompletedSavepoint().orElse(null));
        }
    }

    /**
     * Migrate the old CR to new CR version.
     *
     * @param jobStatus
     * @param triggerNonce
     * @param withOldCR
     */
    public static void checkAndMigrate(JobStatus jobStatus, Long triggerNonce, boolean withOldCR) {
        if (!withOldCR || !SavepointUtils.savepointInProgress(jobStatus)) {
            return;
        }
        var savepointInfo = jobStatus.getSavepointInfo();
        assertNotNull(savepointInfo);
        SavepointUtils.checkAndMigrateDeprecatedTriggerFields(savepointInfo, triggerNonce);
    }
}
