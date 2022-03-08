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

import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;

import java.util.concurrent.TimeUnit;

/** Savepoint utilities. */
public class SavepointUtils {

    public static boolean savepointInProgress(FlinkDeployment flinkDeployment) {
        return flinkDeployment.getStatus().getJobStatus().getSavepointInfo().getTriggerId() != null;
    }

    public static boolean shouldTriggerSavepoint(FlinkDeployment flinkDeployment) {
        if (savepointInProgress(flinkDeployment)) {
            return false;
        }
        return flinkDeployment.getSpec().getJob().getSavepointTriggerNonce()
                != flinkDeployment
                        .getStatus()
                        .getReconciliationStatus()
                        .getLastReconciledSpec()
                        .getJob()
                        .getSavepointTriggerNonce();
    }

    public static boolean gracePeriodEnded(
            FlinkOperatorConfiguration configuration, SavepointInfo savepointInfo) {
        int gracePeriod = configuration.getSavepointTriggerGracePeriodInSec();
        long triggerTimestamp = savepointInfo.getTriggerTimestamp();
        return (System.currentTimeMillis() - triggerTimestamp)
                > TimeUnit.SECONDS.toMillis(gracePeriod);
    }
}
