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

import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;

import java.time.Duration;

/** Status of the Flink JobManager Kubernetes deployment. */
public enum JobManagerDeploymentStatus {

    /** JobManager is running and ready to receive REST API calls. */
    READY,

    /** JobManager is running but not ready yet to receive REST API calls. */
    DEPLOYED_NOT_READY,

    /** JobManager process is starting up. */
    DEPLOYING,

    /** JobManager deployment not found, probably not started or killed by user. */
    // TODO: currently a mix of SUSPENDED and ERROR, needs cleanup
    MISSING,

    /** Deployment in terminal error, requires spec change for reconciliation to continue. */
    ERROR;

    public Duration rescheduleAfter(
            FlinkDeployment flinkDeployment, FlinkOperatorConfiguration operatorConfiguration) {
        Duration rescheduleAfter;
        switch (this) {
            case DEPLOYING:
                rescheduleAfter = operatorConfiguration.getProgressCheckInterval();
                break;
            case READY:
                rescheduleAfter =
                        SavepointUtils.savepointInProgress(
                                        flinkDeployment.getStatus().getJobStatus())
                                ? operatorConfiguration.getProgressCheckInterval()
                                : operatorConfiguration.getReconcileInterval();
                break;
            case MISSING:
            case ERROR:
                rescheduleAfter = operatorConfiguration.getReconcileInterval();
                break;
            case DEPLOYED_NOT_READY:
                rescheduleAfter = operatorConfiguration.getRestApiReadyDelay();
                break;
            default:
                throw new RuntimeException("Unknown status: " + this);
        }
        return rescheduleAfter;
    }
}
