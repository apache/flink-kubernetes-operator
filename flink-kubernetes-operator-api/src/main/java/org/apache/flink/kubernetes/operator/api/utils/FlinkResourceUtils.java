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

package org.apache.flink.kubernetes.operator.api.utils;

import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobSpec;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.CheckpointInfo;
import org.apache.flink.kubernetes.operator.api.status.CommonStatus;
import org.apache.flink.kubernetes.operator.api.status.JobStatus;
import org.apache.flink.kubernetes.operator.api.status.SavepointInfo;

/** Utilities for Flink K8S resource. */
public class FlinkResourceUtils {
    public static CheckpointInfo getCheckpointInfo(AbstractFlinkResource<?, ?> deployment) {
        return getJobStatus(deployment).getCheckpointInfo();
    }

    public static SavepointInfo getSavepointInfo(AbstractFlinkResource<?, ?> deployment) {
        return getJobStatus(deployment).getSavepointInfo();
    }

    public static JobStatus getJobStatus(AbstractFlinkResource<?, ?> deployment) {
        return deployment.getStatus().getJobStatus();
    }

    public static JobState getReconciledJobState(AbstractFlinkResource<?, ?> deployment) {
        return getReconciledJobSpec(deployment).getState();
    }

    public static JobSpec getReconciledJobSpec(AbstractFlinkResource<?, ?> deployment) {
        return deployment
                .getStatus()
                .getReconciliationStatus()
                .deserializeLastReconciledSpec()
                .getJob();
    }

    public static JobSpec getJobSpec(
            AbstractFlinkResource<? extends AbstractFlinkSpec, ? extends CommonStatus<?>>
                    deployment) {
        return deployment.getSpec().getJob();
    }
}
