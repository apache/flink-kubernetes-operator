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

package org.apache.flink.kubernetes.operator.mutator;

import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;

import java.util.HashMap;
import java.util.Optional;

/** Default Flink Mutator. */
public class DefaultFlinkMutator implements FlinkResourceMutator {

    @Override
    public FlinkDeployment mutateDeployment(FlinkDeployment deployment) {

        return deployment;
    }

    @Override
    public FlinkSessionJob mutateSessionJob(
            FlinkSessionJob sessionJob, Optional<FlinkDeployment> session) {

        setSessionTargetLabel(sessionJob);

        return sessionJob;
    }

    @Override
    public FlinkStateSnapshot mutateStateSnapshot(FlinkStateSnapshot stateSnapshot) {
        return stateSnapshot;
    }

    private void setSessionTargetLabel(FlinkSessionJob flinkSessionJob) {
        var labels = flinkSessionJob.getMetadata().getLabels();
        if (labels == null) {
            labels = new HashMap<>();
        }
        var deploymentName = flinkSessionJob.getSpec().getDeploymentName();
        if (deploymentName != null
                && !deploymentName.equals(labels.get(CrdConstants.LABEL_TARGET_SESSION))) {
            labels.put(
                    CrdConstants.LABEL_TARGET_SESSION,
                    flinkSessionJob.getSpec().getDeploymentName());
            flinkSessionJob.getMetadata().setLabels(labels);
        }
    }
}
