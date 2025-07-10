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

package org.apache.flink.kubernetes.operator.controller.bluegreen.handlers;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.hasSpecChanged;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.setLastReconciledSpec;

/** State handler for the INITIALIZING_BLUE state. */
public class InitializingBlueStateHandler extends AbstractBlueGreenStateHandler {

    public InitializingBlueStateHandler(BlueGreenDeploymentService deploymentService) {
        super(FlinkBlueGreenDeploymentState.INITIALIZING_BLUE, deploymentService);
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> handle(BlueGreenContext context) {
        FlinkBlueGreenDeploymentStatus deploymentStatus = context.getDeploymentStatus();

        // Deploy only if this is the initial deployment (no previous spec exists)
        // or if we're recovering from a failure and the spec has changed since the last attempt
        if (deploymentStatus.getLastReconciledSpec() == null
                || (deploymentStatus.getJobStatus().getState().equals(JobStatus.FAILING)
                        && hasSpecChanged(context))) {

            setLastReconciledSpec(context);
            return deploymentService.initiateDeployment(
                    context,
                    BlueGreenDeploymentType.BLUE,
                    FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE,
                    null,
                    true);
        } else {
            LOG.warn(
                    "Ignoring initial deployment. Last Reconciled Spec null: {}. BG Status: {}.",
                    deploymentStatus.getLastReconciledSpec() == null,
                    deploymentStatus.getJobStatus().getState());
            return UpdateControl.noUpdate();
        }
    }
}
