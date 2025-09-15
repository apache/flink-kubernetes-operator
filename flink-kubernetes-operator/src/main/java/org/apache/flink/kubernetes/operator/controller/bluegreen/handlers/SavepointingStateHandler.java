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

import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

import static org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService.patchStatusUpdateControl;
import static org.apache.flink.kubernetes.operator.utils.bluegreen.BlueGreenUtils.getReconciliationReschedInterval;

/** State handler for managing Blue/Green deployment savepointing transitions. */
public class SavepointingStateHandler extends AbstractBlueGreenStateHandler {

    public SavepointingStateHandler(
            FlinkBlueGreenDeploymentState supportedState,
            BlueGreenDeploymentService deploymentService) {
        super(supportedState, deploymentService);
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> handle(BlueGreenContext context) {
        BlueGreenDeploymentType currentType = getCurrentDeploymentType();
        boolean isSavepointReady = deploymentService.monitorSavepoint(context, currentType);

        // Savepoint creation completed, transition back to active state to continue deployment
        if (isSavepointReady) {
            var nextState =
                    getSupportedState() == FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE
                            ? FlinkBlueGreenDeploymentState.ACTIVE_BLUE
                            : FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
            return patchStatusUpdateControl(context, nextState, null, null).rescheduleAfter(0);
        }

        // TODO: this will wait indefinitely for a savepoint to complete,
        //  we could abort the transition, WITHOUT SUSPENDING the FlinkDeployment,
        //  if the grace period is exceeded.
        return UpdateControl.<FlinkBlueGreenDeployment>noUpdate()
                .rescheduleAfter(getReconciliationReschedInterval(context));
    }

    private BlueGreenDeploymentType getCurrentDeploymentType() {
        return getSupportedState() == FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE
                ? BlueGreenDeploymentType.BLUE
                : BlueGreenDeploymentType.GREEN;
    }
}
