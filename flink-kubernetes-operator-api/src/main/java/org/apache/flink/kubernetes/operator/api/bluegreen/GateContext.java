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

package org.apache.flink.kubernetes.operator.api.bluegreen;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.ACTIVE_DEPLOYMENT_TYPE;
import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.DEPLOYMENT_DELETION_DELAY;
import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.IS_FIRST_DEPLOYMENT;
import static org.apache.flink.kubernetes.operator.api.bluegreen.GateContextOptions.TRANSITION_STAGE;

/** Base functionality of the Context used for Gate implementations. */
@Data
@AllArgsConstructor
public class GateContext implements Serializable {

    /** GateContext enum. */
    private final BlueGreenDeploymentType activeBlueGreenDeploymentType;

    private final GateOutputMode outputMode;
    private final int deploymentTeardownDelayMs;
    private final TransitionStage gateStage;
    private final boolean isFirstDeployment;

    public static GateContext create(
            Map<String, String> data, BlueGreenDeploymentType currentBlueGreenDeploymentType) {
        var nextActiveDeploymentType =
                BlueGreenDeploymentType.valueOf(data.get(ACTIVE_DEPLOYMENT_TYPE.getLabel()));

        var deploymentDeletionDelaySec =
                Integer.parseInt(data.get(DEPLOYMENT_DELETION_DELAY.getLabel()));

        var outputMode =
                currentBlueGreenDeploymentType == nextActiveDeploymentType
                        ? GateOutputMode.ACTIVE
                        : GateOutputMode.STANDBY;

        var isFirstDeployment = Boolean.parseBoolean(data.get(IS_FIRST_DEPLOYMENT.getLabel()));

        return new GateContext(
                nextActiveDeploymentType,
                outputMode,
                deploymentDeletionDelaySec,
                TransitionStage.valueOf(data.get(TRANSITION_STAGE.getLabel())),
                isFirstDeployment);
    }
}
