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

package org.apache.flink.kubernetes.operator.controller.bluegreen;

import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.controller.bluegreen.handlers.ActiveStateHandler;
import org.apache.flink.kubernetes.operator.controller.bluegreen.handlers.BlueGreenStateHandler;
import org.apache.flink.kubernetes.operator.controller.bluegreen.handlers.InitializingBlueStateHandler;
import org.apache.flink.kubernetes.operator.controller.bluegreen.handlers.SavepointingStateHandler;
import org.apache.flink.kubernetes.operator.controller.bluegreen.handlers.TransitioningStateHandler;

import java.util.Map;

import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.ACTIVE_GREEN;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.INITIALIZING_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.SAVEPOINTING_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.SAVEPOINTING_GREEN;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE;
import static org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState.TRANSITIONING_TO_GREEN;

/** Registry for Blue/Green deployment state handlers. */
public class BlueGreenStateHandlerRegistry {

    private final Map<FlinkBlueGreenDeploymentState, BlueGreenStateHandler> handlers;

    public BlueGreenStateHandlerRegistry() {
        // Create consolidated service
        BlueGreenDeploymentService deploymentService = new BlueGreenDeploymentService();

        // Create handlers
        this.handlers =
                Map.of(
                        INITIALIZING_BLUE, new InitializingBlueStateHandler(deploymentService),
                        ACTIVE_BLUE, new ActiveStateHandler(ACTIVE_BLUE, deploymentService),
                        ACTIVE_GREEN, new ActiveStateHandler(ACTIVE_GREEN, deploymentService),
                        TRANSITIONING_TO_BLUE,
                                new TransitioningStateHandler(
                                        TRANSITIONING_TO_BLUE, deploymentService),
                        TRANSITIONING_TO_GREEN,
                                new TransitioningStateHandler(
                                        TRANSITIONING_TO_GREEN, deploymentService),
                        SAVEPOINTING_BLUE,
                                new SavepointingStateHandler(SAVEPOINTING_BLUE, deploymentService),
                        SAVEPOINTING_GREEN,
                                new SavepointingStateHandler(
                                        SAVEPOINTING_GREEN, deploymentService));
    }

    /**
     * Gets the appropriate handler for the given state.
     *
     * @param state the Blue/Green deployment state
     * @return the corresponding state handler
     * @throws IllegalStateException if no handler is found for the state
     */
    public BlueGreenStateHandler getHandler(FlinkBlueGreenDeploymentState state) {
        return handlers.get(state);
    }
}
