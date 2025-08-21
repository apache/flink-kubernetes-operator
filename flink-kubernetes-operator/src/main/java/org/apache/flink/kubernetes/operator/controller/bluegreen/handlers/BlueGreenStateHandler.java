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
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

/** Interface for handling specific Blue/Green deployment states. */
public interface BlueGreenStateHandler {

    /**
     * Handles the processing logic for a specific Blue/Green deployment state.
     *
     * @param context the transition context containing all necessary dependencies
     * @return UpdateControl indicating the next action
     */
    UpdateControl<FlinkBlueGreenDeployment> handle(BlueGreenContext context);

    /**
     * Gets the deployment state that this handler supports.
     *
     * @return the supported FlinkBlueGreenDeploymentState
     */
    FlinkBlueGreenDeploymentState getSupportedState();

    /**
     * Validates if this handler can process the given state.
     *
     * @param state the state to validate
     * @return true if this handler can process the state, false otherwise
     */
    default boolean canHandle(FlinkBlueGreenDeploymentState state) {
        return getSupportedState() == state;
    }
}
