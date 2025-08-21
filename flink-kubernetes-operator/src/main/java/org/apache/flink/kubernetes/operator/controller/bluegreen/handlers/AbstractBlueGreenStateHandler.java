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

import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract base class providing common functionality for Blue/Green state handlers. */
public abstract class AbstractBlueGreenStateHandler implements BlueGreenStateHandler {

    private final FlinkBlueGreenDeploymentState supportedState;

    protected static final Logger LOG =
            LoggerFactory.getLogger(AbstractBlueGreenStateHandler.class);

    protected final BlueGreenDeploymentService deploymentService;

    protected AbstractBlueGreenStateHandler(
            FlinkBlueGreenDeploymentState supportedState,
            BlueGreenDeploymentService deploymentService) {
        this.supportedState = supportedState;
        this.deploymentService = deploymentService;
    }

    @Override
    public FlinkBlueGreenDeploymentState getSupportedState() {
        return supportedState;
    }
}
