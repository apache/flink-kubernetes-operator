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

import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeployments;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Simplified context object containing all the necessary state and dependencies for Blue/Green
 * deployment state transitions.
 */
@Getter
@RequiredArgsConstructor
public class BlueGreenContext {
    private final FlinkBlueGreenDeployment bgDeployment;
    private final FlinkBlueGreenDeploymentStatus deploymentStatus;
    private final Context<FlinkBlueGreenDeployment> josdkContext;
    private final FlinkBlueGreenDeployments deployments;
    private final FlinkResourceContextFactory ctxFactory;

    public String getDeploymentName() {
        return bgDeployment.getMetadata().getName();
    }

    public FlinkDeployment getBlueDeployment() {
        return deployments != null ? deployments.getFlinkDeploymentBlue() : null;
    }

    public FlinkDeployment getGreenDeployment() {
        return deployments != null ? deployments.getFlinkDeploymentGreen() : null;
    }

    public FlinkDeployment getDeploymentByType(BlueGreenDeploymentType type) {
        return type == BlueGreenDeploymentType.BLUE ? getBlueDeployment() : getGreenDeployment();
    }

    public BlueGreenDeploymentType getOppositeDeploymentType(BlueGreenDeploymentType type) {
        return type == BlueGreenDeploymentType.BLUE
                ? BlueGreenDeploymentType.GREEN
                : BlueGreenDeploymentType.BLUE;
    }
}
