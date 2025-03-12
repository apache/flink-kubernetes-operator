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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

/** Utility to handle A/B deployments. */
@Data
@NoArgsConstructor
class FlinkBlueGreenDeployments {
    private FlinkDeployment flinkDeploymentBlue;
    private FlinkDeployment flinkDeploymentGreen;

    static FlinkBlueGreenDeployments fromSecondaryResources(
            Context<FlinkBlueGreenDeployment> context) {
        Set<FlinkDeployment> secondaryResources =
                context.getSecondaryResources(FlinkDeployment.class);

        if (secondaryResources.isEmpty() || secondaryResources.size() > 2) {
            FlinkBlueGreenDeploymentController.logAndThrow(
                    "Unexpected number of dependent deployments: " + secondaryResources.size());
        }

        FlinkBlueGreenDeployments flinkBlueGreenDeployments = new FlinkBlueGreenDeployments();

        for (FlinkDeployment dependentDeployment : secondaryResources) {
            var flinkBlueGreenDeploymentType = DeploymentType.fromDeployment(dependentDeployment);

            if (flinkBlueGreenDeploymentType == DeploymentType.BLUE) {
                if (flinkBlueGreenDeployments.getFlinkDeploymentBlue() != null) {
                    FlinkBlueGreenDeploymentController.logAndThrow(
                            "Detected multiple Dependent Deployments of type BLUE");
                }
                flinkBlueGreenDeployments.setFlinkDeploymentBlue(dependentDeployment);
            } else {
                if (flinkBlueGreenDeployments.getFlinkDeploymentGreen() != null) {
                    FlinkBlueGreenDeploymentController.logAndThrow(
                            "Detected multiple Dependent Deployments of type GREEN");
                }
                flinkBlueGreenDeployments.setFlinkDeploymentGreen(dependentDeployment);
            }
        }

        return flinkBlueGreenDeployments;
    }
}
