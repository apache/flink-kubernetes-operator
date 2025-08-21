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

package org.apache.flink.kubernetes.operator.reconciler.diff;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.kubernetes.operator.api.bluegreen.BlueGreenDiffType;
import org.apache.flink.kubernetes.operator.api.diff.DiffType;
import org.apache.flink.kubernetes.operator.api.spec.FlinkBlueGreenDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;

import lombok.NonNull;

/**
 * Diff class for comparing FlinkBlueGreenDeploymentSpec objects. Provides specialized comparison
 * logic that delegates nested FlinkDeploymentSpec comparison to ReflectiveDiffBuilder.
 */
@Experimental
public class FlinkBlueGreenDeploymentSpecDiff {

    private final FlinkBlueGreenDeploymentSpec left;
    private final FlinkBlueGreenDeploymentSpec right;
    private final KubernetesDeploymentMode deploymentMode;

    public FlinkBlueGreenDeploymentSpecDiff(
            KubernetesDeploymentMode deploymentMode,
            @NonNull FlinkBlueGreenDeploymentSpec left,
            @NonNull FlinkBlueGreenDeploymentSpec right) {
        this.deploymentMode = deploymentMode;
        this.left = left;
        this.right = right;

        // Validate that neither spec is null
        validateSpecs();
    }

    /**
     * Compares the Blue/Green deployment specs and returns the appropriate diff type. The
     * comparison focuses solely on the nested FlinkDeploymentSpec differences.
     *
     * @return BlueGreenDiffType indicating the type of difference found
     */
    public BlueGreenDiffType compare() {
        FlinkDeploymentSpec leftSpec = left.getTemplate().getSpec();
        FlinkDeploymentSpec rightSpec = right.getTemplate().getSpec();

        // Case 1: FlinkDeploymentSpecs are identical
        if (leftSpec.equals(rightSpec)) {
            return BlueGreenDiffType.IGNORE;
        }

        // Case 2 & 3: Delegate to ReflectiveDiffBuilder for nested spec comparison
        DiffResult<FlinkDeploymentSpec> diffResult =
                new ReflectiveDiffBuilder<>(deploymentMode, leftSpec, rightSpec).build();

        DiffType diffType = diffResult.getType();

        // Case 2: ReflectiveDiffBuilder returns IGNORE
        if (diffType == DiffType.IGNORE) {
            return BlueGreenDiffType.PATCH_CHILD;
        } else {
            // Case 3: ReflectiveDiffBuilder returns anything else map it to TRANSITION as well
            return BlueGreenDiffType.TRANSITION;
        }
    }

    /**
     * Validates that the specs and their nested components are not null. Throws
     * IllegalArgumentException if any required component is null.
     */
    private void validateSpecs() {
        if (left.getTemplate() == null) {
            throw new IllegalArgumentException("Left spec template cannot be null");
        }
        if (right.getTemplate() == null) {
            throw new IllegalArgumentException("Right spec template cannot be null");
        }
        if (left.getTemplate().getSpec() == null) {
            throw new IllegalArgumentException("Left spec template.spec cannot be null");
        }
        if (right.getTemplate().getSpec() == null) {
            throw new IllegalArgumentException("Right spec template.spec cannot be null");
        }
    }
}
