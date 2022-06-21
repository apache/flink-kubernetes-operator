/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.config;

import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.KubernetesDeploymentMode;

/** The mode of {@link FlinkDeployment}. */
public enum Mode {
    NATIVE_APPLICATION,
    NATIVE_SESSION,
    STANDALONE_APPLICATION,
    STANDALONE_SESSION;

    /**
     * Return the mode of the given FlinkDeployment for Observer and Reconciler. Note, switching
     * mode for an existing deployment is not allowed.
     *
     * @param flinkApp given FlinkDeployment
     * @return Mode
     */
    public static Mode getMode(FlinkDeployment flinkApp) {
        // Try to use lastReconciledSpec if it exists.
        // The mode derived from last-reconciled spec or current spec should be same.
        // If they are different, observation phase will use last-reconciled spec and validation
        // phase will fail.
        FlinkDeploymentSpec lastReconciledSpec =
                flinkApp.getStatus().getReconciliationStatus().deserializeLastReconciledSpec();
        return lastReconciledSpec == null
                ? getMode(flinkApp.getSpec())
                : getMode(lastReconciledSpec);
    }

    private static Mode getMode(FlinkDeploymentSpec spec) {
        KubernetesDeploymentMode deploymentMode = spec.getMode();
        if (deploymentMode == null || deploymentMode == KubernetesDeploymentMode.NATIVE) {
            return spec.getJob() != null ? NATIVE_APPLICATION : NATIVE_SESSION;
        }
        return spec.getJob() != null ? STANDALONE_APPLICATION : STANDALONE_SESSION;
    }
}
