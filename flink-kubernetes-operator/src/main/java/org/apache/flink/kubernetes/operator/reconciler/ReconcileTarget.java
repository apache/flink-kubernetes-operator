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

package org.apache.flink.kubernetes.operator.reconciler;

import org.apache.flink.kubernetes.operator.crd.spec.JobSpec;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;

import javax.annotation.Nullable;

/**
 * The interface is responsible to handle the reconciliation result. For the common logic, it
 * provides method to extract the common view between the {@link
 * org.apache.flink.kubernetes.operator.crd.FlinkDeployment} and {@link
 * org.apache.flink.kubernetes.operator.crd.FlinkSessionJob} to simplify the custom resource
 * manipulation. For the special part of each custom resource, we can extend the interface to let
 * the target custom resource react to the reconciliation result correspondingly.
 *
 * @param <SPEC> the common view of the custom resource getSpec
 */
public interface ReconcileTarget<SPEC extends ReconcileTarget.SpecView> {

    /** The common view of the spec. */
    interface SpecView {
        JobSpec getJobSpec();
    }

    /**
     * Get the current getSpec of the custom resource.
     *
     * @return the current getSpec.
     */
    SPEC getSpec();

    /**
     * Get the current reconciliation status.
     *
     * @return the current reconciliation status.
     */
    ReconciliationStatus<SPEC> getReconcileStatus();

    /**
     * Let the target custom resource handle the reconciliation error.
     *
     * @param error The error to be handled.
     */
    void handleError(@Nullable String error);
}
