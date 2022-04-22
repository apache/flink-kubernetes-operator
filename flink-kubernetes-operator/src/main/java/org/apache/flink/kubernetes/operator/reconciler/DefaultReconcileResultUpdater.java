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

import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;

/** The default updater for the reconciliation result. */
public class DefaultReconcileResultUpdater {

    public static final DefaultReconcileResultUpdater INSTANCE =
            new DefaultReconcileResultUpdater();

    public <SPEC extends ReconcileTarget.SpecView> void updateForSpecReconciliationSuccess(
            ReconcileTarget<SPEC> reconcileTarget, JobState stateAfterReconcile) {
        ReconciliationStatus<SPEC> reconciliationStatus = reconcileTarget.getReconcileStatus();
        reconcileTarget.handleError(null);

        SPEC originalSpec = reconcileTarget.getSpec();
        SPEC clonedSpec = ReconciliationUtils.clone(originalSpec);
        SPEC lastReconciledSpec = reconciliationStatus.deserializeLastReconciledSpec();
        if (lastReconciledSpec != null && lastReconciledSpec.getJobSpec() != null) {
            Long oldSavepointTriggerNonce =
                    lastReconciledSpec.getJobSpec().getSavepointTriggerNonce();
            clonedSpec.getJobSpec().setSavepointTriggerNonce(oldSavepointTriggerNonce);
            clonedSpec.getJobSpec().setState(stateAfterReconcile);
        }
        reconciliationStatus.serializeAndSetLastReconciledSpec(clonedSpec);
        reconciliationStatus.setReconciliationTimestamp(System.currentTimeMillis());
        reconciliationStatus.setState(ReconciliationState.DEPLOYED);

        if (originalSpec.getJobSpec() != null
                && originalSpec.getJobSpec().getState() == JobState.SUSPENDED) {
            // When a job is suspended by the user it is automatically marked stable
            reconciliationStatus.markReconciledSpecAsStable();
        }
    }

    public <SPEC extends ReconcileTarget.SpecView> void updateSavepointReconciliationSuccess(
            ReconcileTarget<SPEC> reconcileTarget) {
        ReconciliationStatus<SPEC> reconciliationStatus = reconcileTarget.getReconcileStatus();
        reconcileTarget.handleError(null);
        SPEC lastReconciledSpec = reconciliationStatus.deserializeLastReconciledSpec();
        lastReconciledSpec
                .getJobSpec()
                .setSavepointTriggerNonce(
                        reconcileTarget.getSpec().getJobSpec().getSavepointTriggerNonce());
        reconciliationStatus.serializeAndSetLastReconciledSpec(lastReconciledSpec);
        reconciliationStatus.setReconciliationTimestamp(System.currentTimeMillis());
    }

    public <SPEC extends ReconcileTarget.SpecView> void updateForReconciliationError(
            ReconcileTarget<SPEC> reconcileTarget, String error) {
        reconcileTarget.handleError(error);
    }
}
