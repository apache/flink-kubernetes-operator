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

import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.status.ReconciliationStatus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Reconciliation utilities. */
public class ReconciliationUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void updateForSpecReconciliationSuccess(FlinkDeployment flinkApp) {
        ReconciliationStatus reconciliationStatus = flinkApp.getStatus().getReconciliationStatus();
        reconciliationStatus.setSuccess(true);
        reconciliationStatus.setError(null);
        FlinkDeploymentSpec clonedSpec = clone(flinkApp.getSpec());
        if (reconciliationStatus.getLastReconciledSpec() != null
                && reconciliationStatus.getLastReconciledSpec().getJob() != null) {
            Long oldSavepointTriggerNonce =
                    reconciliationStatus
                            .getLastReconciledSpec()
                            .getJob()
                            .getSavepointTriggerNonce();
            clonedSpec.getJob().setSavepointTriggerNonce(oldSavepointTriggerNonce);
        }
        reconciliationStatus.setLastReconciledSpec(clonedSpec);
    }

    public static void updateSavepointReconciliationSuccess(FlinkDeployment flinkApp) {
        ReconciliationStatus reconciliationStatus = flinkApp.getStatus().getReconciliationStatus();
        reconciliationStatus.setSuccess(true);
        reconciliationStatus.setError(null);
        reconciliationStatus
                .getLastReconciledSpec()
                .getJob()
                .setSavepointTriggerNonce(flinkApp.getSpec().getJob().getSavepointTriggerNonce());
    }

    public static void updateForReconciliationError(FlinkDeployment flinkApp, String err) {
        ReconciliationStatus reconciliationStatus = flinkApp.getStatus().getReconciliationStatus();
        reconciliationStatus.setSuccess(false);
        reconciliationStatus.setError(err);
    }

    public static <T> T clone(T object) {
        if (object == null) {
            return null;
        }
        try {
            return (T)
                    objectMapper.readValue(
                            objectMapper.writeValueAsString(object), object.getClass());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
