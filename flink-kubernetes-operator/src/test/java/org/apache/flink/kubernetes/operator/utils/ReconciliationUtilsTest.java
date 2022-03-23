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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils}. */
public class ReconciliationUtilsTest {

    FlinkOperatorConfiguration operatorConfiguration =
            FlinkOperatorConfiguration.fromConfiguration(new Configuration());

    @Test
    public void testSpecChangedException() {
        FlinkDeployment previous = TestUtils.buildApplicationCluster();
        FlinkDeployment current = ReconciliationUtils.clone(previous);

        current.getSpec().setImage("changed-image");
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        ReconciliationUtils.toUpdateControl(
                                operatorConfiguration, previous, current, true));
    }

    @Test
    public void testStatusChanged() {
        FlinkDeployment previous = TestUtils.buildApplicationCluster();
        FlinkDeployment current = ReconciliationUtils.clone(previous);

        UpdateControl<FlinkDeployment> updateControl =
                ReconciliationUtils.toUpdateControl(
                        operatorConfiguration, previous, current, false);

        assertFalse(updateControl.isUpdateResource());
        assertFalse(updateControl.isUpdateStatus());
        assertTrue(updateControl.getScheduleDelay().isEmpty());

        // status changed
        current.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
        updateControl =
                ReconciliationUtils.toUpdateControl(operatorConfiguration, previous, current, true);
        assertFalse(updateControl.isUpdateResource());
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(
                operatorConfiguration.getProgressCheckInterval().toMillis(),
                updateControl.getScheduleDelay().get());
    }

    @Test
    public void testRescheduleUpgradeImmediately() {
        FlinkDeployment app = TestUtils.buildApplicationCluster();
        app.getSpec().getJob().setState(JobState.RUNNING);
        FlinkDeployment current = ReconciliationUtils.clone(app);
        current.getStatus()
                .getReconciliationStatus()
                .setLastReconciledSpec(ReconciliationUtils.clone(current.getSpec()));
        ReconciliationUtils.updateForSpecReconciliationSuccess(current, JobState.SUSPENDED);

        UpdateControl<FlinkDeployment> updateControl =
                ReconciliationUtils.toUpdateControl(operatorConfiguration, app, current, true);

        assertFalse(updateControl.isUpdateResource());
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(0, updateControl.getScheduleDelay().get());
    }
}
