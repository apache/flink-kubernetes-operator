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
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.api.utils.BaseTestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils}. */
public class ReconciliationUtilsTest {

    FlinkOperatorConfiguration operatorConfiguration =
            FlinkOperatorConfiguration.fromConfiguration(new Configuration());

    @Test
    public void testRescheduleUpgradeImmediately() {
        FlinkDeployment app = BaseTestUtils.buildApplicationCluster();
        app.getSpec().getJob().setState(JobState.RUNNING);
        app.getStatus().getReconciliationStatus().setState(ReconciliationState.DEPLOYED);
        ReconciliationUtils.updateStatusForDeployedSpec(app, new Configuration());
        FlinkDeployment previous = ReconciliationUtils.clone(app);
        FlinkDeployment current = ReconciliationUtils.clone(app);
        ReconciliationUtils.updateStatusBeforeDeploymentAttempt(current, new Configuration());

        UpdateControl<FlinkDeployment> updateControl =
                ReconciliationUtils.toUpdateControl(operatorConfiguration, current, previous, true);

        assertFalse(updateControl.isUpdateResource());
        assertFalse(updateControl.isUpdateStatus());
        assertEquals(0, updateControl.getScheduleDelay().get());

        updateControl =
                ReconciliationUtils.toUpdateControl(operatorConfiguration, current, current, true);

        assertFalse(updateControl.isUpdateResource());
        assertFalse(updateControl.isUpdateStatus());
        assertNotEquals(0, updateControl.getScheduleDelay().get());
    }

    @Test
    public void testRescheduleDuringScaling() {
        FlinkDeployment app = BaseTestUtils.buildApplicationCluster();
        app.getSpec().getJob().setState(JobState.RUNNING);
        app.getStatus().getReconciliationStatus().setState(ReconciliationState.DEPLOYED);
        var previous = ReconciliationUtils.clone(app);
        ReconciliationUtils.updateAfterScaleUp(
                app,
                new Configuration(),
                Clock.systemDefaultZone(),
                FlinkService.ScalingResult.SCALING_TRIGGERED);

        var updateControl =
                ReconciliationUtils.toUpdateControl(operatorConfiguration, app, previous, true);

        assertTrue(updateControl.getScheduleDelay().get() > 0);
    }

    @Test
    public void testRescheduleIfImmediateFlagSet() {
        var previous = BaseTestUtils.buildApplicationCluster();
        var current = BaseTestUtils.buildApplicationCluster();
        var updateControl =
                ReconciliationUtils.toUpdateControl(operatorConfiguration, current, previous, true);
        assertTrue(updateControl.getScheduleDelay().get() > 0);

        current.getStatus().setImmediateReconciliationNeeded(true);
        updateControl =
                ReconciliationUtils.toUpdateControl(operatorConfiguration, current, previous, true);
        assertEquals(0, updateControl.getScheduleDelay().get());
    }
}
