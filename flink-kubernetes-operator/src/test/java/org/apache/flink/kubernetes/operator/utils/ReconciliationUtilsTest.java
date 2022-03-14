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

import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.observer.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils}. */
public class ReconciliationUtilsTest {

    @Test
    public void testSpecChangedException() {
        FlinkDeployment previous = TestUtils.buildApplicationCluster();
        FlinkDeployment current =
                org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils.clone(previous);

        current.getSpec().setImage("changed-image");
        assertThrows(
                UnsupportedOperationException.class,
                () -> ReconciliationUtils.toUpdateControl(previous, current));
    }

    @Test
    public void testStatusChanged() {
        FlinkDeployment previous = TestUtils.buildApplicationCluster();
        FlinkDeployment current =
                org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils.clone(previous);

        UpdateControl<FlinkDeployment> updateControl =
                ReconciliationUtils.toUpdateControl(previous, current);

        assertFalse(updateControl.isUpdateResource());
        assertFalse(updateControl.isUpdateStatus());
        assertTrue(updateControl.getScheduleDelay().isEmpty());

        // status changed
        current.getStatus().setJobManagerDeploymentStatus(JobManagerDeploymentStatus.DEPLOYING);
        updateControl =
                ReconciliationUtils.toUpdateControl(previous, current)
                        .rescheduleAfter(10, TimeUnit.MILLISECONDS);
        assertFalse(updateControl.isUpdateResource());
        assertTrue(updateControl.isUpdateStatus());
        assertEquals(10, updateControl.getScheduleDelay().get());
    }
}
