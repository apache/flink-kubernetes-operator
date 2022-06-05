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

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.TestingStatusHelper;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.crd.status.Savepoint;
import org.apache.flink.kubernetes.operator.crd.status.SavepointInfo;
import org.apache.flink.kubernetes.operator.crd.status.SavepointTriggerType;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link SavepointObserver}. */
public class SavepointObserverTest {

    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());
    private final TestingFlinkService flinkService = new TestingFlinkService();

    @Test
    public void testBasicObserve() {
        SavepointObserver observer =
                new SavepointObserver(flinkService, configManager, new TestingStatusHelper<>());
        SavepointInfo spInfo = new SavepointInfo();
        Assertions.assertTrue(spInfo.getSavepointHistory().isEmpty());

        Savepoint sp = new Savepoint(1, "sp1", SavepointTriggerType.MANUAL);
        spInfo.updateLastSavepoint(sp);
        observer.cleanupSavepointHistory(spInfo, sp, configManager.getDefaultConfig());

        Assertions.assertNotNull(spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.singletonList(sp), spInfo.getSavepointHistory());
    }

    @Test
    public void testAgeBasedDispose() {
        Configuration conf = new Configuration();
        conf.set(
                KubernetesOperatorConfigOptions.OPERATOR_SAVEPOINT_HISTORY_MAX_AGE,
                Duration.ofMillis(5));

        SavepointObserver observer =
                new SavepointObserver(flinkService, configManager, new TestingStatusHelper<>());
        SavepointInfo spInfo = new SavepointInfo();

        Savepoint sp1 = new Savepoint(1, "sp1", SavepointTriggerType.MANUAL);
        spInfo.updateLastSavepoint(sp1);
        observer.cleanupSavepointHistory(spInfo, sp1, conf);
        Assertions.assertIterableEquals(
                Collections.singletonList(sp1), spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.emptyList(), flinkService.getDisposedSavepoints());

        Savepoint sp2 = new Savepoint(2, "sp2", SavepointTriggerType.MANUAL);
        spInfo.updateLastSavepoint(sp2);
        observer.cleanupSavepointHistory(spInfo, sp2, conf);
        Assertions.assertIterableEquals(
                Collections.singletonList(sp2), spInfo.getSavepointHistory());
        Assertions.assertIterableEquals(
                Collections.singletonList(sp1.getLocation()), flinkService.getDisposedSavepoints());
    }

    @Test
    public void testPeriodicSavepoint() throws Exception {
        var conf = new Configuration();
        var deployment = TestUtils.buildApplicationCluster();
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec());

        var jobStatus = deployment.getStatus().getJobStatus();
        jobStatus.setState("RUNNING");

        var savepointInfo = jobStatus.getSavepointInfo();
        var observer =
                new SavepointObserver(flinkService, configManager, new TestingStatusHelper<>());
        flinkService.triggerSavepoint(null, SavepointTriggerType.PERIODIC, savepointInfo, conf);

        var triggerTs = savepointInfo.getTriggerTimestamp();
        assertEquals(0L, savepointInfo.getLastPeriodicSavepointTimestamp());
        assertEquals(SavepointTriggerType.PERIODIC, savepointInfo.getTriggerType());
        assertTrue(SavepointUtils.savepointInProgress(jobStatus));
        assertTrue(triggerTs > 0);

        // Pending
        observer.observeSavepointStatus(deployment, conf);
        // Completed
        observer.observeSavepointStatus(deployment, conf);
        assertEquals(triggerTs, savepointInfo.getLastPeriodicSavepointTimestamp());
        assertFalse(SavepointUtils.savepointInProgress(jobStatus));
        assertEquals(savepointInfo.getLastSavepoint(), savepointInfo.getSavepointHistory().get(0));
        assertEquals(
                SavepointTriggerType.PERIODIC, savepointInfo.getLastSavepoint().getTriggerType());
    }
}
