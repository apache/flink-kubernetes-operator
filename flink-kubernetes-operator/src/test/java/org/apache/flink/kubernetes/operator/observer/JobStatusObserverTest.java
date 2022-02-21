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
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link JobStatusObserver unit tests */
public class JobStatusObserverTest {

    @Test
    public void observeSessionCluster() throws Exception {
        FlinkService flinkService = new TestingFlinkService();
        JobStatusObserver observer = new JobStatusObserver(flinkService);
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastReconciledSpec(deployment.getSpec());
        assertTrue(
                observer.observeFlinkJobStatus(
                        deployment, FlinkUtils.getEffectiveConfig(deployment)));
    }

    @Test
    public void observeApplicationCluster() throws Exception {
        TestingFlinkService flinkService = new TestingFlinkService();
        JobStatusObserver observer = new JobStatusObserver(flinkService);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        Configuration conf = FlinkUtils.getEffectiveConfig(deployment);

        assertTrue(observer.observeFlinkJobStatus(deployment, conf));
        deployment.setStatus(new FlinkDeploymentStatus());
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastReconciledSpec(deployment.getSpec());

        assertFalse(observer.observeFlinkJobStatus(deployment, conf));

        flinkService.submitApplicationCluster(deployment, conf);
        assertTrue(observer.observeFlinkJobStatus(deployment, conf));

        assertEquals(
                deployment.getMetadata().getName(),
                deployment.getStatus().getJobStatus().getJobName());

        deployment.getSpec().getJob().setState(JobState.SUSPENDED);
        flinkService.clear();
        assertTrue(observer.observeFlinkJobStatus(deployment, conf));
    }
}
