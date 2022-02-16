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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** @link JobStatusObserver unit tests */
public class JobReconcilerTest {

    @Test
    public void testUpgrade() throws Exception {
        TestingFlinkService flinkService = new TestingFlinkService();

        JobReconciler reconciler = new JobReconciler(null, flinkService);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        Configuration config = FlinkUtils.getEffectiveConfig(deployment);

        reconciler.reconcile("test", deployment, config);
        List<Tuple2<String, JobStatusMessage>> runningJobs = flinkService.listJobs();
        assertEquals(1, runningJobs.size());
        assertNull(runningJobs.get(0).f0);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastReconciledSpec(deployment.getSpec());

        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobName(runningJobs.get(0).f1.getJobName());
        jobStatus.setJobId(runningJobs.get(0).f1.getJobId().toHexString());
        jobStatus.setState("RUNNING");

        deployment.getStatus().setJobStatus(jobStatus);

        // Test stateless upgrade
        FlinkDeployment statelessUpgrade = TestUtils.clone(deployment);
        statelessUpgrade.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        statelessUpgrade.getSpec().getFlinkConfiguration().put("new", "conf");
        reconciler.reconcile("test", statelessUpgrade, config);

        runningJobs = flinkService.listJobs();
        assertEquals(1, runningJobs.size());
        assertNull(runningJobs.get(0).f0);

        deployment
                .getStatus()
                .getJobStatus()
                .setJobId(runningJobs.get(0).f1.getJobId().toHexString());

        // Test stateful upgrade
        FlinkDeployment statefulUpgrade = TestUtils.clone(deployment);
        statefulUpgrade.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        statefulUpgrade.getSpec().getFlinkConfiguration().put("new", "conf2");

        reconciler.reconcile("test", statefulUpgrade, new Configuration(config));

        runningJobs = flinkService.listJobs();
        assertEquals(1, runningJobs.size());
        assertEquals(TestingFlinkService.SAVEPOINT, runningJobs.get(0).f0);
    }
}
