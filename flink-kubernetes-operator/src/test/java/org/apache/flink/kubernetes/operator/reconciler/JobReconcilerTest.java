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
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.observer.Observer;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** @link JobStatusObserver unit tests */
public class JobReconcilerTest {

    public static Context createContextWithReadyJobManagerDeployment() {
        return new Context() {
            @Override
            public Optional<RetryInfo> getRetryInfo() {
                return Optional.empty();
            }

            @Override
            public <T> Optional<T> getSecondaryResource(
                    Class<T> expectedType, String eventSourceName) {
                DeploymentStatus status = new DeploymentStatus();
                status.setAvailableReplicas(1);
                status.setReplicas(1);
                DeploymentSpec spec = new DeploymentSpec();
                spec.setReplicas(1);
                Deployment deployment = new Deployment();
                deployment.setSpec(spec);
                deployment.setStatus(status);
                return Optional.of((T) deployment);
            }
        };
    }

    @Test
    public void testUpgrade() throws Exception {
        Context context = JobReconcilerTest.createContextWithReadyJobManagerDeployment();
        TestingFlinkService flinkService = new TestingFlinkService();

        JobReconciler reconciler = new JobReconciler(null, flinkService);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        Configuration config = FlinkUtils.getEffectiveConfig(deployment, new Configuration());

        reconciler.reconcile("test", deployment, context, config);
        List<Tuple2<String, JobStatusMessage>> runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        // Test stateless upgrade
        FlinkDeployment statelessUpgrade = TestUtils.clone(deployment);
        statelessUpgrade.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        statelessUpgrade.getSpec().getFlinkConfiguration().put("new", "conf");
        reconciler.reconcile("test", statelessUpgrade, context, config);

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

        reconciler.reconcile("test", statefulUpgrade, context, new Configuration(config));

        runningJobs = flinkService.listJobs();
        assertEquals(1, runningJobs.size());
        assertEquals("savepoint_0", runningJobs.get(0).f0);
    }

    @Test
    public void testUpgradeModeChangeFromSavepointToLastState() throws Exception {
        final String expectedSavepointPath = "savepoint_0";
        final Context context = JobReconcilerTest.createContextWithReadyJobManagerDeployment();
        final TestingFlinkService flinkService = new TestingFlinkService();
        Observer observer = new Observer(flinkService);

        final JobReconciler reconciler = new JobReconciler(null, flinkService);
        final FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        final Configuration config = FlinkUtils.getEffectiveConfig(deployment, new Configuration());

        reconciler.reconcile("test", deployment, context, config);
        List<Tuple2<String, JobStatusMessage>> runningJobs = flinkService.listJobs();
        verifyAndSetRunningJobsToStatus(deployment, runningJobs);

        // Suspend FlinkDeployment with savepoint upgrade mode
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        deployment.getSpec().getJob().setState(JobState.SUSPENDED);
        deployment.getSpec().setImage("new-image-1");

        reconciler.reconcile("test", deployment, context, config);
        assertEquals(0, flinkService.listJobs().size());
        assertTrue(
                JobState.SUSPENDED
                        .name()
                        .equalsIgnoreCase(deployment.getStatus().getJobStatus().getState()));
        assertEquals(
                expectedSavepointPath,
                deployment.getStatus().getJobStatus().getSavepointLocation());

        // Resume FlinkDeployment with last-state upgrade mode
        deployment
                .getStatus()
                .getReconciliationStatus()
                .getLastReconciledSpec()
                .getJob()
                .setState(JobState.SUSPENDED);
        deployment.getSpec().getJob().setUpgradeMode(UpgradeMode.LAST_STATE);
        deployment.getSpec().getJob().setState(JobState.RUNNING);
        deployment.getSpec().setImage("new-image-2");

        reconciler.reconcile("test", deployment, context, config);
        runningJobs = flinkService.listJobs();
        assertEquals(expectedSavepointPath, config.get(SavepointConfigOptions.SAVEPOINT_PATH));
        assertEquals(1, runningJobs.size());
        assertEquals(expectedSavepointPath, runningJobs.get(0).f0);
    }

    private void verifyAndSetRunningJobsToStatus(
            FlinkDeployment deployment, List<Tuple2<String, JobStatusMessage>> runningJobs) {
        assertEquals(1, runningJobs.size());
        assertNull(runningJobs.get(0).f0);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .setLastReconciledSpec(TestUtils.clone(deployment.getSpec()));

        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobName(runningJobs.get(0).f1.getJobName());
        jobStatus.setJobId(runningJobs.get(0).f1.getJobId().toHexString());
        jobStatus.setState("RUNNING");

        deployment.getStatus().setJobStatus(jobStatus);
    }
}
