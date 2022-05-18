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

package org.apache.flink.kubernetes.operator.observer.sessionjob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.TestingStatusHelper;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.reconciler.sessionjob.FlinkSessionJobReconciler;
import org.apache.flink.kubernetes.operator.utils.SavepointUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/** Tests for {@link SessionJobObserver}. */
@EnableKubernetesMockClient(crud = true)
public class SessionJobObserverTest {
    private KubernetesClient kubernetesClient;
    private final FlinkConfigManager configManager = new FlinkConfigManager(new Configuration());

    @Test
    public void testBasicObserve() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        final var flinkService = new TestingFlinkService();
        final var reconciler = new FlinkSessionJobReconciler(null, flinkService, configManager);
        final var observer =
                new SessionJobObserver(flinkService, configManager, new TestingStatusHelper<>());
        final var readyContext = TestUtils.createContextWithReadyFlinkDeployment();

        // observe the brand new job, nothing to do.
        observer.observe(sessionJob, readyContext);

        // submit job
        reconciler.reconcile(sessionJob, readyContext);
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());

        // observe with empty context will do nothing
        observer.observe(sessionJob, TestUtils.createEmptyContext());
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());

        // observe with ready context
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob.getStatus().getJobStatus().getState());

        flinkService.setPortReady(false);
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());

        sessionJob.getStatus().getJobStatus().setState(JobStatus.RUNNING.name());
        // no matched job id, update the state to unknown
        flinkService.setPortReady(true);
        sessionJob.getStatus().getJobStatus().setJobId(new JobID().toHexString());
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());
        sessionJob.getStatus().getJobStatus().setJobId(jobID);

        // testing multi job

        var sessionJob2 = TestUtils.buildSessionJob();
        // submit the second job
        reconciler.reconcile(sessionJob2, readyContext);
        var jobID2 = sessionJob2.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertNotEquals(jobID, jobID2);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());
        observer.observe(sessionJob2, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob2.getStatus().getJobStatus().getState());
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob.getStatus().getJobStatus().getState());
    }

    @Test
    public void testObserveWithEffectiveConfig() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        final var flinkService = new TestingFlinkService();
        final var reconciler = new FlinkSessionJobReconciler(null, flinkService, configManager);
        final var observer =
                new SessionJobObserver(flinkService, configManager, new TestingStatusHelper<>());
        final var readyContext =
                TestUtils.createContextWithReadyFlinkDeployment(
                        Map.of(RestOptions.PORT.key(), "8088"));

        // submit job
        reconciler.reconcile(sessionJob, readyContext);
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());

        flinkService.setListJobConsumer(
                configuration ->
                        Assertions.assertEquals(8088, configuration.getInteger(RestOptions.PORT)));
        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob.getStatus().getJobStatus().getState());
    }

    @Test
    public void testObserveSavepoint() throws Exception {
        final var sessionJob = TestUtils.buildSessionJob();
        final var flinkService = new TestingFlinkService(kubernetesClient);
        final var reconciler = new FlinkSessionJobReconciler(null, flinkService, configManager);
        final var observer =
                new SessionJobObserver(flinkService, configManager, new TestingStatusHelper<>());
        final var readyContext = TestUtils.createContextWithReadyFlinkDeployment();

        // submit job
        reconciler.reconcile(sessionJob, readyContext);
        var jobID = sessionJob.getStatus().getJobStatus().getJobId();
        Assertions.assertNotNull(jobID);
        Assertions.assertEquals(
                JobStatus.RECONCILING.name(), sessionJob.getStatus().getJobStatus().getState());

        observer.observe(sessionJob, readyContext);
        Assertions.assertEquals(
                JobStatus.RUNNING.name(), sessionJob.getStatus().getJobStatus().getState());

        var savepointInfo = sessionJob.getStatus().getJobStatus().getSavepointInfo();
        Assertions.assertFalse(
                SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));

        flinkService.triggerSavepoint(jobID, savepointInfo, new Configuration());
        Assertions.assertTrue(
                SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));
        Assertions.assertEquals("trigger_0", savepointInfo.getTriggerId());
        observer.observe(sessionJob, readyContext); // pending
        observer.observe(sessionJob, readyContext); // error

        flinkService.triggerSavepoint(jobID, savepointInfo, new Configuration());
        Assertions.assertTrue(
                SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));
        Assertions.assertEquals("trigger_1", savepointInfo.getTriggerId());
        flinkService.triggerSavepoint(jobID, savepointInfo, new Configuration());
        Assertions.assertTrue(
                SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));
        observer.observe(sessionJob, readyContext); // success
        Assertions.assertEquals("savepoint_0", savepointInfo.getLastSavepoint().getLocation());
        Assertions.assertFalse(
                SavepointUtils.savepointInProgress(sessionJob.getStatus().getJobStatus()));
    }
}
