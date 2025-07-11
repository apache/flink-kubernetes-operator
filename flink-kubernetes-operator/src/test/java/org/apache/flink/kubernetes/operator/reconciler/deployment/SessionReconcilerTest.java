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

package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.TestingFlinkService;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.TestReconcilerAdapter;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link org.apache.flink.kubernetes.operator.reconciler.deployment.SessionReconciler}.
 */
@EnableKubernetesMockClient(crud = true)
public class SessionReconcilerTest extends OperatorTestBase {

    @Getter private KubernetesClient kubernetesClient;
    private TestReconcilerAdapter<FlinkDeployment, FlinkDeploymentSpec, FlinkDeploymentStatus>
            reconciler;

    @Override
    public void setup() {
        reconciler =
                new TestReconcilerAdapter<>(
                        this, new SessionReconciler(eventRecorder, statusRecorder));
    }

    @Test
    public void testStartSession() throws Exception {
        var count = new AtomicInteger(0);
        flinkService =
                new TestingFlinkService() {
                    @Override
                    public void submitSessionCluster(Configuration conf) throws Exception {
                        super.submitSessionCluster(conf);
                        count.addAndGet(1);
                    }
                };

        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        reconciler.reconcile(deployment, flinkService.getContext());
        assertEquals(1, count.get());
    }

    @Test
    public void testFailedUpgrade() throws Exception {
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        reconciler.reconcile(deployment, flinkService.getContext());

        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());

        deployment.getSpec().setRestartNonce(1234L);

        flinkService.setDeployFailure(true);
        try {
            reconciler.reconcile(deployment, flinkService.getContext());
            fail();
        } catch (Exception expected) {
        }

        assertEquals(
                ReconciliationState.UPGRADING,
                deployment.getStatus().getReconciliationStatus().getState());
        flinkService.setDeployFailure(false);
        flinkService.clear();
        assertTrue(flinkService.getSessions().isEmpty());
        reconciler.reconcile(deployment, flinkService.getContext());

        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());
        assertEquals(
                1234L,
                deployment
                        .getStatus()
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getRestartNonce());
        assertEquals(Set.of(deployment.getMetadata().getName()), flinkService.getSessions());
    }

    @Test
    public void testSetOwnerReference() throws Exception {
        FlinkDeployment flinkApp = TestUtils.buildApplicationCluster();
        ObjectMeta deployMeta = flinkApp.getMetadata();
        FlinkDeploymentStatus status = flinkApp.getStatus();
        FlinkDeploymentSpec spec = flinkApp.getSpec();
        Configuration deployConfig = configManager.getDeployConfig(deployMeta, spec);

        status.getJobStatus().setState(org.apache.flink.api.common.JobStatus.FINISHED);
        status.setJobManagerDeploymentStatus(JobManagerDeploymentStatus.READY);
        reconciler
                .getReconciler()
                .deploy(getResourceContext(flinkApp), spec, deployConfig, Optional.empty(), false);

        final List<Map<String, String>> expectedOwnerReferences =
                List.of(TestUtils.generateTestOwnerReferenceMap(flinkApp));
        List<Map<String, String>> or =
                deployConfig.get(KubernetesConfigOptions.JOB_MANAGER_OWNER_REFERENCE);
        Assertions.assertEquals(expectedOwnerReferences, or);
    }

    @Test
    public void testUnmanagedJobsIdentification() throws Exception {
        // Create a session cluster deployment with blocking enabled
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(KubernetesOperatorConfigOptions.BLOCK_ON_UNMANAGED_JOBS.key(), "true");

        assertEquals(
                "true",
                deployment
                        .getSpec()
                        .getFlinkConfiguration()
                        .get(KubernetesOperatorConfigOptions.BLOCK_ON_UNMANAGED_JOBS.key()));

        // First, deploy the session cluster to set up proper state
        reconciler.reconcile(deployment, flinkService.getContext());

        // Verify deployment is in DEPLOYED state
        assertEquals(
                ReconciliationState.DEPLOYED,
                deployment.getStatus().getReconciliationStatus().getState());

        // Create different types of jobs
        JobID managedJobId1 = new JobID();
        JobID managedJobId2 = new JobID();
        JobID unmanagedRunningJobId1 = new JobID();
        JobID unmanagedTerminatedJobId = new JobID();
        JobID unmanagedRunningJobId2 = new JobID();

        // Add a mix of managed and unmanaged jobs to the testing service
        flinkService
                .listJobs()
                .add(
                        Tuple3.of(
                                null,
                                new JobStatusMessage(
                                        managedJobId1,
                                        "managed-job-1",
                                        JobStatus.RUNNING,
                                        System.currentTimeMillis()),
                                new Configuration()));
        flinkService
                .listJobs()
                .add(
                        Tuple3.of(
                                null,
                                new JobStatusMessage(
                                        managedJobId2,
                                        "managed-job-2",
                                        JobStatus.RUNNING,
                                        System.currentTimeMillis()),
                                new Configuration()));
        flinkService
                .listJobs()
                .add(
                        Tuple3.of(
                                null,
                                new JobStatusMessage(
                                        unmanagedRunningJobId1,
                                        "unmanaged-running-job-1",
                                        JobStatus.RUNNING,
                                        System.currentTimeMillis()),
                                new Configuration()));
        flinkService
                .listJobs()
                .add(
                        Tuple3.of(
                                null,
                                new JobStatusMessage(
                                        unmanagedTerminatedJobId,
                                        "unmanaged-terminated-job",
                                        JobStatus.CANCELED,
                                        System.currentTimeMillis()),
                                new Configuration()));
        flinkService
                .listJobs()
                .add(
                        Tuple3.of(
                                null,
                                new JobStatusMessage(
                                        unmanagedRunningJobId2,
                                        "unmanaged-running-job-2",
                                        JobStatus.RUNNING,
                                        System.currentTimeMillis()),
                                new Configuration()));

        // Create FlinkSessionJob resources for the managed jobs
        FlinkSessionJob managedSessionJob1 = TestUtils.buildSessionJob();
        managedSessionJob1.getMetadata().setName("managed-session-job-1");
        managedSessionJob1.getStatus().getJobStatus().setJobId(managedJobId1.toHexString());
        kubernetesClient.resource(managedSessionJob1).createOrReplace();

        FlinkSessionJob managedSessionJob2 = TestUtils.buildSessionJob();
        managedSessionJob2.getMetadata().setName("managed-session-job-2");
        managedSessionJob2.getStatus().getJobStatus().setJobId(managedJobId2.toHexString());
        kubernetesClient.resource(managedSessionJob2).createOrReplace();

        Set<FlinkSessionJob> sessionJobs = new HashSet<>();
        sessionJobs.add(managedSessionJob1);
        sessionJobs.add(managedSessionJob2);

        // Test with blocking enabled - should identify running unmanaged jobs correctly
        var context = TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient);
        var resourceContext = getResourceContext(deployment, context);

        // Use reflection to access the private getUnmanagedJobs method
        var sessionReconciler = reconciler.getReconciler();
        var getUnmanagedJobsMethod =
                sessionReconciler
                        .getClass()
                        .getDeclaredMethod(
                                "getUnmanagedJobs", FlinkResourceContext.class, Set.class);
        getUnmanagedJobsMethod.setAccessible(true);

        @SuppressWarnings("unchecked")
        var unmanagedJobs =
                (java.util.Set<String>)
                        getUnmanagedJobsMethod.invoke(
                                sessionReconciler, resourceContext, sessionJobs);

        // Verify only RUNNING unmanaged jobs are identified - should be 2
        assertEquals(2, unmanagedJobs.size(), "Should identify exactly 2 running unmanaged jobs");

        // Verify that none of the managed job IDs are in the unmanaged list
        Set<String> managedJobIds =
                sessionJobs.stream()
                        .map(job -> job.getStatus().getJobStatus().getJobId())
                        .collect(Collectors.toSet());

        for (String managedJobId : managedJobIds) {
            assertFalse(
                    unmanagedJobs.contains(managedJobId),
                    "Managed job " + managedJobId + " should NOT be in unmanaged jobs");
        }

        // Verify managed jobs are properly set up (we should have 2 managed jobs)
        assertEquals(2, managedJobIds.size(), "Should have 2 managed jobs configured");

        // Test scenario with no running unmanaged jobs
        flinkService
                .listJobs()
                .removeIf(
                        job ->
                                job.f1.getJobId().equals(unmanagedRunningJobId1)
                                        || job.f1.getJobId().equals(unmanagedRunningJobId2));

        @SuppressWarnings("unchecked")
        var unmanagedJobsAfterRemoval =
                (java.util.Set<String>)
                        getUnmanagedJobsMethod.invoke(
                                sessionReconciler, resourceContext, sessionJobs);

        assertEquals(
                0,
                unmanagedJobsAfterRemoval.size(),
                "Should have no unmanaged jobs when only managed and terminated jobs exist");
    }
}
