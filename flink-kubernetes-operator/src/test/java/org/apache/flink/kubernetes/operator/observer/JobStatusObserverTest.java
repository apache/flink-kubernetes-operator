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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.util.SerializedThrowable;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for the {@link JobStatusObserver}. */
@EnableKubernetesMockClient(crud = true)
public class JobStatusObserverTest extends OperatorTestBase {

    @Getter private KubernetesClient kubernetesClient;
    private JobStatusObserver<AbstractFlinkResource<?, ?>> observer;

    @Override
    protected void setup() {
        observer = new JobStatusObserver<>(eventRecorder);
    }

    @ParameterizedTest
    @MethodSource("cancellingArgs")
    void testCancellingToMissing(
            JobStatus fromStatus, UpgradeMode upgradeMode, JobState expectedAfter) {
        var job = initSessionJob();
        job.getSpec().getJob().setUpgradeMode(upgradeMode);
        var status = job.getStatus();
        var jobStatus = status.getJobStatus();
        jobStatus.setState(fromStatus);
        assertEquals(
                JobState.RUNNING,
                status.getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        observer.observe(
                getResourceContext(
                        job, TestUtils.createContextWithReadyFlinkDeployment(kubernetesClient)));
        assertEquals(
                JobStatusObserver.JOB_NOT_FOUND_ERR,
                flinkResourceEventCollector.events.poll().getMessage());
        assertEquals(
                expectedAfter,
                status.getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
    }

    @ParameterizedTest
    @EnumSource(value = JobStatus.class, mode = EnumSource.Mode.EXCLUDE, names = "CANCELED")
    void testCancellingToTerminal(JobStatus fromStatus) throws Exception {
        var observer = new JobStatusObserver<>(eventRecorder);
        var deployment = initDeployment();
        var status = deployment.getStatus();
        var jobStatus = status.getJobStatus();
        jobStatus.setState(fromStatus);
        assertEquals(
                JobState.RUNNING,
                status.getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
        FlinkResourceContext<AbstractFlinkResource<?, ?>> ctx = getResourceContext(deployment);
        flinkService.submitApplicationCluster(
                deployment.getSpec().getJob(), ctx.getDeployConfig(deployment.getSpec()), false);
        flinkService.cancelJob(JobID.fromHexString(jobStatus.getJobId()), false);
        observer.observe(ctx);
        assertEquals(
                EventRecorder.Reason.JobStatusChanged.name(),
                flinkResourceEventCollector.events.poll().getReason());
        assertEquals(
                JobState.SUSPENDED,
                status.getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState());
    }

    @Test
    void testFailed() throws Exception {
        var observer = new JobStatusObserver<>(eventRecorder);
        var deployment = initDeployment();
        var status = deployment.getStatus();
        var jobStatus = status.getJobStatus();
        jobStatus.setState(JobStatus.RUNNING);
        FlinkResourceContext<AbstractFlinkResource<?, ?>> ctx = getResourceContext(deployment);
        flinkService.submitApplicationCluster(
                deployment.getSpec().getJob(), ctx.getDeployConfig(deployment.getSpec()), false);

        // Mark failed
        flinkService.setJobFailedErr(
                new Exception("job err", new SerializedThrowable(new Exception("root"))));
        observer.observe(ctx);

        // First event should be job error reported
        var jobErrorEvent = flinkResourceEventCollector.events.poll();
        assertEquals(EventRecorder.Reason.Error.name(), jobErrorEvent.getReason());
        assertEquals("job err -> root", jobErrorEvent.getMessage());

        // Make sure job status still reported
        assertEquals(
                EventRecorder.Reason.JobStatusChanged.name(),
                flinkResourceEventCollector.events.poll().getReason());

        observer.observe(ctx);
        assertTrue(flinkResourceEventCollector.events.isEmpty());
    }

    private static Stream<Arguments> cancellingArgs() {
        var args = new ArrayList<Arguments>();
        for (var status : JobStatus.values()) {
            for (var upgradeMode : UpgradeMode.values()) {
                args.add(
                        Arguments.of(
                                status,
                                upgradeMode,
                                upgradeMode == UpgradeMode.STATELESS
                                                && !status.isGloballyTerminalState()
                                        ? JobState.SUSPENDED
                                        : JobState.RUNNING));
            }
        }
        return args.stream();
    }

    private static FlinkDeployment initDeployment() {
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        var jobId = new JobID().toHexString();
        deployment
                .getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID.key(), jobId);
        deployment.getStatus().getJobStatus().setJobId(jobId);
        deployment
                .getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(deployment.getSpec(), deployment);
        return deployment;
    }

    private static FlinkSessionJob initSessionJob() {
        var job = TestUtils.buildSessionJob();
        var jobId = new JobID().toHexString();
        job.getSpec()
                .getFlinkConfiguration()
                .put(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID.key(), jobId);
        job.getStatus().getJobStatus().setJobId(jobId);
        job.getStatus()
                .getReconciliationStatus()
                .serializeAndSetLastReconciledSpec(job.getSpec(), job);
        return job;
    }
}
