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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.kubernetes.operator.OperatorTestBase;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.spec.JobState;
import org.apache.flink.kubernetes.operator.api.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
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

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    @Test
    public void testExceptionObservedEvenWhenNewStateIsTerminal() throws Exception {
        var deployment = initDeployment();
        var status = deployment.getStatus();
        var jobStatus = status.getJobStatus();
        jobStatus.setState(JobStatus.RUNNING);
        Map<String, String> configuration = new HashMap<>();
        configuration.put(
                KubernetesOperatorConfigOptions.OPERATOR_EVENT_EXCEPTION_LIMIT.key(), "2");
        Configuration operatorConfig = Configuration.fromMap(configuration);
        FlinkResourceContext<AbstractFlinkResource<?, ?>> ctx =
                getResourceContext(deployment, operatorConfig);

        var jobId = JobID.fromHexString(deployment.getStatus().getJobStatus().getJobId());
        ctx.getExceptionCacheEntry().setInitialized(true);
        ctx.getExceptionCacheEntry().setJobId(jobId.toHexString());
        ctx.getExceptionCacheEntry().setLastTimestamp(Instant.ofEpochMilli(500L));
        flinkService.addExceptionHistory(jobId, "ExceptionOne", "trace1", 1000L);

        // Ensure jobFailedErr is null before the observe call
        flinkService.submitApplicationCluster(
                deployment.getSpec().getJob(), ctx.getDeployConfig(deployment.getSpec()), false);
        flinkService.cancelJob(JobID.fromHexString(jobStatus.getJobId()), false);
        flinkService.setJobFailedErr(null);

        observer.observe(ctx);

        var events =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems();
        assertEquals(2, events.size()); // one will be for job status changed
        // assert that none of the events contain JOB_NOT_FOUND_ERR
        assertFalse(
                events.stream()
                        .anyMatch(
                                event ->
                                        event.getMessage()
                                                .contains(JobStatusObserver.JOB_NOT_FOUND_ERR)));
    }

    @Test
    public void testExceptionNotObservedWhenOldStateIsTerminal() throws Exception {
        var deployment = initDeployment();
        var status = deployment.getStatus();
        var jobStatus = status.getJobStatus();
        jobStatus.setState(JobStatus.CANCELED);
        Map<String, String> configuration = new HashMap<>();
        configuration.put(
                KubernetesOperatorConfigOptions.OPERATOR_EVENT_EXCEPTION_LIMIT.key(), "2");
        Configuration operatorConfig = Configuration.fromMap(configuration);
        FlinkResourceContext<AbstractFlinkResource<?, ?>> ctx =
                getResourceContext(deployment, operatorConfig);

        var jobId = JobID.fromHexString(deployment.getStatus().getJobStatus().getJobId());
        ctx.getExceptionCacheEntry().setInitialized(true);
        ctx.getExceptionCacheEntry().setJobId(jobId.toHexString());
        ctx.getExceptionCacheEntry().setLastTimestamp(Instant.ofEpochMilli(500L));
        flinkService.addExceptionHistory(jobId, "ExceptionOne", "trace1", 1000L);

        // Ensure jobFailedErr is null before the observe call
        flinkService.submitApplicationCluster(
                deployment.getSpec().getJob(), ctx.getDeployConfig(deployment.getSpec()), false);
        flinkService.setJobFailedErr(null);

        observer.observe(ctx);

        var events =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems();
        assertEquals(1, events.size()); // only one event for job status changed
        assertEquals(EventRecorder.Reason.JobStatusChanged.name(), events.get(0).getReason());
    }

    @Test
    public void testExceptionLimitConfig() throws Exception {
        var observer = new JobStatusObserver<>(eventRecorder);
        var deployment = initDeployment();
        var status = deployment.getStatus();
        var jobStatus = status.getJobStatus();
        jobStatus.setState(JobStatus.RUNNING);
        Map<String, String> configuration = new HashMap<>();
        configuration.put(
                KubernetesOperatorConfigOptions.OPERATOR_EVENT_EXCEPTION_LIMIT.key(), "2");
        Configuration operatorConfig = Configuration.fromMap(configuration);
        FlinkResourceContext<AbstractFlinkResource<?, ?>> ctx =
                getResourceContext(deployment, operatorConfig); // set a non-terminal state

        var jobId = JobID.fromHexString(deployment.getStatus().getJobStatus().getJobId());
        ctx.getExceptionCacheEntry().setInitialized(true);
        ctx.getExceptionCacheEntry().setJobId(jobId.toHexString());
        ctx.getExceptionCacheEntry().setLastTimestamp(Instant.ofEpochMilli(500L));

        flinkService.submitApplicationCluster(
                deployment.getSpec().getJob(), ctx.getDeployConfig(deployment.getSpec()), false);
        flinkService.addExceptionHistory(jobId, "ExceptionOne", "trace1", 1000L);
        flinkService.addExceptionHistory(jobId, "ExceptionTwo", "trace2", 2000L);
        flinkService.addExceptionHistory(jobId, "ExceptionThree", "trace3", 3000L);

        // Ensure jobFailedErr is null before the observe call
        flinkService.setJobFailedErr(null);

        observer.observe(ctx);

        var events =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems();
        assertEquals(2, events.size());
    }

    @Test
    public void testStackTraceTruncationConfig() throws Exception {
        var deployment = initDeployment();
        var status = deployment.getStatus();
        var jobStatus = status.getJobStatus();
        jobStatus.setState(JobStatus.RUNNING);
        Map<String, String> configuration = new HashMap<>();
        configuration.put(
                KubernetesOperatorConfigOptions.OPERATOR_EVENT_EXCEPTION_STACKTRACE_LINES.key(),
                "2");
        Configuration operatorConfig = Configuration.fromMap(configuration);
        FlinkResourceContext<AbstractFlinkResource<?, ?>> ctx =
                getResourceContext(deployment, operatorConfig);

        var jobId = JobID.fromHexString(deployment.getStatus().getJobStatus().getJobId());
        flinkService.submitApplicationCluster(
                deployment.getSpec().getJob(), ctx.getDeployConfig(deployment.getSpec()), false);
        ReconciliationUtils.updateStatusForDeployedSpec(deployment, new Configuration());
        ctx.getExceptionCacheEntry().setInitialized(true);
        ctx.getExceptionCacheEntry().setJobId(jobId.toHexString());
        ctx.getExceptionCacheEntry().setLastTimestamp(Instant.ofEpochMilli(3000L));

        long exceptionTime = 4000L;
        String longTrace = "line1\nline2\nline3\nline4";
        flinkService.addExceptionHistory(jobId, "StackTraceCheck", longTrace, exceptionTime);

        // Ensure jobFailedErr is null before the observe call
        flinkService.setJobFailedErr(null);
        observer.observe(ctx);

        var events =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems();
        assertEquals(1, events.size());
        String msg = events.get(0).getMessage();
        assertTrue(msg.contains("line1"));
        assertTrue(msg.contains("line2"));
        assertFalse(msg.contains("line3"));
        assertTrue(msg.contains("... (2 more lines)"));
    }

    @Test
    public void testIgnoreOldExceptions() throws Exception {
        var deployment = initDeployment();
        var status = deployment.getStatus();
        var jobStatus = status.getJobStatus();
        jobStatus.setState(JobStatus.RUNNING); // set a non-terminal state

        FlinkResourceContext<AbstractFlinkResource<?, ?>> ctx = getResourceContext(deployment);
        ctx.getExceptionCacheEntry().setInitialized(true);
        ctx.getExceptionCacheEntry().setJobId(deployment.getStatus().getJobStatus().getJobId());
        ctx.getExceptionCacheEntry().setLastTimestamp(Instant.ofEpochMilli(2500L));

        var jobId = JobID.fromHexString(deployment.getStatus().getJobStatus().getJobId());
        flinkService.submitApplicationCluster(
                deployment.getSpec().getJob(), ctx.getDeployConfig(deployment.getSpec()), false);
        // Map exception names to timestamps
        Map<String, Long> exceptionHistory =
                Map.of(
                        "OldException", 1000L,
                        "MidException", 2000L,
                        "NewException", 3000L);
        String dummyStackTrace =
                "org.apache.%s\n"
                        + "\tat org.apache.flink.kubernetes.operator.observer.JobStatusObserverTest.testIgnoreOldExceptions(JobStatusObserverTest.java:1)\n"
                        + "\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n"
                        + "\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\n"
                        + "\tat java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n"
                        + "\tat java.base/java.lang.reflect.Method.invoke(Method.java:566)\n";
        // Add mapped exceptions
        exceptionHistory.forEach(
                (exceptionName, timestamp) -> {
                    String fullStackTrace = String.format(dummyStackTrace, exceptionName);
                    flinkService.addExceptionHistory(
                            jobId, "org.apache." + exceptionName, fullStackTrace, timestamp);
                });

        // Ensure jobFailedErr is null before the observe call
        flinkService.setJobFailedErr(null);
        observer.observe(ctx);

        var events =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems();
        assertEquals(1, events.size());
        assertTrue(events.get(0).getMessage().contains("org.apache.NewException"));
    }

    @Test
    public void testExceptionEventTriggerInitialization() throws Exception {
        var deployment = initDeployment();
        var status = deployment.getStatus();
        var jobStatus = status.getJobStatus();
        jobStatus.setState(JobStatus.RUNNING); // set a non-terminal state

        FlinkResourceContext<AbstractFlinkResource<?, ?>> ctx = getResourceContext(deployment);

        var now = Instant.now();
        var jobId = JobID.fromHexString(deployment.getStatus().getJobStatus().getJobId());
        flinkService.submitApplicationCluster(
                deployment.getSpec().getJob(), ctx.getDeployConfig(deployment.getSpec()), false);

        // Old exception that happened outside of kubernetes event retention should be ignored
        flinkService.addExceptionHistory(
                jobId,
                "OldException",
                "OldException",
                now.minus(Duration.ofHours(1)).toEpochMilli());
        flinkService.addExceptionHistory(
                jobId,
                "NewException",
                "NewException",
                now.minus(Duration.ofMinutes(1)).toEpochMilli());

        // Ensure jobFailedErr is null before the observe call
        flinkService.setJobFailedErr(null);
        observer.observe(ctx);

        var events =
                kubernetesClient
                        .v1()
                        .events()
                        .inNamespace(deployment.getMetadata().getNamespace())
                        .list()
                        .getItems();
        assertEquals(1, events.size());
        assertTrue(events.get(0).getMessage().contains("NewException"));
        assertTrue(ctx.getExceptionCacheEntry().isInitialized());
        assertEquals(
                now.minus(Duration.ofMinutes(1)).truncatedTo(ChronoUnit.MILLIS),
                ctx.getExceptionCacheEntry().getLastTimestamp());
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
