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
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.client.JobStatusMessage;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** @link JobStatusObserver unit tests */
public class JobStatusObserverTest {

    public static final String JOB_NAME = "test1";

    private FlinkService flinkService = Mockito.mock(FlinkService.class);

    @Test
    public void observeSessionCluster() {
        JobStatusObserver observer = new JobStatusObserver(flinkService);
        FlinkDeployment deployment = TestUtils.buildSessionCluster();
        deployment.setStatus(new FlinkDeploymentStatus());
        deployment.getStatus().setSpec(deployment.getSpec());
        assertTrue(
                observer.observeFlinkJobStatus(
                        deployment, FlinkUtils.getEffectiveConfig(deployment)));
    }

    @Test
    public void observeApplicationCluster() throws Exception {
        JobStatusObserver observer = new JobStatusObserver(flinkService);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        assertTrue(
                observer.observeFlinkJobStatus(
                        deployment, FlinkUtils.getEffectiveConfig(deployment)));
        deployment.setStatus(new FlinkDeploymentStatus());
        deployment.getStatus().setSpec(deployment.getSpec());
        verify(flinkService, times(0)).listJobs(any(Configuration.class));

        when(flinkService.listJobs(any(Configuration.class))).thenReturn(Collections.emptyList());
        assertFalse(
                observer.observeFlinkJobStatus(
                        deployment, FlinkUtils.getEffectiveConfig(deployment)));
        verify(flinkService, times(1)).listJobs(any(Configuration.class));

        when(flinkService.listJobs(any(Configuration.class)))
                .thenReturn(
                        Arrays.asList(
                                new JobStatusMessage(
                                        new JobID(), JOB_NAME, JobStatus.RUNNING, 1L)));
        assertTrue(
                observer.observeFlinkJobStatus(
                        deployment, FlinkUtils.getEffectiveConfig(deployment)));
        verify(flinkService, times(2)).listJobs(any(Configuration.class));
        assertEquals(JOB_NAME, deployment.getStatus().getJobStatus().getJobName());
        deployment.getSpec().getJob().setState(JobState.SUSPENDED);
        assertTrue(
                observer.observeFlinkJobStatus(
                        deployment, FlinkUtils.getEffectiveConfig(deployment)));
        verify(flinkService, times(2)).listJobs(any(Configuration.class));
    }
}
