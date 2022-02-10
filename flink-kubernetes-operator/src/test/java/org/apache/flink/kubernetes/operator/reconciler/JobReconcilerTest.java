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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.UpgradeMode;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;

/** @link JobStatusObserver unit tests */
public class JobReconcilerTest {

    public static final String JOB_NAME = "test1";
    public static final String JOB_ID = "fd72014d4c864993a2e5a9287b4a9c5d";

    private FlinkService flinkService = Mockito.mock(FlinkService.class);

    @Test
    public void testUpgrade() throws Exception {
        KubernetesClient kubernetesClient = Mockito.mock(KubernetesClient.class);
        JobReconciler reconciler = new JobReconciler(kubernetesClient, flinkService);
        FlinkDeployment deployment = TestUtils.buildApplicationCluster();
        Configuration config = FlinkUtils.getEffectiveConfig(deployment);

        reconciler.reconcile("test", deployment, config);
        Mockito.verify(flinkService, times(1)).submitApplicationCluster(eq(deployment), eq(config));
        Mockito.clearInvocations(flinkService);
        deployment.getStatus().setSpec(deployment.getSpec());

        JobStatus jobStatus = new JobStatus();
        jobStatus.setJobName(JOB_NAME);
        jobStatus.setJobId(JOB_ID);
        jobStatus.setState("RUNNING");

        deployment.getStatus().setJobStatus(jobStatus);

        // Test stateless upgrade
        FlinkDeployment statelessUpgrade = TestUtils.clone(deployment);
        statelessUpgrade.getSpec().getJob().setUpgradeMode(UpgradeMode.STATELESS);
        statelessUpgrade.getSpec().getFlinkConfiguration().put("new", "conf");
        reconciler.reconcile("test", statelessUpgrade, config);
        Mockito.verify(flinkService, times(1))
                .cancelJob(eq(JobID.fromHexString(JOB_ID)), eq(UpgradeMode.STATELESS), eq(config));

        Mockito.verify(flinkService, times(1))
                .submitApplicationCluster(eq(statelessUpgrade), eq(config));

        Mockito.clearInvocations(flinkService);

        // Test stateful upgrade
        FlinkDeployment statefulUpgrade = TestUtils.clone(deployment);
        statefulUpgrade.getSpec().getJob().setUpgradeMode(UpgradeMode.SAVEPOINT);
        statefulUpgrade.getSpec().getFlinkConfiguration().put("new", "conf");

        Mockito.doReturn(Optional.of("sp")).when(flinkService).cancelJob(any(), any(), any());

        reconciler.reconcile("test", statefulUpgrade, new Configuration(config));
        Mockito.verify(flinkService, times(1))
                .cancelJob(eq(JobID.fromHexString(JOB_ID)), eq(UpgradeMode.SAVEPOINT), eq(config));

        ArgumentCaptor<Configuration> argument = ArgumentCaptor.forClass(Configuration.class);
        Mockito.verify(flinkService, times(1))
                .submitApplicationCluster(eq(statefulUpgrade), argument.capture());
        assertEquals("sp", argument.getValue().get(SavepointConfigOptions.SAVEPOINT_PATH));

        Mockito.verifyNoMoreInteractions(flinkService);
    }
}
