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

package org.apache.flink.kubernetes.operator.observer.deployment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.JobState;
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.observer.SavepointObserver;
import org.apache.flink.kubernetes.operator.observer.context.ApplicationObserverContext;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.StatusHelper;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.apache.flink.api.common.JobStatus.RUNNING;

/** The observer of {@link org.apache.flink.kubernetes.operator.config.Mode#APPLICATION} cluster. */
public class ApplicationObserver extends AbstractDeploymentObserver {

    private final SavepointObserver savepointObserver;
    private final JobStatusObserver<ApplicationObserverContext> jobStatusObserver;

    public ApplicationObserver(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager,
            StatusHelper<FlinkDeploymentStatus> statusHelper) {
        super(kubernetesClient, flinkService, configManager);
        this.savepointObserver = new SavepointObserver(flinkService, configManager, statusHelper);
        this.jobStatusObserver =
                new JobStatusObserver<>(flinkService) {
                    @Override
                    public void onTimeout(ApplicationObserverContext ctx) {
                        observeJmDeployment(ctx.flinkApp, ctx.context, ctx.deployedConfig);
                    }

                    @Override
                    protected Optional<String> updateJobStatus(
                            JobStatus status, List<JobStatusMessage> clusterJobStatuses) {
                        clusterJobStatuses.sort(
                                (j1, j2) -> Long.compare(j2.getStartTime(), j1.getStartTime()));
                        JobStatusMessage newJob = clusterJobStatuses.get(0);

                        status.setState(newJob.getJobState().name());
                        status.setJobName(newJob.getJobName());
                        status.setJobId(newJob.getJobId().toHexString());
                        status.setStartTime(String.valueOf(newJob.getStartTime()));
                        status.setUpdateTime(String.valueOf(System.currentTimeMillis()));
                        return Optional.of(status.getState());
                    }
                };
    }

    @Override
    protected boolean observeFlinkCluster(
            FlinkDeployment flinkApp, Context context, Configuration deployedConfig) {

        var jobStatus = flinkApp.getStatus().getJobStatus();

        boolean jobFound =
                jobStatusObserver.observe(
                        jobStatus,
                        deployedConfig,
                        new ApplicationObserverContext(flinkApp, context, deployedConfig));
        if (jobFound) {
            savepointObserver.observeSavepointStatus(flinkApp, deployedConfig);
        }
        return isJobStable(flinkApp.getStatus());
    }

    private boolean isJobStable(FlinkDeploymentStatus deploymentStatus) {
        var flinkJobStatus =
                org.apache.flink.api.common.JobStatus.valueOf(
                        deploymentStatus.getJobStatus().getState());

        if (flinkJobStatus == RUNNING) {
            // Running jobs are currently always marked stable
            return true;
        }

        var reconciledJobState =
                deploymentStatus
                        .getReconciliationStatus()
                        .deserializeLastReconciledSpec()
                        .getJob()
                        .getState();

        if (reconciledJobState == JobState.RUNNING && flinkJobStatus == FINISHED) {
            // If the job finished on its own, it's marked stable
            return true;
        }

        return false;
    }
}
