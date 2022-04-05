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
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.observer.SavepointObserver;
import org.apache.flink.kubernetes.operator.observer.context.ApplicationObserverContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.List;
import java.util.Optional;

/** The observer of {@link org.apache.flink.kubernetes.operator.config.Mode#APPLICATION} cluster. */
public class ApplicationObserver extends AbstractDeploymentObserver {

    private final SavepointObserver savepointObserver;
    private final JobStatusObserver<ApplicationObserverContext> jobStatusObserver;

    public ApplicationObserver(
            FlinkService flinkService,
            FlinkOperatorConfiguration operatorConfiguration,
            Configuration flinkConfig) {
        super(flinkService, operatorConfiguration, flinkConfig);
        this.savepointObserver = new SavepointObserver(flinkService, operatorConfiguration);
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

        JobStatus jobStatus = flinkApp.getStatus().getJobStatus();

        boolean jobFound =
                jobStatusObserver.observe(
                        jobStatus,
                        deployedConfig,
                        new ApplicationObserverContext(flinkApp, context, deployedConfig));
        if (jobFound) {
            savepointObserver
                    .observe(jobStatus.getSavepointInfo(), jobStatus.getJobId(), deployedConfig)
                    .ifPresent(
                            error ->
                                    ReconciliationUtils.updateForReconciliationError(
                                            flinkApp, error));
        }
        return isJobReady(jobStatus);
    }

    private boolean isJobReady(JobStatus jobStatus) {
        return org.apache.flink.api.common.JobStatus.RUNNING.name().equals(jobStatus.getState());
    }
}
