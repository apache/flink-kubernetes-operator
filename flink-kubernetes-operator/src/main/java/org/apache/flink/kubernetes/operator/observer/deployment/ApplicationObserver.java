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
import org.apache.flink.kubernetes.operator.crd.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.crd.status.JobStatus;
import org.apache.flink.kubernetes.operator.observer.JobStatusObserver;
import org.apache.flink.kubernetes.operator.observer.SavepointObserver;
import org.apache.flink.kubernetes.operator.observer.context.ApplicationObserverContext;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.client.JobStatusMessage;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.List;
import java.util.Optional;

/** The observer of {@link org.apache.flink.kubernetes.operator.config.Mode#APPLICATION} cluster. */
public class ApplicationObserver extends AbstractDeploymentObserver {

    private final SavepointObserver<FlinkDeployment, FlinkDeploymentStatus> savepointObserver;
    private final JobStatusObserver<ApplicationObserverContext> jobStatusObserver;

    public ApplicationObserver(
            FlinkService flinkService,
            FlinkConfigManager configManager,
            EventRecorder eventRecorder) {
        super(flinkService, configManager, eventRecorder);
        this.savepointObserver =
                new SavepointObserver<>(flinkService, configManager, eventRecorder);
        this.jobStatusObserver =
                new JobStatusObserver<>(flinkService, eventRecorder) {
                    @Override
                    public void onTimeout(ApplicationObserverContext ctx) {
                        observeJmDeployment(ctx.flinkApp, ctx.context, ctx.deployedConfig);
                    }

                    @Override
                    protected Optional<JobStatusMessage> filterTargetJob(
                            JobStatus status, List<JobStatusMessage> clusterJobStatuses) {
                        if (!clusterJobStatuses.isEmpty()) {
                            return Optional.of(clusterJobStatuses.get(0));
                        }
                        return Optional.empty();
                    }
                };
    }

    @Override
    protected void observeFlinkCluster(
            FlinkDeployment flinkApp, Context<?> context, Configuration deployedConfig) {

        boolean jobFound =
                jobStatusObserver.observe(
                        flinkApp,
                        deployedConfig,
                        new ApplicationObserverContext(flinkApp, context, deployedConfig));
        if (jobFound) {
            savepointObserver.observeSavepointStatus(flinkApp, deployedConfig);
        }
    }
}
