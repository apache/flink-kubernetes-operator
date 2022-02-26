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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** BaseReconciler with functionality that is common to job and session modes. */
public abstract class BaseReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(BaseReconciler.class);

    public static final int REFRESH_SECONDS = 60;
    public static final int PORT_READY_DELAY_SECONDS = 10;

    private final HashSet<String> jobManagerDeployments = new HashSet<>();

    public boolean removeDeployment(FlinkDeployment flinkApp) {
        return jobManagerDeployments.remove(flinkApp.getMetadata().getUid());
    }

    public abstract UpdateControl<FlinkDeployment> reconcile(
            String operatorNamespace,
            FlinkDeployment flinkApp,
            Context context,
            Configuration effectiveConfig)
            throws Exception;

    protected UpdateControl<FlinkDeployment> checkJobManagerDeployment(
            FlinkDeployment flinkApp,
            Context context,
            Configuration effectiveConfig,
            FlinkService flinkService) {
        if (!jobManagerDeployments.contains(flinkApp.getMetadata().getUid())) {
            Optional<Deployment> deployment = context.getSecondaryResource(Deployment.class);
            if (deployment.isPresent()) {
                DeploymentStatus status = deployment.get().getStatus();
                DeploymentSpec spec = deployment.get().getSpec();
                if (status != null
                        && status.getAvailableReplicas() != null
                        && spec.getReplicas().intValue() == status.getReplicas()
                        && spec.getReplicas().intValue() == status.getAvailableReplicas()) {
                    // typically it takes a few seconds for the REST server to be ready
                    if (flinkService.isJobManagerPortReady(effectiveConfig)) {
                        LOG.info(
                                "JobManager deployment {} in namespace {} is ready",
                                flinkApp.getMetadata().getName(),
                                flinkApp.getMetadata().getNamespace());
                        jobManagerDeployments.add(flinkApp.getMetadata().getUid());
                        if (flinkApp.getStatus().getJobStatus() != null) {
                            // pre-existing deployments on operator restart - proceed with
                            // reconciliation
                            return null;
                        }
                    }
                    LOG.info(
                            "JobManager deployment {} in namespace {} port not ready",
                            flinkApp.getMetadata().getName(),
                            flinkApp.getMetadata().getNamespace());
                    return UpdateControl.updateStatus(flinkApp)
                            .rescheduleAfter(PORT_READY_DELAY_SECONDS, TimeUnit.SECONDS);
                }
                LOG.info(
                        "JobManager deployment {} in namespace {} not yet ready, status {}",
                        flinkApp.getMetadata().getName(),
                        flinkApp.getMetadata().getNamespace(),
                        status);
                // TODO: how frequently do we want here
                return UpdateControl.updateStatus(flinkApp)
                        .rescheduleAfter(REFRESH_SECONDS, TimeUnit.SECONDS);
            }
        }
        return null;
    }
}
