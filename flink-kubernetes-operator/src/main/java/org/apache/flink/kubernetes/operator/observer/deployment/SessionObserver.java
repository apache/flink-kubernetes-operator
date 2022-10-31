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

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.status.ReconciliationState;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import java.util.concurrent.TimeoutException;

/** The observer of the {@link org.apache.flink.kubernetes.operator.config.Mode#SESSION} cluster. */
public class SessionObserver extends AbstractFlinkDeploymentObserver {

    public SessionObserver(
            FlinkService flinkService,
            FlinkConfigManager configManager,
            EventRecorder eventRecorder) {
        super(flinkService, configManager, eventRecorder);
    }

    @Override
    public void observeFlinkCluster(
            FlinkDeployment deployment,
            Context<?> context,
            FlinkDeploymentObserverContext observerContext) {
        // Check if session cluster can serve rest calls following our practice in JobObserver
        try {
            logger.debug("Observing session cluster");
            flinkService.listJobs(observerContext.getDeployedConfig());
            var rs = deployment.getStatus().getReconciliationStatus();
            if (rs.getState() == ReconciliationState.DEPLOYED) {
                rs.markReconciledSpecAsStable();
            }
        } catch (Exception e) {
            logger.error("REST service in session cluster is bad now", e);
            if (e instanceof TimeoutException) {
                // check for problems with the underlying deployment
                observeJmDeployment(deployment, context, observerContext.getDeployedConfig());
            }
        }
    }
}
