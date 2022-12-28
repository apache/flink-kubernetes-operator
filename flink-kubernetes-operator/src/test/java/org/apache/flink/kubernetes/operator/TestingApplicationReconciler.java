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

package org.apache.flink.kubernetes.operator;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.deployment.ApplicationReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;

/** Testing wrapper for {@link ApplicationReconciler}. */
public class TestingApplicationReconciler extends ApplicationReconciler {
    public TestingApplicationReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkConfigManager configManager,
            EventRecorder eventRecorder,
            StatusRecorder<FlinkDeployment, FlinkDeploymentStatus> statusRecorder) {
        super(kubernetesClient, flinkService, configManager, eventRecorder, statusRecorder);
    }

    @Override
    public void reconcile(FlinkDeployment flinkDeployment, Context<?> context) throws Exception {
        var cr = ReconciliationUtils.clone(flinkDeployment);
        cr.setStatus(flinkDeployment.getStatus());
        super.reconcile(cr, context);
    }

    @Override
    public DeleteControl cleanup(FlinkDeployment flinkDeployment, Context<?> context) {
        var cr = ReconciliationUtils.clone(flinkDeployment);
        cr.setStatus(flinkDeployment.getStatus());
        return super.cleanup(cr, context);
    }
}
