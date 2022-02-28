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
import org.apache.flink.kubernetes.operator.observer.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.operator.utils.IngressUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** BaseReconciler with functionality that is common to job and session modes. */
public abstract class BaseReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(BaseReconciler.class);

    public static final int REFRESH_SECONDS = 60;
    public static final int PORT_READY_DELAY_SECONDS = 10;

    protected final KubernetesClient kubernetesClient;
    protected final FlinkService flinkService;

    public BaseReconciler(KubernetesClient kubernetesClient, FlinkService flinkService) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
    }

    public abstract UpdateControl<FlinkDeployment> reconcile(
            String operatorNamespace,
            FlinkDeployment flinkApp,
            Context context,
            Configuration effectiveConfig)
            throws Exception;

    public DeleteControl shutdownAndDelete(
            String operatorNamespace, FlinkDeployment flinkApp, Configuration effectiveConfig) {

        if (JobManagerDeploymentStatus.READY
                == flinkApp.getStatus().getJobManagerDeploymentStatus()) {
            shutdown(flinkApp, effectiveConfig);
        } else {
            FlinkUtils.deleteCluster(flinkApp, kubernetesClient, true);
        }
        IngressUtils.updateIngressRules(
                flinkApp, effectiveConfig, operatorNamespace, kubernetesClient, true);

        return DeleteControl.defaultDelete();
    }

    protected abstract void shutdown(FlinkDeployment flinkApp, Configuration effectiveConfig);
}
