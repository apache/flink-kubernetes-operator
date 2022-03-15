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
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.observer.JobManagerDeploymentStatus;
import org.apache.flink.kubernetes.operator.service.FlinkService;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;

/** BaseReconciler with functionality that is common to job and session modes. */
public abstract class BaseReconciler implements Reconciler {

    protected final FlinkOperatorConfiguration operatorConfiguration;
    protected final KubernetesClient kubernetesClient;
    protected final FlinkService flinkService;

    public BaseReconciler(
            KubernetesClient kubernetesClient,
            FlinkService flinkService,
            FlinkOperatorConfiguration operatorConfiguration) {
        this.kubernetesClient = kubernetesClient;
        this.flinkService = flinkService;
        this.operatorConfiguration = operatorConfiguration;
    }

    @Override
    public DeleteControl cleanup(FlinkDeployment flinkApp, Configuration effectiveConfig) {
        return shutdownAndDelete(flinkApp, effectiveConfig);
    }

    private DeleteControl shutdownAndDelete(
            FlinkDeployment flinkApp, Configuration effectiveConfig) {

        if (JobManagerDeploymentStatus.READY
                == flinkApp.getStatus().getJobManagerDeploymentStatus()) {
            shutdown(flinkApp, effectiveConfig);
        } else {
            FlinkUtils.deleteCluster(flinkApp, kubernetesClient, true);
        }

        return DeleteControl.defaultDelete();
    }

    protected abstract void shutdown(FlinkDeployment flinkApp, Configuration effectiveConfig);
}
