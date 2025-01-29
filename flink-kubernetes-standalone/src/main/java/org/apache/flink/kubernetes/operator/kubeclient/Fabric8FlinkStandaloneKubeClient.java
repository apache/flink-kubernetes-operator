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

package org.apache.flink.kubernetes.operator.kubeclient;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.DeletionPropagation;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.apps.Deployment;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.NamespacedKubernetesClient;

import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The Implementation of {@link FlinkStandaloneKubeClient}. */
public class Fabric8FlinkStandaloneKubeClient extends Fabric8FlinkKubeClient
        implements FlinkStandaloneKubeClient {

    private final NamespacedKubernetesClient internalClient;

    @VisibleForTesting
    public Fabric8FlinkStandaloneKubeClient(
            Configuration flinkConfig,
            NamespacedKubernetesClient client,
            ScheduledExecutorService executorService) {
        super(flinkConfig, client, executorService);
        internalClient = checkNotNull(client);
    }

    @Override
    public void createTaskManagerDeployment(Deployment tmDeployment) {
        this.internalClient.apps().deployments().create(tmDeployment);
    }

    @Override
    public void stopAndCleanupCluster(String clusterId) {
        this.internalClient
                .apps()
                .deployments()
                .withName(StandaloneKubernetesUtils.getJobManagerDeploymentName(clusterId))
                .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                .delete();

        this.internalClient
                .apps()
                .deployments()
                .withName(StandaloneKubernetesUtils.getTaskManagerDeploymentName(clusterId))
                .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                .delete();
    }

    public static Fabric8FlinkStandaloneKubeClient create(
            Configuration conf, ScheduledExecutorService executorService) {
        var client =
                new DefaultKubernetesClient()
                        .inNamespace(conf.get(KubernetesConfigOptions.NAMESPACE));
        return new Fabric8FlinkStandaloneKubeClient(conf, client, executorService);
    }
}
