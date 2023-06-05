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

package org.apache.flink.kubernetes.operator.kubeclient.factory;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.EnvSecretsDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.HadoopConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.KerberosMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.MountSecretsDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesOwnerReference;
import org.apache.flink.kubernetes.operator.kubeclient.decorators.CmdStandaloneTaskManagerDecorator;
import org.apache.flink.kubernetes.operator.kubeclient.decorators.InitStandaloneTaskManagerDecorator;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Pod;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.apps.Deployment;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.Preconditions;

import java.util.stream.Collectors;

/** Utility class for constructing the TaskManager Deployment when deploying in standalone mode. */
public class StandaloneKubernetesTaskManagerFactory {

    public static Deployment buildKubernetesTaskManagerDeployment(
            FlinkPod podTemplate,
            StandaloneKubernetesTaskManagerParameters kubernetesTaskManagerParameters) {
        FlinkPod flinkPod = Preconditions.checkNotNull(podTemplate).copy();

        final KubernetesStepDecorator[] stepDecorators =
                new KubernetesStepDecorator[] {
                    new InitStandaloneTaskManagerDecorator(kubernetesTaskManagerParameters),
                    new EnvSecretsDecorator(kubernetesTaskManagerParameters),
                    new MountSecretsDecorator(kubernetesTaskManagerParameters),
                    new CmdStandaloneTaskManagerDecorator(kubernetesTaskManagerParameters),
                    new HadoopConfMountDecorator(kubernetesTaskManagerParameters),
                    new KerberosMountDecorator(kubernetesTaskManagerParameters),
                    new FlinkConfMountDecorator(kubernetesTaskManagerParameters)
                };

        for (KubernetesStepDecorator stepDecorator : stepDecorators) {
            flinkPod = stepDecorator.decorateFlinkPod(flinkPod);
        }

        final Pod resolvedPod =
                new PodBuilder(flinkPod.getPodWithoutMainContainer())
                        .editOrNewSpec()
                        .addToContainers(flinkPod.getMainContainer())
                        .endSpec()
                        .build();

        return new DeploymentBuilder()
                .withApiVersion(Constants.APPS_API_VERSION)
                .editOrNewMetadata()
                .withName(
                        StandaloneKubernetesUtils.getTaskManagerDeploymentName(
                                kubernetesTaskManagerParameters.getClusterId()))
                .withAnnotations(kubernetesTaskManagerParameters.getAnnotations())
                .withLabels(kubernetesTaskManagerParameters.getLabels())
                .withOwnerReferences(
                        kubernetesTaskManagerParameters.getOwnerReference().stream()
                                .map(e -> KubernetesOwnerReference.fromMap(e).getInternalResource())
                                .collect(Collectors.toList()))
                .endMetadata()
                .editOrNewSpec()
                .withReplicas(kubernetesTaskManagerParameters.getReplicas())
                .editOrNewTemplate()
                .withMetadata(resolvedPod.getMetadata())
                .withSpec(resolvedPod.getSpec())
                .endTemplate()
                .editOrNewSelector()
                .addToMatchLabels(kubernetesTaskManagerParameters.getSelectors())
                .endSelector()
                .endSpec()
                .build();
    }
}
