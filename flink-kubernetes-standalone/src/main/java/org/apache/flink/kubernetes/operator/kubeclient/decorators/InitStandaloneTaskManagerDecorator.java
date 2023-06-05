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

package org.apache.flink.kubernetes.operator.kubeclient.decorators;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractKubernetesStepDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesToleration;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.EnvVar;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.EnvVarBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An initializer for the TaskManager {@link org.apache.flink.kubernetes.kubeclient.FlinkPod} in
 * standalone mode.
 */
public class InitStandaloneTaskManagerDecorator extends AbstractKubernetesStepDecorator {
    private final StandaloneKubernetesTaskManagerParameters kubernetesTaskManagerParameters;

    public InitStandaloneTaskManagerDecorator(
            StandaloneKubernetesTaskManagerParameters kubernetesTaskManagerParameters) {
        this.kubernetesTaskManagerParameters = checkNotNull(kubernetesTaskManagerParameters);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final PodBuilder basicPodBuilder = new PodBuilder(flinkPod.getPodWithoutMainContainer());

        // Overwrite fields
        final String serviceAccountName = kubernetesTaskManagerParameters.getServiceAccount();

        basicPodBuilder
                .withApiVersion(Constants.API_VERSION)
                .editOrNewSpec()
                .withServiceAccount(serviceAccountName)
                .withServiceAccountName(serviceAccountName)
                .endSpec();

        // Merge fields
        basicPodBuilder
                .editOrNewMetadata()
                .addToLabels(kubernetesTaskManagerParameters.getLabels())
                .addToAnnotations(kubernetesTaskManagerParameters.getAnnotations())
                .endMetadata()
                .editOrNewSpec()
                .addToImagePullSecrets(kubernetesTaskManagerParameters.getImagePullSecrets())
                .addToNodeSelector(kubernetesTaskManagerParameters.getNodeSelector())
                .addAllToTolerations(
                        kubernetesTaskManagerParameters.getTolerations().stream()
                                .map(e -> KubernetesToleration.fromMap(e).getInternalResource())
                                .collect(Collectors.toList()))
                .endSpec();

        final Container basicMainContainer = decorateMainContainer(flinkPod.getMainContainer());

        return new FlinkPod.Builder(flinkPod)
                .withPod(basicPodBuilder.build())
                .withMainContainer(basicMainContainer)
                .build();
    }

    private Container decorateMainContainer(Container container) {
        final ContainerBuilder mainContainerBuilder = new ContainerBuilder(container);

        // Overwrite fields
        final ResourceRequirements requirementsInPodTemplate =
                container.getResources() == null
                        ? new ResourceRequirements()
                        : container.getResources();
        final ResourceRequirements resourceRequirements =
                KubernetesUtils.getResourceRequirements(
                        requirementsInPodTemplate,
                        kubernetesTaskManagerParameters.getTaskManagerMemoryMB(),
                        kubernetesTaskManagerParameters.getMemoryLimitFactor(),
                        kubernetesTaskManagerParameters.getTaskManagerCPU(),
                        kubernetesTaskManagerParameters.getCpuLimitFactor(),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        final String image = kubernetesTaskManagerParameters.getImage();
        final String imagePullPolicy = kubernetesTaskManagerParameters.getImagePullPolicy().name();
        mainContainerBuilder
                .withName(Constants.MAIN_CONTAINER_NAME)
                .withImage(image)
                .withImagePullPolicy(imagePullPolicy)
                .withResources(resourceRequirements);

        // Merge fields
        mainContainerBuilder
                .addToPorts(
                        new ContainerPortBuilder()
                                .withName(Constants.TASK_MANAGER_RPC_PORT_NAME)
                                .withContainerPort(kubernetesTaskManagerParameters.getRPCPort())
                                .build())
                .addAllToEnv(getCustomizedEnvs());
        getFlinkLogDirEnv().ifPresent(mainContainerBuilder::addToEnv);

        return mainContainerBuilder.build();
    }

    private List<EnvVar> getCustomizedEnvs() {
        return kubernetesTaskManagerParameters.getEnvironments().entrySet().stream()
                .map(
                        kv ->
                                new EnvVarBuilder()
                                        .withName(kv.getKey())
                                        .withValue(kv.getValue())
                                        .build())
                .collect(Collectors.toList());
    }

    private Optional<EnvVar> getFlinkLogDirEnv() {
        return kubernetesTaskManagerParameters
                .getFlinkLogDirInPod()
                .map(logDir -> new EnvVar(Constants.ENV_FLINK_LOG_DIR, logDir, null));
    }
}
