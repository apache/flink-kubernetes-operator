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

package org.apache.flink.kubernetes.operator.service.minicluster;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractKubernetesStepDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InitJobManagerDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesToleration;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerPort;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.EnvVar;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.EnvVarBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodBuilder;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.API_VERSION;
import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_IP_ADDRESS;
import static org.apache.flink.kubernetes.utils.Constants.POD_IP_FIELD_PATH;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Applies the JobManager pod options to the MiniCluster pod, in the same way as Flink's {@link
 * InitJobManagerDecorator} does for a FlinkDeployment's JobManager pod.
 *
 * <p>{@link InitJobManagerDecorator} itself isn't used, as it doesn't suit a MiniCluster in these
 * ways, which are the only intended differences from it:
 *
 * <p>It sets container resources from the JobManager memory and CPU options, which would override
 * {@code spec.resources}. Resources in the pod template are kept instead.
 *
 * <p>It declares the JobManager RPC and blob server ports, which a MiniCluster doesn't listen on
 * for other pods. Only the REST port is declared, as the port that the REST endpoint listens on. A
 * REST port declared in the pod template keeps its other settings.
 *
 * <p>It leaves the termination grace period to Kubernetes, which doesn't leave the MiniCluster
 * launcher time to stop the job.
 *
 * <p>It logs that a restart policy in the pod template will be overwritten to 'Always', but leaves
 * it unchanged, so a Deployment can't be created from it. The restart policy is set to 'Always'.
 */
class InitMiniClusterDecorator extends AbstractKubernetesStepDecorator {

    /**
     * Grace period for the MiniCluster pod to stop. The MiniCluster launcher uses a 30 second
     * SHUTDOWN_TIMEOUT so this needs to leave room for that before killing the pod.
     */
    @VisibleForTesting static final long TERMINATION_GRACE_PERIOD_SECONDS = 45L;

    /** The only restart policy that a Deployment's pod template can have. */
    @VisibleForTesting static final String RESTART_POLICY_ALWAYS = "Always";

    private final KubernetesJobManagerParameters kubernetesJobManagerParameters;
    private final Configuration flinkConfig;

    InitMiniClusterDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
        this.flinkConfig = checkNotNull(kubernetesJobManagerParameters.getFlinkConfiguration());
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final PodBuilder basicPodBuilder = new PodBuilder(flinkPod.getPodWithoutMainContainer());

        // Overwrite fields
        final String serviceAccountName =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT,
                        kubernetesJobManagerParameters.getServiceAccount(),
                        KubernetesUtils.getServiceAccount(flinkPod),
                        "service account");

        final String dnsPolicy =
                KubernetesUtils.resolveDNSPolicy(
                        flinkPod.getPodWithoutMainContainer().getSpec().getDnsPolicy(),
                        kubernetesJobManagerParameters.isHostNetworkEnabled());

        if (flinkPod.getPodWithoutMainContainer().getSpec().getRestartPolicy() != null) {
            logger.info(
                    "The restart policy of the MiniCluster pod will be overwritten to 'always' "
                            + "since it is controlled by the Kubernetes deployment.");
        }

        basicPodBuilder
                .withApiVersion(API_VERSION)
                .editOrNewSpec()
                .withServiceAccount(serviceAccountName)
                .withServiceAccountName(serviceAccountName)
                .withHostNetwork(kubernetesJobManagerParameters.isHostNetworkEnabled())
                .withDnsPolicy(dnsPolicy)
                .withRestartPolicy(RESTART_POLICY_ALWAYS)
                .endSpec();

        if (flinkPod.getPodWithoutMainContainer().getSpec().getTerminationGracePeriodSeconds()
                == null) {
            basicPodBuilder
                    .editOrNewSpec()
                    .withTerminationGracePeriodSeconds(TERMINATION_GRACE_PERIOD_SECONDS)
                    .endSpec();
        }

        // Specify volume for user artifact(s)
        basicPodBuilder
                .editOrNewSpec()
                .addNewVolume()
                .withName(Constants.USER_ARTIFACTS_VOLUME)
                .withNewEmptyDir()
                .endEmptyDir()
                .endVolume()
                .endSpec();

        // Merge fields
        basicPodBuilder
                .editOrNewMetadata()
                .addToLabels(kubernetesJobManagerParameters.getLabels())
                .addToAnnotations(kubernetesJobManagerParameters.getAnnotations())
                .endMetadata()
                .editOrNewSpec()
                .addToImagePullSecrets(kubernetesJobManagerParameters.getImagePullSecrets())
                .addToNodeSelector(kubernetesJobManagerParameters.getNodeSelector())
                .addAllToTolerations(
                        kubernetesJobManagerParameters.getTolerations().stream()
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
        final String image =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        kubernetesJobManagerParameters.getImage(),
                        container.getImage(),
                        "main container image");
        final String imagePullPolicy =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                        kubernetesJobManagerParameters.getImagePullPolicy().name(),
                        container.getImagePullPolicy(),
                        "main container image pull policy");
        mainContainerBuilder
                .withName(Constants.MAIN_CONTAINER_NAME)
                .withImage(image)
                .withImagePullPolicy(imagePullPolicy);

        // Mount volume for user artifact(s)
        mainContainerBuilder
                .addNewVolumeMount()
                .withName(Constants.USER_ARTIFACTS_VOLUME)
                .withMountPath(kubernetesJobManagerParameters.getUserArtifactsBaseDir())
                .endVolumeMount();

        // Merge fields
        mainContainerBuilder
                .withPorts(getContainerPorts(container))
                .addAllToEnv(getCustomizedEnvs())
                .addNewEnv()
                .withName(ENV_FLINK_POD_IP_ADDRESS)
                .withValueFrom(
                        new EnvVarSourceBuilder()
                                .withNewFieldRef(API_VERSION, POD_IP_FIELD_PATH)
                                .build())
                .endEnv();

        getFlinkLogDirEnv().ifPresent(mainContainerBuilder::addToEnv);
        return mainContainerBuilder.build();
    }

    private List<ContainerPort> getContainerPorts(Container container) {
        final List<ContainerPort> ports =
                container.getPorts() == null
                        ? new ArrayList<>()
                        : new ArrayList<>(container.getPorts());
        if (kubernetesJobManagerParameters.isHostNetworkEnabled()) {
            return ports;
        }
        final int restBindPort = kubernetesJobManagerParameters.getRestBindPort();
        for (int i = 0; i < ports.size(); i++) {
            if (Constants.REST_PORT_NAME.equals(ports.get(i).getName())) {
                ports.set(
                        i,
                        new ContainerPortBuilder(ports.get(i))
                                .withContainerPort(restBindPort)
                                .build());
                return ports;
            }
        }
        ports.add(
                new ContainerPortBuilder()
                        .withName(Constants.REST_PORT_NAME)
                        .withContainerPort(restBindPort)
                        .build());
        return ports;
    }

    private List<EnvVar> getCustomizedEnvs() {
        return kubernetesJobManagerParameters.getEnvironments().entrySet().stream()
                .map(
                        kv ->
                                new EnvVarBuilder()
                                        .withName(kv.getKey())
                                        .withValue(kv.getValue())
                                        .build())
                .collect(Collectors.toList());
    }

    private Optional<EnvVar> getFlinkLogDirEnv() {
        return kubernetesJobManagerParameters
                .getFlinkLogDirInPod()
                .map(logDir -> new EnvVar(Constants.ENV_FLINK_LOG_DIR, logDir, null));
    }
}
