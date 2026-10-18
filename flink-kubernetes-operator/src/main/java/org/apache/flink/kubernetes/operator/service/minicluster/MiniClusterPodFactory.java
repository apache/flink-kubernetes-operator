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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.kubeclient.decorators.EnvSecretsDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.HadoopConfMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.KerberosMountDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.MountSecretsDecorator;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesOwnerReference;
import org.apache.flink.kubernetes.operator.api.FlinkMiniCluster;
import org.apache.flink.kubernetes.operator.api.spec.FlinkMiniClusterSpec;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.HasMetadata;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.apps.Deployment;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.utils.Serialization;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds the Kubernetes resources for a {@link FlinkMiniCluster}.
 *
 * <p>These are:
 *
 * <p>1) A single-replica Deployment whose pod runs the Flink {@code bin/minicluster.sh} launcher
 *
 * <p>2) A Service exposing the Flink REST endpoint
 *
 * <p>3) A ConfigMap holding the Flink and log configuration read by the minicluster.sh launcher
 *
 * <p>As with a FlinkDeployment's JobManager, these are built by Flink's Kubernetes decorators, and
 * so use the Kubernetes model classes shaded into flink-kubernetes. They are created through
 * Flink's Kubernetes client, which owns the Service and ConfigMap by the Deployment it creates.
 */
public final class MiniClusterPodFactory {

    /** Value of the {@link Constants#LABEL_TYPE_KEY} label on every MiniCluster resource. */
    public static final String LABEL_TYPE_MINICLUSTER = "flink-minicluster";

    /** Value of the {@link Constants#LABEL_COMPONENT_KEY} label on the MiniCluster pod. */
    public static final String LABEL_COMPONENT_MINICLUSTER = "minicluster";

    private MiniClusterPodFactory() {}

    /**
     * Builds the Kubernetes resources for a MiniCluster.
     *
     * @param spec spec to deploy, not necessarily the current spec of the resource
     * @param deployConfig the effective Flink configuration to deploy with
     * @return the Deployment, and the Service and ConfigMap that accompany it
     */
    public static KubernetesJobManagerSpecification build(
            FlinkMiniClusterSpec spec, Configuration deployConfig) throws IOException {
        // The REST port needs to be exposed, so it is updated to a fixed value at deploy time
        var conf = new Configuration(deployConfig);
        KubernetesUtils.checkAndUpdatePortConfigOption(
                conf, RestOptions.BIND_PORT, Constants.REST_PORT);
        var parameters = new MiniClusterParameters(conf);

        // The same decorators as a FlinkDeployment's JobManager, apart from:
        // - the init and command decorators, which are specific to a MiniCluster
        // - InternalServiceDecorator, as there are no TaskManager pods to reach the JobManager
        var stepDecorators =
                new KubernetesStepDecorator[] {
                    new InitMiniClusterDecorator(parameters),
                    new EnvSecretsDecorator(parameters),
                    new MountSecretsDecorator(parameters),
                    new CmdMiniClusterDecorator(parameters, spec.getJob()),
                    new ExternalServiceDecorator(parameters),
                    new HadoopConfMountDecorator(parameters),
                    new KerberosMountDecorator(parameters),
                    new FlinkConfMountDecorator(parameters)
                };

        var flinkPod = loadPodTemplate(spec, conf);
        var accompanyingResources = new ArrayList<HasMetadata>();
        for (var stepDecorator : stepDecorators) {
            flinkPod = stepDecorator.decorateFlinkPod(flinkPod);
            accompanyingResources.addAll(stepDecorator.buildAccompanyingKubernetesResources());
        }

        return new KubernetesJobManagerSpecification(
                createDeployment(flinkPod, parameters), accompanyingResources);
    }

    public static Map<String, String> getCommonLabels(String clusterId) {
        var labels = new HashMap<String, String>();
        labels.put(Constants.LABEL_TYPE_KEY, LABEL_TYPE_MINICLUSTER);
        labels.put(Constants.LABEL_APP_KEY, clusterId);
        return labels;
    }

    public static Map<String, String> getSelectorLabels(String clusterId) {
        var labels = getCommonLabels(clusterId);
        labels.put(Constants.LABEL_COMPONENT_KEY, LABEL_COMPONENT_MINICLUSTER);
        return labels;
    }

    /**
     * Loads {@code spec.podTemplate}, with {@code spec.resources} and the startup probe applied to
     * the main container, into the shaded Kubernetes model used by Flink's decorators. This
     * separates the main container from the rest of the pod.
     */
    private static FlinkPod loadPodTemplate(FlinkMiniClusterSpec spec, Configuration conf) {
        var podTemplate =
                spec.getPodTemplate() == null
                        ? new PodTemplateSpec()
                        : ReconciliationUtils.clone(spec.getPodTemplate());
        if (podTemplate.getMetadata() == null) {
            podTemplate.setMetadata(new ObjectMeta());
        }
        if (podTemplate.getSpec() == null) {
            podTemplate.setSpec(new PodSpec());
        }
        if (spec.getResources() != null) {
            applyResourcesToMainContainer(podTemplate.getSpec(), spec.getResources());
        }
        // The MiniCluster pod is the JobManager, so it is probed in the same way
        if (conf.get(KubernetesOperatorConfigOptions.OPERATOR_JM_STARTUP_PROBE_ENABLED)) {
            FlinkUtils.addStartupProbe(podTemplate);
        }

        // The spec uses the operator's Kubernetes model, so it is converted through JSON
        var shadedPodTemplate =
                org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.client.utils.Serialization
                        .unmarshal(
                                Serialization.asJson(podTemplate),
                                org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model
                                        .PodTemplateSpec.class);
        var pod =
                new PodBuilder()
                        .withMetadata(shadedPodTemplate.getMetadata())
                        .withSpec(shadedPodTemplate.getSpec())
                        .build();

        var otherContainers = new ArrayList<Container>();
        Container mainContainer = new Container();
        for (var container : pod.getSpec().getContainers()) {
            if (Constants.MAIN_CONTAINER_NAME.equals(container.getName())) {
                mainContainer = container;
            } else {
                otherContainers.add(container);
            }
        }
        pod.getSpec().setContainers(otherContainers);
        return new FlinkPod(pod, mainContainer);
    }

    /**
     * Applies resources to the main container, adding the main container if needed. Each resource
     * named in {@code spec.resources} (e.g. memory) replaces both the request and the limit for it
     * in the pod template, and other pod template resources (e.g. ephemeral-storage) are kept.
     *
     * <p>Merging requests and limits separately could keep a pod template request alongside a
     * {@code spec.resources} limit, which may exceed it, or differ from the memory that the Flink
     * configuration is sized from.
     */
    private static void applyResourcesToMainContainer(
            PodSpec podSpec, ResourceRequirements resources) {
        var containers = new ArrayList<>(podSpec.getContainers());
        // The last main container, as that is the one the pod is built with
        var mainContainer =
                containers.stream()
                        .filter(c -> Constants.MAIN_CONTAINER_NAME.equals(c.getName()))
                        .reduce((first, second) -> second)
                        .orElseGet(
                                () -> {
                                    var container =
                                            new io.fabric8.kubernetes.api.model.ContainerBuilder()
                                                    .withName(Constants.MAIN_CONTAINER_NAME)
                                                    .build();
                                    containers.add(container);
                                    return container;
                                });
        if (mainContainer.getResources() == null) {
            mainContainer.setResources(new ResourceRequirements());
        }
        var specResourceNames = new HashSet<String>();
        if (resources.getRequests() != null) {
            specResourceNames.addAll(resources.getRequests().keySet());
        }
        if (resources.getLimits() != null) {
            specResourceNames.addAll(resources.getLimits().keySet());
        }
        var mainContainerResources = mainContainer.getResources();
        mainContainerResources.setRequests(
                merge(
                        mainContainerResources.getRequests(),
                        resources.getRequests(),
                        specResourceNames));
        mainContainerResources.setLimits(
                merge(
                        mainContainerResources.getLimits(),
                        resources.getLimits(),
                        specResourceNames));
        podSpec.setContainers(containers);
    }

    private static Map<String, Quantity> merge(
            Map<String, Quantity> podTemplateValues,
            Map<String, Quantity> specValues,
            Set<String> specResourceNames) {
        var merged = new HashMap<String, Quantity>();
        if (podTemplateValues != null) {
            merged.putAll(podTemplateValues);
            merged.keySet().removeAll(specResourceNames);
        }
        if (specValues != null) {
            merged.putAll(specValues);
        }
        return merged;
    }

    private static Deployment createDeployment(
            FlinkPod flinkPod, MiniClusterParameters parameters) {
        var resolvedPod =
                new PodBuilder(flinkPod.getPodWithoutMainContainer())
                        .editOrNewSpec()
                        .addToContainers(flinkPod.getMainContainer())
                        .endSpec()
                        .build();
        return new DeploymentBuilder()
                .withApiVersion(Constants.APPS_API_VERSION)
                .editOrNewMetadata()
                .withName(parameters.getClusterId())
                .withAnnotations(parameters.getAnnotations())
                .withLabels(parameters.getLabels())
                .withOwnerReferences(
                        parameters.getOwnerReference().stream()
                                .map(e -> KubernetesOwnerReference.fromMap(e).getInternalResource())
                                .collect(Collectors.toList()))
                .endMetadata()
                .editOrNewSpec()
                .withReplicas(1)
                // Only one MiniCluster pod can run the job at a time
                .withNewStrategy()
                .withType("Recreate")
                .endStrategy()
                .editOrNewTemplate()
                .withMetadata(resolvedPod.getMetadata())
                .withSpec(resolvedPod.getSpec())
                .endTemplate()
                .editOrNewSelector()
                .addToMatchLabels(parameters.getSelectors())
                .endSelector()
                .endSpec()
                .build();
    }
}
