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

package org.apache.flink.kubernetes.operator.kubeclient.parameters;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A utility class that helps to parse, verify and manage the Kubernetes parameters that are used
 * for constructing the TaskManager deployment used for standalone deployments.
 */
public class StandaloneKubernetesTaskManagerParameters extends AbstractKubernetesParameters {
    private final ClusterSpecification clusterSpecification;

    public StandaloneKubernetesTaskManagerParameters(
            Configuration flinkConfig, ClusterSpecification clusterSpecification) {
        super(flinkConfig);
        this.clusterSpecification = clusterSpecification;
    }

    @Override
    public Map<String, String> getLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.putAll(
                flinkConfig
                        .getOptional(KubernetesConfigOptions.TASK_MANAGER_LABELS)
                        .orElse(Collections.emptyMap()));
        labels.putAll(getSelectors());
        return Collections.unmodifiableMap(labels);
    }

    @Override
    public Map<String, String> getSelectors() {
        return StandaloneKubernetesUtils.getTaskManagerSelectors(getClusterId());
    }

    @Override
    public Map<String, String> getNodeSelector() {
        return Collections.unmodifiableMap(
                flinkConfig
                        .getOptional(KubernetesConfigOptions.TASK_MANAGER_NODE_SELECTOR)
                        .orElse(Collections.emptyMap()));
    }

    @Override
    public Map<String, String> getEnvironments() {
        // TMs have environment set using the pod template and config containerized.taskmanager.env
        return new HashMap<>(
                ConfigurationUtils.getPrefixedKeyValuePairs(
                        ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX, flinkConfig));
    }

    @Override
    public Map<String, String> getAnnotations() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS)
                .orElse(Collections.emptyMap());
    }

    @Override
    public List<Map<String, String>> getTolerations() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.TASK_MANAGER_TOLERATIONS)
                .orElse(Collections.emptyList());
    }

    public int getReplicas() {
        int replicas =
                flinkConfig.get(
                        StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS);
        Preconditions.checkArgument(
                replicas > 0,
                "'%s' should not be configured less than 1.",
                StandaloneKubernetesConfigOptionsInternal.KUBERNETES_TASKMANAGER_REPLICAS.key());
        return replicas;
    }

    public String getServiceAccount() {
        return flinkConfig.get(KubernetesConfigOptions.TASK_MANAGER_SERVICE_ACCOUNT);
    }

    public int getTaskManagerMemoryMB() {
        return clusterSpecification.getTaskManagerMemoryMB();
    }

    public double getTaskManagerCPU() {
        return flinkConfig.getDouble(KubernetesConfigOptions.TASK_MANAGER_CPU);
    }

    public int getRPCPort() {
        final int taskManagerRpcPort =
                KubernetesUtils.parsePort(flinkConfig, TaskManagerOptions.RPC_PORT);
        Preconditions.checkArgument(
                taskManagerRpcPort > 0, "%s should not be 0.", TaskManagerOptions.RPC_PORT.key());
        return taskManagerRpcPort;
    }

    public Optional<File> getPodTemplateFilePath() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE)
                .map(File::new);
    }

    public double getMemoryLimitFactor() {
        return flinkConfig.get(KubernetesConfigOptions.TASK_MANAGER_MEMORY_LIMIT_FACTOR);
    }

    public double getCpuLimitFactor() {
        return flinkConfig.get(KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_FACTOR);
    }

    // Temporally share job manager owner reference config options
    public List<Map<String, String>> getOwnerReference() {
        return flinkConfig
                .getOptional(KubernetesConfigOptions.JOB_MANAGER_OWNER_REFERENCE)
                .orElse(Collections.emptyList());
    }
}
