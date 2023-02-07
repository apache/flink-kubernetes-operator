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
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.operator.utils.StandaloneKubernetesUtils;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Utility class that helps to parse, verify and manage the Kubernetes parameters that are used
 * for constructing the JobManager deployment used for standalone cluster deployments.
 */
public class StandaloneKubernetesJobManagerParameters extends KubernetesJobManagerParameters {

    public StandaloneKubernetesJobManagerParameters(
            Configuration flinkConfig, ClusterSpecification clusterSpecification) {
        super(flinkConfig, clusterSpecification);
    }

    @Override
    public Map<String, String> getLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.putAll(getSelectors());
        labels.putAll(
                flinkConfig
                        .getOptional(KubernetesConfigOptions.JOB_MANAGER_LABELS)
                        .orElse(Collections.emptyMap()));
        return Collections.unmodifiableMap(labels);
    }

    @Override
    public Map<String, String> getSelectors() {
        return StandaloneKubernetesUtils.getJobManagerSelectors(getClusterId());
    }

    @Override
    public Map<String, String> getCommonLabels() {
        return Collections.unmodifiableMap(
                StandaloneKubernetesUtils.getCommonLabels(getClusterId()));
    }

    @Override
    public boolean isInternalServiceEnabled() {
        return true;
    }

    public boolean isApplicationCluster() {
        return flinkConfig
                .get(StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE)
                .equals(StandaloneKubernetesConfigOptionsInternal.ClusterMode.APPLICATION);
    }

    public String getMainClass() {
        if (!isApplicationCluster()) {
            return null;
        }
        return flinkConfig.getString(ApplicationConfiguration.APPLICATION_MAIN_CLASS);
    }

    public Boolean getAllowNonRestoredState() {
        if (flinkConfig.contains(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE)) {
            return flinkConfig.get(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE);
        }
        return null;
    }

    public String getSavepointPath() {
        if (flinkConfig.contains(SavepointConfigOptions.SAVEPOINT_PATH)) {
            return flinkConfig.get(SavepointConfigOptions.SAVEPOINT_PATH);
        }
        return null;
    }

    public boolean isPipelineClasspathDefined() {
        return flinkConfig.contains(PipelineOptions.CLASSPATHS);
    }

    public List<String> getJobSpecArgs() {
        if (flinkConfig.contains(ApplicationConfiguration.APPLICATION_ARGS)) {
            return flinkConfig.get(ApplicationConfiguration.APPLICATION_ARGS);
        }
        return null;
    }

    public boolean isHAEnabled() {
        return HighAvailabilityMode.isHighAvailabilityModeActivated(flinkConfig);
    }
}
