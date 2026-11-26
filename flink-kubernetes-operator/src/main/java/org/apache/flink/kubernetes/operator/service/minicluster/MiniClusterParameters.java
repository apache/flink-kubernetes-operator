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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;

import java.util.Collections;
import java.util.Map;

/**
 * Parameters for the MiniCluster pod, which is configured with the same JobManager pod options as a
 * FlinkDeployment's JobManager, but has its own labels.
 */
class MiniClusterParameters extends KubernetesJobManagerParameters {

    /**
     * Required to create {@link KubernetesJobManagerParameters}, but only used for the JobManager
     * memory, which isn't read for a MiniCluster as its resources come from {@code spec.resources}.
     */
    private static final ClusterSpecification UNUSED_CLUSTER_SPECIFICATION =
            new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();

    MiniClusterParameters(Configuration flinkConfig) {
        super(flinkConfig, UNUSED_CLUSTER_SPECIFICATION);
    }

    @Override
    public Map<String, String> getCommonLabels() {
        return Collections.unmodifiableMap(MiniClusterPodFactory.getCommonLabels(getClusterId()));
    }

    @Override
    public Map<String, String> getSelectors() {
        return Collections.unmodifiableMap(MiniClusterPodFactory.getSelectorLabels(getClusterId()));
    }

    /** JobManager and TaskManager are in the same process, so they don't need a Service. */
    @Override
    public boolean isInternalServiceEnabled() {
        return false;
    }
}
