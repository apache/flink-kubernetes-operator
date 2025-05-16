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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.controller.FlinkDeploymentContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import io.fabric8.kubernetes.client.KubernetesClient;

import javax.annotation.Nonnull;

/** The utils for testing kubernetes autoscaler. */
public class TestingKubernetesAutoscalerUtils {

    @Nonnull
    public static KubernetesJobAutoScalerContext createContext(
            String crName, KubernetesClient kubernetesClient) {
        var cr = TestUtils.buildApplicationCluster();
        cr.getMetadata().setName(crName);
        return new KubernetesJobAutoScalerContext(
                new JobID(),
                JobStatus.RUNNING,
                new Configuration(),
                new UnregisteredMetricsGroup(),
                () -> new RestClusterClient<>(new Configuration(), "test-cluster"),
                new FlinkDeploymentContext(
                        cr,
                        new TestUtils.TestingContext<>() {
                            @Override
                            public KubernetesClient getClient() {
                                return kubernetesClient;
                            }
                        },
                        null,
                        new FlinkConfigManager(new Configuration()),
                        null,
                        null));
    }
}
