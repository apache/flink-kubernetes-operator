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

import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.autoscaler.JobAutoScalerImpl;
import org.apache.flink.kubernetes.operator.resources.ClusterResourceManager;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.FlinkResourceEventCollector;
import org.apache.flink.kubernetes.operator.utils.FlinkStateSnapshotEventCollector;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

/** Test loading the default autoscaling implementation from the classpath. */
@EnableKubernetesMockClient(crud = true)
public class AutoscalerFactoryTest {

    @Getter private KubernetesClient kubernetesClient;

    @Test
    void testLoadDefaultImplementation() {
        JobAutoScaler<ResourceID, KubernetesJobAutoScalerContext> autoScaler =
                AutoscalerFactory.create(
                        kubernetesClient,
                        new EventRecorder(
                                new FlinkResourceEventCollector(),
                                new FlinkStateSnapshotEventCollector()),
                        new ClusterResourceManager(Duration.ZERO, kubernetesClient));
        Assertions.assertTrue(autoScaler instanceof JobAutoScalerImpl);
    }
}
