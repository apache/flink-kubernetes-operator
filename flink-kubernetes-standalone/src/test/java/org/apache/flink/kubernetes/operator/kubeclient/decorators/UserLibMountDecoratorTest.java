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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesJobManagerParameters;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;

import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** @link UserLibMountDecorator unit tests */
public class UserLibMountDecoratorTest {

    @Test
    public void testVolumeAdded() {
        StandaloneKubernetesJobManagerParameters jmParameters =
                createJmParamsWithClusterMode(
                        StandaloneKubernetesConfigOptionsInternal.ClusterMode.APPLICATION);

        UserLibMountDecorator decorator = new UserLibMountDecorator(jmParameters);

        FlinkPod baseFlinkPod = new FlinkPod.Builder().build();

        assertEquals(0, baseFlinkPod.getMainContainer().getVolumeMounts().size());
        assertEquals(0, baseFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes().size());

        FlinkPod decoratedPod = decorator.decorateFlinkPod(baseFlinkPod);
        assertEquals(1, decoratedPod.getMainContainer().getVolumeMounts().size());
        assertEquals(1, decoratedPod.getPodWithoutMainContainer().getSpec().getVolumes().size());

        VolumeMount volumeMount = decoratedPod.getMainContainer().getVolumeMounts().get(0);

        assertEquals("/opt/flink/usrlib", volumeMount.getMountPath());
    }

    @Test
    public void testVolumeNotAdded() {
        StandaloneKubernetesJobManagerParameters jmParameters =
                createJmParamsWithClusterMode(
                        StandaloneKubernetesConfigOptionsInternal.ClusterMode.SESSION);
        UserLibMountDecorator decorator = new UserLibMountDecorator(jmParameters);

        FlinkPod baseFlinkPod = new FlinkPod.Builder().build();
        assertEquals(0, baseFlinkPod.getMainContainer().getVolumeMounts().size());
        assertEquals(0, baseFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes().size());

        FlinkPod decoratedPod = decorator.decorateFlinkPod(baseFlinkPod);
        assertEquals(0, decoratedPod.getMainContainer().getVolumeMounts().size());
        assertEquals(0, decoratedPod.getPodWithoutMainContainer().getSpec().getVolumes().size());
    }

    private StandaloneKubernetesJobManagerParameters createJmParamsWithClusterMode(
            StandaloneKubernetesConfigOptionsInternal.ClusterMode clusterMode) {
        return new StandaloneKubernetesJobManagerParameters(
                new Configuration()
                        .set(StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE, clusterMode),
                new ClusterSpecification.ClusterSpecificationBuilder()
                        .createClusterSpecification());
    }
}
