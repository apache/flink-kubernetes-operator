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
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesJobManagerParameters;
import org.apache.flink.kubernetes.operator.standalone.StandaloneKubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodSpecBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.VolumeBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.VolumeMount;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @link UserLibMountDecorator unit tests
 */
public class UserLibMountDecoratorTest {

    @Test
    public void testVolumeAddedApplicationMode() {
        StandaloneKubernetesJobManagerParameters jmParameters =
                createJmParams(
                        StandaloneKubernetesConfigOptionsInternal.ClusterMode.APPLICATION,
                        Collections.singletonList("/path"));

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
    public void testVolumeNotAddedSessionMode() {
        StandaloneKubernetesJobManagerParameters jmParameters =
                createJmParams(
                        StandaloneKubernetesConfigOptionsInternal.ClusterMode.SESSION,
                        Collections.singletonList("/path"));
        UserLibMountDecorator decorator = new UserLibMountDecorator(jmParameters);

        FlinkPod baseFlinkPod = new FlinkPod.Builder().build();
        assertEquals(0, baseFlinkPod.getMainContainer().getVolumeMounts().size());
        assertEquals(0, baseFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes().size());

        FlinkPod decoratedPod = decorator.decorateFlinkPod(baseFlinkPod);
        assertEquals(0, decoratedPod.getMainContainer().getVolumeMounts().size());
        assertEquals(0, decoratedPod.getPodWithoutMainContainer().getSpec().getVolumes().size());
    }

    @Test
    public void testVolumeNotAddedExistingVolumeMount() {
        StandaloneKubernetesJobManagerParameters jmParameters =
                createJmParams(
                        StandaloneKubernetesConfigOptionsInternal.ClusterMode.APPLICATION,
                        Collections.singletonList("/path"));
        UserLibMountDecorator decorator = new UserLibMountDecorator(jmParameters);

        final String volName = "flink-artifact";
        final String userLibPath = "/opt/flink/usrlib";

        FlinkPod baseFlinkPod =
                new FlinkPod.Builder()
                        .withMainContainer(
                                new ContainerBuilder()
                                        .addNewVolumeMount()
                                        .withName(volName)
                                        .withMountPath(userLibPath)
                                        .endVolumeMount()
                                        .build())
                        .withPod(
                                new PodBuilder()
                                        .withSpec(
                                                new PodSpecBuilder()
                                                        .addNewVolumeLike(
                                                                new VolumeBuilder()
                                                                        .withName(volName)
                                                                        .withNewEmptyDir()
                                                                        .endEmptyDir()
                                                                        .build())
                                                        .endVolume()
                                                        .build())
                                        .build())
                        .build();

        assertEquals(1, baseFlinkPod.getMainContainer().getVolumeMounts().size());
        assertEquals(1, baseFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes().size());

        FlinkPod decoratedPod = decorator.decorateFlinkPod(baseFlinkPod);
        assertEquals(1, decoratedPod.getMainContainer().getVolumeMounts().size());
        assertEquals(1, decoratedPod.getPodWithoutMainContainer().getSpec().getVolumes().size());
    }

    @Test
    public void testVolumeNotAddedNoPipelineClasspaths() {
        StandaloneKubernetesJobManagerParameters jmParameters =
                createJmParams(
                        StandaloneKubernetesConfigOptionsInternal.ClusterMode.APPLICATION, null);
        UserLibMountDecorator decorator = new UserLibMountDecorator(jmParameters);

        FlinkPod baseFlinkPod = new FlinkPod.Builder().build();
        assertEquals(0, baseFlinkPod.getMainContainer().getVolumeMounts().size());
        assertEquals(0, baseFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes().size());

        FlinkPod decoratedPod = decorator.decorateFlinkPod(baseFlinkPod);
        assertEquals(0, decoratedPod.getMainContainer().getVolumeMounts().size());
        assertEquals(0, decoratedPod.getPodWithoutMainContainer().getSpec().getVolumes().size());
    }

    private StandaloneKubernetesJobManagerParameters createJmParams(
            StandaloneKubernetesConfigOptionsInternal.ClusterMode clusterMode,
            List<String> pipelineClasspaths) {
        Configuration configuration =
                new Configuration()
                        .set(StandaloneKubernetesConfigOptionsInternal.CLUSTER_MODE, clusterMode);

        if (pipelineClasspaths != null && !pipelineClasspaths.isEmpty()) {
            configuration.set(PipelineOptions.CLASSPATHS, pipelineClasspaths);
        }

        return new StandaloneKubernetesJobManagerParameters(
                configuration,
                new ClusterSpecification.ClusterSpecificationBuilder()
                        .createClusterSpecification());
    }
}
