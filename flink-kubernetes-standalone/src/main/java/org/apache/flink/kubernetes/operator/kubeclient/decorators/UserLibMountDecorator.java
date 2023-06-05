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
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesJobManagerParameters;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Container;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.ContainerBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Pod;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.PodBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.Volume;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.VolumeBuilder;
import org.apache.flink.kubernetes.shaded.io.fabric8.kubernetes.api.model.VolumeMount;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Mount the Flink User Lib directory to enable Flink to pick up a Jars defined in
 * pipeline.classpaths. Used for starting standalone application clusters
 */
public class UserLibMountDecorator extends AbstractKubernetesStepDecorator {
    private static final String USER_LIB_VOLUME = "user-lib-dir";
    private static final String USER_LIB_PATH = "/opt/flink/usrlib";

    private final StandaloneKubernetesJobManagerParameters kubernetesJobManagerParameters;

    public UserLibMountDecorator(
            StandaloneKubernetesJobManagerParameters kubernetesJobManagerParameters) {
        this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        if (!kubernetesJobManagerParameters.isApplicationCluster()) {
            return flinkPod;
        }

        if (mainContainerHasUserLibPath(flinkPod)) {
            return flinkPod;
        }

        if (!kubernetesJobManagerParameters.isPipelineClasspathDefined()) {
            return flinkPod;
        }

        final Volume userLibVolume =
                new VolumeBuilder()
                        .withName(USER_LIB_VOLUME)
                        .withNewEmptyDir()
                        .endEmptyDir()
                        .build();

        final Pod pod =
                new PodBuilder(flinkPod.getPodWithoutMainContainer())
                        .editSpec()
                        .addNewVolumeLike(userLibVolume)
                        .endVolume()
                        .endSpec()
                        .build();

        final Container mountedMainContainer =
                new ContainerBuilder(flinkPod.getMainContainer())
                        .addNewVolumeMount()
                        .withName(USER_LIB_VOLUME)
                        .withMountPath(USER_LIB_PATH)
                        .endVolumeMount()
                        .build();
        return new FlinkPod.Builder(flinkPod)
                .withPod(pod)
                .withMainContainer(mountedMainContainer)
                .build();
    }

    private boolean mainContainerHasUserLibPath(FlinkPod flinkPod) {
        List<VolumeMount> volumeMounts = flinkPod.getMainContainer().getVolumeMounts();
        return volumeMounts.stream()
                .anyMatch(volumeMount -> volumeMount.getMountPath().startsWith(USER_LIB_PATH));
    }
}
