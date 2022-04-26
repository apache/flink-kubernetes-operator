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
import org.apache.flink.kubernetes.operator.kubeclient.parameters.StandaloneKubernetesJobManagerParameters;

import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** @link UserLibMountDecorator unit tests */
public class UserLibMountDecoratorTest {

    private StandaloneKubernetesJobManagerParameters jmParameters;
    private UserLibMountDecorator decorator;

    @BeforeEach
    public void setup() {
        jmParameters = mock(StandaloneKubernetesJobManagerParameters.class);
        decorator = new UserLibMountDecorator(jmParameters);
    }

    @Test
    public void testVolumeAdded() {

        when(jmParameters.isApplicationCluster()).thenReturn(true);

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
        when(jmParameters.isApplicationCluster()).thenReturn(false);

        FlinkPod baseFlinkPod = new FlinkPod.Builder().build();
        assertEquals(0, baseFlinkPod.getMainContainer().getVolumeMounts().size());
        assertEquals(0, baseFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes().size());

        FlinkPod decoratedPod = decorator.decorateFlinkPod(baseFlinkPod);
        assertEquals(0, decoratedPod.getMainContainer().getVolumeMounts().size());
        assertEquals(0, decoratedPod.getPodWithoutMainContainer().getSpec().getVolumes().size());
    }
}
