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

package org.apache.flink.kubernetes.operator.resources;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link KubernetesResource}. */
public class KubernetesResourceTest {

    @Test
    void testFreeCapacity() {
        assertThat(new KubernetesResource(100, 0).getFree()).isEqualTo(100);
        assertThat(new KubernetesResource(100, 0).getUsed()).isEqualTo(0);
        assertThat(new KubernetesResource(100, 1).getFree()).isEqualTo(99);
        assertThat(new KubernetesResource(100, 1).getUsed()).isEqualTo(1);

        KubernetesResource kubernetesResource = new KubernetesResource(100, 1);
        kubernetesResource.setPending(1);
        assertThat(kubernetesResource.getFree()).isEqualTo(98);
        assertThat(kubernetesResource.getUsed()).isEqualTo(2);
    }

    @Test
    void testCommit() {
        KubernetesResource kubernetesResource = new KubernetesResource(100, 1);
        kubernetesResource.setPending(1);
        assertThat(kubernetesResource.getFree()).isEqualTo(98);

        kubernetesResource.commitPending();
        assertThat(kubernetesResource.getFree()).isEqualTo(98);

        kubernetesResource.setPending(1);
        assertThat(kubernetesResource.getFree()).isEqualTo(97);
    }

    @Test
    void testRelease() {
        KubernetesResource kubernetesResource = new KubernetesResource(100, 1);
        assertThat(kubernetesResource.getUsed()).isEqualTo(1);

        kubernetesResource.release(1);
        assertThat(kubernetesResource.getUsed()).isEqualTo(0);

        kubernetesResource.release(1);
        assertThat(kubernetesResource.getUsed()).isEqualTo(0);
    }
}
