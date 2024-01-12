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

import org.apache.flink.configuration.MemorySize;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link ClusterResourceView}. */
public class ClusterResourceViewTest {

    @Test
    void testStagingAndCommitting() {
        var nodes =
                List.of(
                        new KubernetesNodeResourceInfo(
                                "node1",
                                "node-group-1",
                                new KubernetesResource(11, 1),
                                new KubernetesResource(256, 128)));

        var clusterResourceView = new ClusterResourceView(nodes);

        var cpu = 10;
        var mem = MemorySize.parse("128 bytes");

        assertThat(clusterResourceView.tryReserve(cpu, mem)).isTrue();
        assertThat(clusterResourceView.tryReserve(cpu, mem)).isFalse();

        clusterResourceView.cancelPending();

        assertThat(clusterResourceView.tryReserve(cpu, mem)).isTrue();

        clusterResourceView.commit();

        assertThat(clusterResourceView.tryReserve(cpu, mem)).isFalse();

        clusterResourceView.release(cpu, mem);
        assertThat(clusterResourceView.tryReserve(cpu, mem)).isTrue();
        clusterResourceView.cancelPending();
        clusterResourceView.release(cpu, mem);
        assertThat(clusterResourceView.tryReserve(cpu, mem)).isTrue();

        clusterResourceView.commit();

        assertThat(clusterResourceView.tryReserve(cpu, mem)).isFalse();
    }
}
