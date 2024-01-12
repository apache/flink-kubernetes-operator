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

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.List;

/** A view on Kubernetes cluster resources by nodes and their cpu/memory. */
@RequiredArgsConstructor
@Getter
public class ClusterResourceView {

    private final Instant creationTime = Instant.now();

    private final List<KubernetesNodeResourceInfo> nodes;

    public boolean tryReserve(double cpu, MemorySize memory) {
        for (KubernetesNodeResourceInfo node : nodes) {
            if (node.tryReserve(cpu, memory)) {
                return true;
            }
        }
        return false;
    }

    public void release(double cpu, MemorySize memory) {
        for (KubernetesNodeResourceInfo node : nodes) {
            if (node.tryRelease(cpu, memory.getBytes())) {
                return;
            }
        }
    }

    public void commit() {
        for (KubernetesNodeResourceInfo node : nodes) {
            node.commitPending();
        }
    }

    public void cancelPending() {
        for (KubernetesNodeResourceInfo node : nodes) {
            node.resetPending();
        }
    }

    @Override
    public String toString() {
        return "ClusterResourceView{" + "creationTime=" + creationTime + ", nodes=" + nodes + '}';
    }
}
