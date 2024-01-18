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

import lombok.AllArgsConstructor;
import lombok.Getter;

/** A single Kubernetes node and its resources (cpu / memory). */
@AllArgsConstructor
@Getter
public class KubernetesNodeResourceInfo {
    private String name;
    private String nodeGroup;
    private KubernetesResource cpu;
    private KubernetesResource memory;

    public boolean tryReserve(double cpuAmount, MemorySize memoryAmount) {
        if (cpu.getFree() >= cpuAmount && memory.getFree() >= memoryAmount.getBytes()) {
            cpu.setPending(cpu.getPending() + cpuAmount);
            memory.setPending(memory.getPending() + memoryAmount.getBytes());
            return true;
        }
        return false;
    }

    public void commitPending() {
        cpu.commitPending();
        memory.commitPending();
    }

    public void resetPending() {
        cpu.setPending(0);
        memory.setPending(0);
    }

    public boolean tryRelease(double cpuAmount, double memoryAmount) {
        if (cpu.getUsed() >= cpuAmount && memory.getUsed() >= memoryAmount) {
            cpu.release(cpuAmount);
            memory.release(memoryAmount);
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "KubernetesNodeResourceInfo{"
                + "name='"
                + name
                + '\''
                + ", cpu="
                + cpu
                + ", memory="
                + memory
                + '}';
    }
}
