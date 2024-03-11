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

package org.apache.flink.autoscaler.tuning;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.CommonProcessMemorySpec;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemory;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** The task manager memory spec for memory tuning. */
@AllArgsConstructor
@Getter
public class TuningSimpleMemorySpec {

    private final MemorySize heapSize;
    private final MemorySize managedSize;
    private final MemorySize networkSize;
    private final MemorySize metaspaceSize;

    public TuningSimpleMemorySpec(CommonProcessMemorySpec<TaskExecutorFlinkMemory> memSpecs) {
        this(
                memSpecs.getFlinkMemory().getJvmHeapMemorySize(),
                memSpecs.getFlinkMemory().getManaged(),
                memSpecs.getFlinkMemory().getNetwork(),
                memSpecs.getJvmMetaspaceSize());
    }

    @Override
    public String toString() {
        return "TuningSimpleMemorySpec {"
                + "heap="
                + heapSize.toHumanReadableString()
                + ", managed="
                + managedSize.toHumanReadableString()
                + ", network="
                + networkSize.toHumanReadableString()
                + ", metaspace="
                + metaspaceSize.toHumanReadableString()
                + '}';
    }
}
