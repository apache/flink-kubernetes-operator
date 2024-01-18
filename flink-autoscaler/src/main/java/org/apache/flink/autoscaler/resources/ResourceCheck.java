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

package org.apache.flink.autoscaler.resources;

import org.apache.flink.configuration.MemorySize;

/** An interface for checking the available capacity of the underlying resources. */
public interface ResourceCheck {

    /**
     * Simulates scheduling the provided number of TaskManager instances.
     *
     * @param currentInstances The current number of instances.
     * @param newInstances The new number of instances.
     * @param cpuPerInstance The number of CPU per instances.
     * @param memoryPerInstance The total memory size per instances.
     * @return true if a scheduling configuration was found, false otherwise.
     */
    boolean trySchedule(
            int currentInstances,
            int newInstances,
            double cpuPerInstance,
            MemorySize memoryPerInstance);
}
