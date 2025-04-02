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

package org.apache.flink.kubernetes.operator.mutator;

import org.apache.flink.core.plugin.Plugin;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;

import java.util.Optional;

/** Mutator for Flink Resources. */
public interface FlinkResourceMutator extends Plugin {

    /**
     * Mutate deployment and return the mutated Object.
     *
     * @param deployment A Flink application or session cluster deployment.
     * @return the mutated Flink application or session cluster deployment.
     */
    FlinkDeployment mutateDeployment(FlinkDeployment deployment);

    /**
     * Mutate session job and return the mutated Object.
     *
     * @param sessionJob the session job to be mutated.
     * @param session the target session cluster of the session job to be Mutated.
     * @return the mutated session job.
     */
    FlinkSessionJob mutateSessionJob(FlinkSessionJob sessionJob, Optional<FlinkDeployment> session);

    /**
     * Mutate snapshot and return the mutated Object.
     *
     * @param stateSnapshot the snapshot to be mutated.
     * @return the mutated snapshot.
     */
    FlinkStateSnapshot mutateStateSnapshot(FlinkStateSnapshot stateSnapshot);
}
