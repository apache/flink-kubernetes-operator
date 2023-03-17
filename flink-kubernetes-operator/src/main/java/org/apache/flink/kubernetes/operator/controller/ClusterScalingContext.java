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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.operator.config.FlinkOperatorConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/** A context to hold state across deployments. */
public class ClusterScalingContext {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterScalingContext.class);

    private final Duration clusterCoolDown;

    private Clock clock;
    private Instant nextPossibleRescaling;

    public ClusterScalingContext(FlinkOperatorConfiguration operatorConfig) {
        this(operatorConfig.getRescalingClusterCoolDown(), Clock.systemDefaultZone());
    }

    @VisibleForTesting
    public ClusterScalingContext(Duration clusterCoolDown, Clock clock) {
        this.clusterCoolDown = clusterCoolDown;
        updateClock(clock);
        updateNextPossibleRescaling();
    }

    public boolean maybeExecuteScalingLogic(Supplier<Boolean> scalingLogic) {
        if (clock.instant().isBefore(nextPossibleRescaling)) {
            LOG.info(
                    "Deferring scaling evaluation until {} to allow cluster to cool down.",
                    nextPossibleRescaling);
            return false;
        }

        LOG.info("Evaluating scaling decision");
        boolean scaled = scalingLogic.get();
        if (scaled) {
            updateNextPossibleRescaling();
        }
        return scaled;
    }

    private void updateNextPossibleRescaling() {
        nextPossibleRescaling = clock.instant().plus(clusterCoolDown);
    }

    @VisibleForTesting
    public Duration getClusterCoolDownDuration() {
        return clusterCoolDown;
    }

    @VisibleForTesting
    public void updateClock(Clock clock) {
        this.clock = clock;
    }
}
