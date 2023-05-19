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

package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.health.ClusterHealthInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;

/** An observer to observe the cluster health. */
public class ClusterHealthObserver {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterHealthObserver.class);
    private static final String FULL_RESTARTS_METRIC_NAME = "fullRestarts";
    private static final String NUM_RESTARTS_METRIC_NAME = "numRestarts";
    private static final String NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC_NAME =
            "numberOfCompletedCheckpoints";
    private final ClusterHealthEvaluator clusterHealthEvaluator;

    public ClusterHealthObserver() {
        this.clusterHealthEvaluator = new ClusterHealthEvaluator(Clock.systemDefaultZone());
    }

    /**
     * Observe the health of the flink cluster.
     *
     * @param ctx Resource context.
     */
    public void observe(FlinkResourceContext<FlinkDeployment> ctx) {
        var flinkApp = ctx.getResource();
        try {
            LOG.debug("Observing cluster health");
            var deploymentStatus = flinkApp.getStatus();
            var jobStatus = deploymentStatus.getJobStatus();
            var jobId = jobStatus.getJobId();
            var metrics =
                    ctx.getFlinkService()
                            .getMetrics(
                                    ctx.getObserveConfig(),
                                    jobId,
                                    List.of(
                                            FULL_RESTARTS_METRIC_NAME,
                                            NUM_RESTARTS_METRIC_NAME,
                                            NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC_NAME));
            ClusterHealthInfo observedClusterHealthInfo = new ClusterHealthInfo();
            if (metrics.containsKey(NUM_RESTARTS_METRIC_NAME)) {
                LOG.debug(NUM_RESTARTS_METRIC_NAME + " metric is used");
                observedClusterHealthInfo.setNumRestarts(
                        Integer.parseInt(metrics.get(NUM_RESTARTS_METRIC_NAME)));
            } else if (metrics.containsKey(FULL_RESTARTS_METRIC_NAME)) {
                LOG.debug(
                        FULL_RESTARTS_METRIC_NAME
                                + " metric is used because "
                                + NUM_RESTARTS_METRIC_NAME
                                + " is missing");
                observedClusterHealthInfo.setNumRestarts(
                        Integer.parseInt(metrics.get(FULL_RESTARTS_METRIC_NAME)));
            } else {
                throw new IllegalStateException(
                        "No job restart metric found. Either "
                                + FULL_RESTARTS_METRIC_NAME
                                + "(old and deprecated in never Flink versions) or "
                                + NUM_RESTARTS_METRIC_NAME
                                + "(new) must exist.");
            }
            observedClusterHealthInfo.setNumCompletedCheckpoints(
                    Integer.parseInt(metrics.get(NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC_NAME)));
            LOG.debug("Observed cluster health: {}", observedClusterHealthInfo);

            clusterHealthEvaluator.evaluate(
                    ctx.getObserveConfig(),
                    deploymentStatus.getClusterInfo(),
                    observedClusterHealthInfo);
        } catch (Exception e) {
            LOG.warn("Exception while observing cluster health: {}", e.getMessage());
            // Intentionally not throwing exception since we handle fetch metrics failure as
            // temporary issue
        }
    }
}
