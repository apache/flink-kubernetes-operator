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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.health.ClusterHealthInfo;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;

/** An observer to observe the cluster health. */
public class ClusterHealthObserver {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterHealthObserver.class);
    private static final String FULL_RESTARTS_METRIC_NAME = "fullRestarts";
    private static final String NUM_RESTARTS_METRIC_NAME = "numRestarts";
    private final FlinkService flinkService;
    private final ClusterHealthEvaluator clusterHealthEvaluator;

    public ClusterHealthObserver(FlinkService flinkService) {
        this.flinkService = flinkService;
        this.clusterHealthEvaluator = new ClusterHealthEvaluator(Clock.systemDefaultZone());
    }

    /** Observe the health of the flink cluster. */
    public void observe(FlinkDeployment flinkApp, Configuration deployedConfig) {
        try {
            LOG.info("Observing cluster health");
            var deploymentStatus = flinkApp.getStatus();
            var jobStatus = deploymentStatus.getJobStatus();
            var jobId = jobStatus.getJobId();
            var metrics =
                    flinkService.getMetrics(
                            deployedConfig,
                            jobId,
                            List.of(FULL_RESTARTS_METRIC_NAME, NUM_RESTARTS_METRIC_NAME));
            ClusterHealthInfo observedClusterHealthInfo;
            if (metrics.containsKey(NUM_RESTARTS_METRIC_NAME)) {
                LOG.debug(NUM_RESTARTS_METRIC_NAME + " metric is used");
                observedClusterHealthInfo =
                        ClusterHealthInfo.of(
                                Integer.parseInt(metrics.get(NUM_RESTARTS_METRIC_NAME)));
            } else if (metrics.containsKey(FULL_RESTARTS_METRIC_NAME)) {
                LOG.debug(
                        FULL_RESTARTS_METRIC_NAME
                                + " metric is used because "
                                + NUM_RESTARTS_METRIC_NAME
                                + " is missing");
                observedClusterHealthInfo =
                        ClusterHealthInfo.of(
                                Integer.parseInt(metrics.get(FULL_RESTARTS_METRIC_NAME)));
            } else {
                throw new IllegalStateException(
                        "No job restart metric found. Either "
                                + FULL_RESTARTS_METRIC_NAME
                                + "(old and deprecated in never Flink versions) or "
                                + NUM_RESTARTS_METRIC_NAME
                                + "(new) must exist.");
            }
            LOG.debug("Observed cluster health: {}", observedClusterHealthInfo);

            clusterHealthEvaluator.evaluate(
                    deployedConfig, deploymentStatus.getClusterInfo(), observedClusterHealthInfo);
        } catch (Exception e) {
            LOG.warn("Exception while observing cluster health: {}", e.getMessage());
            // Intentionally not throwing exception since we handle fetch metrics failure as
            // temporary issue
        }
    }
}
