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
import org.apache.flink.kubernetes.operator.health.ClusterHealthInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW;

/** Evaluates whether the cluster is healthy. */
public class ClusterHealthEvaluator {

    private static final String CLUSTER_INFO_KEY = ClusterHealthInfo.class.getSimpleName();

    private static final Logger LOG = LoggerFactory.getLogger(ClusterHealthEvaluator.class);

    private final Clock clock;

    public ClusterHealthEvaluator(Clock clock) {
        this.clock = clock;
    }

    public static ClusterHealthInfo getLastValidClusterHealthInfo(Map<String, String> clusterInfo) {
        LOG.debug("Getting last valid health check info");
        if (clusterInfo.containsKey(CLUSTER_INFO_KEY)) {
            return ClusterHealthInfo.deserialize(clusterInfo.get(CLUSTER_INFO_KEY));
        } else {
            LOG.debug("No last valid health check info");
            return null;
        }
    }

    public static void setLastValidClusterHealthInfo(
            Map<String, String> clusterInfo, ClusterHealthInfo clusterHealthInfo) {
        LOG.debug("Setting last valid health check info");
        clusterInfo.put(CLUSTER_INFO_KEY, ClusterHealthInfo.serialize(clusterHealthInfo));
    }

    public static void removeLastValidClusterHealthInfo(Map<String, String> clusterInfo) {
        LOG.debug("Removing last valid health check info");
        clusterInfo.remove(CLUSTER_INFO_KEY);
    }

    public void evaluate(
            Configuration configuration,
            Map<String, String> clusterInfo,
            ClusterHealthInfo observedClusterHealthInfo) {

        if (ClusterHealthInfo.isValid(observedClusterHealthInfo)) {
            LOG.debug("Observed health info is valid");

            var lastValidClusterHealthInfo = getLastValidClusterHealthInfo(clusterInfo);
            if (lastValidClusterHealthInfo == null) {
                LOG.debug("No last valid health info, skipping health check");
                setLastValidClusterHealthInfo(clusterInfo, observedClusterHealthInfo);
            } else if (observedClusterHealthInfo.getTimeStamp()
                    < lastValidClusterHealthInfo.getTimeStamp()) {
                String msg =
                        "Observed health info timestamp is less than the last valid health info timestamp, this indicates a bug...";
                LOG.error(msg);
                throw new IllegalStateException(msg);
            } else if (observedClusterHealthInfo.getNumRestarts()
                    < lastValidClusterHealthInfo.getNumRestarts()) {
                LOG.debug(
                        "Observed health info number of restarts is less than the last valid health info number of restarts, skipping health check");
                setLastValidClusterHealthInfo(clusterInfo, observedClusterHealthInfo);
            } else {
                boolean isHealthy = true;

                LOG.debug("Valid health info exist, checking cluster health");
                LOG.debug("Last valid health info: {}", lastValidClusterHealthInfo);
                LOG.debug("Observed health info: {}", observedClusterHealthInfo);

                var timestampDiffMs =
                        observedClusterHealthInfo.getTimeStamp()
                                - lastValidClusterHealthInfo.getTimeStamp();
                LOG.debug(
                        "Time difference between health infos: {}",
                        Duration.ofMillis(timestampDiffMs));

                var restartCheckWindow =
                        configuration.get(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_WINDOW);
                var restartCheckWindowMs = restartCheckWindow.toMillis();
                double countMultiplier = (double) restartCheckWindowMs / (double) timestampDiffMs;
                // If the 2 health info timestamp difference is within the window then no
                // scaling needed
                if (countMultiplier > 1) {
                    countMultiplier = 1;
                }
                long numRestarts =
                        (long)
                                ((double)
                                                (observedClusterHealthInfo.getNumRestarts()
                                                        - lastValidClusterHealthInfo
                                                                .getNumRestarts())
                                        * countMultiplier);
                LOG.debug(
                        "Calculated restart count for {} window: {}",
                        restartCheckWindow,
                        numRestarts);

                if (lastValidClusterHealthInfo.getTimeStamp()
                        < clock.millis() - restartCheckWindowMs) {
                    LOG.debug("Last valid health info timestamp is outside of the window");
                    setLastValidClusterHealthInfo(clusterInfo, observedClusterHealthInfo);
                }

                var restartThreshold =
                        configuration.get(OPERATOR_CLUSTER_HEALTH_CHECK_RESTARTS_THRESHOLD);
                if (numRestarts > restartThreshold) {
                    LOG.info("Restart count hit threshold: {}", restartThreshold);
                    setLastValidClusterHealthInfo(clusterInfo, observedClusterHealthInfo);
                    isHealthy = false;
                }

                // Update the health flag
                lastValidClusterHealthInfo = getLastValidClusterHealthInfo(clusterInfo);
                lastValidClusterHealthInfo.setHealthy(isHealthy);
                setLastValidClusterHealthInfo(clusterInfo, lastValidClusterHealthInfo);
            }
        }
    }
}
