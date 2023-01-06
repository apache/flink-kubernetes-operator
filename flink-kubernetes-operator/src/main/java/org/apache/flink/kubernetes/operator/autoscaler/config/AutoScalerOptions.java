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

package org.apache.flink.kubernetes.operator.autoscaler.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.operatorConfig;

/** Config options related to the autoscaler module. */
public class AutoScalerOptions {

    private static ConfigOptions.OptionBuilder autoScalerConfig(String key) {
        return operatorConfig("job.autoscaler." + key);
    }

    public static final ConfigOption<Boolean> AUTOSCALER_ENABLED =
            autoScalerConfig("enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable job autoscaler module.");

    public static final ConfigOption<Boolean> SCALING_ENABLED =
            autoScalerConfig("scaling.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enable vertex scaling execution by the autoscaler. If disabled, the autoscaler will only collect metrics and evaluate the suggested parallelism for each vertex but will not upgrade the jobs.");

    public static final ConfigOption<Duration> METRICS_WINDOW =
            autoScalerConfig("metrics.window")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription("Scaling metrics aggregation window size.");

    public static final ConfigOption<Duration> STABILIZATION_INTERVAL =
            autoScalerConfig("stabilization.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription(
                            "Stabilization period in which no new scaling will be executed");

    public static final ConfigOption<Boolean> SOURCE_SCALING_ENABLED =
            autoScalerConfig("scaling.sources.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable scaling source vertices. "
                                    + "Source vertices set the baseline ingestion rate for the processing based on the backlog size. "
                                    + "If disabled, only regular job vertices will be scaled and source vertices will be unchanged.");

    public static final ConfigOption<Double> TARGET_UTILIZATION =
            autoScalerConfig("target.utilization")
                    .doubleType()
                    .defaultValue(0.7)
                    .withDescription("Target vertex utilization");

    public static final ConfigOption<Double> TARGET_UTILIZATION_BOUNDARY =
            autoScalerConfig("target.utilization.boundary")
                    .doubleType()
                    .defaultValue(0.1)
                    .withDescription(
                            "Target vertex utilization boundary. Scaling won't be performed if utilization is within (target - boundary, target + boundary)");

    public static final ConfigOption<Duration> SCALE_UP_GRACE_PERIOD =
            autoScalerConfig("scale-up.grace-period")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription("Period in which no scale down is allowed after a scale up");

    public static final ConfigOption<Integer> VERTEX_MIN_PARALLELISM =
            autoScalerConfig("vertex.min-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The minimum parallelism the autoscaler can use.");

    public static final ConfigOption<Integer> VERTEX_MAX_PARALLELISM =
            autoScalerConfig("vertex.max-parallelism")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            "The maximum parallelism the autoscaler can use. Note that this limit will be ignored if it is higher than the max parallelism configured in the Flink config or directly on each operator.");

    public static final ConfigOption<Double> MAX_SCALE_DOWN_FACTOR =
            autoScalerConfig("scale-down.max-factor")
                    .doubleType()
                    .defaultValue(0.6)
                    .withDescription(
                            "Max scale down factor. 1 means no limit on scale down, 0.6 means job can only be scaled down with 60% of the original parallelism.");

    public static final ConfigOption<Duration> CATCH_UP_DURATION =
            autoScalerConfig("catch-up.duration")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription(
                            "The target duration for fully processing any backlog after a scaling operation. Set to 0 to disable backlog based scaling.");

    public static final ConfigOption<Duration> RESTART_TIME =
            autoScalerConfig("restart.time")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription(
                            "Expected restart time to be used until the operator can determine it reliably from history.");

    public static final ConfigOption<Boolean> SCALING_EFFECTIVENESS_DETECTION_ENABLED =
            autoScalerConfig("scaling.effectiveness.detection.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable detection of ineffective scaling operations and allowing the autoscaler to block further scale ups.");

    public static final ConfigOption<Double> SCALING_EFFECTIVENESS_THRESHOLD =
            autoScalerConfig("scaling.effectiveness.threshold")
                    .doubleType()
                    .defaultValue(0.1)
                    .withDescription(
                            "Processing rate increase threshold for detecting ineffective scaling threshold. 0.1 means if we do not accomplish at least 10% of the desired capacity increase with scaling, the action is marked ineffective.");
}
