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

package org.apache.flink.autoscaler.config;

import org.apache.flink.autoscaler.metrics.MetricAggregator;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.util.List;

/** Config options related to the autoscaler module. */
public class AutoScalerOptions {

    public static final String DEPRECATED_K8S_OP_CONF_PREFIX = "kubernetes.operator.";
    public static final String AUTOSCALER_CONF_PREFIX = "job.autoscaler.";

    private static String deprecatedOperatorConfigKey(String key) {
        return DEPRECATED_K8S_OP_CONF_PREFIX + AUTOSCALER_CONF_PREFIX + key;
    }

    private static String autoScalerConfigKey(String key) {
        return AUTOSCALER_CONF_PREFIX + key;
    }

    private static ConfigOptions.OptionBuilder autoScalerConfig(String key) {
        return ConfigOptions.key(autoScalerConfigKey(key));
    }

    public static final ConfigOption<Boolean> AUTOSCALER_ENABLED =
            autoScalerConfig("enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("enabled"))
                    .withDescription("Enable job autoscaler module.");

    public static final ConfigOption<Boolean> SCALING_ENABLED =
            autoScalerConfig("scaling.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("scaling.enabled"))
                    .withDescription(
                            "Enable vertex scaling execution by the autoscaler. If disabled, the autoscaler will only collect metrics and evaluate the suggested parallelism for each vertex but will not upgrade the jobs.");

    public static final ConfigOption<Duration> METRICS_WINDOW =
            autoScalerConfig("metrics.window")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(15))
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("metrics.window"))
                    .withDescription("Scaling metrics aggregation window size.");

    public static final ConfigOption<Duration> STABILIZATION_INTERVAL =
            autoScalerConfig("stabilization.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("stabilization.interval"))
                    .withDescription(
                            "Stabilization period in which no new scaling will be executed");

    public static final ConfigOption<Double> TARGET_UTILIZATION =
            autoScalerConfig("target.utilization")
                    .doubleType()
                    .defaultValue(0.7)
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("target.utilization"))
                    .withDescription("Target vertex utilization");

    public static final ConfigOption<Double> TARGET_UTILIZATION_BOUNDARY =
            autoScalerConfig("target.utilization.boundary")
                    .doubleType()
                    .defaultValue(0.3)
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("target.utilization.boundary"))
                    .withDescription(
                            "Target vertex utilization boundary. Scaling won't be performed if the current processing rate is within [target_rate / (target_utilization - boundary), (target_rate / (target_utilization + boundary)]");

    public static final ConfigOption<Duration> SCALE_UP_GRACE_PERIOD =
            autoScalerConfig("scale-up.grace-period")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("scale-up.grace-period"))
                    .withDescription(
                            "Duration in which no scale down of a vertex is allowed after it has been scaled up.");

    public static final ConfigOption<Integer> VERTEX_MIN_PARALLELISM =
            autoScalerConfig("vertex.min-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("vertex.min-parallelism"))
                    .withDescription("The minimum parallelism the autoscaler can use.");

    public static final ConfigOption<Integer> VERTEX_MAX_PARALLELISM =
            autoScalerConfig("vertex.max-parallelism")
                    .intType()
                    .defaultValue(200)
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("vertex.max-parallelism"))
                    .withDescription(
                            "The maximum parallelism the autoscaler can use. Note that this limit will be ignored if it is higher than the max parallelism configured in the Flink config or directly on each operator.");

    public static final ConfigOption<Double> MAX_SCALE_DOWN_FACTOR =
            autoScalerConfig("scale-down.max-factor")
                    .doubleType()
                    .defaultValue(0.6)
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("scale-down.max-factor"))
                    .withDescription(
                            "Max scale down factor. 1 means no limit on scale down, 0.6 means job can only be scaled down with 60% of the original parallelism.");

    public static final ConfigOption<Double> MAX_SCALE_UP_FACTOR =
            autoScalerConfig("scale-up.max-factor")
                    .doubleType()
                    .defaultValue(100000.)
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("scale-up.max-factor"))
                    .withDescription(
                            "Max scale up factor. 2.0 means job can only be scaled up with 200% of the current parallelism.");

    public static final ConfigOption<Duration> CATCH_UP_DURATION =
            autoScalerConfig("catch-up.duration")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("catch-up.duration"))
                    .withDescription(
                            "The target duration for fully processing any backlog after a scaling operation. Set to 0 to disable backlog based scaling.");

    public static final ConfigOption<Duration> RESTART_TIME =
            autoScalerConfig("restart.time")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("restart.time"))
                    .withDescription(
                            "Expected restart time to be used until the operator can determine it reliably from history.");

    public static final ConfigOption<Duration> BACKLOG_PROCESSING_LAG_THRESHOLD =
            autoScalerConfig("backlog-processing.lag-threshold")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDeprecatedKeys(
                            deprecatedOperatorConfigKey("backlog-processing.lag-threshold"))
                    .withDescription(
                            "Lag threshold which will prevent unnecessary scalings while removing the pending messages responsible for the lag.");

    public static final ConfigOption<Duration> OBSERVE_TRUE_PROCESSING_RATE_LAG_THRESHOLD =
            autoScalerConfig("observed-true-processing-rate.lag-threshold")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDeprecatedKeys(
                            deprecatedOperatorConfigKey(
                                    "observed-true-processing-rate.lag-threshold"))
                    .withDescription(
                            "Lag threshold for enabling observed true processing rate measurements.");

    public static final ConfigOption<Double> OBSERVED_TRUE_PROCESSING_RATE_SWITCH_THRESHOLD =
            autoScalerConfig("observed-true-processing-rate.switch-threshold")
                    .doubleType()
                    .defaultValue(0.15)
                    .withDeprecatedKeys(
                            deprecatedOperatorConfigKey(
                                    "observed-true-processing-rate.switch-threshold"))
                    .withDescription(
                            "Percentage threshold for switching to observed from busy time based true processing rate if the measurement is off by at least the configured fraction. For example 0.15 means we switch to observed if the busy time based computation is at least 15% higher during catchup.");

    public static final ConfigOption<Integer> OBSERVED_TRUE_PROCESSING_RATE_MIN_OBSERVATIONS =
            autoScalerConfig("observed-true-processing-rate.min-observations")
                    .intType()
                    .defaultValue(2)
                    .withDeprecatedKeys(
                            deprecatedOperatorConfigKey(
                                    "observed-true-processing-rate.min-observations"))
                    .withDescription(
                            "Minimum nr of observations used when estimating / switching to observed true processing rate.");

    public static final ConfigOption<Boolean> SCALING_EFFECTIVENESS_DETECTION_ENABLED =
            autoScalerConfig("scaling.effectiveness.detection.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys(
                            deprecatedOperatorConfigKey("scaling.effectiveness.detection.enabled"))
                    .withDescription(
                            "Whether to enable detection of ineffective scaling operations and allowing the autoscaler to block further scale ups.");

    public static final ConfigOption<Double> SCALING_EFFECTIVENESS_THRESHOLD =
            autoScalerConfig("scaling.effectiveness.threshold")
                    .doubleType()
                    .defaultValue(0.1)
                    .withDeprecatedKeys(
                            deprecatedOperatorConfigKey("scaling.effectiveness.threshold"))
                    .withDescription(
                            "Processing rate increase threshold for detecting ineffective scaling threshold. 0.1 means if we do not accomplish at least 10% of the desired capacity increase with scaling, the action is marked ineffective.");

    public static final ConfigOption<Integer> VERTEX_SCALING_HISTORY_COUNT =
            autoScalerConfig("history.max.count")
                    .intType()
                    .defaultValue(3)
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("history.max.count"))
                    .withDescription(
                            "Maximum number of past scaling decisions to retain per vertex.");

    public static final ConfigOption<Duration> VERTEX_SCALING_HISTORY_AGE =
            autoScalerConfig("history.max.age")
                    .durationType()
                    .defaultValue(Duration.ofHours(24))
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("history.max.age"))
                    .withDescription("Maximum age for past scaling decisions to retain.");

    public static final ConfigOption<MetricAggregator> BUSY_TIME_AGGREGATOR =
            autoScalerConfig("metrics.busy-time.aggregator")
                    .enumType(MetricAggregator.class)
                    .defaultValue(MetricAggregator.MAX)
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("metrics.busy-time.aggregator"))
                    .withDescription(
                            "Metric aggregator to use for busyTime metrics. This affects how true processing/output rate will be computed. Using max allows us to handle jobs with data skew more robustly, while avg may provide better stability when we know that the load distribution is even.");

    public static final ConfigOption<List<String>> VERTEX_EXCLUDE_IDS =
            autoScalerConfig("vertex.exclude.ids")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("vertex.exclude.ids"))
                    .withDescription(
                            "A (semicolon-separated) list of vertex ids in hexstring for which to disable scaling. Caution: For non-sink vertices this will still scale their downstream operators until https://issues.apache.org/jira/browse/FLINK-31215 is implemented.");

    public static final ConfigOption<Duration> SCALING_EVENT_INTERVAL =
            autoScalerConfig("scaling.event.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDeprecatedKeys(deprecatedOperatorConfigKey("scaling.event.interval"))
                    .withDescription("Time interval to resend the identical event");

    public static final ConfigOption<Duration> FLINK_CLIENT_TIMEOUT =
            autoScalerConfig("flink.rest-client.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("The timeout for waiting the flink rest client to return.");
}
