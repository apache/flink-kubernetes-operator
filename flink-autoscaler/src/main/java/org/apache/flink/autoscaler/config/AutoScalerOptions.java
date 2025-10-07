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

import org.apache.flink.autoscaler.JobVertexScaler;
import org.apache.flink.autoscaler.metrics.MetricAggregator;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;
import java.util.List;

/** Config options related to the autoscaler module. */
public class AutoScalerOptions {

    public static final String OLD_K8S_OP_CONF_PREFIX = "kubernetes.operator.";
    public static final String AUTOSCALER_CONF_PREFIX = "job.autoscaler.";

    private static String oldOperatorConfigKey(String key) {
        return OLD_K8S_OP_CONF_PREFIX + AUTOSCALER_CONF_PREFIX + key;
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
                    .withFallbackKeys(oldOperatorConfigKey("enabled"))
                    .withDescription("Enable job autoscaler module.");

    public static final ConfigOption<Boolean> SCALING_ENABLED =
            autoScalerConfig("scaling.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withFallbackKeys(oldOperatorConfigKey("scaling.enabled"))
                    .withDescription(
                            "Enable vertex scaling execution by the autoscaler. If disabled, the autoscaler will only collect metrics and evaluate the suggested parallelism for each vertex but will not upgrade the jobs.");

    public static final ConfigOption<Duration> METRICS_WINDOW =
            autoScalerConfig("metrics.window")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(15))
                    .withFallbackKeys(oldOperatorConfigKey("metrics.window"))
                    .withDescription("Scaling metrics aggregation window size.");

    public static final ConfigOption<Duration> STABILIZATION_INTERVAL =
            autoScalerConfig("stabilization.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withFallbackKeys(oldOperatorConfigKey("stabilization.interval"))
                    .withDescription(
                            "Stabilization period in which no new scaling will be executed");

    public static final ConfigOption<List<String>> EXCLUDED_PERIODS =
            autoScalerConfig("excluded.periods")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withFallbackKeys(oldOperatorConfigKey("excluded.periods"))
                    .withDescription(
                            "A (semicolon-separated) list of expressions indicate excluded periods during which autoscaling execution is forbidden, the expression consist of two optional subexpressions concatenated with &&, "
                                    + "one is cron expression in Quartz format (6 or 7 positions), "
                                    + "for example, * * 9-11,14-16 * * ? means exclude from 9:00:00am to 11:59:59am and from 2:00:00pm to 4:59:59pm every day, * * * ? * 2-6 means exclude every weekday, etc."
                                    + "see http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html for the usage of cron expression."
                                    + "Caution: in most case cron expression is enough, we introduce the other subexpression: daily expression, because cron can only represent integer hour period without minutes and "
                                    + "seconds suffix, daily expression's formation is startTime-endTime, such as 9:30:30-10:50:20, when exclude from 9:30:30-10:50:20 in Monday and Thursday "
                                    + "we can express it as 9:30:30-10:50:20 && * * * ? * 2,5");

    public static final ConfigOption<Double> UTILIZATION_TARGET =
            autoScalerConfig("utilization.target")
                    .doubleType()
                    .defaultValue(0.7)
                    .withDeprecatedKeys(autoScalerConfigKey("target.utilization"))
                    .withFallbackKeys(
                            oldOperatorConfigKey("utilization.target"),
                            oldOperatorConfigKey("target.utilization"))
                    .withDescription("Target vertex utilization");

    @Deprecated
    public static final ConfigOption<Double> TARGET_UTILIZATION_BOUNDARY =
            autoScalerConfig("target.utilization.boundary")
                    .doubleType()
                    .defaultValue(0.3)
                    .withFallbackKeys(oldOperatorConfigKey("target.utilization.boundary"))
                    .withDescription(
                            "Target vertex utilization boundary. Scaling won't be performed if the processing capacity is within [target_rate / (target_utilization - boundary), (target_rate / (target_utilization + boundary)]");

    public static final ConfigOption<Double> UTILIZATION_MAX =
            autoScalerConfig("utilization.max")
                    .doubleType()
                    .noDefaultValue()
                    .withFallbackKeys(oldOperatorConfigKey("utilization.max"))
                    .withDescription("Max vertex utilization");

    public static final ConfigOption<Double> UTILIZATION_MIN =
            autoScalerConfig("utilization.min")
                    .doubleType()
                    .noDefaultValue()
                    .withFallbackKeys(oldOperatorConfigKey("utilization.min"))
                    .withDescription("Min vertex utilization");

    public static final ConfigOption<Duration> SCALE_DOWN_INTERVAL =
            autoScalerConfig("scale-down.interval")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDeprecatedKeys(autoScalerConfigKey("scale-up.grace-period"))
                    .withFallbackKeys(
                            oldOperatorConfigKey("scale-up.grace-period"),
                            oldOperatorConfigKey("scale-down.interval"))
                    .withDescription(
                            "The delay time for scale down to be executed. If it is greater than 0, the scale down will be delayed. "
                                    + "Delayed rescale can merge multiple scale downs within `scale-down.interval` into a scale down, thereby reducing the number of rescales. "
                                    + "Reducing the frequency of job restarts can improve job availability. "
                                    + "Scale down can be executed directly if it's less than or equal 0.");

    public static final ConfigOption<Integer> VERTEX_MIN_PARALLELISM =
            autoScalerConfig("vertex.min-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withFallbackKeys(oldOperatorConfigKey("vertex.min-parallelism"))
                    .withDescription("The minimum parallelism the autoscaler can use.");

    public static final ConfigOption<Integer> VERTEX_MAX_PARALLELISM =
            autoScalerConfig("vertex.max-parallelism")
                    .intType()
                    .defaultValue(200)
                    .withFallbackKeys(oldOperatorConfigKey("vertex.max-parallelism"))
                    .withDescription(
                            "The maximum parallelism the autoscaler can use. Note that this limit will be ignored if it is higher than the max parallelism configured in the Flink config or directly on each operator.");

    public static final ConfigOption<Double> MAX_SCALE_DOWN_FACTOR =
            autoScalerConfig("scale-down.max-factor")
                    .doubleType()
                    .defaultValue(0.6)
                    .withFallbackKeys(oldOperatorConfigKey("scale-down.max-factor"))
                    .withDescription(
                            "Max scale down factor. 1 means no limit on scale down, 0.6 means job can only be scaled down with 60% of the original parallelism.");

    public static final ConfigOption<Double> MAX_SCALE_UP_FACTOR =
            autoScalerConfig("scale-up.max-factor")
                    .doubleType()
                    .defaultValue(100000.)
                    .withFallbackKeys(oldOperatorConfigKey("scale-up.max-factor"))
                    .withDescription(
                            "Max scale up factor. 2.0 means job can only be scaled up with 200% of the current parallelism.");

    public static final ConfigOption<Duration> CATCH_UP_DURATION =
            autoScalerConfig("catch-up.duration")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withFallbackKeys(oldOperatorConfigKey("catch-up.duration"))
                    .withDescription(
                            "The target duration for fully processing any backlog after a scaling operation. Set to 0 to disable backlog based scaling.");

    public static final ConfigOption<Duration> RESTART_TIME =
            autoScalerConfig("restart.time")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withFallbackKeys(oldOperatorConfigKey("restart.time"))
                    .withDescription(
                            "Expected restart time to be used until the operator can determine it reliably from history.");

    public static final ConfigOption<Boolean> PREFER_TRACKED_RESTART_TIME =
            autoScalerConfig("restart.time-tracking.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys(oldOperatorConfigKey("restart.time-tracking.enabled"))
                    .withDescription(
                            "Whether to use the actual observed rescaling restart times instead of the fixed '"
                                    + RESTART_TIME.key()
                                    + "' configuration. If set to true, the maximum restart duration over a number of "
                                    + "samples will be used. The value of '"
                                    + autoScalerConfigKey("restart.time-tracking.limit")
                                    + "' will act as an upper bound, and the value of '"
                                    + RESTART_TIME.key()
                                    + "' will still be used when there are no rescale samples.");

    public static final ConfigOption<Duration> TRACKED_RESTART_TIME_LIMIT =
            autoScalerConfig("restart.time-tracking.limit")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(15))
                    .withFallbackKeys(oldOperatorConfigKey("restart.time-tracking.limit"))
                    .withDescription(
                            "Maximum cap for the observed restart time when '"
                                    + PREFER_TRACKED_RESTART_TIME.key()
                                    + "' is set to true.");

    public static final ConfigOption<Duration> BACKLOG_PROCESSING_LAG_THRESHOLD =
            autoScalerConfig("backlog-processing.lag-threshold")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withFallbackKeys(oldOperatorConfigKey("backlog-processing.lag-threshold"))
                    .withDescription(
                            "Lag threshold which will prevent unnecessary scalings while removing the pending messages responsible for the lag.");

    public static final ConfigOption<Duration> OBSERVE_TRUE_PROCESSING_RATE_LAG_THRESHOLD =
            autoScalerConfig("observed-true-processing-rate.lag-threshold")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withFallbackKeys(
                            oldOperatorConfigKey("observed-true-processing-rate.lag-threshold"))
                    .withDescription(
                            "Lag threshold for enabling observed true processing rate measurements.");

    public static final ConfigOption<Double> OBSERVED_TRUE_PROCESSING_RATE_SWITCH_THRESHOLD =
            autoScalerConfig("observed-true-processing-rate.switch-threshold")
                    .doubleType()
                    .defaultValue(0.15)
                    .withFallbackKeys(
                            oldOperatorConfigKey("observed-true-processing-rate.switch-threshold"))
                    .withDescription(
                            "Percentage threshold for switching to observed from busy time based true processing rate if the measurement is off by at least the configured fraction. For example 0.15 means we switch to observed if the busy time based computation is at least 15% higher during catchup.");

    public static final ConfigOption<Integer> OBSERVED_TRUE_PROCESSING_RATE_MIN_OBSERVATIONS =
            autoScalerConfig("observed-true-processing-rate.min-observations")
                    .intType()
                    .defaultValue(2)
                    .withFallbackKeys(
                            oldOperatorConfigKey("observed-true-processing-rate.min-observations"))
                    .withDescription(
                            "Minimum nr of observations used when estimating / switching to observed true processing rate.");

    public static final ConfigOption<Boolean> SCALING_EFFECTIVENESS_DETECTION_ENABLED =
            autoScalerConfig("scaling.effectiveness.detection.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys(
                            oldOperatorConfigKey("scaling.effectiveness.detection.enabled"))
                    .withDescription(
                            "Whether to enable detection of ineffective scaling operations and allowing the autoscaler to block further scale ups.");

    public static final ConfigOption<Double> SCALING_EFFECTIVENESS_THRESHOLD =
            autoScalerConfig("scaling.effectiveness.threshold")
                    .doubleType()
                    .defaultValue(0.1)
                    .withFallbackKeys(oldOperatorConfigKey("scaling.effectiveness.threshold"))
                    .withDescription(
                            "Processing rate increase threshold for detecting ineffective scaling threshold. 0.1 means if we do not accomplish at least 10% of the desired capacity increase with scaling, the action is marked ineffective.");

    public static final ConfigOption<Double> GC_PRESSURE_THRESHOLD =
            autoScalerConfig("memory.gc-pressure.threshold")
                    .doubleType()
                    .defaultValue(1.)
                    .withFallbackKeys(oldOperatorConfigKey("memory.gc-pressure.threshold"))
                    .withDescription(
                            "Max allowed GC pressure (percentage spent garbage collecting) during scaling operations. Autoscaling will be paused if the GC pressure exceeds this limit.");

    public static final ConfigOption<Double> HEAP_USAGE_THRESHOLD =
            autoScalerConfig("memory.heap-usage.threshold")
                    .doubleType()
                    .defaultValue(1.)
                    .withFallbackKeys(oldOperatorConfigKey("memory.heap-usage.threshold"))
                    .withDescription(
                            "Max allowed percentage of heap usage during scaling operations. Autoscaling will be paused if the heap usage exceeds this threshold.");

    public static final ConfigOption<Boolean> MEMORY_TUNING_ENABLED =
            autoScalerConfig("memory.tuning.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys(oldOperatorConfigKey("memory.tuning.enabled"))
                    .withDescription(
                            "If enabled, the initial amount of memory specified for TaskManagers will be reduced/increased according to the observed needs.");

    public static final ConfigOption<Boolean> MEMORY_SCALING_ENABLED =
            autoScalerConfig("memory.tuning.scale-down-compensation.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withFallbackKeys(
                            oldOperatorConfigKey("memory.tuning.scale-down-compensation.enabled"))
                    .withDescription(
                            "If this option is enabled and memory tuning is enabled, TaskManager memory will be increased when scaling down. This ensures that after applying memory tuning there is sufficient memory when running with fewer TaskManagers.");

    public static final ConfigOption<Double> MEMORY_TUNING_OVERHEAD =
            autoScalerConfig("memory.tuning.overhead")
                    .doubleType()
                    .defaultValue(0.2)
                    .withFallbackKeys(oldOperatorConfigKey("memory.tuning.overhead"))
                    .withDescription(
                            "Overhead to add to tuning decisions (0-1). This ensures spare capacity and allows the memory to grow beyond the dynamically computed limits, but never beyond the original memory limits.");

    public static final ConfigOption<Boolean> MEMORY_TUNING_MAXIMIZE_MANAGED_MEMORY =
            autoScalerConfig("memory.tuning.maximize-managed-memory")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys(oldOperatorConfigKey("memory.tuning.maximize-managed-memory"))
                    .withDescription(
                            "If enabled and managed memory is used (e.g. RocksDB turned on), any reduction of heap, network, or metaspace memory will increase the managed memory.");

    public static final ConfigOption<Integer> VERTEX_SCALING_HISTORY_COUNT =
            autoScalerConfig("history.max.count")
                    .intType()
                    .defaultValue(3)
                    .withFallbackKeys(oldOperatorConfigKey("history.max.count"))
                    .withDescription(
                            "Maximum number of past scaling decisions to retain per vertex.");

    public static final ConfigOption<Duration> VERTEX_SCALING_HISTORY_AGE =
            autoScalerConfig("history.max.age")
                    .durationType()
                    .defaultValue(Duration.ofHours(24))
                    .withFallbackKeys(oldOperatorConfigKey("history.max.age"))
                    .withDescription("Maximum age for past scaling decisions to retain.");

    public static final ConfigOption<MetricAggregator> BUSY_TIME_AGGREGATOR =
            autoScalerConfig("metrics.busy-time.aggregator")
                    .enumType(MetricAggregator.class)
                    .defaultValue(MetricAggregator.MAX)
                    .withFallbackKeys(oldOperatorConfigKey("metrics.busy-time.aggregator"))
                    .withDescription(
                            "Metric aggregator to use for busyTime metrics. This affects how true processing/output rate will be computed. Using max allows us to handle jobs with data skew more robustly, while avg may provide better stability when we know that the load distribution is even.");

    public static final ConfigOption<List<String>> VERTEX_EXCLUDE_IDS =
            autoScalerConfig("vertex.exclude.ids")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withFallbackKeys(oldOperatorConfigKey("vertex.exclude.ids"))
                    .withDescription(
                            "A (semicolon-separated) list of vertex ids in hexstring for which to disable scaling. Caution: For non-sink vertices this will still scale their downstream operators until https://issues.apache.org/jira/browse/FLINK-31215 is implemented.");

    public static final ConfigOption<Duration> SCALING_EVENT_INTERVAL =
            autoScalerConfig("scaling.event.interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withFallbackKeys(oldOperatorConfigKey("scaling.event.interval"))
                    .withDescription("Time interval to resend the identical event");

    public static final ConfigOption<Duration> FLINK_CLIENT_TIMEOUT =
            autoScalerConfig("flink.rest-client.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withFallbackKeys(oldOperatorConfigKey("flink.rest-client.timeout"))
                    .withDescription("The timeout for waiting the flink rest client to return.");

    public static final ConfigOption<MemorySize> MEMORY_QUOTA =
            autoScalerConfig("quota.memory")
                    .memoryType()
                    .noDefaultValue()
                    .withFallbackKeys(oldOperatorConfigKey("quota.memory"))
                    .withDescription(
                            "Quota of the memory size. When scaling would go beyond this number the the scaling is not going to happen.");

    public static final ConfigOption<Double> CPU_QUOTA =
            autoScalerConfig("quota.cpu")
                    .doubleType()
                    .noDefaultValue()
                    .withFallbackKeys(oldOperatorConfigKey("quota.cpu"))
                    .withDescription(
                            "Quota of the CPU count. When scaling would go beyond this number the the scaling is not going to happen.");

    public static final ConfigOption<JobVertexScaler.KeyGroupOrPartitionsAdjustMode>
            SCALING_KEY_GROUP_PARTITIONS_ADJUST_MODE =
                    autoScalerConfig("scaling.key-group.partitions.adjust.mode")
                            .enumType(JobVertexScaler.KeyGroupOrPartitionsAdjustMode.class)
                            .defaultValue(
                                    JobVertexScaler.KeyGroupOrPartitionsAdjustMode.EVENLY_SPREAD)
                            .withFallbackKeys(
                                    oldOperatorConfigKey(
                                            "scaling.key-group.partitions.adjust.mode"))
                            .withDescription(
                                    "How to adjust the parallelism of Source vertex or upstream shuffle is keyBy");

    public static final ConfigOption<Boolean> OBSERVED_SCALABILITY_ENABLED =
            autoScalerConfig("observed-scalability.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys(oldOperatorConfigKey("observed-scalability.enabled"))
                    .withDescription(
                            "Enables the use of an observed scalability coefficient when computing target parallelism. "
                                    + "If enabled, the system will estimate the scalability coefficient based on historical scaling data "
                                    + "instead of assuming perfect linear scaling. "
                                    + "This helps account for real-world inefficiencies such as network overhead and coordination costs.");

    public static final ConfigOption<Integer> OBSERVED_SCALABILITY_MIN_OBSERVATIONS =
            autoScalerConfig("observed-scalability.min-observations")
                    .intType()
                    .defaultValue(3)
                    .withFallbackKeys(oldOperatorConfigKey("observed-scalability.min-observations"))
                    .withDescription(
                            "Defines the minimum number of historical scaling observations required to estimate the scalability coefficient. "
                                    + "If the number of available observations is below this threshold, the system falls back to assuming linear scaling. "
                                    + "Note: To effectively use a higher minimum observation count, you need to increase "
                                    + VERTEX_SCALING_HISTORY_COUNT.key()
                                    + ". Avoid setting "
                                    + VERTEX_SCALING_HISTORY_COUNT.key()
                                    + " to a very high value, as the number of retained data points is limited by the size of the state store—"
                                    + "particularly when using Kubernetes-based state store.");

    public static final ConfigOption<Double> OBSERVED_SCALABILITY_COEFFICIENT_MIN =
            autoScalerConfig("observed-scalability.coefficient-min")
                    .doubleType()
                    .defaultValue(0.5)
                    .withFallbackKeys(oldOperatorConfigKey("observed-scalability.coefficient-min"))
                    .withDescription(
                            "Minimum allowed value for the observed scalability coefficient. "
                                    + "Prevents aggressive scaling by clamping low coefficient estimates. "
                                    + "If the estimated coefficient falls below this value, it is capped at the configured minimum.");
}
