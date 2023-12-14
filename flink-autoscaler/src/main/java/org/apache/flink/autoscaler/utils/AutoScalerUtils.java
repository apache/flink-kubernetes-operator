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

package org.apache.flink.autoscaler.utils;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.quartz.impl.calendar.CronCalendar;
import org.quartz.impl.calendar.DailyCalendar;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.autoscaler.metrics.ScalingMetric.CATCH_UP_DATA_RATE;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.TARGET_DATA_RATE;

/** AutoScaler utilities. */
public class AutoScalerUtils {
    public static double getTargetProcessingCapacity(
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Configuration conf,
            double targetUtilization,
            boolean withRestart,
            Duration restartTime) {

        // Target = Lag Catchup Rate + Restart Catchup Rate + Processing at utilization
        // Target = LAG/CATCH_UP + INPUT_RATE*RESTART/CATCH_UP + INPUT_RATE/TARGET_UTIL

        double lagCatchupTargetRate = evaluatedMetrics.get(CATCH_UP_DATA_RATE).getCurrent();
        if (Double.isNaN(lagCatchupTargetRate)) {
            return Double.NaN;
        }

        double catchUpTargetSec = conf.get(AutoScalerOptions.CATCH_UP_DURATION).toSeconds();

        targetUtilization = Math.max(0., targetUtilization);
        targetUtilization = Math.min(1., targetUtilization);

        double avgInputTargetRate = evaluatedMetrics.get(TARGET_DATA_RATE).getAverage();
        if (Double.isNaN(avgInputTargetRate)) {
            return Double.NaN;
        }

        if (targetUtilization == 0) {
            return Double.POSITIVE_INFINITY;
        }

        double restartCatchupRate =
                !withRestart || catchUpTargetSec == 0
                        ? 0
                        : (avgInputTargetRate * restartTime.toSeconds()) / catchUpTargetSec;
        double inputTargetAtUtilization = avgInputTargetRate / targetUtilization;

        return Math.round(lagCatchupTargetRate + restartCatchupRate + inputTargetAtUtilization);
    }

    /**
     * Temporarily exclude vertex from scaling for this run. This does not update the
     * scalingRealizer.
     */
    public static boolean excludeVertexFromScaling(Configuration conf, JobVertexID jobVertexId) {
        return excludeVerticesFromScaling(conf, List.of(jobVertexId));
    }

    public static boolean excludeVerticesFromScaling(
            Configuration conf, Collection<JobVertexID> ids) {
        Set<String> excludedIds = new HashSet<>(conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS));
        boolean anyAdded = false;
        for (JobVertexID id : ids) {
            String hexString = id.toHexString();
            anyAdded |= excludedIds.add(hexString);
        }
        conf.set(AutoScalerOptions.VERTEX_EXCLUDE_IDS, new ArrayList<>(excludedIds));
        return anyAdded;
    }

    /** Quartz doesn't have the invertTimeRange flag so rewrite this method. */
    static boolean isTimeIncluded(CronCalendar cron, long timeInMillis) {
        if (cron.getBaseCalendar() != null
                && !cron.getBaseCalendar().isTimeIncluded(timeInMillis)) {
            return false;
        } else {
            return cron.getCronExpression().isSatisfiedBy(new Date(timeInMillis));
        }
    }

    static Optional<DailyCalendar> interpretAsDaily(String subExpression) {
        String[] splits = subExpression.split("-");
        if (splits.length != 2) {
            return Optional.empty();
        }
        try {
            DailyCalendar daily = new DailyCalendar(splits[0], splits[1]);
            daily.setInvertTimeRange(true);
            return Optional.of(daily);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    static Optional<CronCalendar> interpretAsCron(String subExpression) {
        try {
            return Optional.of(new CronCalendar(subExpression));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    static Optional<String> validateExcludedExpression(String expression) {
        String[] subExpressions = expression.split("&&");
        Optional<DailyCalendar> daily = Optional.empty();
        Optional<CronCalendar> cron = Optional.empty();
        if (subExpressions.length > 2) {
            return Optional.of(
                    String.format(
                            "Invalid value %s in the autoscaler config %s",
                            expression, AutoScalerOptions.EXCLUDED_PERIODS.key()));
        }

        for (String subExpression : subExpressions) {
            subExpression = subExpression.strip();
            daily = interpretAsDaily(subExpression);
            cron = interpretAsCron(subExpression);

            if (daily.isEmpty() && cron.isEmpty()) {
                return Optional.of(
                        String.format(
                                "Invalid value %s in the autoscaler config %s, the value is neither a valid daily expression nor a valid cron expression",
                                expression, AutoScalerOptions.EXCLUDED_PERIODS.key()));
            }
        }

        if (subExpressions.length == 2 && (daily.isEmpty() || cron.isEmpty())) {
            return Optional.of(
                    String.format(
                            "Invalid value %s in the autoscaler config %s, the value can not be configured as dailyExpression && dailyExpression or cronExpression && cronExpression",
                            expression, AutoScalerOptions.EXCLUDED_PERIODS.key()));
        }
        return Optional.empty();
    }

    static boolean inExcludedPeriod(String expression, Instant instant) {
        String[] subExpressions = expression.split("&&");
        boolean result = true;
        for (String subExpression : subExpressions) {
            subExpression = subExpression.strip();
            Optional<DailyCalendar> daily = interpretAsDaily(subExpression);
            if (daily.isPresent()) {
                result = result && daily.get().isTimeIncluded(instant.toEpochMilli());
            } else {
                Optional<CronCalendar> cron = interpretAsCron(subExpression);
                result = result && isTimeIncluded(cron.get(), instant.toEpochMilli());
            }
        }
        return result;
    }

    public static boolean inExcludedPeriods(Configuration conf, Instant instant) {
        List<String> excludedExpressions = conf.get(AutoScalerOptions.EXCLUDED_PERIODS);
        for (String expression : excludedExpressions) {
            if (inExcludedPeriod(expression, instant)) {
                return true;
            }
        }
        return false;
    }

    public static Optional<String> validateExcludedPeriods(Configuration conf) {
        List<String> excludedExpressions = conf.get(AutoScalerOptions.EXCLUDED_PERIODS);
        for (String expression : excludedExpressions) {
            Optional<String> errorMsg = validateExcludedExpression(expression);
            if (errorMsg.isPresent()) {
                return errorMsg;
            }
        }
        return Optional.empty();
    }
}
