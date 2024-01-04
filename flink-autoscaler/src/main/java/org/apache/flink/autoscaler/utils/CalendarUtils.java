/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.flink.configuration.Configuration;

import org.quartz.impl.calendar.CronCalendar;
import org.quartz.impl.calendar.DailyCalendar;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/** Calendar utilities. */
public class CalendarUtils {

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
        Optional<DailyCalendar> dailyCalendar = Optional.empty();
        Optional<CronCalendar> cronCalendar = Optional.empty();
        if (subExpressions.length > 2) {
            return Optional.of(
                    String.format(
                            "Invalid value %s in the autoscaler config %s",
                            expression, AutoScalerOptions.EXCLUDED_PERIODS.key()));
        }

        for (String subExpression : subExpressions) {
            subExpression = subExpression.strip();
            Optional<DailyCalendar> daily = interpretAsDaily(subExpression);
            dailyCalendar = daily.isPresent() ? daily : dailyCalendar;
            Optional<CronCalendar> cron = interpretAsCron(subExpression);
            cronCalendar = cron.isPresent() ? cron : cronCalendar;

            if (daily.isEmpty() && cron.isEmpty()) {
                return Optional.of(
                        String.format(
                                "Invalid value %s in the autoscaler config %s, the value is neither a valid daily expression nor a valid cron expression",
                                expression, AutoScalerOptions.EXCLUDED_PERIODS.key()));
            }
        }

        if (subExpressions.length == 2 && (dailyCalendar.isEmpty() || cronCalendar.isEmpty())) {
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
