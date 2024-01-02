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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for CalendarUtils. */
public class CalendarUtilsTest {

    @Test
    public void testValidateExcludedPeriods() {
        Configuration conf = new Configuration();
        conf.set(AutoScalerOptions.EXCLUDED_PERIODS, List.of("09:13:17-08:15:18"));
        assertTrue(CalendarUtils.validateExcludedPeriods(conf).isPresent());

        conf.set(AutoScalerOptions.EXCLUDED_PERIODS, List.of("09:13:17-25:15:18"));
        assertTrue(CalendarUtils.validateExcludedPeriods(conf).isPresent());

        conf.set(
                AutoScalerOptions.EXCLUDED_PERIODS,
                List.of("09:13:17-11:15:18", "18:01:20-16:00:00"));
        assertTrue(CalendarUtils.validateExcludedPeriods(conf).isPresent());

        conf.set(
                AutoScalerOptions.EXCLUDED_PERIODS,
                List.of("09:13:17-11:15:18 && 12:01:20-16:00:00"));
        assertTrue(CalendarUtils.validateExcludedPeriods(conf).isPresent());

        conf.set(
                AutoScalerOptions.EXCLUDED_PERIODS,
                List.of("09:13:17-11:15:18", "14:01:20-16:00:00"));
        assertTrue(CalendarUtils.validateExcludedPeriods(conf).isEmpty());

        conf.set(AutoScalerOptions.EXCLUDED_PERIODS, List.of("* * * ? * 2,5555"));
        assertTrue(CalendarUtils.validateExcludedPeriods(conf).isPresent());

        conf.set(AutoScalerOptions.EXCLUDED_PERIODS, List.of("* * * ? * 2,5 && 18:01:20-16:00:00"));
        assertTrue(CalendarUtils.validateExcludedPeriods(conf).isPresent());

        conf.set(AutoScalerOptions.EXCLUDED_PERIODS, List.of("* * * ? * 2,5 && * * 11-13 * * ?"));
        assertTrue(CalendarUtils.validateExcludedPeriods(conf).isPresent());

        conf.set(AutoScalerOptions.EXCLUDED_PERIODS, List.of("* * * ? * 2,5 && 14:01:20-16:00:00"));
        assertTrue(CalendarUtils.validateExcludedPeriods(conf).isEmpty());
    }

    @Test
    public void testExcludedPeriods() {
        Configuration conf = new Configuration();
        conf.set(
                AutoScalerOptions.EXCLUDED_PERIODS,
                List.of("09:13:17-11:15:18", "14:01:20-16:00:00"));
        // 2023-12-04 is Thursday
        ZonedDateTime zonedDateTime =
                ZonedDateTime.of(
                        LocalDate.of(2023, 12, 14),
                        LocalTime.of(14, 01, 30),
                        ZoneId.systemDefault());
        Instant instant = Instant.ofEpochSecond(zonedDateTime.toEpochSecond());
        assertTrue(CalendarUtils.inExcludedPeriods(conf, instant));
        assertFalse(CalendarUtils.inExcludedPeriods(conf, instant.minusSeconds(20)));
        assertTrue(CalendarUtils.inExcludedPeriods(conf, instant.minus(4, ChronoUnit.HOURS)));

        conf.set(AutoScalerOptions.EXCLUDED_PERIODS, List.of("* * 14-16 * * ?"));
        assertTrue(CalendarUtils.inExcludedPeriods(conf, instant));
        assertFalse(CalendarUtils.inExcludedPeriods(conf, instant.minusSeconds(100)));

        // excluded periods is 14:01:20-16:00:00 in Monday and Thursday
        conf.set(AutoScalerOptions.EXCLUDED_PERIODS, List.of("* * * ? * 2,5 && 14:01:20-16:00:00"));
        assertTrue(CalendarUtils.inExcludedPeriods(conf, instant));
        assertFalse(CalendarUtils.inExcludedPeriods(conf, instant.minus(1, ChronoUnit.DAYS)));
        assertTrue(CalendarUtils.inExcludedPeriods(conf, instant.minus(3, ChronoUnit.DAYS)));
    }
}
