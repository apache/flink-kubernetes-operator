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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/** Date and time related utilities. */
public class DateTimeUtils {

    private static final DateTimeFormatter DEFAULT_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Convert an Instant to a readable format for the system default zone.
     *
     * @param instant The Instant to convert.
     * @return The readable format in the system default zone.
     */
    public static String readable(Instant instant) {
        return readable(instant, ZoneId.systemDefault());
    }

    /**
     * Convert an Instant to a readable format for a given zone.
     *
     * @param instant The Instant to convert.
     * @param zoneId The ZoneId to apply.
     * @return The readable format in the specified time zone.
     */
    public static String readable(Instant instant, ZoneId zoneId) {
        ZonedDateTime dateTime = instant.atZone(zoneId);
        return dateTime.format(DEFAULT_FORMATTER);
    }

    /**
     * Convert an Instant to a format that is used in Kubernetes.
     *
     * @param instant The Instant to convert.
     * @return The Kubernetes format in the system default zone.
     */
    public static String kubernetes(Instant instant) {
        ZonedDateTime dateTime = instant.atZone(ZoneId.systemDefault());
        return dateTime.format(DateTimeFormatter.ISO_INSTANT);
    }

    /**
     * Parses a Kubernetes-compatible datetime.
     *
     * @param datetime datetime in Kubernetes format
     * @return time parsed
     */
    public static Instant parseKubernetes(String datetime) {
        return Instant.parse(datetime);
    }
}
