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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DateTimeUtils}. */
public class DateTimeUtilsTest {

    @Test
    public void testConvertInstantToReadableFormat() {
        Instant instant = Instant.ofEpochMilli(1702456327000L);
        String readableFormat1 = DateTimeUtils.readable(instant, ZoneId.of("Asia/Shanghai"));
        String readableFormat2 = DateTimeUtils.readable(instant, ZoneId.of("Europe/Berlin"));
        assertThat(readableFormat1).isEqualTo("2023-12-13 16:32:07");
        assertThat(readableFormat2).isEqualTo("2023-12-13 09:32:07");
    }
}
