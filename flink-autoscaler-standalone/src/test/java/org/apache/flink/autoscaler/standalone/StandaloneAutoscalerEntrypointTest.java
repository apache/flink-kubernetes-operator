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

package org.apache.flink.autoscaler.standalone;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.CONTROL_LOOP_INTERVAL;
import static org.apache.flink.autoscaler.standalone.config.AutoscalerStandaloneOptions.CONTROL_LOOP_PARALLELISM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class StandaloneAutoscalerEntrypointTest {
    @Test
    public void testLoadConfiguration() {
        // Test for loading configuration from file.
        Configuration conf = StandaloneAutoscalerEntrypoint.loadConfiguration(new String[0]);
        assertNotNull(conf);
        assertEquals(Duration.ofMinutes(1), conf.get(CONTROL_LOOP_INTERVAL));
        assertEquals(20, conf.get(CONTROL_LOOP_PARALLELISM));
        // Test for args override
        String[] args =
                new String[] {
                    "--autoscaler.standalone.control-loop.interval",
                    "2min",
                    "--autoscaler" + ".standalone.control-loop.parallelism",
                    "10"
                };
        Configuration confOverride = StandaloneAutoscalerEntrypoint.loadConfiguration(args);
        assertNotNull(confOverride);
        assertEquals(Duration.ofMinutes(2), confOverride.get(CONTROL_LOOP_INTERVAL));
        assertEquals(10, confOverride.get(CONTROL_LOOP_PARALLELISM));
    }
}
