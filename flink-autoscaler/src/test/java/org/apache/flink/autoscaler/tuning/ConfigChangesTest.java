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

package org.apache.flink.autoscaler.tuning;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ConfigChanges}. */
class ConfigChangesTest {

    @Test
    void testOverridesSerializedInLegacyDialect() {
        boolean previousDialect = GlobalConfiguration.isStandardYaml();
        GlobalConfiguration.setStandardYaml(true);
        try {
            var globalJobParameters = new LinkedHashMap<String, String>();
            globalJobParameters.put("k1", "v1");
            globalJobParameters.put("k2", "v2");
            var changes =
                    new ConfigChanges()
                            .addOverride(PipelineOptions.JARS, List.of("a.jar", "b.jar"))
                            .addOverride(PipelineOptions.GLOBAL_JOB_PARAMETERS, globalJobParameters)
                            .addOverride(
                                    TaskManagerOptions.TOTAL_PROCESS_MEMORY,
                                    MemorySize.ofMebiBytes(1024));

            assertThat(changes.getOverrides().get(PipelineOptions.JARS.key()))
                    .isEqualTo("a.jar;b.jar");
            assertThat(changes.getOverrides().get(PipelineOptions.GLOBAL_JOB_PARAMETERS.key()))
                    .isEqualTo("k1:v1,k2:v2");
            assertThat(changes.getOverrides().get(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key()))
                    .isEqualTo("1 gb");
        } finally {
            GlobalConfiguration.setStandardYaml(previousDialect);
        }
    }
}
