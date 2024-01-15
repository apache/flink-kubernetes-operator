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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.mutator.DefaultFlinkMutator;
import org.apache.flink.kubernetes.operator.mutator.TestMutator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test class for {@link MutatorUtils}. */
public class MutatorUtilsTest {

    @TempDir public Path temporaryFolder;

    @Test
    public void testDiscoverMutators() throws IOException {
        Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            systemEnv.put(
                    ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                    TestUtils.getTestPluginsRootDir(temporaryFolder));
            TestUtils.setEnv(systemEnv);
            assertEquals(
                    new HashSet<>(
                            Arrays.asList(
                                    DefaultFlinkMutator.class.getName(),
                                    TestMutator.class.getName())),
                    MutatorUtils.discoverMutators(new FlinkConfigManager(new Configuration()))
                            .stream()
                            .map(v -> v.getClass().getName())
                            .collect(Collectors.toSet()));
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }
}
