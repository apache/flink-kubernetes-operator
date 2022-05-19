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

package org.apache.flink.kubernetes.operator.listener;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link ListenerUtils}. */
public class ListenerUtilsTest {

    @TempDir Path temporaryFolder;

    @Test
    public void testListenerConfiguration() throws IOException {
        Map<String, String> testConfig = new HashMap<>();

        testConfig.put(
                "kubernetes.operator.plugins.listeners.test1.class",
                TestingListener.class.getName());
        testConfig.put("kubernetes.operator.plugins.listeners.test1.k1", "v1");
        testConfig.put("kubernetes.operator.plugins.listeners.test1.k2", "v2");
        testConfig.put("k3", "v3");

        Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            systemEnv.put(
                    ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                    TestUtils.getTestPluginsRootDir(temporaryFolder));
            TestUtils.setEnv(systemEnv);
            var listeners =
                    new ArrayList<>(
                            ListenerUtils.discoverListeners(
                                    new FlinkConfigManager(Configuration.fromMap(testConfig))));
            assertEquals(1, listeners.size());

            var testingListener = (TestingListener) listeners.get(0);
            assertEquals(
                    Map.of("k1", "v1", "k2", "v2", "class", TestingListener.class.getName()),
                    testingListener.config.toMap());
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }
}
