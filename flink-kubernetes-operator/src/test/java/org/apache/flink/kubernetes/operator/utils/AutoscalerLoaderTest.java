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

package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.autoscaler.factory.JobAutoScalerFactory;
import org.apache.flink.autoscaler.factory.NoopJobAutoscalerFactory;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.kubernetes.operator.TestUtils;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.autoscaler.TestingAutoscalerFactory;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/** Test loading the default autoscaling implementation from the classpath. */
public class AutoscalerLoaderTest {

    @TempDir public Path temporaryFolder;

    @Test
    void testLoadFallbackNoopImplementation() {
        JobAutoScalerFactory<ResourceID, FlinkDeployment> factory =
                AutoscalerLoader.loadJobAutoscalerFactory();
        Assertions.assertTrue(factory instanceof NoopJobAutoscalerFactory);
    }

    @Test
    void testLoadCustomImplementation() throws Exception {
        Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            systemEnv.put(
                    ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                    TestUtils.getTestPluginsRootDir(temporaryFolder));
            TestUtils.setEnv(systemEnv);

            JobAutoScalerFactory<ResourceID, FlinkDeployment> factory =
                    AutoscalerLoader.loadJobAutoscalerFactory();
            Assertions.assertTrue(factory instanceof TestingAutoscalerFactory);
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }
}
