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
import org.apache.flink.kubernetes.operator.validation.DefaultValidator;
import org.apache.flink.kubernetes.operator.validation.TestValidator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test class for {@link ValidatorUtils}. */
public class ValidatorUtilsTest {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String VALIDATOR_NAME = "test-validator";
    private static final String VALIDATOR_JAR = VALIDATOR_NAME + "-test-jar.jar";

    @Test
    public void testDiscoverValidators() throws IOException {
        File validatorRootFolder = temporaryFolder.newFolder();
        File testValidatorFolder = new File(validatorRootFolder, VALIDATOR_NAME);
        assertTrue(testValidatorFolder.mkdirs());
        File testValidatorJar = new File("target", VALIDATOR_JAR);
        assertTrue(testValidatorJar.exists());
        Files.copy(
                testValidatorJar.toPath(),
                Paths.get(testValidatorFolder.toString(), VALIDATOR_JAR));
        Map<String, String> originalEnv = System.getenv();
        try {
            Map<String, String> systemEnv = new HashMap<>(originalEnv);
            systemEnv.put(ConfigConstants.ENV_FLINK_PLUGINS_DIR, validatorRootFolder.getPath());
            TestUtils.setEnv(systemEnv);
            assertEquals(
                    new HashSet<>(
                            Arrays.asList(
                                    DefaultValidator.class.getName(),
                                    TestValidator.class.getName())),
                    ValidatorUtils.discoverValidators(new FlinkConfigManager(new Configuration()))
                            .stream()
                            .map(v -> v.getClass().getName())
                            .collect(Collectors.toSet()));
        } finally {
            TestUtils.setEnv(originalEnv);
        }
    }
}
