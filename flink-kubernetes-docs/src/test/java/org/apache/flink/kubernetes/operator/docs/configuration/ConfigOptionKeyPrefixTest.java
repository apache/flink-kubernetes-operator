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

package org.apache.flink.kubernetes.operator.docs.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.YamlParserUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.operator.docs.configuration.ConfigOptionsDocGenerator.DEFAULT_PATH_PREFIX;
import static org.apache.flink.kubernetes.operator.docs.configuration.ConfigOptionsDocGenerator.LOCATIONS;
import static org.apache.flink.kubernetes.operator.docs.configuration.ConfigOptionsDocGenerator.processConfigOptions;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for FLINK-40873: no config key may be a strict prefix of another config key.
 * When both sides of such a pair are present in a flat config map, converting it to yaml fails with
 * {@code ClassCastException: class java.lang.String cannot be cast to class java.util.Map}.
 */
class ConfigOptionKeyPrefixTest {

    @Test
    void testNoConfigKeyIsAPrefixOfAnotherConfigKey() throws Exception {
        final List<String> keys = collectConfigKeys();
        assertThat(keys).isNotEmpty();

        final List<String> sortedKeys = keys.stream().distinct().sorted().toList();
        final List<String> conflicts = new ArrayList<>();
        for (int i = 0; i < sortedKeys.size(); i++) {
            final String prefix = sortedKeys.get(i) + '.';
            for (int j = i + 1; j < sortedKeys.size(); j++) {
                if (sortedKeys.get(j).startsWith(prefix)) {
                    conflicts.add(sortedKeys.get(i) + " < " + sortedKeys.get(j));
                }
            }
        }

        assertThat(conflicts)
                .as(
                        "A config key must not be a strict prefix of another config key, otherwise "
                                + "dumping the config to yaml fails")
                .isEmpty();
    }

    @Test
    void testAllConfigKeysConvertToYamlAndBack(@TempDir Path tempDir) throws Exception {
        final Map<String, Object> flatConfiguration = new LinkedHashMap<>();
        collectConfigKeys().forEach(key -> flatConfiguration.put(key, "value"));

        // Before FLINK-40873 this threw a ClassCastException for the overlapping keys.
        final List<String> yamlLines =
                YamlParserUtils.convertAndDumpYamlFromFlatMap(flatConfiguration);

        final File yamlFile = tempDir.resolve("flink-conf.yaml").toFile();
        Files.write(yamlFile.toPath(), yamlLines, StandardCharsets.UTF_8);

        assertThat(flatten(YamlParserUtils.loadYamlFile(yamlFile))).isEqualTo(flatConfiguration);
    }

    private static List<String> collectConfigKeys() throws Exception {
        final String rootDir = ConfigOptionsDocGeneratorTest.getProjectRootDir();
        final List<String> keys = new ArrayList<>();
        for (OptionsClassLocation location : LOCATIONS) {
            processConfigOptions(
                    rootDir,
                    location.getModule(),
                    location.getPackage(),
                    DEFAULT_PATH_PREFIX,
                    optionsClass -> {
                        for (Field field : optionsClass.getFields()) {
                            if (!ConfigOption.class.equals(field.getType())) {
                                continue;
                            }
                            try {
                                keys.add(((ConfigOption<?>) field.get(null)).key());
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException("Cannot read " + field, e);
                            }
                        }
                    });
        }
        return keys;
    }

    private static Map<String, Object> flatten(Map<String, Object> nested) {
        final Map<String, Object> flat = new LinkedHashMap<>();
        flattenInto(flat, "", nested);
        return flat;
    }

    private static void flattenInto(Map<String, Object> flat, String prefix, Map<?, ?> nested) {
        nested.forEach(
                (key, value) -> {
                    final String fullKey =
                            prefix.isEmpty() ? String.valueOf(key) : prefix + '.' + key;
                    if (value instanceof Map) {
                        flattenInto(flat, fullKey, (Map<?, ?>) value);
                    } else {
                        flat.put(fullKey, value);
                    }
                });
    }
}
