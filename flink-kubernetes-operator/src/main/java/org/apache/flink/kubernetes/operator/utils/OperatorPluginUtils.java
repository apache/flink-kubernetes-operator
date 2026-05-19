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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Builds the single, process-wide {@link PluginManager} used by the operator and webhook for plugin
 * discovery (metric reporters, file systems, validators, mutators, listeners).
 *
 * <p>Plugin classloaders delegate {@code io.fabric8} and {@code com.fasterxml} to the parent so all
 * plugins share the operator's fabric8 client and Jackson, avoiding duplicate informers and version
 * skew.
 */
public final class OperatorPluginUtils {

    private static final List<String> EXTRA_PARENT_FIRST_PATTERNS =
            List.of("io.fabric8", "com.fasterxml");

    private OperatorPluginUtils() {}

    /** Returns a copy of {@code baseConf} with the operator's parent-first patterns merged in. */
    private static Configuration enrichWithPluginParentFirstPatterns(Configuration baseConf) {
        var conf = new Configuration(baseConf);
        var patterns =
                new ArrayList<>(
                        conf.getOptional(
                                        CoreOptions
                                                .PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL)
                                .orElse(Collections.emptyList()));
        for (var p : EXTRA_PARENT_FIRST_PATTERNS) {
            if (!patterns.contains(p)) {
                patterns.add(p);
            }
        }
        conf.set(CoreOptions.PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL, patterns);
        return conf;
    }

    /** Creates the shared {@link PluginManager} from {@code baseConf}, enriching it as needed. */
    public static PluginManager createPluginManager(Configuration baseConf) {
        return PluginUtils.createPluginManagerFromRootFolder(
                enrichWithPluginParentFirstPatterns(baseConf));
    }
}
