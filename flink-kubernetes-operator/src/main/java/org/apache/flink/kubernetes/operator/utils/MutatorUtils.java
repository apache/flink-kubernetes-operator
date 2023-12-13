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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;
import org.apache.flink.kubernetes.operator.mutator.DefaultFlinkMutator;
import org.apache.flink.kubernetes.operator.mutator.FlinkResourceMutator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/** Mutator utilities. */
public final class MutatorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MutatorUtils.class);

    /**
     * discovers mutators.
     *
     * @param configManager Flink Config manager
     * @return Set of FlinkResourceMutator
     */
    public static Set<FlinkResourceMutator> discoverMutators(FlinkConfigManager configManager) {
        var conf = configManager.getDefaultConfig();
        Set<FlinkResourceMutator> flinkmutator = new HashSet<>();
        DefaultFlinkMutator defaultFlinkMutator = new DefaultFlinkMutator();
        defaultFlinkMutator.configure(conf);
        flinkmutator.add(defaultFlinkMutator);
        PluginUtils.createPluginManagerFromRootFolder(conf)
                .load(FlinkResourceMutator.class)
                .forEachRemaining(
                        mutator -> {
                            LOG.info(
                                    "Discovered mutator from plugin directory[{}]: {}.",
                                    System.getenv()
                                            .getOrDefault(
                                                    ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                                                    ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS),
                                    mutator.getClass().getName());
                            mutator.configure(conf);
                            flinkmutator.add(mutator);
                        });
        return flinkmutator;
    }
}
