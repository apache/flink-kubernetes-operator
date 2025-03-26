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

package org.apache.flink.kubernetes.operator.hooks;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;

import lombok.experimental.UtilityClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** Utility class for flink resource hooks. */
@UtilityClass
public class FlinkResourceHookUtils {

    public static final String FLINK_RESOURCES_HOOKS_ACTIVE_KEY = "flink.resources.hooks.active";
    public static final String FLINK_RESOURCES_HOOKS_RECONCILIATION_INTERVAL_KEY =
            "flink.resources.hooks.reconciliation-interval";

    private static final Logger LOG = LoggerFactory.getLogger(FlinkResourceHookUtils.class);

    public static Collection<FlinkResourceHook> discoverHooks(FlinkConfigManager configManager) {
        var conf = configManager.getDefaultConfig();
        Set<FlinkResourceHook> hooks = new HashSet<>();
        PluginUtils.createPluginManagerFromRootFolder(conf)
                .load(FlinkResourceHook.class)
                .forEachRemaining(
                        hook -> {
                            LOG.info(
                                    "Discovered hooks from plugin directory[{}]: {}.",
                                    System.getenv()
                                            .getOrDefault(
                                                    ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                                                    ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS),
                                    hook.getClass().getName());
                            hook.configure(conf);
                            hooks.add(hook);
                        });
        return hooks;
    }

    public static boolean areHooksActive(AbstractFlinkSpec lastReconciledSpec) {
        return lastReconciledSpec != null
                && lastReconciledSpec.getFlinkConfiguration() != null
                && lastReconciledSpec
                        .getFlinkConfiguration()
                        .containsKey(FLINK_RESOURCES_HOOKS_ACTIVE_KEY);
    }

    public static Optional<Duration> getHooksReconciliationInterval(
            AbstractFlinkSpec reconciledSpec) {
        if (areHooksActive(reconciledSpec)) {
            try {
                var reconcileInterval =
                        Duration.parse(
                                reconciledSpec
                                        .getFlinkConfiguration()
                                        .get(
                                                FlinkResourceHookUtils
                                                        .FLINK_RESOURCES_HOOKS_RECONCILIATION_INTERVAL_KEY));
                return Optional.of(reconcileInterval);
            } catch (Exception e) {
                LOG.error("Failed to parse reconciliation interval", e);
            }
        }
        return Optional.empty();
    }
}
