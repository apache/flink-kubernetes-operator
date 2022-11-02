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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.K8S_OP_CONF_PREFIX;

/** Flink resource listener utilities. */
public class ListenerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ListenerUtils.class);

    private static final String PREFIX = K8S_OP_CONF_PREFIX + "plugins.listeners.";
    private static final String SUFFIX = ".class";
    private static final Pattern PTN =
            Pattern.compile(Pattern.quote(PREFIX) + "([\\S&&[^.]]*)" + Pattern.quote(SUFFIX));
    private static final List<String> EXTRA_PARENT_FIRST_PATTERNS =
            List.of("io.fabric8", "com.fasterxml");

    /**
     * Load {@link FlinkResourceListener} implementations from the plugin directory. Only listeners
     * that are explicitly named and configured will be enabled.
     *
     * <p>Config format: kubernetes.operator.plugins.listeners.test.class: com.myorg.MyListener
     * kubernetes.operator.plugins.listeners.test.k1: v1
     *
     * @param configManager {@link FlinkConfigManager} to access plugin configurations.
     * @return Enabled listeners.
     */
    public static Collection<FlinkResourceListener> discoverListeners(
            FlinkConfigManager configManager) {
        var listeners = new ArrayList<FlinkResourceListener>();
        var conf = getListenerBaseConf(configManager);

        Map<String, Configuration> listenerConfigs = loadListenerConfigs(conf);
        PluginUtils.createPluginManagerFromRootFolder(conf)
                .load(FlinkResourceListener.class)
                .forEachRemaining(
                        listener -> {
                            String clazz = listener.getClass().getName();
                            LOG.info(
                                    "Discovered resource listener from plugin directory[{}]: {}.",
                                    System.getenv()
                                            .getOrDefault(
                                                    ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                                                    ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS),
                                    clazz);

                            if (listenerConfigs.containsKey(clazz)) {
                                LOG.info("Initializing {}", clazz);
                                listener.configure(listenerConfigs.get(clazz));
                                listeners.add(listener);
                            } else {
                                LOG.warn("No configuration found for {}", clazz);
                            }
                        });
        return listeners;
    }

    private static Configuration getListenerBaseConf(FlinkConfigManager configManager) {
        var conf = new Configuration(configManager.getDefaultConfig());
        List<String> additionalPatterns =
                new ArrayList<>(
                        conf.getOptional(
                                        CoreOptions
                                                .PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL)
                                .orElse(Collections.emptyList()));
        additionalPatterns.addAll(EXTRA_PARENT_FIRST_PATTERNS);
        conf.set(
                CoreOptions.PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL,
                additionalPatterns);
        return conf;
    }

    @VisibleForTesting
    protected static Map<String, Configuration> loadListenerConfigs(Configuration configuration) {
        Map<String, Configuration> listenerConfigs = new HashMap<>();
        for (var enableListeners : findEnabledListeners(configuration)) {
            DelegatingConfiguration delegatingConfiguration =
                    new DelegatingConfiguration(configuration, PREFIX + enableListeners.f0 + '.');
            listenerConfigs.put(enableListeners.f1, delegatingConfiguration);
        }
        return listenerConfigs;
    }

    private static Set<Tuple2<String, String>> findEnabledListeners(Configuration configuration) {
        Set<Tuple2<String, String>> result = new HashSet<>();
        for (String key : configuration.keySet()) {
            Matcher matcher = PTN.matcher(key);
            if (matcher.matches()) {
                result.add(
                        Tuple2.of(
                                matcher.group(1),
                                configuration.get(
                                        ConfigOptions.key(key).stringType().noDefaultValue())));
            }
        }
        return result;
    }
}
