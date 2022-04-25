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

package org.apache.flink.kubernetes.operator.config;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.OperatorUtils;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_DYNAMIC_CONFIG_CHECK_INTERVAL;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_DYNAMIC_CONFIG_ENABLED;

/** Configuration manager for the Flink operator. */
public class FlinkConfigManager {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkConfigManager.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final int MAX_CACHE_SIZE = 1000;

    private volatile Configuration defaultConfig;
    private volatile FlinkOperatorConfiguration operatorConfiguration;

    private final Cache<Tuple4<String, String, ObjectNode, Boolean>, Configuration> cache;
    private Set<String> namespaces = OperatorUtils.getWatchedNamespaces();

    public FlinkConfigManager() {
        this(GlobalConfiguration.loadConfiguration());
    }

    public FlinkConfigManager(Configuration defaultConfig) {
        Duration rescheduleInterval =
                defaultConfig.get(
                        KubernetesOperatorConfigOptions.OPERATOR_RECONCILER_RESCHEDULE_INTERVAL);
        this.cache =
                CacheBuilder.newBuilder()
                        .maximumSize(MAX_CACHE_SIZE)
                        .expireAfterAccess(Duration.ofSeconds(rescheduleInterval.getSeconds() * 3))
                        .removalListener(
                                removalNotification ->
                                        FlinkConfigBuilder.cleanupTmpFiles(
                                                (Configuration) removalNotification.getValue()))
                        .build();

        updateDefaultConfig(defaultConfig);
        if (defaultConfig.getBoolean(OPERATOR_DYNAMIC_CONFIG_ENABLED)) {
            scheduleConfigWatcher();
        }
    }

    public Configuration getDefaultConfig() {
        return defaultConfig;
    }

    @VisibleForTesting
    public void updateDefaultConfig(Configuration newConf) {
        if (newConf.equals(defaultConfig)) {
            LOG.info("Default configuration did not change, nothing to do...");
            return;
        }

        LOG.info("Updating default configuration");
        this.operatorConfiguration =
                FlinkOperatorConfiguration.fromConfiguration(newConf, namespaces);
        this.defaultConfig = newConf.clone();
        cache.invalidateAll();
    }

    public FlinkOperatorConfiguration getOperatorConfiguration() {
        return operatorConfiguration;
    }

    public Configuration getObserveConfig(FlinkDeployment deployment) {
        return getConfig(
                deployment.getMetadata(), ReconciliationUtils.getDeployedSpec(deployment), false);
    }

    public Configuration getDeployConfig(ObjectMeta objectMeta, FlinkDeploymentSpec spec) {
        return getConfig(objectMeta, spec, true);
    }

    @SneakyThrows
    private Configuration getConfig(
            ObjectMeta objectMeta, FlinkDeploymentSpec spec, boolean forDeploy) {

        var key =
                Tuple4.of(
                        objectMeta.getNamespace(),
                        objectMeta.getName(),
                        objectMapper.convertValue(spec, ObjectNode.class),
                        forDeploy);

        var conf = cache.getIfPresent(key);
        if (conf == null) {
            conf = generateConfig(key);
            cache.put(key, conf);
        }

        // Always return a copy of the configuration to avoid polluting the cache
        return conf.clone();
    }

    private Configuration generateConfig(Tuple4<String, String, ObjectNode, Boolean> key) {
        try {
            LOG.info("Generating new {} config", key.f3 ? "deployment" : "observe");
            return FlinkConfigBuilder.buildFrom(
                    key.f0,
                    key.f1,
                    objectMapper.convertValue(key.f2, FlinkDeploymentSpec.class),
                    defaultConfig,
                    key.f3);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    private void scheduleConfigWatcher() {
        var checkInterval = defaultConfig.get(OPERATOR_DYNAMIC_CONFIG_CHECK_INTERVAL);
        var millis = checkInterval.toMillis();
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(new ConfigUpdater(), millis, millis, TimeUnit.MILLISECONDS);
        LOG.info("Enabled dynamic config updates, checking config changes every {}", checkInterval);
    }

    @VisibleForTesting
    public void setWatchedNamespaces(Set<String> namespaces) {
        this.namespaces = namespaces;
        operatorConfiguration =
                FlinkOperatorConfiguration.fromConfiguration(defaultConfig, namespaces);
    }

    private class ConfigUpdater implements Runnable {
        public void run() {
            try {
                LOG.debug("Checking for config update changes...");
                updateDefaultConfig(GlobalConfiguration.loadConfiguration());
            } catch (Exception e) {
                LOG.error("Error while updating operator configuration", e);
            }
        }
    }
}
