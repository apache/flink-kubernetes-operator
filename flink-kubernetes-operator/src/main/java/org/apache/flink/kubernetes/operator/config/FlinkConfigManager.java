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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.operator.crd.FlinkDeployment;
import org.apache.flink.kubernetes.operator.crd.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.OperatorUtils;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava30.com.google.common.cache.LoadingCache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_DYNAMIC_CONFIG_CHECK_INTERVAL;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_DYNAMIC_CONFIG_ENABLED;

/** Configuration manager for the Flink operator. */
public class FlinkConfigManager {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkConfigManager.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private volatile Configuration defaultConfig;
    private volatile FlinkOperatorConfiguration operatorConfiguration;
    private final AtomicLong defaultConfigVersion = new AtomicLong(0);

    private final LoadingCache<Key, Configuration> cache;
    private Set<String> namespaces = OperatorUtils.getWatchedNamespaces();

    public FlinkConfigManager() {
        this(GlobalConfiguration.loadConfiguration());
    }

    public FlinkConfigManager(Configuration defaultConfig) {
        Duration cacheTimeout =
                defaultConfig.get(KubernetesOperatorConfigOptions.OPERATOR_CONFIG_CACHE_TIMEOUT);
        this.cache =
                CacheBuilder.newBuilder()
                        .maximumSize(
                                defaultConfig.get(
                                        KubernetesOperatorConfigOptions.OPERATOR_CONFIG_CACHE_SIZE))
                        .expireAfterAccess(cacheTimeout)
                        .removalListener(
                                removalNotification ->
                                        FlinkConfigBuilder.cleanupTmpFiles(
                                                (Configuration) removalNotification.getValue()))
                        .build(
                                new CacheLoader<>() {
                                    @Override
                                    public Configuration load(Key k) {
                                        return generateConfig(k);
                                    }
                                });

        updateDefaultConfig(defaultConfig);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay(
                cache::cleanUp,
                cacheTimeout.toMillis(),
                cacheTimeout.toMillis(),
                TimeUnit.MILLISECONDS);

        if (defaultConfig.getBoolean(OPERATOR_DYNAMIC_CONFIG_ENABLED)) {
            scheduleConfigWatcher(executorService);
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

        LOG.info("Updating default configuration to {}", newConf);
        this.operatorConfiguration =
                FlinkOperatorConfiguration.fromConfiguration(newConf, namespaces);
        this.defaultConfig = newConf.clone();
        // We do not invalidate the cache to avoid deleting currently used temp files,
        // simply bump the version
        this.defaultConfigVersion.incrementAndGet();
    }

    public FlinkOperatorConfiguration getOperatorConfiguration() {
        return operatorConfiguration;
    }

    public Configuration getDeployConfig(ObjectMeta objectMeta, FlinkDeploymentSpec spec) {
        return getConfig(objectMeta, spec);
    }

    public Configuration getObserveConfig(FlinkDeployment deployment) {
        var deployedSpec = ReconciliationUtils.getDeployedSpec(deployment);
        if (deployedSpec == null) {
            throw new RuntimeException(
                    "Cannot create observe config before first deployment, this indicates a bug.");
        }
        return getConfig(deployment.getMetadata(), deployedSpec);
    }

    public Configuration getSessionJobConfig(
            FlinkDeployment deployment, FlinkSessionJob flinkSessionJob) {
        Configuration sessionJobConfig = getObserveConfig(deployment);

        // merge session job specific config
        Map<String, String> sessionJobFlinkConfiguration =
                flinkSessionJob.getSpec().getFlinkConfiguration();
        if (sessionJobFlinkConfiguration != null) {
            sessionJobFlinkConfiguration.forEach(sessionJobConfig::setString);
        }
        return sessionJobConfig;
    }

    @SneakyThrows
    private Configuration getConfig(ObjectMeta objectMeta, FlinkDeploymentSpec spec) {
        var key =
                Key.builder()
                        .configVersion(defaultConfigVersion.get())
                        .name(objectMeta.getName())
                        .namespace(objectMeta.getNamespace())
                        .spec(objectMapper.convertValue(spec, ObjectNode.class))
                        .build();

        // Always return a copy of the configuration to avoid polluting the cache
        return cache.get(key).clone();
    }

    private Configuration generateConfig(Key key) {
        try {
            LOG.info("Generating new config");
            return FlinkConfigBuilder.buildFrom(
                    key.namespace,
                    key.name,
                    objectMapper.convertValue(key.spec, FlinkDeploymentSpec.class),
                    defaultConfig);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    private void scheduleConfigWatcher(ScheduledExecutorService executorService) {
        var checkInterval = defaultConfig.get(OPERATOR_DYNAMIC_CONFIG_CHECK_INTERVAL);
        var millis = checkInterval.toMillis();
        executorService.scheduleAtFixedRate(
                new ConfigUpdater(), millis, millis, TimeUnit.MILLISECONDS);
        LOG.info("Enabled dynamic config updates, checking config changes every {}", checkInterval);
    }

    @VisibleForTesting
    public void setWatchedNamespaces(Set<String> namespaces) {
        this.namespaces = namespaces;
        operatorConfiguration =
                FlinkOperatorConfiguration.fromConfiguration(defaultConfig, namespaces);
    }

    @VisibleForTesting
    protected Cache<Key, Configuration> getCache() {
        return cache;
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

    @Value
    @Builder
    private static class Key {
        long configVersion;
        String namespace;
        String name;
        ObjectNode spec;
    }
}
