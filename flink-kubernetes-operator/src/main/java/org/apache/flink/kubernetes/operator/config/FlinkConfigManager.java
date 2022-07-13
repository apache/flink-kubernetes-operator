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
import org.apache.flink.kubernetes.operator.crd.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.crd.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

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
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

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
    private final Consumer<Set<String>> namespaceListener;

    @VisibleForTesting
    public FlinkConfigManager(Configuration defaultConfig) {
        this(defaultConfig, ns -> {});
    }

    public FlinkConfigManager(Consumer<Set<String>> namespaceListener) {
        this(loadGlobalConfiguration(), namespaceListener);
    }

    public FlinkConfigManager(
            Configuration defaultConfig, Consumer<Set<String>> namespaceListener) {
        this.namespaceListener = namespaceListener;
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
        return defaultConfig.clone();
    }

    @VisibleForTesting
    public void updateDefaultConfig(Configuration newConf) {
        if (ObjectUtils.allNotNull(this.defaultConfig, newConf)
                && this.defaultConfig.toMap().equals(newConf.toMap())) {
            LOG.info("Default configuration did not change, nothing to do...");
            return;
        } else {
            LOG.info("Setting default configuration to {}", newConf);
        }

        var oldNs =
                Optional.ofNullable(this.operatorConfiguration)
                        .map(FlinkOperatorConfiguration::getWatchedNamespaces)
                        .orElse(Set.of());
        this.operatorConfiguration = FlinkOperatorConfiguration.fromConfiguration(newConf);
        var newNs = this.operatorConfiguration.getWatchedNamespaces();
        if (this.operatorConfiguration.isDynamicNamespacesEnabled() && !oldNs.equals(newNs)) {
            this.namespaceListener.accept(operatorConfiguration.getWatchedNamespaces());
        }
        this.defaultConfig = newConf.clone();
        // We do not invalidate the cache to avoid deleting currently used temp files,
        // simply bump the version
        this.defaultConfigVersion.incrementAndGet();
    }

    public FlinkOperatorConfiguration getOperatorConfiguration() {
        return operatorConfiguration;
    }

    public Configuration getDeployConfig(ObjectMeta objectMeta, FlinkDeploymentSpec spec) {
        var conf = getConfig(objectMeta, spec);
        FlinkUtils.setGenerationAnnotation(conf, objectMeta.getGeneration());
        return conf;
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
            FlinkDeployment deployment, FlinkSessionJobSpec sessionJobSpec) {
        Configuration sessionJobConfig = getObserveConfig(deployment);

        // merge session job specific config
        var sessionJobFlinkConfiguration = sessionJobSpec.getFlinkConfiguration();
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
    protected Cache<Key, Configuration> getCache() {
        return cache;
    }

    private static Configuration loadGlobalConfiguration() {
        return loadGlobalConfiguration(EnvUtils.get(EnvUtils.ENV_CONF_OVERRIDE_DIR));
    }

    @VisibleForTesting
    protected static Configuration loadGlobalConfiguration(Optional<String> confOverrideDir) {
        if (confOverrideDir.isPresent()) {
            Configuration configOverrides =
                    GlobalConfiguration.loadConfiguration(confOverrideDir.get());
            LOG.debug("Loading default configuration with overrides from " + confOverrideDir.get());
            return GlobalConfiguration.loadConfiguration(configOverrides);
        }
        LOG.debug("Loading default configuration");
        return GlobalConfiguration.loadConfiguration();
    }

    private class ConfigUpdater implements Runnable {
        public void run() {
            try {
                LOG.debug("Checking for config update changes...");
                updateDefaultConfig(loadGlobalConfiguration());
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
