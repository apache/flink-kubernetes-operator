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
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkSessionJobSpec;
import org.apache.flink.kubernetes.operator.api.spec.FlinkVersion;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.utils.EnvUtils;
import org.apache.flink.kubernetes.operator.utils.FlinkUtils;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava31.com.google.common.cache.LoadingCache;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.configuration.CheckpointingOptions.SAVEPOINT_DIRECTORY;
import static org.apache.flink.kubernetes.operator.config.FlinkConfigBuilder.applyJobConfig;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.K8S_OP_CONF_PREFIX;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.NAMESPACE_CONF_PREFIX;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_DYNAMIC_CONFIG_CHECK_INTERVAL;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.OPERATOR_DYNAMIC_CONFIG_ENABLED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.SNAPSHOT_RESOURCE_ENABLED;
import static org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions.VERSION_CONF_PREFIX;

/** Configuration manager for the Flink operator. */
public class FlinkConfigManager {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkConfigManager.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private volatile Configuration defaultConfig;
    private volatile FlinkOperatorConfiguration defaultOperatorConfiguration;
    // TODO: From 1.11 release, snapshot CRD should be mandatory and this can be removed.
    private final boolean snapshotCrdInstalled;
    private final AtomicLong defaultConfigVersion = new AtomicLong(0);
    private final LoadingCache<Key, Configuration> cache;
    private final Consumer<Set<String>> namespaceListener;
    private volatile Map<FlinkVersion, List<String>> relevantFlinkVersionPrefixes;

    protected static final Pattern FLINK_VERSION_PATTERN =
            Pattern.compile(
                    VERSION_CONF_PREFIX.replaceAll("\\.", "\\\\\\.")
                            + "v(?<major>\\d+)_(?<minor>\\d+)(?<gt>\\"
                            + KubernetesOperatorConfigOptions.FLINK_VERSION_GREATER_THAN_SUFFIX
                            + ")?\\..*");

    @VisibleForTesting
    public FlinkConfigManager(Configuration defaultConfig) {
        this(defaultConfig, ns -> {}, true);
    }

    public FlinkConfigManager(
            Consumer<Set<String>> namespaceListener, boolean snapshotCrdInstalled) {
        this(loadGlobalConfiguration(), namespaceListener, snapshotCrdInstalled);
    }

    public FlinkConfigManager(
            Configuration defaultConfig,
            Consumer<Set<String>> namespaceListener,
            boolean snapshotCrdInstalled) {
        this.snapshotCrdInstalled = snapshotCrdInstalled;
        this.namespaceListener = namespaceListener;
        Duration cacheTimeout =
                defaultConfig.get(KubernetesOperatorConfigOptions.OPERATOR_CONFIG_CACHE_TIMEOUT);
        this.relevantFlinkVersionPrefixes = new HashMap<>();
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

    /**
     * Update the base configuration for the operator. Newly generated configs (observe, deploy,
     * etc.) will use this as the base.
     *
     * @param newConf New config base.
     */
    @VisibleForTesting
    public void updateDefaultConfig(Configuration newConf) {
        if (ObjectUtils.allNotNull(this.defaultConfig, newConf)
                && this.defaultConfig.toMap().equals(newConf.toMap())) {
            LOG.info("Default configuration did not change, nothing to do...");
            return;
        } else {
            LOG.info("Setting default configuration to {}", newConf);
        }

        // Even if snapshot resources are enabled, we need to disable them if the CRD was not
        // installed in the current Kubernetes cluster.
        if (newConf.get(SNAPSHOT_RESOURCE_ENABLED) && !snapshotCrdInstalled) {
            LOG.warn(
                    "{} CRD was not installed, snapshot resources will be disabled!",
                    FlinkStateSnapshot.class.getSimpleName());
            newConf.set(SNAPSHOT_RESOURCE_ENABLED, false);
        }

        var oldNs =
                Optional.ofNullable(this.defaultOperatorConfiguration)
                        .map(FlinkOperatorConfiguration::getWatchedNamespaces)
                        .orElse(Set.of());
        this.defaultOperatorConfiguration = FlinkOperatorConfiguration.fromConfiguration(newConf);
        var newNs = this.defaultOperatorConfiguration.getWatchedNamespaces();
        if (this.defaultOperatorConfiguration.isDynamicNamespacesEnabled()
                && !oldNs.equals(newNs)) {
            this.namespaceListener.accept(defaultOperatorConfiguration.getWatchedNamespaces());
        }
        this.defaultConfig = newConf.clone();
        // We do not invalidate the cache to avoid deleting currently used temp files,
        // simply bump the version
        this.defaultConfigVersion.incrementAndGet();

        // We clear the cached relevant Flink version prefixes as the base config may include new
        // version overrides.
        // This will trigger a regeneration of the prefixes in the next call to getDefaultConfig.
        relevantFlinkVersionPrefixes = new HashMap<>();
    }

    /**
     * @return The base configuration for Flink Operator. This is not tied to any specific resource
     *     and is aimed to be used for platform level settings.
     */
    public FlinkOperatorConfiguration getOperatorConfiguration() {
        return defaultOperatorConfiguration;
    }

    /**
     * @return The base configuration for Flink Operator. This is not tied to any specific resource
     *     and is aimed to be used for platform level settings.
     */
    public Configuration getDefaultConfig() {
        return defaultConfig.clone();
    }

    /**
     * Get the operator configuration for the given namespace and flink version combination. This is
     * different from the platform level base config as it may contain namespaces or version
     * overrides.
     *
     * @param namespace Resource namespace
     * @param flinkVersion Resource Flink version
     * @return Base config
     */
    public FlinkOperatorConfiguration getOperatorConfiguration(
            String namespace, FlinkVersion flinkVersion) {
        return FlinkOperatorConfiguration.fromConfiguration(
                getDefaultConfig(namespace, flinkVersion));
    }

    /**
     * This method will search the keys of the supplied map and find any that contain a flink
     * version string that is relevant to the supplied flink version.
     *
     * <p>Relevance is defined as any key with the {@link
     * KubernetesOperatorConfigOptions#VERSION_CONF_PREFIX} followed by either the supplied flink
     * version (with or without the {@link
     * KubernetesOperatorConfigOptions#FLINK_VERSION_GREATER_THAN_SUFFIX}) or a lower flink version
     * string followed by the {@link
     * KubernetesOperatorConfigOptions#FLINK_VERSION_GREATER_THAN_SUFFIX}.
     *
     * <p>Prefixes are returned in ascending order of flink version.
     *
     * @param baseConfMap The configuration map that should be searched for relevant Flink version
     *     prefixes.
     * @return A list of relevant Flink version prefixes in order of ascending Flink version.
     */
    protected static List<String> getRelevantVersionPrefixes(
            Map<String, String> baseConfMap, FlinkVersion flinkVersion) {
        SortedMap<FlinkVersion, String> greaterThanVersionPrefixes = new TreeMap<>();

        for (Map.Entry<String, String> entry : baseConfMap.entrySet()) {
            Matcher versionMatcher = FLINK_VERSION_PATTERN.matcher(entry.getKey());
            if (versionMatcher.matches() && versionMatcher.group("gt") != null) {
                try {
                    FlinkVersion keyFlinkVersion =
                            FlinkVersion.fromMajorMinor(
                                    Integer.parseInt(versionMatcher.group("major")),
                                    Integer.parseInt(versionMatcher.group("minor")));
                    if (flinkVersion.isEqualOrNewer(keyFlinkVersion)) {
                        greaterThanVersionPrefixes.put(
                                keyFlinkVersion,
                                VERSION_CONF_PREFIX
                                        + keyFlinkVersion
                                        + KubernetesOperatorConfigOptions
                                                .FLINK_VERSION_GREATER_THAN_SUFFIX
                                        + ".");
                    }
                } catch (NumberFormatException numberFormatException) {
                    LOG.warn("Unable to parse version number in config key: {}", entry.getKey());
                } catch (IllegalArgumentException illegalArgumentException) {
                    LOG.warn("Unknown Flink version in config key: {}", entry.getKey());
                }
            }
        }

        // Extract the prefixes from the sorted map, these will be ascending Flink version order
        List<String> sortedRelevantVersionPrefixes =
                new ArrayList<>(greaterThanVersionPrefixes.values());

        // Add the current flink version prefix (without the greater than symbol) to the set.
        // Any current flink version prefix with the greater than symbol would already have been
        // added
        // in the loop above.
        sortedRelevantVersionPrefixes.add(VERSION_CONF_PREFIX + flinkVersion + ".");

        return sortedRelevantVersionPrefixes;
    }

    /**
     * Get the base configuration for the given namespace and flink version combination. This is
     * different from the platform level base config as it may contain namespaces or version
     * overrides.
     *
     * @param namespace Resource namespace
     * @param flinkVersion Resource Flink version
     * @return Base config
     */
    public Configuration getDefaultConfig(String namespace, FlinkVersion flinkVersion) {
        var baseConfMap = defaultConfig.toMap();
        var conf = new Configuration(defaultConfig);

        if (namespace != null) {
            applyDefault(NAMESPACE_CONF_PREFIX + namespace + ".", baseConfMap, conf);
        }

        if (flinkVersion != null) {
            // Fetch or create a list of Flink version configs that apply to this current
            // FlinkVersion. That will include all versions that are equal to or lower than
            // the current one that are suffixed by a `+`
            List<String> versionPrefixes =
                    relevantFlinkVersionPrefixes.computeIfAbsent(
                            flinkVersion,
                            fv -> getRelevantVersionPrefixes(baseConfMap, flinkVersion));

            // The version prefixes are returned in ascending order of Flink version, so configs
            // attached to newer versions will override older ones. For example v1_16+.conf1 will
            // be overridden if a key containing v1_18+.conf1 is present.
            for (String versionPrefix : versionPrefixes) {
                applyDefault(versionPrefix, baseConfMap, conf);
            }
        }

        return conf;
    }

    /**
     * Get deployment configuration that will be passed to the Flink Cluster clients during cluster
     * submission.
     *
     * @param objectMeta Resource meta
     * @param spec Resource spec
     * @return Deployment config
     */
    public Configuration getDeployConfig(ObjectMeta objectMeta, FlinkDeploymentSpec spec) {
        var conf = getConfig(objectMeta, spec);
        FlinkUtils.setGenerationAnnotation(conf, objectMeta.getGeneration());
        return conf;
    }

    /**
     * Get the observe configuration that can be used to interact with already submitted clusters
     * through the Flink rest clients.
     *
     * @param deployment Deployment resource
     * @return Observe config
     */
    public Configuration getObserveConfig(FlinkDeployment deployment) {
        var deployedSpec = ReconciliationUtils.getDeployedSpec(deployment);
        if (deployedSpec == null) {
            throw new RuntimeException(
                    "Cannot create observe config before first deployment, this indicates a bug.");
        }
        var conf = getConfig(deployment.getMetadata(), deployedSpec);
        applyConfigsFromCurrentSpec(deployment.getSpec(), conf, SAVEPOINT_DIRECTORY);
        return conf;
    }

    private void addOperatorConfigsFromSpec(AbstractFlinkSpec spec, Configuration conf) {
        // Observe config should include the latest operator related settings
        if (spec.getFlinkConfiguration() != null) {
            spec.getFlinkConfiguration()
                    .forEach(
                            (k, v) -> {
                                if (k.startsWith(K8S_OP_CONF_PREFIX)
                                        || k.startsWith(AutoScalerOptions.AUTOSCALER_CONF_PREFIX)) {
                                    conf.setString(k, v);
                                }
                            });
        }
    }

    @SuppressWarnings("unchecked")
    private void applyConfigsFromCurrentSpec(
            AbstractFlinkSpec spec, Configuration conf, ConfigOption... configOptions) {
        addOperatorConfigsFromSpec(spec, conf);
        if (spec.getFlinkConfiguration() != null) {
            var deployConfig = Configuration.fromMap(spec.getFlinkConfiguration());
            for (ConfigOption configOption : configOptions) {
                deployConfig.getOptional(configOption).ifPresent(v -> conf.set(configOption, v));
            }
        }
    }

    /**
     * Get configuration for interacting with session jobs. Similar to the observe configuration for
     * FlinkDeployments.
     *
     * @param deployment FlinkDeployment for the session cluster
     * @param sessionJobSpec Session job spec
     * @return Session job config
     */
    public Configuration getSessionJobConfig(
            String name, FlinkDeployment deployment, FlinkSessionJobSpec sessionJobSpec) {
        Configuration sessionJobConfig = getObserveConfig(deployment);

        // merge session job specific config
        var sessionJobFlinkConfiguration = sessionJobSpec.getFlinkConfiguration();
        if (sessionJobFlinkConfiguration != null) {
            sessionJobFlinkConfiguration.forEach(sessionJobConfig::setString);
        }
        applyJobConfig(name, sessionJobConfig, sessionJobSpec.getJob());
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
            LOG.debug("Generating new config");
            var spec = objectMapper.convertValue(key.spec, FlinkDeploymentSpec.class);
            return FlinkConfigBuilder.buildFrom(
                    key.namespace,
                    key.name,
                    spec,
                    getDefaultConfig(key.namespace, spec.getFlinkVersion()));
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
            LOG.debug(
                    "Loading default configuration with overrides from {}", confOverrideDir.get());
            return GlobalConfiguration.loadConfiguration(configOverrides);
        }
        LOG.debug("Loading default configuration");
        return GlobalConfiguration.loadConfiguration();
    }

    private static void applyDefault(
            String prefix, Map<String, String> from, Configuration toConf) {
        int pLen = prefix.length();
        from.forEach(
                (k, v) -> {
                    if (k.startsWith(prefix)) {
                        toConf.setString(k.substring(pLen), v);
                    }
                });
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
