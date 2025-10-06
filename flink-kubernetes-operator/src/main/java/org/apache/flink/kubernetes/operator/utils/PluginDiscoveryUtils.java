package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.operator.config.FlinkConfigManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class PluginDiscoveryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PluginDiscoveryUtils.class);

    public static <T> Set<T> discoverResources(
            FlinkConfigManager configManager, Class<T> resourceClass) {
        var conf = configManager.getDefaultConfig();

        Set<T> resources = new HashSet<>();

        PluginUtils.createPluginManagerFromRootFolder(conf)
                .load(resourceClass)
                .forEachRemaining(
                        resource -> {
                            LOG.info(
                                    "Discovered resource from plugin directory [{}]: {}.",
                                    System.getenv()
                                            .getOrDefault(
                                                    ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                                                    ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS),
                                    resource.getClass().getName());
                            resources.add(resource);
                        });

        return resources;
    }
}
