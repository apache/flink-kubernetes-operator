package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.flink.kubernetes.operator.decorators.KubernetesStepDecoratorPlugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/** Decorator utilities. */
public final class DecoratorUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DecoratorUtils.class);

    public static Set<KubernetesStepDecorator> discoverDecorators(
            Configuration configuration,
            KubernetesStepDecoratorPlugin.DecoratorComponent decoratorComponent) {
        Set<KubernetesStepDecorator> decorators = new HashSet<>();
        LOG.info(
                "Discovering kubernetes decorator from plugin directory[{}].",
                System.getenv()
                        .getOrDefault(
                                ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                                ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS));

        PluginUtils.createPluginManagerFromRootFolder(configuration)
                .load(KubernetesStepDecoratorPlugin.class)
                .forEachRemaining(
                        decorator -> {
                            if (decorator.getDecoratorComponent() == decoratorComponent) {
                                LOG.info(
                                        "Discovered kubernetes decorator for component: {} from plugin directory[{}]: {}.",
                                        decoratorComponent,
                                        System.getenv()
                                                .getOrDefault(
                                                        ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                                                        ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS),
                                        decorator.getClass().getName());

                                decorators.add(decorator);
                            }
                        });
        return decorators;
    }
}
