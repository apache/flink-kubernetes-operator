package org.apache.flink.kubernetes.operator.utils;

import org.apache.flink.autoscaler.ScalingDecisionFilter;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

public class AutoscalerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerUtils.class);

    @SuppressWarnings({"unchecked"})
    public static Collection<ScalingDecisionFilter<ResourceID, KubernetesJobAutoScalerContext>>
    discoverScalingDecisionFilters(Configuration conf) {
        var filters =
                new ArrayList<ScalingDecisionFilter<ResourceID, KubernetesJobAutoScalerContext>>();
        PluginUtils.createPluginManagerFromRootFolder(conf)
                .load(ScalingDecisionFilter.class)
                .forEachRemaining(
                        filter -> {
                            LOG.info(
                                    "Discovered ScalingDecisionFilter from plugin directory[{}]: {}.",
                                    System.getenv()
                                            .getOrDefault(
                                                    ConfigConstants.ENV_FLINK_PLUGINS_DIR,
                                                    ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS),
                                    filter.getClass().getName());
                            filters.add(filter);
                        });
        return filters;
    }
}
