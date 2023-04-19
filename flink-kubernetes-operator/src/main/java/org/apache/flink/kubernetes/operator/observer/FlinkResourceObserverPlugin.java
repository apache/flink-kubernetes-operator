package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.core.plugin.Plugin;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;

/** The Observer Plugin of custom resource. */
public interface FlinkResourceObserverPlugin<CR extends AbstractFlinkResource<?, ?>>
        extends Plugin {
    enum ObserverType {
        FLINK_DEPLOYMENT,
        FLINK_SESSION_JOB
    }

    /** The custom resource type to observe. */
    default ObserverType getObserverType() {
        return ObserverType.FLINK_DEPLOYMENT;
    }

    void observe(FlinkResourceContext<CR> ctx);
}
