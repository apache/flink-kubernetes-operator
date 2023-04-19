package org.apache.flink.kubernetes.operator.observer;

import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;

public class TestFlinkResourceObserverPlugin
        implements FlinkResourceObserverPlugin<FlinkSessionJob> {
    @Override
    public ObserverType getObserverType() {
        return ObserverType.FLINK_SESSION_JOB;
    }

    @Override
    public void observe(FlinkResourceContext<FlinkSessionJob> ctx) {}
}
