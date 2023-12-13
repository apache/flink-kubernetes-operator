package org.apache.flink.mutator;

import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.mutator.FlinkResourceMutator;

import java.util.Optional;

/** Custom Flink Mutator. */
public class CustomFlinkMutator implements FlinkResourceMutator {

    @Override
    public FlinkDeployment mutateDeployment(FlinkDeployment deployment) {

        return deployment;
    }

    @Override
    public FlinkSessionJob mutateSessionJob(
            FlinkSessionJob sessionJob, Optional<FlinkDeployment> session) {

        return sessionJob;
    }
}
