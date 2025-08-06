package org.apache.flink.kubernetes.operator.controller.bluegreen;

import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentStatus;
import org.apache.flink.kubernetes.operator.controller.FlinkBlueGreenDeployments;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Simplified context object containing all the necessary state and dependencies for Blue/Green
 * deployment state transitions.
 */
@Getter
@RequiredArgsConstructor
public class BlueGreenContext {
    private final FlinkBlueGreenDeployment bgDeployment;
    private final FlinkBlueGreenDeploymentStatus deploymentStatus;
    private final Context<FlinkBlueGreenDeployment> josdkContext;
    private final FlinkBlueGreenDeployments deployments;
    private final FlinkResourceContextFactory ctxFactory;

    public String getDeploymentName() {
        return bgDeployment.getMetadata().getName();
    }

    public FlinkDeployment getBlueDeployment() {
        return deployments != null ? deployments.getFlinkDeploymentBlue() : null;
    }

    public FlinkDeployment getGreenDeployment() {
        return deployments != null ? deployments.getFlinkDeploymentGreen() : null;
    }

    public FlinkDeployment getDeploymentByType(DeploymentType type) {
        return type == DeploymentType.BLUE ? getBlueDeployment() : getGreenDeployment();
    }
}
