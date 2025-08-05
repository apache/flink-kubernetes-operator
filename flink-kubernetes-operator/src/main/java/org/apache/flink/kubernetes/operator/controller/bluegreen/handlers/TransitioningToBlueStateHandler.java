package org.apache.flink.kubernetes.operator.controller.bluegreen.handlers;

import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

/** State handler for the TRANSITIONING_TO_BLUE state. */
public class TransitioningToBlueStateHandler extends AbstractBlueGreenStateHandler {

    public TransitioningToBlueStateHandler(BlueGreenDeploymentService deploymentService) {
        super(FlinkBlueGreenDeploymentState.TRANSITIONING_TO_BLUE, deploymentService);
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> handle(BlueGreenContext context) {
        return deploymentService.monitorTransition(context, DeploymentType.GREEN);
    }
}
