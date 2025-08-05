package org.apache.flink.kubernetes.operator.controller.bluegreen.handlers;

import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

/** State handler for the ACTIVE_BLUE state. */
public class ActiveBlueStateHandler extends AbstractBlueGreenStateHandler {

    public ActiveBlueStateHandler(BlueGreenDeploymentService deploymentService) {
        super(FlinkBlueGreenDeploymentState.ACTIVE_BLUE, deploymentService);
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> handle(BlueGreenContext context) {
        return deploymentService.checkAndInitiateDeployment(context, DeploymentType.BLUE);
    }
}
