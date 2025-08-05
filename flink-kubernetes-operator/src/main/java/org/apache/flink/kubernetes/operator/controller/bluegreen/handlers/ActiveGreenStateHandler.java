package org.apache.flink.kubernetes.operator.controller.bluegreen.handlers;

import org.apache.flink.kubernetes.operator.api.FlinkBlueGreenDeployment;
import org.apache.flink.kubernetes.operator.api.bluegreen.DeploymentType;
import org.apache.flink.kubernetes.operator.api.status.FlinkBlueGreenDeploymentState;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenContext;
import org.apache.flink.kubernetes.operator.controller.bluegreen.BlueGreenDeploymentService;

import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

/** State handler for the ACTIVE_GREEN state. */
public class ActiveGreenStateHandler extends AbstractBlueGreenStateHandler {

    public ActiveGreenStateHandler(BlueGreenDeploymentService deploymentService) {
        super(FlinkBlueGreenDeploymentState.ACTIVE_GREEN, deploymentService);
    }

    @Override
    public UpdateControl<FlinkBlueGreenDeployment> handle(BlueGreenContext context) {
        return deploymentService.checkAndInitiateDeployment(context, DeploymentType.GREEN);
    }
}
